package blockvalidation

import (
	"bufio"
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// bufioReaderPool reduces GC pressure by reusing bufio.Reader instances.
// Using 32KB buffers provides excellent I/O performance for sequential reads
// while dramatically reducing memory pressure and GC overhead (16x reduction from previous 512KB).
var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 1024*1024) // Temp changed to 1MB buffer for scaling env - 32KB buffer - optimized for sequential I/O
	},
}

// bufioWriterPool reuses the buffers that absorb bt.Tx.SerializeTo's many small writes when a
// subtree_data body is streamed into the blob store. 64 KiB, not 1 MiB: the buffer exists only to
// stop each 4-byte and varint-sized write becoming a synchronous io.Pipe rendezvous, which 64 KiB
// does just as well, and up to blockvalidation_fetch_num_workers x
// blockvalidation_subtree_fetch_concurrency of these are live at once
// (bsv-blockchain/teranode#1139).
var bufioWriterPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewWriterSize(nil, 64*1024)
	},
}

// SubtreeWriteJob represents a subtree file write that can be processed asynchronously.
// The subtree structure is built synchronously (needed for merkle validation),
// but the actual I/O can be deferred to a background worker pool.
type SubtreeWriteJob struct {
	SubtreeHash   chainhash.Hash
	Subtree       *subtreepkg.Subtree // Serialized lazily by write worker to avoid holding bytes in channel
	BlockHash     string              // For logging
	BlockHeight   uint32              // For DAH calculation
	SubtreeIdx    int                 // For logging
	AlreadyExists bool                // Skip write if already exists
	// Done, when set, is counted down exactly once this job has been fully handled (written,
	// skipped, or errored) by a worker — never per worker-goroutine exit. tryQuickValidation
	// waits on it before running cleanup, so cleanup can never race ahead of a still-in-flight
	// write for the same block (bitcoin-sv/teranode#4692). Nil-guarded everywhere it is used, so a
	// job constructed without one (existing tests, or a future direct caller of
	// subtreeWriteWorker) never panics on a nil receiver.
	Done *sync.WaitGroup
}

// subtreeWriteWorker processes subtree write jobs from a channel.
// If any write fails, it returns an error which cancels the errgroup context,
// propagating the failure to all other goroutines including UTXO processing.
//
// Each received job's handling is wrapped in an immediately-invoked function so that
// job.Done.Done() (bitcoin-sv/teranode#4692) is scoped to that ONE job, not to this worker
// goroutine's eventual exit: a bare `defer job.Done.Done()` placed directly in the `case` body
// would defer to when subtreeWriteWorker itself returns (channel close, ctx cancellation, or an
// error) — i.e. after every block in the whole catch-up run, not this job's own block. Placed
// that way, a caller blocked in wg.Wait() for exactly this job would never observe it complete
// until the entire run finished, a real deadlock. The closure form covers every exit for this
// job — AlreadyExists, a serialize error, a store error, and success — in one place.
func (u *BlockValidation) subtreeWriteWorker(ctx context.Context, writeJobsChan <-chan *SubtreeWriteJob) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-writeJobsChan:
			if !ok {
				// Channel closed, all jobs processed
				return nil
			}

			if err := func() error {
				if job.Done != nil {
					defer job.Done.Done()
				}

				if job.AlreadyExists {
					// Subtree already exists with assembly's finite DAH — no change needed.
					// The block persister will promote to permanent when the block is confirmed.
					return nil
				}

				// Serialize lazily at write time to avoid holding bytes in the channel buffer
				subtreeBytes, err := job.Subtree.Serialize()
				if err != nil {
					return errors.NewProcessingError("[subtreeWriteWorker][%s] failed to serialize subtree %d (%s)", job.BlockHash, job.SubtreeIdx, job.SubtreeHash.String(), err)
				}

				// Write the subtree file with finite DAH (temporary until block persister confirms)
				dah := job.BlockHeight + u.subtreeBlockHeightRetention
				if err := u.subtreeStore.Set(ctx,
					job.SubtreeHash[:],
					fileformat.FileTypeSubtree,
					subtreeBytes,
					bloboptions.WithAllowOverwrite(true),
					bloboptions.WithDeleteAt(dah),
				); err != nil {
					return errors.NewProcessingError("[subtreeWriteWorker][%s] failed to store subtree %d (%s)", job.BlockHash, job.SubtreeIdx, job.SubtreeHash.String(), err)
				}

				return nil
			}(); err != nil {
				return err
			}
		}
	}
}

// buildSubtreeAndQueueWrite builds the subtree structure synchronously (needed for merkle validation)
// and returns a write job that can be processed asynchronously.
// This separates the CPU-bound work (building/serializing) from I/O-bound work (writing).
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: The block being processed
//   - subtreeIdx: Index of the subtree in block.Subtrees
//   - subtree: The subtree structure with transaction hashes
//   - txs: Transactions in this subtree (excluding coinbase nil entry)
//   - subtreeHash: Hash of the subtree
//   - carriedFullSubtree: the already-present full .subtree blob, loaded and anchored
//     to its key during the batch read, or nil when no such blob existed
//
// Returns:
//   - *SubtreeWriteJob: Job to be processed by async writer (nil if no write needed)
//   - error: If building the subtree fails
func (u *BlockValidation) buildSubtreeAndQueueWrite(_ context.Context, block *model.Block, subtreeIdx int, subtree *subtreepkg.Subtree, txs []*bt.Tx, subtreeHash chainhash.Hash, carriedFullSubtree *subtreepkg.Subtree, outpointOnly bool) (*SubtreeWriteJob, error) {
	// The full subtree already existed, and the batch reader loaded it, anchored it
	// to its key and handed it over. It is deliberately NOT read here: this runs in
	// the arm beside createAndSpendUTXOsForBatch, so anything a read here could
	// establish would be established after the mutations had already begun
	// (bitcoin-sv/teranode#4838). Its root was memoized by the reader, on the
	// reader's goroutine, before it was published.
	if carriedFullSubtree != nil {
		block.SubtreeSlices[subtreeIdx] = carriedFullSubtree

		return &SubtreeWriteJob{
			SubtreeHash:   subtreeHash,
			BlockHash:     block.Hash().String(),
			BlockHeight:   block.Height,
			SubtreeIdx:    subtreeIdx,
			AlreadyExists: true,
		}, nil
	}

	// Build new subtree (no disk I/O needed - we know it doesn't exist)
	fullSubtree, err := subtreepkg.NewIncompleteTreeByLeafCount(subtree.Size())
	if err != nil {
		return nil, errors.NewProcessingError("[buildSubtreeAndQueueWrite][%s] failed to create full subtree %s", block.Hash().String(), subtreeHash.String(), err)
	}

	// Add coinbase node for first subtree
	if subtreeIdx == 0 {
		if err = fullSubtree.AddCoinbaseNode(); err != nil {
			return nil, errors.NewProcessingError("[buildSubtreeAndQueueWrite][%s] failed to add coinbase node to full subtree %s", block.Hash().String(), subtreeHash.String(), err)
		}
	}

	for _, tx := range txs {
		var fee uint64
		if !outpointOnly {
			var err error
			fee, err = util.GetFees(tx)
			if err != nil {
				return nil, errors.NewProcessingError("[buildSubtreeAndQueueWrite][%s] failed to get fee for tx %s in subtree %s", block.Hash().String(), tx.TxIDChainHash().String(), subtreeHash.String(), err)
			}
		}

		sizeInBytes := uint64(tx.Size())

		if err := fullSubtree.AddNode(*tx.TxIDChainHash(), fee, sizeInBytes); err != nil {
			return nil, errors.NewProcessingError("[buildSubtreeAndQueueWrite][%s] failed to add tx node %s to full subtree %s", block.Hash().String(), tx.TxIDChainHash().String(), subtreeHash.String(), err)
		}
	}

	// Set on block for merkle validation (synchronous)
	block.SubtreeSlices[subtreeIdx] = fullSubtree

	// Memoize the root hash on THIS (building) goroutine, before the job is handed to the
	// async writer (bitcoin-sv/teranode#4692). go-subtree's RootHash() lazily computes and
	// caches into st.rootHash on first call, with no locking around the memoization write.
	// The write worker's Serialize() calls RootHash() too, and CheckMerkleRoot's Duplicate()
	// concurrently reads st.rootHash on this SAME object once stage 3's g.Wait() hands off to
	// validateSubtrees — a write on the worker goroutine racing a read on the merkle-check
	// goroutine over the same field. Calling RootHash() once here — before the channel send,
	// which gives the worker its happens-before edge — means every later call, on either
	// goroutine, hits the already-populated fast path (`if st.rootHash != nil`) and only ever
	// reads. RootHash() is nil-safe on an empty subtree (returns nil rather than panicking),
	// so no separate emptiness guard is needed here.
	_ = fullSubtree.RootHash()

	return &SubtreeWriteJob{
		SubtreeHash:   subtreeHash,
		Subtree:       fullSubtree,
		BlockHash:     block.Hash().String(),
		BlockHeight:   block.Height,
		SubtreeIdx:    subtreeIdx,
		AlreadyExists: false,
	}, nil
}

// blockIDToUint32 narrows an assigned (uint64) block id to the uint32 the model
// uses, erroring instead of silently wrapping — a wrap would alias a different
// block's id and break the one-id-per-hash idempotency AssignBlockID guarantees.
func blockIDToUint32(id uint64, blockHash string) (uint32, error) {
	v, err := safeconversion.Uint64ToUint32(id)
	if err != nil {
		return 0, errors.NewProcessingError("[%s] assigned block id %d exceeds uint32", blockHash, id, err)
	}
	return v, nil
}

// quickValidateBlock performs optimized validation for blocks below checkpoints.
// This follows the legacy sync approach: create all UTXOs first, then validate later.
// This is safe because checkpoints guarantee these blocks are valid.
// NOTE: Since BlockValidation doesn't have direct access to the validator,
// we focus on UTXO creation which is the main optimization.
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: Block to validate
//
// Returns:
//   - error: If validation fails
func (u *BlockValidation) quickValidateBlock(ctx context.Context, block *model.Block, peerID, baseURL string) (err error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "quickValidateBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[quickValidateBlock][%s] performing quick validation for checkpointed block at height %d", block.Hash().String(), block.Height),
	)
	defer deferFn()

	// The ONE boundary for a subtree blob whose nodes do not hash to its key. Written
	// as a deferred rewrite of the named return rather than a call on each failure
	// path, because the whole-block pass and all three processing variants return
	// through here and a future return must not be able to bypass it. A no-op for
	// every error that does not carry the marker (bitcoin-sv/teranode#4838).
	defer func() {
		err = u.quarantineSubtreeKeyMismatch(ctx, err)
	}()

	// Enforce the block-version floor before any body/coinbase inspection (header-first parity
	// with svnode ContextualCheckBlockHeader). Needs only header version, height, and params.
	if err := model.CheckBlockVersion(block.Header.Version, block.Height, u.settings.ChainCfgParams); err != nil {
		return errors.NewBlockInvalidError("[quickValidateBlock][%s] outdated block version", block.Hash().String(), err)
	}

	// Reject blocks without a valid coinbase (e.g. from seeded peers that don't have full block data)
	if block.CoinbaseTx == nil || len(block.CoinbaseTx.Inputs) == 0 {
		return errors.NewBlockIncompleteError("[quickValidateBlock][%s] coinbase tx is nil or has no inputs, peer may not have full block data", block.Hash().String())
	}

	// Bind a no-subtrees body to the header before anything else touches it
	// (bitcoin-sv/teranode#4692). This route never calls block.Valid, so without this the
	// coinbase-only binding that model applies at its own binding block never runs here at all, and
	// a peer-chosen body for an honest header hash — subtree list emptied, any coinbase it likes —
	// reaches commitBlock and is recorded with subtrees_set and mined_set, with nothing to revisit
	// it. A no-op when the body carries subtrees: those are bound by validateSubtrees' CheckMerkleRoot
	// further down. Placed ahead of AssignBlockID so a corrupt body never even takes a block ID.
	if err := block.CheckCoinbaseOnlyBodyBound(); err != nil {
		return err
	}

	// The binding says the header commits to this body; it does not say the single
	// transaction in a no-subtree body is coinbase-shaped. The route's other coinbase
	// checks are len(Inputs) != 0 and the scriptSig length bound, neither of which
	// rejects an ordinary spend standing in for the coinbase. Deliberately placed AFTER
	// the binding: the transaction is then the miner's own committed body, so a
	// non-coinbase one is genuine invalidity — ahead of the binding the same failure
	// would be indistinguishable from a corrupted download (bitcoin-sv/teranode#4692).
	// Scoped to the no-subtree shape, which is the shape this binding covers. A body that
	// carries subtrees is bound later, by validateSubtrees' CheckMerkleRoot, and gets the
	// same shape check there — behind its own binding, so both carry the invalid class for
	// the same reason.
	//
	// IsConsensusCoinbase rather than go-bt's Tx.IsCoinbase: the latter is a disjunction that
	// also accepts a 0xFFFFFFFF sequence number in place of a null prevout index, admitting a
	// transaction svnode rejects.
	if len(block.Subtrees) == 0 && !model.IsConsensusCoinbase(block.CoinbaseTx) {
		return errors.NewBlockInvalidError("[quickValidateBlock][%s] coinbase-only body whose only transaction is not a coinbase", block.Hash().String())
	}

	// Compute the below-checkpoint fast-path mode ONCE for this block and thread it through
	// the pipeline (rather than re-deriving it at each seam) so every phase agrees.
	outpointOnly := u.quickValidateOutpointOnly(block)
	if outpointOnly {
		prometheusBlockValidationOutpointOnlyBlocks.Inc()
	}

	var id uint64

	if len(block.Subtrees) > 0 {
		// Prove the peer-supplied body hashes to the checkpoint-certified header
		// BEFORE the pipeline is entered, so no block-id assignment and no UTXO
		// mutation can be driven by a body the header does not commit to
		// (bitcoin-sv/teranode#4838). Reads subtree structures only; the pipeline's
		// per-batch streaming of transaction bodies is untouched. The verdict classes
		// are the ones validateSubtrees has always produced, so the caller routes
		// them exactly as before.
		if err = u.bindSubtreeBodyToHeader(ctx, block); err != nil {
			return err
		}

		// Process all subtrees in streaming fashion - creates UTXOs, spends, writes files
		// This function waits for all processing to complete before returning, ensuring block.ID is set
		_, err = u.processBlockSubtrees(ctx, block, outpointOnly)
		if err != nil {
			// Preserve a corrupt-body verdict from validateSubtrees (bitcoin-sv/teranode#4692) instead
			// of shadowing it with an outer ErrProcessing.
			if errors.IsBlockCorrupt(err) || errors.Is(err, errors.ErrBlockInvalid) {
				return err
			}

			return errors.NewProcessingError("[quickValidateBlock][%s] failed to process block subtrees", block.Hash().String(), err)
		}

		// Verify block ID was assigned during processing (sanity check)
		if block.ID == 0 {
			return errors.NewProcessingError("[quickValidateBlock][%s] block ID was not assigned during subtree processing", block.Hash().String())
		}
	} else {
		// No subtrees to process, assign block ID idempotently
		id, err = u.blockchainClient.AssignBlockID(ctx, block.Hash())
		if err != nil {
			return errors.NewProcessingError("[quickValidateBlock][%s] failed to assign block ID", block.Hash().String(), err)
		}
		block.ID, err = blockIDToUint32(id, block.Hash().String())
		if err != nil {
			return err
		}
	}

	if err := u.checkQuickValidationCoinbase(block, "quickValidateBlock"); err != nil {
		return err
	}

	return u.commitBlock(ctx, block, peerID, "quickValidateBlock")
}

// quickValidateBlockAsync performs optimized validation with async file writes.
// Similar to quickValidateBlock but sends subtree file writes to a background channel,
// allowing the caller to proceed to the next block's UTXO processing while writes continue.
//
// IMPORTANT: The writeJobsChan must be processed by workers in a shared errgroup.
// If any write fails, the errgroup context is cancelled, which cancels this function's context.
//
// Parameters:
//   - ctx: Context for cancellation (cancelled if any writer fails)
//   - block: Block to validate
//   - baseURL: URL of the peer providing the block
//   - writeJobsChan: Channel to send write jobs to background workers
//
// Returns:
//   - *sync.WaitGroup: counts every write job THIS call queued to the shared async subtree
//     writer, non-nil on every return path (bitcoin-sv/teranode#4692) — already Wait()-safe (zero
//     pending) when block.Subtrees is empty or a failure occurred before any job was queued, so
//     the caller never needs a nil check
//   - map[chainhash.Hash]map[fileformat.FileType]struct{}: exactly which (hash, fileType) pairs
//     this call itself freshly wrote, for removeCatchupSubtreeFiles to restrict deletion to
//   - error: If validation fails or context is cancelled
func (u *BlockValidation) quickValidateBlockAsync(ctx context.Context, block *model.Block, peerID, baseURL string, writeJobsChan chan<- *SubtreeWriteJob) (wg *sync.WaitGroup, freshlyWritten map[chainhash.Hash]map[fileformat.FileType]struct{}, err error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "quickValidateBlockAsync",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[quickValidateBlockAsync][%s] performing async quick validation for checkpointed block at height %d", block.Hash().String(), block.Height),
	)
	defer deferFn()

	// The ONE boundary for a subtree blob whose nodes do not hash to its key — see
	// the identical defer in quickValidateBlock (bitcoin-sv/teranode#4838). By the
	// time this runs the processing errgroup has returned, so no reader of a subtree
	// blob is still live.
	defer func() {
		err = u.quarantineSubtreeKeyMismatch(ctx, err)
	}()

	// Already Wait()-safe (zero pending): used on every path that never reaches
	// processBlockSubtreesPipelineAsync, so this function's *sync.WaitGroup return is never nil.
	emptyWG := &sync.WaitGroup{}
	wg = emptyWG

	// Enforce the block-version floor before any body/coinbase inspection (header-first parity
	// with svnode ContextualCheckBlockHeader). Needs only header version, height, and params.
	if err := model.CheckBlockVersion(block.Header.Version, block.Height, u.settings.ChainCfgParams); err != nil {
		return emptyWG, nil, errors.NewBlockInvalidError("[quickValidateBlockAsync][%s] outdated block version", block.Hash().String(), err)
	}

	// Reject blocks without a valid coinbase (e.g. from seeded peers that don't have full block data)
	if block.CoinbaseTx == nil || len(block.CoinbaseTx.Inputs) == 0 {
		return emptyWG, nil, errors.NewBlockIncompleteError("[quickValidateBlockAsync][%s] coinbase tx is nil or has no inputs, peer may not have full block data", block.Hash().String())
	}

	// Bind a no-subtrees body to the header before anything else touches it
	// (bitcoin-sv/teranode#4692). This route never calls block.Valid, so without this the
	// coinbase-only binding that model applies at its own binding block never runs here at all, and
	// a peer-chosen body for an honest header hash — subtree list emptied, any coinbase it likes —
	// reaches commitBlock and is recorded with subtrees_set and mined_set, with nothing to revisit
	// it. A no-op when the body carries subtrees: those are bound by validateSubtrees' CheckMerkleRoot
	// further down. Placed ahead of AssignBlockID so a corrupt body never even takes a block ID.
	if err := block.CheckCoinbaseOnlyBodyBound(); err != nil {
		return emptyWG, nil, err
	}

	// The binding says the header commits to this body; it does not say the single
	// transaction in a no-subtree body is coinbase-shaped. The route's other coinbase
	// checks are len(Inputs) != 0 and the scriptSig length bound, neither of which
	// rejects an ordinary spend standing in for the coinbase. Deliberately placed AFTER
	// the binding: the transaction is then the miner's own committed body, so a
	// non-coinbase one is genuine invalidity — ahead of the binding the same failure
	// would be indistinguishable from a corrupted download (bitcoin-sv/teranode#4692).
	// Scoped to the no-subtree shape, which is the shape this binding covers. A body that
	// carries subtrees is bound later, by validateSubtrees' CheckMerkleRoot, and gets the
	// same shape check there — behind its own binding, so both carry the invalid class for
	// the same reason.
	//
	// IsConsensusCoinbase rather than go-bt's Tx.IsCoinbase: the latter is a disjunction that
	// also accepts a 0xFFFFFFFF sequence number in place of a null prevout index, admitting a
	// transaction svnode rejects.
	if len(block.Subtrees) == 0 && !model.IsConsensusCoinbase(block.CoinbaseTx) {
		return emptyWG, nil, errors.NewBlockInvalidError("[quickValidateBlockAsync][%s] coinbase-only body whose only transaction is not a coinbase", block.Hash().String())
	}

	// Compute the below-checkpoint fast-path mode ONCE for this block and thread it through
	// the pipeline (rather than re-deriving it at each seam) so every phase agrees.
	outpointOnly := u.quickValidateOutpointOnly(block)
	if outpointOnly {
		prometheusBlockValidationOutpointOnlyBlocks.Inc()
	}

	var id uint64

	if len(block.Subtrees) > 0 {
		// Prove the peer-supplied body hashes to the checkpoint-certified header
		// BEFORE the pipeline is entered — see quickValidateBlock
		// (bitcoin-sv/teranode#4838). freshlyWritten is still nil here, which
		// tryQuickValidation handles: it merges it with the fetch phase's own set,
		// and the merge is nil-safe.
		if err = u.bindSubtreeBodyToHeader(ctx, block); err != nil {
			return emptyWG, nil, err
		}

		// Process subtrees with async file writes
		prefetchDepth := u.settings.BlockValidation.SubtreeBatchPrefetchDepth
		if prefetchDepth <= 0 {
			prefetchDepth = 2 // Default for async mode
		}
		_, wg, freshlyWritten, err = u.processBlockSubtreesPipelineAsync(ctx, block, prefetchDepth, writeJobsChan, outpointOnly)
		if err != nil {
			// Preserve a corrupt-body verdict from validateSubtrees (bitcoin-sv/teranode#4692) instead
			// of shadowing it with an outer ErrProcessing, so the caller re-downloads a
			// fresh body rather than treating it as a transient processing error.
			if errors.IsBlockCorrupt(err) || errors.Is(err, errors.ErrBlockInvalid) {
				return wg, freshlyWritten, err
			}

			return wg, freshlyWritten, errors.NewProcessingError("[quickValidateBlockAsync][%s] failed to process block subtrees", block.Hash().String(), err)
		}
	}

	// If no block ID was assigned during processing, assign idempotently
	if block.ID == 0 {
		id, err = u.blockchainClient.AssignBlockID(ctx, block.Hash())
		if err != nil {
			return wg, freshlyWritten, errors.NewProcessingError("[quickValidateBlockAsync][%s] failed to assign block ID", block.Hash().String(), err)
		}
		block.ID, err = blockIDToUint32(id, block.Hash().String())
		if err != nil {
			return wg, freshlyWritten, err
		}
	}

	if err := u.checkQuickValidationCoinbase(block, "quickValidateBlockAsync"); err != nil {
		return wg, freshlyWritten, err
	}

	return wg, freshlyWritten, u.commitBlock(ctx, block, peerID, "quickValidateBlockAsync")
}

// checkQuickValidationCoinbase enforces model.CoinbaseCommonRuleViolation (bitcoin-sv/teranode#4835)
// and model.CoinbaseScriptSigLengthInBounds (bitcoin-sv/teranode#4692) on the quick-validation path,
// which never calls block.Valid and so would otherwise never run either coinbase check at all. They
// run in bitcoin-sv's CheckCoinbase order, transaction rules before bad-cb-length. Called once,
// after subtree processing (if any) has already returned successfully, at which point the body is
// merkle-bound on BOTH shapes:
//   - block.Subtrees non-empty: processBlockSubtrees / processBlockSubtreesPipelineAsync's common
//     tail (validateSubtrees) already ran CheckMerkleRoot successfully.
//   - block.Subtrees empty: model.Block.CheckCoinbaseOnlyBodyBound ran at this route's entry, and
//     for a single-transaction block the header merkle root IS the coinbase txid — the same binding
//     model applies at its own binding block.
//
// So a coinbase rule breach here is genuine consensus invalidity on either shape, condemnable once,
// exactly as Valid classifies both checks through bindErr. There is no unbound shape left on this
// route for a corrupt verdict to apply to.
//
// This is defence-in-depth, not a commonly-reachable path: quick validation only runs for blocks at
// or below the highest hash-verified checkpoint for this catchup run (catchup.go,
// tryQuickValidation).
func (u *BlockValidation) checkQuickValidationCoinbase(block *model.Block, caller string) error {
	if reason := model.CoinbaseCommonRuleViolation(block.CoinbaseTx, block.Height, u.settings.ChainCfgParams); reason != "" {
		return errors.NewBlockInvalidError("[%s][%s] coinbase breaks a transaction rule: %s", caller, block.Hash().String(), reason)
	}

	if !model.CoinbaseScriptSigLengthInBounds(block.CoinbaseTx, u.settings.ChainCfgParams) {
		return errors.NewBlockInvalidError("[%s][%s] bad coinbase length", caller, block.Hash().String())
	}

	return nil
}

// commitBlock performs the shared final commit for the quick-validation path:
// add the block to the blockchain (subtrees + mined already set), unlock any
// locked UTXOs, and mark the block present in cache. It sends no
// BlockSubtreesSet notification, because the insert already wrote subtrees_set. Extracted verbatim from quickValidateBlock /
// quickValidateBlockAsync so both share one commit tail; it is also the per-block
// commit unit the Step-8 parallel window's ordered committer calls in height order.
// caller labels logs to preserve each call site's existing text.
func (u *BlockValidation) commitBlock(ctx context.Context, block *model.Block, peerID, caller string) error {
	// add block directly to blockchain
	if err := u.blockchainClient.AddBlock(ctx,
		block,
		peerID,
		options.WithSubtreesSet(true),
		options.WithMinedSet(true),
		options.WithID(uint64(block.ID)),
	); err != nil {
		return errors.NewProcessingError("[%s][%s] failed to add block to blockchain", caller, block.Hash().String(), err)
	}

	// Unlock all UTXOs - final commit point (no-op when the lock was never taken; #1103).
	if err := u.unlockSubtreeTransactionsIfNeeded(ctx, block, caller); err != nil {
		return err
	}

	// No updateSubtreesDAH here. The AddBlock above writes subtrees_set and mined_set in the
	// insert itself, so a SetBlockSubtreesSet call would re-UPDATE a row that is already true,
	// commit on its own, clear the blockchain response cache and notify every subscriber. The
	// one subscriber that acts on BlockSubtreesSet, the setMined worker, skips a block whose
	// mined_set is already true. The full-validation paths insert with subtrees_set false and
	// still make the call.

	// Mark block as existing in cache.
	if err := u.SetBlockExists(block.Hash()); err != nil {
		u.logger.Errorf("[%s][%s] failed to set block exists cache: %s", caller, block.Hash().String(), err)
	}

	return nil
}

// subtreeResult holds the result of reading a subtree, sent through a channel
type subtreeResult struct {
	subtree     *subtreepkg.Subtree
	subtreeData *subtreepkg.Data
	subtreeHash chainhash.Hash
	subtreeIdx  int
	// fullSubtree is the already-present full .subtree blob, loaded and anchored
	// during this read rather than re-read at the point it is consumed. Nil when no
	// such blob exists, which is the first-attempt case.
	fullSubtree *subtreepkg.Subtree
	err         error
}

// processBlockSubtrees processes subtrees in batches to balance RAM usage and parallelism.
// For each batch of subtrees it: reads, extends transactions, creates UTXOs, spends, writes files.
// Transaction hashes can be extracted from block.SubtreeSlices after this call.
//
// Routes to either sequential or pipeline processing based on SubtreeBatchPrefetchDepth setting.
//
// Returns:
//   - uint64: Existing BlockID if retry detected, 0 otherwise
//   - error: If processing fails
func (u *BlockValidation) processBlockSubtrees(ctx context.Context, block *model.Block, outpointOnly bool) (uint64, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "processBlockSubtrees",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[processBlockSubtrees][%s] processing %d subtrees in batches of %d", block.Hash().String(), len(block.Subtrees), u.settings.BlockValidation.SubtreeBatchSize),
	)
	defer deferFn()

	if len(block.Subtrees) == 0 {
		return 0, errors.NewProcessingError("[processBlockSubtrees][%s] block has no subtrees", block.Hash().String())
	}

	prefetchDepth := u.settings.BlockValidation.SubtreeBatchPrefetchDepth
	if prefetchDepth <= 0 {
		return u.processBlockSubtreesSequential(ctx, block, outpointOnly)
	}
	return u.processBlockSubtreesPipeline(ctx, block, prefetchDepth, outpointOnly)
}

// processBlockSubtreesSequential processes subtrees sequentially, one batch at a time.
// This is the fallback when SubtreeBatchPrefetchDepth is 0.
func (u *BlockValidation) processBlockSubtreesSequential(ctx context.Context, block *model.Block, outpointOnly bool) (uint64, error) {
	numSubtrees := len(block.Subtrees)
	block.SubtreeSlices = make([]*subtreepkg.Subtree, numSubtrees)
	var existingBlockID uint64

	// Get block ID first (check for retry using first tx after reading first batch)
	blockIDSet := false

	// Track extended transactions across batches for same-block parent resolution
	extendedTxs := make(map[chainhash.Hash]*bt.Tx)

	// Process subtrees in batches
	subtreeBatchSize := u.settings.BlockValidation.SubtreeBatchSize
	for batchStart := 0; batchStart < numSubtrees; batchStart += subtreeBatchSize {
		batchEnd := batchStart + subtreeBatchSize
		if batchEnd > numSubtrees {
			batchEnd = numSubtrees
		}

		// Phase 1-3: Read subtrees and extend transactions (shared with normal validation)
		batch, err := u.processSubtreeBatch(ctx, block, batchStart, batchEnd, extendedTxs, outpointOnly)
		if err != nil {
			return 0, err
		}

		// Phase 4: Check for retry and get block ID (only on first batch)
		// This is specific to quick validation to handle retries gracefully.
		// batchTxs[0] is the first non-coinbase tx; its recorded mined-in id is
		// this block's. The legacy quick path (netsync reuseBlockIDFromUTXO) keys
		// the same recovery on its first non-coinbase tx — keep them in sync.
		if !blockIDSet && len(batch.batchTxs) > 0 {
			existingMeta, err := u.utxoStore.Get(ctx, batch.batchTxs[0].TxIDChainHash(), fields.BlockIDs)
			if err == nil && existingMeta != nil && len(existingMeta.BlockIDs) > 0 {
				existingBlockID = uint64(existingMeta.BlockIDs[0])
				block.ID = existingMeta.BlockIDs[0]
				u.logger.Debugf("[processBlockSubtreesSequential][%s] reusing BlockID %d from retry", block.Hash().String(), existingBlockID)
			} else if block.ID == 0 {
				id, err := u.blockchainClient.AssignBlockID(ctx, block.Hash())
				if err != nil {
					return 0, errors.NewProcessingError("[processBlockSubtreesSequential][%s] failed to assign block ID", block.Hash().String(), err)
				}
				block.ID, err = blockIDToUint32(id, block.Hash().String())
				if err != nil {
					return 0, err
				}
			}
			blockIDSet = true
		}

		// Phase 5-6: Create and spend UTXOs (quick validation specific - bypasses service validation)
		if err := u.createAndSpendUTXOsForBatch(ctx, block, batch); err != nil {
			return 0, err
		}

		// Phase 7: Write subtree files (shared with normal validation)
		if err := u.writeSubtreeFilesForBatch(ctx, block, batch); err != nil {
			batch.Close()
			return 0, err
		}

		// Release mmap resources for completed batch
		batch.Close()
	}

	return u.validateSubtrees(ctx, block, existingBlockID)
}

// processBlockSubtreesPipeline processes subtrees using a fan-in pipeline that overlaps I/O with processing.
//
// Three pipeline stages:
//  1. Reader: Prefetch batches from disk (I/O bound)
//  2. Extender: Extend transactions (CPU/network bound, sequential for extendedTxs map)
//  3. Processor: UTXO create+spend AND write files in parallel per batch
//
// Error Handling:
// If any stage encounters an error, the errgroup context is cancelled, stopping all stages.
// Partial UTXO state changes are safe because:
//   - By default, UTXOs are created with WithLocked(true), preventing other operations from using them.
//     If processing fails, unlockSubtreeTransactions is never called, so the locked UTXOs remain locked
//     and the partial changes are effectively rolled back.
//   - When QuickValidateSkipUtxoLock is enabled (blocks at or below the highest checkpoint), UTXOs are
//     created unlocked, so this lock-based rollback barrier does not apply; recovery instead relies on
//     retry convergence below.
//   - On retry, Create() returns ErrTxExists, and SetMinedMulti() updates the correct BlockID.
func (u *BlockValidation) processBlockSubtreesPipeline(ctx context.Context, block *model.Block, prefetchDepth int, outpointOnly bool) (uint64, error) {
	numSubtrees := len(block.Subtrees)
	block.SubtreeSlices = make([]*subtreepkg.Subtree, numSubtrees)
	var existingBlockID uint64
	blockIDSet := false

	// Channel for prefetched batches (subtrees read, txs not extended)
	prefetchChan := make(chan *SubtreeProcessingBatch, prefetchDepth)

	// Channel for extended batches ready for UTXO ops
	extendedChan := make(chan *SubtreeProcessingBatch, prefetchDepth)

	g, gCtx := errgroup.WithContext(ctx)

	// Ensure channels are drained on error to prevent goroutine leaks and memory leaks
	// from large SubtreeProcessingBatch structs stuck in channel buffers
	defer func() {
		for range prefetchChan {
			// Drain prefetchChan if it's still open
		}
		for range extendedChan {
			// Drain extendedChan if it's still open
		}
	}()

	// Stage 1: Reader - prefetch batches from disk
	g.Go(func() error {
		defer close(prefetchChan)
		subtreeBatchSize := u.settings.BlockValidation.SubtreeBatchSize
		for batchStart := 0; batchStart < numSubtrees; batchStart += subtreeBatchSize {
			batchEnd := batchStart + subtreeBatchSize
			if batchEnd > numSubtrees {
				batchEnd = numSubtrees
			}

			start := time.Now()
			batch, err := u.prefetchSubtreeBatch(gCtx, block, batchStart, batchEnd, outpointOnly)
			if err != nil {
				return err
			}
			u.logger.Infof("[pipeline:prefetch][%s] batch %d-%d prefetched in %v", block.Hash().String(), batchStart, batchEnd, time.Since(start))

			select {
			case prefetchChan <- batch:
			case <-gCtx.Done():
				return gCtx.Err()
			}
		}
		return nil
	})

	// Stage 2: Extender - extend transactions (sequential for extendedTxs map)
	g.Go(func() error {
		defer close(extendedChan)
		extendedTxs := make(map[chainhash.Hash]*bt.Tx)
		for batch := range prefetchChan {
			start := time.Now()
			if err := u.extendBatch(gCtx, block, batch, extendedTxs); err != nil {
				return err
			}
			u.logger.Infof("[pipeline:extend][%s] batch %d-%d extended (%d txs) in %v", block.Hash().String(), batch.batchStart, batch.batchEnd, len(batch.batchTxs), time.Since(start))

			select {
			case extendedChan <- batch:
			case <-gCtx.Done():
				return gCtx.Err()
			}
		}
		return nil
	})

	// Stage 3: Processor - UTXO create+spend AND write files in parallel (per batch)
	g.Go(func() error {
		for batch := range extendedChan {
			// Block ID check (first batch only)
			if !blockIDSet && len(batch.batchTxs) > 0 {
				existingMeta, err := u.utxoStore.Get(gCtx, batch.batchTxs[0].TxIDChainHash(), fields.BlockIDs)
				if err == nil && existingMeta != nil && len(existingMeta.BlockIDs) > 0 {
					existingBlockID = uint64(existingMeta.BlockIDs[0])
					block.ID = existingMeta.BlockIDs[0]
					u.logger.Debugf("[processBlockSubtreesPipeline][%s] reusing BlockID %d from retry", block.Hash().String(), existingBlockID)
				} else if block.ID == 0 {
					id, err := u.blockchainClient.AssignBlockID(gCtx, block.Hash())
					if err != nil {
						return errors.NewProcessingError("[processBlockSubtreesPipeline][%s] failed to assign block ID", block.Hash().String(), err)
					}
					block.ID, err = blockIDToUint32(id, block.Hash().String())
					if err != nil {
						return err
					}
				}
				blockIDSet = true
			}

			// Run UTXO ops and file writes in parallel for this batch
			start := time.Now()
			var utxoDuration, writeDuration time.Duration
			batchG, batchCtx := errgroup.WithContext(gCtx)
			batchG.Go(func() error {
				utxoStart := time.Now()
				err := u.createAndSpendUTXOsForBatch(batchCtx, block, batch)
				utxoDuration = time.Since(utxoStart)
				return err
			})
			batchG.Go(func() error {
				writeStart := time.Now()
				err := u.writeSubtreeFilesForBatch(batchCtx, block, batch)
				writeDuration = time.Since(writeStart)
				return err
			})
			if err := batchG.Wait(); err != nil {
				return err
			}
			u.logger.Infof("[pipeline:process][%s] batch %d-%d processed in %v (utxo=%v, write=%v)", block.Hash().String(), batch.batchStart, batch.batchEnd, time.Since(start), utxoDuration, writeDuration)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return 0, err
	}

	return u.validateSubtrees(ctx, block, existingBlockID)
}

// processBlockSubtreesPipelineAsync processes subtrees with async file writes across blocks.
// Similar to processBlockSubtreesPipeline but sends write jobs to a shared channel instead
// of blocking on file writes. This allows file I/O from Block N to overlap with UTXO
// processing for Block N+1.
//
// The writeJobsChan is shared across multiple blocks. If any write worker fails, the context
// is cancelled, which stops this function immediately.
//
// Parameters:
//   - ctx: Context for cancellation (cancelled if any writer fails)
//   - block: The block to process
//   - prefetchDepth: Number of batches to prefetch ahead
//   - writeJobsChan: Channel to send write jobs to background workers
//
// Returns:
//   - uint64: Existing BlockID if retry detected, 0 otherwise
//   - *sync.WaitGroup: counts every write job THIS call queued to writeJobsChan; the caller must
//     wait on it (context-aware, never bare — see tryQuickValidation) before trusting that a
//     failed attempt's writes have all settled, since the shared write-worker pool outlives this
//     one call (bitcoin-sv/teranode#4692)
//   - map[chainhash.Hash]map[fileformat.FileType]struct{}: exactly which (hash, fileType) pairs
//     this call itself freshly wrote, for removeCatchupSubtreeFiles to restrict deletion to
//   - error: If processing fails or context is cancelled
func (u *BlockValidation) processBlockSubtreesPipelineAsync(ctx context.Context, block *model.Block, prefetchDepth int, writeJobsChan chan<- *SubtreeWriteJob, outpointOnly bool) (uint64, *sync.WaitGroup, map[chainhash.Hash]map[fileformat.FileType]struct{}, error) {
	numSubtrees := len(block.Subtrees)
	block.SubtreeSlices = make([]*subtreepkg.Subtree, numSubtrees)
	var existingBlockID uint64
	blockIDSet := false

	// Scoped to this one block's call (bitcoin-sv/teranode#4692): wg tracks every write job queued
	// below, and freshness tracks which (hash, fileType) pairs were freshly written. Neither is
	// reused across blocks or across attempts.
	wg := &sync.WaitGroup{}
	freshness := newSubtreeFreshness()

	// Channel for prefetched batches (subtrees read, txs not extended)
	prefetchChan := make(chan *SubtreeProcessingBatch, prefetchDepth)

	// Channel for extended batches ready for UTXO ops
	extendedChan := make(chan *SubtreeProcessingBatch, prefetchDepth)

	g, gCtx := errgroup.WithContext(ctx)

	// Stage 1: Reader - prefetch batches from disk
	g.Go(func() error {
		defer close(prefetchChan)
		subtreeBatchSize := u.settings.BlockValidation.SubtreeBatchSize
		for batchStart := 0; batchStart < numSubtrees; batchStart += subtreeBatchSize {
			batchEnd := batchStart + subtreeBatchSize
			if batchEnd > numSubtrees {
				batchEnd = numSubtrees
			}

			start := time.Now()
			batch, err := u.prefetchSubtreeBatch(gCtx, block, batchStart, batchEnd, outpointOnly)
			if err != nil {
				return err
			}
			u.logger.Debugf("[pipeline:prefetch:async][%s] batch %d-%d prefetched in %v", block.Hash().String(), batchStart, batchEnd, time.Since(start))

			select {
			case prefetchChan <- batch:
			case <-gCtx.Done():
				return gCtx.Err()
			}
		}
		return nil
	})

	// Stage 2: Extender - extend transactions (sequential for extendedTxs map)
	g.Go(func() error {
		defer close(extendedChan)
		extendedTxs := make(map[chainhash.Hash]*bt.Tx)
		for batch := range prefetchChan {
			start := time.Now()
			if err := u.extendBatch(gCtx, block, batch, extendedTxs); err != nil {
				return err
			}
			u.logger.Debugf("[pipeline:extend:async][%s] batch %d-%d extended (%d txs) in %v", block.Hash().String(), batch.batchStart, batch.batchEnd, len(batch.batchTxs), time.Since(start))

			select {
			case extendedChan <- batch:
			case <-gCtx.Done():
				return gCtx.Err()
			}
		}
		return nil
	})

	// Stage 3: Processor - UTXO create+spend, then queue write jobs (per batch)
	// Unlike the sync version, we don't wait for writes - just queue them
	g.Go(func() error {
		for batch := range extendedChan {
			// Block ID check (first batch only)
			if !blockIDSet && len(batch.batchTxs) > 0 {
				existingMeta, err := u.utxoStore.Get(gCtx, batch.batchTxs[0].TxIDChainHash(), fields.BlockIDs)
				if err == nil && existingMeta != nil && len(existingMeta.BlockIDs) > 0 {
					existingBlockID = uint64(existingMeta.BlockIDs[0])
					block.ID = existingMeta.BlockIDs[0]
					u.logger.Debugf("[processBlockSubtreesPipelineAsync][%s] reusing BlockID %d from retry", block.Hash().String(), existingBlockID)
				} else if block.ID == 0 {
					id, err := u.blockchainClient.AssignBlockID(gCtx, block.Hash())
					if err != nil {
						return errors.NewProcessingError("[processBlockSubtreesPipelineAsync][%s] failed to assign block ID", block.Hash().String(), err)
					}
					block.ID, err = blockIDToUint32(id, block.Hash().String())
					if err != nil {
						return err
					}
				}
				blockIDSet = true
			}

			// Run UTXO ops and subtree building in parallel
			// Subtree building sets block.SubtreeSlices and queues write jobs
			start := time.Now()
			var utxoDuration, buildDuration time.Duration
			batchG, batchCtx := errgroup.WithContext(gCtx)
			batchG.Go(func() error {
				utxoStart := time.Now()
				err := u.createAndSpendUTXOsForBatch(batchCtx, block, batch)
				utxoDuration = time.Since(utxoStart)
				return err
			})
			batchG.Go(func() error {
				buildStart := time.Now()
				// Build subtrees and queue write jobs (doesn't wait for I/O)
				err := u.buildSubtreeJobsForBatch(batchCtx, block, batch, writeJobsChan, wg, freshness)
				buildDuration = time.Since(buildStart)
				return err
			})
			if err := batchG.Wait(); err != nil {
				batch.Close()
				return err
			}
			u.logger.Infof("[pipeline:process:async][%s] batch %d-%d processed in %v (utxo=%v, build+queue=%v)", block.Hash().String(), batch.batchStart, batch.batchEnd, time.Since(start), utxoDuration, buildDuration)

			// Release mmap resources for completed batch
			batch.Close()
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return 0, wg, freshness.snapshot(), err
	}

	resultID, err := u.validateSubtrees(ctx, block, existingBlockID)

	return resultID, wg, freshness.snapshot(), err
}

// validateSubtrees validates subtree sizes and merkle root after processing.
func (u *BlockValidation) validateSubtrees(ctx context.Context, block *model.Block, existingBlockID uint64) (uint64, error) {
	if err := checkSubtreeBodyBinding(ctx, block, "validateSubtrees"); err != nil {
		return 0, err
	}

	return existingBlockID, nil
}

// checkSubtreeBodyBinding runs every check that relates a block's subtree slices to
// its header, in the order they have always run: subtree-size uniformity, the merkle
// root, the coinbase placeholder at slot [0][0], the duplicate-transaction scan, and
// the coinbase shape.
//
// Extracted so the same sequence can be run twice on different objects: once on a
// probe carrying the structures as served, before anything durable happens, and once
// at the tail on the rebuilt slices. Nothing here reads transaction bodies — the
// checks compose node hashes and the coinbase — so it can run on a block whose
// subtree_data has never been opened.
//
// caller names the pass, and the two are not interchangeable to whoever reads the
// failure. The same message from the pre-bind pass means the body a peer served never
// bound and nothing was mutated; from the tail it means the REBUILT slices disagree
// with the header after this block's transactions were already created and spent. A
// single literal label made the two indistinguishable in a log.
func checkSubtreeBodyBinding(ctx context.Context, block *model.Block, caller string) error {
	// Validate subtree sizes
	subtreeSize := 0
	for i := 0; i < len(block.SubtreeSlices)-1; i++ {
		if i == 0 {
			subtreeSize = block.SubtreeSlices[i].Length()
		} else if block.SubtreeSlices[i].Length() != subtreeSize {
			// Body-derived subtree-shape check on the quick path: return corrupt DIRECTLY
			// (not wrapped in ErrProcessing, which would shadow it at ValidateBlock and
			// route it as a transient processing error). The caller re-downloads a fresh
			// body instead of poisoning the hash (bitcoin-sv/teranode#4692).
			return errors.NewBlockCorruptError("[%s][%s] subtree %d size mismatch", caller, block.Hash().String(), i)
		}
	}

	// Verify merkle root
	if err := block.CheckMerkleRoot(ctx); err != nil {
		// CheckMerkleRoot already classifies its failures — corrupt for the tier-2
		// merkle/subtree-shape checks, processing/storage for infrastructure. Return a
		// corrupt verdict UNWRAPPED so it is not shadowed by an outer ErrProcessing
		// (bitcoin-sv/teranode#4692); a shadowed corrupt would be mis-routed as transient. Wrap the
		// non-corrupt (infrastructure) errors with the [caller][hash] site context, as
		// the sibling subtree-size check above does — the wrap keeps the infrastructure
		// classification (ErrProcessing/ErrStorage) in the cause chain.
		if errors.IsBlockCorrupt(err) {
			return err
		}

		return errors.NewProcessingError("[%s][%s] merkle root check failed", caller, block.Hash().String(), err)
	}

	// CVE-2012-2459. The merkle root CANNOT detect a duplicated trailing transaction:
	// the duplicate-last-node-when-odd rule makes the mutated body produce the SAME root,
	// and so the same block hash, as the honest block. A body that binds to a
	// checkpoint-certified header can therefore still carry a repeated transaction,
	// reusing the honest block's proof of work at no mining cost. Block.Valid runs the
	// pooled/disk-backed equivalent at its step 11; this route holds the slices in memory
	// and must run the slice-only scan, which is what model.CheckSubtreeSlicesForDuplicateTxs
	// documents itself as being for.
	//
	// Corrupt, not invalid: a duplicated transaction is a defect in the body a peer served,
	// and the honest hash it binds to must not be condemned (bitcoin-sv/teranode#4692).
	// The dedup scan below skips slot [0][0] only when it holds the coinbase placeholder,
	// so "first node is the placeholder" is its unstated precondition, and Valid enforces it
	// as its own step 7. Without it a first subtree whose node 0 is a real txid — reachable on
	// a retry that reuses a locally present .subtree blob — passes the merkle check with the
	// body's true first transaction silently substituted, and the scan would then treat that
	// txid as an ordinary node.
	if len(block.SubtreeSlices) > 0 {
		first := block.SubtreeSlices[0]
		if first == nil {
			return errors.NewProcessingError("[%s][%s] first subtree was released during validation", caller, block.Hash().String())
		}

		if len(first.Nodes) == 0 {
			return errors.NewBlockCorruptError("[%s][%s] first subtree has no nodes", caller, block.Hash().String())
		}

		if !first.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholder) {
			return errors.NewBlockCorruptError("[%s][%s] first transaction in first subtree is not a coinbase placeholder: %s", caller, block.Hash().String(), first.Nodes[0].Hash.String())
		}
	}

	if err := model.CheckSubtreeSlicesForDuplicateTxs(block.SubtreeSlices); err != nil {
		return err
	}

	// The coinbase shape, for the subtree-carrying body. CheckMerkleRoot above substitutes
	// block.CoinbaseTx's txid for the placeholder before hashing, so by here the header
	// commits to this exact transaction — which is why an unshaped coinbase is genuine
	// invalidity rather than a corrupt download. The no-subtree shape gets the same check at
	// this route's entry points, behind its own binding.
	//
	// The shape check in getBlockTransactions does NOT cover this: it inspects
	// subtreeData.Txs[0], a different object from block.CoinbaseTx.
	if !model.IsConsensusCoinbase(block.CoinbaseTx) {
		return errors.NewBlockInvalidError("[%s][%s] block coinbase tx is not a valid coinbase tx", caller, block.Hash().String())
	}

	return nil
}

// subtreeStructure is what one read of a subtree's node list yields: the structure
// itself and — when the promoted full blob is also present — that blob, loaded and
// anchored in the same call rather than re-read where it is consumed.
//
// The file type findLocalSubtreeFile resolved is not carried on the struct but
// recorded on the mismatch error, because that is the only consumer and re-resolving
// it after a failure can select a different sibling than the one actually read.
type subtreeStructure struct {
	subtree     *subtreepkg.Subtree
	fullSubtree *subtreepkg.Subtree
}

// readSubtreeStructure reads and deserializes a single subtree's node list from the
// local blob store.
//
// It is the one place on the quick-validation route that turns a subtree hash into a
// node list — the whole-block pre-bind pass and all three per-batch readers go
// through it — so anything that must hold for every read of a subtree on this route
// belongs here rather than being restated at each caller.
func (u *BlockValidation) readSubtreeStructure(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash) (subtreeStructure, error) {
	// On retry the subtree may already be promoted to FileTypeSubtree (the
	// "already validated" marker) and FileTypeSubtreeToCheck cleaned up, so
	// consult both file types — see findLocalSubtreeFile.
	localFileType, localExists, err := findLocalSubtreeFile(ctx, u.subtreeStore, *subtreeHash)
	if err != nil {
		return subtreeStructure{}, errors.NewStorageError("[getBlockTransactions][%s] failed to locate subtree %s", block.Hash().String(), subtreeHash.String(), err)
	}
	if !localExists {
		return subtreeStructure{}, errors.NewNotFoundError("[getBlockTransactions][%s] subtree %s not found locally", block.Hash().String(), subtreeHash.String())
	}
	subtreeReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash[:], localFileType)
	if err != nil {
		return subtreeStructure{}, errors.NewNotFoundError("[getBlockTransactions][%s] failed to get subtree %s", block.Hash().String(), subtreeHash.String(), err)
	}
	defer func() {
		if subtreeReader != nil {
			subtreeReader.Close()
		}
	}()

	// Use pooled buffered reader to reduce GC pressure
	bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
	bufferedReader.Reset(subtreeReader)
	defer func() {
		bufferedReader.Reset(nil)
		bufioReaderPool.Put(bufferedReader)
	}()

	// subtree only contains the tx hashes (nodes) of the subtree
	var subtree *subtreepkg.Subtree
	if u.mmapDir != "" {
		subtree, err = subtreepkg.NewSubtreeFromReaderMmap(bufferedReader, u.mmapDir)
		if err != nil {
			// Fallback to heap on mmap failure. The mmap attempt has already consumed
			// bytes from subtreeReader, which is not seekable, so resetting the buffered
			// reader onto it would read from mid-stream and produce a corrupt subtree.
			// Open a fresh reader from the store so the heap path reads from the start.
			u.logger.Warnf("[getBlockTransactions][%s] mmap deserialization failed for subtree %s, falling back to heap: %v", block.Hash().String(), subtreeHash.String(), err)

			// The mmap attempt has consumed subtreeReader and it is no longer used; close it
			// now rather than leaving it open alongside the fallback reader until return.
			_ = subtreeReader.Close()
			subtreeReader = nil

			fallbackReader, ferr := u.subtreeStore.GetIoReader(ctx, subtreeHash[:], localFileType)
			if ferr != nil {
				return subtreeStructure{}, errors.NewNotFoundError("[getBlockTransactions][%s] failed to re-open subtree %s for heap fallback", block.Hash().String(), subtreeHash.String(), ferr)
			}
			defer fallbackReader.Close()

			bufferedReader.Reset(fallbackReader)
			subtree, err = subtreepkg.NewSubtreeFromReader(bufferedReader)
		}
	} else {
		subtree, err = subtreepkg.NewSubtreeFromReader(bufferedReader)
	}
	if err != nil {
		return subtreeStructure{}, errors.NewProcessingError("[getBlockTransactions][%s] failed to deserialize subtree %s", block.Hash().String(), subtreeHash.String(), err)
	}

	// A zero-node subtree cannot be honest, and this route has no other check for it
	// (bitcoin-sv/teranode#4692). CheckBlockSubtrees never runs on the quick path, so neither
	// validateSubtreeLeafCount (the fetch route) nor loadSubtreeBatch's own zero-length guard (the
	// local-read route) — both in services/subtreevalidation/check_block_subtrees.go — covers this
	// third local read. The downstream constructor does reject a zero leaf count today, but only
	// incidentally, as a consequence of how it derives a tree height; stating the rule here keeps
	// the guarantee local and stable if that constructor ever changes. Placed before the
	// subtree-data read so a junk blob costs one deserialisation, not two.
	if subtree.Length() == 0 {
		releaseSubtreeStructure(subtree)
		return subtreeStructure{}, errors.NewProcessingError("[getBlockTransactions][%s] subtree %s has zero nodes", block.Hash().String(), subtreeHash.String())
	}

	// Anchor the node list to the key by RECOMPUTING its root, not by reading the
	// root the file claims. Deserialization copies the .subtree header's root
	// straight into the cache RootHash() returns, so comparing that to the key
	// compares one peer-supplied value against another and says nothing about Nodes
	// — and Nodes is what drives every create and spend below. Block.CheckMerkleRoot
	// composes the same cached value for every subtree after the first, so proving
	// claim == recomputed == key here is what gives that composition a premise
	// (bitcoin-sv/teranode#4838).
	if err := model.ValidateSubtreeNodesMatchKey(subtree, subtreeHash); err != nil {
		releaseSubtreeStructure(subtree)

		return subtreeStructure{}, u.rejectKeyMismatchAndAuditSibling(ctx, block, subtreeHash, localFileType, err)
	}

	structure := subtreeStructure{subtree: subtree}

	// The promoted FileTypeSubtree blob, when one exists, is loaded and anchored
	// HERE rather than where it is consumed. Its consumer runs in the arm beside
	// createAndSpendUTXOsForBatch in both pipelined variants and after it in the
	// sequential one, so a check placed there could only ever fire once the
	// mutations had begun. Loaded as its own object rather than aliasing the
	// structure above, since the two have different owners and different lifetimes.
	fullSubtreeExists := localFileType == fileformat.FileTypeSubtree
	if !fullSubtreeExists {
		// Fail CLOSED on a probe failure rather than proceeding as though the blob were
		// absent: treating "cannot tell" as "not there" would skip the anchor for a full
		// blob that does exist, and the invariant this route relies on is that every
		// already-present full blob was anchored (bitcoin-sv/teranode#4838). Classified
		// as storage, the same class findLocalSubtreeFile gives its own probe failures.
		var existsErr error

		fullSubtreeExists, existsErr = u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		if existsErr != nil {
			releaseSubtreeStructure(subtree)

			return subtreeStructure{}, errors.NewStorageError("[getBlockTransactions][%s] failed to probe for full subtree %s", block.Hash().String(), subtreeHash.String(), existsErr)
		}
	}

	if fullSubtreeExists {
		fullSubtree, err := u.readFullSubtreeAnchored(ctx, block, subtreeHash)
		if err != nil {
			releaseSubtreeStructure(subtree)

			return subtreeStructure{}, err
		}

		structure.fullSubtree = fullSubtree
	}

	return structure, nil
}

// subtreeSiblingFileType returns the other file type a subtree hash can be stored
// under. findLocalSubtreeFile consults exactly these two.
func subtreeSiblingFileType(fileType fileformat.FileType) fileformat.FileType {
	if fileType == fileformat.FileTypeSubtree {
		return fileformat.FileTypeSubtreeToCheck
	}

	return fileformat.FileTypeSubtree
}

// rejectKeyMismatchAndAuditSibling builds the mismatch error for a blob whose nodes do
// not hash to its key, and — crucially — audits the OTHER file type stored under the
// same hash before returning.
//
// Without that audit the hole the quarantine exists to close reopens by a different
// route (bitcoin-sv/teranode#4838). findLocalSubtreeFile prefers
// FileTypeSubtreeToCheck, so when both blobs exist and the preferred one is forged,
// naming only the blob that was read means the quarantine deletes only that one. The
// attempt is then an ordinary local fault, normal validation runs, its own
// findLocalSubtreeFile selects the SURVIVING sibling, and its loader checks only the
// .subtree header's claimed root — the one check a forged header defeats.
//
// So: a sibling that is also forged is named on the same error and quarantined with
// it; a sibling that anchors cleanly is left alone, because it has been proved to
// belong to this key and normal validation may safely use it; and a sibling that
// EXISTS but cannot be audited marks the attempt unquarantined, so tryQuickValidation
// aborts rather than falling through to a loader that cannot see the forgery.
func (u *BlockValidation) rejectKeyMismatchAndAuditSibling(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash, readFileType fileformat.FileType, anchorErr error) error {
	err := errors.NewProcessingError("[getBlockTransactions][%s] subtree %s does not match its key", block.Hash().String(), subtreeHash.String(), anchorErr)
	refs := []subtreeBlobRef{{hash: *subtreeHash, fileType: readFileType}}

	sibling := subtreeSiblingFileType(readFileType)

	exists, existsErr := u.subtreeStore.Exists(ctx, subtreeHash[:], sibling)
	if existsErr != nil {
		u.logger.Errorf("[getBlockTransactions][%s] subtree %s: cannot probe sibling %s of a mismatching blob, aborting: %v", block.Hash().String(), subtreeHash.String(), sibling, existsErr)

		return markUnquarantinedLocalSubtree(markSubtreeKeyMismatch(err, refs...))
	}

	if !exists {
		return markSubtreeKeyMismatch(err, refs...)
	}

	siblingBytes, getErr := u.subtreeStore.Get(ctx, subtreeHash[:], sibling)
	if getErr != nil {
		u.logger.Errorf("[getBlockTransactions][%s] subtree %s: cannot read sibling %s of a mismatching blob, aborting: %v", block.Hash().String(), subtreeHash.String(), sibling, getErr)

		return markUnquarantinedLocalSubtree(markSubtreeKeyMismatch(err, refs...))
	}

	siblingSubtree, deserErr := u.newSubtreeFromBytes(siblingBytes)
	if deserErr != nil {
		// Bytes under this key that will not deserialize cannot be handed on either.
		// Named for the quarantine rather than left behind: normal validation would
		// only fail on them, and a re-fetch replaces them.
		return markSubtreeKeyMismatch(err, append(refs, subtreeBlobRef{hash: *subtreeHash, fileType: sibling})...)
	}

	siblingAnchorErr := model.ValidateSubtreeNodesMatchKey(siblingSubtree, subtreeHash)

	releaseSubtreeStructure(siblingSubtree)

	if siblingAnchorErr != nil {
		refs = append(refs, subtreeBlobRef{hash: *subtreeHash, fileType: sibling})
	}

	return markSubtreeKeyMismatch(err, refs...)
}

// readFullSubtreeAnchored loads the promoted FileTypeSubtree blob for subtreeHash
// and anchors it to that key by recomputation, exactly as the structure read does.
//
// The returned subtree is the object the batch carries and the block's slice
// eventually becomes, so its root is memoized on this goroutine before it is
// published — the same reason buildSubtreeAndQueueWrite memoizes the tree it builds.
func (u *BlockValidation) readFullSubtreeAnchored(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash) (*subtreepkg.Subtree, error) {
	fullSubtreeBytes, err := u.subtreeStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		return nil, errors.NewNotFoundError("[getBlockTransactions][%s] failed to get existing full subtree %s", block.Hash().String(), subtreeHash.String(), err)
	}

	fullSubtree, err := u.newSubtreeFromBytes(fullSubtreeBytes)
	if err != nil {
		return nil, errors.NewProcessingError("[getBlockTransactions][%s] failed to deserialize full subtree %s", block.Hash().String(), subtreeHash.String(), err)
	}

	if err := model.ValidateSubtreeNodesMatchKey(fullSubtree, subtreeHash); err != nil {
		releaseSubtreeStructure(fullSubtree)

		return nil, markSubtreeKeyMismatch(
			errors.NewProcessingError("[getBlockTransactions][%s] full subtree %s does not match its key", block.Hash().String(), subtreeHash.String(), err),
			subtreeBlobRef{hash: *subtreeHash, fileType: fileformat.FileTypeSubtree},
		)
	}

	_ = fullSubtree.RootHash()

	return fullSubtree, nil
}

// releaseSubtreeStructure drops a subtree the reader owns and is not going to
// return, unmapping it when it is mmap-backed. Nodes are detached before the
// close, the order model.Block's own release uses: Close leaves Nodes pointing at
// the region it has just unmapped.
func releaseSubtreeStructure(subtree *subtreepkg.Subtree) {
	if subtree == nil {
		return
	}

	if subtree.IsMmapBacked() {
		_ = subtree.ReleaseNodes()
	}

	_ = subtree.Close()
}

// readSubtree reads a single subtree from disk and validates its transactions.
func (u *BlockValidation) readSubtree(ctx context.Context, block *model.Block, subtreeIdx int, subtreeHash *chainhash.Hash) (result subtreeResult) {
	structure, err := u.readSubtreeStructure(ctx, block, subtreeHash)
	if err != nil {
		return subtreeResult{err: err}
	}

	subtree := structure.subtree

	// The structure read owns both objects until this function returns them on the
	// result; on any failure below, release the full blob it loaded rather than
	// leaving an mmap region mapped for a subtree nobody will ever consume.
	defer func() {
		if result.err != nil {
			releaseSubtreeStructure(structure.fullSubtree)
		}
	}()

	// get the subtree data from disk
	subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return subtreeResult{err: errors.NewNotFoundError("[getBlockTransactions][%s] failed to get subtree data %s", block.Hash().String(), subtreeHash.String(), err)}
	}
	defer subtreeDataReader.Close()

	// Pooled buffered reader, as the structure read uses, to reduce GC pressure
	bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
	bufferedReader.Reset(subtreeDataReader)
	defer func() {
		bufferedReader.Reset(nil)
		bufioReaderPool.Put(bufferedReader)
	}()

	// The subtree data reader compares MOST transactions it stores against the node
	// they occupy — but not all of them, so the comparison cannot be delegated to it
	// (bitcoin-sv/teranode#4838). Its running index advances only on a compared store,
	// and a transaction that is coinbase-shaped while that index stands at 1 is
	// diverted into slot 0, overwriting what is already there and continuing WITHOUT
	// advancing the index and WITHOUT any node comparison. In a subtree that carries
	// no coinbase placeholder — every subtree after the first — the index stands at 1
	// straight after the first real transaction, so one slot per such subtree is
	// written from bytes nothing ever tied to the header. The body then drops a
	// header-committed transaction and carries a fabricated one in its place while
	// every structural anchor, and the merkle root composed from the node lists, still
	// agree.
	//
	// The loop below closes that by comparing every slot itself.
	subtreeData, err := subtreepkg.NewSubtreeDataFromReader(subtree, bufferedReader)
	if err != nil {
		return subtreeResult{err: errors.NewProcessingError("[getBlockTransactions][%s] failed to deserialize subtree data %s: %v", block.Hash().String(), subtreeHash.String(), err)}
	}

	// Validate transactions in this subtree
	for idx, tx := range subtreeData.Txs {
		if subtreeIdx == 0 && idx == 0 {
			// First tx in first subtree must be coinbase
			if tx != nil && !tx.IsCoinbase() {
				return subtreeResult{err: errors.NewProcessingError("[getBlockTransactions][%s] invalid coinbase tx at index %d in subtree %s", block.Hash().String(), idx, subtreeHash.String())}
			}
			subtreeData.Txs[idx] = nil // set to nil to indicate coinbase
		} else {
			if tx == nil {
				return subtreeResult{err: errors.NewProcessingError("[getBlockTransactions][%s] missing tx at index %d in subtree %s", block.Hash().String(), idx, subtreeHash.String())}
			}

			// Deliberately NOT scoped to non-first subtrees. The defect is a slot that
			// was never compared; keying our guard on the reader's current diversion
			// condition would let it silently stop covering the slot if that condition
			// ever changes. The cost is nil: the reader calls SetTxHash on every
			// transaction it stores, including the diverted one, so this is a cache read.
			if idx >= len(subtree.Nodes) {
				return subtreeResult{err: errors.NewProcessingError("[getBlockTransactions][%s] subtree data %s carries a transaction at index %d beyond the subtree's %d nodes", block.Hash().String(), subtreeHash.String(), idx, len(subtree.Nodes))}
			}

			if !subtree.Nodes[idx].Hash.Equal(*tx.TxIDChainHash()) {
				// Routed through the key-mismatch quarantine rather than to
				// NewBlockCorruptError or a bare processing error. Corrupt would strike the
				// catch-up PRIMARY, which under parallel fetch need not be the peer that
				// served this subtree, and the fetch-site hash check the corrupt branch
				// justifies itself by is exactly the check that misses this shape. A bare
				// processing error would leave the forged body on disk for a reader with
				// the same blind spot. The quarantine deletes this exact blob, confirms the
				// deletion, aborts the run fail-closed when it cannot, and applies no ban
				// score to a peer nothing has proved served it.
				return subtreeResult{err: markSubtreeKeyMismatch(
					errors.NewProcessingError("[getBlockTransactions][%s] subtree data %s transaction at index %d does not match its node: node %s, transaction %s",
						block.Hash().String(), subtreeHash.String(), idx, subtree.Nodes[idx].Hash.String(), tx.TxIDChainHash().String()),
					subtreeBlobRef{hash: *subtreeHash, fileType: fileformat.FileTypeSubtreeData},
				)}
			}
		}
	}

	return subtreeResult{
		subtree:     subtree,
		subtreeData: subtreeData,
		subtreeHash: *subtreeHash,
		subtreeIdx:  subtreeIdx,
		fullSubtree: structure.fullSubtree,
	}
}

// writeSubtreeFilesFromTxs writes the full subtree file to disk.
// Takes transactions directly (without coinbase nil entry).
// Note: Subtree meta files (.subtreemeta) are intentionally skipped during quick validation
// for performance. They will be generated on-demand if needed later.
func (u *BlockValidation) writeSubtreeFilesFromTxs(ctx context.Context, block *model.Block, subtreeIdx int, subtree *subtreepkg.Subtree, txs []*bt.Tx, subtreeHash chainhash.Hash, carriedFullSubtree *subtreepkg.Subtree, outpointOnly bool) error {
	// carriedFullSubtree was read and anchored during the batch read (readSubtree);
	// use it instead of issuing another subtreeStore round-trip here, which would in
	// any case come too late to stop this batch's create and spend.
	if carriedFullSubtree == nil {
		fullSubtree, err := subtreepkg.NewIncompleteTreeByLeafCount(subtree.Size())
		if err != nil {
			return errors.NewProcessingError("[writeSubtreeFilesFromTxs][%s] failed to create full subtree %s", block.Hash().String(), subtreeHash.String(), err)
		}

		// Add coinbase node for first subtree
		if subtreeIdx == 0 {
			if err = fullSubtree.AddCoinbaseNode(); err != nil {
				return errors.NewProcessingError("[writeSubtreeFilesFromTxs][%s] failed to add coinbase node to full subtree %s", block.Hash().String(), subtreeHash.String(), err)
			}
		}

		for _, tx := range txs {
			// Get fee and size directly instead of using TxMetaDataFromTx which also
			// computes TxInpoints (only needed for subtreeMeta, which we skip during quick validation).
			// On the outpoint-only fast path, skip GetFees (inputs are un-decorated; fee is 0).
			var fee uint64
			if !outpointOnly {
				var err error
				fee, err = util.GetFees(tx)
				if err != nil {
					return errors.NewProcessingError("[writeSubtreeFilesFromTxs][%s] failed to get fee for tx %s in subtree %s", block.Hash().String(), tx.TxIDChainHash().String(), subtreeHash.String(), err)
				}
			}

			sizeInBytes := uint64(tx.Size())

			if err := fullSubtree.AddNode(*tx.TxIDChainHash(), fee, sizeInBytes); err != nil {
				return errors.NewProcessingError("[writeSubtreeFilesFromTxs][%s] failed to add tx node %s to full subtree %s", block.Hash().String(), tx.TxIDChainHash().String(), subtreeHash.String(), err)
			}
		}

		block.SubtreeSlices[subtreeIdx] = fullSubtree

		fullSubtreeBytes, err := fullSubtree.Serialize()
		if err != nil {
			return errors.NewProcessingError("[writeSubtreeFilesFromTxs][%s] failed to serialize full subtree %s", block.Hash().String(), subtreeHash.String(), err)
		}

		// Write with finite DAH — block persister will promote to permanent when block is confirmed
		dah := block.Height + u.subtreeBlockHeightRetention
		if err = u.subtreeStore.Set(ctx,
			subtreeHash[:],
			fileformat.FileTypeSubtree,
			fullSubtreeBytes,
			bloboptions.WithAllowOverwrite(true),
			bloboptions.WithDeleteAt(dah),
		); err != nil {
			return errors.NewProcessingError("[writeSubtreeFilesFromTxs][%s] failed to store full subtree %s", block.Hash().String(), subtreeHash.String(), err)
		}
	} else {
		block.SubtreeSlices[subtreeIdx] = carriedFullSubtree

		// Subtree already exists with assembly's finite DAH — no change needed.
		// The block persister will promote to permanent when the block is confirmed.
	}

	// Note: Subtree meta file (.subtreemeta) writing is intentionally skipped during quick validation
	// for checkpoint-verified blocks. This significantly improves catchup performance by avoiding:
	// - Existence check for subtree meta
	// - SetTxInpointsFromTx processing for each transaction
	// - Serialization and storage of subtree meta
	// The subtree meta can be regenerated on-demand if needed for merkle proof serving.

	return nil
}

// unlockSubtreeTransactionsIfNeeded runs the post-AddBlock unlock pass that clears the
// per-tx lock taken during quick validation — unless the QuickValidateSkipUtxoLock
// optimization applies to this block, in which case the UTXOs were never locked at create
// time and there is nothing to unlock. callerTag identifies the caller in error messages.
// Shared by quickValidateBlock and quickValidateBlockAsync. See issue #1103.
func (u *BlockValidation) unlockSubtreeTransactionsIfNeeded(ctx context.Context, block *model.Block, callerTag string) error {
	if u.quickValidateSkipsUtxoLock(block) {
		return nil
	}

	if err := u.unlockSubtreeTransactions(ctx, block.SubtreeSlices); err != nil {
		return errors.NewProcessingError("[%s][%s] failed to unlock UTXOs", callerTag, block.Hash().String(), err)
	}

	return nil
}

// unlockSubtreeTransactions unlocks all transactions in the given subtrees in parallel.
// It skips the coinbase placeholder at index 0 of the first subtree.
func (u *BlockValidation) unlockSubtreeTransactions(ctx context.Context, subtrees []*subtreepkg.Subtree) error {
	if len(subtrees) == 0 {
		return nil
	}

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(u.logger, g, 128)

	for subtreeIdx, subtree := range subtrees {
		if subtree == nil || len(subtree.Nodes) == 0 {
			continue
		}

		// For first subtree, skip coinbase at index 0
		startIdx := 0
		if subtreeIdx == 0 {
			startIdx = 1
		}

		if startIdx >= len(subtree.Nodes) {
			continue
		}

		// Capture for goroutine
		nodes := subtree.Nodes
		start := startIdx

		g.Go(func() error {
			txHashes := make([]chainhash.Hash, len(nodes)-start)
			for i := start; i < len(nodes); i++ {
				txHashes[i-start] = nodes[i].Hash
			}
			return u.utxoStore.SetLocked(gCtx, txHashes, false)
		})
	}

	return g.Wait()
}

// SubtreeProcessingBatch holds data for processing a batch of subtrees.
// This struct is used to pass results between batch processing phases
// to avoid recomputing data and enable parallel operations.
type SubtreeProcessingBatch struct {
	// subtrees contains the raw subtree structures (tx hashes/nodes)
	subtrees []*subtreepkg.Subtree

	// subtreeData contains the full transaction data for each subtree
	subtreeData []*subtreepkg.Data

	// subtreeHashes contains the root hash of each subtree
	subtreeHashes []chainhash.Hash

	// txRanges maps batch index to [start, end) indices in batchTxs
	txRanges [][2]int

	// batchTxs contains all transactions in this batch (excluding coinbase nil entries)
	batchTxs []*bt.Tx

	// fullSubtrees carries the already-present full .subtree blob for each subtree
	// in this batch, loaded and ANCHORED during the read that produced the batch.
	// Nil at an index means no such blob existed and the write phase must build one.
	//
	// The blob is carried rather than re-read at the point it is consumed because
	// that consumption runs beside createAndSpendUTXOsForBatch in both pipelined
	// variants and after it in the sequential one, so an anchor placed there could
	// only fire once the mutations had begun (bitcoin-sv/teranode#4838).
	//
	// Ownership transfers to block.SubtreeSlices when the write phase takes an
	// entry, which nils it here; whatever is left is closed by Close.
	fullSubtrees []*subtreepkg.Subtree

	// batchStart is the global starting index in block.Subtrees
	batchStart int

	// batchEnd is the global ending index (exclusive) in block.Subtrees
	batchEnd int

	// outpointOnly is the below-checkpoint fast-path mode for this block, computed ONCE
	// per block in the quickValidate entry point and threaded down (never re-derived per
	// seam). Consumers read this field so every phase — decorate, fee, create, spend —
	// sees a single consistent decision and cannot drift. See quickValidateOutpointOnly.
	outpointOnly bool
}

// Close releases mmap-backed subtree resources in this batch.
//
// The carried full subtrees are released too: an entry still present here is one
// the write phase never took ownership of, so nothing else can be holding it, and
// leaving it would leak one mapped region per subtree per batch.
func (b *SubtreeProcessingBatch) Close() {
	for _, st := range b.subtrees {
		if st != nil {
			st.Close()
		}
	}

	for i, st := range b.fullSubtrees {
		if st != nil {
			releaseSubtreeStructure(st)
			b.fullSubtrees[i] = nil
		}
	}
}

// extendTxFromSameBlockParents extends tx's inputs whose parents are present in
// parents (same-block parents already decoded in this batch), returning whether
// any input still needs an external (store) lookup.
//
// PreviousTxOutIndex comes from the untrusted child tx, so the parent's output
// count is bounds-checked before indexing: an out-of-range reference returns an
// error instead of panicking the node with index-out-of-range (issue 1283). This
// mirrors the guard in services/validator/Validator.go extendTransaction.
func extendTxFromSameBlockParents(tx *bt.Tx, parents map[chainhash.Hash]*bt.Tx) (needsExternalLookup bool, err error) {
	for j, input := range tx.Inputs {
		// Skip a nil input rather than dereferencing it. The inline loop this
		// replaced had no check, so nothing regresses, but
		// discardSuppliedPreviousOutputs walks the same slice a few lines earlier
		// and does guard it — one of the two asserting the hazard while the other
		// panics on it is worse than either choice made consistently.
		if input == nil {
			continue
		}

		parentHash := input.PreviousTxIDChainHash()

		parentTx, ok := parents[*parentHash]
		if !ok {
			needsExternalLookup = true
			continue
		}

		vout := input.PreviousTxOutIndex
		if parentTx.Outputs == nil || int(vout) >= len(parentTx.Outputs) || parentTx.Outputs[vout] == nil {
			return false, errors.NewProcessingError("tx %s input %d references non-existent output %d of same-block parent %s",
				tx.TxIDChainHash().String(), j, vout, parentHash.String())
		}

		tx.Inputs[j].PreviousTxSatoshis = parentTx.Outputs[vout].Satoshis
		tx.Inputs[j].PreviousTxScript = parentTx.Outputs[vout].LockingScript
	}

	return needsExternalLookup, nil
}

// discardSuppliedPreviousOutputs clears the previous-output metadata a
// transaction arrived with, so the extension paths below repopulate it from the
// block's own parents or from the local UTXO store.
//
// Subtree data is fetched from the peer that announced the block and arrives in
// extended format, which means the peer — not this node — would otherwise choose
// the locking script and value used for script execution and value conservation.
// That is exploitable because the UTXO commitment (util.UTXOHashInto) hashes
// `lockingScript || VarInt(satoshis)` without a script-length prefix: the
// script/value boundary is not pinned, so a shorter spendable script paired with
// a larger value reproduces a genuine output's commitment and passes the store's
// utxoHash check (GHSA-v76m-6vc7-g7c7).
//
// An in-block parent's outputs are committed to by its txid, so resolving
// against them is as authoritative as the store.
func discardSuppliedPreviousOutputs(tx *bt.Tx) {
	for _, input := range tx.Inputs {
		if input == nil {
			continue
		}

		input.PreviousTxScript = nil
		input.PreviousTxSatoshis = 0
	}

	// IsExtended() also reports true from this flag alone, so clearing the
	// per-input fields is not enough on its own.
	tx.SetExtended(false)
}

// processSubtreeBatch reads and extends a batch of subtrees.
// This is the shared first phase of both quick and normal validation.
//
// It performs:
// 1. Parallel reading of subtrees from disk
// 2. Same-block parent resolution (extends tx inputs from in-memory txs)
// 3. External UTXO lookups for remaining unextended inputs
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: The block being processed
//   - batchStart: Starting index in block.Subtrees
//   - batchEnd: Ending index (exclusive) in block.Subtrees
//   - extendedTxsFromPrevBatches: Map of tx hash -> extended tx from previous batches
//
// Returns:
//   - *SubtreeProcessingBatch: Batch data with extended transactions
//   - error: If reading or extension fails
func (u *BlockValidation) processSubtreeBatch(
	ctx context.Context,
	block *model.Block,
	batchStart, batchEnd int,
	extendedTxsFromPrevBatches map[chainhash.Hash]*bt.Tx,
	outpointOnly bool,
) (*SubtreeProcessingBatch, error) {
	batchSize := batchEnd - batchStart

	batch := &SubtreeProcessingBatch{
		subtrees:      make([]*subtreepkg.Subtree, batchSize),
		subtreeData:   make([]*subtreepkg.Data, batchSize),
		subtreeHashes: make([]chainhash.Hash, batchSize),
		txRanges:      make([][2]int, batchSize),
		batchTxs:      make([]*bt.Tx, 0),
		fullSubtrees:  make([]*subtreepkg.Subtree, batchSize),
		batchStart:    batchStart,
		batchEnd:      batchEnd,
		outpointOnly:  outpointOnly,
	}

	// Phase 1: Read subtrees in parallel
	subtreeChannels := make([]chan subtreeResult, batchSize)
	for i := range subtreeChannels {
		subtreeChannels[i] = make(chan subtreeResult, 1)
	}

	readerCtx, cancelReaders := context.WithCancel(ctx)
	g, gCtx := errgroup.WithContext(readerCtx)
	util.SafeSetLimit(u.logger, g, 128)

	for i := 0; i < batchSize; i++ {
		globalIdx := batchStart + i
		localIdx := i
		hash := block.Subtrees[globalIdx]
		resultChan := subtreeChannels[localIdx]
		g.Go(func() error {
			result := u.readSubtree(gCtx, block, globalIdx, hash)
			select {
			case resultChan <- result:
			case <-gCtx.Done():
				return gCtx.Err()
			}
			return nil
		})
	}

	// stopReaders cancels the per-batch readers and JOINS them, so no reader of a
	// subtree blob is still live once this function returns. The deferred quarantine at
	// the entry points deletes exact blobs, and it must not race a read of one
	// (bitcoin-sv/teranode#4838). Joining is cheap: every send is into a
	// single-slot buffered channel, so a cancelled reader always reaches its return.
	readersDone := make(chan struct{})

	go func() {
		_ = g.Wait()
		for _, ch := range subtreeChannels {
			close(ch)
		}

		close(readersDone)
	}()

	stopReaders := func() {
		cancelReaders()
		<-readersDone
	}

	// Phase 2: Collect results and extend same-block parents
	txsNeedingExtension := make([]*bt.Tx, 0)

	for i := 0; i < batchSize; i++ {
		result, ok := <-subtreeChannels[i]
		if !ok {
			stopReaders()
			return nil, errors.NewProcessingError("[processSubtreeBatch][%s] channel %d closed", block.Hash().String(), batchStart+i)
		}
		if result.err != nil {
			stopReaders()
			return nil, result.err
		}

		batch.subtrees[i] = result.subtree
		batch.subtreeData[i] = result.subtreeData
		batch.subtreeHashes[i] = result.subtreeHash
		batch.fullSubtrees[i] = result.fullSubtree

		startIdx := len(batch.batchTxs)
		for _, tx := range result.subtreeData.Txs {
			if tx == nil {
				continue // skip coinbase
			}

			// Never trust previous-output metadata supplied by the announcing
			// peer; re-resolve it locally. Skipped on the outpoint-only fast
			// path, which does no script or value checks at all (see Phase 3).
			if !outpointOnly {
				discardSuppliedPreviousOutputs(tx)
			}

			// Try to extend from same-block parents first
			if !tx.IsExtended() {
				needsExternalLookup, extendErr := extendTxFromSameBlockParents(tx, extendedTxsFromPrevBatches)
				if extendErr != nil {
					stopReaders()
					return nil, errors.NewProcessingError("[processSubtreeBatch][%s] same-block parent extension failed", block.Hash().String(), extendErr)
				}

				if needsExternalLookup {
					txsNeedingExtension = append(txsNeedingExtension, tx)
				}
			}

			extendedTxsFromPrevBatches[*tx.TxIDChainHash()] = tx
			tx.SetTxHash(tx.TxIDChainHash())
			batch.batchTxs = append(batch.batchTxs, tx)
		}
		batch.txRanges[i] = [2]int{startIdx, len(batch.batchTxs)}
	}

	// Phase 3: Extend remaining transactions using bulk UTXO store lookup.
	// Skipped on the outpoint-only fast path: below-checkpoint blocks are certified
	// valid and parent satoshis/scripts are not needed for UTXO create/spend.
	if !outpointOnly && len(txsNeedingExtension) > 0 {
		if err := u.utxoStore.BatchPreviousOutputsDecorate(ctx, txsNeedingExtension); err != nil {
			stopReaders()
			return nil, errors.NewProcessingError("[processSubtreeBatch][%s] failed to extend transactions: %v", block.Hash().String(), err)
		}
	}

	stopReaders()
	return batch, nil
}

// createAndSpendUTXOsForBatch creates and spends UTXOs for all transactions in a batch.
// This is used by quick validation for checkpoint-verified blocks.
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: The block being processed (provides BlockID and Height)
//   - batch: The processed batch with extended transactions
//
// Returns:
//   - error: If UTXO creation or spending fails
func (u *BlockValidation) createAndSpendUTXOsForBatch(ctx context.Context, block *model.Block, batch *SubtreeProcessingBatch) error {
	if len(batch.batchTxs) == 0 {
		return nil
	}

	outpointOnly := batch.outpointOnly

	// Invariant I4 (fail-closed): outpoint-only create+spend must never run above the highest
	// hardcoded checkpoint. Key the guard on the ACTUAL per-block mode (batch.outpointOnly, the
	// same value that drives create/spend below) rather than on the raw setting+store: under a
	// catchup-checkpoint override, blocks in (hardcodedCheckpoint, overrideHeight] legitimately
	// enter quick validation in NORMAL mode (batch.outpointOnly == false, no fast-path op), and
	// gating on setting+store alone would wrongly trip on them. This is not a tautology: if a
	// future change makes quickValidateOutpointOnly return true above the hardcoded checkpoint,
	// batch.outpointOnly would be true there and this guard fires — catching exactly that bug,
	// while never rejecting a valid normal-mode block. HighestCheckpointHeight uses the hardcoded
	// checkpoints (never the operator override).
	if outpointOnly && block.Height > blockchain.HighestCheckpointHeight(u.settings.ChainCfgParams.Checkpoints) {
		return errors.NewProcessingError("[createAndSpendUTXOsForBatch] invariant I4 violated: outpoint-only mode active above checkpoint at height %d", block.Height)
	}

	lockUTXOs := !u.quickValidateSkipsUtxoLock(block)

	// Phase 1: Create UTXOs in parallel, collecting any that already exist
	createG, createCtx := errgroup.WithContext(ctx)
	// Set concurrency to 8x StoreBatcherSize to allow sufficient parallelism while the
	// UTXO store batches operations internally. This multiplier balances throughput with
	// resource usage, allowing multiple batches to be in flight simultaneously.
	util.SafeSetLimit(u.logger, createG, u.settings.UtxoStore.StoreBatcherSize*8)

	// Track transactions that already exist so we can update their mined info
	var existingTxsMu sync.Mutex
	var existingTxHashes []*chainhash.Hash

	minedBlockInfo := utxo.MinedBlockInfo{
		BlockID:     block.ID,
		BlockHeight: block.Height,
	}

	batchSize := batch.batchEnd - batch.batchStart
	for i := 0; i < batchSize; i++ {
		globalSubtreeIdx := batch.batchStart + i
		txRange := batch.txRanges[i]
		for txIdx := txRange[0]; txIdx < txRange[1]; txIdx++ {
			tx := batch.batchTxs[txIdx]

			if shouldSkipUnspendableCreate(lockUTXOs, u.settings, tx, block.Height) {
				// Not written to the store; its inputs are still spent in Phase 2.
				continue
			}

			sIdx := globalSubtreeIdx
			createG.Go(func() error {
				_, _, err := u.utxoStore.SpendAndCreate(createCtx, tx, block.Height, utxo.WithCreateOnly(),
					utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
						BlockID:     block.ID,
						BlockHeight: block.Height,
						SubtreeIdx:  sIdx,
					}), utxo.WithLocked(lockUTXOs), utxo.WithSkipExtendedInputs(outpointOnly))
				if err != nil {
					if errors.Is(err, errors.ErrTxExists) {
						// Transaction already exists - collect it for mined info update
						txHash := tx.TxIDChainHash()
						existingTxsMu.Lock()
						existingTxHashes = append(existingTxHashes, txHash)
						existingTxsMu.Unlock()
						return nil
					}
					return errors.NewProcessingError("[createAndSpendUTXOsForBatch][%s] failed to create UTXO for tx %s", block.Hash().String(), tx.TxIDChainHash().String(), err)
				}
				return nil
			})
		}
	}

	if err := createG.Wait(); err != nil {
		return err
	}

	// Phase 1.5: Update mined info for transactions that already existed
	// This handles the case where a previous attempt created UTXOs with a different
	// block ID. Chunked via the shared helper (issue 936): on a fat-batch retry every tx
	// in the batch already exists, and a single unchunked SetMinedMulti call would
	// overrun the aerospike client connection pool.
	if len(existingTxHashes) > 0 {
		if err := utxo.SetMinedMultiChunked(ctx, u.logger, u.utxoStore, existingTxHashes, minedBlockInfo,
			u.settings.UtxoStore.MaxMinedBatchSize, u.settings.UtxoStore.MaxMinedRoutines); err != nil {
			return errors.NewProcessingError("[createAndSpendUTXOsForBatch][%s] failed to update mined info for %d existing txs", block.Hash().String(), len(existingTxHashes), err)
		}
	}

	// Phase 2: Spend all transactions, retrying transient store errors the way the
	// legacy path does (services/legacy/netsync PreValidateTransactions). Unlike
	// legacy, conflicts are NOT tolerated here: legacy runs the validator with
	// WithCreateConflicting so block assembly's ProcessConflicting later reconciles
	// the loser, but the quick path never writes conflicting subtree nodes and has
	// no such resolver — tolerating ErrTxConflicting would leave an output's spend
	// permanently attributed to a non-canonical tx. Hard-fail instead (fail-closed).
	// Dirty-restart replay does not need conflict tolerance: re-spending an output
	// with the same spender is the store's idempotent success path.
	return u.spendBatchWithRetry(ctx, block, batch.batchTxs, outpointOnly)
}

// spendRetryBackoffDefault is the pause between spend retry attempts. Matches the
// legacy path's retryBackoff (services/legacy/netsync PreValidateTransactions).
const spendRetryBackoffDefault = 2 * time.Second

// spendBatchWithRetry spends txs in parallel with bounded retries. Per attempt:
// retryable errors (transient store overload) queue the tx for the next attempt;
// anything else — including ErrTxConflicting and ErrSpent — fails hard immediately
// (fail-closed: the quick path has no ProcessConflicting pipeline to reconcile a
// tolerated conflict; see the phase-2 call site). Gives up early when an attempt
// makes no progress. Retry cadence mirrors the legacy path's PreValidateTransactions
// (maxRetries=10, 2s backoff) so both below-checkpoint implementations converge
// identically after dirty restarts.
func (u *BlockValidation) spendBatchWithRetry(ctx context.Context, block *model.Block, txs []*bt.Tx, outpointOnly bool) error {
	const maxRetries = 10

	backoff := u.spendRetryBackoff
	if backoff <= 0 {
		backoff = spendRetryBackoffDefault
	}

	pending := txs
	total := len(txs)

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return errors.NewProcessingError("[spendBatchWithRetry][%s] context cancelled", block.Hash().String())
		}

		if attempt > 0 {
			u.logger.Infof("[spendBatchWithRetry][%s] retry %d/%d: %d of %d transactions remaining", block.Hash().String(), attempt, maxRetries, len(pending), total)
			time.Sleep(backoff)
		}

		spendG, spendCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(u.logger, spendG, u.settings.UtxoStore.SpendBatcherSize*u.settings.UtxoStore.SpendBatcherConcurrency*2)

		var (
			mu        sync.Mutex
			retryable []*bt.Tx
			lastErr   error
			hardFail  error
		)

		for _, tx := range pending {
			tx := tx
			spendG.Go(func() error {
				if _, _, err := u.utxoStore.SpendAndCreate(spendCtx, tx, block.Height, utxo.WithSpendOnly(),
					utxo.WithIgnoreLocked(true), utxo.WithSkipUTXOHashCheck(outpointOnly)); err != nil {
					if errors.IsRetryableError(err) {
						mu.Lock()
						retryable = append(retryable, tx)
						lastErr = err
						mu.Unlock()
						return nil
					}
					mu.Lock()
					hardFail = errors.NewProcessingError("[spendBatchWithRetry][%s] failed to spend tx %s", block.Hash().String(), tx.TxIDChainHash().String(), err)
					mu.Unlock()
				}
				return nil
			})
		}

		_ = spendG.Wait()

		if hardFail != nil {
			return hardFail
		}

		if len(retryable) == 0 {
			if attempt > 0 {
				u.logger.Infof("[spendBatchWithRetry][%s] all spends succeeded after %d retries", block.Hash().String(), attempt)
			}
			return nil
		}

		if attempt > 0 && len(retryable) >= len(pending) {
			return errors.NewProcessingError("[spendBatchWithRetry][%s] %d of %d spends failed with no progress, giving up", block.Hash().String(), len(retryable), total, lastErr)
		}

		pending = retryable
	}

	return errors.NewProcessingError("[spendBatchWithRetry][%s] %d of %d spends still failing after %d retries", block.Hash().String(), len(pending), total, maxRetries)
}

// writeSubtreeFilesForBatch writes the full subtree files (.subtree) for a batch.
// Note: Subtree metadata files (.subtreemeta) are skipped during quick validation
// for performance optimization. They can be regenerated on-demand if needed.
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: The block being processed
//   - batch: The processed batch with extended transactions
//
// Returns:
//   - error: If file writing fails
func (u *BlockValidation) writeSubtreeFilesForBatch(ctx context.Context, block *model.Block, batch *SubtreeProcessingBatch) error {
	writeG, writeCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(u.logger, writeG, u.settings.BlockValidation.SubtreeBatchWriteConcurrency)

	batchSize := batch.batchEnd - batch.batchStart
	for i := 0; i < batchSize; i++ {
		globalIdx := batch.batchStart + i
		localIdx := i
		subtree := batch.subtrees[localIdx]
		txRange := batch.txRanges[localIdx]
		subtreeTxs := batch.batchTxs[txRange[0]:txRange[1]]
		subtreeHash := batch.subtreeHashes[localIdx]

		// Ownership of the carried blob moves to block.SubtreeSlices here, so clear
		// the batch's reference: Close must not release a subtree the block is now
		// holding for the merkle check.
		carriedFullSubtree := batch.fullSubtrees[localIdx]
		batch.fullSubtrees[localIdx] = nil

		writeG.Go(func() error {
			return u.writeSubtreeFilesFromTxs(writeCtx, block, globalIdx, subtree, subtreeTxs, subtreeHash, carriedFullSubtree, batch.outpointOnly)
		})
	}

	return writeG.Wait()
}

// buildSubtreeJobsForBatch builds subtree structures and queues write jobs to a channel.
// This is the async variant of writeSubtreeFilesForBatch - it builds the subtree structures
// synchronously (needed for merkle validation) but defers the actual I/O to background workers.
//
// The function sends jobs to writeJobsChan. If the channel send would block and context is
// cancelled (e.g., due to a write error), this function returns immediately with the context error.
//
// Parameters:
//   - ctx: Context for cancellation (cancelled if any writer fails)
//   - block: The block being processed
//   - batch: The processed batch with extended transactions
//   - writeJobsChan: Channel to send write jobs to background workers
//   - wg: per-block WaitGroup (bitcoin-sv/teranode#4692); Add(1) happens here, at enqueue time,
//     never inside the worker — see the ordering-invariant comment on the send loop below
//   - freshness: records (hash, FileTypeSubtree) for every index this batch is about to freshly
//     write, so removeCatchupSubtreeFiles can later restrict deletion to exactly those pairs
//
// Returns:
//   - error: If building subtrees fails or context is cancelled
func (u *BlockValidation) buildSubtreeJobsForBatch(ctx context.Context, block *model.Block, batch *SubtreeProcessingBatch, writeJobsChan chan<- *SubtreeWriteJob, wg *sync.WaitGroup, freshness *subtreeFreshness) error {
	// Build subtrees in parallel (CPU-bound work)
	buildG, buildCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(u.logger, buildG, u.settings.BlockValidation.SubtreeBatchWriteConcurrency)

	batchSize := batch.batchEnd - batch.batchStart
	jobs := make([]*SubtreeWriteJob, batchSize)

	for i := 0; i < batchSize; i++ {
		globalIdx := batch.batchStart + i
		localIdx := i
		subtree := batch.subtrees[localIdx]
		txRange := batch.txRanges[localIdx]
		subtreeTxs := batch.batchTxs[txRange[0]:txRange[1]]
		subtreeHash := batch.subtreeHashes[localIdx]

		// Ownership of the carried blob moves to block.SubtreeSlices here, so clear
		// the batch's reference: Close must not release a subtree the block is now
		// holding for the merkle check.
		carriedFullSubtree := batch.fullSubtrees[localIdx]
		batch.fullSubtrees[localIdx] = nil

		buildG.Go(func() error {
			job, err := u.buildSubtreeAndQueueWrite(buildCtx, block, globalIdx, subtree, subtreeTxs, subtreeHash, carriedFullSubtree, batch.outpointOnly)
			if err != nil {
				return err
			}
			// Each goroutine writes a distinct index; buildG.Wait() below
			// provides the happens-before that makes these writes visible.
			jobs[localIdx] = job
			return nil
		})
	}

	if err := buildG.Wait(); err != nil {
		return err
	}

	// Unlike the two fetch producers (fetchAndStoreSubtree / fetchAndStoreSubtreeData), which mark
	// fresh only after their own Set succeeds, quick validation marks fresh here at enqueue time —
	// before the asynchronous subtreeWriteWorker has landed the write — because whether a full
	// subtree already existed was settled synchronously during prefetch, so freshness is already
	// known and needs nothing back from the worker. Marking before the write lands is deliberate and
	// harmless: a pair whose write never lands is simply not on disk, and the cleanup path's Del
	// tolerates ErrNotFound (see removeCatchupSubtreeFiles in catchup.go)
	// (bitcoin-sv/teranode#4692). AlreadyExists is read from the job rather than from the batch
	// because the batch's carried reference has been handed to the block by now.
	for i := 0; i < batchSize; i++ {
		if jobs[i] != nil && !jobs[i].AlreadyExists {
			freshness.markFresh(batch.subtreeHashes[i], fileformat.FileTypeSubtree)
		}
	}

	// Queue all jobs to the channel (non-blocking with context check).
	//
	// Ordering invariant (bitcoin-sv/teranode#4692), stated explicitly because getting it backwards
	// reopens the exact race this barrier exists to close: wg.Add(1) MUST happen here, at enqueue
	// time, before the channel send — never inside subtreeWriteWorker. If Add happened in the
	// worker instead, there would be a window between this send and a worker actually receiving
	// the job during which it is neither counted by Add nor observable any other way; a Wait()
	// call landing in that window would see a WaitGroup with nothing added yet and return
	// immediately, missing the in-flight write entirely.
	for _, job := range jobs {
		if job == nil {
			continue
		}

		wg.Add(1)
		job.Done = wg

		select {
		case writeJobsChan <- job:
		case <-ctx.Done():
			// The job was never handed to a worker, so nothing will ever call Done() for
			// it — do so here, or a cancelled send would leave the count permanently
			// non-zero and hang a later Wait().
			wg.Done()
			return ctx.Err()
		}
	}

	return nil
}

// prefetchSubtreeBatch reads subtrees from disk without extending transactions.
// This is the first phase of the pipeline, focused on I/O.
//
// Populates: subtrees, subtreeData, subtreeHashes, batchStart, batchEnd
// Does NOT populate: txRanges, batchTxs (filled during extend phase)
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: The block being processed
//   - batchStart: Starting index in block.Subtrees
//   - batchEnd: Ending index (exclusive) in block.Subtrees
//
// Returns:
//   - *SubtreeProcessingBatch: Batch with subtree data (txs not yet extended)
//   - error: If reading fails
func (u *BlockValidation) prefetchSubtreeBatch(
	ctx context.Context,
	block *model.Block,
	batchStart, batchEnd int,
	outpointOnly bool,
) (*SubtreeProcessingBatch, error) {
	batchSize := batchEnd - batchStart

	batch := &SubtreeProcessingBatch{
		subtrees:      make([]*subtreepkg.Subtree, batchSize),
		subtreeData:   make([]*subtreepkg.Data, batchSize),
		subtreeHashes: make([]chainhash.Hash, batchSize),
		txRanges:      make([][2]int, batchSize),
		batchTxs:      make([]*bt.Tx, 0),
		fullSubtrees:  make([]*subtreepkg.Subtree, batchSize),
		batchStart:    batchStart,
		batchEnd:      batchEnd,
		outpointOnly:  outpointOnly,
	}

	// Read subtrees in parallel
	subtreeChannels := make([]chan subtreeResult, batchSize)
	for i := range subtreeChannels {
		subtreeChannels[i] = make(chan subtreeResult, 1)
	}

	readerCtx, cancelReaders := context.WithCancel(ctx)
	g, gCtx := errgroup.WithContext(readerCtx)
	util.SafeSetLimit(u.logger, g, 128)

	for i := 0; i < batchSize; i++ {
		globalIdx := batchStart + i
		localIdx := i
		hash := block.Subtrees[globalIdx]
		resultChan := subtreeChannels[localIdx]
		g.Go(func() error {
			result := u.readSubtree(gCtx, block, globalIdx, hash)
			select {
			case resultChan <- result:
			case <-gCtx.Done():
				return gCtx.Err()
			}
			return nil
		})
	}

	// stopReaders cancels the per-batch readers and JOINS them, so no reader of a
	// subtree blob is still live once this function returns. The deferred quarantine at
	// the entry points deletes exact blobs, and it must not race a read of one
	// (bitcoin-sv/teranode#4838). Joining is cheap: every send is into a
	// single-slot buffered channel, so a cancelled reader always reaches its return.
	readersDone := make(chan struct{})

	go func() {
		_ = g.Wait()
		for _, ch := range subtreeChannels {
			close(ch)
		}

		close(readersDone)
	}()

	stopReaders := func() {
		cancelReaders()
		<-readersDone
	}

	// Collect results (no extension yet)
	for i := 0; i < batchSize; i++ {
		result, ok := <-subtreeChannels[i]
		if !ok {
			stopReaders()
			return nil, errors.NewProcessingError("[prefetchSubtreeBatch][%s] channel %d closed", block.Hash().String(), batchStart+i)
		}
		if result.err != nil {
			stopReaders()
			return nil, result.err
		}

		batch.subtrees[i] = result.subtree
		batch.subtreeData[i] = result.subtreeData
		batch.subtreeHashes[i] = result.subtreeHash
		batch.fullSubtrees[i] = result.fullSubtree
	}

	stopReaders()
	return batch, nil
}

// extendBatch extends transactions using extendedTxs map and UTXO store.
// This is the second phase of the pipeline, handling tx extension.
//
// Populates: txRanges, batchTxs. Updates extendedTxs map.
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: The block being processed
//   - batch: The prefetched batch with subtree data
//   - extendedTxs: Map of tx hash -> extended tx from previous batches (updated in place)
//
// Returns:
//   - error: If extension fails
func (u *BlockValidation) extendBatch(
	ctx context.Context,
	block *model.Block,
	batch *SubtreeProcessingBatch,
	extendedTxs map[chainhash.Hash]*bt.Tx,
) error {
	batchSize := batch.batchEnd - batch.batchStart
	txsNeedingExtension := make([]*bt.Tx, 0)

	for i := 0; i < batchSize; i++ {
		startIdx := len(batch.batchTxs)
		for _, tx := range batch.subtreeData[i].Txs {
			if tx == nil {
				continue // skip coinbase
			}

			// Never trust previous-output metadata supplied by the announcing
			// peer; re-resolve it locally. Skipped on the outpoint-only fast
			// path, which does no script or value checks at all.
			if !batch.outpointOnly {
				discardSuppliedPreviousOutputs(tx)
			}

			// Try to extend from same-block parents first
			if !tx.IsExtended() {
				needsExternalLookup, extendErr := extendTxFromSameBlockParents(tx, extendedTxs)
				if extendErr != nil {
					return errors.NewProcessingError("[extendBatch][%s] same-block parent extension failed", block.Hash().String(), extendErr)
				}

				if needsExternalLookup {
					txsNeedingExtension = append(txsNeedingExtension, tx)
				}
			}

			extendedTxs[*tx.TxIDChainHash()] = tx
			tx.SetTxHash(tx.TxIDChainHash())
			batch.batchTxs = append(batch.batchTxs, tx)
		}
		batch.txRanges[i] = [2]int{startIdx, len(batch.batchTxs)}
	}

	// Extend remaining transactions using bulk UTXO store lookup.
	// Skipped on the outpoint-only fast path (see processSubtreeBatch Phase 3 comment).
	if !batch.outpointOnly && len(txsNeedingExtension) > 0 {
		if err := u.utxoStore.BatchPreviousOutputsDecorate(ctx, txsNeedingExtension); err != nil {
			return errors.NewProcessingError("[extendBatch][%s] failed to extend transactions: %v", block.Hash().String(), err)
		}
	}

	return nil
}
