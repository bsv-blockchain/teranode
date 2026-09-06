package blockvalidation

import (
	"bufio"
	"context"
	"strings"
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
	"github.com/bsv-blockchain/teranode/settings"
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
}

// subtreeWriteWorker processes subtree write jobs from a channel.
// If any write fails, it returns an error which cancels the errgroup context,
// propagating the failure to all other goroutines including UTXO processing.
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

			if job.AlreadyExists {
				// Subtree already exists with assembly's finite DAH — no change needed.
				// The block persister will promote to permanent when the block is confirmed.
				continue
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
//
// Returns:
//   - *SubtreeWriteJob: Job to be processed by async writer (nil if no write needed)
//   - error: If building the subtree fails
func (u *BlockValidation) buildSubtreeAndQueueWrite(ctx context.Context, block *model.Block, subtreeIdx int, subtree *subtreepkg.Subtree, txs []*bt.Tx, subtreeHash chainhash.Hash, fullSubtreeExists, outpointOnly bool) (*SubtreeWriteJob, error) {
	// If we already know the full subtree exists (checked during prefetch), load it
	// This avoids redundant disk I/O during the build phase
	if fullSubtreeExists {
		fullSubtreeBytes, err := u.subtreeStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		if err != nil {
			return nil, errors.NewProcessingError("[buildSubtreeAndQueueWrite][%s] failed to get existing full subtree %s", block.Hash().String(), subtreeHash.String(), err)
		}

		fullSubtree, err := u.newSubtreeFromBytes(fullSubtreeBytes)
		if err != nil {
			return nil, errors.NewProcessingError("[buildSubtreeAndQueueWrite][%s] failed to deserialize full subtree %s", block.Hash().String(), subtreeHash.String(), err)
		}

		block.SubtreeSlices[subtreeIdx] = fullSubtree

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
//
// entry is the block's slot in the quick window, or nil for the pre-window behaviour: with a
// nil entry nothing here waits, registers or signals, and the block is committed inline the
// way it always was.
func (u *BlockValidation) quickValidateBlock(ctx context.Context, block *model.Block, peerID, baseURL string, entry *windowEntry) error {
	if entry == nil {
		return u.quickValidateBlockInner(ctx, block, peerID, baseURL, nil)
	}

	// Leave runs only once the pipeline has returned, so no store call of this attempt can
	// still be in flight when the entry's ids leave the window's maps: it is the drain barrier.
	defer entry.Leave()

	// The entry's own context, so an abort of a predecessor cancels this block's goroutines
	// rather than leaving them running against a window that has already given up on them.
	err := u.quickValidateBlockInner(entry.Context(), block, peerID, baseURL, entry)
	if err != nil {
		entry.Fail(err)

		// Return the recorded outcome, on the CALLER's context: the inner wait runs on the
		// entry's own context, which an abort cancels, so it hands back a generic cancellation
		// where this one hands back the predecessor's recorded service error. That is the error
		// legacy sync has to see to treat the failure as ours rather than the peer's.
		if werr := entry.WaitCommitted(ctx); werr != nil {
			return werr
		}

		return err
	}

	return nil
}

func (u *BlockValidation) quickValidateBlockInner(ctx context.Context, block *model.Block, peerID, baseURL string, entry *windowEntry) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "quickValidateBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[quickValidateBlock][%s] performing quick validation for checkpointed block at height %d", block.Hash().String(), block.Height),
	)
	defer deferFn()

	// Enforce the block-version floor before any body/coinbase inspection (header-first parity
	// with svnode ContextualCheckBlockHeader). Needs only header version, height, and params.
	if err := model.CheckBlockVersion(block.Header.Version, block.Height, u.settings.ChainCfgParams); err != nil {
		return errors.NewBlockInvalidError("[quickValidateBlock][%s] outdated block version", block.Hash().String(), err)
	}

	// Reject blocks without a valid coinbase (e.g. from seeded peers that don't have full block data)
	if block.CoinbaseTx == nil || len(block.CoinbaseTx.Inputs) == 0 {
		return errors.NewBlockIncompleteError("[quickValidateBlock][%s] coinbase tx is nil or has no inputs, peer may not have full block data", block.Hash().String())
	}

	// Compute the below-checkpoint fast-path mode ONCE for this block and thread it through
	// the pipeline (rather than re-deriving it at each seam) so every phase agrees.
	outpointOnly := u.quickValidateOutpointOnly(block)
	if outpointOnly {
		prometheusBlockValidationOutpointOnlyBlocks.Inc()
	}

	var (
		err error
		id  uint64
	)

	if len(block.Subtrees) > 0 {
		// Process all subtrees in streaming fashion - creates UTXOs, spends, writes files
		// This function waits for all processing to complete before returning, ensuring block.ID is set
		_, err = u.processBlockSubtrees(ctx, block, outpointOnly, entry)
		if err != nil {
			return errors.NewProcessingError("[quickValidateBlock][%s] failed to process block subtrees", block.Hash().String(), err)
		}

		// Verify block ID was assigned during processing (sanity check)
		if block.ID == 0 {
			return errors.NewProcessingError("[quickValidateBlock][%s] block ID was not assigned during subtree processing", block.Hash().String())
		}

		// Belt and braces for the one case the in-pipeline probe cannot reach: a retry that
		// arrives with block.ID already set and whose every batch turns out to be empty never
		// runs the probe, so nothing there signals. Without this a successor would wait on
		// idAssigned until this block committed. IDAssigned is idempotent.
		if entry != nil {
			entry.IDAssigned()
		}
	} else {
		// Block ids are handed out in the order they are asked for, so a successor must not
		// ask before its predecessor has.
		if entry != nil {
			if err = entry.WaitPredecessorIDAssigned(ctx); err != nil {
				return err
			}
		}

		// No subtrees to process, assign block ID idempotently
		id, err = u.blockchainClient.AssignBlockID(ctx, block.Hash())
		if err != nil {
			return errors.NewProcessingError("[quickValidateBlock][%s] failed to assign block ID", block.Hash().String(), err)
		}
		block.ID, err = blockIDToUint32(id, block.Hash().String())
		if err != nil {
			return err
		}

		if entry != nil {
			entry.IDAssigned()
			// A coinbase-only block registers no batches at all, so nothing else would ever
			// close its registered channel. Without this a successor's
			// WaitPredecessorsRegistered would fall through to the committed channel instead,
			// which only closes once the ordered committer has run this block — collapsing the
			// window to depth 1 for exactly the blocks early mainnet is made of.
			entry.RegistrationComplete()
		}
	}

	if entry == nil {
		return u.commitBlock(ctx, block, peerID, "quickValidateBlock")
	}

	// In the window the commit is not ours to make: the committer runs it in height order, and
	// this call returns whatever that produced for this block.
	entry.StoreDone()

	// ctx here is the ENTRY's context, which failLocked cancels. For a successor aborted by a
	// predecessor that means this call usually returns a generic cancellation rather than the
	// predecessor's recorded service error. The wrapper re-waits on the caller's context and
	// that is what surfaces the real cause, so neither wait can be simplified away.
	return entry.WaitCommitted(ctx)
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
//   - error: If validation fails or context is cancelled
func (u *BlockValidation) quickValidateBlockAsync(ctx context.Context, block *model.Block, peerID, baseURL string, writeJobsChan chan<- *SubtreeWriteJob) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "quickValidateBlockAsync",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[quickValidateBlockAsync][%s] performing async quick validation for checkpointed block at height %d", block.Hash().String(), block.Height),
	)
	defer deferFn()

	// Enforce the block-version floor before any body/coinbase inspection (header-first parity
	// with svnode ContextualCheckBlockHeader). Needs only header version, height, and params.
	if err := model.CheckBlockVersion(block.Header.Version, block.Height, u.settings.ChainCfgParams); err != nil {
		return errors.NewBlockInvalidError("[quickValidateBlockAsync][%s] outdated block version", block.Hash().String(), err)
	}

	// Reject blocks without a valid coinbase (e.g. from seeded peers that don't have full block data)
	if block.CoinbaseTx == nil || len(block.CoinbaseTx.Inputs) == 0 {
		return errors.NewBlockIncompleteError("[quickValidateBlockAsync][%s] coinbase tx is nil or has no inputs, peer may not have full block data", block.Hash().String())
	}

	// Compute the below-checkpoint fast-path mode ONCE for this block and thread it through
	// the pipeline (rather than re-deriving it at each seam) so every phase agrees.
	outpointOnly := u.quickValidateOutpointOnly(block)
	if outpointOnly {
		prometheusBlockValidationOutpointOnlyBlocks.Inc()
	}

	var (
		err error
		id  uint64
	)

	if len(block.Subtrees) > 0 {
		// Process subtrees with async file writes
		prefetchDepth := u.settings.BlockValidation.SubtreeBatchPrefetchDepth
		if prefetchDepth <= 0 {
			prefetchDepth = 2 // Default for async mode
		}
		_, err = u.processBlockSubtreesPipelineAsync(ctx, block, prefetchDepth, writeJobsChan, outpointOnly)
		if err != nil {
			return errors.NewProcessingError("[quickValidateBlockAsync][%s] failed to process block subtrees", block.Hash().String(), err)
		}
	}

	// If no block ID was assigned during processing, assign idempotently
	if block.ID == 0 {
		id, err = u.blockchainClient.AssignBlockID(ctx, block.Hash())
		if err != nil {
			return errors.NewProcessingError("[quickValidateBlockAsync][%s] failed to assign block ID", block.Hash().String(), err)
		}
		block.ID, err = blockIDToUint32(id, block.Hash().String())
		if err != nil {
			return err
		}
	}

	return u.commitBlock(ctx, block, peerID, "quickValidateBlockAsync")
}

// commitBlock performs the shared final commit for the quick-validation path:
// add the block to the blockchain (subtrees + mined already set), unlock any
// locked UTXOs, update subtree DAH (which fires BlockSubtreesSet), and mark the
// block present in cache. Extracted verbatim from quickValidateBlock /
// quickValidateBlockAsync so both share one commit tail; it is also the per-block
// commit unit the quick window's committer runs in height order, see quick_window.go.
// caller labels logs to preserve each call site's existing text.
func (u *BlockValidation) commitBlock(ctx context.Context, block *model.Block, peerID, caller string) error {
	start := time.Now()

	if err := u.blockchainClient.AddBlock(ctx,
		block,
		peerID,
		options.WithSubtreesSet(true),
		options.WithMinedSet(true),
		options.WithID(uint64(block.ID)),
	); err != nil {
		return errors.NewProcessingError("[%s][%s] failed to add block to blockchain", caller, block.Hash().String(), err)
	}

	prometheusBlockValidationQuickCommitAddBlock.Observe(time.Since(start).Seconds())
	start = time.Now()

	// Unlock all UTXOs - final commit point (no-op when the lock was never taken; #1103).
	if err := u.unlockSubtreeTransactionsIfNeeded(ctx, block, caller); err != nil {
		return err
	}

	prometheusBlockValidationQuickCommitUnlock.Observe(time.Since(start).Seconds())
	start = time.Now()

	// Update subtrees DAH and send BlockSubtreesSet notification.
	if err := u.updateSubtreesDAH(ctx, block); err != nil {
		return errors.NewProcessingError("[%s][%s] failed to update subtrees DAH", caller, block.Hash().String(), err)
	}

	prometheusBlockValidationQuickCommitSubtreesSet.Observe(time.Since(start).Seconds())
	start = time.Now()

	// Mark block as existing in cache.
	if err := u.SetBlockExists(block.Hash()); err != nil {
		u.logger.Errorf("[%s][%s] failed to set block exists cache: %s", caller, block.Hash().String(), err)
	}

	prometheusBlockValidationQuickCommitBlockExists.Observe(time.Since(start).Seconds())

	return nil
}

// subtreeResult holds the result of reading a subtree, sent through a channel
type subtreeResult struct {
	subtree           *subtreepkg.Subtree
	subtreeData       *subtreepkg.Data
	subtreeHash       chainhash.Hash
	subtreeIdx        int
	fullSubtreeExists bool // True if full .subtree file already exists (checked during prefetch)
	err               error
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
func (u *BlockValidation) processBlockSubtrees(ctx context.Context, block *model.Block, outpointOnly bool, entry *windowEntry) (uint64, error) {
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
		return u.processBlockSubtreesSequential(ctx, block, outpointOnly, entry)
	}
	return u.processBlockSubtreesPipeline(ctx, block, prefetchDepth, outpointOnly, entry)
}

// processBlockSubtreesSequential processes subtrees sequentially, one batch at a time.
// This is the fallback when SubtreeBatchPrefetchDepth is 0.
func (u *BlockValidation) processBlockSubtreesSequential(ctx context.Context, block *model.Block, outpointOnly bool, entry *windowEntry) (uint64, error) {
	numSubtrees := len(block.Subtrees)
	block.SubtreeSlices = make([]*subtreepkg.Subtree, numSubtrees)
	var existingBlockID uint64

	// Get block ID first (check for retry using first tx after reading first batch)
	blockIDSet := false

	// Track extended transactions across batches for same-block parent resolution
	extendedTxs := make(map[chainhash.Hash]*bt.Tx)

	if entry != nil {
		// A block that gives up before producing its last batch must not leave a successor
		// waiting for a registration that will never come. RegistrationComplete is idempotent,
		// so this defer and the explicit call after the loop coexist.
		defer entry.RegistrationComplete()

		// The open-gate map must be complete before the first dependency check, which is
		// inside the first createAndSpendUTXOsForBatch call below.
		if err := entry.WaitPredecessorsRegistered(ctx); err != nil {
			return 0, err
		}
	}

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

		if err := registerBatchWithWindow(entry, batch); err != nil {
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
				// Block ids are handed out in the order they are asked for.
				if entry != nil {
					if err := entry.WaitPredecessorIDAssigned(ctx); err != nil {
						return 0, err
					}
				}

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

			if entry != nil {
				entry.IDAssigned()
			}
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

	// Registration is complete only here, after the LAST batch: unlike the pipeline variant,
	// whose extender stage registers each batch as it produces it, this loop does its store work
	// batch by batch on one goroutine, so a successor's WaitPredecessorsRegistered is held until
	// the whole block has been claimed. On a multi-batch block that costs the successor its
	// overlap. It does not bite today: legacy blocks arrive as one subtree, so this loop runs a
	// single batch, and the pipeline variant is what a multi-batch block takes.
	if entry != nil {
		entry.RegistrationComplete()
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
func (u *BlockValidation) processBlockSubtreesPipeline(ctx context.Context, block *model.Block, prefetchDepth int, outpointOnly bool, entry *windowEntry) (uint64, error) {
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

		if entry != nil {
			// The extender owns registration: a successor's dependency check is only complete
			// once every batch of this block has claimed its ids. The defer covers the error
			// path too, so a block that gives up never leaves a successor waiting.
			defer entry.RegistrationComplete()
		}

		extendedTxs := make(map[chainhash.Hash]*bt.Tx)
		for batch := range prefetchChan {
			start := time.Now()
			if err := u.extendBatch(gCtx, block, batch, extendedTxs); err != nil {
				return err
			}

			if err := registerBatchWithWindow(entry, batch); err != nil {
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
		if entry != nil {
			// The open-gate map must be complete before the first dependency check, which is
			// inside the first createAndSpendUTXOsForBatch call below.
			if err := entry.WaitPredecessorsRegistered(gCtx); err != nil {
				return err
			}
		}

		for batch := range extendedChan {
			// Block ID check (first batch only)
			if !blockIDSet && len(batch.batchTxs) > 0 {
				existingMeta, err := u.utxoStore.Get(gCtx, batch.batchTxs[0].TxIDChainHash(), fields.BlockIDs)
				if err == nil && existingMeta != nil && len(existingMeta.BlockIDs) > 0 {
					existingBlockID = uint64(existingMeta.BlockIDs[0])
					block.ID = existingMeta.BlockIDs[0]
					u.logger.Debugf("[processBlockSubtreesPipeline][%s] reusing BlockID %d from retry", block.Hash().String(), existingBlockID)
				} else if block.ID == 0 {
					// Block ids are handed out in the order they are asked for.
					if entry != nil {
						if err := entry.WaitPredecessorIDAssigned(gCtx); err != nil {
							return err
						}
					}

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

				if entry != nil {
					entry.IDAssigned()
				}
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
//   - error: If processing fails or context is cancelled
//
// Native catch-up is outside the quick window by design (spec section 11), and this async
// variant is the catch-up path only, so it takes no window entry: nothing here registers,
// waits on a predecessor or signals.
func (u *BlockValidation) processBlockSubtreesPipelineAsync(ctx context.Context, block *model.Block, prefetchDepth int, writeJobsChan chan<- *SubtreeWriteJob, outpointOnly bool) (uint64, error) {
	numSubtrees := len(block.Subtrees)
	block.SubtreeSlices = make([]*subtreepkg.Subtree, numSubtrees)
	var existingBlockID uint64
	blockIDSet := false

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
				err := u.buildSubtreeJobsForBatch(batchCtx, block, batch, writeJobsChan)
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
		return 0, err
	}

	return u.validateSubtrees(ctx, block, existingBlockID)
}

// validateSubtrees validates subtree sizes and merkle root after processing.
func (u *BlockValidation) validateSubtrees(ctx context.Context, block *model.Block, existingBlockID uint64) (uint64, error) {
	// Validate subtree sizes
	subtreeSize := 0
	for i := 0; i < len(block.SubtreeSlices)-1; i++ {
		if i == 0 {
			subtreeSize = block.SubtreeSlices[i].Length()
		} else if block.SubtreeSlices[i].Length() != subtreeSize {
			return 0, errors.NewProcessingError("[validateSubtrees][%s] subtree %d size mismatch", block.Hash().String(), i)
		}
	}

	// Verify merkle root
	if err := block.CheckMerkleRoot(ctx); err != nil {
		return 0, errors.NewProcessingError("[validateSubtrees][%s] merkle root mismatch", block.Hash().String(), err)
	}

	return existingBlockID, nil
}

// readSubtree reads a single subtree from disk and validates its transactions.
func (u *BlockValidation) readSubtree(ctx context.Context, block *model.Block, subtreeIdx int, subtreeHash *chainhash.Hash) subtreeResult {
	// On retry the subtree may already be promoted to FileTypeSubtree (the
	// "already validated" marker) and FileTypeSubtreeToCheck cleaned up, so
	// consult both file types — see findLocalSubtreeFile.
	localFileType, localExists, err := findLocalSubtreeFile(ctx, u.subtreeStore, *subtreeHash)
	if err != nil {
		return subtreeResult{err: errors.NewStorageError("[getBlockTransactions][%s] failed to locate subtree %s", block.Hash().String(), subtreeHash.String(), err)}
	}
	if !localExists {
		return subtreeResult{err: errors.NewNotFoundError("[getBlockTransactions][%s] subtree %s not found locally", block.Hash().String(), subtreeHash.String())}
	}
	subtreeReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash[:], localFileType)
	if err != nil {
		return subtreeResult{err: errors.NewNotFoundError("[getBlockTransactions][%s] failed to get subtree %s", block.Hash().String(), subtreeHash.String(), err)}
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
				return subtreeResult{err: errors.NewNotFoundError("[getBlockTransactions][%s] failed to re-open subtree %s for heap fallback", block.Hash().String(), subtreeHash.String(), ferr)}
			}
			defer fallbackReader.Close()

			bufferedReader.Reset(fallbackReader)
			subtree, err = subtreepkg.NewSubtreeFromReader(bufferedReader)
		}
	} else {
		subtree, err = subtreepkg.NewSubtreeFromReader(bufferedReader)
	}
	if err != nil {
		return subtreeResult{err: errors.NewProcessingError("[getBlockTransactions][%s] failed to deserialize subtree %s", block.Hash().String(), subtreeHash.String(), err)}
	}

	// get the subtree data from disk
	subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return subtreeResult{err: errors.NewNotFoundError("[getBlockTransactions][%s] failed to get subtree data %s", block.Hash().String(), subtreeHash.String(), err)}
	}
	defer subtreeDataReader.Close()

	// Reuse the same pooled reader for subtree data
	bufferedReader.Reset(subtreeDataReader)

	// the subtree data reader will make sure the data matches the transaction ids from the subtree
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
		}
	}

	// Check if full .subtree file already exists (for retry scenarios). If the
	// reader above already pulled from FileTypeSubtree we know it's present
	// without another store round-trip.
	fullSubtreeExists := localFileType == fileformat.FileTypeSubtree
	if !fullSubtreeExists {
		fullSubtreeExists, _ = u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	}

	return subtreeResult{
		subtree:           subtree,
		subtreeData:       subtreeData,
		subtreeHash:       *subtreeHash,
		subtreeIdx:        subtreeIdx,
		fullSubtreeExists: fullSubtreeExists,
	}
}

// writeSubtreeFilesFromTxs writes the full subtree file to disk.
// Takes transactions directly (without coinbase nil entry).
// Note: Subtree meta files (.subtreemeta) are intentionally skipped during quick validation
// for performance. They will be generated on-demand if needed later.
func (u *BlockValidation) writeSubtreeFilesFromTxs(ctx context.Context, block *model.Block, subtreeIdx int, subtree *subtreepkg.Subtree, txs []*bt.Tx, subtreeHash chainhash.Hash, fullSubtreeExists, outpointOnly bool) error {
	// fullSubtreeExists was already computed during prefetch (readSubtree); reuse it
	// instead of issuing another subtreeStore.Exists round-trip here.
	if !fullSubtreeExists {
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
		fullSubtreeBytes, err := u.subtreeStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		if err != nil {
			return errors.NewNotFoundError("[writeSubtreeFilesFromTxs][%s] failed to get full subtree %s", block.Hash().String(), subtreeHash.String(), err)
		}

		fullSubtree, err := u.newSubtreeFromBytes(fullSubtreeBytes)
		if err != nil {
			return errors.NewProcessingError("[writeSubtreeFilesFromTxs][%s] failed to deserialize full subtree %s", block.Hash().String(), subtreeHash.String(), err)
		}

		block.SubtreeSlices[subtreeIdx] = fullSubtree

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

	// hasInBlockParent is parallel to batchTxs: true when at least one of that transaction's
	// inputs spends an output of another transaction of THIS block. The extend stage already
	// walks every input against the per-block extendedTxs map to fill in parent satoshis and
	// scripts, so the answer costs nothing extra; it is recorded here rather than recomputed.
	//
	// It is per batch, never per block: a block may hold a billion transactions, the batch is
	// the bounded unit, and this slice dies with the batch.
	//
	// The predicate is in-BLOCK, not in-batch: the extendedTxs map it is derived from
	// accumulates across every batch of the block, so a child whose parent landed in an
	// EARLIER batch is classified as chained even though that parent's create has long since
	// committed. That is conservative rather than wrong — batches are consumed strictly
	// serially, so such a child could safely take the one-wave path — and it costs only the
	// transactions that straddle a batch boundary. The whole argument rests on one block being
	// applied at a time; work that overlaps blocks has to revisit it, because a parent from a
	// concurrently-applied block would not be in this map at all.
	//
	// A short or nil slice means "not derived", and txIsIndependent then answers false — a
	// transaction of unknown provenance takes the conservative two-phase path. That is what
	// keeps a hand-built batch (tests, and any future caller that fills batchTxs directly)
	// behaving exactly as it did before this field existed.
	hasInBlockParent []bool

	// fullSubtreeExists tracks which subtrees already have full .subtree files
	// Populated during prefetch to avoid disk I/O during build phase
	fullSubtreeExists []bool

	// batchStart is the global starting index in block.Subtrees
	batchStart int

	// batchEnd is the global ending index (exclusive) in block.Subtrees
	batchEnd int

	// outpointOnly is the below-checkpoint fast-path mode for this block, computed ONCE
	// per block in the quickValidate entry point and threaded down (never re-derived per
	// seam). Consumers read this field so every phase — decorate, fee, create, spend —
	// sees a single consistent decision and cannot drift. See quickValidateOutpointOnly.
	outpointOnly bool

	// window is the quick-window entry this batch belongs to, nil outside the window.
	window *windowEntry

	// gate releases spends of this batch's transactions by in-flight successor blocks. Closed
	// once both create waves have returned (a returned store call is a committed one). nil for
	// a batch with no transactions: nothing can ever wait on it.
	gate *batchGate

	// waitGates are predecessor gates this batch's chained spends must wait on. Derived once,
	// in the single-goroutine partition loop of createAndSpendUTXOsForBatch.
	waitGates []*batchGate
}

// windowOf returns the window this batch's entry belongs to, nil outside the window. Read from
// the entry rather than from the BlockValidation so a batch can never be matched against a
// different window than the one that issued its gate.
func (b *SubtreeProcessingBatch) windowOf() *quickWindow {
	if b == nil || b.window == nil {
		return nil
	}

	return b.window.w
}

// registerBatchWithWindow claims this batch's transaction ids for entry and attaches the gate
// that releases in-flight successors' spends of them. A batch with no transactions claims
// nothing and gets no gate.
func registerBatchWithWindow(entry *windowEntry, batch *SubtreeProcessingBatch) error {
	if entry == nil {
		return nil
	}

	batch.window = entry

	if len(batch.batchTxs) == 0 {
		return nil
	}

	txids := make([]chainhash.Hash, 0, len(batch.batchTxs))
	for _, tx := range batch.batchTxs {
		txids = append(txids, *tx.TxIDChainHash())
	}

	gate, err := entry.RegisterBatch(txids)
	if err != nil {
		return err
	}

	batch.gate = gate

	return nil
}

// appendGateOnce adds gate unless it is already in gates. The list holds at most one gate per
// in-flight predecessor batch, so the linear scan is over a handful of entries.
func appendGateOnce(gates []*batchGate, gate *batchGate) []*batchGate {
	for _, g := range gates {
		if g == gate {
			return gates
		}
	}

	return append(gates, gate)
}

// txIsIndependent reports whether the transaction at index i of batchTxs spends no output of
// any transaction of this block, and so can have its inputs spent and its outputs created in
// one call: nothing in the block has to be created first for its spend to find a coin.
//
// Out of range, or a batch whose partition was never derived, answers false. Unknown means
// chained, which is the path that was always taken.
func (b *SubtreeProcessingBatch) txIsIndependent(i int) bool {
	if i < 0 || i >= len(b.hasInBlockParent) {
		return false
	}

	return !b.hasInBlockParent[i]
}

// Close releases mmap-backed subtree resources in this batch.
func (b *SubtreeProcessingBatch) Close() {
	for _, st := range b.subtrees {
		if st != nil {
			st.Close()
		}
	}
}

// extendTxFromSameBlockParents extends tx's inputs whose parents are present in
// parents (same-block parents already decoded in this batch), returning whether
// any input still needs an external (store) lookup and whether any input found
// its parent in this block.
//
// The second answer is free here — the loop already asks the map about every
// input — and it is what lets createAndSpendUTXOsForBatch apply a transaction
// with no in-block parent in a single spend-and-create call.
//
// PreviousTxOutIndex comes from the untrusted child tx, so the parent's output
// count is bounds-checked before indexing: an out-of-range reference returns an
// error instead of panicking the node with index-out-of-range (issue 1283). This
// mirrors the guard in services/validator/Validator.go extendTransaction.
func extendTxFromSameBlockParents(tx *bt.Tx, parents map[chainhash.Hash]*bt.Tx) (needsExternalLookup, hasSameBlockParent bool, err error) {
	for j, input := range tx.Inputs {
		parentHash := input.PreviousTxIDChainHash()

		parentTx, ok := parents[*parentHash]
		if !ok {
			needsExternalLookup = true
			continue
		}

		hasSameBlockParent = true

		vout := input.PreviousTxOutIndex
		if parentTx.Outputs == nil || int(vout) >= len(parentTx.Outputs) {
			return false, false, errors.NewProcessingError("tx %s input %d references non-existent output %d of same-block parent %s",
				tx.TxIDChainHash().String(), j, vout, parentHash.String())
		}

		tx.Inputs[j].PreviousTxSatoshis = parentTx.Outputs[vout].Satoshis
		tx.Inputs[j].PreviousTxScript = parentTx.Outputs[vout].LockingScript
	}

	return needsExternalLookup, hasSameBlockParent, nil
}

// hasSameBlockParentInput reports whether any of tx's inputs spends an output of a
// transaction of this block.
//
// A transaction that arrives already extended never enters extendTxFromSameBlockParents, so
// it would otherwise have no answer at all, and "no answer" means chained — the slow path,
// taken for every transaction of a block whose subtree data was written extended. This scan
// gives it the real answer for the price of one map lookup per input.
func hasSameBlockParentInput(tx *bt.Tx, parents map[chainhash.Hash]*bt.Tx) bool {
	for _, input := range tx.Inputs {
		if _, ok := parents[*input.PreviousTxIDChainHash()]; ok {
			return true
		}
	}

	return false
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

	go func() {
		_ = g.Wait()
		for _, ch := range subtreeChannels {
			close(ch)
		}
	}()

	// Phase 2: Collect results and extend same-block parents
	txsNeedingExtension := make([]*bt.Tx, 0)

	for i := 0; i < batchSize; i++ {
		result, ok := <-subtreeChannels[i]
		if !ok {
			cancelReaders()
			return nil, errors.NewProcessingError("[processSubtreeBatch][%s] channel %d closed", block.Hash().String(), batchStart+i)
		}
		if result.err != nil {
			cancelReaders()
			return nil, result.err
		}

		batch.subtrees[i] = result.subtree
		batch.subtreeData[i] = result.subtreeData
		batch.subtreeHashes[i] = result.subtreeHash

		startIdx := len(batch.batchTxs)
		for _, tx := range result.subtreeData.Txs {
			if tx == nil {
				continue // skip coinbase
			}

			// Try to extend from same-block parents first
			inBlockParent := false

			if !tx.IsExtended() {
				needsExternalLookup, hasSameBlockParent, extendErr := extendTxFromSameBlockParents(tx, extendedTxsFromPrevBatches)
				if extendErr != nil {
					cancelReaders()
					return nil, errors.NewProcessingError("[processSubtreeBatch][%s] same-block parent extension failed", block.Hash().String(), extendErr)
				}

				inBlockParent = hasSameBlockParent

				if needsExternalLookup {
					txsNeedingExtension = append(txsNeedingExtension, tx)
				}
			} else {
				inBlockParent = hasSameBlockParentInput(tx, extendedTxsFromPrevBatches)
			}

			batch.hasInBlockParent = append(batch.hasInBlockParent, inBlockParent)

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
			cancelReaders()
			return nil, errors.NewProcessingError("[processSubtreeBatch][%s] failed to extend transactions: %v", block.Hash().String(), err)
		}
	}

	cancelReaders()
	return batch, nil
}

// quickValidateCreateLimit returns the caller concurrency for each of quick
// validation's two creating waves: the combined one-wave apply and the chained
// create wave of createAndSpendUTXOsForBatch. Those two now run at the same
// time, each bounded by this number, so the peak callers a batch puts on the
// store is up to twice what this returns. The one-wave apply also spends as it
// creates, so this is no longer a bound taken before any input is spent.
//
// This is a callers-in-flight limit, not a store-statements-in-flight limit:
// each caller blocks until the batch it lands in commits, so the number of
// statements actually in flight to the store is (callers / batch size).
//
// blockvalidation_quick_validate_create_concurrency, when set above 0, is used
// directly. Otherwise the limit is derived from the UTXO store's own batcher
// settings: StoreBatcherSize * BatcherMaxConcurrent, so every batcher worker
// can have a full batch of callers queued behind it, floored at
// StoreBatcherSize * 8 for a store whose BatcherMaxConcurrent is 0 (unbounded
// workers) — the flat multiplier the derivation replaces.
func quickValidateCreateLimit(bv *settings.BlockValidationSettings, store *settings.UtxoStoreSettings) int {
	if bv.QuickValidateCreateConcurrency > 0 {
		return bv.QuickValidateCreateConcurrency
	}

	derived := store.StoreBatcherSize * store.BatcherMaxConcurrent
	floor := store.StoreBatcherSize * 8

	if derived > floor {
		return derived
	}

	return floor
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

	// Partition the batch. A transaction whose inputs all come from outside this block has
	// nothing in the block to wait for: its parents' coins are already committed, so its
	// spends and its creates can go to the store in ONE call. A transaction that spends a
	// sibling still needs the two waves, because that sibling's create has to commit first.
	//
	// A transaction whose create is skipped as unspendable also takes the two-wave path,
	// whatever its parents: skipping the create there means a spend-only call, which the
	// combined call is not.
	independent := make([]txApply, 0, len(batch.batchTxs))

	var (
		chainedCreates []txApply
		chainedSpends  []*bt.Tx
		// chainedCount and unspendable measure different axes (parentage and
		// spendability) and overlap: a chained tx with only unspendable outputs
		// counts in both, so independent+chained+unspendable can exceed the batch.
		chainedCount int
		unspendable  int
		// dependent counts transactions routed to the chained bucket ONLY because one of
		// their inputs spends a coin an in-flight predecessor block is still creating.
		dependent int
	)

	// nil outside the quick window, which makes every window step below a no-op.
	w := batch.windowOf()

	batchSize := batch.batchEnd - batch.batchStart
	for i := 0; i < batchSize; i++ {
		globalSubtreeIdx := batch.batchStart + i
		txRange := batch.txRanges[i]

		for txIdx := txRange[0]; txIdx < txRange[1]; txIdx++ {
			tx := batch.batchTxs[txIdx]
			item := txApply{tx: tx, subtreeIdx: globalSubtreeIdx}

			// An unspendable transaction is not written to the store at all; its inputs are
			// still spent, so it joins the spend wave and no create wave.
			skipCreate := shouldSkipUnspendableCreate(lockUTXOs, u.settings, tx, block.Height)
			if skipCreate {
				unspendable++
			}

			// Every input of every transaction is checked, chained or not: a transaction with
			// no in-block parent can still spend a coin an in-flight PREDECESSOR block is
			// creating, and that coin is not in the store until that block's gate closes.
			waitsOnPredecessor := false

			if w != nil {
				for _, in := range tx.Inputs {
					if g := w.GateFor(batch.window, in.PreviousTxIDChainHash()); g != nil {
						waitsOnPredecessor = true
						batch.waitGates = appendGateOnce(batch.waitGates, g)
					}
				}
			}

			if waitsOnPredecessor {
				dependent++
			}

			if !batch.txIsIndependent(txIdx) {
				chainedCount++
			} else if !waitsOnPredecessor && !skipCreate {
				independent = append(independent, item)
				continue
			}

			if !skipCreate {
				chainedCreates = append(chainedCreates, item)
			}

			chainedSpends = append(chainedSpends, tx)
		}
	}

	// Track transactions that already exist so we can update their mined info. Both waves
	// feed the same list, and it is stamped once, after both.
	var (
		existingTxsMu    sync.Mutex
		existingTxHashes []*chainhash.Hash
	)

	collectExisting := func(tx *bt.Tx) {
		txHash := tx.TxIDChainHash()

		existingTxsMu.Lock()
		existingTxHashes = append(existingTxHashes, txHash)
		existingTxsMu.Unlock()
	}

	// This limit bounds callers, not store statements: each caller blocks until the batch it
	// lands in commits, so statements actually in flight to the store is (callers / batch size).
	// A flat 8x StoreBatcherSize therefore capped every store at 8 statements in flight no
	// matter how many batcher workers it runs — with a 50-row batch and 64 workers that is 8
	// statements where the store could sustain far more. quickValidateCreateLimit derives the
	// caller count from the store's own batcher settings instead, so every worker can have a
	// full batch of callers queued behind it.
	createLimit := quickValidateCreateLimit(&u.settings.BlockValidation, &u.settings.UtxoStore)

	var oneWaveDuration, chainedDuration time.Duration

	// Closed ONLY when the one-wave apply has succeeded. The chained spend wave waits on it
	// as well as on its own create wave, and that is a correctness requirement, not caution:
	// a chained transaction may spend the output of an INDEPENDENT sibling, whose create
	// commits in the one-wave apply. Spending it before that commit would find no coin.
	//
	// Closing it on failure too — with a defer, say — would be a real regression. The
	// errgroup records the error and cancels gCtx only AFTER the goroutine's function has
	// returned, so a defer would leave a window in which both select arms are ready and the
	// choice between them is random: the chained spend wave could run in full against a
	// partially applied one-wave. The old two-phase code guaranteed no spend at all after a
	// create-phase failure, and that guarantee is kept here by never releasing the barrier on
	// a failure and by re-checking gCtx on both sides of the select.
	//
	// The waves still start together, so the wait costs nothing whenever the chained create
	// wave is the slower of the two, which is the case whenever chained transactions dominate.
	oneWaveDone := make(chan struct{})

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		start := time.Now()
		defer func() { oneWaveDuration = time.Since(start) }()

		if err := u.applyOneWave(gCtx, block, independent, lockUTXOs, outpointOnly, createLimit, batch.window, collectExisting); err != nil {
			return err
		}

		close(oneWaveDone)

		return nil
	})

	g.Go(func() error {
		start := time.Now()
		defer func() { chainedDuration = time.Since(start) }()

		if err := u.createWave(gCtx, block, chainedCreates, lockUTXOs, outpointOnly, createLimit, w, collectExisting); err != nil {
			return err
		}

		if err := gCtx.Err(); err != nil {
			return err
		}

		select {
		case <-oneWaveDone:
		case <-gCtx.Done():
			return gCtx.Err()
		}

		if err := gCtx.Err(); err != nil {
			return err
		}

		// Both creating waves have returned without error, and a returned store call is a
		// committed one, so from this point an in-flight successor may spend this batch's
		// coins. The gate closes BEFORE this batch waits on its own predecessors, so a
		// successor is never held up by a wait this batch is itself doing.
		if batch.gate != nil {
			batch.gate.Close()
		}

		// No caller slot is held here: waiting on a predecessor while holding one would let a
		// block ahead of us starve for the budget it needs to close the very gate we want.
		if len(batch.waitGates) > 0 {
			gateWaitStart := time.Now()

			for _, gate := range batch.waitGates {
				if err := gate.Wait(gCtx); err != nil {
					return err
				}
			}

			prometheusBlockValidationQuickWindowGateWait.Observe(time.Since(gateWaitStart).Seconds())
			prometheusBlockValidationQuickWindowGateWaits.Inc()
		}

		// Spend the chained transactions, retrying transient store errors the way the
		// legacy path does (services/legacy/netsync PreValidateTransactions). Unlike
		// legacy, conflicts are NOT tolerated here: legacy runs the validator with
		// WithCreateConflicting so block assembly's ProcessConflicting later reconciles
		// the loser, but the quick path never writes conflicting subtree nodes and has
		// no such resolver — tolerating ErrTxConflicting would leave an output's spend
		// permanently attributed to a non-canonical tx. Hard-fail instead (fail-closed).
		// Dirty-restart replay does not need conflict tolerance: re-spending an output
		// with the same spender is the store's idempotent success path.
		return u.spendBatchWithRetry(gCtx, block, chainedSpends, outpointOnly, batch.window)
	})

	applyErr := g.Wait()

	// Logged on the failure path too: when a batch fails, how long each wave ran for is the
	// first thing anyone reading the log wants.
	u.logger.Infof("[createAndSpendUTXOsForBatch][%s] batch %d-%d applied: independent=%d chained=%d dependent=%d unspendable=%d existing=%d (onewave=%v, chained=%v, err=%v)",
		block.Hash().String(), batch.batchStart, batch.batchEnd, len(independent), chainedCount, dependent, unspendable, len(existingTxHashes), oneWaveDuration, chainedDuration, applyErr)

	if applyErr != nil {
		return applyErr
	}

	// Update mined info for transactions that already existed. This handles the case where a
	// previous attempt created UTXOs with a different block ID. Chunked via the shared helper
	// (issue 936): on a fat-batch retry every tx in the batch already exists, and a single
	// unchunked SetMinedMulti call would overrun the aerospike client connection pool.
	if len(existingTxHashes) > 0 {
		minedBlockInfo := utxo.MinedBlockInfo{
			BlockID:     block.ID,
			BlockHeight: block.Height,
		}

		if err := utxo.SetMinedMultiChunked(ctx, u.logger, u.utxoStore, existingTxHashes, minedBlockInfo,
			u.settings.UtxoStore.MaxMinedBatchSize, u.settings.UtxoStore.MaxMinedRoutines); err != nil {
			return errors.NewProcessingError("[createAndSpendUTXOsForBatch][%s] failed to update mined info for %d existing txs", block.Hash().String(), len(existingTxHashes), err)
		}
	}

	return nil
}

// txApply is one transaction a UTXO wave has to apply, carrying the subtree index its mined
// block info records. The index belongs to the transaction, not to the goroutine, which is why
// it travels with it rather than being closed over.
type txApply struct {
	tx         *bt.Tx
	subtreeIdx int
}

// applyOneWave applies transactions that spend nothing created in this block: their inputs are
// spent and their outputs created by ONE store call each, so the batch pays for one round of
// statements instead of two.
//
// Neither WithCreateOnly nor WithSpendOnly is passed, which is what makes the call combined.
// WithIgnoreLocked matches the spend wave it replaces, and both outpoint-only flags are passed
// because the one call now does the work both phases used to.
//
// ErrTxExists is handled exactly as the create wave handles it: the transaction is collected
// for the mined-info stamp and the call counts as done. That is safe because the store leaves
// the spends of an existing transaction in place rather than rolling them back — verified
// against the utxoset store, whose runSpendAndCreateBatch commits the spend phase and then
// reports the create's ErrTxExists, and against utxo.SequentialSpendAndCreate, which returns
// the same error with its spends still applied.
func (u *BlockValidation) applyOneWave(ctx context.Context, block *model.Block, items []txApply,
	lockUTXOs, outpointOnly bool, limit int, owner *windowEntry, collectExisting func(*bt.Tx)) error {
	return u.applyTxsWithRetry(ctx, block, "applyOneWave", items, limit, owner, func(ctx context.Context, item txApply) error {
		_, _, err := u.utxoStore.SpendAndCreate(ctx, item.tx, block.Height,
			utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
				BlockID:     block.ID,
				BlockHeight: block.Height,
				SubtreeIdx:  item.subtreeIdx,
			}),
			utxo.WithLocked(lockUTXOs),
			utxo.WithIgnoreLocked(true),
			utxo.WithSkipExtendedInputs(outpointOnly),
			utxo.WithSkipUTXOHashCheck(outpointOnly))
		if err != nil {
			if errors.Is(err, errors.ErrTxExists) {
				collectExisting(item.tx)
				return nil
			}

			if errors.IsRetryableError(err) {
				return err
			}

			return errors.NewProcessingError("[createAndSpendUTXOsForBatch][%s] failed to apply tx %s", block.Hash().String(), item.tx.TxIDChainHash().String(), err)
		}

		return nil
	})
}

// createWave creates UTXOs in parallel for transactions that spend a sibling, collecting any
// that already exist. This is the first of the two waves such a transaction still needs, and
// it is unchanged from when every transaction took it.
func (u *BlockValidation) createWave(ctx context.Context, block *model.Block, items []txApply,
	lockUTXOs, outpointOnly bool, limit int, w *quickWindow, collectExisting func(*bt.Tx)) error {
	if len(items) == 0 {
		return nil
	}

	createG, createCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(u.logger, createG, limit)

	for _, item := range items {
		createG.Go(func() error {
			// The per-wave limit above is this block's ceiling; the window's budget is the
			// ceiling shared by every block in flight, so both apply.
			if w != nil {
				if err := w.AcquireCaller(createCtx); err != nil {
					return errors.NewServiceError("[createAndSpendUTXOsForBatch][%s] failed to acquire a quick-window caller slot for tx %s", block.Hash().String(), item.tx.TxIDChainHash().String(), err)
				}

				defer w.ReleaseCaller()
			}

			_, _, err := u.utxoStore.SpendAndCreate(createCtx, item.tx, block.Height, utxo.WithCreateOnly(),
				utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
					BlockID:     block.ID,
					BlockHeight: block.Height,
					SubtreeIdx:  item.subtreeIdx,
				}), utxo.WithLocked(lockUTXOs), utxo.WithSkipExtendedInputs(outpointOnly))
			if err != nil {
				if errors.Is(err, errors.ErrTxExists) {
					// Transaction already exists - collect it for mined info update
					collectExisting(item.tx)
					return nil
				}

				return errors.NewProcessingError("[createAndSpendUTXOsForBatch][%s] failed to create UTXO for tx %s", block.Hash().String(), item.tx.TxIDChainHash().String(), err)
			}

			return nil
		})
	}

	return createG.Wait()
}

// spendRetryBackoffDefault is the pause between spend retry attempts. Matches the
// legacy path's retryBackoff (services/legacy/netsync PreValidateTransactions).
const spendRetryBackoffDefault = 2 * time.Second

// spentWithoutConflict reports whether err is a "utxo already spent" that names no spending
// transaction. One that DOES name a spender is a genuine conflict and keeps its own hard-fail:
// only the anonymous form is the shape a missed gate produces.
//
// Every store in this tree builds a real double spend through errors.NewUtxoSpentError with a
// non-nil SpendingData, so "no spender named" really does exclude them: sql (sql.go, five call
// sites, including the concurrently-spent branch that substitutes the attempted spend data when
// the winner is unknown), aerospike (spend.go), and the shared duplicate_spend.go rejection that
// both go through; nullstore never raises it at all. A future store that reported ErrSpent
// with an empty SpendingData would be treated as a possible gate miss, which is the fail-closed
// direction: a local fault and a retry rather than a peer ban.
func spentWithoutConflict(err error) bool {
	if !errors.Is(err, errors.ErrSpent) {
		return false
	}

	var spent *errors.UtxoSpentErrData
	if errors.AsData(err, &spent) && spent != nil && spent.SpendingData != nil {
		return false
	}

	return true
}

// windowMissSuspects returns the input parents the error actually points at.
//
// An ErrSpent carries the outpoint in structured data (UtxoSpentErrData.Hash is the parent
// whose output was spent), so it names exactly one. ErrTxNotFound carries nothing structured —
// the SQL store spells it "output <txid>:<vout> not found" in the message and nothing else — so
// the parents are picked out by looking for their txid in the error text, which any store that
// reports the outpoint at all must contain.
//
// An error that names no parent this transaction spends falls back to every parent. That keeps
// the backstop working against a store whose wording we cannot read, at the cost of the wider
// any-registered-parent rule for those.
func windowMissSuspects(tx *bt.Tx, err error) []*chainhash.Hash {
	var spent *errors.UtxoSpentErrData
	if errors.AsData(err, &spent) && spent != nil {
		named := spent.Hash

		return []*chainhash.Hash{&named}
	}

	text := err.Error()
	named := make([]*chainhash.Hash, 0, 1)

	for _, in := range tx.Inputs {
		if parent := in.PreviousTxIDChainHash(); strings.Contains(text, parent.String()) {
			named = append(named, parent)
		}
	}

	if len(named) > 0 {
		return named
	}

	all := make([]*chainhash.Hash, 0, len(tx.Inputs))
	for _, in := range tx.Inputs {
		all = append(all, in.PreviousTxIDChainHash())
	}

	return all
}

// windowMissError reclassifies a spend that found no coin although ANOTHER in-flight block had
// registered the parent transaction with the window. That combination cannot be the peer's
// fault: the parent is claimed by a block we are applying right now, so a gate should have held
// this spend back and did not. It is counted and returned as a local service fault, so legacy
// sync retries the delivery instead of banning the peer that made it. Every other failure is
// returned untouched.
//
// owner is the calling block's own entry, and it is excluded: a parent this block registered
// itself was never behind a gate (in-block parents take the two-phase path), so a miss on one of
// them is this block's own hard failure. Reclassifying it would turn a genuine block failure
// into an endless local re-request and would move the miss counter, whose ship gate is zero.
func windowMissError(block *model.Block, w *quickWindow, owner *windowEntry, tx *bt.Tx, err error) error {
	if w == nil || tx == nil {
		return err
	}

	if !errors.Is(err, errors.ErrTxNotFound) && !spentWithoutConflict(err) {
		return err
	}

	for _, parent := range windowMissSuspects(tx, err) {
		if !w.Registered(owner, parent) {
			continue
		}

		prometheusBlockValidationQuickWindowMissTotal.Inc()

		return errors.NewServiceError("[quickWindow][%s] spend of %s found no coin although parent %s was registered by an in-flight block; gate miss, failing the block as a local fault", block.Hash().String(), tx.TxIDChainHash().String(), parent.String(), err)
	}

	return err
}

// applyTxsWithRetry applies every item in parallel, bounded by limit, with bounded retries.
// Per attempt: an apply that returns a retryable error (transient store overload) queues its
// item for the next attempt; anything else — including ErrTxConflicting and ErrSpent — fails
// hard immediately (fail-closed: the quick path has no ProcessConflicting pipeline to
// reconcile a tolerated conflict). Gives up early when an attempt makes no progress. Retry
// cadence mirrors the legacy path's PreValidateTransactions (maxRetries=10, 2s backoff) so
// both below-checkpoint implementations converge identically after dirty restarts.
//
// apply decides what a failure means: it returns the store's error unwrapped when the item
// should be retried, and a wrapped one when the block should fail. Two waves share this loop —
// the combined one-wave apply and the spend wave — and both need exactly this behaviour, so it
// lives here once. stage names the caller in the log and error text.
//
// owner is the calling block's window entry, nil outside the window: it supplies both the
// window whose caller budget the waves share and the identity the miss backstop excludes.
func (u *BlockValidation) applyTxsWithRetry(ctx context.Context, block *model.Block, stage string,
	items []txApply, limit int, owner *windowEntry, apply func(context.Context, txApply) error) error {
	const maxRetries = 10

	w := owner.windowOf()

	if len(items) == 0 {
		return nil
	}

	backoff := u.spendRetryBackoff
	if backoff <= 0 {
		backoff = spendRetryBackoffDefault
	}

	pending := items
	total := len(items)

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return errors.NewProcessingError("[%s][%s] context cancelled", stage, block.Hash().String())
		}

		if attempt > 0 {
			u.logger.Infof("[%s][%s] retry %d/%d: %d of %d transactions remaining", stage, block.Hash().String(), attempt, maxRetries, len(pending), total)
			time.Sleep(backoff)
		}

		applyG, applyCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(u.logger, applyG, limit)

		var (
			mu        sync.Mutex
			retryable []txApply
			lastErr   error
			hardFail  error
		)

		for _, item := range pending {
			applyG.Go(func() error {
				// The per-wave limit above is this block's ceiling; the window's budget is the
				// ceiling shared by every block in flight, so both apply.
				if w != nil {
					if err := w.AcquireCaller(applyCtx); err != nil {
						mu.Lock()
						hardFail = errors.NewServiceError("[%s][%s] failed to acquire a quick-window caller slot for tx %s", stage, block.Hash().String(), item.tx.TxIDChainHash().String(), err)
						mu.Unlock()

						return nil
					}

					defer w.ReleaseCaller()
				}

				if err := apply(applyCtx, item); err != nil {
					if errors.IsRetryableError(err) {
						mu.Lock()
						retryable = append(retryable, item)
						lastErr = err
						mu.Unlock()

						return nil
					}

					mu.Lock()
					hardFail = windowMissError(block, w, owner, item.tx, err)
					mu.Unlock()
				}

				return nil
			})
		}

		_ = applyG.Wait()

		if hardFail != nil {
			return hardFail
		}

		if len(retryable) == 0 {
			if attempt > 0 {
				u.logger.Infof("[%s][%s] all %d transactions applied after %d retries", stage, block.Hash().String(), total, attempt)
			}

			return nil
		}

		if attempt > 0 && len(retryable) >= len(pending) {
			return errors.NewProcessingError("[%s][%s] %d of %d applies failed with no progress, giving up", stage, block.Hash().String(), len(retryable), total, lastErr)
		}

		pending = retryable
	}

	return errors.NewProcessingError("[%s][%s] %d of %d applies still failing after %d retries", stage, block.Hash().String(), len(pending), total, maxRetries)
}

// spendBatchWithRetry spends txs in parallel with bounded retries, over the shared retry loop.
// It is the second wave for transactions that spend a sibling of the same block; transactions
// with no in-block parent never reach it, because applyOneWave has already spent them.
func (u *BlockValidation) spendBatchWithRetry(ctx context.Context, block *model.Block, txs []*bt.Tx, outpointOnly bool, owner *windowEntry) error {
	items := make([]txApply, len(txs))
	for i, tx := range txs {
		items[i] = txApply{tx: tx}
	}

	limit := u.settings.UtxoStore.SpendBatcherSize * u.settings.UtxoStore.SpendBatcherConcurrency * 2

	return u.applyTxsWithRetry(ctx, block, "spendBatchWithRetry", items, limit, owner, func(ctx context.Context, item txApply) error {
		if _, _, err := u.utxoStore.SpendAndCreate(ctx, item.tx, block.Height, utxo.WithSpendOnly(),
			utxo.WithIgnoreLocked(true), utxo.WithSkipUTXOHashCheck(outpointOnly)); err != nil {
			if errors.IsRetryableError(err) {
				return err
			}

			return errors.NewProcessingError("[spendBatchWithRetry][%s] failed to spend tx %s", block.Hash().String(), item.tx.TxIDChainHash().String(), err)
		}

		return nil
	})
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
		fullSubtreeExists := batch.fullSubtreeExists[localIdx]

		writeG.Go(func() error {
			return u.writeSubtreeFilesFromTxs(writeCtx, block, globalIdx, subtree, subtreeTxs, subtreeHash, fullSubtreeExists, batch.outpointOnly)
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
//
// Returns:
//   - error: If building subtrees fails or context is cancelled
func (u *BlockValidation) buildSubtreeJobsForBatch(ctx context.Context, block *model.Block, batch *SubtreeProcessingBatch, writeJobsChan chan<- *SubtreeWriteJob) error {
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
		fullSubtreeExists := batch.fullSubtreeExists[localIdx]

		buildG.Go(func() error {
			job, err := u.buildSubtreeAndQueueWrite(buildCtx, block, globalIdx, subtree, subtreeTxs, subtreeHash, fullSubtreeExists, batch.outpointOnly)
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

	// Queue all jobs to the channel (non-blocking with context check)
	for _, job := range jobs {
		if job == nil {
			continue
		}
		select {
		case writeJobsChan <- job:
		case <-ctx.Done():
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
		subtrees:          make([]*subtreepkg.Subtree, batchSize),
		subtreeData:       make([]*subtreepkg.Data, batchSize),
		subtreeHashes:     make([]chainhash.Hash, batchSize),
		txRanges:          make([][2]int, batchSize),
		batchTxs:          make([]*bt.Tx, 0),
		fullSubtreeExists: make([]bool, batchSize),
		batchStart:        batchStart,
		batchEnd:          batchEnd,
		outpointOnly:      outpointOnly,
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

	go func() {
		_ = g.Wait()
		for _, ch := range subtreeChannels {
			close(ch)
		}
	}()

	// Collect results (no extension yet)
	for i := 0; i < batchSize; i++ {
		result, ok := <-subtreeChannels[i]
		if !ok {
			cancelReaders()
			return nil, errors.NewProcessingError("[prefetchSubtreeBatch][%s] channel %d closed", block.Hash().String(), batchStart+i)
		}
		if result.err != nil {
			cancelReaders()
			return nil, result.err
		}

		batch.subtrees[i] = result.subtree
		batch.subtreeData[i] = result.subtreeData
		batch.subtreeHashes[i] = result.subtreeHash
		batch.fullSubtreeExists[i] = result.fullSubtreeExists
	}

	cancelReaders()
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

			// Try to extend from same-block parents first
			inBlockParent := false

			if !tx.IsExtended() {
				needsExternalLookup, hasSameBlockParent, extendErr := extendTxFromSameBlockParents(tx, extendedTxs)
				if extendErr != nil {
					return errors.NewProcessingError("[extendBatch][%s] same-block parent extension failed", block.Hash().String(), extendErr)
				}

				inBlockParent = hasSameBlockParent

				if needsExternalLookup {
					txsNeedingExtension = append(txsNeedingExtension, tx)
				}
			} else {
				inBlockParent = hasSameBlockParentInput(tx, extendedTxs)
			}

			batch.hasInBlockParent = append(batch.hasInBlockParent, inBlockParent)

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
