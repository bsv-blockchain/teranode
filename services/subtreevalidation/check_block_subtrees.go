package subtreevalidation

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/adaptivefetch"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// bufioReaderPool reduces GC pressure by reusing bufio.Reader instances.
// With 14,496 subtrees per block, using 32KB buffers provides excellent I/O performance
// while dramatically reducing memory pressure and GC overhead (16x reduction from previous 512KB).
var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 1024*1024) // Temp changed to 1MB buffer for scaling env - 32KB buffer - optimized for sequential I/O
	},
}

// countingReadCloser wraps an io.ReadCloser and counts bytes read
type countingReadCloser struct {
	reader    io.ReadCloser
	bytesRead *uint64 // Pointer to allow external access to count
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	atomic.AddUint64(c.bytesRead, uint64(n))
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.reader.Close()
}

// CheckBlockSubtrees validates that all subtrees referenced in a block exist in storage.
//
// subtree information for blocks that reference unavailable subtrees.
func (u *Server) CheckBlockSubtrees(ctx context.Context, request *subtreevalidation_api.CheckBlockSubtreesRequest) (*subtreevalidation_api.CheckBlockSubtreesResponse, error) {
	block, err := model.NewBlockFromBytes(request.Block)
	if err != nil {
		return nil, errors.NewProcessingError("[CheckBlockSubtrees] Failed to get block from blockchain client", err)
	}

	// Pre-CSV candidate block timestamp can be picked up cheaply from the block
	// header up front; the post-CSV candidate-parent MTP requires a blockchain
	// round-trip and is set up lower down, after the "all subtrees already
	// exist" early return so the no-work path skips the extra call.
	var candidateBlockTime uint32
	if block.Height < uint32(u.settings.ChainCfgParams.CSVHeight) {
		candidateBlockTime = block.Header.Timestamp
	}

	// Extract PeerID from request for tracking
	peerID := request.PeerId

	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "CheckBlockSubtrees",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusSubtreeValidationCheckSubtree),
		tracing.WithLogMessage(u.logger, "[CheckBlockSubtrees] called for block %s at height %d", block.Hash().String(), block.Height),
	)
	defer deferFn()

	// Panic recovery to ensure pause lock is always released even on crashes
	defer func() {
		if r := recover(); r != nil {
			u.logger.Errorf("[CheckBlockSubtrees] PANIC recovered for block %s: %v", block.Hash().String(), r)
			// Panic is re-raised after this defer completes, ensuring all defers execute
			panic(r)
		}
	}()

	// Check which subtrees are missing, waiting for any in-flight validations to complete.
	// When a subtree notification and block notification arrive simultaneously, the subtree
	// handler may still be processing. Without waiting, we'd immediately mark it as missing
	// and fetch subtree_data from the peer's asset-cache (expensive Aerospike reconstruction),
	// which can fail under load and cascade into CATCHINGBLOCKS mode.
	//
	// The existence check is bounded-parallel: on NFS-backed blob stores each Exists call is
	// a network round-trip, so the sequential cost grows linearly with block size. Bounding
	// concurrency at CheckBlockSubtreesConcurrency keeps the burst predictable.
	subtreeMissing := make([]bool, len(block.Subtrees))
	existsGroup, existsCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(existsGroup, u.settings.SubtreeValidation.CheckBlockSubtreesConcurrency)

	for idx, subtreeHash := range block.Subtrees {
		idx := idx
		subtreeHash := subtreeHash

		existsGroup.Go(func() error {
			if u.quorum != nil {
				locked, exists, release, err := u.quorum.TryLockIfNotExistsWithTimeout(existsCtx, subtreeHash, fileformat.FileTypeSubtree)
				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees] Failed to acquire quorum lock or determine subtree existence", err)
				}

				if locked {
					// File doesn't exist and no one else is working on it — release lock and mark missing.
					release()
					subtreeMissing[idx] = true
					return nil
				}

				if !exists {
					// Timed out waiting for in-flight handler — still treat as missing.
					subtreeMissing[idx] = true
				}
				// exists==true: subtree was completed by in-flight handler — no action needed.
				return nil
			}

			subtreeExists, err := u.subtreeStore.Exists(existsCtx, subtreeHash[:], fileformat.FileTypeSubtree)
			if err != nil {
				return errors.NewProcessingError("[CheckBlockSubtrees] Failed to check if subtree exists in store", err)
			}
			if !subtreeExists {
				subtreeMissing[idx] = true
			}
			return nil
		})
	}

	if err := existsGroup.Wait(); err != nil {
		return nil, err
	}

	missingSubtrees := make([]chainhash.Hash, 0, len(block.Subtrees))
	for idx, subtreeHash := range block.Subtrees {
		if subtreeMissing[idx] {
			missingSubtrees = append(missingSubtrees, *subtreeHash)
		}
	}

	// Early return if all subtrees already exist - no need for pause logic
	if len(missingSubtrees) == 0 {
		return &subtreevalidation_api.CheckBlockSubtreesResponse{
			Blessed: true,
		}, nil
	}

	u.logger.Infof("[CheckBlockSubtrees] Found %d missing subtrees for block %s, proceeding with validation", len(missingSubtrees), block.Hash().String())

	// Post-CSV candidate-parent MTP for the validator's consensus-path
	// finality check (Options.CandidateParentMedianTime). Source matches
	// bitcoin-sv's pindexPrev->GetMedianTimePast() for a candidate at height H:
	// median of timestamps at [H-11, H-1] following the parent's actual chain.
	// We delegate to blockchainClient.GetBlockHeaders which has a fork-aware
	// SQL fallback (recursive parent_id CTE) — so a side-chain candidate
	// receives MTP for ITS parent chain, not the main chain at height H.
	// Required on every post-CSV consensus request: the validator hard-errors
	// when the field is missing, so failing to populate here would surface as
	// a downstream rejection rather than degrading silently.
	var candidateParentMedianTime uint32
	if block.Height >= uint32(u.settings.ChainCfgParams.CSVHeight) {
		var mtpErr error

		// fetchCandidateParentMedianTime already includes the parent hash in its
		// error messages, so we just wrap with our service tag here. Reaching the
		// inner error formatting without a nil check would dereference the parent
		// hash pointer; that nil-guard lives inside fetchCandidateParentMedianTime.
		candidateParentMedianTime, mtpErr = u.fetchCandidateParentMedianTime(ctx, block.Header.HashPrevBlock)
		if mtpErr != nil {
			return nil, errors.NewProcessingError("[CheckBlockSubtrees] candidate-parent MTP", mtpErr)
		}
	}

	// BATCHED SUBTREE LOADING: Get blockIds once before batching
	blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.HashPrevBlock, uint64(u.settings.GetUtxoStoreBlockHeightRetention()*2))
	if err != nil {
		return nil, errors.NewProcessingError("[CheckSubtree] Failed to get block headers from blockchain client", err)
	}

	blockIds := make(map[uint32]bool, len(blockHeaderIDs))
	for _, blockID := range blockHeaderIDs {
		blockIds[blockID] = true
	}

	dah := u.utxoStore.GetBlockHeight() + u.settings.GetSubtreeValidationBlockHeightRetention()

	// Calculate batch size dynamically based on configured transaction batch size
	totalSubtrees := len(missingSubtrees)
	totalProcessedTxs := 0
	var subtreesBatchSize int

	txBatchSize := u.settings.SubtreeValidation.TxBatchSize

	if txBatchSize == 0 {
		// No batching - process all subtrees at once
		subtreesBatchSize = totalSubtrees
	} else if block.TransactionCount > 0 && len(block.Subtrees) > 0 {
		// Calculate exact txs per subtree using block metadata
		txsPerSubtree := int(block.TransactionCount / uint64(len(block.Subtrees)))
		if txsPerSubtree == 0 {
			subtreesBatchSize = 1
		} else {
			subtreesBatchSize = txBatchSize / txsPerSubtree
			if subtreesBatchSize == 0 {
				subtreesBatchSize = 1 // Minimum 1 subtree per batch
			}
		}
	} else {
		// Fallback if metadata not available (shouldn't happen)
		subtreesBatchSize = 1
		u.logger.Warnf("[CheckBlockSubtrees] Block metadata incomplete (txs=%d, subtrees=%d), using 1 subtree per batch",
			block.TransactionCount, len(block.Subtrees))
	}

	// Block-scoped in-block-parent metadata accumulator. Lives for the entire
	// CheckBlockSubtrees call: survives across batches AND across the
	// ordered-retry phase below. Mutated only at synchronisation points (after
	// each level's g.Wait() inside processTransactionsInLevels, and after
	// Phase 2 g.Wait() in the ordered retry). Per-tx goroutines see only
	// their own pre-filtered map; the shared accumulator is never read or
	// written from within a per-tx goroutine.
	//
	// blockAccumulator is the underlying live map. batchAccumulator wraps it
	// as the single-map composite (delta=live) used by the sequential batch
	// loop below. validateMissingSubtreesWithOrderedRetryAccumulated constructs
	// its own snapshot+delta and single-map composites internally.
	blockAccumulator := make(map[chainhash.Hash]*validator.ParentTxMetadata)
	batchAccumulator := &parentMetadataAccumulator{delta: blockAccumulator}

	// Process subtrees in batches to limit memory usage
	// Each batch loads subtree data, processes transactions, then GCs before next batch
	for batchStart := 0; batchStart < totalSubtrees; batchStart += subtreesBatchSize {
		batchEnd := batchStart + subtreesBatchSize
		if batchEnd > totalSubtrees {
			batchEnd = totalSubtrees
		}

		batchNum := (batchStart / subtreesBatchSize) + 1
		batchSubtrees := missingSubtrees[batchStart:batchEnd]
		u.logger.Debugf("[CheckBlockSubtrees] Processing subtree batch %d/%d with %d subtrees for block %s", batchNum, (totalSubtrees+subtreesBatchSize-1)/subtreesBatchSize, len(batchSubtrees), block.Hash().String())

		// Load transactions for this batch of subtrees in parallel
		subtreeTxs := make([][]*bt.Tx, len(batchSubtrees))
		batchArenas := make([]*bt.Arena, len(batchSubtrees))
		g, gCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(g, u.settings.SubtreeValidation.CheckBlockSubtreesConcurrency)

		for subtreeIdx, subtreeHash := range batchSubtrees {
			subtreeHash := subtreeHash
			subtreeIdx := subtreeIdx

			g.Go(func() (err error) {
				// A subtree may be available locally under either:
				//   - FileTypeSubtreeToCheck — fetched from a peer, pending validation
				//   - FileTypeSubtree        — already validated (e.g. legacy catch-up's
				//                               quickValidationMode validated txs inline
				//                               before writing the subtree)
				// We must consult both before falling back to an HTTP fetch. Otherwise
				// CheckBlockSubtrees will try to HTTP-download a subtree we already have
				// — and for baseURL="legacy" the synthetic URL has no scheme, so the
				// request fails outright.
				localFileType, localExists, err := u.findLocalSubtreeFile(gCtx, subtreeHash)
				if err != nil {
					return errors.NewStorageError("[CheckBlockSubtrees][%s] failed to check if subtree exists in store", subtreeHash.String(), err)
				}

				var subtreeToCheck *subtreepkg.Subtree

				if localExists {
					// read from whichever local file we found
					subtreeReader, err := u.subtreeStore.GetIoReader(gCtx, subtreeHash[:], localFileType)
					if err != nil {
						return errors.NewStorageError("[CheckBlockSubtrees][%s] failed to get subtree from store", subtreeHash.String(), err)
					}
					defer subtreeReader.Close()

					// Use pooled bufio.Reader to reduce allocations (eliminates 50% of GC pressure)
					bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
					bufferedReader.Reset(subtreeReader)
					defer func() {
						bufferedReader.Reset(nil) // Clear reference before returning to pool
						bufioReaderPool.Put(bufferedReader)
					}()

					subtreeToCheck, err = subtreepkg.NewSubtreeFromReader(bufferedReader)
					if err != nil {
						return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to deserialize subtree", subtreeHash.String(), err)
					}
				} else {
					// get the subtree from the peer
					url := fmt.Sprintf("%s/subtree/%s", request.BaseUrl, subtreeHash.String())

					// Bound the body at the receive-side policy cap (MaxIncomingSubtreeBytes) so a
					// malicious peer can't OOM us by streaming oversized responses. This must be
					// independent of local BlockAssembly.MaximumMerkleItemsPerSubtree, which only
					// controls what *this node* assembles; peers may legitimately produce larger subtrees.
					maxSubtreeBytes := u.settings.SubtreeValidation.MaxIncomingSubtreeBytes

					subtreeNodeBytes, err := util.DoHTTPRequestBounded(gCtx, url, maxSubtreeBytes)
					if err != nil {
						return errors.NewServiceError("[CheckBlockSubtrees][%s] failed to get subtree from %s", subtreeHash.String(), url, err)
					}

					// Track bytes downloaded from peer
					if u.p2pClient != nil && peerID != "" {
						if err := u.p2pClient.RecordBytesDownloaded(gCtx, peerID, uint64(len(subtreeNodeBytes))); err != nil {
							u.logger.Warnf("[CheckBlockSubtrees][%s] failed to record %d bytes downloaded from peer %s: %v", subtreeHash.String(), len(subtreeNodeBytes), peerID, err)
						}
					}

					// Bound the leaf count by the receive-side cap (same rationale as the body cap above):
					// peers may legitimately produce subtrees larger than the local assembly policy. The
					// bounded HTTP read already enforces this, but we keep the explicit check as a guard
					// before subtreepkg.NewIncompleteTreeByLeafCount allocates against the count.
					leafCount := len(subtreeNodeBytes) / chainhash.HashSize
					maxIncomingLeaves := int(maxSubtreeBytes / int64(chainhash.HashSize))
					if err := validateSubtreeLeafCount(subtreeHash, leafCount, maxIncomingLeaves); err != nil {
						return err
					}

					subtreeToCheck, err = subtreepkg.NewIncompleteTreeByLeafCount(leafCount)
					if err != nil {
						return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to create subtree structure", subtreeHash.String(), err)
					}

					var nodeHash chainhash.Hash
					for i := 0; i < len(subtreeNodeBytes)/chainhash.HashSize; i++ {
						copy(nodeHash[:], subtreeNodeBytes[i*chainhash.HashSize:(i+1)*chainhash.HashSize])

						if nodeHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
							if err = subtreeToCheck.AddCoinbaseNode(); err != nil {
								return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to add coinbase node to subtree", subtreeHash.String(), err)
							}
						} else {
							if err = subtreeToCheck.AddNode(nodeHash, 0, 0); err != nil {
								return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to add node to subtree", subtreeHash.String(), err)
							}
						}
					}

					if !subtreeHash.Equal(*subtreeToCheck.RootHash()) {
						return errors.NewProcessingError("[CheckBlockSubtrees][%s] subtree root hash mismatch: %s", subtreeHash.String(), subtreeToCheck.RootHash().String())
					}

					subtreeBytes, err := subtreeToCheck.Serialize()
					if err != nil {
						return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to serialize subtree", subtreeHash.String(), err)
					}

					// Store the subtreeToCheck for later processing
					// we not set a DAH as this is part of a block and will be permanently stored anyway
					if err = u.subtreeStore.Set(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes, options.WithDeleteAt(dah)); err != nil {
						return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to store subtree", subtreeHash.String(), err)
					}
				}

				// Adaptive-fetch gate: when optimistic, skip subtreeData entirely.
				//
				// What this skip is: the work below is only a prewarm. It bulk-loads
				// the subtree's txs from subtreeData so the real validation step
				// (ValidateSubtreeInternal, run for every subtree further down) finds
				// them already cached. Skipping it leaves subtreeTxs[subtreeIdx] nil,
				// so the bulk processTransactionsInLevels does nothing for this subtree.
				//
				// What this skip is NOT: it does not skip validation. Every subtree
				// still goes through ValidateSubtreeInternal below, which checks the
				// UTXO store for each tx and fetches anything genuinely missing from
				// peers on demand (getSubtreeMissingTxs → processMissingTransactions).
				// So if the optimistic assumption is wrong — a tx was not delivered by
				// propagation — that tx is still recovered and validated normally. The
				// only cost of a wrong guess is bandwidth (the tx is fetched during
				// validation instead of being prewarmed here); correctness and data
				// integrity are never at risk.
				//
				// Capture the live mode (not just the boolean) so the
				// observation we record below can be tagged with the mode
				// the work was actually performed in. Subtree workers run
				// concurrently via the errgroup and the mode can transition
				// between sample and Record; tagging the observation lets
				// the state machine drop cross-mode samples instead of
				// applying them to the wrong window.
				modeAtSample := u.adaptiveFetch.Mode()
				optimistic := modeAtSample == adaptivefetch.ModeOptimistic

				if !optimistic {
					// PHASE 2: Exact pre-allocation. Only sized when we will actually
					// populate the slice (pessimistic path). In optimistic mode we
					// leave subtreeTxs[subtreeIdx] as nil to avoid allocating a
					// large *bt.Tx-pointer backing array that would never be filled —
					// downstream consolidation handles nil/empty entries naturally.
					subtreeTxs[subtreeIdx] = make([]*bt.Tx, 0, subtreeToCheck.Length())

					// Allocate a per-subtree arena for zero-copy script decoding.
					// The arena is stored in batchArenas[subtreeIdx] so it can be released
					// after processTransactionsInLevels consumes the batch's txs.
					// Only allocated on the pessimistic path; in optimistic mode no
					// subtreeData is fetched, so no arena is needed and
					// batchArenas[subtreeIdx] stays nil (the release loops guard on nil).
					arena := getSubtreeArena()
					batchArenas[subtreeIdx] = arena

					subtreeDataExists, err := u.subtreeStore.Exists(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeData)
					if err != nil {
						return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to check if subtree data exists in store", subtreeHash.String(), err)
					}

					if !subtreeDataExists {
						// get the subtree data from the peer and process it directly
						url := fmt.Sprintf("%s/subtree_data/%s", request.BaseUrl, subtreeHash.String())

						// Retry on 503 — peer's asset service may reject under admission control
						// while it generates the file on-demand from Aerospike.
						//
						// IMPORTANT: pass the parent ctx, NOT gCtx, to the HTTP fetch and the
						// stream processor. gCtx is the errgroup's cancellable context — using
						// it here means a single sibling failure cancels every in-flight
						// subtree_data download in this batch. Because each cancellation closes
						// the upstream connection, the peer aborts its on-demand creation
						// (storer.Abort), discarding work that was already paid for in Aerospike
						// reads. Detaching from gCtx lets each fetch complete (or hit its own
						// http_streaming_timeout) so the peer can finish writing its subtreeData
						// file — converting wasted Aerospike work into a pre-warmed cache for
						// the next retry. The trade-off is that batch failure detection waits
						// for in-flight peers instead of cancelling early; acceptable here
						// because the per-fetch streaming timeout still bounds it.
						body, subtreeDataErr := util.DoHTTPRequestBodyReaderWithRetry(ctx, url)
						if subtreeDataErr != nil {
							return errors.NewServiceError("[CheckBlockSubtrees][%s] failed to get subtree data from %s", subtreeHash.String(), url, subtreeDataErr)
						}

						// Wrap with counting reader to track bytes downloaded
						var bytesRead uint64
						countingBody := &countingReadCloser{
							reader:    body,
							bytesRead: &bytesRead,
						}

						// Process transactions directly from the stream while storing to disk.
						// Same rationale as above for using ctx instead of gCtx.
						err = u.processSubtreeDataStream(ctx, subtreeToCheck, countingBody, &subtreeTxs[subtreeIdx], dah, arena)
						_ = countingBody.Close()

						// Track bytes downloaded from peer after stream is consumed
						// Decouple the context to ensure tracking completes even if parent context is cancelled
						if u.p2pClient != nil && peerID != "" {
							trackCtx, _, deferFn := tracing.DecoupleTracingSpan(gCtx, "subtreevalidation", "recordBytesDownloaded")
							defer deferFn()
							if err := u.p2pClient.RecordBytesDownloaded(trackCtx, peerID, bytesRead); err != nil {
								u.logger.Warnf("[CheckBlockSubtrees][%s] failed to record %d bytes downloaded from peer %s: %v", subtreeHash.String(), bytesRead, peerID, err)
							}
						}

						if err != nil {
							return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to process subtree data stream", subtreeHash.String(), err)
						}
					} else {
						// SubtreeData exists, extract transactions from stored file
						err = u.extractAndCollectTransactions(gCtx, subtreeToCheck, &subtreeTxs[subtreeIdx], arena)
						if err != nil {
							return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to extract transactions", subtreeHash.String(), err)
						}
					}
				}

				// Record a synthetic warm-up observation for the adaptive-fetch
				// state machine. The rationale (why MissingFetches is 0 today,
				// why that is safe, and the TODO to plumb real counts) lives
				// once on adaptivefetch.State.RecordSyntheticWarmup. The State is
				// armed on first FSM RUNNING (see Server), so a node stays
				// pessimistic through cold-start IBD and only earns optimism once
				// proven synced.
				u.adaptiveFetch.RecordSyntheticWarmup(modeAtSample, subtreeToCheck.Length(), 0)

				return nil
			})
		}

		if err = g.Wait(); err != nil {
			// Release arenas allocated by goroutines that completed before the error.
			for i := range batchArenas {
				if batchArenas[i] != nil {
					putSubtreeArena(batchArenas[i])
				}
			}
			return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to get subtree tx hashes for batch %d", batchNum, err)
		}

		// Collect all transactions from this batch of subtrees
		// Calculate exact capacity needed across all subtrees in this batch to avoid reallocations
		totalTxCapacity := 0
		for _, txs := range subtreeTxs {
			totalTxCapacity += len(txs)
		}
		allTransactions := make([]*bt.Tx, 0, totalTxCapacity)
		for _, txs := range subtreeTxs {
			if len(txs) > 0 {
				allTransactions = append(allTransactions, txs...)
			}
		}

		// Release 2D subtree transaction slice after consolidation
		// All transactions now in allTransactions, original 2D structure no longer needed
		subtreeTxs = nil //nolint:ineffassign // Intentional early GC hint

		batchTxCount := len(allTransactions)
		totalBatches := (totalSubtrees + subtreesBatchSize - 1) / subtreesBatchSize
		u.logger.Debugf("[CheckBlockSubtrees] Batch %d/%d loaded %d transactions for block %s, now processing", batchNum, totalBatches, batchTxCount, block.Hash().String())

		// Process transactions for this batch
		if batchTxCount > 0 {
			if err = u.processTransactionsInLevels(ctx, allTransactions, *block.Hash(), chainhash.Hash{}, block.Height, candidateBlockTime, candidateParentMedianTime, blockIds, batchAccumulator); err != nil {
				// Release arenas before returning — txs won't be consumed further.
				for i := range batchArenas {
					if batchArenas[i] != nil {
						putSubtreeArena(batchArenas[i])
					}
				}
				return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to process transactions in batch %d", batchNum, err)
			}
			totalProcessedTxs += batchTxCount

			// Release transaction slice after processing completes
			// Transactions are now in UTXO store and validator cache, original slice no longer needed
			allTransactions = nil //nolint:ineffassign // Intentional early GC hint
		}

		// Release per-subtree arenas: all *bt.Tx pointers were consumed by
		// processTransactionsInLevels above, so the arena-backed script slices
		// are no longer referenced and the arenas can be returned to the pool.
		for i := range batchArenas {
			if batchArenas[i] != nil {
				putSubtreeArena(batchArenas[i])
				batchArenas[i] = nil
			}
		}
		batchArenas = nil //nolint:ineffassign // Intentional early GC hint

		batchSubtrees = nil //nolint:ineffassign // Intentional early GC hint for batch slice view
		u.logger.Debugf("[CheckBlockSubtrees] Batch %d/%d complete for block %s (%d txs processed, %d total), memory reclaimed", batchNum, totalBatches, block.Hash().String(), batchTxCount, totalProcessedTxs)
	}

	u.logger.Infof("[CheckBlockSubtrees] Completed processing %d transactions across %d subtree batches", totalProcessedTxs, (totalSubtrees+subtreesBatchSize-1)/subtreesBatchSize)

	// validateSubtree is the per-subtree action used by both the parallel and
	// sequential passes below. Extracted as a closure so the phase-2/phase-3
	// ordering logic (validateMissingSubtreesWithOrderedRetryAccumulated) can
	// be unit tested against a stub validator without requiring full subtree
	// data infrastructure. The accumulator argument is a single-map view
	// (delta=live) in Phase 3 (sequential) and a snapshot+delta view in
	// Phase 2 (parallel) — see validateMissingSubtreesWithOrderedRetryAccumulated.
	validateSubtree := func(validateCtx context.Context, subtreeHash chainhash.Hash, accumulator *parentMetadataAccumulator) (*subtreepkg.Subtree, error) {
		v := ValidateSubtree{
			SubtreeHash:   subtreeHash,
			BaseURL:       request.BaseUrl,
			AllowFailFast: false,
			PeerID:        peerID,
		}

		return u.validateSubtreeInternalImpl(
			validateCtx,
			v,
			block.Height,
			blockIds,
			accumulator,
			validator.WithSkipPolicyChecks(true),
			validator.WithCreateConflicting(true),
			validator.WithIgnoreLocked(true),
			validator.WithCandidateBlockTime(candidateBlockTime),
			validator.WithCandidateParentMedianTime(candidateParentMedianTime),
		)
	}

	if err := u.validateMissingSubtreesWithOrderedRetryAccumulated(ctx, missingSubtrees, blockAccumulator, validateSubtree); err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &subtreevalidation_api.CheckBlockSubtreesResponse{
		Blessed: true,
	}, nil
}

// findLocalSubtreeFile reports whether this node already has a copy of the given
// subtree in its subtree store, and which file type holds it. It checks
// FileTypeSubtreeToCheck first (the "downloaded from peer, pending validation"
// marker used on the normal p2p path) and then falls back to FileTypeSubtree
// (the "already validated" marker used by legacy catch-up's quickValidationMode
// and by block assembly / block persister). Either file carries the same
// tx-hash list, so CheckBlockSubtrees can proceed either way.
//
// This avoids a pathological fallback to HTTP when the subtree is in fact
// present locally — particularly important for baseURL="legacy", where the
// synthetic URL "legacy/subtree/<hash>" has no scheme and cannot be fetched.
func (u *Server) findLocalSubtreeFile(ctx context.Context, subtreeHash chainhash.Hash) (fileformat.FileType, bool, error) {
	exists, err := u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	if err != nil {
		return fileformat.FileTypeUnknown, false, err
	}
	if exists {
		return fileformat.FileTypeSubtreeToCheck, true, nil
	}
	exists, err = u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		return fileformat.FileTypeUnknown, false, err
	}
	if exists {
		return fileformat.FileTypeSubtree, true, nil
	}
	return fileformat.FileTypeUnknown, false, nil
}

// validateMissingSubtreesWithOrderedRetry runs phase-2 parallel validation and
// phase-3 ordered sequential revalidation.
//
// Phase 2 — parallel: every subtree in missingSubtrees is validated concurrently
// (bounded by CheckBlockSubtreesConcurrency). Failures are recorded positionally
// in a []bool indexed by the subtree's position in missingSubtrees (block order)
// so the retry pass sees them in block order rather than in goroutine-completion
// order.
//
// Phase 3 — sequential: the failed subtrees are revalidated one at a time in
// missingSubtrees order. Because transactions within a block can depend on
// transactions in earlier subtrees of the same block (cross-subtree parents),
// walking the failures in block order guarantees that by the time subtree N is
// retried, every earlier subtree has already been validated successfully — so
// the cache contains every parent subtree N could depend on. One ordered pass
// is therefore sufficient; any remaining failure is a real validation error,
// not an ordering artefact, and is returned to the caller.
//
// The validateFn parameter is the per-subtree action. Injecting it keeps this
// function small enough to unit-test the phase-2/phase-3 interaction against a
// stub validator without needing real subtree data, peer HTTP, or a full store.
func (u *Server) validateMissingSubtreesWithOrderedRetry(
	ctx context.Context,
	missingSubtrees []chainhash.Hash,
	validateFn func(ctx context.Context, subtreeHash chainhash.Hash) (*subtreepkg.Subtree, error),
) error {
	// Phase 2: Parallel validation. Failures are collected positionally so the
	// sequential revalidation pass below walks them in block-subtree order.
	// Cross-subtree parent dependencies within a block only resolve
	// left-to-right; arbitrary goroutine-completion order would leave children
	// ahead of their parents.
	failedParallel := make([]bool, len(missingSubtrees))

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, u.settings.SubtreeValidation.CheckBlockSubtreesConcurrency)

	for i, subtreeHash := range missingSubtrees {
		i, subtreeHash := i, subtreeHash

		g.Go(func() error {
			if _, err := validateFn(gCtx, subtreeHash); err != nil {
				u.logger.Debugf("[CheckBlockSubtreesRequest] Failed to validate subtree %s: %v", subtreeHash.String(), err)
				failedParallel[i] = true

				return nil
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed during parallel subtree validation", err)
	}

	// Phase 3: Sequential revalidation in block-subtree order.
	//
	// Transactions within a block can depend on transactions in earlier
	// subtrees of the same block (cross-subtree parents). The parallel pass
	// above races on these dependencies and fails children whose parents
	// haven't populated the cache yet. Walking the failures in block order
	// resolves them in a single pass: subtree N's validation populates the
	// cache for subtrees > N.
	//
	// If a subtree still fails here it is a real error (not an ordering
	// artefact), because all earlier subtrees in the block have already been
	// validated successfully — either in the parallel pass, or in this loop.
	for i, subtreeHash := range missingSubtrees {
		if !failedParallel[i] {
			continue
		}

		if _, err := validateFn(ctx, subtreeHash); err != nil {
			return errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to validate subtree %s", subtreeHash.String(), err)
		}
	}

	return nil
}

// validateMissingSubtreesWithOrderedRetryAccumulated is the accumulator-aware
// sibling of validateMissingSubtreesWithOrderedRetry, used by the
// block-validation path. The block-scoped accumulator (passed in as the
// live map) survives the whole CheckBlockSubtrees call and feeds
// in-block-parent metadata into per-tx validations across batches and across
// ordered-retry phases.
//
// Phase 2 (parallel) must not mutate the shared accumulator from goroutines
// — the same validateFn would race. The safe shape is: one read-only shared
// snapshot of the live accumulator + one fresh per-subtree delta map.
// Each Phase 2 goroutine holds a parentMetadataAccumulator value pointing
// at the SAME snapshot (read-only — no synchronization required) and its
// OWN delta. Reads check delta first then snapshot; writes go to delta only.
// This avoids the O(len(missingSubtrees) * len(liveAccumulator)) copy
// churn that a per-subtree full snapshot would incur on large blocks.
//
// After g.Wait(), Phase 2 deltas are merged into the live accumulator in
// block-subtree order, **but only for subtrees that fully succeeded**. A
// subtree that failed in Phase 2 is retried in Phase 3 and its txs are
// merged then (preserves the invariant "accumulator entries describe txs
// from successful subtrees only"). Merges use first-writer-wins, so the
// earliest block-order subtree's entry for a given hash sticks.
//
// Phase 3 (sequential) reuses the live accumulator directly — single
// goroutine, no race. Phase 3 retries see all Phase 2 successes plus any
// earlier Phase 3 successes from this loop.
func (u *Server) validateMissingSubtreesWithOrderedRetryAccumulated(
	ctx context.Context,
	missingSubtrees []chainhash.Hash,
	liveAccumulator map[chainhash.Hash]*validator.ParentTxMetadata,
	validateFn func(ctx context.Context, subtreeHash chainhash.Hash, accumulator *parentMetadataAccumulator) (*subtreepkg.Subtree, error),
) error {
	failedParallel := make([]bool, len(missingSubtrees))
	subtreeDeltas := make([]map[chainhash.Hash]*validator.ParentTxMetadata, len(missingSubtrees))

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, u.settings.SubtreeValidation.CheckBlockSubtreesConcurrency)

	// Single shared read-only snapshot of the live accumulator. All Phase 2
	// goroutines reference this same map for reads; writes go to per-subtree
	// deltas. Safe because the live accumulator is NOT mutated between
	// here and g.Wait() — the only writers are the deltas, and the per-batch
	// processTransactionsInLevels writes have already drained sequentially
	// before this function was called.
	snapshot := liveAccumulator

	for i, subtreeHash := range missingSubtrees {
		i, subtreeHash := i, subtreeHash

		// Each subtree gets its own fresh delta map (owned by that one
		// goroutine — no concurrent access). Allocated empty; the goroutine
		// writes its successful tx hashes into it via acc.add during
		// processMissingTransactions per-level merges.
		deltaForSubtree := make(map[chainhash.Hash]*validator.ParentTxMetadata)
		subtreeDeltas[i] = deltaForSubtree

		accForSubtree := &parentMetadataAccumulator{
			snapshot: snapshot,
			delta:    deltaForSubtree,
		}

		g.Go(func() error {
			if _, err := validateFn(gCtx, subtreeHash, accForSubtree); err != nil {
				u.logger.Debugf("[CheckBlockSubtreesRequest] Phase 2 failed to validate subtree %s: %v", subtreeHash.String(), err)
				failedParallel[i] = true
				return nil
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed during parallel subtree validation", err)
	}

	// Merge Phase 2 successes into the live accumulator in block-subtree
	// order. Failed-Phase-2 subtrees are skipped: their partial contributions
	// (if any) are retried in Phase 3 and merged there with full state.
	// Each delta is small (only the txs THIS subtree added), so the merge
	// is O(sum-of-delta-sizes), not O(missingSubtrees * liveAccumulator).
	for i := range missingSubtrees {
		if failedParallel[i] {
			continue
		}
		for h, m := range subtreeDeltas[i] {
			if _, exists := liveAccumulator[h]; !exists {
				liveAccumulator[h] = m
			}
		}
		// Release the delta — its entries now live in liveAccumulator (for
		// successful subtrees) or have been deliberately dropped (failed
		// subtrees — handled by Phase 3 below).
		subtreeDeltas[i] = nil
	}

	// Phase 3: sequential retries with the live accumulator. delta=live;
	// no snapshot needed because there is no concurrency to isolate against.
	liveView := &parentMetadataAccumulator{delta: liveAccumulator}
	for i, subtreeHash := range missingSubtrees {
		if !failedParallel[i] {
			continue
		}

		if _, err := validateFn(ctx, subtreeHash, liveView); err != nil {
			return errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to validate subtree %s", subtreeHash.String(), err)
		}
	}

	return nil
}

// extractAndCollectTransactions extracts all transactions from a subtree's data file
// and adds them to the shared collection for block-wide processing.
// When arena is non-nil, script bytes are arena-allocated (caller owns arena lifetime).
func (u *Server) extractAndCollectTransactions(ctx context.Context, subtree *subtreepkg.Subtree, subtreeTransactions *[]*bt.Tx, arena *bt.Arena) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "extractAndCollectTransactions",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[extractAndCollectTransactions] called for subtree %s", subtree.RootHash().String()),
	)
	defer deferFn()

	// Get subtreeData reader
	subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return errors.NewStorageError("[extractAndCollectTransactions] failed to get subtreeData from store", err)
	}
	defer subtreeDataReader.Close()

	// Use pooled bufio.Reader to accelerate reading and reduce allocations
	bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
	bufferedReader.Reset(subtreeDataReader)
	defer func() {
		bufferedReader.Reset(nil)
		bufioReaderPool.Put(bufferedReader)
	}()

	// Read transactions directly into the shared collection
	txCount, err := u.readTransactionsFromSubtreeDataStream(subtree, bufferedReader, subtreeTransactions, arena)
	if err != nil {
		return errors.NewProcessingError("[extractAndCollectTransactions] failed to read transactions from subtreeData", err)
	}

	if txCount != subtree.Length() {
		return errors.NewProcessingError("[extractAndCollectTransactions] transaction count mismatch: expected %d, got %d", subtree.Length(), txCount)
	}

	u.logger.Debugf("[extractAndCollectTransactions] Extracted %d transactions from subtree %s", txCount, subtree.RootHash().String())

	return nil
}

// processSubtreeDataStream downloads subtreeData and simultaneously stores to disk while parsing transactions.
// PHASE 1: Concurrent streaming - eliminates storage read-back by writing to disk while parsing.
// When arena is non-nil, script bytes are arena-allocated (caller owns arena lifetime).
func (u *Server) processSubtreeDataStream(ctx context.Context, subtree *subtreepkg.Subtree,
	body io.ReadCloser, allTransactions *[]*bt.Tx, dah uint32, arena *bt.Arena) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processSubtreeDataStream",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[processSubtreeDataStream] called for subtree %s", subtree.RootHash().String()),
	)
	defer deferFn()

	// Create a pipe for concurrent storage write
	pr, pw := io.Pipe()

	// Channel to capture storage errors
	storeDone := make(chan error, 1)

	// Goroutine to write to storage concurrently
	go func() {
		err := u.subtreeStore.SetFromReader(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, pr, options.WithDeleteAt(dah))
		storeDone <- err
		// If storage failed, close pipe writer to unblock any pending writes
		// This prevents deadlock when SetFromReader returns an error without fully draining the pipe reader
		if err != nil {
			pw.CloseWithError(err)
		}
	}()

	// Use TeeReader to split network stream to storage and parser simultaneously
	teeReader := io.TeeReader(body, pw)

	// Use pooled bufio.Reader for parsing
	bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
	bufferedReader.Reset(teeReader)
	defer func() {
		bufferedReader.Reset(nil)
		bufioReaderPool.Put(bufferedReader)
	}()

	// Parse transactions while writing to storage
	txCount, parseErr := u.readTransactionsFromSubtreeDataStream(subtree, bufferedReader, allTransactions, arena)

	// Close the pipe writer to signal completion to storage goroutine
	// Use CloseWithError if parsing failed to properly signal the storage goroutine
	if parseErr != nil {
		pw.CloseWithError(parseErr)
	} else {
		pw.Close()
	}

	// Wait for storage operation to complete
	storeErr := <-storeDone

	// Check for errors from both operations
	if storeErr != nil {
		return errors.NewProcessingError("[processSubtreeDataStream] failed to store subtree data", storeErr)
	}

	if parseErr != nil {
		return errors.NewProcessingError("[processSubtreeDataStream] failed to parse transactions", parseErr)
	}

	// Verify transaction count
	if txCount != subtree.Length() {
		return errors.NewProcessingError("[processSubtreeDataStream] transaction count mismatch: expected %d, got %d", subtree.Length(), txCount)
	}

	u.logger.Debugf("[processSubtreeDataStream] Processed %d transactions from subtree %s (single-pass streaming)",
		txCount, subtree.RootHash().String())

	return nil
}

// readTransactionsFromSubtreeDataStream reads transactions directly from subtreeData stream.
// When arena is non-nil, per-script byte slices are drawn from the arena (caller must keep
// the arena alive for as long as the returned *bt.Tx values are in use, and call
// putSubtreeArena only after the txs are fully consumed). When arena is nil, scripts are
// heap-allocated via the standard tx.ReadFrom path.
func (u *Server) readTransactionsFromSubtreeDataStream(subtree *subtreepkg.Subtree, reader io.Reader, subtreeTransactions *[]*bt.Tx, arena *bt.Arena) (int, error) {
	txIndex := 0

	if len(subtree.Nodes) > 0 && subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
		txIndex = 1
	}

	var hashScratch []byte

	for {
		tx := &bt.Tx{}

		_, err := tx.ReadFromWithArena(reader, arena)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// End of stream reached
				break
			}
			return txIndex, errors.NewProcessingError("[readTransactionsFromSubtreeDataStream] error reading transaction", err)
		}

		if tx.IsCoinbase() && txIndex == 1 {
			// we did get an unexpected coinbase transaction
			// reset the index to 0 to check the coinbase
			txIndex = 0
		}

		var h chainhash.Hash
		h, hashScratch = tx.HashTxIDInto(hashScratch)
		tx.SetTxHash(&h)

		// Basic sanity check: ensure the transaction hash matches the expected hash from the subtree
		if txIndex < subtree.Length() {
			expectedHash := subtree.Nodes[txIndex].Hash
			// The coinbase placeholder (all-F's) is only treated as valid at index 0 of this subtree when the
			// corresponding transaction is coinbase. The actual coinbase tx hash may be unavailable when the
			// subtree structure is built, so this special case is allowed only for that local position.
			isCoinbasePlaceholder := txIndex == 0 && tx.IsCoinbase() && expectedHash.Equal(subtreepkg.CoinbasePlaceholderHashValue)
			if !isCoinbasePlaceholder && !expectedHash.Equal(*tx.TxIDChainHash()) {
				return txIndex, errors.NewProcessingError("[readTransactionsFromSubtreeDataStream] transaction hash mismatch at index %d: expected %s, got %s", txIndex, expectedHash.String(), tx.TxIDChainHash().String())
			}
		} else {
			return txIndex, errors.NewProcessingError("[readTransactionsFromSubtreeDataStream] more transactions than expected in subtreeData")
		}

		*subtreeTransactions = append(*subtreeTransactions, tx)
		txIndex++
	}

	return txIndex, nil
}

// processTransactionsInLevels processes all transactions from all subtrees using level-based validation.
//
// candidateBlockTime and candidateParentMedianTime are paired finality-time
// sources for the validator's consensus path. Pre-CSV consumes
// candidateBlockTime; post-CSV consumes candidateParentMedianTime. Each is
// expected to be zero in the era it is not consumed; see Options.CandidateBlockTime
// and Options.CandidateParentMedianTime in services/validator/options.go.
// This ensures transactions are processed in dependency order while maximizing parallelism
// processTransactionsInLevels processes one batch of subtree transactions in
// dependency-level order. When blockAccumulator is non-nil it doubles as the
// block-scoped in-block-parent metadata accumulator:
//   - Per-tx pre-filter: each tx's ParentMetadata is filtered down to the
//     accumulator entries the tx's input prevouts actually reference, then
//     attached to a per-tx Options clone before the validation goroutine is
//     spawned. The shared accumulator is never touched from per-tx goroutines.
//   - Cross-level growth: after each level's g.Wait(), the successful txs of
//     that level are merged into the accumulator. By the time level N+1
//     spawns, the accumulator covers every successful tx from levels 0..N
//     across all earlier batches and earlier levels of this batch.
//
// This closes both Path 2 (skip-level grandparents, which the prior per-level
// buildParentMetadata only fed for level-1) and the cross-batch case (an
// in-block parent created in batch K is visible to a child in batch K+1
// because the accumulator survives across batch boundaries when the caller
// reuses the same map).
//
// When blockAccumulator is nil this function still works for callers that
// don't hold block context (peer-announced subtree validation): ParentMetadata
// is simply not set on per-tx Options and the validator falls back to the
// UTXO store. That path remains best-effort.
func (u *Server) processTransactionsInLevels(ctx context.Context, allTransactions []*bt.Tx, blockHash chainhash.Hash, subtreeHash chainhash.Hash, blockHeight uint32, candidateBlockTime uint32, candidateParentMedianTime uint32, blockIds map[uint32]bool, blockAccumulator *parentMetadataAccumulator) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processTransactionsInLevels",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[processTransactionsInLevels] Processing %d transactions at block height %d", len(allTransactions), blockHeight),
	)
	defer deferFn()

	if len(allTransactions) == 0 {
		return nil
	}

	txHashes := make([]chainhash.Hash, len(allTransactions))

	for i, tx := range allTransactions {
		if tx == nil {
			return errors.NewProcessingError("[processTransactionsInLevels] transaction is nil at index %d", i)
		}

		txHashes[i] = *tx.TxIDChainHash()
	}

	// Pre-check: identify transactions that are already validated in cache or UTXO store
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	missed, err := u.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] Failed to check txMeta cache", err)
	}

	if missed > 0 {
		u.logger.Debugf("[processTransactionsInLevels] Pre-check: %d/%d transactions missed in cache, checking UTXO store", missed, len(txHashes))

		batched := u.settings.SubtreeValidation.BatchMissingTransactions
		missed, err = u.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, blockIds, batched, false)
		if err != nil {
			return errors.NewProcessingError("[processTransactionsInLevels] Failed to check txMeta store", err)
		}
	}

	alreadyValidated := len(txHashes) - missed

	// Seed the block-scoped accumulator with txs that are part of this
	// candidate batch but were already known to the cache or UTXO store
	// (e.g. validated earlier via peer-announced subtree path, or seen in
	// an earlier batch of this same block). Without this seeding, a child
	// in this batch — or in a later batch — referencing such a parent would
	// see an empty ParentMetadata entry, fall through to the UTXO-store
	// BlockHeights path, find it empty (the parent's blocks_transactions
	// row is only written by SetMinedMulti after this block is accepted),
	// and the validator would stamp unconfirmedParentHeight — triggering
	// bad-txns-unconfirmed-input-in-block on a legitimate block.
	//
	// Done before the missed==0 early return so even an all-known batch
	// contributes its parents to the accumulator for subsequent batches.
	// first-writer-wins (acc.add): if an entry already exists from an
	// earlier batch we keep it.
	if blockAccumulator != nil {
		for i, tx := range allTransactions {
			if tx != nil && txMetaSlice[i].isSet {
				blockAccumulator.add(txHashes[i], &validator.ParentTxMetadata{BlockHeight: blockHeight})
			}
		}
	}

	if missed == 0 {
		u.logger.Debugf("[processTransactionsInLevels] All transactions already validated, skipping processing")
		return nil
	} else if alreadyValidated > 0 {
		u.logger.Debugf("[processTransactionsInLevels] Pre-check: %d/%d transactions already validated, %d need validation", alreadyValidated, len(txHashes), missed)
	}

	// Convert transactions to missingTx format for prepareTxsPerLevel
	missingTxs := make([]missingTx, len(allTransactions))

	for i, tx := range allTransactions {
		if txMetaSlice[i].isSet {
			// Transaction already validated, skip
			continue
		}

		missingTxs[i] = missingTx{
			tx:  tx,
			idx: i,
		}
	}

	u.logger.Debugf("[processTransactionsInLevels] Organizing %d transactions into dependency levels", len(allTransactions))

	// Use the existing prepareTxsPerLevel logic to organize transactions by dependency levels
	maxLevel, txsPerLevel, err := u.selectPrepareTxsPerLevel(ctx, missingTxs)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] Failed to prepare transactions per level", err)
	}

	// PHASE 2 OPTIMIZATION: Track total count before clearing slices
	totalTxCount := len(allTransactions)

	// PHASE 2 OPTIMIZATION: Clear original slices to allow GC
	// Transactions are now organized in txsPerLevel, original slices no longer needed
	// These explicit nils help GC reclaim memory earlier rather than waiting for function scope end
	allTransactions = nil //nolint:ineffassign // Intentional early GC hint
	missingTxs = nil      //nolint:ineffassign // Intentional early GC hint

	u.logger.Debugf("[processTransactionsInLevels] Processing transactions across %d levels", maxLevel+1)

	validatorOptions := []validator.Option{
		validator.WithSkipPolicyChecks(true),
		validator.WithCreateConflicting(true),
		validator.WithIgnoreLocked(true),
		validator.WithCandidateBlockTime(candidateBlockTime),
		validator.WithCandidateParentMedianTime(candidateParentMedianTime),
	}

	currentState, err := u.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] Failed to get FSM current state", err)
	}

	// During legacy syncing or catching up, disable adding transactions to block assembly
	if *currentState == blockchain.FSMStateLEGACYSYNCING || *currentState == blockchain.FSMStateCATCHINGBLOCKS {
		validatorOptions = append(validatorOptions, validator.WithAddTXToBlockAssembly(false))
	}

	// Pre-process validation options
	processedValidatorOptions := validator.ProcessOptions(validatorOptions...)

	// Pre-warm the MTP store once before spawning per-transaction goroutines, so each goroutine
	// can read mtpStore[h] without locking and without making gRPC calls.
	if err = u.validatorClient.EnsureMTPLoaded(ctx, blockHeight); err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] failed to pre-load MTP store: %v", err)
	}

	// Track validation results
	var (
		errorsFound         atomic.Uint64
		missingParentErrors atomic.Uint64
	)

	// Track successfully validated transactions for THIS level only. After the
	// level's g.Wait() these are merged into blockAccumulator so future levels
	// (and future batches / ordered-retry phases) see them via per-tx
	// pre-filtering. Each level's set is freshly allocated to avoid retaining
	// pointers to released tx data.
	var levelSuccessMutex sync.Mutex

	// Process each level in series, but all transactions within a level in parallel
	for level := uint32(0); level <= maxLevel; level++ {
		levelTxs := txsPerLevel[level]
		if len(levelTxs) == 0 {
			continue
		}

		u.logger.Debugf("[processTransactionsInLevels] Processing level %d/%d with %d transactions", level+1, maxLevel+1, len(levelTxs))

		// Collect successful txs for THIS level so we can merge them into the
		// block-scoped accumulator after g.Wait(). Pre-allocated to the level
		// size — the worst-case is every tx succeeds.
		levelSuccessfulTxs := make([]chainhash.Hash, 0, len(levelTxs))

		// PHASE 2 OPTIMIZATION: Extend transactions with in-block parent outputs
		// This avoids Aerospike fetches for intra-block dependencies (~500MB+ savings)
		// Build parent map ONCE per level and reuse for all children (O(n) instead of O(n²))
		if level > 0 {
			// Build parent map once for the entire level
			parentMap := buildParentMapFromLevel(txsPerLevel[level-1])

			if len(parentMap) > 0 {
				u.logger.Debugf("[processTransactionsInLevels] Built parent map with %d transactions for level %d extension", len(parentMap), level)

				totalExtended := 0
				for _, mTx := range levelTxs {
					if mTx.tx != nil {
						extendedCount := extendTxWithInBlockParents(mTx.tx, parentMap)
						totalExtended += extendedCount
					}
				}

				if totalExtended > 0 {
					u.logger.Debugf("[processTransactionsInLevels] Extended %d inputs from previous level for level %d", totalExtended, level)
				}
			}
		}

		// Process all transactions at this level in parallel
		g, gCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(g, u.settings.SubtreeValidation.SpendBatcherSize*2)

		for _, mTx := range levelTxs {
			tx := mTx.tx
			if tx == nil {
				return errors.NewProcessingError("[processTransactionsInLevels] transaction is nil at level %d", level)
			}

			// Skip transactions that were already validated (found in cache or UTXO store)
			if txMetaSlice[mTx.idx].isSet {
				u.logger.Debugf("[processTransactionsInLevels] Transaction %s already validated (pre-check), skipping", tx.TxIDChainHash().String())
				return nil
			}

			// Pre-filter the block-scoped accumulator to just this tx's input
			// parents and clone Options so the spawned goroutine never touches
			// the shared accumulator. When blockAccumulator is nil (peer-only
			// caller), filterParentMetadataForInputs returns nil and the
			// per-tx Options carry no ParentMetadata — same effect as the
			// peer-announced path that has no block context.
			perTxOpts := *processedValidatorOptions
			perTxOpts.ParentMetadata = filterParentMetadataForInputs(tx, blockAccumulator)

			g.Go(func() error {
				// Use existing blessMissingTransaction logic for validation
				txMeta, err := u.blessMissingTransaction(gCtx, blockHash, subtreeHash, tx, blockHeight, blockIds, &perTxOpts)
				if err != nil {
					u.logger.Debugf("[processTransactionsInLevels] Failed to validate transaction %s: %v", tx.TxIDChainHash().String(), err)

					// TX_EXISTS is not an error - transaction was already validated
					if errors.Is(err, errors.ErrTxExists) {
						u.logger.Debugf("[processTransactionsInLevels] Transaction %s already exists, skipping", tx.TxIDChainHash().String())
						// Mark as successful since it already exists. The map
						// mutation against blockAccumulator happens after
						// g.Wait() — the goroutine only writes to the
						// per-level slice under a mutex.
						levelSuccessMutex.Lock()
						levelSuccessfulTxs = append(levelSuccessfulTxs, *tx.TxIDChainHash())
						levelSuccessMutex.Unlock()
						return nil
					}

					// Count all other errors
					errorsFound.Add(1)

					if errors.Is(err, errors.ErrTxMissingParent) {
						// missingParentErrors drives the all-missing-parent deferral below;
						// resolution happens in Phase-3 ordered sequential revalidation.
						missingParentErrors.Add(1)
						u.logger.Debugf("[processTransactionsInLevels] Transaction %s missing parent (deferred to sequential revalidation)", tx.TxIDChainHash().String())
					} else if errors.Is(err, errors.ErrTxInvalid) && !errors.Is(err, errors.ErrTxPolicy) {
						// Truly invalid (non-policy) transactions fail the level — no deferral
						// possible because phase 3 revalidation can't resolve these.
						u.logger.Warnf("[processTransactionsInLevels] Invalid transaction detected: %s: %v", tx.TxIDChainHash().String(), err)
						return err
					} else {
						u.logger.Errorf("[processTransactionsInLevels] Processing error for transaction %s: %v", tx.TxIDChainHash().String(), err)
					}

					return nil // Don't fail the entire level
				}

				// Validation succeeded - mark transaction as successful
				levelSuccessMutex.Lock()
				levelSuccessfulTxs = append(levelSuccessfulTxs, *tx.TxIDChainHash())
				levelSuccessMutex.Unlock()

				if txMeta == nil {
					u.logger.Debugf("[processTransactionsInLevels] Transaction metadata is nil for %s", tx.TxIDChainHash().String())
				} else {
					u.logger.Debugf("[processTransactionsInLevels] Successfully validated transaction %s", tx.TxIDChainHash().String())
				}

				return nil
			})
		}

		// Fail early if we get an actual tx error thrown
		if err = g.Wait(); err != nil {
			return errors.NewProcessingError("[processTransactionsInLevels] Failed to process level %d", level+1, err)
		}

		// Synchronisation point: merge this level's successes into the
		// block-scoped accumulator. Safe — all per-tx goroutines for this
		// level have returned (g.Wait above), and the next level's goroutines
		// haven't been spawned yet. Per-tx goroutines never read or write the
		// accumulator directly; they only see their own pre-computed filtered
		// map and write tx hashes into levelSuccessfulTxs under a mutex.
		// acc.add is first-writer-wins: if a tx was already seeded as
		// already-known (or merged in an earlier level) the existing entry
		// is preserved.
		if blockAccumulator != nil {
			for _, txHash := range levelSuccessfulTxs {
				blockAccumulator.add(txHash, &validator.ParentTxMetadata{BlockHeight: blockHeight})
			}
		}

		u.logger.Debugf("[processTransactionsInLevels] Processing level %d/%d with %d transactions DONE", level+1, maxLevel+1, len(levelTxs))

		// PHASE 2 OPTIMIZATION: Release grandparent level (level-2) after current level succeeds
		// Keep current level (being processed) and parent level (level-1) for safety
		// This ensures we always hold at most 2 levels: current + parents
		// Level-2 (grandparents) is safe to release because their outputs are in UTXO store
		if level > 1 {
			txsPerLevel[level-2] = nil
			u.logger.Debugf("[processTransactionsInLevels] Released memory for level %d (grandparent level)", level-2)
		}
	}

	if errorsFound.Load() > 0 {
		// When every error is a missing-parent error, defer the failure to the
		// caller's sequential revalidation pass instead of aborting this batch.
		// A tx's parent can live in a later batch (not yet processed); once all
		// batches are complete and the sequential pass re-validates the failed
		// subtrees in block order, the parent is in the UTXO store and the
		// child resolves. Failing fatally here would skip that recovery and
		// stall the block (observed on teratestnet at block 15,631 where
		// 1,305 of 9,216 txs had cross-subtree parents).
		if errorsFound.Load() == missingParentErrors.Load() {
			u.logger.Infof("[processTransactionsInLevels] %d missing-parent errors (deferred to sequential revalidation)", errorsFound.Load())
			return nil
		}
		return errors.NewProcessingError("[processTransactionsInLevels] Completed processing with %d errors (%d missing-parent)", errorsFound.Load(), missingParentErrors.Load())
	}

	u.logger.Debugf("[processTransactionsInLevels] Successfully processed all %d transactions", totalTxCount)

	txMetaSlice = nil //nolint:ineffassign // Intentional early GC hint

	return nil
}

// buildParentMapFromLevel builds a hash map of all transactions in a level for quick parent lookups.
// This map is built ONCE per level and reused for all child transactions in the next level,
// avoiding O(n²) complexity from rebuilding the map for every child transaction.
func buildParentMapFromLevel(parentLevelTxs []missingTx) map[chainhash.Hash]*bt.Tx {
	if len(parentLevelTxs) == 0 {
		return nil
	}

	parentMap := make(map[chainhash.Hash]*bt.Tx, len(parentLevelTxs))
	for _, mTx := range parentLevelTxs {
		if mTx.tx != nil {
			parentMap[*mTx.tx.TxIDChainHash()] = mTx.tx
		}
	}
	return parentMap
}

// buildParentMetadata creates a map of parent transaction metadata for use by the validator.
// This allows the validator to skip UTXO store lookups for in-block parents.
//
// CRITICAL: Only includes transactions that successfully validated (present in successfulTxs).
// This prevents validation bypass where child references a failed parent transaction.
//
// The metadata includes block height (where the parent will be mined) which is needed
// for coinbase maturity checks and other validation rules.
func buildParentMetadata(parentLevelTxs []missingTx, blockHeight uint32, successfulTxs map[chainhash.Hash]bool) map[chainhash.Hash]*validator.ParentTxMetadata {
	if len(parentLevelTxs) == 0 || len(successfulTxs) == 0 {
		return nil
	}

	metadata := make(map[chainhash.Hash]*validator.ParentTxMetadata, len(successfulTxs))
	for _, mTx := range parentLevelTxs {
		if mTx.tx != nil {
			txHash := *mTx.tx.TxIDChainHash()
			// Only include transactions that successfully validated
			if successfulTxs[txHash] {
				metadata[txHash] = &validator.ParentTxMetadata{
					BlockHeight: blockHeight,
				}
			}
		}
	}
	return metadata
}

// parentMetadataAccumulator carries the block-scoped in-block-parent metadata
// view used by per-tx pre-filtering inside processTransactionsInLevels /
// processMissingTransactions. The view is logically (snapshot ∪ delta):
// reads check delta first, then snapshot; writes go to delta only.
//
// Two usage modes:
//
//   - Single-map (snapshot=nil, delta=live): used by the batch loop in
//     CheckBlockSubtrees and by Phase 3 sequential retries. There is no
//     concurrency, so delta IS the live accumulator and per-level merges
//     write directly into it.
//
//   - Snapshot+delta (snapshot=live, delta=per-subtree-fresh): used by
//     Phase 2 parallel subtree validation. Multiple goroutines hold their
//     own accumulator value pointing at the SAME snapshot map (read-only
//     access, no synchronization required) and their OWN fresh delta map.
//     After Phase 2 g.Wait(), the successful subtrees' deltas are merged
//     into the live accumulator in block-subtree order. This avoids the
//     O(len(missingSubtrees) * len(accumulator)) copy churn that a
//     per-subtree full snapshot would incur on large blocks.
//
// A nil *parentMetadataAccumulator is the peer-announced path: per-tx
// filtering is a no-op and per-level merges are skipped — preserves the
// pre-accumulator peer-validation behaviour.
type parentMetadataAccumulator struct {
	snapshot map[chainhash.Hash]*validator.ParentTxMetadata
	delta    map[chainhash.Hash]*validator.ParentTxMetadata
}

// lookup returns the metadata for h, checking delta first then snapshot.
// Returns nil if h is in neither map (or the accumulator itself is nil).
func (a *parentMetadataAccumulator) lookup(h chainhash.Hash) *validator.ParentTxMetadata {
	if a == nil {
		return nil
	}
	if a.delta != nil {
		if m, ok := a.delta[h]; ok {
			return m
		}
	}
	if a.snapshot != nil {
		if m, ok := a.snapshot[h]; ok {
			return m
		}
	}
	return nil
}

// add inserts (h, m) into delta when h is not already covered by snapshot
// or by a prior delta entry. First-writer-wins; preserves the consensus
// invariant that an in-block parent's BlockHeight is recorded ONCE per block
// (the height of the block being validated). Callers must serialise add()
// calls on the same accumulator value — single-map mode relies on per-level
// g.Wait() barriers; snapshot+delta mode gives each delta to exactly one
// goroutine.
func (a *parentMetadataAccumulator) add(h chainhash.Hash, m *validator.ParentTxMetadata) {
	if a == nil {
		return
	}
	if a.lookup(h) != nil {
		return
	}
	if a.delta == nil {
		a.delta = make(map[chainhash.Hash]*validator.ParentTxMetadata)
	}
	a.delta[h] = m
}

// filterParentMetadataForInputs returns the subset of accumulator entries
// whose hashes appear in this transaction's input prevouts. Used as the
// per-tx pre-filter step before spawning a validation goroutine, so the
// goroutine body never touches the shared block-scoped accumulator and the
// gRPC/HTTP request only carries the (typically small) set of in-block-parent
// metadata this tx actually needs — not the full block accumulator.
//
// Returns nil when there are no matches or when either input is empty. A nil
// result is semantically identical to "no ParentMetadata supplied" downstream
// and round-trips through proto as an absent field.
//
// Pre-filtering happens on the caller goroutine before g.Go, so this helper
// is called single-threaded for each tx; safe even though Go map iteration
// itself is not concurrent-write-safe.
func filterParentMetadataForInputs(tx *bt.Tx, acc *parentMetadataAccumulator) map[chainhash.Hash]*validator.ParentTxMetadata {
	if tx == nil || acc == nil {
		return nil
	}
	var out map[chainhash.Hash]*validator.ParentTxMetadata
	for _, in := range tx.Inputs {
		if in == nil {
			continue
		}
		hp := in.PreviousTxIDChainHash()
		if hp == nil {
			continue
		}
		if meta := acc.lookup(*hp); meta != nil {
			if out == nil {
				// Lazy-allocate: most txs have zero in-block parents in the
				// accumulator, so we avoid the map allocation in the common case.
				out = make(map[chainhash.Hash]*validator.ParentTxMetadata, len(tx.Inputs))
			}
			out[*hp] = meta
		}
	}
	return out
}

// extendTxWithInBlockParents extends a transaction's inputs with parent output data
// from a pre-built parent map, avoiding Aerospike fetches for intra-block dependencies.
// This is a critical optimization that eliminates ~500MB+ of UTXO store fetches per block.
//
// Sets the transaction as extended only if ALL inputs are successfully extended.
func extendTxWithInBlockParents(tx *bt.Tx, parentMap map[chainhash.Hash]*bt.Tx) int {
	if tx == nil || len(parentMap) == 0 {
		return 0
	}

	// Skip if already extended
	if tx.IsExtended() {
		return 0
	}

	extendedCount := 0
	allInputsExtended := true

	for _, input := range tx.Inputs {
		parentHash := input.PreviousTxIDChainHash()
		if parentHash == nil {
			continue // Input doesn't need extension
		}

		// Try to extend this input
		parentTx, found := parentMap[*parentHash]
		if !found || int(input.PreviousTxOutIndex) >= len(parentTx.Outputs) {
			allInputsExtended = false
			continue
		}

		// Extend this input
		output := parentTx.Outputs[input.PreviousTxOutIndex]
		input.PreviousTxSatoshis = output.Satoshis
		input.PreviousTxScript = output.LockingScript
		extendedCount++
	}

	// Only mark as fully extended if we successfully extended all inputs
	if allInputsExtended && extendedCount > 0 {
		tx.SetExtended(true)
	}

	return extendedCount
}

// validateSubtreeLeafCount rejects peer-supplied leaf counts that exceed the
// configured policy cap before they reach allocation paths such as
// subtreepkg.NewIncompleteTreeByLeafCount, where the capacity argument would
// otherwise drive an unbounded make() backed by attacker-controlled bytes.
func validateSubtreeLeafCount(subtreeHash chainhash.Hash, leafCount, policyMax int) error {
	if leafCount > policyMax {
		return errors.NewProcessingError("[CheckBlockSubtrees][%s] subtree response exceeds policy max %d nodes (got %d)",
			subtreeHash.String(), policyMax, leafCount)
	}

	return nil
}

// fetchCandidateParentMedianTime returns the candidate-parent MTP for the
// post-CSV consensus path. See the equivalent helper in services/legacy/netsync
// for the rationale of the two-step fallback: the batched API is cache-friendly
// but its in-process cache is keyed by (parentHash, count), so a reorg-race
// poisoning of that cache entry would re-trigger the same re-anchor failure on
// retry. Falling back to a hash-keyed parent-chain walk is race-safe because
// GetBlockHeader's cache is keyed by hash and block contents are immutable.
func (u *Server) fetchCandidateParentMedianTime(ctx context.Context, parentHash *chainhash.Hash) (uint32, error) {
	if parentHash == nil {
		return 0, errors.NewProcessingError("nil parent hash")
	}

	headers, _, err := u.blockchainClient.GetBlockHeaders(ctx, parentHash, blockchain.MedianTimeBlocks)
	if err != nil {
		return 0, errors.NewProcessingError("parent hash %s: failed to fetch parent-chain headers", parentHash.String(), err)
	}

	mtp, anchorErr := candidateParentMedianTimeFromHeaders(parentHash, headers)
	if anchorErr == nil {
		return mtp, nil
	}

	walked, walkErr := u.walkParentChain(ctx, parentHash, blockchain.MedianTimeBlocks)
	if walkErr != nil {
		return 0, errors.NewProcessingError("parent hash %s: batched-API re-anchor failed (%v); fallback walk failed", parentHash.String(), anchorErr, walkErr)
	}

	mtp, err = candidateParentMedianTimeFromHeaders(parentHash, walked)
	if err != nil {
		return 0, errors.NewProcessingError("parent hash %s: re-anchor failed on both batched fetch (%v) and hash-walk fallback", parentHash.String(), anchorErr, err)
	}

	return mtp, nil
}

// walkParentChain fetches exactly depth block headers starting at startHash and
// walking backwards via HashPrevBlock. See the equivalent helper in
// services/legacy/netsync for the rationale — duplicated by design (small,
// internal, avoids a new shared util package).
//
// nil pointers and nil header responses are hard errors: production callers
// only invoke this at heights at or above CSVHeight, well past the chain's
// first `depth` blocks, so we never legitimately walk off the beginning.
// Tolerating short returns would silently produce an incomplete MTP on a
// transient cache miss; raising loudly forces the caller to surface it.
func (u *Server) walkParentChain(ctx context.Context, startHash *chainhash.Hash, depth uint64) ([]*model.BlockHeader, error) {
	headers := make([]*model.BlockHeader, 0, depth)
	cur := startHash

	for i := uint64(0); i < depth; i++ {
		if cur == nil {
			return nil, errors.NewProcessingError("walkParentChain: nil prev-block link at depth %d (walked off the chain)", i)
		}

		header, _, err := u.blockchainClient.GetBlockHeader(ctx, cur)
		if err != nil {
			return nil, errors.NewProcessingError("walkParentChain: failed at depth %d (hash %s)", i, cur.String(), err)
		}

		if header == nil {
			return nil, errors.NewProcessingError("walkParentChain: nil header at depth %d (hash %s) — possible transient cache miss", i, cur.String())
		}

		headers = append(headers, header)
		cur = header.HashPrevBlock
	}

	return headers, nil
}

// candidateParentMedianTimeFromHeaders verifies that the supplied headers form
// a contiguous chain ending at parentHash, then returns the median of their
// timestamps. See the equivalent helper in services/legacy/netsync for the
// full rationale — the function is duplicated by design (small, internal,
// avoids a new shared util package) and both copies must hold the same
// contract.
//
// The verification closes a concurrency gap in
// blockchainClient.GetBlockHeaders: its main-chain fast path probes the start
// hash's on_main_chain status in one SQL statement and then runs the SELECT
// that returns the headers in a second statement. A reorg fired between the
// two statements (READ COMMITTED isolation) would return main-chain headers
// at the same height range that no longer correspond to parentHash — silently
// swapping the timestamp set we compute MTP over. Re-anchoring the result
// locally is O(11) and bulletproof: we check that the newest returned header
// equals parentHash and that each consecutive pair is linked via
// HashPrevBlock → Hash().
//
// Empty input and any verification failure surface as a hard error: silently
// returning 0 would let the caller pass Options.CandidateParentMedianTime=0
// to the validator, which now rejects post-CSV consensus requests with a
// missing parent MTP (no tip-MTP soft-fall) — but the error here gives a
// more precise diagnostic at the source rather than waiting for the
// validator's downstream rejection.
func candidateParentMedianTimeFromHeaders(parentHash *chainhash.Hash, headers []*model.BlockHeader) (uint32, error) {
	if len(headers) == 0 {
		return 0, errors.NewProcessingError("cannot compute median timestamp from zero headers")
	}

	if parentHash == nil {
		return 0, errors.NewProcessingError("nil parent hash")
	}

	// Each element is guarded against nil — production paths (SQL store,
	// gRPC client) do not emit nil entries, but the helper is meant to
	// hard-fail on bad header data rather than panic.
	if headers[0] == nil {
		return 0, errors.NewProcessingError("nil header at depth 0")
	}

	headHash := headers[0].Hash()
	if headHash == nil || !headHash.IsEqual(parentHash) {
		return 0, errors.NewProcessingError("returned chain head does not match requested parent hash (possible reorg between header probe and fetch)")
	}

	for i := 1; i < len(headers); i++ {
		if headers[i] == nil {
			return 0, errors.NewProcessingError("nil header at depth %d", i)
		}

		prev := headers[i-1].HashPrevBlock
		cur := headers[i].Hash()
		if prev == nil || cur == nil || !prev.IsEqual(cur) {
			return 0, errors.NewProcessingError("parent-chain link broken at depth %d (possible reorg between header probe and fetch)", i)
		}
	}

	timestamps := make([]uint32, len(headers))
	for i, h := range headers {
		timestamps[i] = h.Timestamp
	}

	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })

	return timestamps[len(timestamps)/2], nil
}
