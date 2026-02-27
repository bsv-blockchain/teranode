package blockvalidation

import (
	"bufio"
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
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
	SubtreeBytes  []byte
	BlockHash     string // For logging
	BlockHeight   uint32 // For DAH calculation
	SubtreeIdx    int    // For logging
	AlreadyExists bool   // Skip write if already exists
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

			// Write the subtree file with finite DAH (temporary until block persister confirms)
			dah := job.BlockHeight + u.subtreeBlockHeightRetention
			if err := u.subtreeStore.Set(ctx,
				job.SubtreeHash[:],
				fileformat.FileTypeSubtree,
				job.SubtreeBytes,
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
func (u *BlockValidation) buildSubtreeAndQueueWrite(ctx context.Context, block *model.Block, subtreeIdx int, subtree *subtreepkg.Subtree, txs []*bt.Tx, subtreeHash chainhash.Hash, fullSubtreeExists bool) (*SubtreeWriteJob, error) {
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
		fee, err := util.GetFees(tx)
		if err != nil {
			return nil, errors.NewProcessingError("[buildSubtreeAndQueueWrite][%s] failed to get fee for tx %s in subtree %s", block.Hash().String(), tx.TxIDChainHash().String(), subtreeHash.String(), err)
		}

		sizeInBytes := uint64(tx.Size())

		if err = fullSubtree.AddNode(*tx.TxIDChainHash(), fee, sizeInBytes); err != nil {
			return nil, errors.NewProcessingError("[buildSubtreeAndQueueWrite][%s] failed to add tx node %s to full subtree %s", block.Hash().String(), tx.TxIDChainHash().String(), subtreeHash.String(), err)
		}
	}

	// Set on block for merkle validation (synchronous)
	block.SubtreeSlices[subtreeIdx] = fullSubtree

	// Serialize for async write
	subtreeBytes, err := fullSubtree.Serialize()
	if err != nil {
		return nil, errors.NewProcessingError("[buildSubtreeAndQueueWrite][%s] failed to serialize full subtree %s", block.Hash().String(), subtreeHash.String(), err)
	}

	return &SubtreeWriteJob{
		SubtreeHash:   subtreeHash,
		SubtreeBytes:  subtreeBytes,
		BlockHash:     block.Hash().String(),
		BlockHeight:   block.Height,
		SubtreeIdx:    subtreeIdx,
		AlreadyExists: false,
	}, nil
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
func (u *BlockValidation) quickValidateBlock(ctx context.Context, block *model.Block, baseURL string) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "quickValidateBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[quickValidateBlock][%s] performing quick validation for checkpointed block at height %d", block.Hash().String(), block.Height),
	)
	defer deferFn()

	var (
		err error
		id  uint64
	)

	if len(block.Subtrees) > 0 {
		// Process all subtrees in streaming fashion - creates UTXOs, spends, writes files
		// This function waits for all processing to complete before returning, ensuring block.ID is set
		_, err = u.processBlockSubtrees(ctx, block)
		if err != nil {
			return errors.NewProcessingError("[quickValidateBlock][%s] failed to process block subtrees", block.Hash().String(), err)
		}

		// Verify block ID was assigned during processing (sanity check)
		if block.ID == 0 {
			return errors.NewProcessingError("[quickValidateBlock][%s] block ID was not assigned during subtree processing", block.Hash().String())
		}
	} else {
		// No subtrees to process, get next block ID
		id, err = u.blockchainClient.GetNextBlockID(ctx)
		if err != nil {
			return errors.NewProcessingError("[quickValidateBlock][%s] failed to get next block ID", block.Hash().String(), err)
		}
		block.ID = uint32(id) // nolint:gosec
	}

	// add block directly to blockchain
	if err = u.blockchainClient.AddBlock(ctx,
		block,
		baseURL,
		options.WithSubtreesSet(true),
		options.WithMinedSet(true),
		options.WithID(uint64(block.ID)),
	); err != nil {
		return errors.NewProcessingError("[quickValidateBlock][%s] failed to add block to blockchain", block.Hash().String(), err)
	}

	// Unlock all UTXOs - final commit point
	if err = u.unlockSubtreeTransactions(ctx, block.SubtreeSlices); err != nil {
		return errors.NewProcessingError("[quickValidateBlock][%s] failed to unlock UTXOs", block.Hash().String(), err)
	}

	// Update subtrees DAH and send BlockSubtreesSet notification
	// This matches the normal validation flow and ensures:
	// 1. Subtree retention periods are properly managed
	// 2. BlockSubtreesSet notification is sent to trigger setMinedChan
	// 3. Transactions are marked as mined in the UTXO store
	if err = u.updateSubtreesDAH(ctx, block); err != nil {
		return errors.NewProcessingError("[quickValidateBlock][%s] failed to update subtrees DAH", block.Hash().String(), err)
	}

	// Mark block as existing in cache
	if err = u.SetBlockExists(block.Hash()); err != nil {
		u.logger.Errorf("[ValidateBlock][%s] failed to set block exists cache: %s", block.Hash().String(), err)
	}

	return nil
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
func (u *BlockValidation) quickValidateBlockAsync(ctx context.Context, block *model.Block, baseURL string, writeJobsChan chan<- *SubtreeWriteJob) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "quickValidateBlockAsync",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[quickValidateBlockAsync][%s] performing async quick validation for checkpointed block at height %d", block.Hash().String(), block.Height),
	)
	defer deferFn()

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
		_, err = u.processBlockSubtreesPipelineAsync(ctx, block, prefetchDepth, writeJobsChan)
		if err != nil {
			return errors.NewProcessingError("[quickValidateBlockAsync][%s] failed to process block subtrees", block.Hash().String(), err)
		}
	}

	// If no block ID was assigned during processing, get next block ID
	if block.ID == 0 {
		id, err = u.blockchainClient.GetNextBlockID(ctx)
		if err != nil {
			return errors.NewProcessingError("[quickValidateBlockAsync][%s] failed to get next block ID", block.Hash().String(), err)
		}
		block.ID = uint32(id) // nolint:gosec
	}

	// add block directly to blockchain
	if err = u.blockchainClient.AddBlock(ctx,
		block,
		baseURL,
		options.WithSubtreesSet(true),
		options.WithMinedSet(true),
		options.WithID(uint64(block.ID)),
	); err != nil {
		return errors.NewProcessingError("[quickValidateBlockAsync][%s] failed to add block to blockchain", block.Hash().String(), err)
	}

	// Unlock all UTXOs - final commit point
	if err = u.unlockSubtreeTransactions(ctx, block.SubtreeSlices); err != nil {
		return errors.NewProcessingError("[quickValidateBlockAsync][%s] failed to unlock UTXOs", block.Hash().String(), err)
	}

	// Update subtrees DAH and send BlockSubtreesSet notification
	if err = u.updateSubtreesDAH(ctx, block); err != nil {
		return errors.NewProcessingError("[quickValidateBlockAsync][%s] failed to update subtrees DAH", block.Hash().String(), err)
	}

	// Mark block as existing in cache
	if err = u.SetBlockExists(block.Hash()); err != nil {
		u.logger.Errorf("[quickValidateBlockAsync][%s] failed to set block exists cache: %s", block.Hash().String(), err)
	}

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
func (u *BlockValidation) processBlockSubtrees(ctx context.Context, block *model.Block) (uint64, error) {
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
		return u.processBlockSubtreesSequential(ctx, block)
	}
	return u.processBlockSubtreesPipeline(ctx, block, prefetchDepth)
}

// processBlockSubtreesSequential processes subtrees sequentially, one batch at a time.
// This is the fallback when SubtreeBatchPrefetchDepth is 0.
func (u *BlockValidation) processBlockSubtreesSequential(ctx context.Context, block *model.Block) (uint64, error) {
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
		batch, err := u.processSubtreeBatch(ctx, block, batchStart, batchEnd, extendedTxs)
		if err != nil {
			return 0, err
		}

		// Phase 4: Check for retry and get block ID (only on first batch)
		// This is specific to quick validation to handle retries gracefully
		if !blockIDSet && len(batch.batchTxs) > 0 {
			existingMeta, err := u.utxoStore.Get(ctx, batch.batchTxs[0].TxIDChainHash(), fields.BlockIDs)
			if err == nil && existingMeta != nil && len(existingMeta.BlockIDs) > 0 {
				existingBlockID = uint64(existingMeta.BlockIDs[0])
				block.ID = existingMeta.BlockIDs[0]
				u.logger.Debugf("[processBlockSubtreesSequential][%s] reusing BlockID %d from retry", block.Hash().String(), existingBlockID)
			} else if block.ID == 0 {
				id, err := u.blockchainClient.GetNextBlockID(ctx)
				if err != nil {
					return 0, errors.NewProcessingError("[processBlockSubtreesSequential][%s] failed to get block ID", block.Hash().String(), err)
				}
				block.ID = uint32(id) // nolint:gosec
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
//   - All UTXOs are created with WithLocked(true), preventing other operations from using them
//   - If processing fails, locked UTXOs remain in the UTXO store until unlockSubtreeTransactions
//   - Since unlockSubtreeTransactions is never called on error, partial changes are effectively rolled back
//   - On retry, Create() returns ErrTxExists, and SetMinedMulti() updates the correct BlockID
func (u *BlockValidation) processBlockSubtreesPipeline(ctx context.Context, block *model.Block, prefetchDepth int) (uint64, error) {
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
			batch, err := u.prefetchSubtreeBatch(gCtx, block, batchStart, batchEnd)
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
					id, err := u.blockchainClient.GetNextBlockID(gCtx)
					if err != nil {
						return errors.NewProcessingError("[processBlockSubtreesPipeline][%s] failed to get block ID", block.Hash().String(), err)
					}
					block.ID = uint32(id) // nolint:gosec
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
//   - error: If processing fails or context is cancelled
func (u *BlockValidation) processBlockSubtreesPipelineAsync(ctx context.Context, block *model.Block, prefetchDepth int, writeJobsChan chan<- *SubtreeWriteJob) (uint64, error) {
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
			batch, err := u.prefetchSubtreeBatch(gCtx, block, batchStart, batchEnd)
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
					id, err := u.blockchainClient.GetNextBlockID(gCtx)
					if err != nil {
						return errors.NewProcessingError("[processBlockSubtreesPipelineAsync][%s] failed to get block ID", block.Hash().String(), err)
					}
					block.ID = uint32(id) // nolint:gosec
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
	// get the subtree from disk, should be in .subtreeToCheck
	subtreeReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	if err != nil {
		return subtreeResult{err: errors.NewNotFoundError("[getBlockTransactions][%s] failed to get subtree %s", block.Hash().String(), subtreeHash.String(), err)}
	}
	defer subtreeReader.Close()

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
			// Fallback to heap on mmap failure — reset reader and retry
			u.logger.Warnf("[getBlockTransactions][%s] mmap deserialization failed for subtree %s, falling back to heap: %v", block.Hash().String(), subtreeHash.String(), err)
			bufferedReader.Reset(subtreeReader)
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

	// Check if full .subtree file already exists (for retry scenarios)
	// This check is done here during prefetch I/O to avoid separate disk access during build phase
	fullSubtreeExists, _ := u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)

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
func (u *BlockValidation) writeSubtreeFilesFromTxs(ctx context.Context, block *model.Block, subtreeIdx int, subtree *subtreepkg.Subtree, txs []*bt.Tx, subtreeHash chainhash.Hash) error {
	fullSubtreeExists, err := u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		return errors.NewProcessingError("[writeSubtreeFilesFromTxs][%s] failed to check existence of full subtree %s", block.Hash().String(), subtreeHash.String(), err)
	}

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
			// computes TxInpoints (only needed for subtreeMeta, which we skip during quick validation)
			fee, err := util.GetFees(tx)
			if err != nil {
				return errors.NewProcessingError("[writeSubtreeFilesFromTxs][%s] failed to get fee for tx %s in subtree %s", block.Hash().String(), tx.TxIDChainHash().String(), subtreeHash.String(), err)
			}

			sizeInBytes := uint64(tx.Size())

			if err = fullSubtree.AddNode(*tx.TxIDChainHash(), fee, sizeInBytes); err != nil {
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

// unlockSubtreeTransactions unlocks all transactions in the given subtrees in parallel.
// It skips the coinbase placeholder at index 0 of the first subtree.
func (u *BlockValidation) unlockSubtreeTransactions(ctx context.Context, subtrees []*subtreepkg.Subtree) error {
	if len(subtrees) == 0 {
		return nil
	}

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, 128)

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

	// fullSubtreeExists tracks which subtrees already have full .subtree files
	// Populated during prefetch to avoid disk I/O during build phase
	fullSubtreeExists []bool

	// batchStart is the global starting index in block.Subtrees
	batchStart int

	// batchEnd is the global ending index (exclusive) in block.Subtrees
	batchEnd int
}

// Close releases mmap-backed subtree resources in this batch.
func (b *SubtreeProcessingBatch) Close() {
	for _, st := range b.subtrees {
		if st != nil {
			st.Close()
		}
	}
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
	}

	// Phase 1: Read subtrees in parallel
	subtreeChannels := make([]chan subtreeResult, batchSize)
	for i := range subtreeChannels {
		subtreeChannels[i] = make(chan subtreeResult, 1)
	}

	readerCtx, cancelReaders := context.WithCancel(ctx)
	g, gCtx := errgroup.WithContext(readerCtx)
	util.SafeSetLimit(g, 128)

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
			if !tx.IsExtended() {
				needsExternalLookup := false
				for j, input := range tx.Inputs {
					parentHash := input.PreviousTxIDChainHash()
					if parentTx, ok := extendedTxsFromPrevBatches[*parentHash]; ok {
						tx.Inputs[j].PreviousTxSatoshis = parentTx.Outputs[input.PreviousTxOutIndex].Satoshis
						tx.Inputs[j].PreviousTxScript = parentTx.Outputs[input.PreviousTxOutIndex].LockingScript
					} else {
						needsExternalLookup = true
					}
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

	// Phase 3: Extend remaining transactions in parallel using UTXO store
	if len(txsNeedingExtension) > 0 {
		extendG, extendCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(extendG, 256)

		for _, tx := range txsNeedingExtension {
			tx := tx
			extendG.Go(func() error {
				return u.utxoStore.PreviousOutputsDecorate(extendCtx, tx)
			})
		}

		if err := extendG.Wait(); err != nil {
			cancelReaders()
			return nil, errors.NewProcessingError("[processSubtreeBatch][%s] failed to extend transactions: %v", block.Hash().String(), err)
		}

		// Verify all inputs are now extended
		for _, tx := range txsNeedingExtension {
			for j, input := range tx.Inputs {
				if input.PreviousTxSatoshis == 0 && input.PreviousTxScript == nil {
					parentHash := input.PreviousTxIDChainHash()
					cancelReaders()
					return nil, errors.NewProcessingError(
						"[processSubtreeBatch][%s] parent tx %s not found for input %d of tx %s",
						block.Hash().String(), parentHash.String(), j, tx.TxIDChainHash().String())
				}
			}
		}
	}

	cancelReaders()
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

	// Phase 1: Create UTXOs in parallel, collecting any that already exist
	createG, createCtx := errgroup.WithContext(ctx)
	// Set concurrency to 8x StoreBatcherSize to allow sufficient parallelism while the
	// UTXO store batches operations internally. This multiplier balances throughput with
	// resource usage, allowing multiple batches to be in flight simultaneously.
	util.SafeSetLimit(createG, u.settings.UtxoStore.StoreBatcherSize*8)

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
			sIdx := globalSubtreeIdx
			createG.Go(func() error {
				_, err := u.utxoStore.Create(createCtx, tx, block.Height, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
					BlockID:     block.ID,
					BlockHeight: block.Height,
					SubtreeIdx:  sIdx,
				}), utxo.WithLocked(true))
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
	// This handles the case where a previous attempt created UTXOs with a different block ID
	if len(existingTxHashes) > 0 {
		if _, err := u.utxoStore.SetMinedMulti(ctx, existingTxHashes, minedBlockInfo); err != nil {
			return errors.NewProcessingError("[createAndSpendUTXOsForBatch][%s] failed to update mined info for %d existing txs", block.Hash().String(), len(existingTxHashes), err)
		}
	}

	// Phase 2: Spend all transactions in parallel
	spendG, spendCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(spendG, u.settings.UtxoStore.SpendBatcherSize*u.settings.UtxoStore.SpendBatcherConcurrency*2)

	for _, tx := range batch.batchTxs {
		tx := tx
		spendG.Go(func() error {
			if _, err := u.utxoStore.Spend(spendCtx, tx, block.Height, utxo.IgnoreFlags{IgnoreLocked: true}); err != nil {
				return errors.NewProcessingError("[createAndSpendUTXOsForBatch][%s] failed to spend tx %s", block.Hash().String(), tx.TxIDChainHash().String(), err)
			}
			return nil
		})
	}

	return spendG.Wait()
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
	util.SafeSetLimit(writeG, u.settings.BlockValidation.SubtreeBatchWriteConcurrency)

	batchSize := batch.batchEnd - batch.batchStart
	for i := 0; i < batchSize; i++ {
		globalIdx := batch.batchStart + i
		localIdx := i
		subtree := batch.subtrees[localIdx]
		txRange := batch.txRanges[localIdx]
		subtreeTxs := batch.batchTxs[txRange[0]:txRange[1]]
		subtreeHash := batch.subtreeHashes[localIdx]

		writeG.Go(func() error {
			return u.writeSubtreeFilesFromTxs(writeCtx, block, globalIdx, subtree, subtreeTxs, subtreeHash)
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
	util.SafeSetLimit(buildG, u.settings.BlockValidation.SubtreeBatchWriteConcurrency)

	batchSize := batch.batchEnd - batch.batchStart
	jobs := make([]*SubtreeWriteJob, batchSize)
	var jobsMu sync.Mutex

	for i := 0; i < batchSize; i++ {
		globalIdx := batch.batchStart + i
		localIdx := i
		subtree := batch.subtrees[localIdx]
		txRange := batch.txRanges[localIdx]
		subtreeTxs := batch.batchTxs[txRange[0]:txRange[1]]
		subtreeHash := batch.subtreeHashes[localIdx]
		fullSubtreeExists := batch.fullSubtreeExists[localIdx]

		buildG.Go(func() error {
			job, err := u.buildSubtreeAndQueueWrite(buildCtx, block, globalIdx, subtree, subtreeTxs, subtreeHash, fullSubtreeExists)
			if err != nil {
				return err
			}
			jobsMu.Lock()
			jobs[localIdx] = job
			jobsMu.Unlock()
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
	}

	// Read subtrees in parallel
	subtreeChannels := make([]chan subtreeResult, batchSize)
	for i := range subtreeChannels {
		subtreeChannels[i] = make(chan subtreeResult, 1)
	}

	readerCtx, cancelReaders := context.WithCancel(ctx)
	g, gCtx := errgroup.WithContext(readerCtx)
	util.SafeSetLimit(g, 128)

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
			if !tx.IsExtended() {
				needsExternalLookup := false
				for j, input := range tx.Inputs {
					parentHash := input.PreviousTxIDChainHash()
					if parentTx, ok := extendedTxs[*parentHash]; ok {
						tx.Inputs[j].PreviousTxSatoshis = parentTx.Outputs[input.PreviousTxOutIndex].Satoshis
						tx.Inputs[j].PreviousTxScript = parentTx.Outputs[input.PreviousTxOutIndex].LockingScript
					} else {
						needsExternalLookup = true
					}
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

	// Extend remaining transactions in parallel using UTXO store
	if len(txsNeedingExtension) > 0 {
		extendG, extendCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(extendG, 256)

		for _, tx := range txsNeedingExtension {
			tx := tx
			extendG.Go(func() error {
				return u.utxoStore.PreviousOutputsDecorate(extendCtx, tx)
			})
		}

		if err := extendG.Wait(); err != nil {
			return errors.NewProcessingError("[extendBatch][%s] failed to extend transactions: %v", block.Hash().String(), err)
		}

		// Verify all inputs are now extended
		for _, tx := range txsNeedingExtension {
			for j, input := range tx.Inputs {
				if input.PreviousTxSatoshis == 0 && input.PreviousTxScript == nil {
					parentHash := input.PreviousTxIDChainHash()
					return errors.NewProcessingError(
						"[extendBatch][%s] parent tx %s not found for input %d of tx %s",
						block.Hash().String(), parentHash.String(), j, tx.TxIDChainHash().String())
				}
			}
		}
	}

	return nil
}
