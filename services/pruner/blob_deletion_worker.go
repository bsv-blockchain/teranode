package pruner

import (
	"context"
	"strconv"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/util"
)

// deletionResult distinguishes between a file that was actually removed from disk
// vs one that was already absent. Both are non-errors, but tracking the difference
// lets us detect misconfiguration (e.g. wrong volume mount) where every file appears
// "already missing".
type deletionResult int

const (
	deletionDeleted  deletionResult = iota // file existed and was removed
	deletionNotFound                       // file was already absent (idempotent success)
)

func (s *Server) blobDeletionWorker() {
	for {
		select {
		case <-s.ctx.Done():
			return

		case sig := <-s.blobNotify:
			s.logger.Debugf("Processing blob deletions for height %d", sig.blockHeight)
			s.processBlobDeletionsAtHeight(sig.blockHeight, sig.blockHash)
		}
	}
}

func (s *Server) processBlobDeletionsAtHeight(blockHeight uint32, blockHash chainhash.Hash) {
	if s.settings.Pruner.SkipBlobDeletion {
		return
	}

	ctx := s.ctx
	blockHashStr := blockHash.String()

	// Wait for block to be mined (only in OnBlockMined trigger mode AND when block assembly is running)
	// OnBlockPersisted mode receives notifications after block is already mined
	if s.settings.Pruner.BlockTrigger == settings.PrunerBlockTriggerOnBlockMined && s.blockAssemblyClient != nil {
		s.logger.Debugf("[pruner][%s:%d] blob deletion: waiting for mined_set=true", blockHashStr, blockHeight)
		if !s.waitForBlockMinedStatus(ctx, &blockHash) {
			s.logger.Warnf("[pruner][%s:%d] blob deletion: skipped - timeout waiting for mined_set", blockHashStr, blockHeight)
			return
		}
	}

	// Safety check: block assembly must be running
	if !s.checkBlockAssemblySafeForPruner(ctx, "blob deletion", blockHeight) {
		return
	}

	// Safety check: respect persisted height
	persistedHeight := s.lastPersistedHeight.Load()
	safetyWindow := s.settings.Pruner.BlobDeletionSafetyWindow
	safeHeight := blockHeight

	if persistedHeight > 0 && safetyWindow > 0 {
		if blockHeight > persistedHeight+safetyWindow {
			s.logger.Debugf("Skipping blob deletion at height %d (persisted: %d, safety: %d)",
				blockHeight, persistedHeight, safetyWindow)
			return
		}
		if persistedHeight > safetyWindow {
			safeHeight = persistedHeight - safetyWindow
		}
	}

	batchSize := s.settings.Pruner.BlobDeletionBatchSize
	lockTimeout := 300 // 5 minutes - should be plenty for processing
	maxRetries := s.settings.Pruner.BlobDeletionMaxRetries

	// Track totals across all batches
	var totalDeleted, totalNotFound, totalFail int64
	var batchNum int
	overallStartTime := time.Now()

	// Loop through batches until no more deletions available
	for {
		batchNum++

		// Acquire batch with locking from blockchain service (uses SELECT...FOR UPDATE SKIP LOCKED)
		batchToken, deletions, err := s.blockchainClient.AcquireBlobDeletionBatch(ctx, safeHeight, int(batchSize), lockTimeout)
		if err != nil {
			s.logger.Errorf("[pruner][%s:%d] blob deletion: failed to acquire batch %d: %v", blockHashStr, blockHeight, batchNum, err)
			blobDeletionErrorsTotal.WithLabelValues("acquisition").Inc()
			break
		}

		if batchToken == "" || len(deletions) == 0 {
			if batchNum == 1 {
				s.logger.Debugf("[pruner][%s:%d] blob deletion: no blob deletions available", blockHashStr, blockHeight)
			}
			break
		}

		batchStartTime := time.Now()
		s.logger.Infof("[pruner][%s:%d] blob deletion: acquired batch %d with %s deletions", blockHashStr, blockHeight, batchNum, util.FormatComma(int64(len(deletions))))

		// Track completed and failed deletions for this batch
		completedIDs := make([]int64, 0, len(deletions))
		failedIDs := make([]int64, 0, len(deletions))

		var deletedCount, notFoundCount, failCount int64

		// Process all deletions in the batch
		for i, deletion := range deletions {
			storeType := storetypes.BlobStoreType(deletion.StoreType)

			s.logger.Debugf("[pruner][%s:%d] blob deletion: processing deletion %s/%s (id=%d, key=%x)",
				blockHashStr, blockHeight, util.FormatComma(int64(i+1)), util.FormatComma(int64(len(deletions))), deletion.Id, deletion.BlobKey)

			result, err := s.processOneDeletion(ctx, deletion, blockHashStr, blockHeight)
			if err != nil {
				s.logger.Warnf("[pruner][%s:%d] blob deletion: failed to delete blob %x from %s (attempt %d/%d): %v",
					blockHashStr, blockHeight, deletion.BlobKey, storeType.String(),
					int(deletion.RetryCount)+1, maxRetries, err)

				failedIDs = append(failedIDs, deletion.Id)

				// Check if this will be removed due to max retries
				if int(deletion.RetryCount)+1 >= maxRetries {
					s.logger.Errorf("[pruner][%s:%d] blob deletion: blob %x will be removed after %d failed attempts",
						blockHashStr, blockHeight, deletion.BlobKey, maxRetries)
					blobDeletionErrorsTotal.WithLabelValues(storeType.String()).Inc()
					failCount++
				}
			} else {
				completedIDs = append(completedIDs, deletion.Id)
				storeID := storetypes.BlobStoreType(deletion.StoreType).String()
				switch result {
				case deletionDeleted:
					deletedCount++
					blobDeletionProcessedTotal.Inc()
				case deletionNotFound:
					notFoundCount++
					blobDeletionNotFoundTotal.WithLabelValues(storeID).Inc()
				}
			}
		}

		// Warn when an entire batch has no files on disk — strong signal of a
		// volume mount misconfiguration (e.g. pruner cannot see the real blob store).
		if notFoundCount > 0 && notFoundCount == int64(len(deletions)) {
			// Log the store type(s) actually present in this batch, not a hardcoded type.
			storeTypes := make(map[storetypes.BlobStoreType]struct{})
			for _, d := range deletions {
				storeTypes[storetypes.BlobStoreType(d.StoreType)] = struct{}{}
			}
			storeInfo := make([]string, 0, len(storeTypes))
			for st := range storeTypes {
				urlStr := st.String()
				if u, err := s.settings.GetBlobStoreURL(int32(st)); err == nil && u != nil {
					urlStr = st.String() + "=" + u.String()
				}
				storeInfo = append(storeInfo, urlStr)
			}
			s.logger.Warnf("[pruner][%s:%d] blob deletion: all %d blobs in batch were already missing from disk — verify the pruner pod has the correct volume mount for blob store(s): %v",
				blockHashStr, blockHeight, len(deletions), storeInfo)
		}

		// Complete the batch in a single gRPC call
		s.logger.Infof("[pruner][%s:%d] blob deletion: completing batch %d - %s deleted, %s not found, %s failed",
			blockHashStr, blockHeight, batchNum, util.FormatComma(deletedCount), util.FormatComma(notFoundCount), util.FormatComma(int64(len(failedIDs))))

		err = s.blockchainClient.CompleteBlobDeletionBatch(ctx, batchToken, completedIDs, failedIDs, maxRetries)
		if err != nil {
			s.logger.Errorf("[pruner][%s:%d] blob deletion: failed to complete batch %d: %v", blockHashStr, blockHeight, batchNum, err)
			blobDeletionErrorsTotal.WithLabelValues("completion").Inc()
			// Batch will be released when token expires - deletions will be retried
			break
		}

		duration := time.Since(batchStartTime).Round(time.Second)
		s.logger.Infof("[pruner][%s:%d] blob deletion: batch %d complete - %s deleted, %s not found, %s failed (took %s)",
			blockHashStr, blockHeight, batchNum, util.FormatComma(deletedCount), util.FormatComma(notFoundCount), util.FormatComma(failCount), duration)

		// Update totals
		totalDeleted += deletedCount
		totalNotFound += notFoundCount
		totalFail += failCount
	}

	// Log overall summary if we processed multiple batches
	if batchNum > 2 {
		totalDuration := time.Since(overallStartTime).Round(time.Second)
		s.logger.Infof("[pruner][%s:%d] blob deletion: processed %d batches - %s total deleted, %s total not found, %s total failed (took %s)",
			blockHashStr, blockHeight, batchNum-1, util.FormatComma(totalDeleted), util.FormatComma(totalNotFound), util.FormatComma(totalFail), totalDuration)
	}

	// Notify observer if registered (for testing)
	if s.blobDeletionObserver != nil {
		s.blobDeletionObserver.OnBlobDeletionComplete(blockHeight, totalDeleted+totalNotFound, totalFail)
	}
}

func (s *Server) processOneDeletion(ctx context.Context, deletion *blockchain_api.ScheduledDeletion, blockHashStr string, blockHeight uint32) (deletionResult, error) {
	storeType := storetypes.BlobStoreType(deletion.StoreType)

	// Get or create blob store for this store type
	blobStore, err := s.getBlobStore(storeType)
	if err != nil {
		return deletionNotFound, errors.NewNotFoundError("failed to get blob store for %s: %v", storeType.String(), err)
	}

	// Convert file type string to FileType
	fileType := fileformat.FileType(deletion.FileType)

	// Check existence before deletion so we can distinguish "actually deleted" from
	// "already absent". The file store's Del() returns nil in both cases, but we need
	// to tell them apart to detect volume mount misconfigurations where the pruner
	// cannot see the real blob files.
	existed, existsErr := blobStore.Exists(ctx, deletion.BlobKey, fileType)
	if existsErr != nil {
		s.logger.Debugf("[pruner][%s:%d] blob deletion: exists check failed for key=%x: %v", blockHashStr, blockHeight, deletion.BlobKey, existsErr)
		// Assume existed so we don't inflate not-found counts due to transient errors.
		// Fall through to Del which will report the real error if the store is down.
		existed = true
	}

	// Delete blob (idempotent — succeeds even if file is already gone)
	startTime := time.Now()
	err = blobStore.Del(ctx, deletion.BlobKey, fileType)
	duration := time.Since(startTime)

	blobDeletionDurationSeconds.WithLabelValues(storeType.String()).Observe(duration.Seconds())

	if err != nil {
		return deletionNotFound, err
	}

	if !existed {
		s.logger.Debugf("[pruner][%s:%d] blob deletion: blob key=%x was already absent from %s", blockHashStr, blockHeight, deletion.BlobKey, storeType.String())
		return deletionNotFound, nil
	}

	s.logger.Debugf("[pruner][%s:%d] blob deletion: deleted blob key=%x from %s (took %s)",
		blockHashStr, blockHeight, deletion.BlobKey, storeType.String(), duration.Round(time.Second))

	return deletionDeleted, nil
}

// getBlobStore gets or creates a blob store for the given store type enum.
// Stores are lazily created on first use and cached in s.blobStores.
func (s *Server) getBlobStore(storeType storetypes.BlobStoreType) (blob.Store, error) {
	// Check cache first
	if store, ok := s.blobStores[storeType]; ok {
		return store, nil
	}

	// Not in cache - create from settings (lazy initialization)
	if s.settings == nil {
		return nil, errors.NewNotFoundError("settings unavailable, cannot create blob store for type %s", storeType.String())
	}

	storeURL, err := s.settings.GetBlobStoreURL(int32(storeType))
	if err != nil {
		return nil, err
	}

	if storeURL == nil {
		return nil, errors.NewConfigurationError("blob store type %s has nil URL in settings", storeType.String())
	}

	// Default hash prefix per store type (must match daemon_stores.go)
	hashPrefix := defaultHashPrefix(storeType)
	if storeURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(storeURL.Query().Get("hashPrefix"))
		if err != nil {
			return nil, errors.NewConfigurationError("blob store type %s has invalid hashPrefix: %v", storeType.String(), err)
		}
	}

	// Create new store instance with hash prefix to match daemon store layout
	store, err := blob.NewStore(s.logger, storeURL, bloboptions.WithHashPrefix(hashPrefix))
	if err != nil {
		return nil, errors.NewStorageError("failed to create blob store for %s", storeType.String(), err)
	}

	s.logger.Infof("[pruner] blob deletion: initialized %s store at %s (hashPrefix=%d)", storeType.String(), storeURL.String(), hashPrefix)

	// Cache it (note: map writes in Go are not thread-safe, but this is acceptable
	// since it's idempotent and only happens once per store type)
	s.blobStores[storeType] = store

	return store, nil
}

// defaultHashPrefix returns the default hash prefix for a given store type,
// matching the defaults in daemon/daemon_stores.go.
func defaultHashPrefix(storeType storetypes.BlobStoreType) int {
	switch storeType {
	case storetypes.TXSTORE:
		return 2
	case storetypes.SUBTREESTORE:
		return 2
	case storetypes.BLOCKSTORE:
		return -2
	case storetypes.TEMPSTORE:
		return 0
	case storetypes.BLOCKPERSISTERSTORE:
		return 2
	default:
		return 2
	}
}

func (s *Server) updateBlobDeletionMetrics() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Query blockchain service for pending deletions count
			// We fetch a small batch just to get the count (blockchain service could expose a count method in the future)
			deletions, err := s.blockchainClient.GetPendingBlobDeletions(s.ctx, 999999, 1000)
			if err == nil {
				blobDeletionPendingGauge.Set(float64(len(deletions)))
			}
		}
	}
}
