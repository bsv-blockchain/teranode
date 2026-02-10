package pruner

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/dustin/go-humanize"
)

func (s *Server) blobDeletionWorker() {
	for {
		select {
		case <-s.ctx.Done():
			return

		case req := <-s.blobDeletionCh:
			// Drain channel to get latest request
			latestReq := req
		drainLoop:
			for {
				select {
				case nextReq := <-s.blobDeletionCh:
					latestReq = nextReq
				default:
					break drainLoop
				}
			}

			s.logger.Debugf("Processing blob deletions for height %d", latestReq.Height)
			s.processBlobDeletionsAtHeight(latestReq.Height, latestReq.BlockHash)
		}
	}
}

func (s *Server) processBlobDeletionsAtHeight(height uint32, blockHash *chainhash.Hash) {
	if !s.settings.Pruner.BlobDeletionEnabled {
		return
	}

	ctx := s.ctx
	hashStr := "<unknown>"
	if blockHash != nil {
		hashStr = blockHash.String()
	}

	// Wait for block to be mined (only if blockHash provided AND block assembly is running)
	// When block assembly is not running (e.g., in tests), blocks are immediately mined
	if blockHash != nil && s.blockAssemblyClient != nil {
		s.logger.Debugf("[pruner][%s:%d] blob deletion: waiting for mined_set=true", hashStr, height)
		if !s.waitForBlockMinedStatus(ctx, blockHash) {
			s.logger.Warnf("[pruner][%s:%d] blob deletion: skipped - timeout waiting for mined_set", hashStr, height)
			return
		}
	}

	// Safety check: block assembly must be running
	if !s.checkBlockAssemblySafeForPruner(ctx, "blob deletion", height) {
		return
	}

	// Safety check: respect persisted height
	persistedHeight := s.lastPersistedHeight.Load()
	safetyWindow := s.settings.Pruner.BlobDeletionSafetyWindow
	safeHeight := height

	if persistedHeight > 0 && safetyWindow > 0 {
		if height > persistedHeight+safetyWindow {
			s.logger.Debugf("Skipping blob deletion at height %d (persisted: %d, safety: %d)",
				height, persistedHeight, safetyWindow)
			return
		}
		if persistedHeight > safetyWindow {
			safeHeight = persistedHeight - safetyWindow
		}
	}

	// Acquire batch with locking from blockchain service (uses SELECT...FOR UPDATE SKIP LOCKED)
	batchSize := s.settings.Pruner.BlobDeletionBatchSize
	lockTimeout := 300 // 5 minutes - should be plenty for processing

	batchToken, deletions, err := s.blockchainClient.AcquireBlobDeletionBatch(ctx, safeHeight, int(batchSize), lockTimeout)
	if err != nil {
		s.logger.Errorf("Failed to acquire blob deletion batch: %v", err)
		blobDeletionErrorsTotal.WithLabelValues("acquisition").Inc()
		return
	}

	if batchToken == "" || len(deletions) == 0 {
		s.logger.Debugf("[pruner][%s:%d] blob deletion: no blob deletions available", hashStr, height)
		return
	}

	batchStartTime := time.Now()
	s.logger.Infof("[pruner][%s:%d] blob deletion: acquired blob deletion batch with %s deletions", hashStr, height, humanize.Comma(int64(len(deletions))))

	// Track completed and failed deletions
	completedIDs := make([]int64, 0, len(deletions))
	failedIDs := make([]int64, 0, len(deletions))

	var successCount, failCount int64
	maxRetries := s.settings.Pruner.BlobDeletionMaxRetries

	// Process all deletions in the batch
	for i, deletion := range deletions {
		storeType := storetypes.BlobStoreType(deletion.StoreType)

		s.logger.Debugf("[pruner][%s:%d] blob deletion: processing deletion %s/%s (id=%d, key=%x)",
			hashStr, height, humanize.Comma(int64(i+1)), humanize.Comma(int64(len(deletions))), deletion.Id, deletion.BlobKey)

		if err := s.processOneDeletion(ctx, deletion, hashStr, height); err != nil {
			s.logger.Warnf("[pruner][%s:%d] blob deletion: failed to delete blob %x from %s (attempt %d/%d): %v",
				hashStr, height, deletion.BlobKey, storeType.String(),
				int(deletion.RetryCount)+1, maxRetries, err)

			failedIDs = append(failedIDs, deletion.Id)

			// Check if this will be removed due to max retries
			if int(deletion.RetryCount)+1 >= maxRetries {
				s.logger.Errorf("[pruner][%s:%d] blob deletion: blob %x will be removed after %d failed attempts",
					hashStr, height, deletion.BlobKey, maxRetries)
				blobDeletionErrorsTotal.WithLabelValues(storeType.String()).Inc()
				failCount++
			}
		} else {
			completedIDs = append(completedIDs, deletion.Id)
			successCount++
			blobDeletionProcessedTotal.Inc()
		}
	}

	// Complete the entire batch in a single gRPC call
	s.logger.Infof("[pruner][%s:%d] blob deletion: completing batch - %s succeeded, %s failed",
		hashStr, height, humanize.Comma(successCount), humanize.Comma(int64(len(failedIDs))))

	err = s.blockchainClient.CompleteBlobDeletionBatch(ctx, batchToken, completedIDs, failedIDs, maxRetries)
	if err != nil {
		s.logger.Errorf("[pruner][%s:%d] blob deletion: failed to complete batch: %v", hashStr, height, err)
		blobDeletionErrorsTotal.WithLabelValues("completion").Inc()
		// Batch will be released when token expires - deletions will be retried
		return
	}

	duration := time.Since(batchStartTime).Round(time.Second)
	s.logger.Infof("[pruner][%s:%d] blob deletion: batch complete - %s succeeded, %s failed (took %s)",
		hashStr, height, humanize.Comma(successCount), humanize.Comma(failCount), duration)

	// Notify observer if registered (for testing)
	if s.blobDeletionObserver != nil {
		s.blobDeletionObserver.OnBlobDeletionComplete(height, successCount, failCount)
	}
}

func (s *Server) processOneDeletion(ctx context.Context, deletion *blockchain_api.ScheduledDeletion, hashStr string, height uint32) error {
	storeType := storetypes.BlobStoreType(deletion.StoreType)

	// Get or create blob store for this store type
	blobStore, err := s.getBlobStore(storeType)
	if err != nil {
		return errors.NewNotFoundError("failed to get blob store for %s: %v", storeType.String(), err)
	}

	// Convert file type string to FileType
	fileType := fileformat.FileType(deletion.FileType)

	// Delete blob
	startTime := time.Now()
	err = blobStore.Del(ctx, deletion.BlobKey, fileType)
	duration := time.Since(startTime)

	blobDeletionDurationSeconds.WithLabelValues(storeType.String()).Observe(duration.Seconds())

	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			// Already deleted - success (idempotent)
			s.logger.Debugf("[pruner][%s:%d] blob deletion: blob key=%x already deleted (idempotent success)", hashStr, height, deletion.BlobKey)
			return nil
		}
		return err
	}

	s.logger.Debugf("[pruner][%s:%d] blob deletion: deleted blob key=%x from %s (took %s)",
		hashStr, height, deletion.BlobKey, storeType.String(), duration.Round(time.Second))

	return nil
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

	// Create new store instance
	store, err := blob.NewStore(s.logger, storeURL)
	if err != nil {
		return nil, errors.NewStorageError("failed to create blob store for %s", storeType.String(), err)
	}

	// Cache it (note: map writes in Go are not thread-safe, but this is acceptable
	// since it's idempotent and only happens once per store type)
	s.blobStores[storeType] = store

	return store, nil
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
