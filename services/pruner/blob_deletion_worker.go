package pruner

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
)

func (s *Server) blobDeletionWorker() {
	for {
		select {
		case <-s.ctx.Done():
			return

		case height := <-s.blobDeletionCh:
			// Drain channel to get latest height
			for {
				select {
				case h := <-s.blobDeletionCh:
					height = h
				default:
					goto processHeight
				}
			}

		processHeight:
			s.processBlobDeletionsAtHeight(height)
		}
	}
}

func (s *Server) processBlobDeletionsAtHeight(height uint32) {
	if !s.settings.Pruner.BlobDeletionEnabled {
		return
	}

	ctx := s.ctx

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
		s.logger.Debugf("No blob deletions available at height %d", height)
		return
	}

	s.logger.Infof("Acquired blob deletion batch: token=%s, count=%d, height=%d", batchToken, len(deletions), height)
	s.logger.Debugf("[BLOB_DELETION_BATCH] Starting batch: height=%d count=%d batch_size=%d token=%s",
		height, len(deletions), batchSize, batchToken)

	for i, d := range deletions {
		s.logger.Debugf("[BLOB_DELETION_BATCH] Deletion %d/%d: id=%d key=%x type=%s store=%d height=%d",
			i+1, len(deletions), d.Id, d.BlobKey, d.FileType, d.StoreType, d.DeleteAtHeight)
	}

	// Track completed and failed deletions
	completedIDs := make([]int64, 0, len(deletions))
	failedIDs := make([]int64, 0, len(deletions))

	var successCount, failCount int64
	maxRetries := s.settings.Pruner.BlobDeletionMaxRetries

	// Process all deletions in the batch
	for _, deletion := range deletions {
		storeType := storetypes.BlobStoreType(deletion.StoreType)

		if err := s.processOneDeletion(ctx, deletion); err != nil {
			s.logger.Warnf("Failed to delete blob %x from %s (attempt %d/%d): %v",
				deletion.BlobKey, storeType.String(), int(deletion.RetryCount)+1, maxRetries, err)

			failedIDs = append(failedIDs, deletion.Id)

			// Check if this will be removed due to max retries
			if int(deletion.RetryCount)+1 >= maxRetries {
				s.logger.Errorf("Blob %x will be removed after %d failed attempts", deletion.BlobKey, maxRetries)
				blobDeletionErrorsTotal.WithLabelValues(storeType.String()).Inc()
				failCount++
			}
		} else {
			s.logger.Debugf("[BLOB_DELETION] Successfully deleted blob: key=%x store=%s height=%d",
				deletion.BlobKey, storeType.String(), deletion.DeleteAtHeight)

			completedIDs = append(completedIDs, deletion.Id)
			successCount++
		}
	}

	// Complete the entire batch in a single gRPC call
	s.logger.Infof("Completing batch %s: %d succeeded, %d failed", batchToken, successCount, len(failedIDs))

	err = s.blockchainClient.CompleteBlobDeletionBatch(ctx, batchToken, completedIDs, failedIDs, maxRetries)
	if err != nil {
		s.logger.Errorf("Failed to complete blob deletion batch %s: %v", batchToken, err)
		blobDeletionErrorsTotal.WithLabelValues("completion").Inc()
		// Batch will be released when token expires - deletions will be retried
		return
	}

	s.logger.Infof("Blob deletion batch %s completed: %d succeeded, %d failed", batchToken, successCount, failCount)
	blobDeletionProcessedTotal.Add(float64(successCount))

	// Notify observer if registered (for testing)
	if s.blobDeletionObserver != nil {
		s.blobDeletionObserver.OnBlobDeletionComplete(height, successCount, failCount)
	}
}

func (s *Server) processOneDeletion(ctx context.Context, deletion *blockchain_api.ScheduledDeletion) error {
	storeType := storetypes.BlobStoreType(deletion.StoreType)

	s.logger.Debugf("[BLOB_DELETION] Starting deletion: store=%s blob_key=%x file_type=%s height=%d",
		storeType.String(), deletion.BlobKey, deletion.FileType, deletion.DeleteAtHeight)

	// Get or create blob store for this store type
	blobStore, err := s.getBlobStore(storeType)
	if err != nil {
		s.logger.Debugf("[BLOB_DELETION] Failed to get blob store: %v", err)
		return errors.NewNotFoundError("failed to get blob store for %s: %v", storeType.String(), err)
	}

	s.logger.Debugf("[BLOB_DELETION] Blob store obtained for %s", storeType.String())

	// Convert file type string to FileType
	fileType := fileformat.FileType(deletion.FileType)

	// Delete blob
	s.logger.Debugf("[BLOB_DELETION] Calling blobStore.Del(): store=%s key=%x type=%s",
		storeType.String(), deletion.BlobKey, fileType)

	startTime := time.Now()
	err = blobStore.Del(ctx, deletion.BlobKey, fileType)
	duration := time.Since(startTime)

	blobDeletionDurationSeconds.WithLabelValues(storeType.String()).Observe(duration.Seconds())

	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			// Already deleted - success (idempotent)
			s.logger.Debugf("[BLOB_DELETION] Blob already deleted (not found): key=%x store=%s - SUCCESS",
				deletion.BlobKey, storeType)
			return nil
		}
		s.logger.Debugf("[BLOB_DELETION] Deletion FAILED: key=%x store=%s error=%v",
			deletion.BlobKey, storeType, err)
		return err
	}

	s.logger.Debugf("[BLOB_DELETION] Deletion SUCCESS: key=%x store=%s duration=%v height=%d",
		deletion.BlobKey, storeType, duration, deletion.DeleteAtHeight)

	return nil
}

// getBlobStore gets or creates a blob store for the given store type enum.
// Stores are lazily created on first use and cached in s.blobStores.
func (s *Server) getBlobStore(storeType storetypes.BlobStoreType) (blob.Store, error) {
	// Check cache first
	if store, ok := s.blobStores[storeType]; ok {
		s.logger.Debugf("[BLOB_STORE] Using cached blob store for %s", storeType.String())
		return store, nil
	}

	s.logger.Debugf("[BLOB_STORE] Blob store not in cache, creating new instance for %s", storeType.String())

	// Not in cache - create from settings (lazy initialization)
	if s.settings == nil {
		return nil, errors.NewNotFoundError("settings unavailable, cannot create blob store for type %s", storeType.String())
	}

	storeURL, err := s.settings.GetBlobStoreURL(int32(storeType))
	if err != nil {
		s.logger.Debugf("[BLOB_STORE] Failed to get store URL from settings: store_type=%s error=%v", storeType.String(), err)
		return nil, err
	}

	if storeURL == nil {
		s.logger.Debugf("[BLOB_STORE] Store URL is nil in settings: store_type=%s", storeType.String())
		return nil, errors.NewConfigurationError("blob store type %s has nil URL in settings", storeType.String())
	}

	s.logger.Debugf("[BLOB_STORE] Creating new blob store: type=%s url=%s", storeType.String(), storeURL.String())

	// Create new store instance
	store, err := blob.NewStore(s.logger, storeURL)
	if err != nil {
		s.logger.Debugf("[BLOB_STORE] Failed to create blob store: type=%s url=%s error=%v", storeType.String(), storeURL.String(), err)
		return nil, errors.NewStorageError("failed to create blob store for %s", storeType.String(), err)
	}

	// Cache it (note: map writes in Go are not thread-safe, but this is acceptable
	// since it's idempotent and only happens once per store type)
	s.blobStores[storeType] = store
	s.logger.Debugf("[BLOB_STORE] Successfully created and cached blob store: type=%s url=%s", storeType.String(), storeURL.String())

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
