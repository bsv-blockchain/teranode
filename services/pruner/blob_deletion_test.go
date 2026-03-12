package pruner

import (
	"context"
	"crypto/rand"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// mockBlockchainClient implements a simple mock for blockchain client blob deletion methods
type mockBlockchainClient struct {
	*blockchain.Mock
	scheduledDeletions map[int64]*blockchain_api.ScheduledDeletion
	nextID             int64
	acquiredBatches    map[string][]int64
}

func newMockBlockchainClient() *mockBlockchainClient {
	return &mockBlockchainClient{
		Mock:               &blockchain.Mock{},
		scheduledDeletions: make(map[int64]*blockchain_api.ScheduledDeletion),
		nextID:             1,
		acquiredBatches:    make(map[string][]int64),
	}
}

func (m *mockBlockchainClient) ScheduleBlobDeletion(ctx context.Context, blobKey []byte, fileType string, storeType storetypes.BlobStoreType, deleteAtHeight uint32) (int64, bool, error) {
	id := m.nextID
	m.nextID++
	m.scheduledDeletions[id] = &blockchain_api.ScheduledDeletion{
		Id:             id,
		BlobKey:        blobKey,
		FileType:       fileType,
		StoreType:      int32(storeType),
		DeleteAtHeight: deleteAtHeight,
		RetryCount:     0,
	}
	return id, true, nil
}

func (m *mockBlockchainClient) GetPendingBlobDeletions(ctx context.Context, height uint32, limit int) ([]*blockchain_api.ScheduledDeletion, error) {
	var result []*blockchain_api.ScheduledDeletion
	for _, d := range m.scheduledDeletions {
		if d.DeleteAtHeight <= height {
			result = append(result, d)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *mockBlockchainClient) AcquireBlobDeletionBatch(ctx context.Context, height uint32, limit int, lockTimeoutSeconds int) (string, []*blockchain_api.ScheduledDeletion, error) {
	var result []*blockchain_api.ScheduledDeletion
	var ids []int64
	for _, d := range m.scheduledDeletions {
		if d.DeleteAtHeight <= height {
			result = append(result, d)
			ids = append(ids, d.Id)
			if len(result) >= limit {
				break
			}
		}
	}

	if len(result) == 0 {
		return "", nil, nil
	}

	token := "test_token"
	m.acquiredBatches[token] = ids
	return token, result, nil
}

func (m *mockBlockchainClient) CompleteBlobDeletionBatch(ctx context.Context, batchToken string, completedIDs []int64, failedIDs []int64, maxRetries int) error {
	for _, id := range completedIDs {
		delete(m.scheduledDeletions, id)
	}
	for _, id := range failedIDs {
		if d, ok := m.scheduledDeletions[id]; ok {
			d.RetryCount++
			if int(d.RetryCount) >= maxRetries {
				delete(m.scheduledDeletions, id)
			}
		}
	}
	delete(m.acquiredBatches, batchToken)
	return nil
}

func (m *mockBlockchainClient) RemoveBlobDeletion(ctx context.Context, deletionID int64) error {
	delete(m.scheduledDeletions, deletionID)
	return nil
}

func (m *mockBlockchainClient) IncrementBlobDeletionRetry(ctx context.Context, deletionID int64, maxRetries int) (bool, int, error) {
	if d, ok := m.scheduledDeletions[deletionID]; ok {
		d.RetryCount++
		if int(d.RetryCount) >= maxRetries {
			delete(m.scheduledDeletions, deletionID)
			return true, int(d.RetryCount), nil
		}
		return false, int(d.RetryCount), nil
	}
	return false, 0, nil
}

func (m *mockBlockchainClient) CompleteBlobDeletions(ctx context.Context, completedIDs []int64, failedIDs []int64, maxRetries int) (int, int, error) {
	removedCount := 0
	retryIncrementedCount := 0

	for _, id := range completedIDs {
		delete(m.scheduledDeletions, id)
		removedCount++
	}

	for _, id := range failedIDs {
		if d, ok := m.scheduledDeletions[id]; ok {
			d.RetryCount++
			if int(d.RetryCount) >= maxRetries {
				delete(m.scheduledDeletions, id)
				removedCount++
			} else {
				retryIncrementedCount++
			}
		}
	}

	return removedCount, retryIncrementedCount, nil
}

// testBlobDeletionObserver implements BlobDeletionObserver for unit tests
type testBlobDeletionObserver struct {
	t        *testing.T
	complete chan blobDeletionEvent
}

type blobDeletionEvent struct {
	height       uint32
	successCount int64
	failCount    int64
}

func (o *testBlobDeletionObserver) OnBlobDeletionComplete(height uint32, successCount, failCount int64) {
	o.t.Logf("✓ Blob deletion complete for height %d: %d succeeded, %d failed", height, successCount, failCount)
	select {
	case o.complete <- blobDeletionEvent{height: height, successCount: successCount, failCount: failCount}:
	default:
		o.t.Logf("Warning: observer channel full")
	}
}

func (o *testBlobDeletionObserver) waitFor(timeout time.Duration) (blobDeletionEvent, error) {
	select {
	case event := <-o.complete:
		return event, nil
	case <-time.After(timeout):
		return blobDeletionEvent{}, errors.NewProcessingError("timeout waiting for blob deletion")
	}
}

// TestBlobDeletionSchedulingAndExecution verifies that the pruner correctly schedules
// and executes blob deletions at specified heights using the blockchain service.
func TestBlobDeletionSchedulingAndExecution(t *testing.T) {
	// Initialize prometheus metrics (required for worker)
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.New("test")

	// Create mock blockchain client
	mockBlockchain := newMockBlockchainClient()

	// Create temporary file store
	testDir := t.TempDir()
	storeURL := &url.URL{
		Scheme: "file",
		Path:   filepath.Join(testDir, "blobs"),
	}

	testStore, err := blob.NewStore(logger, storeURL)
	require.NoError(t, err)

	// Create blob stores map with enum keys
	blobStores := map[storetypes.BlobStoreType]blob.Store{
		storetypes.TXSTORE: testStore,
	}

	// Create test observer
	observer := &testBlobDeletionObserver{
		t:        t,
		complete: make(chan blobDeletionEvent, 10),
	}

	// Create test server (minimal setup)
	server := &Server{
		ctx:                  ctx,
		logger:               logger,
		blobStores:           blobStores,
		blockchainClient:     mockBlockchain,
		blobDeletionObserver: observer,
	}

	// Set up mock settings
	server.settings = &settings.Settings{
		Pruner: settings.PrunerSettings{
			SkipBlobDeletion:         false,
			BlobDeletionSafetyWindow: 0,
			BlobDeletionBatchSize:    100,
			BlobDeletionMaxRetries:   3,
		},
	}

	// Create test blobs
	testBlobs := []struct {
		key            []byte
		data           []byte
		deleteAtHeight uint32
	}{
		{
			key:            make([]byte, 32),
			data:           []byte("test blob 1"),
			deleteAtHeight: 10,
		},
		{
			key:            make([]byte, 32),
			data:           []byte("test blob 2"),
			deleteAtHeight: 10,
		},
		{
			key:            make([]byte, 32),
			data:           []byte("test blob 3"),
			deleteAtHeight: 20,
		},
	}

	// Generate random keys
	for i := range testBlobs {
		_, err := rand.Read(testBlobs[i].key)
		require.NoError(t, err)
	}

	// Write blobs to store
	for i, blob := range testBlobs {
		err = testStore.Set(ctx, blob.key, fileformat.FileTypeTesting, blob.data)
		require.NoError(t, err)
		t.Logf("Created test blob %d with key=%x", i, blob.key[:8])
	}

	// Verify blobs exist
	for i, blob := range testBlobs {
		exists, err := testStore.Exists(ctx, blob.key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists, "Blob %d should exist after creation", i)
	}

	// Schedule deletions via mock blockchain client
	for i, blob := range testBlobs {
		id, scheduled, err := mockBlockchain.ScheduleBlobDeletion(ctx, blob.key, string(fileformat.FileTypeTesting), storetypes.TXSTORE, blob.deleteAtHeight)
		require.NoError(t, err)
		require.True(t, scheduled)
		t.Logf("Scheduled deletion %d with id=%d, DAH=%d", i, id, blob.deleteAtHeight)
	}

	// Verify deletions are in queue
	deletions, err := mockBlockchain.GetPendingBlobDeletions(ctx, 100, 100)
	require.NoError(t, err)
	require.Equal(t, 3, len(deletions), "Should have 3 pending deletions")

	// Process deletions at height 10
	t.Log("Processing deletions at height 10")
	server.processBlobDeletionsAtHeight(10, chainhash.Hash{})

	// Wait for completion via observer
	event, err := observer.waitFor(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, uint32(10), event.height)
	require.Equal(t, int64(2), event.successCount, "Should have deleted 2 blobs")
	require.Equal(t, int64(0), event.failCount, "Should have 0 failures")

	// Verify first 2 blobs are deleted
	for i := 0; i < 2; i++ {
		exists, err := testStore.Exists(ctx, testBlobs[i].key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.False(t, exists, "Blob %d should be deleted at height 10", i)
		t.Logf("✓ Blob %d deleted", i)
	}

	// Verify third blob still exists
	exists, err := testStore.Exists(ctx, testBlobs[2].key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.True(t, exists, "Blob 2 should still exist (DAH=20)")
	t.Log("✓ Blob 2 preserved")

	// Verify queue self-cleaned (first 2 deletions removed at height 10)
	deletions, err = mockBlockchain.GetPendingBlobDeletions(ctx, 10, 100)
	require.NoError(t, err)
	require.Equal(t, 0, len(deletions), "Should have 0 deletions ready at height 10 (already processed)")
	t.Log("✓ Queue self-cleaned for height 10")

	// Verify third deletion still scheduled
	deletions, err = mockBlockchain.GetPendingBlobDeletions(ctx, 20, 100)
	require.NoError(t, err)
	require.Equal(t, 1, len(deletions), "Should still have 1 deletion for height 20")

	// Process deletions at height 20
	t.Log("Processing deletions at height 20")
	server.processBlobDeletionsAtHeight(20, chainhash.Hash{})

	// Wait for completion
	event, err = observer.waitFor(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, uint32(20), event.height)
	require.Equal(t, int64(1), event.successCount, "Should have deleted 1 blob")

	// Verify third blob is deleted
	exists, err = testStore.Exists(ctx, testBlobs[2].key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.False(t, exists, "Blob 2 should be deleted at height 20")
	t.Log("✓ Blob 2 deleted")

	// Verify queue is completely empty
	deletions, err = mockBlockchain.GetPendingBlobDeletions(ctx, 100, 100)
	require.NoError(t, err)
	require.Equal(t, 0, len(deletions), "Queue should be completely empty")
	t.Log("✓ All deletions processed, queue empty")
}

// TestBlobDeletionIdempotency verifies that deleting an already-deleted blob doesn't error.
func TestBlobDeletionIdempotency(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.New("test")

	mockBlockchain := newMockBlockchainClient()

	// Create test store
	testDir := t.TempDir()
	storeURL := &url.URL{
		Scheme: "file",
		Path:   filepath.Join(testDir, "blobs"),
	}

	testStore, err := blob.NewStore(logger, storeURL)
	require.NoError(t, err)

	blobStores := map[storetypes.BlobStoreType]blob.Store{
		storetypes.TXSTORE: testStore,
	}

	observer := &testBlobDeletionObserver{
		t:        t,
		complete: make(chan blobDeletionEvent, 10),
	}

	server := &Server{
		ctx:                  ctx,
		logger:               logger,
		blobStores:           blobStores,
		blockchainClient:     mockBlockchain,
		blobDeletionObserver: observer,
	}

	server.settings = &settings.Settings{
		Pruner: settings.PrunerSettings{
			SkipBlobDeletion:       false,
			BlobDeletionBatchSize:  100,
			BlobDeletionMaxRetries: 3,
		},
	}

	// Create and then manually delete blob
	testKey := make([]byte, 32)
	_, err = rand.Read(testKey)
	require.NoError(t, err)

	testData := []byte("test blob for idempotency")
	err = testStore.Set(ctx, testKey, fileformat.FileTypeTesting, testData)
	require.NoError(t, err)

	// Manually delete the blob
	err = testStore.Del(ctx, testKey, fileformat.FileTypeTesting)
	require.NoError(t, err)

	// Verify blob is gone
	exists, err := testStore.Exists(ctx, testKey, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.False(t, exists, "Blob should be deleted")

	// Schedule deletion (blob already gone)
	id, scheduled, err := mockBlockchain.ScheduleBlobDeletion(ctx, testKey, string(fileformat.FileTypeTesting), storetypes.TXSTORE, 10)
	require.NoError(t, err)
	require.True(t, scheduled)
	t.Logf("Scheduled deletion of already-deleted blob, id=%d", id)

	// Process deletions (should handle gracefully)
	server.processBlobDeletionsAtHeight(10, chainhash.Hash{})

	// Wait for completion (blob already deleted, so it should succeed idempotently)
	event, err := observer.waitFor(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, int64(1), event.successCount, "Should count 1 idempotent deletion as success")

	// Verify queue is cleaned up (no errors)
	deletions, err := mockBlockchain.GetPendingBlobDeletions(ctx, 100, 100)
	require.NoError(t, err)
	require.Equal(t, 0, len(deletions), "Queue should be empty after processing")
	t.Log("✓ Pruner handled already-deleted blob gracefully")
}
