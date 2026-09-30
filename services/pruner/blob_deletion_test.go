package pruner

import (
	"context"
	"crypto/rand"
	"net/http/httptest"
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
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
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

// TestBlobDeletionSafetyWindowBoundary verifies that blob deletions are skipped when
// blockHeight <= safetyWindow, and only processed once blockHeight exceeds the window.
func TestBlobDeletionSafetyWindowBoundary(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.New("test")

	mockBlockchain := newMockBlockchainClient()

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
			SkipBlobDeletion:         false,
			BlobDeletionSafetyWindow: 5,
			BlobDeletionBatchSize:    100,
			BlobDeletionMaxRetries:   3,
		},
	}

	// Create and store a test blob
	testKey := make([]byte, 32)
	_, err = rand.Read(testKey)
	require.NoError(t, err)

	err = testStore.Set(ctx, testKey, fileformat.FileTypeTesting, []byte("safety window test blob"))
	require.NoError(t, err)

	// Schedule deletion at height 3
	_, scheduled, err := mockBlockchain.ScheduleBlobDeletion(ctx, testKey, string(fileformat.FileTypeTesting), storetypes.TXSTORE, 3)
	require.NoError(t, err)
	require.True(t, scheduled)

	// Process at height 3 (blockHeight <= safetyWindow=5): should skip all deletions
	t.Log("Processing at height 3 (blockHeight <= safetyWindow=5): should skip")
	server.processBlobDeletionsAtHeight(3, chainhash.Hash{})

	// Observer should NOT fire — processBlobDeletionsAtHeight is synchronous, so
	// if it were going to enqueue an event it would have done so before returning.
	select {
	case <-observer.complete:
		t.Fatal("Expected no deletion event when blockHeight <= safetyWindow")
	default:
		t.Log("Correctly skipped deletions when blockHeight <= safetyWindow")
	}

	// Verify blob still exists
	exists, err := testStore.Exists(ctx, testKey, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.True(t, exists, "Blob should still exist when blockHeight <= safetyWindow")

	// Process at height 5 (blockHeight == safetyWindow): should still skip
	t.Log("Processing at height 5 (blockHeight == safetyWindow): should skip")
	server.processBlobDeletionsAtHeight(5, chainhash.Hash{})

	select {
	case <-observer.complete:
		t.Fatal("Expected no deletion event when blockHeight == safetyWindow")
	default:
		t.Log("Correctly skipped deletions when blockHeight == safetyWindow")
	}

	exists, err = testStore.Exists(ctx, testKey, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.True(t, exists, "Blob should still exist when blockHeight == safetyWindow")

	// Process at height 8 (blockHeight=8 > safetyWindow=5, safeHeight=3, DAH=3 <= 3): should delete
	t.Log("Processing at height 8 (safeHeight=3, DAH=3): should delete")
	server.processBlobDeletionsAtHeight(8, chainhash.Hash{})

	event, err := observer.waitFor(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, uint32(8), event.height)
	require.Equal(t, int64(1), event.successCount, "Should delete 1 blob")

	exists, err = testStore.Exists(ctx, testKey, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.False(t, exists, "Blob should be deleted once blockHeight > safetyWindow and safeHeight >= DAH")
	t.Log("Correctly deleted blob after safety window satisfied")
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
			SkipBlobDeletion:         false,
			BlobDeletionSafetyWindow: 0,
			BlobDeletionBatchSize:    100,
			BlobDeletionMaxRetries:   3,
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

// blobDeletionServiceClient hands the pruner's two batch calls to a real blockchain service
// over a sqlitememory store, so retry counting and dropping are the store's own. The
// embedded client only satisfies the rest of ClientI; the pruner calls nothing else here.
type blobDeletionServiceClient struct {
	*blockchain.Mock
	service *blockchain.Blockchain
}

func (c *blobDeletionServiceClient) AcquireBlobDeletionBatch(ctx context.Context, height uint32, limit int, lockTimeoutSeconds int) (string, []*blockchain_api.ScheduledDeletion, error) {
	resp, err := c.service.AcquireBlobDeletionBatch(ctx, &blockchain_api.AcquireBlobDeletionBatchRequest{
		Height:             height,
		Limit:              int32(limit),
		LockTimeoutSeconds: int32(lockTimeoutSeconds),
	})
	if err != nil {
		return "", nil, err
	}

	return resp.BatchToken, resp.Deletions, nil
}

func (c *blobDeletionServiceClient) CompleteBlobDeletionBatch(ctx context.Context, batchToken string, completedIDs []int64, failedIDs []int64, maxRetries int) error {
	_, err := c.service.CompleteBlobDeletionBatch(ctx, &blockchain_api.CompleteBlobDeletionBatchRequest{
		BatchToken:   batchToken,
		CompletedIds: completedIDs,
		FailedIds:    failedIDs,
		MaxRetries:   int32(maxRetries),
	})

	return err
}

// newBlobDeletionServiceHarness builds a pruner Server whose batch calls go to a real
// blockchain service over sqlitememory, and returns it with the SQL store the deletions are
// scheduled in and read back from.
func newBlobDeletionServiceHarness(t *testing.T, blobStores map[storetypes.BlobStoreType]blob.Store) (*Server, *blockchainsql.SQL, *testBlobDeletionObserver) {
	t.Helper()

	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Pruner.SkipBlobDeletion = false
	tSettings.Pruner.BlobDeletionSafetyWindow = 0
	tSettings.Pruner.BlobDeletionBatchSize = 100
	tSettings.Pruner.BlobDeletionMaxRetries = 3

	sqlURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	sqlStore, err := blockchainsql.New(logger, sqlURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlStore.Close(context.Background()) })

	bcServer, err := blockchain.New(ctx, logger, tSettings, sqlStore, nil)
	require.NoError(t, err)

	observer := &testBlobDeletionObserver{
		t:        t,
		complete: make(chan blobDeletionEvent, 10),
	}

	server := &Server{
		ctx:                  ctx,
		logger:               logger,
		settings:             tSettings,
		blobStores:           blobStores,
		blockchainClient:     &blobDeletionServiceClient{Mock: &blockchain.Mock{}, service: bcServer},
		blobDeletionObserver: observer,
	}

	return server, sqlStore, observer
}

// TestBlobDeletion_UnauthorizedHTTPStoreKeepsDeletionQueued pins what the pruner does when an
// HTTP blob store answers 401 - no token configured here, or a different one from the
// server's. Retrying cannot fix that, so the deletion must stay queued with its retry count
// untouched, rather than be counted as a failure and dropped after maxRetries while the blob
// stays on the server forever. Once the tokens match, the next pass deletes it.
func TestBlobDeletion_UnauthorizedHTTPStoreKeepsDeletionQueued(t *testing.T) {
	const token = "pruner-token"

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	memoryURL, err := url.Parse("memory://")
	require.NoError(t, err)

	blobServer, err := blob.NewHTTPBlobServer(logger, memoryURL, token)
	require.NoError(t, err)

	httpServer := httptest.NewServer(blobServer)
	t.Cleanup(httpServer.Close)

	serverURL, err := url.Parse(httpServer.URL)
	require.NoError(t, err)

	authed, err := blob.NewStore(logger, serverURL, bloboptions.WithHTTPAuthToken(token))
	require.NoError(t, err)

	// The shipped default: no token configured.
	unauthed, err := blob.NewStore(logger, serverURL, bloboptions.WithHTTPAuthToken(""))
	require.NoError(t, err)

	key := []byte("pruner-unauthorized-key")
	require.NoError(t, authed.Set(ctx, key, fileformat.FileTypeTesting, []byte("blob held on the server")))

	server, sqlStore, observer := newBlobDeletionServiceHarness(t, map[storetypes.BlobStoreType]blob.Store{
		storetypes.TXSTORE: unauthed,
	})

	_, err = sqlStore.ScheduleBlobDeletion(ctx, &blockchainsql.ScheduleRequest{
		BlobKey:        key,
		FileType:       string(fileformat.FileTypeTesting),
		StoreType:      int32(storetypes.TXSTORE),
		DeleteAtHeight: 10,
	})
	require.NoError(t, err)

	maxRetries := server.settings.Pruner.BlobDeletionMaxRetries

	for pass := 1; pass <= maxRetries+1; pass++ {
		server.processBlobDeletionsAtHeight(10, chainhash.Hash{})

		event, err := observer.waitFor(5 * time.Second)
		require.NoError(t, err)
		require.Zero(t, event.successCount, "pass %d: a refused deletion is not a success", pass)
		require.Zero(t, event.failCount, "pass %d: a refused deletion is not counted as a failure", pass)
	}

	pending, err := sqlStore.GetPendingBlobDeletions(ctx, 10, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1, "the deletion must still be queued after more passes than maxRetries")
	require.Zero(t, pending[0].RetryCount, "a configuration error must not use up the retry budget")

	exists, err := authed.Exists(ctx, key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.True(t, exists, "the blob must still be on the server")

	// Fix the configuration: the next pass deletes the blob and empties the queue.
	server.blobStores[storetypes.TXSTORE] = authed

	server.processBlobDeletionsAtHeight(10, chainhash.Hash{})

	event, err := observer.waitFor(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, int64(1), event.successCount)

	exists, err = authed.Exists(ctx, key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.False(t, exists, "the blob must be deleted once the tokens match")

	pending, err = sqlStore.GetPendingBlobDeletions(ctx, 10, 10)
	require.NoError(t, err)
	require.Empty(t, pending)
}

// TestBlobDeletion_StoreConstructionConfigErrorKeepsDeletionQueued pins that a blob store the
// pruner cannot even build - here a store type with no URL configured - is held the same way
// as a 401: the configuration error survives the wrap in processOneDeletion, and the deletion
// stays queued with its retry count untouched.
func TestBlobDeletion_StoreConstructionConfigErrorKeepsDeletionQueued(t *testing.T) {
	ctx := context.Background()

	server, sqlStore, observer := newBlobDeletionServiceHarness(t, map[storetypes.BlobStoreType]blob.Store{})

	// GetBlobStoreURL then returns (nil, nil), which getBlobStore reports as a configuration error.
	server.settings.Block.TxStore = nil

	_, err := sqlStore.ScheduleBlobDeletion(ctx, &blockchainsql.ScheduleRequest{
		BlobKey:        []byte("pruner-no-store-key"),
		FileType:       string(fileformat.FileTypeTesting),
		StoreType:      int32(storetypes.TXSTORE),
		DeleteAtHeight: 10,
	})
	require.NoError(t, err)

	maxRetries := server.settings.Pruner.BlobDeletionMaxRetries

	for pass := 1; pass <= maxRetries+1; pass++ {
		server.processBlobDeletionsAtHeight(10, chainhash.Hash{})

		event, err := observer.waitFor(5 * time.Second)
		require.NoError(t, err)
		require.Zero(t, event.successCount, "pass %d", pass)
		require.Zero(t, event.failCount, "pass %d", pass)
	}

	pending, err := sqlStore.GetPendingBlobDeletions(ctx, 10, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1, "the deletion must still be queued after more passes than maxRetries")
	require.Zero(t, pending[0].RetryCount, "a configuration error must not use up the retry budget")

	row := pending[0]

	_, err = server.processOneDeletion(ctx, &blockchain_api.ScheduledDeletion{
		Id:             row.ID,
		BlobKey:        row.BlobKey,
		FileType:       row.FileType,
		StoreType:      row.StoreType,
		DeleteAtHeight: row.DeleteAtHeight,
	}, "", 10)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrConfiguration), "the store-construction configuration error must stay classifiable")
	require.NotContains(t, err.Error(), "MISSING", "the wrapped error must not leave a formatting artefact")
}
