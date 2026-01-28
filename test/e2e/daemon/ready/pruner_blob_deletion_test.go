package smoke

// TODO: This E2E test needs to be updated to use blockchain service instead of pruner service.
// Blob deletion scheduling is now managed by the blockchain service, not the pruner service.
//
// The test should be refactored to:
// 1. Connect to blockchain service instead of pruner service
// 2. Use blockchain_api.BlobStoreType instead of pruner_api.BlobStoreType
// 3. Call blockchain service gRPC methods: ScheduleBlobDeletion, ListScheduledDeletions, etc.
// 4. Update assertions to reflect new architecture
//
// Commenting out until refactored.

/*
import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestPrunerBlobDeletionScheduling verifies that the blockchain service correctly schedules
// blob deletions in its queue when blobs are created with DAH.
//
// NOTE: This test only validates SCHEDULING functionality (gRPC API and queue operations).
// Actual DELETION execution is fully tested in unit tests (services/pruner/blob_deletion_test.go)
// which directly call processBlobDeletionsAtHeight() without the complexity of daemon startup.
//
// Test flow:
// 1. Create TestDaemon with Blockchain and Pruner services
// 2. Create file blob store with blockchain client injected
// 3. Write test blobs with DAH (Delete-At-Height)
// 4. Verify blobs are scheduled in blockchain queue via gRPC API
// 5. Verify queue queries work (List, filters)
// 6. Verify cancellation works
func TestPrunerBlobDeletionScheduling(t *testing.T) {
	t.Skip("TODO: Needs refactoring to use blockchain service instead of pruner service")
	// Create test daemon with Blockchain and Pruner services
	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnablePruner:     true,
		EnableRPC:        true,
		UTXOStoreType:    "aerospike",
		UseUnifiedLogger: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				// Enable blob deletion with aggressive settings for testing
				s.Pruner.BlobDeletionEnabled = true
				s.Pruner.BlobDeletionSafetyWindow = 0 // No safety window for testing
				s.Pruner.BlobDeletionBatchSize = 100
				s.Pruner.BlobDeletionMaxRetries = 3
				// Use OnBlockMined trigger since we don't have block persister
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockMined
			},
		),
	})
	defer node.Stop(t, true)

	// Get current blockchain height
	ctx := context.Background()
	_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	currentHeight := meta.Height
	t.Logf("Current blockchain height: %d", currentHeight)

	// Wait a moment for pruner service to be fully ready
	time.Sleep(2 * time.Second)

	// Connect to pruner service
	conn, err := grpc.DialContext(ctx, node.Settings.Pruner.GRPCAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second))
	require.NoError(t, err)
	defer conn.Close()

	prunerClient := pruner_api.NewPrunerAPIClient(conn)
	prunerClientWrapper := options.NewGRPCPrunerClient(prunerClient, node.Logger)

	// Create test blob store AFTER daemon is ready (so pruner client works)
	testStoreDir := filepath.Join(t.TempDir(), "test_blobs")
	err = os.MkdirAll(testStoreDir, 0755)
	require.NoError(t, err)

	storeURL := &url.URL{
		Scheme: "file",
		Path:   testStoreDir,
	}

	testStore, err := blob.NewStore(node.Logger, storeURL,
		options.WithPrunerClient(prunerClientWrapper),
		options.WithStoreType(int32(pruner_api.BlobStoreType_TXSTORE)))
	require.NoError(t, err)
	t.Log("Created test blob store with pruner client")

	// Create test blobs with different DAH values
	testBlobs := []struct {
		key             []byte
		data            []byte
		deleteAtHeight  uint32
		shouldBeDeleted bool
	}{
		{
			key:             make([]byte, 32),
			data:            []byte("test blob 1 - delete at current+1"),
			deleteAtHeight:  currentHeight + 1,
			shouldBeDeleted: true,
		},
		{
			key:             make([]byte, 32),
			data:            []byte("test blob 2 - delete at current+2"),
			deleteAtHeight:  currentHeight + 2,
			shouldBeDeleted: false, // Won't be deleted after 1 block
		},
		{
			key:             make([]byte, 32),
			data:            []byte("test blob 3 - delete at current+1"),
			deleteAtHeight:  currentHeight + 1,
			shouldBeDeleted: true,
		},
	}

	// Generate random keys for test blobs
	for i := range testBlobs {
		_, err := rand.Read(testBlobs[i].key)
		require.NoError(t, err)
	}

	// Write blobs with DAH
	for i, blob := range testBlobs {
		err = testStore.Set(ctx, blob.key, fileformat.FileTypeTesting, blob.data,
			options.WithDeleteAt(blob.deleteAtHeight))
		require.NoError(t, err)
		t.Logf("Created test blob %d with key=%x, DAH=%d", i, blob.key[:8], blob.deleteAtHeight)
	}

	// Verify blobs exist
	for i, blob := range testBlobs {
		exists, err := testStore.Exists(ctx, blob.key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists, "Blob %d should exist after creation", i)
	}

	// Give the pruner client time to schedule asynchronously (gRPC calls are async)
	time.Sleep(1 * time.Second)

	// Verify blobs are scheduled in pruner queue (list all, don't filter by store)
	listResp, err := prunerClient.ListScheduledDeletions(ctx, &pruner_api.ListScheduledDeletionsRequest{
		Limit: 100,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(listResp.Deletions), 3, "Should have at least 3 scheduled deletions")
	t.Logf("✓ Pruner queue has %d scheduled deletions", len(listResp.Deletions))

	// Verify query by height range works
	listResp, err = prunerClient.ListScheduledDeletions(ctx, &pruner_api.ListScheduledDeletionsRequest{
		MaxHeight: currentHeight + 1,
		Limit:     100,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(listResp.Deletions), "Should have 2 deletions for DAH <= %d", currentHeight+1)
	t.Log("✓ Height range filtering works")

	// Verify query by future height
	listResp, err = prunerClient.ListScheduledDeletions(ctx, &pruner_api.ListScheduledDeletionsRequest{
		MinHeight: currentHeight + 2,
		Limit:     100,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(listResp.Deletions), "Should have 1 deletion for DAH >= %d", currentHeight+2)
	t.Log("✓ Future deletions queried correctly")

	// Test cancellation by deleting one of the scheduled blobs
	testBlob := testBlobs[0]
	cancelResp, err := prunerClient.CancelBlobDeletion(ctx, &pruner_api.CancelBlobDeletionRequest{
		BlobKey:   testBlob.key,
		FileType:  string(fileformat.FileTypeTesting),
		StoreType: pruner_api.BlobStoreType_TXSTORE,
	})
	require.NoError(t, err)
	require.True(t, cancelResp.Cancelled, "Cancellation should succeed")
	t.Log("✓ Cancellation via gRPC works")

	// Verify cancelled blob no longer in queue
	listResp, err = prunerClient.ListScheduledDeletions(ctx, &pruner_api.ListScheduledDeletionsRequest{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(listResp.Deletions), "Should have 2 deletions after cancelling 1")
	t.Log("✓ Queue updated after cancellation")

	t.Log("✅ All pruner scheduling operations validated successfully")
	t.Log("Note: Actual deletion execution is tested in services/pruner/blob_deletion_test.go")
}

// TestPrunerBlobDeletionCancellation is redundant - cancellation is tested in the main test above
// and in unit tests (services/pruner/blob_deletion_test.go).
func TestPrunerBlobDeletionCancellation(t *testing.T) {
	t.Skip("Covered by TestPrunerBlobDeletionScheduling and unit tests")
	// Create test daemon
	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnablePruner:     true,
		EnableRPC:        true,
		UTXOStoreType:    "aerospike",
		UseUnifiedLogger: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.Pruner.BlobDeletionEnabled = true
				s.Pruner.BlobDeletionSafetyWindow = 0
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockMined
			},
		),
	})
	defer node.Stop(t, true)

	ctx := context.Background()
	_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	currentHeight := meta.Height

	// Wait for pruner to be ready
	time.Sleep(2 * time.Second)

	// Connect to pruner
	conn, err := grpc.DialContext(ctx, node.Settings.Pruner.GRPCAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second))
	require.NoError(t, err)
	defer conn.Close()

	prunerClient := pruner_api.NewPrunerAPIClient(conn)
	prunerClientWrapper := options.NewGRPCPrunerClient(prunerClient, node.Logger)

	// Create test store after daemon is ready
	testStoreDir := filepath.Join(t.TempDir(), "test_blobs_cancel")
	err = os.MkdirAll(testStoreDir, 0755)
	require.NoError(t, err)

	storeURL := &url.URL{
		Scheme: "file",
		Path:   testStoreDir,
	}

	testStore, err := blob.NewStore(node.Logger, storeURL,
		options.WithPrunerClient(prunerClientWrapper),
		options.WithStoreType(int32(pruner_api.BlobStoreType_SUBTREESTORE)))
	require.NoError(t, err)

	// Create test blob with DAH
	testKey := make([]byte, 32)
	_, err = rand.Read(testKey)
	require.NoError(t, err)

	testData := []byte("test blob - will be cancelled")
	deleteAt := currentHeight + 1

	err = testStore.Set(ctx, testKey, fileformat.FileTypeTesting, testData,
		options.WithDeleteAt(deleteAt))
	require.NoError(t, err)
	t.Logf("Created test blob with key=%x, DAH=%d", testKey[:8], deleteAt)

	// Verify blob is scheduled
	time.Sleep(1 * time.Second) // Give the pruner client time to schedule
	listResp, err := prunerClient.ListScheduledDeletions(ctx, &pruner_api.ListScheduledDeletionsRequest{
		StoreType: pruner_api.BlobStoreType_SUBTREESTORE,
		Limit:     100,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(listResp.Deletions), 1, "Should have at least 1 scheduled deletion")
	t.Logf("Pruner queue has %d scheduled deletions", len(listResp.Deletions))

	// Cancel the deletion by setting DAH to 0
	t.Log("Cancelling deletion by setting DAH to 0")
	err = testStore.SetDAH(ctx, testKey, fileformat.FileTypeTesting, 0)
	require.NoError(t, err)

	// Give cancellation time to propagate
	time.Sleep(1 * time.Second)

	// Verify deletion is cancelled in queue
	found := false
	listResp, err = prunerClient.ListScheduledDeletions(ctx, &pruner_api.ListScheduledDeletionsRequest{
		StoreType: pruner_api.BlobStoreType_SUBTREESTORE,
		Limit:     100,
	})
	require.NoError(t, err)
	for _, d := range listResp.Deletions {
		if bytes.Equal(d.BlobKey, testKey) {
			found = true
			break
		}
	}
	require.False(t, found, "Test blob should not be in queue after cancellation")
	t.Log("✓ Deletion cancelled in pruner queue")

	// Generate blocks past the original DAH
	t.Log("Generating blocks to verify blob persists...")
	_, err = node.CallRPC(node.Ctx, "generate", []any{2})
	require.NoError(t, err)

	// Wait for blocks to be processed
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, newMeta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			return false
		}
		return newMeta.Height >= currentHeight+2
	}, 10*time.Second, 100*time.Millisecond, "Blocks were not processed")

	// Wait for pruner processing
	time.Sleep(5 * time.Second)

	// Verify blob still exists (not deleted)
	exists, err := testStore.Exists(ctx, testKey, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.True(t, exists, "Blob should still exist after cancellation")
	t.Log("✓ Blob persisted after DAH cancellation")
}

// TestPrunerBlobDeletionIdempotency is fully tested in unit tests.
func TestPrunerBlobDeletionIdempotency(t *testing.T) {
	t.Skip("Fully covered by unit test services/pruner/blob_deletion_test.go::TestBlobDeletionIdempotency")
	// Create test daemon
	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnablePruner:     true,
		EnableRPC:        true,
		UTXOStoreType:    "aerospike",
		UseUnifiedLogger: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.Pruner.BlobDeletionEnabled = true
				s.Pruner.BlobDeletionSafetyWindow = 0
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockMined
			},
		),
	})
	defer node.Stop(t, true)

	ctx := context.Background()
	_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	currentHeight := meta.Height

	// Wait for pruner to be ready
	time.Sleep(2 * time.Second)

	// Connect to pruner
	conn, err := grpc.DialContext(ctx, node.Settings.Pruner.GRPCAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second))
	require.NoError(t, err)
	defer conn.Close()

	prunerClient := pruner_api.NewPrunerAPIClient(conn)
	prunerClientWrapper := options.NewGRPCPrunerClient(prunerClient, node.Logger)

	// Create test store after daemon is ready
	testStoreDir := filepath.Join(t.TempDir(), "test_blobs_idempotent")
	err = os.MkdirAll(testStoreDir, 0755)
	require.NoError(t, err)

	storeURL := &url.URL{
		Scheme: "file",
		Path:   testStoreDir,
	}

	testStore, err := blob.NewStore(node.Logger, storeURL,
		options.WithPrunerClient(prunerClientWrapper),
		options.WithStoreType(int32(pruner_api.BlobStoreType_BLOCKSTORE)))
	require.NoError(t, err)

	// Create test blob
	testKey := make([]byte, 32)
	_, err = rand.Read(testKey)
	require.NoError(t, err)

	testData := []byte("test blob - manual deletion")
	deleteAt := currentHeight + 1

	err = testStore.Set(ctx, testKey, fileformat.FileTypeTesting, testData,
		options.WithDeleteAt(deleteAt))
	require.NoError(t, err)
	t.Logf("Created test blob with key=%x, DAH=%d", testKey[:8], deleteAt)

	// Manually delete the blob before pruner runs
	t.Log("Manually deleting blob before pruner processes it")
	err = testStore.Del(ctx, testKey, fileformat.FileTypeTesting)
	require.NoError(t, err)

	// Verify blob is gone
	exists, err := testStore.Exists(ctx, testKey, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.False(t, exists, "Blob should be deleted")

	// Generate block to trigger pruner
	t.Log("Generating block to trigger pruner...")
	_, err = node.CallRPC(node.Ctx, "generate", []any{1})
	require.NoError(t, err)

	// Wait for block processing
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, newMeta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			return false
		}
		return newMeta.Height >= currentHeight+1
	}, 10*time.Second, 100*time.Millisecond, "Block was not processed")

	// Wait for pruner
	time.Sleep(5 * time.Second)

	// Verify pruner handled the already-deleted blob gracefully (no errors, queue cleaned)
	listResp, err := prunerClient.ListScheduledDeletions(ctx, &pruner_api.ListScheduledDeletionsRequest{
		MaxHeight: currentHeight + 1,
		StoreType: pruner_api.BlobStoreType_BLOCKSTORE,
		Limit:     100,
	})
	require.NoError(t, err)
	// The queue should not have the deleted blob anymore (pruner removes it after successful deletion attempt)
	// Since blob was already manually deleted, pruner should handle gracefully and remove from queue
	require.Equal(t, 0, len(listResp.Deletions), "Queue should be clean after idempotent deletion")
	t.Log("✓ Pruner handled already-deleted blob gracefully")
}

// TestPrunerBlobDeletionRetry verifies that the pruner retries failed deletions
// and eventually gives up after max retries.
func TestPrunerBlobDeletionRetry(t *testing.T) {
	t.Skip("TODO: Implement retry test - requires simulating deletion failures")
	// This test would require:
	// 1. Creating a blob store wrapper that fails deletions N times
	// 2. Verifying retry_count increments
	// 3. Verifying row is removed after max retries
}

// TestPrunerBlobDeletionMultipleStores verifies that the pruner correctly handles
// deletions from multiple different blob stores.
func TestPrunerBlobDeletionMultipleStores(t *testing.T) {
	t.Skip("TODO: Implement multiple stores test - requires custom stores to be registered with pruner")
	// This test would require:
	// 1. Creating custom blob stores
	// 2. Registering them with the pruner's blobStores map
	// 3. Or, using daemon's existing stores (tx, subtree) and creating blobs in each

	// Create test daemon
	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnablePruner:     true,
		EnableRPC:        true,
		UTXOStoreType:    "aerospike",
		UseUnifiedLogger: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.Pruner.BlobDeletionEnabled = true
				s.Pruner.BlobDeletionSafetyWindow = 0
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockMined
			},
		),
	})
	defer node.Stop(t, true)

	ctx := context.Background()
	_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	currentHeight := meta.Height

	// Connect to pruner
	conn, err := grpc.DialContext(ctx, node.Settings.Pruner.GRPCAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second))
	require.NoError(t, err)
	defer conn.Close()

	prunerClient := pruner_api.NewPrunerAPIClient(conn)
	prunerClientWrapper := options.NewGRPCPrunerClient(prunerClient, node.Logger)

	// Create multiple blob stores with different store IDs
	stores := make(map[string]blob.Store)
	storeIDs := []string{"test_store_a", "test_store_b", "test_store_c"}

	for _, storeID := range storeIDs {
		testStoreDir := filepath.Join(t.TempDir(), storeID)
		err = os.MkdirAll(testStoreDir, 0755)
		require.NoError(t, err)

		storeURL := &url.URL{
			Scheme: "file",
			Path:   testStoreDir,
		}

		store, err := blob.NewStore(node.Logger, storeURL,
			options.WithPrunerClient(prunerClientWrapper),
			options.WithStoreType(int32(pruner_api.BlobStoreType_TXSTORE)))
		require.NoError(t, err)

		stores[storeID] = store
	}

	// Create blobs in each store
	blobKeys := make(map[string][]byte)
	for storeID, store := range stores {
		key := make([]byte, 32)
		_, err = rand.Read(key)
		require.NoError(t, err)

		data := []byte(fmt.Sprintf("test blob in store %s", storeID))
		err = store.Set(ctx, key, fileformat.FileTypeTesting, data,
			options.WithDeleteAt(currentHeight+1))
		require.NoError(t, err)

		blobKeys[storeID] = key
		t.Logf("Created blob in store %s with key=%x", storeID, key[:8])
	}

	// Verify all blobs are scheduled
	listResp, err := prunerClient.ListScheduledDeletions(ctx, &pruner_api.ListScheduledDeletionsRequest{
		Limit: 100,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(listResp.Deletions), 3, "Should have at least 3 scheduled deletions")

	// Generate block
	t.Log("Generating block to trigger pruner...")
	_, err = node.CallRPC(node.Ctx, "generate", []any{1})
	require.NoError(t, err)

	// Wait for block
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, newMeta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			return false
		}
		return newMeta.Height >= currentHeight+1
	}, 10*time.Second, 100*time.Millisecond, "Block was not processed")

	// Wait for pruner
	time.Sleep(5 * time.Second)

	// Verify all blobs are deleted from all stores
	for storeID, store := range stores {
		key := blobKeys[storeID]
		exists, err := store.Exists(ctx, key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.False(t, exists, "Blob in store %s should have been deleted", storeID)
		t.Logf("✓ Blob in store %s was deleted", storeID)
	}

	// Verify queue is clean
	listResp, err = prunerClient.ListScheduledDeletions(ctx, &pruner_api.ListScheduledDeletionsRequest{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Equal(t, 0, len(listResp.Deletions), "Queue should be completely empty")
	t.Log("✓ All stores processed successfully")
}
*/
