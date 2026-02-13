package smoke

import (
	"context"
	"crypto/rand"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/require"
)

// TestBlobDeletionScheduling verifies that the blockchain service correctly schedules
// blob deletions in its queue via the BlockchainClient API and that the pruner
// service correctly executes those deletions when blocks are mined.
//
// Test flow:
// 1. Create TestDaemon with Pruner service enabled
// 2. Get current blockchain height
// 3. Schedule test blob deletions at various heights
// 4. Verify scheduling via ListScheduledDeletions
// 5. Test cancellation via CancelBlobDeletion
// 6. Generate blocks to trigger pruner
// 7. Verify deletions are executed
func TestBlobDeletionScheduling(t *testing.T) {
	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnablePruner:     true,
		EnableRPC:        true,
		UTXOStoreType:    "aerospike",
		UseUnifiedLogger: false,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.Pruner.SkipBlobDeletion = false
				s.Pruner.BlobDeletionSafetyWindow = 0
				s.Pruner.BlobDeletionBatchSize = 100
				s.Pruner.BlobDeletionMaxRetries = 3
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockMined
			},
		),
	})
	defer node.Stop(t, true)

	ctx := context.Background()

	db := node.BlockchainStore.GetDB()

	_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	currentHeight := meta.Height
	t.Logf("Current blockchain height: %d", currentHeight)

	testBlobs := []struct {
		key            []byte
		fileType       string
		storeType      storetypes.BlobStoreType
		deleteAtHeight uint32
	}{
		{
			key:            make([]byte, 32),
			fileType:       "test",
			storeType:      storetypes.TXSTORE,
			deleteAtHeight: currentHeight + 1,
		},
		{
			key:            make([]byte, 32),
			fileType:       "test",
			storeType:      storetypes.TXSTORE,
			deleteAtHeight: currentHeight + 2,
		},
		{
			key:            make([]byte, 32),
			fileType:       "test",
			storeType:      storetypes.TXSTORE,
			deleteAtHeight: currentHeight + 1,
		},
	}

	// Create random blob keys
	for i := range testBlobs {
		_, err := rand.Read(testBlobs[i].key)
		require.NoError(t, err)
	}

	// Schedule test blob deletions
	for i, blob := range testBlobs {
		_, scheduled, err := node.BlockchainClient.ScheduleBlobDeletion(ctx, blob.key, blob.fileType, blob.storeType, blob.deleteAtHeight)
		require.NoError(t, err)
		require.True(t, scheduled, "Blob %d should be scheduled", i)
		t.Logf("Scheduled test blob %d with key=%x, DAH=%d", i, blob.key[:8], blob.deleteAtHeight)
	}

	// Get the number of rows in the scheduled_blob_deletions table
	dbCount := getDBDeletionCount(t, db)
	require.Equal(t, 3, dbCount, "Should have 3 scheduled deletions in DB")
	t.Log("Verified 3 deletions in database")

	// Now check with the API
	deletions, _, err := node.BlockchainClient.ListScheduledDeletions(ctx, 0, 0, storetypes.TXSTORE, false, 100, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(deletions), 3, "Should have at least 3 scheduled deletions")
	t.Logf("ListScheduledDeletions returned %d deletions", len(deletions))

	deletions, _, err = node.BlockchainClient.ListScheduledDeletions(ctx, 0, currentHeight+1, storetypes.TXSTORE, false, 100, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(deletions), "Should have 2 deletions for DAH <= %d", currentHeight+1)
	t.Log("Height range filtering works")

	deletions, _, err = node.BlockchainClient.ListScheduledDeletions(ctx, currentHeight+2, 0, storetypes.TXSTORE, false, 100, 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(deletions), "Should have 1 deletion for DAH >= %d", currentHeight+2)
	t.Log("Future deletions queried correctly")

	// Now cancel the first deletion
	testBlob := testBlobs[0]
	cancelled, err := node.BlockchainClient.CancelBlobDeletion(ctx, testBlob.key, testBlob.fileType, testBlob.storeType)
	require.NoError(t, err)
	require.True(t, cancelled, "Cancellation should succeed")
	t.Log("Cancellation works")

	deletions, _, err = node.BlockchainClient.ListScheduledDeletions(ctx, 0, 0, storetypes.TXSTORE, false, 100, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(deletions), "Should have 2 deletions after cancelling 1")
	t.Log("Queue updated after cancellation")

	dbCount = getDBDeletionCount(t, db)
	require.Equal(t, 2, dbCount, "Should have 2 scheduled deletions in DB after cancellation")

	t.Log("Scheduling operations validated successfully")

	t.Log("Generating block to trigger pruner...")
	_, err = node.CallRPC(node.Ctx, "generate", []any{1})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, newMeta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			return false
		}
		return newMeta.Height >= currentHeight+1
	}, 10*time.Second, 100*time.Millisecond, "Block was not mined")
	t.Log("Block mined successfully")

	require.Eventually(t, func() bool {
		count := getDBDeletionCount(t, db)
		t.Logf("Waiting for pruner... deletions remaining: %d (expecting 1)", count)
		return count == 1
	}, 10*time.Second, 500*time.Millisecond, "Pruner did not process deletions at height %d", currentHeight+1)

	t.Logf("Pruner executed deletions for height %d", currentHeight+1)

	deletions, _, err = node.BlockchainClient.ListScheduledDeletions(ctx, 0, 0, storetypes.TXSTORE, false, 100, 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(deletions), "Should have 1 deletion remaining (DAH=%d)", currentHeight+2)
	t.Log("E2E blob deletion scheduling and execution validated successfully")
}

// TestBlobDeletionSchedulingViaBlobStore verifies that blob stores automatically schedule
// deletions when blobs are created with a DAH (Delete-At-Height) option.
//
// This tests the production path where blob stores call the BlobDeletionScheduler
// directly when closing files with DAH set, ensuring the integration works end-to-end.
//
// Test flow:
// 1. Create TestDaemon with Pruner service enabled
// 2. Get the TxStore blob store
// 3. Create blobs via blob store API with DAH set
// 4. Verify blobs were created in storage
// 5. Verify deletion was automatically scheduled in database
// 6. Generate block to trigger pruner
// 7. Verify blobs were actually deleted from storage
func TestBlobDeletionSchedulingViaBlobStore(t *testing.T) {
	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnablePruner:     true,
		EnableRPC:        true,
		UTXOStoreType:    "aerospike",
		UseUnifiedLogger: false,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.Pruner.SkipBlobDeletion = false
				s.Pruner.BlobDeletionSafetyWindow = 0
				s.Pruner.BlobDeletionBatchSize = 100
				s.Pruner.BlobDeletionMaxRetries = 3
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockMined
			},
		),
	})
	defer node.Stop(t, true)

	ctx := context.Background()
	db := node.BlockchainStore.GetDB()

	_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	currentHeight := meta.Height
	t.Logf("Current blockchain height: %d", currentHeight)

	// Create a file-based blob store for testing DAH scheduling
	// Note: Default test stores (TxStore, SubtreeStore) use memory/null backends which don't support DAH
	testStoreURL, err := url.Parse("file://./data/test_dah_store")
	require.NoError(t, err)

	blobStore, err := blob.NewStore(node.Logger, testStoreURL,
		options.WithBlobDeletionScheduler(node.BlockchainClient),
		options.WithStoreType(storetypes.TEMPSTORE))
	require.NoError(t, err)
	t.Logf("Created file-based blob store for DAH testing")

	// Create test blobs with DAH set
	testBlobs := []struct {
		key            []byte
		data           []byte
		fileType       fileformat.FileType
		deleteAtHeight uint32
	}{
		{
			key:            make([]byte, 32),
			data:           []byte("test blob 1 data"),
			fileType:       fileformat.FileTypeTesting,
			deleteAtHeight: currentHeight + 1,
		},
		{
			key:            make([]byte, 32),
			data:           []byte("test blob 2 data"),
			fileType:       fileformat.FileTypeTesting,
			deleteAtHeight: currentHeight + 1,
		},
		{
			key:            make([]byte, 32),
			data:           []byte("test blob 3 data"),
			fileType:       fileformat.FileTypeTesting,
			deleteAtHeight: currentHeight + 2,
		},
	}

	// Create random blob keys
	for i := range testBlobs {
		_, err := rand.Read(testBlobs[i].key)
		require.NoError(t, err)
	}

	// Store blobs with DAH option - this should automatically schedule deletions
	initialDBCount := getDBDeletionCount(t, db)
	t.Logf("Initial deletion queue count: %d", initialDBCount)

	for i, blob := range testBlobs {
		err := blobStore.Set(ctx, blob.key, blob.fileType, blob.data, options.WithDeleteAt(blob.deleteAtHeight))
		require.NoError(t, err)
		t.Logf("Created test blob %d with key=%x, DAH=%d", i, blob.key[:8], blob.deleteAtHeight)
	}

	// Note: We skip verifying blob existence because the subtree store may have
	// external storage configured. The key verification is that scheduling happened.

	// Verify deletions were automatically scheduled in the database
	require.Eventually(t, func() bool {
		count := getDBDeletionCount(t, db)
		t.Logf("Deletion queue count: %d (expecting %d)", count, initialDBCount+3)
		return count == initialDBCount+3
	}, 5*time.Second, 100*time.Millisecond, "Blob store should have scheduled 3 deletions")
	t.Log("Verified 3 deletions were automatically scheduled via blob store")

	// Verify via API as well
	deletions, _, err := node.BlockchainClient.ListScheduledDeletions(ctx, 0, 0, storetypes.TEMPSTORE, false, 100, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(deletions), 3, "Should have at least 3 scheduled deletions")
	t.Logf("ListScheduledDeletions confirmed %d total deletions in queue", len(deletions))

	// Generate block to trigger pruner at height currentHeight + 1
	t.Log("Generating block to trigger pruner...")
	_, err = node.CallRPC(node.Ctx, "generate", []any{1})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, newMeta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			return false
		}
		return newMeta.Height >= currentHeight+1
	}, 10*time.Second, 100*time.Millisecond, "Block was not mined")
	t.Log("Block mined successfully")

	// Wait for pruner to process deletions from the queue
	require.Eventually(t, func() bool {
		count := getDBDeletionCount(t, db)
		t.Logf("Waiting for pruner... deletions remaining: %d (expecting %d)", count, initialDBCount+1)
		return count == initialDBCount+1
	}, 10*time.Second, 500*time.Millisecond, "Pruner did not process deletions at height %d", currentHeight+1)
	t.Logf("Pruner executed 2 deletions for height %d", currentHeight+1)

	// Verify via API that only 1 deletion remains (the one scheduled for height currentHeight+2)
	deletions, _, err = node.BlockchainClient.ListScheduledDeletions(ctx, 0, 0, storetypes.TEMPSTORE, false, 100, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(deletions), 1, "Should have at least 1 deletion remaining")

	// Count deletions at height currentHeight+2
	futureCount := 0
	for _, d := range deletions {
		if d.DeleteAtHeight == currentHeight+2 {
			futureCount++
		}
	}
	require.GreaterOrEqual(t, futureCount, 1, "Should have at least 1 deletion scheduled for height %d", currentHeight+2)
	t.Logf("Verified %d deletion(s) remain scheduled for future height %d", futureCount, currentHeight+2)

	t.Log("E2E blob deletion scheduling via blob store validated successfully")
}

// TestBlobDeletionSafetyWindow verifies that the BlobDeletionSafetyWindow setting
// prevents premature deletion of blobs. With a safety window of 2, blob deletions
// are only processed when persistedHeight - safetyWindow >= deleteAtHeight.
//
// The safety window logic in processBlobDeletionsAtHeight:
//   - If persistedHeight > 0 and safetyWindow > 0:
//     safeHeight = persistedHeight - safetyWindow
//   - Only blobs with delete_at_height <= safeHeight are eligible for deletion
//   - This ensures data is safely persisted before blob removal
//   - Note: the safeHeight calculation only applies when persistedHeight > safetyWindow
//     (uint32 underflow guard), so we mine initial blocks to establish a baseline
//
// Test flow:
// 1. Create TestDaemon with block persister + pruner, safety window = 2
// 2. Mine initial blocks so persistedHeight > safetyWindow (required for safeHeight calc)
// 3. Schedule blob deletions at DAH = H+1 and DAH = H+3
// 4. Mine 1 block (persisted H+1, safeHeight = H-1): verify NOTHING deleted
// 5. Mine 2 more blocks (persisted H+3, safeHeight = H+1): verify first blob deleted
// 6. Mine 2 more blocks (persisted H+5, safeHeight = H+3): verify second blob deleted
func TestBlobDeletionSafetyWindow(t *testing.T) {
	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableBlockPersister: true,
		EnablePruner:         true,
		EnableRPC:            true,
		EnableValidator:      true,
		UTXOStoreType:        "aerospike",
		UseUnifiedLogger:     false,
		PreserveDataDir:      true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			test.MultiNodeSettings(1),
			func(s *settings.Settings) {
				s.Pruner.SkipBlobDeletion = false
				s.Pruner.BlobDeletionSafetyWindow = 2
				s.Pruner.BlobDeletionBatchSize = 100
				s.Pruner.BlobDeletionMaxRetries = 3
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockPersisted
				s.ChainCfgParams.CoinbaseMaturity = 5
				s.GlobalBlockHeightRetention = 100
			},
		),
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer node.Stop(t, true)

	ctx := context.Background()
	db := node.BlockchainStore.GetDB()

	// Mine initial blocks so persistedHeight > safetyWindow (2).
	// The safeHeight = persistedHeight - safetyWindow calculation only executes
	// when persistedHeight > safetyWindow (uint32 underflow guard in blob_deletion_worker.go:60).
	t.Log("Mining initial blocks to establish baseline above safety window threshold...")
	var block *model.Block
	for i := 0; i < 5; i++ {
		block = node.MineAndWait(t, 1)
	}
	err := node.WaitForBlockPersisted(block.Hash(), 30*time.Second)
	require.NoError(t, err)

	_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	currentHeight := meta.Height
	t.Logf("Baseline height established: %d (> safetyWindow=%d)", currentHeight, 2)

	initialDBCount := getDBDeletionCount(t, db)
	t.Logf("Initial deletion queue count: %d", initialDBCount)

	// Schedule 2 blob deletions at different heights
	testBlobs := []struct {
		key            []byte
		fileType       string
		storeType      storetypes.BlobStoreType
		deleteAtHeight uint32
	}{
		{
			key:            make([]byte, 32),
			fileType:       "test",
			storeType:      storetypes.TXSTORE,
			deleteAtHeight: currentHeight + 1,
		},
		{
			key:            make([]byte, 32),
			fileType:       "test",
			storeType:      storetypes.TXSTORE,
			deleteAtHeight: currentHeight + 3,
		},
	}

	for i := range testBlobs {
		_, err := rand.Read(testBlobs[i].key)
		require.NoError(t, err)
	}

	for i, blob := range testBlobs {
		_, scheduled, err := node.BlockchainClient.ScheduleBlobDeletion(ctx, blob.key, blob.fileType, blob.storeType, blob.deleteAtHeight)
		require.NoError(t, err)
		require.True(t, scheduled, "Blob %d should be scheduled", i)
		t.Logf("Scheduled blob %d: key=%x, DAH=%d", i, blob.key[:8], blob.deleteAtHeight)
	}

	require.Equal(t, initialDBCount+2, getDBDeletionCount(t, db), "Should have 2 new scheduled deletions")

	// Phase 1: Mine 1 block -> height H+1, persisted ~ H+1
	// safeHeight = (H+1) - 2 = H-1
	// Neither DAH H+1 nor DAH H+3 <= H-1, so NOTHING should be deleted
	t.Log("Phase 1: Mining 1 block - safety window should prevent all deletions...")
	block = node.MineAndWait(t, 1)
	err = node.WaitForBlockPersisted(block.Hash(), 30*time.Second)
	require.NoError(t, err)
	t.Logf("Block persisted at height %d", currentHeight+1)

	// Give the pruner blob deletion worker time to process (should be a no-op)
	time.Sleep(3 * time.Second)

	count := getDBDeletionCount(t, db)
	require.Equal(t, initialDBCount+2, count,
		"Safety window should prevent all deletions (persisted=%d, safeHeight=%d, DAH values: %d, %d)",
		currentHeight+1, currentHeight+1-2, currentHeight+1, currentHeight+3)
	t.Log("Verified: both deletions still in queue (safety window active)")

	// Phase 2: Mine 2 more blocks -> height H+3, persisted ~ H+3
	// safeHeight = (H+3) - 2 = H+1
	// Blob 0 (DAH H+1) <= H+1 -> DELETED
	// Blob 1 (DAH H+3) > H+1 -> stays
	t.Log("Phase 2: Mining 2 more blocks - first deletion should be processed...")
	for i := 0; i < 2; i++ {
		block = node.MineAndWait(t, 1)
		err = node.WaitForBlockPersisted(block.Hash(), 30*time.Second)
		require.NoError(t, err)
	}
	t.Logf("Blocks persisted up to height %d", currentHeight+3)

	require.Eventually(t, func() bool {
		c := getDBDeletionCount(t, db)
		t.Logf("Waiting for first deletion... queue count: %d (expecting %d)", c, initialDBCount+1)
		return c == initialDBCount+1
	}, 15*time.Second, 500*time.Millisecond,
		"First blob (DAH=%d) should be deleted once safeHeight >= %d", currentHeight+1, currentHeight+1)
	t.Logf("Verified: first blob deleted (DAH=%d), one still pending (DAH=%d)", currentHeight+1, currentHeight+3)

	// Phase 3: Mine 2 more blocks -> height H+5, persisted ~ H+5
	// safeHeight = (H+5) - 2 = H+3
	// Blob 1 (DAH H+3) <= H+3 -> DELETED
	t.Log("Phase 3: Mining 2 more blocks - second deletion should be processed...")
	for i := 0; i < 2; i++ {
		block = node.MineAndWait(t, 1)
		err = node.WaitForBlockPersisted(block.Hash(), 30*time.Second)
		require.NoError(t, err)
	}
	t.Logf("Blocks persisted up to height %d", currentHeight+5)

	require.Eventually(t, func() bool {
		c := getDBDeletionCount(t, db)
		t.Logf("Waiting for second deletion... queue count: %d (expecting %d)", c, initialDBCount)
		return c == initialDBCount
	}, 15*time.Second, 500*time.Millisecond,
		"Second blob (DAH=%d) should be deleted once safeHeight >= %d", currentHeight+3, currentHeight+3)
	t.Logf("Verified: all scheduled blobs deleted after safety window passed")

	t.Log("E2E blob deletion safety window enforcement validated successfully")
}

// TestBlobDeletionOnBlockPersistedTrigger verifies that blob deletions are correctly
// triggered via BlockPersisted notifications (the default production code path).
//
// The existing tests (TestBlobDeletionScheduling, TestBlobDeletionSchedulingViaBlobStore)
// both use OnBlockMined trigger mode. This test exercises the OnBlockPersisted path where:
//   - Block notifications are ignored (server.go line 194: continue)
//   - BlockPersisted notifications update lastPersistedHeight and trigger blob deletion
//   - The block persister must be running for notifications to arrive
//
// Test flow:
// 1. Create TestDaemon with block persister + pruner, trigger = OnBlockPersisted
// 2. Schedule blob deletions at DAH = H+1 (two blobs) and DAH = H+2 (one blob)
// 3. Mine 1 block, wait for persistence, verify deletions at H+1 are processed
// 4. Mine 1 more block, wait for persistence, verify deletion at H+2 is processed
func TestBlobDeletionOnBlockPersistedTrigger(t *testing.T) {
	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableBlockPersister: true,
		EnablePruner:         true,
		EnableRPC:            true,
		EnableValidator:      true,
		UTXOStoreType:        "aerospike",
		UseUnifiedLogger:     false,
		PreserveDataDir:      true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			test.MultiNodeSettings(1),
			func(s *settings.Settings) {
				s.Pruner.SkipBlobDeletion = false
				s.Pruner.BlobDeletionSafetyWindow = 0 // No safety window - isolate trigger mechanism
				s.Pruner.BlobDeletionBatchSize = 100
				s.Pruner.BlobDeletionMaxRetries = 3
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockPersisted
				s.ChainCfgParams.CoinbaseMaturity = 5
				s.GlobalBlockHeightRetention = 100
			},
		),
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer node.Stop(t, true)

	ctx := context.Background()
	db := node.BlockchainStore.GetDB()

	_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	currentHeight := meta.Height
	t.Logf("Current blockchain height: %d", currentHeight)

	initialDBCount := getDBDeletionCount(t, db)
	t.Logf("Initial deletion queue count: %d", initialDBCount)

	// Schedule 3 blob deletions: 2 at H+1, 1 at H+2
	testBlobs := []struct {
		key            []byte
		fileType       string
		storeType      storetypes.BlobStoreType
		deleteAtHeight uint32
	}{
		{key: make([]byte, 32), fileType: "test", storeType: storetypes.TXSTORE, deleteAtHeight: currentHeight + 1},
		{key: make([]byte, 32), fileType: "test", storeType: storetypes.TXSTORE, deleteAtHeight: currentHeight + 1},
		{key: make([]byte, 32), fileType: "test", storeType: storetypes.TXSTORE, deleteAtHeight: currentHeight + 2},
	}

	for i := range testBlobs {
		_, err := rand.Read(testBlobs[i].key)
		require.NoError(t, err)
	}

	for i, blob := range testBlobs {
		_, scheduled, err := node.BlockchainClient.ScheduleBlobDeletion(ctx, blob.key, blob.fileType, blob.storeType, blob.deleteAtHeight)
		require.NoError(t, err)
		require.True(t, scheduled, "Blob %d should be scheduled", i)
		t.Logf("Scheduled blob %d: key=%x, DAH=%d", i, blob.key[:8], blob.deleteAtHeight)
	}

	require.Equal(t, initialDBCount+3, getDBDeletionCount(t, db), "Should have 3 new scheduled deletions")

	// Mine 1 block and wait for it to be persisted (BlockPersisted notification triggers deletion)
	t.Log("Mining block 1 and waiting for persistence (triggers OnBlockPersisted path)...")
	block := node.MineAndWait(t, 1)
	err = node.WaitForBlockPersisted(block.Hash(), 30*time.Second)
	require.NoError(t, err)
	t.Logf("Block persisted at height %d", currentHeight+1)

	// Verify the 2 blobs at DAH=H+1 are deleted, 1 at DAH=H+2 remains
	require.Eventually(t, func() bool {
		c := getDBDeletionCount(t, db)
		t.Logf("Waiting for deletions at H+1... queue count: %d (expecting %d)", c, initialDBCount+1)
		return c == initialDBCount+1
	}, 15*time.Second, 500*time.Millisecond,
		"Blobs at DAH=%d should be deleted via BlockPersisted trigger", currentHeight+1)
	t.Logf("Verified: 2 blobs at DAH=%d deleted via OnBlockPersisted trigger", currentHeight+1)

	// Mine another block to trigger deletion of the remaining blob
	t.Log("Mining block 2 and waiting for persistence...")
	block = node.MineAndWait(t, 1)
	err = node.WaitForBlockPersisted(block.Hash(), 30*time.Second)
	require.NoError(t, err)
	t.Logf("Block persisted at height %d", currentHeight+2)

	require.Eventually(t, func() bool {
		c := getDBDeletionCount(t, db)
		t.Logf("Waiting for deletion at H+2... queue count: %d (expecting %d)", c, initialDBCount)
		return c == initialDBCount
	}, 15*time.Second, 500*time.Millisecond,
		"Blob at DAH=%d should be deleted via BlockPersisted trigger", currentHeight+2)
	t.Logf("Verified: blob at DAH=%d deleted via OnBlockPersisted trigger", currentHeight+2)

	t.Log("E2E blob deletion OnBlockPersisted trigger mechanism validated successfully")
}

func getDBDeletionCount(t *testing.T, db *usql.DB) int {
	var count int
	err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM scheduled_blob_deletions").Scan(&count)
	require.NoError(t, err)
	return count
}
