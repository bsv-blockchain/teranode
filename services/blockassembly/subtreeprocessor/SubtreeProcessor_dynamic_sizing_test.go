package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestSubtreeProcessor_LowVolumeNeverIncreases tests that with low transaction volume,
// the subtree size never increases even with high utilization
func TestSubtreeProcessor_LowVolumeNeverIncreases(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 8
	settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	// Handle channel reads to prevent blocking
	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)
	stp.Start(ctx)

	// Test: High utilization but low volume (< 50 nodes per subtree)
	t.Run("high utilization low volume keeps size", func(t *testing.T) {
		stp.currentItemsPerFile.Store(8)

		// Simulate 87.5% utilization (7 nodes in size-8 subtree)
		// Populate the ring buffer
		r := stp.subtreeNodeCounts
		for i := 0; i < 5; i++ {
			r.Value = 7
			r = r.Next()
		}

		// Even with fast subtree creation (200ms intervals)
		stp.blockIntervals = []time.Duration{
			200 * time.Millisecond,
			200 * time.Millisecond,
			200 * time.Millisecond,
		}

		initialSize := stp.currentItemsPerFile.Load()
		stp.adjustSubtreeSize()

		// Size should NOT increase despite high utilization and fast creation
		// because volume is low (7 nodes < 50 threshold)
		require.Equal(t, initialSize, stp.currentItemsPerFile.Load(),
			"Size should not increase with low transaction volume")
	})

	t.Run("very low utilization decreases size", func(t *testing.T) {
		stp.currentItemsPerFile.Store(32)

		// Simulate 6.25% utilization (2 nodes in size-32 subtree)
		r := stp.subtreeNodeCounts
		for i := 0; i < 5; i++ {
			r.Value = 2
			r = r.Next()
		}
		stp.blockIntervals = []time.Duration{1 * time.Second}

		stp.adjustSubtreeSize()

		// Size should decrease when utilization is < 10%
		require.Less(t, stp.currentItemsPerFile.Load(), int32(32),
			"Size should decrease with very low utilization")
		require.GreaterOrEqual(t, int(stp.currentItemsPerFile.Load()),
			settings.BlockAssembly.MinimumMerkleItemsPerSubtree,
			"Size should not go below minimum")
	})

	t.Run("moderate utilization maintains size", func(t *testing.T) {
		stp.currentItemsPerFile.Store(16)

		// Simulate 50% utilization (8 nodes in size-16 subtree)
		r := stp.subtreeNodeCounts
		for i := 0; i < 5; i++ {
			r.Value = 8
			r = r.Next()
		}
		stp.blockIntervals = []time.Duration{1 * time.Second}

		initialSize := stp.currentItemsPerFile.Load()
		stp.adjustSubtreeSize()

		// Size should stay the same with moderate utilization
		require.Equal(t, initialSize, stp.currentItemsPerFile.Load(),
			"Size should remain stable with moderate utilization")
	})
}

// TestSubtreeProcessor_UsageBasedCapping tests that size increases are capped
// based on actual observed usage
func TestSubtreeProcessor_UsageBasedCapping(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 32
	settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)
	stp.Start(ctx)

	t.Run("caps increase based on max observed nodes", func(t *testing.T) {
		stp.currentItemsPerFile.Store(32)

		// High utilization with moderate volume
		// Max 27 nodes seen, average 25
		nodes := []int{23, 25, 27, 24, 26, 25, 24, 26, 25, 25}
		r := stp.subtreeNodeCounts
		for _, n := range nodes {
			r.Value = n
			r = r.Next()
		}

		// Very fast subtree creation that would normally trigger 4x increase
		stp.blockIntervals = []time.Duration{
			100 * time.Millisecond,
			100 * time.Millisecond,
			100 * time.Millisecond,
		}

		stp.adjustSubtreeSize()

		// Size should be capped at 2x max observed (27*2=54 -> round to 64)
		// Not allowed to go higher even though timing suggests it
		require.LessOrEqual(t, stp.currentItemsPerFile.Load(), int32(64),
			"Size should be capped based on actual usage")
	})

	t.Run("allows increase when volume justifies it", func(t *testing.T) {
		stp.currentItemsPerFile.Store(64)

		// High utilization with high volume (> 50 nodes)
		nodes := []int{60, 62, 58, 61, 59, 60, 61, 60, 60, 60}
		r := stp.subtreeNodeCounts
		for _, n := range nodes {
			r.Value = n
			r = r.Next()
		}

		// Fast creation that justifies increase
		stp.blockIntervals = []time.Duration{
			200 * time.Millisecond,
			200 * time.Millisecond,
			200 * time.Millisecond,
		}

		initialSize := stp.currentItemsPerFile.Load()
		stp.adjustSubtreeSize()

		// Size should increase when volume is high enough
		require.Greater(t, stp.currentItemsPerFile.Load(), initialSize,
			"Size should increase with high volume and fast creation")
	})
}

// TestSubtreeProcessor_RealWorldScenario tests a realistic scenario with varying load
func TestSubtreeProcessor_RealWorldScenario(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 1024
	settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)
	stp.Start(ctx)

	t.Run("adapts to changing load patterns", func(t *testing.T) {
		// Start with high load
		stp.currentItemsPerFile.Store(1024)
		nodes := []int{900, 950, 920, 940, 910}
		r := stp.subtreeNodeCounts
		for _, n := range nodes {
			r.Value = n
			r = r.Next()
		}
		stp.blockIntervals = []time.Duration{500 * time.Millisecond}

		stp.adjustSubtreeSize()
		highLoadSize := stp.currentItemsPerFile.Load()
		require.GreaterOrEqual(t, highLoadSize, int32(1024),
			"Should maintain or increase size under high load")

		// Transition to low load (2-4 tx/s scenario)
		stp.currentItemsPerFile.Store(highLoadSize)
		nodes = []int{3, 4, 2, 3, 4, 3, 2, 4, 3, 3}
		r = stp.subtreeNodeCounts
		for _, n := range nodes {
			r.Value = n
			r = r.Next()
		}
		stp.blockIntervals = []time.Duration{1 * time.Second}

		// Should decrease over multiple adjustments
		for i := 0; i < 5; i++ {
			prevSize := stp.currentItemsPerFile.Load()
			stp.adjustSubtreeSize()

			// Each adjustment should decrease or maintain size, never increase
			require.LessOrEqual(t, stp.currentItemsPerFile.Load(), prevSize,
				"Size should decrease or stay same under low load")

			// Reset counters as the real code does
			if stp.currentItemsPerFile.Load() < prevSize {
				nodes = []int{3, 4, 2, 3, 4, 3, 2, 4, 3, 3}
				r = stp.subtreeNodeCounts
				for _, n := range nodes {
					r.Value = n
					r = r.Next()
				}
			}
		}

		// Should eventually reach a small size
		require.LessOrEqual(t, stp.currentItemsPerFile.Load(), int32(64),
			"Size should decrease significantly under sustained low load")
	})
}

// TestSubtreeProcessor_CompleteSubtreeTracking tests that node counts are properly
// tracked when subtrees complete
func TestSubtreeProcessor_CompleteSubtreeTracking(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 8

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)
	stp.Start(ctx)

	t.Run("tracks node counts correctly", func(t *testing.T) {
		// Create a subtree with known node count
		newSubtree, err := subtreepkg.NewTreeByLeafCount(8)
		require.NoError(t, err)
		stp.currentSubtree.Store(newSubtree)

		// Add some nodes (including coinbase)
		err = stp.currentSubtree.Load().AddCoinbaseNode()
		require.NoError(t, err)

		// Add 4 more transaction nodes
		for i := 0; i < 4; i++ {
			hash := chainhash.Hash{}
			copy(hash[:], []byte{byte(i)})
			node := subtreepkg.Node{
				Hash:        hash,
				Fee:         100,
				SizeInBytes: 250,
			}
			err = stp.currentSubtree.Load().AddSubtreeNode(node)
			require.NoError(t, err)
		}

		// Process the complete subtree
		err = stp.processCompleteSubtree(false)
		require.NoError(t, err)

		// Should have tracked 5 nodes (including coinbase)
		// Check the first value in the ring
		count := 0
		actualNodes := 0
		stp.subtreeNodeCounts.Do(func(v interface{}) {
			if v != nil {
				count++
				if count == 1 {
					actualNodes = v.(int)
				}
			}
		})
		require.Equal(t, 1, count, "Should have tracked one subtree")
		require.Equal(t, 5, actualNodes,
			"Should track total number of nodes including coinbase")

		// Test that old samples are removed after limit
		// First, fill up remaining slots (we already have 1, buffer size is 18)
		r := stp.subtreeNodeCounts.Next() // Skip the first one we already added
		for i := 0; i < 16; i++ {         // 17 more to reach 18 total
			r.Value = 5
			r = r.Next()
		}

		// Create another subtree that should trigger the limit
		newSubtree2, err := subtreepkg.NewTreeByLeafCount(8)
		require.NoError(t, err)
		stp.currentSubtree.Store(newSubtree2)
		err = stp.currentSubtree.Load().AddCoinbaseNode()
		require.NoError(t, err)

		for i := 0; i < 3; i++ {
			hash := chainhash.Hash{}
			copy(hash[:], []byte{byte(i + 10)})
			node := subtreepkg.Node{
				Hash:        hash,
				Fee:         100,
				SizeInBytes: 250,
			}
			err = stp.currentSubtree.Load().AddSubtreeNode(node)
			require.NoError(t, err)
		}

		// This should add one more, reaching 18 (full buffer)
		err = stp.processCompleteSubtree(false)
		require.NoError(t, err)

		// Count values in ring
		count = 0
		stp.subtreeNodeCounts.Do(func(v interface{}) {
			if v != nil {
				count++
			}
		})
		require.Equal(t, 18, count, "Should have 18 samples (full buffer)")

		// Add one more to test that it maintains the limit
		newSubtree3, err := subtreepkg.NewTreeByLeafCount(8)
		require.NoError(t, err)
		stp.currentSubtree.Store(newSubtree3)
		err = stp.currentSubtree.Load().AddCoinbaseNode()
		require.NoError(t, err)

		hash := chainhash.Hash{}
		copy(hash[:], []byte{byte(20)})
		node := subtreepkg.Node{
			Hash:        hash,
			Fee:         100,
			SizeInBytes: 250,
		}
		err = stp.currentSubtree.Load().AddSubtreeNode(node)
		require.NoError(t, err)

		err = stp.processCompleteSubtree(false)
		require.NoError(t, err)

		// Should still be at max 18 samples (ring automatically overwrites oldest)
		count = 0
		stp.subtreeNodeCounts.Do(func(v interface{}) {
			if v != nil {
				count++
			}
		})
		require.Equal(t, 18, count,
			"Should still have 18 samples (ring overwrites oldest)")
	})
}

// TestSubtreeProcessor_MinimumSizeRespected tests that minimum size is always respected
func TestSubtreeProcessor_MinimumSizeRespected(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 8
	settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)
	stp.Start(ctx)

	t.Run("never goes below minimum", func(t *testing.T) {
		stp.currentItemsPerFile.Store(4) // At minimum

		// Extremely low utilization that would normally trigger decrease
		r := stp.subtreeNodeCounts
		for i := 0; i < 5; i++ {
			r.Value = 1
			r = r.Next()
		}
		stp.blockIntervals = []time.Duration{5 * time.Second}

		stp.adjustSubtreeSize()

		// Should stay at minimum
		require.Equal(t, int32(settings.BlockAssembly.MinimumMerkleItemsPerSubtree),
			stp.currentItemsPerFile.Load(),
			"Size should not go below minimum")
	})
}

// TestSubtreeProcessor_HighVolumeScaling tests that the dynamic sizing correctly
// scales up when there's genuinely high transaction volume
func TestSubtreeProcessor_HighVolumeScaling(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 64
	settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	// Handle channel reads to prevent blocking
	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)
	stp.Start(ctx)

	t.Run("scales up with sustained high volume", func(t *testing.T) {
		// Start with a moderate size
		stp.currentItemsPerFile.Store(64)

		// Simulate high transaction volume (1000+ tx/s)
		// With size 64, we'd be creating subtrees very frequently
		// Let's say we're seeing 90% full subtrees (57-58 nodes each)
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 57 + (i % 3) // 57, 58, 59, 57, 58, 59...
			r = r.Next()
		}

		// Subtrees are being created every 50ms (20 per second)
		// This represents ~1140 transactions per second (57 * 20)
		stp.blockIntervals = []time.Duration{
			50 * time.Millisecond,
			50 * time.Millisecond,
			50 * time.Millisecond,
		}

		initialSize := stp.currentItemsPerFile.Load()
		stp.adjustSubtreeSize()

		// Size should increase significantly due to high volume and fast creation
		require.Greater(t, stp.currentItemsPerFile.Load(), initialSize,
			"Size should increase with high transaction volume")

		// With ratio = 1000ms/50ms = 20, and starting at 64,
		// new size would be 64*20 = 1280, rounded to 2048
		// But capped at 2x per adjustment = 128
		require.Equal(t, int32(128), stp.currentItemsPerFile.Load(),
			"Size should double with very high load")
	})

	t.Run("continues scaling with extreme volume", func(t *testing.T) {
		// Now at 128, still seeing high volume
		stp.currentItemsPerFile.Store(128)

		// Even higher utilization now (120+ nodes per subtree)
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 120 + (i % 5) // 120-124 nodes
			r = r.Next()
		}

		// Still creating subtrees very fast (25ms intervals)
		// This represents ~4800 tx/s (120 * 40)
		stp.blockIntervals = []time.Duration{
			25 * time.Millisecond,
			25 * time.Millisecond,
			25 * time.Millisecond,
		}

		stp.adjustSubtreeSize()

		// Should continue scaling up
		require.Equal(t, int32(256), stp.currentItemsPerFile.Load(),
			"Should continue doubling with extreme load")
	})

	t.Run("scales to maximum with massive volume", func(t *testing.T) {
		// Set near maximum to test ceiling
		stp.currentItemsPerFile.Store(16384)

		// Extremely high volume - nearly full subtrees
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 16000 + (i * 30) // 16000-16270 nodes
			r = r.Next()
		}

		// Creating subtrees every 10ms (100 per second)
		// This represents 1.6M tx/s!
		stp.blockIntervals = []time.Duration{
			10 * time.Millisecond,
			10 * time.Millisecond,
			10 * time.Millisecond,
		}

		stp.adjustSubtreeSize()

		// Should hit the maximum
		require.Equal(t, int32(settings.BlockAssembly.MaximumMerkleItemsPerSubtree),
			stp.currentItemsPerFile.Load(),
			"Should reach maximum size with massive volume")
	})

	t.Run("scales down when volume decreases", func(t *testing.T) {
		// Start at a high size from previous high volume
		stp.currentItemsPerFile.Store(8192)

		// Volume has dropped significantly (only 100-200 tx per subtree)
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 100 + (i * 10) // 100-190 nodes
			r = r.Next()
		}

		// Subtrees now created every 2 seconds
		stp.blockIntervals = []time.Duration{
			2 * time.Second,
			2 * time.Second,
			2 * time.Second,
		}

		stp.adjustSubtreeSize()

		// With ratio = 1s/2s = 0.5, size should halve
		// But utilization is also low (100-190 out of 8192 = ~2%)
		// So it should decrease based on low utilization
		require.Less(t, stp.currentItemsPerFile.Load(), int32(8192),
			"Size should decrease when volume drops")
		require.LessOrEqual(t, stp.currentItemsPerFile.Load(), int32(4096),
			"Should decrease significantly with low utilization")
	})

	t.Run("handles burst traffic correctly", func(t *testing.T) {
		// Start at a reasonable size
		stp.currentItemsPerFile.Store(256)

		// Sudden burst - subtrees are completely full
		r := stp.subtreeNodeCounts
		for i := 0; i < 5; i++ {
			r.Value = 255 // Nearly full
			r = r.Next()
		}

		// Creating subtrees very rapidly during burst
		stp.blockIntervals = []time.Duration{
			20 * time.Millisecond,
			20 * time.Millisecond,
			20 * time.Millisecond,
		}

		stp.adjustSubtreeSize()

		// Should increase to handle burst
		require.Greater(t, stp.currentItemsPerFile.Load(), int32(256),
			"Should increase size to handle burst traffic")

		// Now simulate burst ending
		stp.currentItemsPerFile.Store(512) // After increase

		// Traffic back to much lower (20-30 nodes per subtree)
		r = stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 20 + (i % 10)
			r = r.Next()
		}

		// Normal intervals again
		stp.blockIntervals = []time.Duration{
			1 * time.Second,
			1 * time.Second,
			1 * time.Second,
		}

		stp.adjustSubtreeSize()

		// Should decrease back down (utilization ~5%)
		require.Less(t, stp.currentItemsPerFile.Load(), int32(512),
			"Should decrease after burst ends with low utilization")
	})

	t.Run("realistic high volume scenario", func(t *testing.T) {
		// Simulate realistic high-volume scenario
		// Target: 100,000 tx/s (current BSV record levels)
		// If we want subtrees every second with 100k tx/s,
		// we need subtree size of ~100,000

		// Start small and let it scale
		stp.currentItemsPerFile.Store(1024)

		// First adjustment - seeing 950+ nodes per subtree
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 950 + (i * 5) // 950-995 nodes
			r = r.Next()
		}

		// Creating subtrees every 100ms (10 per second)
		// This is ~9500 tx/s
		stp.blockIntervals = []time.Duration{
			100 * time.Millisecond,
			100 * time.Millisecond,
			100 * time.Millisecond,
		}

		// Should scale up over multiple adjustments
		sizes := []int32{}
		for i := 0; i < 5; i++ {
			prevSize := stp.currentItemsPerFile.Load()
			stp.adjustSubtreeSize()
			sizes = append(sizes, stp.currentItemsPerFile.Load())

			// Simulate continued high load
			if stp.currentItemsPerFile.Load() > prevSize {
				// Update node counts to match new size
				newNodeCount := int(float64(stp.currentItemsPerFile.Load()) * 0.93) // 93% full
				r = stp.subtreeNodeCounts
				for j := 0; j < 10; j++ {
					r.Value = newNodeCount + (j % 10)
					r = r.Next()
				}
			}
		}

		// Should have scaled up significantly
		require.Greater(t, stp.currentItemsPerFile.Load(), int32(1024),
			"Should scale up from initial size")

		// Log the scaling progression
		t.Logf("Size progression with high volume: 1024 -> %v", sizes)

		// Final size should be appropriate for the load
		// With 100ms intervals and needing to handle ~950 tx per subtree,
		// optimal size would be around 2048-4096
		require.GreaterOrEqual(t, stp.currentItemsPerFile.Load(), int32(2048),
			"Should reach appropriate size for sustained high volume")
		require.LessOrEqual(t, stp.currentItemsPerFile.Load(), int32(8192),
			"Should not overshoot reasonable size")
	})
}

// TestSubtreeProcessor_VolumeThresholds tests the 50-node threshold for volume detection
func TestSubtreeProcessor_VolumeThresholds(t *testing.T) {
	// Setup
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.UseDynamicSubtreeSize = true
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 64
	settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	settings.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)
	stp.Start(ctx)

	t.Run("just below threshold blocks increase", func(t *testing.T) {
		stp.currentItemsPerFile.Store(64)

		// 49 nodes per subtree - just below threshold
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 49
			r = r.Next()
		}

		// Fast creation that would normally trigger increase
		stp.blockIntervals = []time.Duration{
			100 * time.Millisecond,
			100 * time.Millisecond,
			100 * time.Millisecond,
		}

		initialSize := stp.currentItemsPerFile.Load()
		stp.adjustSubtreeSize()

		// Should NOT increase due to low volume check
		require.Equal(t, initialSize, stp.currentItemsPerFile.Load(),
			"Should not increase with 49 nodes (below 50 threshold)")
	})

	t.Run("just above threshold allows increase", func(t *testing.T) {
		stp.currentItemsPerFile.Store(64)

		// 60 nodes per subtree - well above threshold and high utilization (93.75%)
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 60
			r = r.Next()
		}

		// Same fast creation
		stp.blockIntervals = []time.Duration{
			100 * time.Millisecond,
			100 * time.Millisecond,
			100 * time.Millisecond,
		}

		initialSize := stp.currentItemsPerFile.Load()
		stp.adjustSubtreeSize()

		// Should increase now that we're above threshold with high utilization
		require.Greater(t, stp.currentItemsPerFile.Load(), initialSize,
			"Should increase with 60 nodes (above 50 threshold with high utilization)")
	})

	t.Run("exactly at threshold with high utilization allows increase", func(t *testing.T) {
		stp.currentItemsPerFile.Store(60)

		// Exactly 50 nodes per subtree - 83% utilization triggers high path
		r := stp.subtreeNodeCounts
		for i := 0; i < 10; i++ {
			r.Value = 50
			r = r.Next()
		}

		// Fast creation
		stp.blockIntervals = []time.Duration{
			100 * time.Millisecond,
			100 * time.Millisecond,
			100 * time.Millisecond,
		}

		initialSize := stp.currentItemsPerFile.Load()
		stp.adjustSubtreeSize()

		// Should increase at exactly 50 with high utilization
		require.Greater(t, stp.currentItemsPerFile.Load(), initialSize,
			"Should increase with exactly 50 nodes when utilization > 80%")
	})
}
