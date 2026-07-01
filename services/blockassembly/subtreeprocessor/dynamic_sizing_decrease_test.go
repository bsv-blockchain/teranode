package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newDynamicSizingProcessor builds a started SubtreeProcessor with dynamic
// subtree sizing enabled and the given size bounds. It mirrors the setup used by
// the other dynamic-sizing tests in this package.
func newDynamicSizingProcessor(t *testing.T, initial, minSize, maxSize int) *SubtreeProcessor {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.UseDynamicSubtreeSize = true
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = initial
	tSettings.BlockAssembly.MinimumMerkleItemsPerSubtree = minSize
	tSettings.BlockAssembly.MaximumMerkleItemsPerSubtree = maxSize

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
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
	// Registered before Stop's cleanup so it runs after Stop (LIFO).
	t.Cleanup(func() { close(done) })

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, blob_memory.New(), &blockchain.Mock{}, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	return stp
}

// setRingNodeCounts populates the subtree-utilization ring buffer with a fixed
// per-subtree node count, driving the average utilization used by
// adjustSubtreeSize. The ring is re-read from the processor because a size
// adjustment replaces it with a fresh ring.
func setRingNodeCounts(stp *SubtreeProcessor, perSubtree, samples int) {
	r := stp.subtreeNodeCounts
	for range samples {
		r.Value = perSubtree
		r = r.Next()
	}
}

func requirePowerOfTwo(t *testing.T, n int32) {
	t.Helper()
	require.True(t, n > 0 && n&(n-1) == 0, "expected a power of two, got %d", n)
}

// TestSubtreeProcessor_DecreaseHalvesAndRoundsToPowerOfTwo pins BA-SUBTREE-022:
// the decrease path multiplies the current size by 0.5 and rounds up to the
// nearest power of two, and a single evaluation never reduces the size by more
// than a 0.5x factor.
func TestSubtreeProcessor_DecreaseHalvesAndRoundsToPowerOfTwo(t *testing.T) {
	// Power-of-two sizes (BA-SUBTREE-025 guarantees these in practice) halve
	// exactly, since 0.5x of a power of two is already a power of two.
	t.Run("halves a power-of-two size exactly", func(t *testing.T) {
		stp := newDynamicSizingProcessor(t, 1024, 4, 32768)

		for _, size := range []int32{1024, 512, 256, 64} {
			stp.currentItemsPerFile.Store(size)
			// One tx per subtree => utilization 1/size, far below the 10% floor.
			setRingNodeCounts(stp, 1, 5)
			stp.blockIntervals = []time.Duration{time.Second}

			stp.adjustSubtreeSize()

			got := stp.currentItemsPerFile.Load()
			require.Equal(t, size/2, got, "size %d must decrease to exactly half (%d)", size, size/2)
			requirePowerOfTwo(t, got)
		}
	})

	// An off-nominal non-power-of-two size exercises the round-up: 0.5*100 = 50
	// rounds up to 64. This also demonstrates the "never more than 0.5x" cap —
	// the result (64) is above half of 100 (50), i.e. a smaller reduction, never
	// a larger one.
	t.Run("rounds up to nearest power of two without exceeding a 0.5x decrease", func(t *testing.T) {
		stp := newDynamicSizingProcessor(t, 1024, 4, 32768)

		stp.currentItemsPerFile.Store(100)
		setRingNodeCounts(stp, 1, 5)
		stp.blockIntervals = []time.Duration{time.Second}

		stp.adjustSubtreeSize()

		got := stp.currentItemsPerFile.Load()
		require.Equal(t, int32(64), got, "0.5*100=50 must round up to the nearest power of two (64)")
		requirePowerOfTwo(t, got)
		require.GreaterOrEqual(t, got, int32(100/2),
			"BA-SUBTREE-022: a single evaluation must not reduce size below a 0.5x factor")
	})
}

// TestSubtreeProcessor_ConvergesToMinimumUnderLowTraffic pins BA-SUBTREE-026:
// under prolonged low traffic the subtree size converges to
// MinimumMerkleItemsPerSubtree and never drops below it. (The complementary
// "fires on the periodic-announcement timer at the floor" half of BA-SUBTREE-026
// is covered by TestPeriodicAnnouncementOfIncompleteSubtree / BA-SUBTREE-008.)
func TestSubtreeProcessor_ConvergesToMinimumUnderLowTraffic(t *testing.T) {
	const (
		minSize  = 64
		maxSteps = 20
	)

	stp := newDynamicSizingProcessor(t, 1024, minSize, 32768)
	stp.currentItemsPerFile.Store(1024)

	var size int32

	for range maxSteps {
		// Sustained low traffic: ~one tx per subtree every evaluation.
		setRingNodeCounts(stp, 1, 5)
		stp.blockIntervals = []time.Duration{time.Second}

		prev := stp.currentItemsPerFile.Load()
		stp.adjustSubtreeSize()
		size = stp.currentItemsPerFile.Load()

		require.GreaterOrEqual(t, int(size), minSize, "size must never drop below the minimum during convergence")

		if size == prev {
			break // converged: a low-traffic evaluation no longer changes the size
		}
	}

	require.Equal(t, int32(minSize), size,
		"BA-SUBTREE-026: prolonged low traffic must converge the subtree size to the minimum")

	// At the floor the size is stable: a further low-traffic evaluation (which
	// computes a sub-minimum target and clamps it) leaves the size at the minimum.
	setRingNodeCounts(stp, 1, 5)
	stp.adjustSubtreeSize()
	require.Equal(t, int32(minSize), stp.currentItemsPerFile.Load(),
		"size must stay clamped at the minimum, never below")
}
