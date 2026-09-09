package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// validateChunkPlan asserts the universal invariants of planCreateChunks:
//   - windows are ordered, contiguous within themselves, and non-empty;
//   - windows ∪ oversized cover [0,n) exactly once each (no gap, no overlap);
//   - every window's estimated wire size (chunkOverhead + Σ rowBytes) stays at
//     or below threshold, UNLESS the window is a single row (a lone
//     over-threshold-but-sendable row legitimately forms its own window);
//   - no window's estimate ever reaches hardLimit;
//   - every oversized index alone would reach hardLimit.
func validateChunkPlan(t *testing.T, rowBytes []int64, threshold, chunkOverhead, hardLimit int64, windows [][2]int, oversized []int) {
	t.Helper()

	seen := make([]bool, len(rowBytes))
	prevHi := 0

	for wi, w := range windows {
		lo, hi := w[0], w[1]
		require.Less(t, lo, hi, "window %d must be non-empty", wi)
		require.GreaterOrEqual(t, lo, prevHi, "window %d must not overlap/precede the previous", wi)
		prevHi = hi

		var sum int64 = chunkOverhead
		for k := lo; k < hi; k++ {
			require.False(t, seen[k], "index %d covered twice", k)
			seen[k] = true
			sum += rowBytes[k]
		}

		require.Less(t, sum, hardLimit, "window %d estimate %d must stay below hardLimit", wi, sum)
		if hi-lo > 1 {
			require.LessOrEqual(t, sum, threshold, "multi-row window %d estimate %d must stay within threshold", wi, sum)
		}
	}

	for _, k := range oversized {
		require.False(t, seen[k], "oversized index %d also appeared in a window", k)
		seen[k] = true
		require.GreaterOrEqual(t, chunkOverhead+rowBytes[k], hardLimit, "oversized index %d should reach hardLimit", k)
	}

	for k, ok := range seen {
		require.True(t, ok, "index %d covered by neither a window nor oversized", k)
	}
}

func TestPlanCreateChunks_SmallBatchSingleWindow(t *testing.T) {
	const (
		threshold     = int64(512) << 20
		chunkOverhead = int64(1024)
		hardLimit     = int64(0x3fffffff - 1)
	)

	// Five ordinary ~250-byte txs: comfortably one INSERT, exactly as before.
	rowBytes := []int64{250, 300, 280, 260, 290}

	windows, oversized := planCreateChunks(rowBytes, threshold, chunkOverhead, hardLimit)

	require.Empty(t, oversized)
	require.Equal(t, [][2]int{{0, 5}}, windows, "a small batch must be a single window")
	validateChunkPlan(t, rowBytes, threshold, chunkOverhead, hardLimit, windows, oversized)
}

func TestPlanCreateChunks_SplitsOversizedBlock(t *testing.T) {
	// Model testnet block 1512872: ~80 txs each ~17.1 MB, ~1.386 GiB total.
	const (
		threshold     = int64(512) << 20 // 536870912
		chunkOverhead = int64(1024)
		hardLimit     = int64(0x3fffffff - 1)
		nTx           = 80
		txBytes       = int64(17108401)
	)

	rowBytes := make([]int64, nTx)
	for i := range rowBytes {
		rowBytes[i] = txBytes
	}

	windows, oversized := planCreateChunks(rowBytes, threshold, chunkOverhead, hardLimit)

	require.Empty(t, oversized, "no single 17 MB tx exceeds the hard limit")
	require.Greater(t, len(windows), 1, "a 1.386 GiB block must split into multiple INSERTs")
	// floor(512 MiB / 17.1 MB) = 31 rows/window -> ceil(80/31) = 3 windows.
	require.Equal(t, 3, len(windows), "expected three ~462 MB chunks")
	validateChunkPlan(t, rowBytes, threshold, chunkOverhead, hardLimit, windows, oversized)

	// Every row is placed (none dropped): windows cover all 80 indices.
	covered := 0
	for _, w := range windows {
		covered += w[1] - w[0]
	}
	require.Equal(t, nTx, covered)
}

func TestPlanCreateChunks_SoloRowOverThresholdUnderHardLimit(t *testing.T) {
	const (
		threshold     = int64(512) << 20
		chunkOverhead = int64(1024)
		hardLimit     = int64(0x3fffffff - 1)
	)

	// A single ~700 MiB tx: over the 512 MiB threshold but under the ~1 GiB hard
	// limit. It must become its own window (one INSERT), not be dropped.
	big := int64(700) << 20
	rowBytes := []int64{500, big, 500}

	windows, oversized := planCreateChunks(rowBytes, threshold, chunkOverhead, hardLimit)

	require.Empty(t, oversized)
	require.Equal(t, [][2]int{{0, 1}, {1, 2}, {2, 3}}, windows, "the big row gets a solo window, neighbours their own")
	validateChunkPlan(t, rowBytes, threshold, chunkOverhead, hardLimit, windows, oversized)
}

func TestPlanCreateChunks_OversizedRowReported(t *testing.T) {
	const (
		threshold     = int64(512) << 20
		chunkOverhead = int64(1024)
		hardLimit     = int64(0x3fffffff - 1)
	)

	// Row 1 alone reaches the hard wire limit: it can never be sent and must be
	// reported (not placed in any window), while rows 0 and 2 still insert.
	rowBytes := []int64{500, hardLimit, 500}

	windows, oversized := planCreateChunks(rowBytes, threshold, chunkOverhead, hardLimit)

	require.Equal(t, []int{1}, oversized)
	// Rows 0 and 2 are split into separate windows around the oversized row.
	require.Equal(t, [][2]int{{0, 1}, {2, 3}}, windows)
	validateChunkPlan(t, rowBytes, threshold, chunkOverhead, hardLimit, windows, oversized)
}

func TestPlanCreateChunks_OversizedRowAtBoundaryNoEmptyWindow(t *testing.T) {
	const (
		threshold     = int64(512) << 20
		chunkOverhead = int64(1024)
		hardLimit     = int64(0x3fffffff - 1)
	)

	// Oversized row immediately follows a natural chunk boundary AND sits first
	// and last — exercises the empty-flush guard at both ends.
	rowBytes := []int64{hardLimit, 500, hardLimit}

	windows, oversized := planCreateChunks(rowBytes, threshold, chunkOverhead, hardLimit)

	require.Equal(t, []int{0, 2}, oversized)
	require.Equal(t, [][2]int{{1, 2}}, windows, "only the middle sendable row forms a window; no empty windows")
	validateChunkPlan(t, rowBytes, threshold, chunkOverhead, hardLimit, windows, oversized)
}

func TestPlanCreateChunks_Empty(t *testing.T) {
	windows, oversized := planCreateChunks(nil, int64(512)<<20, 1024, int64(0x3fffffff-1))
	require.Empty(t, windows)
	require.Empty(t, oversized)
}
