package profiling

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseRegionStat verifies the smaps statistic parser extracted from
// parseSmaps: each recognised line sets the matching region field (in bytes),
// Size/Rss/Pss also accumulate into the breakdown totals, and unrecognised
// lines are a no-op.
func TestParseRegionStat(t *testing.T) {
	region := &MemoryRegion{}
	breakdown := &MemoryBreakdown{}

	lines := []string{
		"Size:                  8 kB",
		"Rss:                   4 kB",
		"Pss:                   2 kB",
		"Shared_Clean:          1 kB",
		"Shared_Dirty:          3 kB",
		"Private_Clean:         5 kB",
		"Private_Dirty:         6 kB",
		"Referenced:            7 kB",
		"Anonymous:             9 kB",
		"Swap:                 10 kB",
		"VmFlags: rd wr mr mw me ac",  // unrecognised -> no-op
		"KernelPageSize:        4 kB", // unrecognised (not a tracked prefix)
	}
	for _, l := range lines {
		parseRegionStat(l, region, breakdown)
	}

	const kB = int64(1024)
	// Per-region fields (value * 1024).
	require.Equal(t, 8*kB, region.Size)
	require.Equal(t, 4*kB, region.Rss)
	require.Equal(t, 2*kB, region.Pss)
	require.Equal(t, 1*kB, region.SharedClean)
	require.Equal(t, 3*kB, region.SharedDirty)
	require.Equal(t, 5*kB, region.PrivateClean)
	require.Equal(t, 6*kB, region.PrivateDirty)
	require.Equal(t, 7*kB, region.Referenced)
	require.Equal(t, 9*kB, region.Anonymous)
	require.Equal(t, 10*kB, region.Swap)

	// Aggregate totals (only Size/Rss/Pss accumulate).
	require.Equal(t, 8*kB, breakdown.TotalVirtual)
	require.Equal(t, 4*kB, breakdown.TotalRss)
	require.Equal(t, 2*kB, breakdown.TotalPss)

	// Unrecognised prefixes (KernelPageSize also mentions "kB") must not leak
	// into any tracked field — nothing above should have been overwritten.
	require.Equal(t, 8*kB, region.Size, "unrecognised KernelPageSize line must not affect Size")
}

// TestParseRegionStat_Accumulates confirms Size/Rss/Pss sum across regions,
// matching the running-total behaviour parseSmaps relies on.
func TestParseRegionStat_Accumulates(t *testing.T) {
	breakdown := &MemoryBreakdown{}

	r1 := &MemoryRegion{}
	parseRegionStat("Size: 8 kB", r1, breakdown)
	parseRegionStat("Rss: 4 kB", r1, breakdown)

	r2 := &MemoryRegion{}
	parseRegionStat("Size: 2 kB", r2, breakdown)
	parseRegionStat("Rss: 1 kB", r2, breakdown)

	const kB = int64(1024)
	require.Equal(t, 10*kB, breakdown.TotalVirtual)
	require.Equal(t, 5*kB, breakdown.TotalRss)
	require.Equal(t, int64(0), breakdown.TotalPss)
	// Each region keeps only its own value.
	require.Equal(t, 8*kB, r1.Size)
	require.Equal(t, 2*kB, r2.Size)
}
