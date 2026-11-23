package utxo

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// A record counts under own always, and under parents only when it spends a parent.
func TestCountLaneRecord(t *testing.T) {
	const store = "test-coverage"

	count := func(kind, carried string) float64 {
		return testutil.ToFloat64(prometheusLaneRecordPositions.WithLabelValues(store, kind, carried))
	}

	ownYes, ownNo := count(LaneCoverageOwn, "yes"), count(LaneCoverageOwn, "no")
	parentsYes, parentsNo := count(LaneCoverageParents, "yes"), count(LaneCoverageParents, "no")

	CountLaneRecord(store, true, false, true)
	require.Equal(t, ownYes+1, count(LaneCoverageOwn, "yes"))
	require.Equal(t, parentsYes, count(LaneCoverageParents, "yes"), "a record with no parents is left out of the parents kind")
	require.Equal(t, parentsNo, count(LaneCoverageParents, "no"))

	CountLaneRecord(store, false, true, false)
	require.Equal(t, ownNo+1, count(LaneCoverageOwn, "no"))
	require.Equal(t, parentsNo+1, count(LaneCoverageParents, "no"))

	CountLaneRecord(store, true, true, true)
	require.Equal(t, ownYes+2, count(LaneCoverageOwn, "yes"))
	require.Equal(t, parentsYes+1, count(LaneCoverageParents, "yes"))

	// Each kind takes its own carried label.
	CountLaneRecord(store, true, true, false)
	require.Equal(t, ownYes+3, count(LaneCoverageOwn, "yes"), "own carried, parents not")
	require.Equal(t, parentsNo+2, count(LaneCoverageParents, "no"))

	CountLaneRecord(store, false, true, true)
	require.Equal(t, ownNo+2, count(LaneCoverageOwn, "no"), "parents carried, own not")
	require.Equal(t, parentsYes+2, count(LaneCoverageParents, "yes"))
}
