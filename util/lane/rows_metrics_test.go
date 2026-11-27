package lane

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestRowMetricsRecordNothingForAnUnknownBlockLabel: the block label is peer or own; any other
// value records nothing, so no third series appears.
func TestRowMetricsRecordNothingForAnUnknownBlockLabel(t *testing.T) {
	CountRowInsertFailure("other")
	CountRowMismatch("other", MismatchFewer)
	CountMissingBitmaps("other", MissingBitmapsAbsent, 1)
	ObserveGate("other", "build", time.Millisecond)
	ObserveBlockRows("other", BlockRows{Lanes: []LaneRowStats{{Lane: 1, Bytes: 10, Holes: []uint64{1}}}})

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, f := range families {
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				require.False(t, l.GetName() == "block" && l.GetValue() == "other", "%s has a block=other series", f.GetName())
			}
		}
	}
}
