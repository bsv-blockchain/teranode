package legacy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The connection manager counting more outbound slots than there are outbound peers is a leak. On
// 2026-09-24 it counted 8 against 4 peers for seven hours and nothing said so. A gap seen in two
// consecutive checks, a minute apart, is reported; one alone may be a peer connecting or leaving.
func TestASlotGapIsReportedWhenItPersists(t *testing.T) {
	var w slotGapWatch

	require.Zero(t, w.observe(8, 8), "no gap")
	require.Zero(t, w.observe(8, 4), "a gap seen once may be a peer in transit")
	require.Equal(t, 4, w.observe(8, 4), "seen twice running, it is reported")
	require.Zero(t, w.observe(8, 8), "and clears when it closes")
	require.Zero(t, w.observe(8, 5), "a new gap starts over")
}
