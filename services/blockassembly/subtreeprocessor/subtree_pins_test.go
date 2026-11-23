package subtreeprocessor

import (
	"testing"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func mmapSubtree(t *testing.T) *subtreepkg.Subtree {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCountMmap(4, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	return st
}

// Data replaced while it was being pinned may already have been closed, so its pins are
// dropped and the data that replaced it is pinned instead.
func TestPinCurrent_RetriesWhenTheDataIsReplacedWhilePinning(t *testing.T) {
	old := &PrecomputedMiningData{Subtrees: []*subtreepkg.Subtree{mmapSubtree(t)}}
	current := &PrecomputedMiningData{Subtrees: []*subtreepkg.Subtree{mmapSubtree(t), mmapSubtree(t)}}

	// the first load returns old, then the processor replaces it before the re-check
	loads := []*PrecomputedMiningData{old, current, current, current}
	load := func() *PrecomputedMiningData {
		data := loads[0]
		loads = loads[1:]

		return data
	}

	var pins SubtreePins

	hold := pins.NewHold()

	require.Same(t, current, hold.PinCurrent(load))
	require.ElementsMatch(t, current.Subtrees, pins.Pinned())

	hold.Release()
	hold.Release()
	require.Empty(t, pins.Pinned())
}

func TestSubtreePins_CountsEachHoldAndIgnoresHeapSubtrees(t *testing.T) {
	heap, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	mapped := mmapSubtree(t)

	var pins SubtreePins

	a, b := pins.NewHold(), pins.NewHold()
	a.Pin(heap, mapped, nil)
	b.Pin(mapped)

	require.Equal(t, []*subtreepkg.Subtree{mapped}, pins.Pinned())

	a.Release()
	require.Equal(t, []*subtreepkg.Subtree{mapped}, pins.Pinned())

	b.Release()
	require.Empty(t, pins.Pinned())
}
