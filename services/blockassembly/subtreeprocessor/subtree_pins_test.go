package subtreeprocessor

import (
	"slices"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
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

// A subtree a rebuild sends to storage must stay mapped across a close until the store's
// work ends, on both rebuild paths, and be closed by the first close after that.
func TestRebuiltSubtreesStayMappedUntilTheirStoreEnds(t *testing.T) {
	rebuild := map[string]func(t *testing.T, stp *SubtreeProcessor){
		"storeAndAnnounceChainedSubtrees": func(t *testing.T, stp *SubtreeProcessor) {
			for range 2 {
				st, err := stp.newSubtree(4)
				require.NoError(t, err)

				stp.chainedSubtrees = append(stp.chainedSubtrees, st)
			}

			require.NoError(t, stp.storeAndAnnounceChainedSubtrees(t.Context(), "test", 0))
		},
		"parallelBuildRemainderSubtrees": func(t *testing.T, stp *SubtreeProcessor) {
			current, err := stp.newSubtree(4)
			require.NoError(t, err)
			require.NoError(t, current.AddCoinbaseNode())
			stp.currentSubtree.Store(current)

			// 3 fill the current subtree and 4 the next, so two subtrees are chained and stored
			kept := make([]subtreepkg.Node, 9)
			for i := range kept {
				kept[i] = subtreepkg.Node{Hash: chainhash.Hash{byte(i + 1)}, Fee: 1, SizeInBytes: 250}
			}

			require.NoError(t, stp.parallelBuildRemainderSubtrees(t.Context(), kept, nil, true))
		},
	}

	for name, run := range rebuild {
		t.Run(name, func(t *testing.T) {
			newSubtreeChan := make(chan NewSubtreeRequest, 16)

			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

			stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, nil, nil, nil, newSubtreeChan, WithMmapDir(t.TempDir()))
			require.NoError(t, err)

			stp.chainedSubtrees = nil

			run(t, stp)

			// the requests are queued for a storage worker that has not finished with them
			var reqs []NewSubtreeRequest

			sent := make([]*subtreepkg.Subtree, 0, 2)

			for len(newSubtreeChan) > 0 {
				req := <-newSubtreeChan
				reqs = append(reqs, req)
				sent = append(sent, req.Subtree)
			}

			require.Len(t, sent, 2)
			require.True(t, samePointers(sent, stp.chainedSubtrees), "the rebuild did not send its chained subtrees")

			stp.retireChainedSubtrees()
			stp.closeRetiredSubtrees()
			// compared by pointer: a failure must not print a subtree that may be unmapped
			require.True(t, samePointers(sent, stp.retiredSubtrees), "a subtree was closed while its store was still pending")

			for _, req := range reqs {
				req.Done()
				req.ErrChan <- nil
			}

			stp.closeRetiredSubtrees()
			require.Zero(t, len(stp.retiredSubtrees), "a subtree stayed mapped after its store ended")
		})
	}
}

func samePointers(a, b []*subtreepkg.Subtree) bool {
	if len(a) != len(b) {
		return false
	}

	for _, st := range a {
		if !slices.Contains(b, st) {
			return false
		}
	}

	return true
}
