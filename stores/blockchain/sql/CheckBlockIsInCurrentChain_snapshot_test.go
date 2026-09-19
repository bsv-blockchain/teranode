package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAReaderThatSnapshottedBeforeAMutatorsRebuildDoesNotAnswerFromIt names the
// interleaving icellan raised against CheckBlockIsInCurrentChain.go:38-42, where the guard
// is read before maxBlockID:
//
//	the reader loads mainChainRebuilding as 0
//	  -> a concurrent StoreBlock for a sibling fork raises the guard, commits the row and
//	     calls updateMaxBlockID(newBlockID)
//	  -> the reader loads maxID, which now includes the fork id
//	  -> the reader snapshots offChainBlockIDs, still the pre-rebuild map
//	  -> id <= maxID and absent from the set, so the forked-set route answers true for a
//	     block that is not on the chain.
//
// The fix is not to move the two reads. Loading maxBlockID and the set pointer before the
// guard check narrows the window without closing it: a mutator that finishes entirely
// between the reader's two loads still leaves a fresh maxBlockID beside a stale set and a
// guard that reads zero. What closes it is the pair of post-snapshot checks on the single
// line in checkBlockIsInCurrentChainInMemory:
//
//	if s.mainChainRebuilding.Load() > 0 || setEpoch < s.chainStateEpoch.Load()
//
// Each subtest below pins one half of that line by putting the store into the exact state
// the descheduled reader is holding, and fails if its half is removed.
//
// Both drive checkBlockIsInCurrentChainInMemory directly, because the window is a
// scheduling gap between two loads inside one function and no store-level call can open it
// deterministically. maxID is passed in exactly as CheckBlockIsInCurrentChain read it:
// after the mutator advanced it.
func TestAReaderThatSnapshottedBeforeAMutatorsRebuildDoesNotAnswerFromIt(t *testing.T) {
	// forkWithStaleSnapshot builds a two-block main chain, records the forked set and epoch
	// a reader would have snapshotted before a fork arrives, stores the fork through the
	// real StoreBlock, then puts that pre-fork snapshot back. It returns the store, the
	// committed fork id, and the maxBlockID the reader read after the mutator advanced it.
	forkWithStaleSnapshot := func(t *testing.T) (*SQL, uint32, uint32) {
		t.Helper()

		s := newStoreWithInMemoryChainCheck(t)
		t.Cleanup(func() { _ = s.Close(context.Background()) })

		storeBlocks(t, s, block1, block2)

		s.offChainBlockIDsMu.RLock()
		staleSet := s.offChainBlockIDs
		s.offChainBlockIDsMu.RUnlock()

		staleEpoch := s.offChainSetEpoch.Load()

		forkID, _, err := s.StoreBlock(context.Background(), blockAlternative2, "peer")
		require.NoError(t, err)

		_, forked := staleSet[uint32(forkID)]
		require.False(t, forked, "precondition: the pre-fork snapshot cannot contain the fork block")

		maxID := uint32(s.maxBlockID.Load())
		require.GreaterOrEqual(t, maxID, uint32(forkID), "precondition: the mutator advanced maxBlockID past the fork id")

		s.offChainBlockIDsMu.Lock()
		s.offChainBlockIDs = staleSet
		s.offChainSetEpoch.Store(staleEpoch)
		s.offChainBlockIDsMu.Unlock()

		return s, uint32(forkID), maxID
	}

	// The mutator is still inside StoreBlock. On a forked-set node the guard goes up before
	// the INSERT and comes down in a defer that runs after the rebuild, so it is still up,
	// and the epoch bump the rebuild performs has not happened yet. The installed set's
	// epoch and chainStateEpoch are therefore equal and only the guard can catch this.
	t.Run("mutator still holds the guard", func(t *testing.T) {
		s, forkID, maxID := forkWithStaleSnapshot(t)

		s.offChainBlockIDsMu.Lock()
		s.offChainSetEpoch.Store(s.chainStateEpoch.Load())
		s.offChainBlockIDsMu.Unlock()

		s.mainChainRebuilding.Add(1)
		defer s.mainChainRebuilding.Add(-1)

		result, route, _, err := s.checkBlockIsInCurrentChainInMemory(context.Background(), []uint32{forkID}, maxID)
		require.NoError(t, err)
		require.Equal(t, answeredBySQL, route, "a snapshot taken while a mutator holds the guard must not be answered from")
		require.False(t, result, "the forked-set route accepted a fork block from a pre-rebuild snapshot")
	})

	// The mutator finished and dropped the guard between the reader's two looks, which is
	// the half the guard cannot cover. Every mutator bumps chainStateEpoch after its write
	// and before dropping the guard, so the reader's post-snapshot epoch read sees the bump
	// while the snapshot it is holding still carries the older epoch.
	t.Run("mutator finished and dropped the guard", func(t *testing.T) {
		s, forkID, maxID := forkWithStaleSnapshot(t)

		require.Zero(t, s.mainChainRebuilding.Load(), "precondition: the mutator has dropped the guard")
		require.Less(t, s.offChainSetEpoch.Load(), s.chainStateEpoch.Load(),
			"precondition: the reader's snapshot predates the mutator's write")

		result, route, _, err := s.checkBlockIsInCurrentChainInMemory(context.Background(), []uint32{forkID}, maxID)
		require.NoError(t, err)
		require.Equal(t, answeredBySQL, route, "a snapshot whose epoch predates the latest write must not be answered from")
		require.False(t, result, "the forked-set route accepted a fork block from a pre-rebuild snapshot")
	})
}
