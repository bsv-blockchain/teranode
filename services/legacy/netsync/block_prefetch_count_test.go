package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// enablePrefetchBudgetForTest wires the download-admission budget onto an
// already-built manager, exactly as New's own constructor does: the semaphore,
// its capacity, and the dedup set created in lockstep with it. Tests that need a
// real prefetch budget call this rather than reaching into the three fields
// separately, so a future field added to that trio is not missed here.
func enablePrefetchBudgetForTest(t *testing.T, sm *SyncManager, capacity int64) {
	t.Helper()

	sm.blockPrefetchBudgetSlots = capacity
	sm.blockPrefetchBudget = semaphore.NewWeighted(capacity)
	sm.inFlightBlocks = make(map[chainhash.Hash]*inFlightBlock)
}

// TestAcquireBlockPrefetch_PipelineChargesOneSlotPerBlock pins the replacement.
// A byte budget cannot admit a block larger than itself without clamping, and a
// clamped block waits for the budget to empty completely; a slot admits any block
// at any size. Every block on the pipeline path is charged one slot, whatever
// its size, because the bytes are already gone by the time admission runs.
func TestAcquireBlockPrefetch_PipelineChargesOneSlotPerBlock(t *testing.T) {
	sm := newDemotionManager(t)
	enablePrefetchBudgetForTest(t, sm, 2)

	require.NoError(t, sm.AcquireBlockPrefetch(context.Background(), chainhash.Hash{0x01}))
	require.NoError(t, sm.AcquireBlockPrefetch(context.Background(), chainhash.Hash{0x02}))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := sm.AcquireBlockPrefetch(ctx, chainhash.Hash{0x03})
	require.Error(t, err, "the third block waits, because the budget is a count of two")
}

// TestAcquireBlockPrefetch_RefusesADuplicate pins the half of the admission
// gate that is NOT about memory. Changing the unit must not weaken the rule
// that stops a second copy of a block entering the pipeline behind the first.
func TestAcquireBlockPrefetch_RefusesADuplicate(t *testing.T) {
	sm := newDemotionManager(t)
	enablePrefetchBudgetForTest(t, sm, 4)

	require.NoError(t, sm.AcquireBlockPrefetch(context.Background(), chainhash.Hash{0x01}))

	err := sm.AcquireBlockPrefetch(context.Background(), chainhash.Hash{0x01})
	require.ErrorIs(t, err, ErrDuplicateBlockInFlight)
}
