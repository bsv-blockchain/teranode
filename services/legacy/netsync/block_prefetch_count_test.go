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

	sm.blockPrefetchBudgetBytes = capacity
	sm.blockPrefetchBudget = semaphore.NewWeighted(capacity)
	sm.inFlightBlocks = make(map[chainhash.Hash]*inFlightBlock)
}

// TestAcquireBlockPrefetch_PipelineChargesOneSlotPerBlock pins the replacement.
// A byte budget cannot admit a block larger than itself without clamping, and a
// clamped block waits for the budget to empty completely; a slot admits any block
// at any size. On the pipeline path the bytes are already gone by the time this
// runs, so charging them reserves memory nobody is holding.
func TestAcquireBlockPrefetch_PipelineChargesOneSlotPerBlock(t *testing.T) {
	sm := newDemotionManager(t)
	sm.blockPark = &blockPark{} // enabled: streaming (and so the pipeline) is active
	enablePrefetchBudgetForTest(t, sm, 2)

	huge := int64(512 << 20) // far larger than any byte budget this node would set

	w1, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, huge)
	require.NoError(t, err)
	require.Equal(t, int64(1), w1, "one block costs one slot, whatever its size")

	w2, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x02}, huge)
	require.NoError(t, err)
	require.Equal(t, int64(1), w2)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = sm.AcquireBlockPrefetch(ctx, nil, chainhash.Hash{0x03}, huge)
	require.Error(t, err, "the third block waits, because the budget is a count of two")
}

// TestAcquireBlockPrefetch_WithoutThePipelineStillChargesBytes pins that the old
// path is untouched, which is what makes a disabled park safe: with no park to
// stream into, the wire layer falls back to decoding and OnBlock's byte-weighted
// admission is the only one that ever runs.
func TestAcquireBlockPrefetch_WithoutThePipelineStillChargesBytes(t *testing.T) {
	sm := newDemotionManager(t)
	sm.blockPark = nil // disabled: no pipeline, the byte-weighted path applies
	enablePrefetchBudgetForTest(t, sm, 8<<20)

	w, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, 1<<20)
	require.NoError(t, err)
	require.Equal(t, int64(1<<20), w, "the byte path charges the block's size, as it always has")
}

// TestAcquireBlockPrefetch_PipelineStillRefusesADuplicate pins the half of the
// admission gate that is NOT about memory. Changing the unit must not weaken the
// rule that stops a second copy of a block entering the pipeline behind the first.
func TestAcquireBlockPrefetch_PipelineStillRefusesADuplicate(t *testing.T) {
	sm := newDemotionManager(t)
	sm.blockPark = &blockPark{} // enabled: streaming (and so the pipeline) is active
	enablePrefetchBudgetForTest(t, sm, 4)

	_, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, 1<<20)
	require.NoError(t, err)

	_, err = sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, 1<<20)
	require.ErrorIs(t, err, ErrDuplicateBlockInFlight)
}
