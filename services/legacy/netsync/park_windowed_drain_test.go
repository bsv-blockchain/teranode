package netsync

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestParkedDispatchMayBeWindowed is the fix for the reason a node with a hundred
// blocks already on disk still leaves its validator idle.
//
// A dispatch is marked windowed, meaning it may run alongside another block, in
// exactly one place: when its parent is still being validated. Every block drained
// from the park has a parent that is already committed, so no parked block was
// ever windowed, and an unwindowed block is admitted only into a completely empty
// window. During catch-up 91% of blocks arrive out of order and go through the
// park, so the overwhelming majority were processed strictly one at a time with
// the validator necessarily empty before each one started.
//
// The window's own depth and byte budget exist to keep more than one block in
// flight. The path carrying most blocks could not use them.
//
// A drained block is a safer candidate than the live one this was built for: its
// parent is in the chain rather than merely in flight, and its height is known
// because the sweep carries the parent's height for exactly this purpose.
func TestParkedDispatchMayBeWindowed(t *testing.T) {
	// Two parked blocks in one chain, both committable, with the parent's height
	// known. Today the first is admitted and the second cannot be, because an
	// unwindowed dispatch demands a completely empty window and the first one is
	// still in it. That is the idle: 128 blocks on disk and one in flight.
	setUp := func(t *testing.T) (*parkWiringHarness, chainhash.Hash) {
		t.Helper()

		h := newParkWiringHarness(t, true)
		h.sm.drainAsync.Store(true)

		// The window route is what makes a drained block eligible to run beside
		// another, and it needs the depth, the unified below-checkpoint route and
		// a store that honours the outpoint-only fast path. Set before the
		// dispatcher is built, because the depth is resolved at construction.
		h.sm.settings.BlockValidation.QuickWindowBlocks = 4
		h.sm.settings.BlockValidation.QuickValidateSkipUtxoLock = true
		h.sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly = 20
		h.sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
		h.sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true
		h.sm.utxoStore = &utxo.MockUtxostore{SupportsOutpointOnlySpendResult: true}

		h.sm.dispatcher = newBlockDispatcher(h.sm)
		require.GreaterOrEqual(t, h.sm.dispatcher.depth, 2, "precondition: the window must have room for two")
		require.True(t, h.sm.windowRoute(700001), "precondition: the window route must be on for these heights")

		// Never return from the worker, so the first block stays in the window
		// while the second is offered. Without this the first settles and the
		// test proves nothing about concurrency.
		h.sm.dispatcher.parkedRun = func(ctx context.Context, _ *blockDispatch) error {
			<-ctx.Done()

			return nil
		}

		parent := chainhash.Hash{0xaa}

		first := parkedBlock{hash: chainhash.Hash{0x01}, prevBlock: parent, size: 1 << 20}
		require.True(t, h.sm.blockPark.AdoptWritten(first))

		second := parkedBlock{hash: chainhash.Hash{0x02}, prevBlock: first.hash, size: 1 << 20}
		require.True(t, h.sm.blockPark.AdoptWritten(second))

		// The sweep carries the parent's height, which is what makes each child's
		// height knowable without a store lookup.
		h.sm.drainQueue = []drainRequest{{parent: parent, parentHeight: 700000}}

		return h, first.hash
	}

	t.Run("a second drained block is admitted while the first is still in flight", func(t *testing.T) {
		h, firstHash := setUp(t)
		bd := h.sm.dispatcher

		require.True(t, h.sm.drainStep(bd), "the first block must be placed")
		require.Len(t, bd.frontier, 1)
		require.Equal(t, uint32(700001), bd.frontier[0].height,
			"a drained dispatch must carry a real height, or nothing can chain onto it")

		// Its own commit is what normally queues its children; here the first is
		// still in flight, so queue the drain the way the tail would.
		h.sm.drainQueue = []drainRequest{{parent: firstHash, parentHeight: 700001}}

		require.True(t, h.sm.drainStep(bd),
			"the second block is committable and the window has room, so nothing should stop it")
		require.Len(t, bd.frontier, 2,
			"two drained blocks in flight at once is the whole point; one at a time is the idle")
		require.Equal(t, uint32(700002), bd.frontier[1].height)
	})

	t.Run("a drained block with no known height keeps the old one-at-a-time rule", func(t *testing.T) {
		h, _ := setUp(t)
		bd := h.sm.dispatcher

		// Height zero is what a block recovered from disk after a restart carries,
		// and a zero in the window is refused as a parent, so such a block must
		// not be admitted alongside something it cannot be chained to.
		h.sm.drainQueue = []drainRequest{{parent: chainhash.Hash{0xaa}, parentHeight: 0}}

		require.True(t, h.sm.drainStep(bd))
		require.Len(t, bd.frontier, 1)
		require.Zero(t, bd.frontier[0].height)

		h.sm.drainQueue = []drainRequest{{parent: chainhash.Hash{0x01}, parentHeight: 0}}

		require.False(t, h.sm.drainStep(bd),
			"with no height to chain on, the second block must wait for an empty window as it always did")
		require.Len(t, bd.frontier, 1)
	})
}

// TestDrainOpenDoesNotRequireAnEmptyWindow pins the consumer's half of the same
// fix. Relaxing the dispatch guard alone changes nothing in production, because
// the loop would not choose the drain in the first place while the window held
// anything.
func TestDrainOpenDoesNotRequireAnEmptyWindow(t *testing.T) {
	src := readManagerSource(t)

	i := strings.Index(src, "drainOpen :=")
	require.Positive(t, i, "the loop no longer computes drainOpen, so this test needs rewriting rather than deleting")

	line := src[i:]
	if cut := strings.Index(line, "\n"); cut > 0 {
		line = line[:cut]
	}

	require.False(t, strings.Contains(line, "frontierEmpty"),
		"requiring an empty window here restates the rule a windowed drained block was just freed from, and would keep the validator idle between every parked block")
	require.Contains(t, line, "len(sm.drainQueue) > 0",
		"something queued is what makes the drain a candidate; capacity is drainStep's own test")
}
