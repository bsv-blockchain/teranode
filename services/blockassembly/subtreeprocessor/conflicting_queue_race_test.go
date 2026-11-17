package subtreeprocessor

import (
	"context"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDequeueDuringBlockMovement_RejectsChildOfConflictingParent demonstrates
// the production bug observed on teranode-mainnet-eu-1 (v0.15.0-beta-3): a
// child transaction whose parent is already marked Conflicting=true in the
// UTXO store still lands in the block-assembly subtree. The mining candidate
// then fails ValidateBlock with "parent transaction X of tx Y has no block
// IDs" and is rejected with bad-txns-inputs-missingorspent.
//
// Race in production:
//
//	T0  parent P added to UTXO store via validator.
//	T1  ProcessConflicting (during moveForwardBlock with ConflictingNodes)
//	    flags P.Conflicting=true. Cascade walks P.outputs -> recorded
//	    spenders, finds none for child C: C's Spend has not been committed
//	    yet (C is mid-flight in the BA queue). Cascade misses C.
//	T2  Event loop falls into dequeueDuringBlockMovement to drain whatever
//	    accumulated during the moveForwardBlock case. The drain filter only
//	    checked self-hash against transactionMap and losingTxHashesMap. No
//	    parent-inpoints check. C admitted into subtree.
//	T3  C lands in subtree. Mining candidate built. Block REJECTED.
//
// Fix: processConflictingTransactions now returns a transient set of every
// hash flagged Conflicting=true by the BFS cascade (immediate losers + every
// descendant returned by MarkConflictingRecursively). That set is threaded
// through RemainderTransactionParams.ConflictingHashes into
// dequeueDuringBlockMovement, which rejects any node whose own hash is in
// the set OR whose TxInpoints.ParentTxHashes contains a hash in the set.
// On parent match the node's hash is also added to the set so any
// later-in-batch descendants are caught. The set is scoped to this single
// drain — the default-case dequeue path is left untouched.
func TestDequeueDuringBlockMovement_RejectsChildOfConflictingParent(t *testing.T) {
	stp := newTestProcessorNoStart(t)

	parentHash := chainhash.HashH([]byte("conflicting-parent"))
	childHash := chainhash.HashH([]byte("child-of-conflicting-parent"))
	otherHash := chainhash.HashH([]byte("unrelated-tx"))

	mkInpoints := func(parent chainhash.Hash) *subtreepkg.TxInpoints {
		in := &bt.Input{PreviousTxOutIndex: 0}
		require.NoError(t, in.PreviousTxIDAdd(&parent))

		ti, err := subtreepkg.NewTxInpointsFromInputs([]*bt.Input{in})
		require.NoError(t, err)

		return &ti
	}

	childNode := subtreepkg.Node{Hash: childHash, Fee: 1, SizeInBytes: 250}
	childInpoints := mkInpoints(parentHash)
	otherNode := subtreepkg.Node{Hash: otherHash, Fee: 2, SizeInBytes: 220}
	otherInpoints := mkInpoints(chainhash.HashH([]byte("unrelated-parent")))

	// Set the drain clock 1ms ahead of the enqueue clock so the
	// per-drain cutoff (validFromMillis = drainClock at window=0) admits
	// the enqueued batch. This is deterministic — no wall-clock waits —
	// and lets the test exercise the conflicting-parent rejection path
	// directly.
	enqueueAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	drainAt := enqueueAt.Add(1 * time.Millisecond)
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: drainAt}

	stp.queue.enqueueBatch(
		[]subtreepkg.Node{childNode, otherNode},
		[]*subtreepkg.TxInpoints{childInpoints, otherInpoints},
	)

	conflictingHashes := map[chainhash.Hash]struct{}{
		parentHash: {},
	}

	require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, conflictingHashes, true))

	hashes := collectSubtreeHashes(stp)

	assert.NotContains(t, hashes, childHash,
		"child of conflicting parent must be rejected by the dequeue filter")
	assert.Contains(t, hashes, otherHash,
		"unrelated tx must still pass through the filter")

	// Cascade through the set: rejected child hash should now be in
	// conflictingHashes so any later-in-batch descendant of the child is
	// also rejected without a store round-trip.
	_, marked := conflictingHashes[childHash]
	assert.True(t, marked, "rejected child must be added to the transient set "+
		"so its own descendants are caught later in the same drain")
}

// TestDequeueDuringBlockMovement_DrainCutoffAtClockNow pins, at the real
// call site, that with DoubleSpendWindow=0 dequeueDuringBlockMovement takes
// a batch enqueued in the drain's own millisecond and holds back a later one.
//
// The cutoff is the drain clock, inclusive, so the drain filters a batch
// that reached the queue before it against the block it moves, and still
// does not chase fresh ingest produced by AddTxBatchColumnar, the
// behaviour that previously stalled the scaling-2 pod inside
// moveForwardBlock.
//
// The Start-loop default-case dequeue still uses its own zero-guard
// (the default: branch of SubtreeProcessor.Start) and is NOT affected by
// this test.
//
// The "+1ms" subtest exercises the complementary case where the
// SubtreeProcessor clock has advanced past the enqueue clock; the
// batch passes the cutoff and drains.
func TestDequeueDuringBlockMovement_DrainCutoffAtClockNow(t *testing.T) {
	t.Run("same_millisecond_batch_drains", func(t *testing.T) {
		stp := newTestProcessorNoStart(t)
		require.Zero(t, stp.settings.BlockAssembly.DoubleSpendWindow,
			"default window is 0; cutoff-at-now is the property being asserted")

		fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		stp.clock = fixedClock{t: fixed}
		stp.queue.clock = fixedClock{t: fixed}

		txHash := chainhash.HashH([]byte("zero-window-same-ms"))
		stp.queue.enqueueBatch(
			[]subtreepkg.Node{{Hash: txHash, Fee: 1, SizeInBytes: 220}},
			[]*subtreepkg.TxInpoints{{}},
		)
		require.Equal(t, int64(1), stp.queue.length())

		require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))

		require.Zero(t, stp.queue.length(),
			"a same-ms batch must drain: the cutoff includes the drain's millisecond")
		require.Contains(t, collectSubtreeHashes(stp), txHash)
	})

	t.Run("later_batch_held_back", func(t *testing.T) {
		stp := newTestProcessorNoStart(t)

		drainAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		stp.clock = fixedClock{t: drainAt}
		stp.queue.clock = fixedClock{t: drainAt.Add(time.Millisecond)}

		txHash := chainhash.HashH([]byte("zero-window-later-ms"))
		stp.queue.enqueueBatch(
			[]subtreepkg.Node{{Hash: txHash, Fee: 1, SizeInBytes: 220}},
			[]*subtreepkg.TxInpoints{{}},
		)

		require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))

		require.Equal(t, int64(1), stp.queue.length(), "a batch stamped after the drain clock must be held back")
		require.NotContains(t, collectSubtreeHashes(stp), txHash)
	})

	t.Run("cutoff_is_sampled_once_at_entry", func(t *testing.T) {
		stp := newTestProcessorNoStart(t)

		drainAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		// The first reading is the drain's entry; every later one has moved on.
		stp.clock = &firstThenLaterClock{first: drainAt, later: drainAt.Add(5 * time.Millisecond)}

		before := chainhash.HashH([]byte("zero-window-entry-ms"))
		after := chainhash.HashH([]byte("zero-window-after-entry"))

		stp.queue.clock = fixedClock{t: drainAt}
		stp.queue.enqueueBatch([]subtreepkg.Node{{Hash: before, Fee: 1, SizeInBytes: 220}}, []*subtreepkg.TxInpoints{{}})
		stp.queue.clock = fixedClock{t: drainAt.Add(time.Millisecond)}
		stp.queue.enqueueBatch([]subtreepkg.Node{{Hash: after, Fee: 1, SizeInBytes: 220}}, []*subtreepkg.TxInpoints{{}})

		require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))

		hashes := collectSubtreeHashes(stp)
		require.Contains(t, hashes, before)
		require.NotContains(t, hashes, after, "the cutoff is the clock at entry, not a reading taken during the drain")
		require.Equal(t, int64(1), stp.queue.length())
	})

	t.Run("positive_window_cutoff_is_sampled_once_at_entry", func(t *testing.T) {
		stp := newTestProcessorNoStart(t)
		stp.settings.BlockAssembly.DoubleSpendWindow = time.Second

		drainAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		cutoff := drainAt.Add(-time.Second)
		// The first reading is the drain's entry; every later one has moved on.
		stp.clock = &firstThenLaterClock{first: drainAt, later: drainAt.Add(5 * time.Millisecond)}

		older := chainhash.HashH([]byte("positive-window-entry-older"))
		newer := chainhash.HashH([]byte("positive-window-entry-newer"))

		stp.queue.clock = fixedClock{t: cutoff.Add(-time.Millisecond)}
		stp.queue.enqueueBatch([]subtreepkg.Node{{Hash: older, Fee: 1, SizeInBytes: 220}}, []*subtreepkg.TxInpoints{{}})
		stp.queue.clock = fixedClock{t: cutoff.Add(time.Millisecond)}
		stp.queue.enqueueBatch([]subtreepkg.Node{{Hash: newer, Fee: 1, SizeInBytes: 220}}, []*subtreepkg.TxInpoints{{}})

		require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))

		hashes := collectSubtreeHashes(stp)
		require.Contains(t, hashes, older)
		require.NotContains(t, hashes, newer, "the cutoff is the clock at entry minus the window, not a later reading")
	})

	t.Run("item_bound_stops_same_millisecond_arrivals", func(t *testing.T) {
		stp := newTestProcessorNoStart(t)

		drainAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		stp.clock = fixedClock{t: drainAt}
		stp.queue.clock = fixedClock{t: drainAt}

		stp.queue.enqueueBatch([]subtreepkg.Node{{Hash: chainhash.HashH([]byte("item-bound-0")), Fee: 1, SizeInBytes: 220}}, []*subtreepkg.TxInpoints{{}})

		// Each chunk the drain hands over brings two more batches stamped in
		// the drain's millisecond, which the inclusive cutoff admits. Two, not
		// one, so a live read of the queue length stays ahead of what the
		// drain has taken; only the length read at entry ends the drain. The
		// cap keeps a broken bound from looping forever.
		const arrivalCap = 10

		chunks := 0
		require.NoError(t, stp.forEachDrainChunk(1, func(batches []*TxBatch) (int, error) {
			chunks++
			if chunks <= arrivalCap {
				for arrival := 0; arrival < 2; arrival++ {
					stp.queue.enqueueBatch(
						[]subtreepkg.Node{{Hash: chainhash.HashH([]byte("item-bound-" + strconv.Itoa(chunks) + "-" + strconv.Itoa(arrival))), Fee: 1, SizeInBytes: 220}},
						[]*subtreepkg.TxInpoints{{}},
					)
				}
			}

			return 0, nil
		}))

		require.Equal(t, 1, chunks, "the drain must stop at the one item queued at entry")
		require.Equal(t, int64(2), stp.queue.length(), "the batches that arrived during the drain stay queued")
	})

	t.Run("negative_window_cuts_off_at_entry", func(t *testing.T) {
		stp := newTestProcessorNoStart(t)
		stp.settings.BlockAssembly.DoubleSpendWindow = -time.Second

		drainAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		stp.clock = fixedClock{t: drainAt}

		sameMs := chainhash.HashH([]byte("negative-window-same-ms"))
		laterMs := chainhash.HashH([]byte("negative-window-later-ms"))

		stp.queue.clock = fixedClock{t: drainAt}
		stp.queue.enqueueBatch([]subtreepkg.Node{{Hash: sameMs, Fee: 1, SizeInBytes: 220}}, []*subtreepkg.TxInpoints{{}})
		stp.queue.clock = fixedClock{t: drainAt.Add(time.Millisecond)}
		stp.queue.enqueueBatch([]subtreepkg.Node{{Hash: laterMs, Fee: 1, SizeInBytes: 220}}, []*subtreepkg.TxInpoints{{}})

		require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))

		// A negative window behaves like no window, on both sides of the cutoff.
		hashes := collectSubtreeHashes(stp)
		require.Contains(t, hashes, sameMs, "a batch stamped in the drain's millisecond must drain, as at window 0")
		require.NotContains(t, hashes, laterMs, "a negative window must not move the cutoff past the drain clock")
		require.Equal(t, int64(1), stp.queue.length())
	})

	t.Run("positive_window_cutoff_is_exclusive", func(t *testing.T) {
		stp := newTestProcessorNoStart(t)
		stp.settings.BlockAssembly.DoubleSpendWindow = time.Second

		drainAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		cutoff := drainAt.Add(-time.Second)
		stp.clock = fixedClock{t: drainAt}

		older := chainhash.HashH([]byte("positive-window-before-cutoff"))
		atCutoff := chainhash.HashH([]byte("positive-window-at-cutoff"))

		stp.queue.clock = fixedClock{t: cutoff.Add(-time.Millisecond)}
		stp.queue.enqueueBatch([]subtreepkg.Node{{Hash: older, Fee: 1, SizeInBytes: 220}}, []*subtreepkg.TxInpoints{{}})
		stp.queue.clock = fixedClock{t: cutoff}
		stp.queue.enqueueBatch([]subtreepkg.Node{{Hash: atCutoff, Fee: 1, SizeInBytes: 220}}, []*subtreepkg.TxInpoints{{}})

		require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))

		hashes := collectSubtreeHashes(stp)
		require.Contains(t, hashes, older, "a batch older than the window must drain")
		require.NotContains(t, hashes, atCutoff, "with a window, a batch stamped exactly at now - window must wait")
		require.Equal(t, int64(1), stp.queue.length())
	})

	t.Run("control_one_ms_advance_drains_the_batch", func(t *testing.T) {
		stp := newTestProcessorNoStart(t)
		require.Zero(t, stp.settings.BlockAssembly.DoubleSpendWindow)

		enqueueAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		drainAt := enqueueAt.Add(1 * time.Millisecond)

		stp.queue.clock = fixedClock{t: enqueueAt}
		stp.clock = fixedClock{t: drainAt}

		txHash := chainhash.HashH([]byte("zero-window-advance"))
		stp.queue.enqueueBatch(
			[]subtreepkg.Node{{Hash: txHash, Fee: 1, SizeInBytes: 220}},
			[]*subtreepkg.TxInpoints{{}},
		)
		require.Equal(t, int64(1), stp.queue.length())

		require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))

		require.Equal(t, int64(0), stp.queue.length(),
			"1ms advance must let the batch drain")
		assert.Contains(t, collectSubtreeHashes(stp), txHash,
			"drained batch must be admitted into the subtree")
	})
}

// newTestProcessorNoStart builds a SubtreeProcessor without starting the
// event-loop goroutine. This lets the test drive dequeueDuringBlockMovement
// directly with a known queue state and a known conflictingHashes set, with
// no race against the default-case dequeue.
func newTestProcessorNoStart(t *testing.T) *SubtreeProcessor {
	t.Helper()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(context.Background(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 32

	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, settings, blob_memory.New(), nil, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	return stp
}

func collectSubtreeHashes(stp *SubtreeProcessor) []chainhash.Hash {
	out := make([]chainhash.Hash, 0)
	for _, st := range stp.chainedSubtrees {
		for _, n := range st.Nodes {
			out = append(out, n.Hash)
		}
	}
	if cs := stp.currentSubtree.Load(); cs != nil {
		for _, n := range cs.Nodes {
			out = append(out, n.Hash)
		}
	}
	return out
}

// firstThenLaterClock returns first on its first reading and later after that.
type firstThenLaterClock struct {
	first, later time.Time
	read         bool
}

func (c *firstThenLaterClock) Now() time.Time {
	if c.read {
		return c.later
	}

	c.read = true

	return c.first
}
