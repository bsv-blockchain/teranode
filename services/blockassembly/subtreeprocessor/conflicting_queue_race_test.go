package subtreeprocessor

import (
	"context"
	"net/url"
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
// call site, that dequeueDuringBlockMovement holds back batches enqueued
// in the same millisecond as the drain — even with DoubleSpendWindow=0.
//
// The drain's validFromMillis is always set (clock.Now() when
// DoubleSpendWindow=0, clock.Now()-DoubleSpendWindow otherwise) so the
// queue filter at queue.go:96 (`batch.time >= validFromMillis -> hold`)
// uses the drain clock as the cutoff. This bounds the drain to batches
// that already existed before the drain started, preventing the loop
// from chasing fresh ingest produced by AddTxBatchColumnar — the
// behaviour that previously stalled the scaling-2 pod inside
// moveForwardBlock.
//
// The Start-loop default-case dequeue still uses its own zero-guard
// (SubtreeProcessor.go:807-813) and is NOT affected by this test.
//
// The "+1ms" subtest exercises the complementary case where the
// SubtreeProcessor clock has advanced past the enqueue clock; the
// batch passes the cutoff and drains.
func TestDequeueDuringBlockMovement_DrainCutoffAtClockNow(t *testing.T) {
	t.Run("same_millisecond_batch_held_back", func(t *testing.T) {
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

		require.Equal(t, int64(1), stp.queue.length(),
			"same-ms batch must be held back: validFromMillis == batch.time")
		require.NotContains(t, collectSubtreeHashes(stp), txHash,
			"held-back batch must not appear in chainedSubtrees / currentSubtree")
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
