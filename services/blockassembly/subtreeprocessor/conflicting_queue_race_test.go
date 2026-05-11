package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"
	"time"

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

	childNode := subtreepkg.Node{Hash: childHash, Fee: 1, SizeInBytes: 250}
	childInpoints := &subtreepkg.TxInpoints{
		ParentTxHashes: []chainhash.Hash{parentHash},
		Idxs:           [][]uint32{{0}},
	}
	otherNode := subtreepkg.Node{Hash: otherHash, Fee: 2, SizeInBytes: 220}
	otherInpoints := &subtreepkg.TxInpoints{
		ParentTxHashes: []chainhash.Hash{chainhash.HashH([]byte("unrelated-parent"))},
		Idxs:           [][]uint32{{0}},
	}

	stp.queue.enqueueBatch(
		[]subtreepkg.Node{childNode, otherNode},
		[]*subtreepkg.TxInpoints{childInpoints, otherInpoints},
	)

	// dequeueDuringBlockMovement holds back batches enqueued at-or-after
	// (now - DoubleSpendWindow). Default window is 0, so it holds batches
	// with time == now. A short sleep moves batch.time strictly into the
	// past so the drain releases it.
	time.Sleep(5 * time.Millisecond)

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

// TestDequeueDuringBlockMovement_ZeroWindowAsymmetry pins, at the real call
// site, the divergence between the two validFromMillis formulas inside
// SubtreeProcessor:
//
//	Start loop (SubtreeProcessor.go:807-813) zero-guards the calculation:
//	  if DoubleSpendWindow == 0 → validFromMillis = 0 → queue filter off.
//
//	dequeueDuringBlockMovement (SubtreeProcessor.go:3789) does not:
//	  validFromMillis = clock.Now().Add(-1 * window).UnixMilli() always.
//
// With DoubleSpendWindow == 0 (the documented default - see
// settings/blockassembly_settings.go:29) and the SubtreeProcessor clock
// equal to the queue clock at enqueue time, the drain formula produces
// validFromMillis = batch.time, the queue filter at queue.go:96 fires
// (validFromMillis > 0 && time >= validFromMillis), and the batch is
// held back.
//
// The "+1ms" subtest is the control: advancing only the
// SubtreeProcessor clock by 1 millisecond is enough to move batch.time
// strictly below validFromMillis, after which the drain admits it.
// That is the deterministic equivalent of the time.Sleep(5ms) workaround
// at conflicting_queue_race_test.go:75.
func TestDequeueDuringBlockMovement_ZeroWindowAsymmetry(t *testing.T) {
	t.Run("same_millisecond_batch_is_held_back", func(t *testing.T) {
		stp := newTestProcessorNoStart(t)
		require.Zero(t, stp.settings.BlockAssembly.DoubleSpendWindow,
			"default window is 0; the asymmetry only manifests at 0")

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
			"drain held back a same-millisecond batch at window=0; the Start "+
				"loop's zero-guard would have admitted it")
		require.NotContains(t, collectSubtreeHashes(stp), txHash,
			"the held-back batch must not appear in chainedSubtrees / currentSubtree")
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
