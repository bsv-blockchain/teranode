package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// txInvs builds n transaction announcements with distinct hashes. tag keeps
// the hashes of two calls apart.
func txInvs(n int, tag byte) []*wire.InvVect {
	items := make([]*wire.InvVect, n)

	for i := range items {
		h := chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), tag, 0x1f}
		items[i] = wire.NewInvVect(wire.InvTypeTx, &h)
	}

	return items
}

// inv is one transaction announcement for h.
func inv(h chainhash.Hash) *wire.InvVect {
	return wire.NewInvVect(wire.InvTypeTx, &h)
}

// droppedInvs reads the announcement drop counter for one reason.
func droppedInvs(reason string) float64 {
	return testutil.ToFloat64(prometheusLegacyNetsyncInvDropped.WithLabelValues(reason))
}

// TestInvQueueMaxHashes_FloorsAtOneChunk pins the legal range of
// legacy_invQueueMaxHashes: below one chunk falls back to the default, and zero
// is not unbounded, because an unbounded queue is the 2026-10-19 incident.
func TestInvQueueMaxHashes_FloorsAtOneChunk(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	require.Equal(t, 400000, invQueueMaxHashes(ulogger.TestLogger{}, tSettings), "the default is eight maximum-size announcements")

	tSettings.Legacy.InvQueueMaxHashes = 100
	require.Equal(t, 400000, invQueueMaxHashes(ulogger.TestLogger{}, tSettings), "below one chunk falls back to the default")

	tSettings.Legacy.InvQueueMaxHashes = 0
	require.Equal(t, 400000, invQueueMaxHashes(ulogger.TestLogger{}, tSettings), "zero is not unbounded")

	tSettings.Legacy.InvQueueMaxHashes = txInvLookupChunk
	require.Equal(t, txInvLookupChunk, invQueueMaxHashes(ulogger.TestLogger{}, tSettings), "one chunk is the floor")
}

// TestAdmitTxInvs_OneMaximumMessageIsAdmittedWhole pins the case the first
// draft of the design failed: a single 50,000-hash announcement from an honest
// peer on an idle node is queued whole, thirteen chunks, nothing dropped.
func TestAdmitTxInvs_OneMaximumMessageIsAdmittedWhole(t *testing.T) {
	sm, p, state, _ := newInvHarness(t, test.CreateBaseTestSettings(t), nil, blockchain2.FSMStateRUNNING)
	before := droppedInvs("peer_share")

	sm.admitTxInvs(p, state, txInvs(wire.MaxInvPerMsg, 0x01))

	require.Len(t, sm.invQueue, 13)
	require.Equal(t, int64(wire.MaxInvPerMsg), sm.invQueued.Load())
	require.Equal(t, int64(wire.MaxInvPerMsg), state.queuedInvs.Load())
	require.Equal(t, before, droppedInvs("peer_share"))
}

// TestAdmitTxInvs_DropsAPeerOverItsShare pins the per-peer share: a peer with
// two maximum-size announcements queued has its third dropped and counted,
// and a second peer is not affected.
func TestAdmitTxInvs_DropsAPeerOverItsShare(t *testing.T) {
	sm, p, state, _ := newInvHarness(t, test.CreateBaseTestSettings(t), nil, blockchain2.FSMStateRUNNING)
	before := droppedInvs("peer_share")

	for tag := byte(1); tag <= 3; tag++ {
		sm.admitTxInvs(p, state, txInvs(wire.MaxInvPerMsg, tag))
	}

	require.Equal(t, int64(invPeerShare), state.queuedInvs.Load(), "two maximum-size messages are the share")
	require.Equal(t, int64(invPeerShare), sm.invQueued.Load())
	require.Equal(t, before+float64(wire.MaxInvPerMsg), droppedInvs("peer_share"))

	p2, state2 := newHarnessPeer(t, sm)
	sm.admitTxInvs(p2, state2, txInvs(wire.MaxInvPerMsg, 0x09))
	require.Equal(t, int64(wire.MaxInvPerMsg), state2.queuedInvs.Load(), "a second peer is unaffected")
	require.Equal(t, int64(invPeerShare+wire.MaxInvPerMsg), sm.invQueued.Load())
}

// TestAdmitTxInvs_DropsAtTheNodeCapAndClampsTheShare pins the node cap and
// the clamp: with the cap below the share, the share is the cap, and a chunk
// that fits its peer's share but not the node is dropped under node_cap and
// not charged to the peer.
func TestAdmitTxInvs_DropsAtTheNodeCapAndClampsTheShare(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.InvQueueMaxHashes = 2 * txInvLookupChunk

	sm, p, state, _ := newInvHarness(t, tSettings, nil, blockchain2.FSMStateRUNNING)
	require.Equal(t, 2*txInvLookupChunk, sm.invShare(), "the share clamps to a cap below it")

	p2, state2 := newHarnessPeer(t, sm)
	before := droppedInvs("node_cap")

	sm.admitTxInvs(p, state, txInvs(txInvLookupChunk, 0x01))
	sm.admitTxInvs(p2, state2, txInvs(txInvLookupChunk, 0x02))
	sm.admitTxInvs(p, state, txInvs(txInvLookupChunk, 0x03))

	require.Equal(t, int64(2*txInvLookupChunk), sm.invQueued.Load())
	require.Equal(t, int64(txInvLookupChunk), state.queuedInvs.Load(), "the dropped chunk is not charged to the peer")
	require.Equal(t, int64(txInvLookupChunk), state2.queuedInvs.Load())
	require.Equal(t, before+float64(txInvLookupChunk), droppedInvs("node_cap"))
}

// TestAdmitTxInvs_AFullChannelDropsUnderNodeCap pins the non-blocking send's
// default branch: production sizes the channel to the cap so it never binds
// first, but if it did, the chunk is dropped and both counters given back.
func TestAdmitTxInvs_AFullChannelDropsUnderNodeCap(t *testing.T) {
	sm, p, state, _ := newInvHarness(t, test.CreateBaseTestSettings(t), nil, blockchain2.FSMStateRUNNING)
	sm.invQueue = make(chan *invChunk, 1)
	before := droppedInvs("node_cap")

	sm.admitTxInvs(p, state, txInvs(1, 0x01))
	sm.admitTxInvs(p, state, txInvs(1, 0x02))

	require.Equal(t, int64(1), sm.invQueued.Load())
	require.Equal(t, int64(1), state.queuedInvs.Load())
	require.Equal(t, before+1, droppedInvs("node_cap"))
}

// TestLookupInvBatch_CoalescesAcrossPeersAndRequestsPerPeer pins the batching
// that replaces the shared get batcher's timer: two peers' small announcements
// queued before a worker runs go to the store in one read, each peer's
// request queue gains its own missing hashes, and every count is given back.
func TestLookupInvBatch_CoalescesAcrossPeersAndRequestsPerPeer(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	store := newLookupStore(t, tSettings, "inv_queue_coalesce")
	sm, p, state, _ := newInvHarness(t, tSettings, store, blockchain2.FSMStateRUNNING)
	p2, state2 := newHarnessPeer(t, sm)

	sm.admitTxInvs(p, state, txInvs(10, 0x01))
	sm.admitTxInvs(p2, state2, txInvs(10, 0x02))

	touched := sm.lookupInvBatch(<-sm.invQueue)

	require.Equal(t, int64(1), store.batches.Load(), "both peers' chunks go to the store in one read")
	require.Equal(t, int64(20), store.batchSize.Load())
	require.Len(t, touched, 2)
	require.Equal(t, 10, state.requestQueue.Length())
	require.Equal(t, 10, state2.requestQueue.Length())
	require.Zero(t, sm.invQueued.Load())
	require.Zero(t, state.queuedInvs.Load()+state2.queuedInvs.Load())
	require.Empty(t, sm.invQueue)
}

// TestLookupInvBatch_ReadsEachHashOnce pins the deduplication before the
// read: a hash two peers announce, or one peer announces twice, is read once
// and requested from the first announcer only; a hash already asked for, or
// already rejected, is neither read nor requested.
func TestLookupInvBatch_ReadsEachHashOnce(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	store := newLookupStore(t, tSettings, "inv_queue_dedup")
	sm, p, state, _ := newInvHarness(t, tSettings, store, blockchain2.FSMStateRUNNING)
	p2, state2 := newHarnessPeer(t, sm)

	shared := chainhash.Hash{0x5a}
	requested := chainhash.Hash{0x5b}
	rejected := chainhash.Hash{0x5c}
	sm.requestedTxns.Set(requested, struct{}{})
	sm.rejectedTxns.Set(rejected, struct{}{})

	sm.admitTxInvs(p, state, []*wire.InvVect{inv(shared), inv(shared), inv(requested), inv(rejected), inv(chainhash.Hash{0x01})})
	sm.admitTxInvs(p2, state2, []*wire.InvVect{inv(shared), inv(chainhash.Hash{0x02})})

	sm.lookupInvBatch(<-sm.invQueue)

	require.Equal(t, int64(1), store.batches.Load())
	require.Equal(t, int64(3), store.batchSize.Load(), "shared once, plus one own hash per peer")
	require.Equal(t, 2, state.requestQueue.Length(), "the first peer asks for shared and its own")
	require.Equal(t, 1, state2.requestQueue.Length(), "the second peer asks for its own only")

	_, marked := sm.requestedTxns.Get(shared)
	require.True(t, marked, "a hash read stays marked for the request-set expiry")
}

// TestLookupInvBatch_UnmarksOnAStoreError pins that a failed read leaves no
// hash marked, so the next announcement of it is read again, and that the
// failure is counted by hashes.
func TestLookupInvBatch_UnmarksOnAStoreError(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	store := newLookupStore(t, tSettings, "inv_queue_fail")
	sm, p, state, _ := newInvHarness(t, tSettings, store, blockchain2.FSMStateRUNNING)
	before := droppedInvs("lookup_failed")

	store.fail = errors.NewProcessingError("store down")
	sm.admitTxInvs(p, state, txInvs(5, 0x01))

	require.Empty(t, sm.lookupInvBatch(<-sm.invQueue))
	require.Zero(t, sm.requestedTxns.Len(), "a failed read leaves nothing marked")
	require.Zero(t, state.requestQueue.Length())
	require.Zero(t, sm.invQueued.Load())
	require.Equal(t, before+5, droppedInvs("lookup_failed"))

	store.fail = nil
	sm.admitTxInvs(p, state, txInvs(5, 0x01))
	sm.lookupInvBatch(<-sm.invQueue)

	require.Equal(t, 5, state.requestQueue.Length(), "the next announcement is read again")
}

// TestLookupInvBatch_SkipsAPeerThatLeftOrReconnected pins the liveness check:
// a chunk whose peer has gone, or has reconnected with a new state, is
// neither read nor marked, and still gives the node count back.
func TestLookupInvBatch_SkipsAPeerThatLeftOrReconnected(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	store := newLookupStore(t, tSettings, "inv_queue_gone")
	sm, p, state, _ := newInvHarness(t, tSettings, store, blockchain2.FSMStateRUNNING)

	sm.admitTxInvs(p, state, txInvs(3, 0x01))
	sm.peerStates.Delete(p)

	require.Empty(t, sm.lookupInvBatch(<-sm.invQueue))
	require.Zero(t, store.batches.Load())
	require.Zero(t, sm.requestedTxns.Len())
	require.Zero(t, sm.invQueued.Load(), "a gone peer's chunk still gives the node count back")

	// Reconnected: the same peer key, a new state.
	fresh := &peerSyncState{requestQueue: txmap.NewSyncedSlice[wire.InvVect](wire.MaxInvPerMsg)}
	sm.peerStates.Set(p, fresh)
	sm.admitTxInvs(p, state, txInvs(3, 0x02)) // queued against the old state

	require.Empty(t, sm.lookupInvBatch(<-sm.invQueue))
	require.Zero(t, store.batches.Load())
	require.Zero(t, fresh.requestQueue.Length())
	require.Zero(t, sm.invQueued.Load())
}

// TestTakeInvBatch_StopsAtAFullBatchOrAnEmptyQueue pins the coalescing rule:
// take chunks without waiting until the batch holds a full chunk's worth of
// hashes or the queue is empty.
func TestTakeInvBatch_StopsAtAFullBatchOrAnEmptyQueue(t *testing.T) {
	sm, p, state, _ := newInvHarness(t, test.CreateBaseTestSettings(t), nil, blockchain2.FSMStateRUNNING)

	sm.admitTxInvs(p, state, txInvs(txInvLookupChunk+10, 0x01)) // one full chunk and one of 10

	require.Len(t, sm.takeInvBatch(<-sm.invQueue), 1, "a full first chunk takes nothing more")
	require.Len(t, sm.takeInvBatch(<-sm.invQueue), 1, "an empty queue ends the batch")
}

// TestLookupInvBatch_ReadsInSlicesOfOneChunk pins the read bound: a worker's
// batch may coalesce to nearly two chunks, but each store read is at most one
// chunk, because an Aerospike server refuses a batch request over its
// batch-max-requests, 5,000 by default, and that refusal would drop every
// hash in the batch under exactly the load that makes a batch coalesce.
func TestLookupInvBatch_ReadsInSlicesOfOneChunk(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	store := newLookupStore(t, tSettings, "inv_queue_slices")
	sm, p, state, _ := newInvHarness(t, tSettings, store, blockchain2.FSMStateRUNNING)

	sm.admitTxInvs(p, state, txInvs(txInvLookupChunk-1, 0x01)) // one chunk short of full
	sm.admitTxInvs(p, state, txInvs(txInvLookupChunk, 0x02))   // one full chunk

	touched := sm.lookupInvBatch(<-sm.invQueue)

	require.Len(t, touched, 2, "both chunks are in one batch")
	require.Equal(t, int64(2), store.batches.Load(), "a batch over one chunk is read in two slices")
	require.LessOrEqual(t, store.batchSize.Load(), int64(txInvLookupChunk), "no store read is larger than one chunk")
	require.Equal(t, 2*txInvLookupChunk-1, state.requestQueue.Length(), "every hash is still asked for")
	require.Zero(t, sm.invQueued.Load())
}
