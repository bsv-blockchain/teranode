package netsync

import (
	"bytes"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
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

// admitTxInvs is the two production halves of admitting announcements run
// back to back, as for a registered peer: chargeTxInvs on the read loop, then
// queueTxChunks on the handler goroutine.
func (sm *SyncManager) admitTxInvs(peer *peerpkg.Peer, state *peerSyncState, txInvs []*wire.InvVect) {
	sm.queueTxChunks(peer, state, sm.chargeTxInvs(peer, txInvs))
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

// TestNewInvMsg_ChargesTheNodeCapBeforeAHandlerRuns pins where the bound
// starts: on the read loop, before the message waits in msgChan or a handler
// goroutine waits on the blockchain. The message keeps only its block items,
// the transaction items are charged to the node cap as they are cut into
// chunks, and a chunk over the cap is dropped there and counted.
func TestNewInvMsg_ChargesTheNodeCapBeforeAHandlerRuns(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.InvQueueMaxHashes = txInvLookupChunk

	sm, p, _, _ := newInvHarness(t, tSettings, nil, blockchain2.FSMStateRUNNING)
	before := droppedInvs(invDropNodeCap)

	msg := wire.NewMsgInv()
	require.NoError(t, msg.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &chainhash.Hash{0xb1})))

	for _, iv := range txInvs(2*txInvLookupChunk, 0x01) {
		require.NoError(t, msg.AddInvVect(iv))
	}

	got := sm.newInvMsg(msg, p)

	require.Len(t, got.inv.InvList, 1, "only the block item stays in the message")
	require.Len(t, got.txChunks, 1, "the chunk over the cap is not kept")
	require.Equal(t, int64(txInvLookupChunk), sm.invQueued.Load(), "the kept chunk is charged before any handler runs")
	require.Equal(t, before+float64(txInvLookupChunk), droppedInvs(invDropNodeCap))
	require.Empty(t, sm.invQueue, "nothing is queued for a lookup before the handler resolves the peer")
}

// TestNewInvMsg_KeepsNothingOfTheDecodedMessage pins the end state of a
// partly admitted message: the decoder puts every item in one backing array,
// and once newInvMsg returns, nothing it built may keep that array alive,
// including when one chunk was admitted and the next dropped. The decoded
// array carries a finalizer, and the test waits for the collector to run it
// while the message newInvMsg built is still in use.
func TestNewInvMsg_KeepsNothingOfTheDecodedMessage(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.InvQueueMaxHashes = txInvLookupChunk

	sm, p, _, _ := newInvHarness(t, tSettings, nil, blockchain2.FSMStateRUNNING)

	var freed atomic.Bool

	got := func() *invMsg {
		src := wire.NewMsgInv()
		require.NoError(t, src.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &chainhash.Hash{0xb1})))

		for _, iv := range txInvs(2*txInvLookupChunk, 0x02) {
			require.NoError(t, src.AddInvVect(iv))
		}

		var buf bytes.Buffer
		require.NoError(t, src.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding))

		decoded := wire.NewMsgInv()
		require.NoError(t, decoded.Bsvdecode(bytes.NewReader(buf.Bytes()), wire.ProtocolVersion, wire.BaseEncoding))

		// The first item is the start of the decoder's one backing array.
		runtime.SetFinalizer(decoded.InvList[0], func(*wire.InvVect) { freed.Store(true) })

		return sm.newInvMsg(decoded, p)
	}()

	require.Eventually(t, func() bool {
		runtime.GC()

		return freed.Load()
	}, 5*time.Second, 10*time.Millisecond, "the decoded message is still reachable from what newInvMsg built")

	require.Len(t, got.inv.InvList, 1)
	require.Equal(t, chainhash.Hash{0xb1}, got.inv.InvList[0].Hash)
	require.Len(t, got.txChunks, 1, "one chunk admitted, one dropped at the cap")
	require.Len(t, got.txChunks[0].items, txInvLookupChunk)
	require.Equal(t, int64(txInvLookupChunk), sm.invQueued.Load(), "the counters charge what is held, no more")
}

// TestDispatchInvMsg_BoundsHandlersWaitingOnTheBlockchain reproduces the
// accumulation the review of PR 2045 found: each inv message gets a handler
// goroutine, and while the blockchain service is slow every handler waits in
// it. Here the FSM lookup is held. A message carrying only transaction items
// gets a handler only while fewer than invHandlerSlots are running; past that
// it is dropped, counted, and its charge given back. A message carrying a
// block item still gets one with every slot taken. Released, the waiting
// handlers queue their chunks and give their slots back.
func TestDispatchInvMsg_BoundsHandlersWaitingOnTheBlockchain(t *testing.T) {
	sm, p, state, _ := newInvHarness(t, test.CreateBaseTestSettings(t), nil, blockchain2.FSMStateRUNNING)

	var (
		gate      = make(chan struct{})
		gateOnce  sync.Once
		blockSeen atomic.Bool
		running   = blockchain2.FSMStateRUNNING
		blockHash = chainhash.Hash{0xb1}
		client    = &blockchain2.Mock{}
		openGate  = func() { gateOnce.Do(func() { close(gate) }) }
		oneTxInv  = func(i int) *wire.MsgInv {
			return &wire.MsgInv{InvList: []*wire.InvVect{inv(chainhash.Hash{byte(i), byte(i >> 8), 0x44})}}
		}
	)

	client.On("GetFSMCurrentState", mock.Anything).Run(func(mock.Arguments) { <-gate }).Return(&running, nil)
	client.On("GetBlockHeader", mock.Anything, &blockHash).
		Run(func(mock.Arguments) { blockSeen.Store(true) }).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 1}, nil)
	client.On("GetBlockLocator", mock.Anything, &blockHash, uint32(0)).
		Return(nil, errors.NewProcessingError("no locator in this test"))

	sm.blockchainClient = client

	t.Cleanup(func() {
		openGate()
		require.Eventually(t, func() bool { return sm.invHandlers.Load() == 0 }, 5*time.Second, 5*time.Millisecond)
	})

	const over = 5

	before := droppedInvs(invDropHandlersBusy)

	for i := 0; i < invHandlerSlots+over; i++ {
		sm.dispatchInvMsg(sm.newInvMsg(oneTxInv(i), p))
	}

	require.Equal(t, int64(invHandlerSlots), sm.invHandlers.Load(), "no more handlers than the slots")
	require.Equal(t, before+over, droppedInvs(invDropHandlersBusy), "the messages over the slots are dropped and counted")
	require.Equal(t, int64(invHandlerSlots), sm.invQueued.Load(), "the waiting handlers' hashes stay charged, the dropped ones do not")

	blockInv := wire.NewMsgInv()
	require.NoError(t, blockInv.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &blockHash)))
	sm.dispatchInvMsg(sm.newInvMsg(blockInv, p))

	require.Eventually(t, blockSeen.Load, 5*time.Second, 5*time.Millisecond, "a block announcement must get a handler with every slot taken")

	openGate()

	require.Eventually(t, func() bool { return sm.invHandlers.Load() == 0 }, 5*time.Second, 5*time.Millisecond, "handlers give their slots back")
	require.Len(t, sm.invQueue, invHandlerSlots, "each released handler queued its chunk")
	require.Equal(t, int64(invHandlerSlots), state.queuedInvs.Load())
}

// TestLookupInvBatch_ConcurrentWorkersClaimEachHashOnce pins the claim the
// lookup workers make with SetIfAbsent before the read: eight peers announce
// the same hashes, eight workers look the chunks up at once, and each hash is
// read once and asked of one peer only. A Get followed by a Set lets two
// workers both claim a hash, read it twice and ask two peers for it. The
// window between the two is short, so the test runs twenty rounds on fresh
// hashes; with a Get and a Set, a round shows a double claim more often than
// not, and twenty rounds all but certainly do.
func TestLookupInvBatch_ConcurrentWorkersClaimEachHashOnce(t *testing.T) {
	const peers, hashes, rounds = 8, 2000, 20

	tSettings := test.CreateBaseTestSettings(t)
	store := newLookupStore(t, tSettings, "inv_queue_concurrent_claim")
	sm, p, state, _ := newInvHarness(t, tSettings, store, blockchain2.FSMStateRUNNING)

	owners := []*peerpkg.Peer{p}
	states := []*peerSyncState{state}

	for i := 1; i < peers; i++ {
		pi, si := newHarnessPeer(t, sm)
		owners = append(owners, pi)
		states = append(states, si)
	}

	requestedSoFar := func() int {
		n := 0
		for _, s := range states {
			n += s.requestQueue.Length()
		}

		return n
	}

	for round := 0; round < rounds; round++ {
		shared := txInvs(hashes, byte(0x40+round))

		for i := range owners {
			sm.admitTxInvs(owners[i], states[i], shared)
		}

		require.Len(t, sm.invQueue, peers)

		chunks := make([]*invChunk, 0, peers)
		for i := 0; i < peers; i++ {
			chunks = append(chunks, <-sm.invQueue)
		}

		readBefore, requestedBefore := store.read.Load(), requestedSoFar()

		var (
			wg    sync.WaitGroup
			start = make(chan struct{})
		)

		for _, c := range chunks {
			wg.Add(1)

			go func(c *invChunk) {
				defer wg.Done()

				<-start
				sm.lookupInvBatch(c)
			}(c)
		}

		close(start)
		wg.Wait()

		require.Equal(t, int64(hashes), store.read.Load()-readBefore, "round %d: each hash must be read once across the workers", round)
		require.Equal(t, hashes, requestedSoFar()-requestedBefore, "round %d: each hash must be asked of one peer only", round)
		require.Zero(t, sm.invQueued.Load())
	}
}
