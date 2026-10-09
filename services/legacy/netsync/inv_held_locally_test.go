package netsync

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// The inv path at the tip is the only block discovery a node has above the last
// checkpoint, and until this file's tests it asked two of the four questions the
// wanted-range pass asks before fetching a block: parked, and being validated. Not
// "bytes arriving now" and not "being converted". A multi-gigabyte block still
// streaming from one peer at 61 seconds, past the ledger's retry window, was asked
// of every peer that announced it, and each copy streamed whole into a side file
// and was drained. blockHeldLocally is the one answer all three inv-path callers
// now share with the wanted-range pass.

// locatorSpyClient counts GetBlockLocator calls and delegates to the real client,
// the way nilMetaClient wraps it in pipeline_parent_height_test.go, so the
// failure the real client produces for a hash the chain does not hold is what the
// inv path sees.
type locatorSpyClient struct {
	blockchain2.ClientI

	calls atomic.Int32
}

func (c *locatorSpyClient) GetBlockLocator(ctx context.Context, h *chainhash.Hash, height uint32) ([]*chainhash.Hash, error) {
	c.calls.Add(1)

	return c.ClientI.GetBlockLocator(ctx, h, height)
}

// newInvPathManager is newPipelineParkManager (real sqlitememory LocalClient, real
// park over a memory blob store) with the fields handleInvMsg dereferences,
// headers-first OFF, and the announcer elected sync peer so handleInvMsg's
// peer != sp && !current() return is not taken.
//
// The ledger is real: without one Add returns false, the drain logs "ledger full"
// and holds the item, and every negative assertion in this file would pass
// without the code under test. Each negative test therefore carries a positive
// control, a second genuinely unknown hash in the same inv that must be asked for.
func newInvPathManager(t *testing.T) (*SyncManager, *peerpkg.Peer, *peerSyncState, *peerMsgRecorder, *locatorSpyClient) {
	t.Helper()

	// AdoptWritten's setGauges reaches the gauges the real constructor registers.
	initPrometheusMetrics()

	sm := newPipelineParkManager(t, memory.New(), 8)
	spy := &locatorSpyClient{ClientI: sm.blockchainClient}
	sm.blockchainClient = spy
	sm.peerStates = txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState]()
	sm.blockDownloads = newBlockDownloadTracker(blockRequestAssignmentTTL)
	sm.streams = newStreamRegistry()
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](10)
	sm.headersFirstMode.Store(false)

	announcer, _, rec := connectRecordingPeer(t, 72, 1000)
	state := registerInvPeer(sm, announcer, 1000)
	sm.storeSyncPeer(announcer, &syncPeerState{})

	return sm, announcer, state, rec, spy
}

// requireOnlyControlAsked is the shared end state of every negative inv test: the
// control was asked for, so the fixture provably sends getdata, and the held
// block was not, neither in the ledger nor on the wire. The two hashes travel in
// one getdata, so once the control has arrived the held block's absence from the
// same recorder is a fact and not a timeout.
func requireOnlyControlAsked(t *testing.T, sm *SyncManager, rec *peerMsgRecorder, held, control chainhash.Hash) {
	t.Helper()

	require.True(t, WaitUntil(func() bool { return rec.askedForSince(0, control) }, 5*time.Second),
		"positive control: the unknown block in the same inv must be asked for")
	require.True(t, sm.blockDownloads.RequestedWithin(control, time.Minute))

	require.False(t, rec.askedForSince(0, held), "a block this node already holds or is taking in must not be asked for")
	require.False(t, sm.blockDownloads.RequestedWithin(held, time.Minute))
}

func TestHaveInventory_ABlockArrivingNowIsHeld(t *testing.T) {
	sm, _, _, _, _ := newInvPathManager(t)

	hash := chainhash.Hash{0x71}
	inv := &wire.InvVect{Type: wire.InvTypeBlock, Hash: hash}

	held, err := sm.haveInventory(inv)
	require.NoError(t, err)
	require.False(t, held, "precondition: the block is in no park and not in the chain")

	sm.streams.start(hash, 100, newTestPeer(t, "10.0.9.1:8333"), 4<<30, time.Now())

	held, err = sm.haveInventory(inv)
	require.NoError(t, err)
	require.True(t, held, "a block whose bytes are arriving is held; asking for it again downloads it twice")
}

func TestHaveInventory_ABlockBeingConvertedIsHeld(t *testing.T) {
	sm, _, _, _, _ := newInvPathManager(t)

	hash := chainhash.Hash{0x72}
	inv := &wire.InvVect{Type: wire.InvTypeBlock, Hash: hash}

	held, err := sm.haveInventory(inv)
	require.NoError(t, err)
	require.False(t, held, "precondition: the block is in no park and not in the chain")

	sm.inFlightBlocksMu.Lock()
	sm.inFlightBlocks = map[chainhash.Hash]*inFlightBlock{hash: {}}
	sm.inFlightBlocksMu.Unlock()

	held, err = sm.haveInventory(inv)
	require.NoError(t, err)
	require.True(t, held, "a block being converted is held; asking for it again downloads it twice")
}

// A block streaming from a second peer is announced by the sync peer. The getdata
// that follows must name the control and not the arriving block.
func TestHandleInvMsg_ABlockArrivingNowIsNotAskedForAgain(t *testing.T) {
	sm, announcer, _, rec, _ := newInvPathManager(t)

	arriving := chainhash.Hash{0x73}
	control := chainhash.Hash{0x74}

	sm.streams.start(arriving, 100, newTestPeer(t, "10.0.9.2:8333"), 4<<30, time.Now())

	sm.handleInvMsg(&invMsg{inv: blockInv(arriving, control), peer: announcer})

	requireOnlyControlAsked(t, sm, rec, arriving, control)
}

// The drain is the one caller that does not go through processInvMsg's own check,
// and a stream can start between the Append and the drain. A held item is consumed,
// not left to block the queue behind it.
func TestDrainRequestQueue_SkipsABlockArrivingNow(t *testing.T) {
	sm, announcer, state, _, _ := newInvPathManager(t)

	arriving := chainhash.Hash{0x75}
	control := chainhash.Hash{0x76}

	state.requestQueue.Append(wire.NewInvVect(wire.InvTypeBlock, &arriving))
	state.requestQueue.Append(wire.NewInvVect(wire.InvTypeBlock, &control))

	sm.streams.start(arriving, 100, newTestPeer(t, "10.0.9.3:8333"), 4<<30, time.Now())

	gd := sm.drainRequestQueue(announcer, state)

	require.Len(t, gd.InvList, 1)
	require.Equal(t, control, gd.InvList[0].Hash)
	require.Zero(t, state.requestQueue.Length(), "the held item is consumed, not left to block the queue")
	require.False(t, sm.blockDownloads.RequestedWithin(arriving, time.Minute))
	require.True(t, sm.blockDownloads.RequestedWithin(control, time.Minute))
}

// A block asked of a peer more than a retry window ago, whose owner has sent no
// byte of it because it is still delivering an earlier block, is in none of the
// in-memory states and outside the ledger's retry window. The wanted-range pass
// has guarded that since the 447 MB block at height 705,000 (wanted_range_assign.go,
// AnyOwner over lastBlockBytes); the drain now does the same.
//
// The ledger record is aged through the tracker's own clock, not ForgiveOwners:
// a forgiven owner is not an active one, and a fresh record would make the
// RequestedWithin guard above this one consume the item before the busy-owner
// guard ever ran, which is the vacuous shape this test must not have.
func TestDrainRequestQueue_SkipsABlockABusyOwnerStillOwes(t *testing.T) {
	sm, announcer, state, _, _ := newInvPathManager(t)

	owed := chainhash.Hash{0x77}
	control := chainhash.Hash{0x78}
	earlier := chainhash.Hash{0x79}
	busy := newTestPeer(t, "10.0.9.4:8333")

	sm.blockDownloads.now = func() time.Time { return time.Now().Add(-2 * blockRequestRetryInterval) }
	require.True(t, sm.blockDownloads.Add(busy, owed))
	sm.blockDownloads.now = time.Now

	require.False(t, sm.blockDownloads.RequestedWithin(owed, blockRequestRetryInterval),
		"precondition: the request is older than the retry window, so the ledger alone would re-ask")

	// busy finished an earlier block a moment ago: it is delivering, not quiet.
	s := sm.streams.start(earlier, 99, busy, 1<<20, time.Now().Add(-time.Second))
	sm.streams.finish(s, time.Now(), true)

	state.requestQueue.Append(wire.NewInvVect(wire.InvTypeBlock, &owed))
	state.requestQueue.Append(wire.NewInvVect(wire.InvTypeBlock, &control))

	gd := sm.drainRequestQueue(announcer, state)

	require.Len(t, gd.InvList, 1)
	require.Equal(t, control, gd.InvList[0].Hash)
	require.Zero(t, state.requestQueue.Length(), "the owed item is consumed, not left to block the queue")
	require.False(t, sm.blockDownloads.RequestedWithin(owed, blockRequestRetryInterval), "nothing asked for the owed block again")
	require.True(t, sm.blockDownloads.RequestedWithin(control, time.Minute))
}

// A parked block announced last in an inv used to reach the getblocks branch,
// which anchors its locator on the announced block; the real client fails to build
// a locator for a hash the chain does not hold, and that logged an Error per inv
// at the tip. A held block now returns before that branch. The control proves the
// branch still runs for a block the chain does hold.
func TestHandleInvMsg_AHeldBlockLastInAnInvSendsNoGetblocks(t *testing.T) {
	sm, announcer, _, rec, spy := newInvPathManager(t)

	parked := adoptRecord(t, sm.blockPark, chainhash.Hash{0xaa}, 1)
	require.True(t, sm.blockPark.Has(parked))

	sm.handleInvMsg(&invMsg{inv: blockInv(parked), peer: announcer})

	require.Zero(t, spy.calls.Load(), "a held block must not have a locator built on it")
	require.False(t, WaitUntil(func() bool { return rec.getBlocksCount() > 0 }, invQuietPeriod),
		"a held block last in an inv sends no getblocks")
	require.False(t, sm.blockDownloads.RequestedWithin(parked, time.Minute))

	genesis := *sm.chainParams.GenesisHash

	sm.handleInvMsg(&invMsg{inv: blockInv(genesis), peer: announcer})

	require.Equal(t, int32(1), spy.calls.Load(), "positive control: a chain-held last block still reaches the getblocks branch")
	require.True(t, WaitUntil(func() bool { return rec.getBlocksCount() == 1 }, 5*time.Second),
		"positive control: the getblocks branch still sends for a chain-held last block")
}

// The sink has returned, so nothing is arriving or converting; the consumer has not
// yet run handleBlockOnDiskMsg, so nothing is parked. That window is on every
// block, and handleBlockOnDiskMsg's own ForgiveOwners at intake back-dates the
// ledger so RequestedWithin no longer suppresses either. Only the record on disk
// says the block is here, and holdsBlock is what reads it.
func TestHandleInvMsg_AConvertedBlockNotYetAdoptedIsNotAskedFor(t *testing.T) {
	sm, announcer, _, rec, _ := newInvPathManager(t)

	blk := wireBlockWithTxs(t, 6, false)
	pipelineHeaderFixture(t, sm, blk)

	hash := blk.MsgBlock().BlockHash()
	header := blk.MsgBlock().Header
	body := blockBodyBytes(t, blk)
	control := chainhash.Hash{0x7a}

	converted, err := sm.pipelineBlockSink(hash, &header, bytes.NewReader(body), sinkPayloadLen(body))
	require.NoError(t, err)
	require.True(t, converted)

	require.False(t, sm.blockPark.Has(hash), "precondition: the consumer has not adopted it")
	require.False(t, sm.streams.arriving(hash), "precondition: the stream is over")
	require.False(t, sm.conversionInFlight(hash), "precondition: the admission is released")
	require.True(t, sm.holdsBlock(sm.ctx, hash), "precondition: the complete record is on disk")
	require.False(t, sm.blockDownloads.RequestedWithin(hash, blockRequestRetryInterval),
		"precondition: the ledger does not suppress a re-ask, so only holdsBlock can")
	require.False(t, sm.blockPark.adoptStranded(sm.ctx, hash, sm.subtreeStore),
		"precondition: a record written a moment ago is in its hand-off, not stranded")

	sm.handleInvMsg(&invMsg{inv: blockInv(hash, control), peer: announcer})

	requireOnlyControlAsked(t, sm, rec, hash, control)
	require.False(t, sm.blockPark.Has(hash), "the hand-off gap is covered by holdsBlock alone, not by an adoption under the consumer")
}

// A complete record on disk that nothing announced: the peer that streamed it
// dropped between the sink writing it and the on-disk message reaching the
// consumer (adoptStranded's doc; mainnet stopped at 650,021 on 2026-09-23 that
// way). Above the checkpoint the wanted-range pass never visits the height, so the
// inv path is the only thing that does. Swallowing the inv because the block is
// held would leave the record unadopted until a restart; the inv path adopts it
// as the wanted-range pass does, and still asks nobody for it.
//
// On the file-store harness because adoptStranded stats the record file in the
// park's directory, which the memory fixture does not have.
func TestHandleInvMsg_AStrandedRecordIsAdoptedNotFetched(t *testing.T) {
	strand := func(t *testing.T) (*parkWiringHarness, chainhash.Hash) {
		t.Helper()

		h := newParkWiringHarness(t, true)
		h.sm.headersFirstMode.Store(false)

		state, ok := h.sm.peerStates.Get(h.peer)
		require.True(t, ok)

		state.requestQueue = txmap.NewSyncedSlice[wire.InvVect](maxRequestedBlocks)

		// The sink writes the record; the on-disk message never reaches the
		// consumer, so handleBlockOnDiskMsg is deliberately not called.
		msgBlock := h.blocks[1].MsgBlock()
		hash := msgBlock.BlockHash()
		body := blockBodyBytes(t, bsvutil.NewBlock(msgBlock))

		converted, err := h.sm.pipelineBlockSink(hash, &msgBlock.Header, bytes.NewReader(body), sinkPayloadLen(body))
		require.NoError(t, err)
		require.True(t, converted)

		require.False(t, h.sm.blockPark.Has(hash), "precondition: nothing adopted the record")
		require.True(t, h.sm.holdsBlock(h.sm.ctx, hash), "precondition: the record is complete on disk")

		return h, hash
	}

	t.Run("aged past strandedRecordAge: adopted, not asked for", func(t *testing.T) {
		h, hash := strand(t)

		recordPath := filepath.Join(h.parkDir, hash.String()+"."+string(fileformat.FileTypeBlock))
		old := time.Now().Add(-2 * strandedRecordAge)
		require.NoError(t, os.Chtimes(recordPath, old, old))

		h.sm.handleInvMsg(&invMsg{inv: blockInv(hash), peer: h.peer})

		require.True(t, h.sm.blockPark.Has(hash), "a stranded complete record is adopted on the inv path")
		require.False(t, h.sm.blockDownloads.RequestedWithin(hash, time.Minute), "a block already on disk is not downloaded again")
		require.False(t, WaitUntil(func() bool { return h.rec.getDataCount() > 0 }, invQuietPeriod))
	})

	t.Run("fresh: in its hand-off, neither adopted nor asked for", func(t *testing.T) {
		h, hash := strand(t)

		h.sm.handleInvMsg(&invMsg{inv: blockInv(hash), peer: h.peer})

		require.False(t, h.sm.blockPark.Has(hash), "a record written a moment ago is the consumer's to adopt")
		require.False(t, h.sm.blockDownloads.RequestedWithin(hash, time.Minute))
		require.False(t, WaitUntil(func() bool { return h.rec.getDataCount() > 0 }, invQuietPeriod))
	})
}
