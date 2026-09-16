package netsync

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// TestHandleBlockOnDiskMsg_ReleasesTheDownloadAssignment is the regression test
// for the ownership leak: the streamed route never called RemoveOwner or
// ForgiveOwners, the same pair handleBlockMsg calls the moment it dequeues a
// decoded block (manager.go, around handleBlockMsg's "This peer has answered"
// comment).
//
// Before this branch made the pipeline send every block down the on-disk route,
// that gap only bit blocks above the 64 MiB decode threshold, rare enough that
// nobody noticed a peer's assignment count creeping up. Streaming is now
// unconditional whenever the park is enabled, so every block takes this route,
// and a peer that delivers sixteen blocks (the default MaxBlocksInTransitPerPeer)
// has its CountForPeer stick there forever — the scheduler's per-peer budget
// (block_scheduler.go:144) reaches zero and stops asking that peer for anything
// else until the hour-long assignment TTL expires.
//
// This drives three blocks through handleBlockOnDiskMsg for one peer and
// requires CountForPeer to return to zero, exactly as it does for the decoded
// route today.
func TestHandleBlockOnDiskMsg_ReleasesTheDownloadAssignment(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.sm.drainAsync.Store(true)
	h.sm.parkCommits = make(chan parkCommit, 4)

	// A parent already in the harness's header list, so every delivered block
	// is reachable and handleBlockOnDiskMsg does not discard it before reaching
	// the code under test.
	parent := h.blocks[1].MsgBlock().BlockHash()

	mkBody := func(nonce uint32) peerpkg.BlockBody {
		header := wire.BlockHeader{Version: 1, PrevBlock: parent, Nonce: nonce}
		return peerpkg.BlockBody{Header: header, TxCount: 1, Size: 4096, Hash: header.BlockHash()}
	}

	bodies := []peerpkg.BlockBody{mkBody(1), mkBody(2), mkBody(3)}

	for _, b := range bodies {
		require.True(t, h.sm.blockDownloads.Add(h.peer, b.Hash),
			"seed one assignment per block, exactly what the download walk does before asking a peer for it")
	}

	require.Equal(t, 3, h.sm.blockDownloads.CountForPeer(h.peer),
		"sanity: three assignments outstanding before any delivery")

	for _, b := range bodies {
		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: b, peer: h.peer})
	}

	require.Equal(t, 0, h.sm.blockDownloads.CountForPeer(h.peer),
		"every delivered block must release its assignment, or CountForPeer sticks at MaxBlocksInTransitPerPeer and the scheduler stops asking this peer for anything else")
}

// TestHandleBlockOnDiskMsg_ReleasesTheAssociationPrimarysAssignment is the
// regression test for fix-round item 2. A BlockPriority association routes a
// block's body to its own DATA1/DATA2 stream sub-peer, so msg.peer here is
// that sub-peer, never the primary the download ledger actually records
// ownership under (manager.go:3271-3276's resolve before RemoveOwner, and
// BlockRequested's identical resolve at manager.go:6530-6538). Passing
// msg.peer straight through to RemoveOwner made it a silent no-op under a
// multistream association: sub-peers are never registered in peerStates and
// so never own anything in blockDownloads. It stayed invisible because
// ForgiveOwners is peer-agnostic and released every OTHER peer's assignment
// on the same hash regardless — only this peer's own CountForPeer stuck.
func TestHandleBlockOnDiskMsg_ReleasesTheAssociationPrimarysAssignment(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.sm.drainAsync.Store(true)
	h.sm.parkCommits = make(chan parkCommit, 4)

	// h.peer is already registered as a primary (newParkWiringHarness's own
	// setup, via registerRacePeer). subPeer is a stream sub-peer associated
	// with it — exactly what a BlockPriority association's body stream
	// delivers as msg.peer, and never itself registered in peerStates.
	subPeer := &peerpkg.Peer{}
	subPeer.SetAssociation(peerpkg.NewAssociation([]byte{0x01}, h.peer))

	parent := h.blocks[1].MsgBlock().BlockHash()
	header := wire.BlockHeader{Version: 1, PrevBlock: parent, Nonce: 99}
	body := peerpkg.BlockBody{Header: header, TxCount: 1, Size: 4096, Hash: header.BlockHash()}

	require.True(t, h.sm.blockDownloads.Add(h.peer, body.Hash),
		"the ledger records ownership under the primary, exactly as the download walk does")

	h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: subPeer})

	// HasOwner, not just CountForPeer: ForgiveOwners is peer-agnostic and marks
	// every owner of this hash forgiven regardless of which peer RemoveOwner was
	// actually called with, and a forgiven-but-not-removed record still reads as
	// zero in CountForPeer (its own "forgiven" check) — so CountForPeer alone
	// passes even when RemoveOwner silently no-ops on the wrong (sub-peer)
	// identity, exactly the false-green fix round 1 caught. HasOwner does NOT
	// consult "forgiven" (see its own doc comment): only RemoveOwner actually
	// deleting the primary's record makes this false.
	require.False(t, h.sm.blockDownloads.HasOwner(h.peer, body.Hash),
		"RemoveOwner must actually delete the primary's record; a call keyed by the sub-peer instead leaves it in place and this would still (wrongly) read true")

	require.Equal(t, 0, h.sm.blockDownloads.CountForPeer(h.peer),
		"the delivery arrived via the association's sub-peer, so the release must resolve to the primary the ledger recorded ownership under, not stay a no-op keyed by the sub-peer identity")
}

// TestPipelineOnDiskRoute_AdmissionBoundsInFlightConversions is the regression
// test for the missing admission control. AcquireBlockPrefetch is reached only
// from OnBlock, which the peer dispatches only for a whole-block message; with
// the pipeline on every block comes back as *peer.MsgBlockOnDisk instead, so
// OnBlock — and the admission check inside it — never runs, and the pipeline
// path has no bound on how many blocks convert concurrently.
//
// This occupies the admission budget's only slot directly (simulating another
// peer's conversion already in flight), then drives a second, real, convertible
// block through the sink actually installed for the wire layer and requires that
// it stays blocked until the held slot is released.
func TestPipelineOnDiskRoute_AdmissionBoundsInFlightConversions(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	sm.blockPrefetchBudgetBytes = 1
	sm.blockPrefetchBudget = semaphore.NewWeighted(1)
	sm.inFlightBlocks = make(map[chainhash.Hash]*inFlightBlock)

	var installedSink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error)

	sm.installStreamingBlockPath(func(
		sink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error),
		gate func(chainhash.Hash, *wire.BlockHeader) error,
		del func(chainhash.Hash, bool) error,
	) {
		installedSink = sink
	})
	require.NotNil(t, installedSink, "the park is enabled, so a sink must have been installed")

	// Occupy the only slot, simulating a first block another peer's read loop
	// is already converting.
	heldHash := chainhash.Hash{0x01}
	weight, err := sm.AcquireBlockPrefetch(context.Background(), nil, heldHash, 999)
	require.NoError(t, err, "occupying the only slot must succeed before the second block can be shown to wait on it")

	// A second, distinct, well-formed block. Distinct transaction count from
	// any other fixture in this package: wireBlockWithTxs is deterministic and
	// the wire header's one-second timestamp resolution means two blocks built
	// moments apart with the same transaction count hash identically.
	blk := wireBlockWithTxs(t, 7, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)
	header := &blk.MsgBlock().Header
	hash := *blk.Hash()

	var (
		converted bool
		sinkErr   error
	)

	done := make(chan struct{})

	go func() {
		converted, sinkErr = installedSink(hash, header, bytes.NewReader(body), int64(len(body)))
		close(done)
	}()

	select {
	case <-done:
		t.Fatalf("the on-disk route converted a second block while the admission budget's only slot was already held (converted=%v err=%v) — admission control is not reachable from this path", converted, sinkErr)
	case <-time.After(200 * time.Millisecond):
		// Expected: still blocked on the budget.
	}

	sm.ReleaseBlockPrefetch(heldHash, weight)

	require.True(t, WaitUntil(func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 2*time.Second), "releasing the held slot must let the second block's conversion proceed")

	require.NoError(t, sinkErr, "the second block is well-formed and must convert cleanly once admitted")
	require.True(t, converted, "a well-formed block below the checkpoint must convert, not merely be accepted")
}

// TestAdmitPipelineSink_FallsBackWhenAcquireTimesOut is the regression test for
// fix-round item 1: a park in AcquireBlockPrefetch runs INSIDE readMessageStreaming
// (services/legacy/peer/peer.go), and peer.inHandler only stops the peer's idle
// timer AFTER that call returns, so an unbounded park here could trip that timer
// and disconnect a perfectly healthy peer over this node's own admission
// backpressure. admitPipelineSink now bounds the acquire strictly below
// legacy_peerIdleTimeout and falls back to the plain body-write path rather
// than erroring (an error here would disconnect the peer just as surely as the
// idle timer would) or parking further.
//
// legacy_peerIdleTimeout is set small so the bound (half of it) is reached in
// well under a second rather than the real default's ~62.5s, keeping this test
// fast without weakening what it proves: the held slot is never released, so
// the only way the second call can return is by falling back.
func TestAdmitPipelineSink_FallsBackWhenAcquireTimesOut(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.settings.Legacy.PeerIdleTimeout = 200 * time.Millisecond

	sm.blockPrefetchBudgetBytes = 1
	sm.blockPrefetchBudget = semaphore.NewWeighted(1)
	sm.inFlightBlocks = make(map[chainhash.Hash]*inFlightBlock)

	var installedSink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error)

	sm.installStreamingBlockPath(func(
		sink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error),
		gate func(chainhash.Hash, *wire.BlockHeader) error,
		del func(chainhash.Hash, bool) error,
	) {
		installedSink = sink
	})
	require.NotNil(t, installedSink)

	// Occupy the only slot and never release it: the second call's acquire has
	// no way to succeed within its bound, which is the point.
	heldHash := chainhash.Hash{0x03}
	_, err := sm.AcquireBlockPrefetch(context.Background(), nil, heldHash, 1)
	require.NoError(t, err, "occupying the only slot must succeed before the timeout can be shown to fire")

	blk := wireBlockWithTxs(t, 11, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)
	header := &blk.MsgBlock().Header
	hash := *blk.Hash()

	var (
		converted bool
		sinkErr   error
	)

	done := make(chan struct{})

	go func() {
		converted, sinkErr = installedSink(hash, header, bytes.NewReader(body), int64(len(body)))
		close(done)
	}()

	select {
	case <-done:
		// Expected: the bound (100ms, half of PeerIdleTimeout) fires well
		// before this 2s ceiling.
	case <-time.After(2 * time.Second):
		t.Fatal("the acquire must fall back once its bound expires, not park indefinitely on a slot that is never released")
	}

	require.NoError(t, sinkErr, "a timed-out acquire must fall back to the plain body-write path, not return an error that would cost the peer its connection")
	require.False(t, converted, "the fallback path never converts, it only writes the whole body")

	exists, err := sm.blockPark.store.Exists(context.Background(), hash[:], parkFileType)
	require.NoError(t, err)
	require.True(t, exists, "the fallback must have actually written the whole body via streamingBlockSink, not silently dropped it")
}
