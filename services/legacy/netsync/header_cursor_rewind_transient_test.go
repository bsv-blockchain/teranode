package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// refusedBlock is a headers-first manager walked to the one moment that matters:
// the front block of the header list has been asked for, has arrived, and
// HandleBlockDirect has dropped it. Its header is off the front of the walk and
// its download record has been released, so nothing but a rewind can bring it
// back. Which kind of drop it was, a busy store that says nothing about the
// block or a judgement on the block itself, is the caller's choice.
type refusedBlock struct {
	sm    *SyncManager
	hash  chainhash.Hash
	block *wire.MsgBlock
	peer  *peerpkg.Peer
	// deliverErr is what handleBlockMsg handed back for that first delivery.
	// It is the value the read loop classifies with shouldDisconnectOnBlockErr,
	// so it is what decides whether the delivering peer keeps its association.
	deliverErr error
}

// newRefusedBlock builds that state with a transient store fault, the failure
// the rewind machinery was originally written for. backoffBase is how long the
// block's throttle should last, which is the difference between the tests below.
func newRefusedBlock(t *testing.T, peerIdx uint8, backoffBase time.Duration) *refusedBlock {
	t.Helper()

	return newDroppedBlock(t, peerIdx, backoffBase, errors.NewStorageError("the store is busy"))
}

// newDroppedBlock is newRefusedBlock with the failure spelled out. failWith is
// what the blockchain client returns for the front block's existence check, and
// so decides which arm of handleBlockMsg's error handling runs: a storage or
// service error is transient, a context error is our own abort, and anything
// else is a judgement on the block.
func newDroppedBlock(t *testing.T, peerIdx uint8, backoffBase time.Duration, failWith error) *refusedBlock {
	t.Helper()

	// The headers are built first, because the storage mock has to be told which
	// block it is going to refuse.
	var nonce uint32

	anchor := chainhash.Hash{0xb1}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)
	failing := hashes[0]

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	client.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{}}, nil)
	// The failure under test, on this one block only.
	client.On("GetBlockExists", mock.Anything, &failing).
		Return(false, failWith)
	client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	client.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.blockFailureBackoff = expiringmap.New[chainhash.Hash, *blockFailureState](time.Hour)
	sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](time.Hour)
	sm.settings.Legacy.BlockFailureBackoffBase = backoffBase
	sm.settings.Legacy.BlockFailureBackoffMaxDuration = time.Hour

	t.Cleanup(func() { sm.blockFailureBackoff.Stop(); sm.recentlyFailedBlocks.Stop() })

	// The round's checkpoint is the last of these headers. That is the shape a
	// real round has, and it is the only shape in which blocks are fetched at
	// all: handleHeadersMsg appends up to the checkpoint height, and the batch
	// that reaches it is the same batch that takes the previous round's anchor
	// off the front and starts the download walk. Parking the checkpoint far
	// above the list instead leaves the node in the first half of a round, where
	// the front is still an anchor already in the chain and nothing may ask for
	// a block — so the walk under test would never run in production at all.
	syncPeer, _, _ := connectRacePeer(t, peerIdx, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	sm.resetHeaderState(&anchor, 10)

	// resetHeaderState now derives the checkpoint from the height it is given,
	// so the synthetic one this test needs is put back after it.
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 15, Hash: &hashes[len(hashes)-1]}
	// resetHeaderState turns headers-first mode off; the walk under test only
	// runs in headers-first mode.
	sm.headersFirstMode.Store(true)

	// Reaching the checkpoint is what trims the anchor and sends the round's
	// first getdata, so this one call puts the node in the state the tests
	// below start from.
	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	require.Equal(t, len(hashes), sm.headerListLen(),
		"the checkpoint batch links all five headers and takes the anchor off the front")

	require.True(t, WaitUntil(func() bool { return sm.blockDownloads.RequestedWithin(failing, time.Minute) }, 5*time.Second),
		"the front block should have been asked for")

	block := &wire.MsgBlock{Header: *msg.Headers[0]}

	deliverErr := sm.handleBlockMsg(&blockQueueMsg{
		block:     block,
		blockHash: failing,
		peer:      syncPeer,
	})

	if errors.IsContextError(failWith) {
		// The context arm answers nil on purpose: our own abort says nothing
		// about the block and there is nothing to hand back to the caller.
		require.NoError(t, deliverErr, "a cancelled block is not reported as an error")
	} else {
		require.Error(t, deliverErr, "the block was dropped")
	}

	require.False(t, sm.blockDownloads.RequestedWithin(failing, time.Minute),
		"delivering the block released the peer's obligation, so nothing is outstanding for it any more")

	return &refusedBlock{sm: sm, hash: failing, block: block, peer: syncPeer, deliverErr: deliverErr}
}

// TestSyncManager_ABlockTheStoreRefusesIsAskedForAgain is the case the rewind
// machinery was built for and was not applied to.
//
// A block that fails to store with a local fault — the store is busy, a batch
// timed out — is not a bad block. But its header has already been taken off the
// front of the header list, and in headers-first mode that list is the only
// thing that fetches anything: the getblocks the drop paths send is thrown away
// by processInvMsg while headers-first mode is on. So the block left the walk
// and nothing ever asked for it again; the node sat still until the 180-second
// stall detector rotated the sync peer and rebuilt the header list from scratch.
//
// The end state asserted here is the one a stalled node never reaches: the block
// is outstanding again — and with no other block arriving and no headers message
// coming, so nothing but the rewind and the sweep's own top-up can have done it.
func TestSyncManager_ABlockTheStoreRefusesIsAskedForAgain(t *testing.T) {
	r := newRefusedBlock(t, 63, time.Millisecond)

	require.True(t, WaitUntil(func() bool {
		// What the park sweep's ticker calls, once every 30 seconds.
		r.sm.resumeHeaderWalk()

		return r.sm.blockDownloads.RequestedWithin(r.hash, time.Minute)
	}, 5*time.Second), "a block the store refused must be asked for again once its backoff has expired")
}

// TestSyncManager_ABlockInsideItsBackoffIsNotAskedForYet is the brake the rewind
// cannot ship without.
//
// The per-block backoff exists so a block that has just failed on a struggling
// store is not immediately downloaded and decorated all over again. Putting the
// walk back on the block would undo that by itself, because the very next round
// would ask for it. So the walk stops at a block still inside its backoff — and
// stops rather than skips, because skipping moves the cursor past it and loses
// the block a second time, which is the failure the rewind exists to prevent.
func TestSyncManager_ABlockInsideItsBackoffIsNotAskedForYet(t *testing.T) {
	r := newRefusedBlock(t, 64, time.Hour)

	// A full walk, synchronously: a block is recorded in the download ledger
	// before its getdata is queued, so the ledger says what this round asked for
	// without waiting on the wire.
	r.sm.fetchHeaderBlocks()
	r.sm.resumeHeaderWalk()

	require.False(t, r.sm.blockDownloads.RequestedWithin(r.hash, time.Minute),
		"a block still inside its transient-failure backoff must not be asked for again yet")
}

// TestSyncManager_ABlockReDeliveredInsideItsBackoffKeepsItsPlaceInTheWalk
// closes the loop the previous two tests open.
//
// Once the walk is back on a block the store refused, a second copy can still
// arrive — a peer racing the frontier, or a getdata answered twice — and it
// arrives while the block is still inside its backoff. That copy is skipped
// without being processed, which is right; what is not right is that skipping it
// also takes its header off the front of the walk. Undoing the rewind on the
// strength of a duplicate delivery leaves the block in exactly the state the
// rewind was there to prevent: downloaded, dropped, and never asked for again.
func TestSyncManager_ABlockReDeliveredInsideItsBackoffKeepsItsPlaceInTheWalk(t *testing.T) {
	// An hour-long backoff, so the duplicate below is certainly inside it and
	// the test is not racing a clock.
	r := newRefusedBlock(t, 65, time.Hour)

	fs, ok := r.sm.blockFailureBackoff.Get(r.hash)
	require.True(t, ok, "the refusal should have started a backoff")
	require.True(t, fs.nextRetry.After(time.Now()), "the backoff should still be running")

	// A second peer was asked for the same block while the sync peer was slow on
	// it — that is what the frontier race does — so a second copy is still owed
	// and still on its way.
	r.sm.blockDownloads.Add(r.peer, r.hash)

	// It turns up while the backoff is running.
	require.Error(t, r.sm.handleBlockMsg(&blockQueueMsg{
		block:     r.block,
		blockHash: r.hash,
		peer:      r.peer,
	}), "a block inside its backoff is skipped rather than processed")

	// The store recovers and the throttle lifts.
	r.sm.blockFailureBackoff.Delete(r.hash)

	require.True(t, WaitUntil(func() bool {
		r.sm.resumeHeaderWalk()

		return r.sm.blockDownloads.RequestedWithin(r.hash, time.Minute)
	}, 5*time.Second), "a duplicate delivery must not cost the block its place in the download walk")
}

// TestSyncManager_AJudgedBlockIsAskedForAgain is ChiR1: the drop path that was
// left with no recovery at all.
//
// A block that fails for a reason other than a transient local fault is judged:
// we reject it to the peer, and we used not to put the walk back on it. That was
// safe while a judged block "kept the recovery it has always had, the stall
// detector rebuilds the header list from a fresh peer". With
// legacy_multiPeerBlockDownload on there is no such rebuild: handleCheckSyncPeer
// demotes the stalled sync peer and returns, and demoteSyncPeer deliberately
// leaves the header state alone. The only surviving reset outside a disconnect
// is resetHeaderStateIfEmpty, which does nothing unless the list is ALREADY
// empty, so it cannot refill a header taken out of the middle of the walk.
//
// Below a checkpoint that header is one the node must have, so the walk could
// never reach the tip again. The end state asserted here is that the block is
// outstanding once more, with no headers message and no other block arriving, so
// nothing but the rewind can have put it there.
func TestSyncManager_AJudgedBlockIsAskedForAgain(t *testing.T) {
	r := newDroppedBlock(t, 66, time.Millisecond, errors.NewProcessingError("this block is not acceptable"))

	require.True(t, WaitUntil(func() bool {
		// What the park sweep's ticker calls, once every 30 seconds.
		r.sm.resumeHeaderWalk()

		return r.sm.blockDownloads.RequestedWithin(r.hash, time.Minute)
	}, 5*time.Second), "a judged block must be asked for again once its backoff has expired")
}

// TestSyncManager_AJudgedBlockIsThrottledBeforeItIsAskedForAgain is the brake
// that makes the rewind above shippable.
//
// The reason a judged block was left out of the rewind is a real one: asking for
// a block we have just rejected downloads it and rejects it again, and each
// re-delivery costs the delivering peer its whole association. The read loop
// turns any non-transient block error into disconnectMisbehaving, which resolves
// to the association primary (peer_server.go:1197, :1486, :1389), so the peer is
// removed from the node outright. The stall timer never gets a say: it is
// refreshed at receipt by HandleBlockDirect, but the eviction happens on the same
// delivery, long before any stall window could be consulted. So an unthrottled
// rewind would burn one peer per retry.
//
// The answer is not to leave the walk wedged. It is to throttle the retry with
// the same per-block backoff the transient path uses, and to stop blaming peers
// for a block we have already judged (ChiR7, the judgedBefore arm in
// handleBlockMsg). The backoff holds the WALK and not just the decorate, so the
// round stops on the block rather than running on into descendants that can
// never commit.
func TestSyncManager_AJudgedBlockIsThrottledBeforeItIsAskedForAgain(t *testing.T) {
	r := newDroppedBlock(t, 67, time.Hour, errors.NewProcessingError("this block is not acceptable"))

	fs, ok := r.sm.blockFailureBackoff.Get(r.hash)
	require.True(t, ok, "a judged block must start a backoff, or the rewind spins")
	require.True(t, fs.nextRetry.After(time.Now()), "the backoff should still be running")

	r.sm.fetchHeaderBlocks()
	r.sm.resumeHeaderWalk()

	require.False(t, r.sm.blockDownloads.RequestedWithin(r.hash, time.Minute),
		"a judged block still inside its backoff must not be asked for again yet")
}

// TestSyncManager_ABlockDroppedOnAContextErrorIsAskedForAgain covers the second
// half of ChiR1. handleBlockMsg's context-error arm returned nil with no rewind,
// no failure marker and no getblocks, so a block whose processing was cancelled
// mid-flight left the walk exactly as a judged one did.
//
// The recentlyFailedBlocks assertion is what pins the test to that arm rather
// than to the judged arm below it: the judged arm marks the block, the context
// arm returns before it.
func TestSyncManager_ABlockDroppedOnAContextErrorIsAskedForAgain(t *testing.T) {
	r := newDroppedBlock(t, 68, time.Millisecond, context.Canceled)

	_, marked := r.sm.recentlyFailedBlocks.Get(r.hash)
	require.False(t, marked, "a cancelled block is not a failed block, so the context arm must have run")

	require.True(t, WaitUntil(func() bool {
		r.sm.resumeHeaderWalk()

		return r.sm.blockDownloads.RequestedWithin(r.hash, time.Minute)
	}, 5*time.Second), "a block dropped on a context error must be asked for again")
}

// deliverJudgedBlockAgain replays the retry the ChiR1 rewind enables: the walk
// has put the judged block back, its backoff has expired, and some peer answers
// the fresh getdata with the same block. It returns what handleBlockMsg handed
// back for that second delivery.
//
// The waits are on the backoff, not on a sleep: a block still inside its window
// is skipped by the earlier arm of handleBlockMsg, which spares the peer for a
// different reason entirely and would make the assertion below meaningless.
func deliverJudgedBlockAgain(t *testing.T, r *refusedBlock, deliverer *peerpkg.Peer) error {
	t.Helper()

	require.True(t, WaitUntil(func() bool {
		fs, ok := r.sm.blockFailureBackoff.Get(r.hash)
		return ok && time.Now().After(fs.nextRetry)
	}, 5*time.Second), "the millisecond backoff must expire, or the delivery hits the backoff skip instead")

	_, judged := r.sm.recentlyFailedBlocks.Get(r.hash)
	require.True(t, judged, "the first delivery must have marked the block, or there is no second-delivery case to test")

	// The walk asked this peer for the block, so it owes us a copy.
	r.sm.blockDownloads.Add(deliverer, r.hash)

	return r.sm.handleBlockMsg(&blockQueueMsg{
		block:     r.block,
		blockHash: r.hash,
		peer:      deliverer,
	})
}

// TestSyncManager_TheFirstDelivererOfAJudgedBlockIsBlamed pins the half of ChiR7
// that must NOT change: a peer that hands us a block we then judge is still
// disconnected for it.
//
// The end state is the classification, because that is the whole of the
// decision: the read loop calls shouldDisconnectOnBlockErr, which is exactly
// !errors.IsTransientLocalError (peer_server.go:1334-1340), and a false answer
// runs disconnectMisbehaving, evicting the peer's whole association resolved to
// the association primary (peer_server.go:1197, :1486, :1389).
func TestSyncManager_TheFirstDelivererOfAJudgedBlockIsBlamed(t *testing.T) {
	r := newDroppedBlock(t, 69, time.Millisecond, errors.NewProcessingError("this block is not acceptable"))

	require.Error(t, r.deliverErr)
	require.False(t, errors.IsTransientLocalError(r.deliverErr),
		"the first deliverer of a judged block must still lose its association")
}

// TestSyncManager_ASecondDelivererOfAJudgedBlockIsNotBlamed is ChiR7.
//
// The ChiR1 rewind put the walk back on a judged block, so once its backoff
// expires the assigner hands that hash to whichever peer has budget. Nothing
// else in that arm changed: it returned the judged error, the read loop read it
// as the peer's fault, and disconnectMisbehaving evicted that peer's whole
// association. Before the rewind exactly one peer paid for an unvalidatable
// block, because it was never asked for again. After it, every retry cost
// another peer, which is peer churn spent on the suppliers this sync depends on.
//
// Below a checkpoint the peer cannot be at fault: the header is
// checkpoint-verified and the block hashes to it, so a peer answering our
// getdata answered correctly and the rejection is ours. The reject message still
// goes out, so the peer is told the block is bad. Only the eviction is spared.
func TestSyncManager_ASecondDelivererOfAJudgedBlockIsNotBlamed(t *testing.T) {
	r := newDroppedBlock(t, 70, time.Millisecond, errors.NewProcessingError("this block is not acceptable"))

	// A different peer answers the retry, which is the case that matters: the
	// first deliverer has already paid, and this one is about to pay for it.
	second, _, _ := connectRacePeer(t, 71, 1000)
	registerRacePeer(r.sm, second)

	err := deliverJudgedBlockAgain(t, r, second)

	require.Error(t, err, "the block still fails, so the caller still hears about it")
	require.True(t, errors.IsTransientLocalError(err),
		"a peer answering our own retry of an already-judged block must keep its association")
}

// TestSyncManager_ASecondDelivererIsStillBlamedForALocalFault guards the sparing
// from swallowing the case it is not for.
//
// judgedBefore only spares a peer when the failure was a judgement on the block.
// A transient local fault takes the earlier arm — no reject, its own
// classification — and that arm already keeps the peer, so nothing here should
// depend on the ChiR7 branch to do it.
func TestSyncManager_ASecondDelivererIsStillBlamedForALocalFault(t *testing.T) {
	r := newRefusedBlock(t, 72, time.Millisecond)

	require.True(t, errors.IsTransientLocalError(r.deliverErr),
		"a store fault is not the peer's fault on the first delivery either")
}
