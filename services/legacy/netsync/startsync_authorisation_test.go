package netsync

import (
	"container/list"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newSyncPeerChangeManager builds the smallest SyncManager that can run the real
// startSync and the real handleBlockMsg back to back, with a blockchain client
// that reports us at height 500 and treats any block we feed it as an orphan —
// so handleBlockMsg gets past the unrequested-block gate and then returns
// harmlessly, leaving the gate as the only thing under test.
func newSyncPeerChangeManager(t *testing.T) *SyncManager {
	t.Helper()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS

	bestHeader := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 500}, nil)
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{bestHeader.Hash()}, nil)
	blockchainClient.On("CatchUpBlocks", mock.Anything).Return(nil)
	blockchainClient.On("Run", mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewBlockNotFoundError("not found"))

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         test.CreateBaseTestSettings(t),
		chainParams:      &chaincfg.MainNetParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		headerList:       list.New(),
		blockDownloads:   newBlockDownloadTracker(blockRequestAssignmentTTL),
		racedBlocks: expiringmap.New[chainhash.Hash, map[*peerpkg.Peer]struct{}](racedBlockGraceTTL).
			WithMaxSize(racedBlockGraceMaxTracked),
	}

	t.Cleanup(func() { sm.racedBlocks.Stop() })

	return sm
}

// registerSyncPeerChangePeer adds a peer to the manager with its own requested-tx
// map, so handleBlockMsg can resolve it.
func registerSyncPeerChangePeer(t *testing.T, sm *SyncManager, p *peerpkg.Peer, candidate bool) {
	t.Helper()

	state := &peerSyncState{
		syncCandidate: candidate,
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
	}
	t.Cleanup(state.requestedTxns.Stop)

	sm.peerStates.Set(p, state)
}

// TestStartSync_HonestPeerKeepsItsAuthorisationAcrossASyncPeerChange is the
// regression test for the defect the single owner-keyed ledger introduced.
//
// The story it encodes is the one the stall recovery actually walks. The sync
// peer goes quiet on the frontier block, so the frontier race asks an honest
// second peer for that same block and records that we asked it. The 180 second
// stall timer then fires, and handleCheckSyncPeer -> updateSyncPeer -> startSync
// elects a new sync peer. The honest peer's copy — which we asked for — arrives
// a moment later.
//
// Before the ledger existed, startSync cleared the GLOBAL re-request map while
// the disconnect decision read a SEPARATE per-peer map, so a sync-peer change
// could not revoke anybody's permission to deliver. Once both maps became one
// ledger, that same clear started wiping the very record the disconnect gate
// consults, and the honest peer lost its whole association for answering a
// question we asked it.
func TestStartSync_HonestPeerKeepsItsAuthorisationAcrossASyncPeerChange(t *testing.T) {
	sm := newSyncPeerChangeManager(t)

	// A block whose parent we do not know, so handleBlockMsg treats it as an
	// orphan once it is past the gate.
	prevHash := chainhash.Hash{0x7f}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	frontier := msgBlock.Header.BlockHash()

	// The peer startSync will elect: a connected sync candidate ahead of us.
	newSyncPeer, _, _ := connectRacePeer(t, 21, 1000)
	registerSyncPeerChangePeer(t, sm, newSyncPeer, true)

	// The honest peer the frontier race asked for the stuck block. It is not a
	// sync candidate, so the election below is deterministic.
	honest, _, _ := connectRacePeer(t, 22, 1000)
	registerSyncPeerChangePeer(t, sm, honest, false)

	sm.blockDownloads.Add(honest, frontier)
	require.True(t, sm.blockDownloads.HasOwner(honest, frontier),
		"sanity: we asked the honest peer for this block")

	// The stall timer fires and a new sync peer is elected.
	sm.startSync()
	require.Equal(t, newSyncPeer, sm.loadSyncPeer(),
		"sanity: startSync must have run to completion, so the ledger-wide clear was reached")

	// The honest peer's copy turns up.
	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   frontier,
		blockHeight: 501,
		peer:        honest,
	})

	require.True(t, honest.Connected(),
		"an honest peer delivering a block we asked it for must keep its connection across a sync-peer change")

	if err != nil {
		require.NotContains(t, err.Error(), "unrequested",
			"the block was requested, so it must never be judged unrequested")
	}
}

// TestStartSync_DoesNotReopenBlocksOwedByPeersStillOnTheJob pins the deletion of
// the whole-ledger back-date, which is the mechanism behind the historical
// duplicate-commit storm and the 40P01 deadlock on the transaction unique index.
//
// It used to be here for a good reason: a block the departing sync peer never
// sent has to be fetchable from somebody else without waiting out the re-request
// interval. But it reopened EVERY outstanding block, not the departing peer's,
// and it was survivable only because a sync-peer change threw the header list
// away in the same breath and left nothing to re-walk. Now that a demotion keeps
// the list, reopening everything would have the very next pass hand every
// in-flight block to a second peer.
//
// The recovery it provided is now scoped to the peer that actually stalled — see
// TestDemotion_ReopensOnlyTheDemotedPeersSliceAndRewindsToItsLowestBlock.
func TestStartSync_DoesNotReopenBlocksOwedByPeersStillOnTheJob(t *testing.T) {
	sm := newSyncPeerChangeManager(t)

	inFlight := chainhash.Hash{0xc3}

	newSyncPeer, _, _ := connectRacePeer(t, 23, 1000)
	registerSyncPeerChangePeer(t, sm, newSyncPeer, true)

	// A peer that is not the one being replaced, and is answering a question we
	// put to it right now.
	busy, _, _ := connectRacePeer(t, 24, 1000)
	registerSyncPeerChangePeer(t, sm, busy, false)

	sm.blockDownloads.Add(busy, inFlight)
	require.True(t, sm.blockDownloads.RequestedWithin(inFlight, blockRequestRetryInterval),
		"sanity: the block was just asked for, so the walk would skip it")

	sm.startSync()
	require.Equal(t, newSyncPeer, sm.loadSyncPeer())

	require.True(t, sm.blockDownloads.RequestedWithin(inFlight, blockRequestRetryInterval),
		"a sync peer change must leave another peer's in-flight block vouching for itself, or the next pass asks a second peer for it")
	require.True(t, sm.blockDownloads.HasOwner(busy, inFlight),
		"and that peer keeps its permission to deliver it")
}
