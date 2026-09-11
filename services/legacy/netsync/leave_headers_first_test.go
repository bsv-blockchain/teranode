package netsync

import (
	"context"
	"sync"
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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// getBlocksRecorder collects the getblocks messages a peer's remote end is sent.
type getBlocksRecorder struct {
	mu   sync.Mutex
	msgs []*wire.MsgGetBlocks
}

func (r *getBlocksRecorder) record(msg *wire.MsgGetBlocks) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.msgs = append(r.msgs, msg)
}

func (r *getBlocksRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.msgs)
}

// connectGetBlocksPeer returns a live peer whose remote end records every
// getblocks it is sent, which is how sync asks for the next batch of blocks once
// it is out of headers-first mode.
func connectGetBlocksPeer(t *testing.T, idx uint8, lastBlock int32) (*peerpkg.Peer, *getBlocksRecorder) {
	t.Helper()

	rec := &getBlocksRecorder{}
	chainParams := &chaincfg.MainNetParams

	remoteCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnGetBlocks: func(_ *peerpkg.Peer, msg *wire.MsgGetBlocks) {
				rec.record(msg)
			},
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}
	localCfg := peerpkg.Config{
		Listeners:        peerpkg.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}

	remote, local, err := MakeConnectedPeers(t, remoteCfg, localCfg, idx)
	require.NoError(t, err)

	local.UpdateLastBlockHeight(lastBlock)

	t.Cleanup(func() {
		local.DisconnectWithInfo("test over")
		remote.DisconnectWithInfo("test over")
	})

	return local, rec
}

// TestLeaveHeadersFirstMode_LetsSyncAskForTheNextBatchAgain covers the other
// wipe path. Once the final checkpoint is reached the node leaves headers-first
// mode and drives the rest of the chain with getblocks, and the only thing that
// re-primes that once everything has gone quiet is handleBlockMsg's fallback:
// when there are no headers left to fetch, we are not current, and nothing is in
// flight with this peer, ask it for the next batch.
//
// "No headers left to fetch" is read off startHeader, and leaveHeadersFirstMode
// wipes the header list and the index but used to leave startHeader pointing
// into the list it just emptied. A non-nil pointer reads as "there is still work
// queued", so the fallback never fired and sync sat there with an idle peer.
//
// The assertion is the end state a node needs: the peer is asked for more
// blocks.
func TestLeaveHeadersFirstMode_LetsSyncAskForTheNextBatchAgain(t *testing.T) {
	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}]()

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	// Height 100 against a peer that reports 1000 keeps the node not-current,
	// which is the state the fallback exists for.
	blockchainClient.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	// The block is already in the chain, so HandleBlockDirect returns straight
	// away and handleBlockMsg carries on to the decision under test.
	blockchainClient.Mock.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)
	blockchainClient.Mock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewNotFoundError("not found"))

	sm.blockchainClient = blockchainClient

	syncPeer, rec := connectGetBlocksPeer(t, 47, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	checkpointHash := chainhash.Hash{0xcc}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 1_000_000, Hash: &checkpointHash}

	var nonce uint32

	anchor := chainhash.Hash{0xa7}
	msg, _ := linkedHeaders(anchor, 5, &nonce)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	// The final checkpoint has been reached, so sync leaves headers-first mode.
	sm.leaveHeadersFirstMode()

	require.Zero(t, sm.headerListLen(), "leaving headers-first mode empties the header list")

	// A block now arrives with nothing else outstanding for this peer. There are
	// no headers left to walk, so the only way sync moves on is by asking this
	// peer for the next batch.
	arriving := wire.NewMsgBlock(wire.NewBlockHeader(1, &chainhash.Hash{0xa7}, &chainhash.Hash{}, 0, 0))
	arrivingHash := arriving.Header.BlockHash()

	sm.blockDownloads.Add(syncPeer, arrivingHash)

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       arriving,
		blockHash:   arrivingHash,
		blockHeight: 101,
		peer:        syncPeer,
	})
	require.NoError(t, err)

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"with no headers left and nothing in flight, sync must ask the peer for the next batch of blocks")
}

// TestHandleHeadersMsg_SurvivesTheCheckpointGoingNilUnderIt pins the window
// between advancing past the final checkpoint and actually leaving headers-first
// mode.
//
// checkpointBlockCommitted sets nextCheckpoint to nil under headerMu and only
// calls leaveHeadersFirstMode after releasing the lock. A headers goroutine that
// had already passed the mode check can take the lock in that gap and read the
// nil. Multi-peer demotion makes overlapping headers replies from the outgoing
// and incoming sync peer ordinary, so this is a state a running node reaches
// rather than a theoretical one — and the two reads it makes of nextCheckpoint
// were bare dereferences, so it did not fail gracefully, it panicked and took
// the goroutine with it.
//
// The end state asserted is the one that matters: the batch is handled without
// panicking, and nothing is asked for on behalf of a mode we are leaving.
func TestHandleHeadersMsg_SurvivesTheCheckpointGoingNilUnderIt(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe1}
	msg, _ := linkedHeaders(anchor, 3, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)

	peer, _, _ := connectRacePeer(t, 60, 1000)
	registerRacePeer(sm, peer)
	sm.storeSyncPeer(peer, &syncPeerState{})

	sm.resetHeaderState(&anchor, 10)
	sm.headersFirstMode.Store(true)

	// The state the gap leaves behind: past the final checkpoint, but the mode
	// flag has not been turned off yet.
	sm.headerMu.Lock()
	sm.nextCheckpoint = nil
	sm.headerMu.Unlock()

	require.NotPanics(t, func() {
		sm.handleHeadersMsg(&headersMsg{headers: msg, peer: peer})
	}, "a headers batch arriving past the final checkpoint must not take the goroutine down")

	require.True(t, peer.Connected(),
		"the sender answered our own getheaders and must not be punished for the checkpoint moving")

	// And the batch was actually processed rather than the goroutine dying
	// part-way through it: the anchor plus three linked headers.
	require.Equal(t, 4, sm.headerListLen(),
		"the headers must still link, or the guard has turned a panic into silently dropping the batch")
}
