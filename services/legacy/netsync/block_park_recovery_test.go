package netsync

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// peerMsgRecorder collects what a peer's remote end is actually sent. The park
// paths are all defined by what reaches the peer — a getblocks that keeps it
// sending, a getdata that asks for a block again, a reject that tells it a block
// was bad — so every assertion here is made on the far side of the wire rather
// than on manager state.
type peerMsgRecorder struct {
	mu        sync.Mutex
	getData   []chainhash.Hash
	getBlocks int
	rejects   []chainhash.Hash
}

func (r *peerMsgRecorder) recordGetData(msg *wire.MsgGetData) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, iv := range msg.InvList {
		if iv.Type == wire.InvTypeBlock {
			r.getData = append(r.getData, iv.Hash)
		}
	}
}

func (r *peerMsgRecorder) recordGetBlocks() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.getBlocks++
}

func (r *peerMsgRecorder) recordReject(msg *wire.MsgReject) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.rejects = append(r.rejects, msg.Hash)
}

func (r *peerMsgRecorder) getBlocksCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.getBlocks
}

func (r *peerMsgRecorder) getDataCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.getData)
}

// askedForSince reports whether hash appears in a getdata recorded after the
// first from getdata block hashes already seen.
func (r *peerMsgRecorder) askedForSince(from int, hash chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if from > len(r.getData) {
		return false
	}

	for _, got := range r.getData[from:] {
		if got.IsEqual(&hash) {
			return true
		}
	}

	return false
}

func (r *peerMsgRecorder) wasRejected(hash chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, got := range r.rejects {
		if got.IsEqual(&hash) {
			return true
		}
	}

	return false
}

// connectRecordingPeer returns a live peer whose remote end records the three
// message kinds the park paths are supposed to send.
func connectRecordingPeer(t *testing.T, idx uint8, lastBlock int32) (*peerpkg.Peer, *peerpkg.Peer, *peerMsgRecorder) {
	t.Helper()

	rec := &peerMsgRecorder{}
	chainParams := &chaincfg.MainNetParams

	remoteCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnGetData:   func(_ *peerpkg.Peer, msg *wire.MsgGetData) { rec.recordGetData(msg) },
			OnGetBlocks: func(_ *peerpkg.Peer, _ *wire.MsgGetBlocks) { rec.recordGetBlocks() },
			OnReject:    func(_ *peerpkg.Peer, msg *wire.MsgReject) { rec.recordReject(msg) },
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

	return local, remote, rec
}

// TestSyncManager_AParkedOrphanIsStillAnsweredWithAGetblocks is the one the
// park broke. In the legacy sync protocol an orphan is not only a block out of
// order: the peer pushes its tip after delivering a batch and then waits for the
// next getblocks before it sends anything else. Keeping the block instead of
// throwing it away must not swallow that answer — the park keeps the download,
// the getblocks fetches the gap, and they are not alternatives.
//
// Out of headers-first mode, which is every node past the final checkpoint,
// nothing else sends anything: fetchMoreHeaderBlocks returns immediately. The
// peer would sit silent until the stall detector rotated it.
func TestSyncManager_AParkedOrphanIsStillAnsweredWithAGetblocks(t *testing.T) {
	for _, tc := range []struct {
		name         string
		headersFirst bool
	}{
		{name: "past the final checkpoint", headersFirst: false},
		{name: "in headers-first mode", headersFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newParkWiringHarness(t, true)
			h.sm.headersFirstMode.Store(tc.headersFirst)

			child := h.blocks[1].MsgBlock().BlockHash()

			h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

			require.NoError(t, h.deliver(t, 1))

			require.Equal(t, 1, h.sm.blockPark.Len(), "the block must be kept, not thrown away")
			require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".msgBlock")

			require.True(t, WaitUntil(func() bool { return h.rec.getBlocksCount() > 0 }, 5*time.Second),
				"an orphan must be answered with a getblocks or the peer sends nothing more")
		})
	}
}

// TestSyncManager_AParkedFrontBlockIsAskedForAgainWhenItIsGivenUp is the case
// the rewind machinery exists for and nothing exercised. A block that arrives as
// the front of the header list has its header removed and unindexed before the
// park ever sees it, so by the time the park gives the block up there is nothing
// left to look the header up by. Unless the park carried that header node with
// the block, the rewind finds nothing, and the block is in neither the header
// list, nor the park, nor any download ledger — sync cannot pass it again
// without a peer rotation that rebuilds the whole walk.
func TestSyncManager_AParkedFrontBlockIsAskedForAgainWhenItIsGivenUp(t *testing.T) {
	h := newParkWiringHarness(t, true)

	front := h.blocks[0].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	require.NoError(t, h.deliver(t, 0))
	require.Equal(t, 1, h.sm.blockPark.Len(), "the front block parks like any other orphan")

	// This is what makes the case different: its header left the list on
	// arrival, so an index lookup can no longer find it.
	h.sm.headerMu.Lock()
	_, stillIndexed := h.sm.headerIndex[front]
	h.sm.headerMu.Unlock()

	require.False(t, stillIndexed, "an arriving front block's header is removed before the park sees it")

	// The parent never arrives and the block's time runs out.
	h.sm.sweepParkedBlocks(time.Now().Add(parkEntryTTL + time.Second))

	require.Zero(t, h.sm.blockPark.Len())

	before := h.rec.getDataCount()

	h.sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, front) }, 5*time.Second),
		"a block given up on must go back into the download walk, or nothing ever asks for it again")
}

// TestSyncManager_AParkedBlockThatWillNotReadBackIsAskedForAgain covers the
// drain's read failure: the blob has gone or will not decode, so there is
// nothing to commit and nothing to put back. The block has to re-enter the
// download walk or it is simply lost.
func TestSyncManager_AParkedBlockThatWillNotReadBackIsAskedForAgain(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &parent).Return(true, nil)
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// The blob is destroyed under the park: a truncated write, a disk that lost
	// the file, a sweep that took it.
	require.NoError(t, os.Remove(filepath.Join(h.parkDir, child.String()+".msgBlock")))

	before := h.rec.getDataCount()

	// The parent commits, so the drain reaches for the child and finds nothing.
	require.NoError(t, h.deliver(t, 0))

	require.Zero(t, h.sm.blockPark.Len(), "a block that cannot be read back must not stay in the index")

	h.sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 5*time.Second),
		"a parked block that will not read back must be asked for again")
}

// TestSyncManager_AParkedBlockThatWillNotCommitIsGivenUpAndRejected covers the
// rest of the drain's failure branch, none of which any test reached: the blob
// is dropped, the block goes back into the download walk, and the peer that
// actually sent it — not a fallback peer, and not nobody — is told it was
// rejected.
//
// Whether the peer is told at all is the node's state and not the error's, and
// both sides of that are driven here. handleBlockMsg suppresses every reject
// while the node is catching blocks, because then it is replaying history rather
// than judging a peer's tip — and during initial sync this drain is the MAIN
// commit path, so a parked block must not earn its peer a reject that the same
// block delivered live would not.
func TestSyncManager_AParkedBlockThatWillNotCommitIsGivenUpAndRejected(t *testing.T) {
	for _, tc := range []struct {
		name         string
		fsmState     blockchain2.FSMStateType
		expectReject bool
	}{
		{
			name:         "catching blocks, so the peer is not blamed",
			fsmState:     blockchain2.FSMStateCATCHINGBLOCKS,
			expectReject: false,
		},
		{
			name:         "running, so the peer is told the block was bad",
			fsmState:     blockchain2.FSMStateRUNNING,
			expectReject: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newParkWiringHarnessInState(t, true, tc.fsmState)

			child := h.blocks[1].MsgBlock().BlockHash()
			parent := h.blocks[0].MsgBlock().BlockHash()

			// First lookup parks the block. The second, on the drain, fails with
			// a fault that is the block's and not the local node's, so the error
			// itself is a judgement on the block.
			h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()
			h.client.On("GetBlockExists", mock.Anything, &child).
				Return(false, errors.NewBlockInvalidError("this block is not one we can take")).Once()
			h.client.On("GetBlockExists", mock.Anything, &parent).Return(true, nil)
			h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

			require.NoError(t, h.deliver(t, 1))
			require.Equal(t, 1, h.sm.blockPark.Len())

			before := h.rec.getDataCount()

			require.NoError(t, h.deliver(t, 0))

			require.Zero(t, h.sm.blockPark.Len(), "a block that will not commit must not stay parked")

			for _, name := range parkDirEntries(t, h.parkDir) {
				require.NotContains(t, name, child.String(), "a block given up on must not leave its blob behind")
			}

			if tc.expectReject {
				require.True(t, WaitUntil(func() bool { return h.rec.wasRejected(child) }, 5*time.Second),
					"the peer that sent the block must be the one told it was rejected")
			} else {
				require.False(t, h.rec.wasRejected(child),
					"while catching blocks no reject is sent, whether the block is committed from the wire or from the park")
			}

			h.sm.fetchHeaderBlocks()

			require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 5*time.Second),
				"a block given up on must go back into the download walk")
		})
	}
}

// TestSyncManager_AParkedBlockWhoseParentGoesMissingAgainStaysParked covers the
// first of the two ways a drain declines to commit without giving the block up.
// A reorg can take the parent back out from under a block that was about to be
// committed; the block itself is still perfectly good, so it has to go back in
// the index with its blob intact rather than be written off and re-downloaded.
func TestSyncManager_AParkedBlockWhoseParentGoesMissingAgainStaysParked(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	// The parent commits, but the child's own parent lookup still fails, which
	// is exactly what a reorg under the drain looks like.
	h.client.On("GetBlockExists", mock.Anything, &parent).Return(true, nil)
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	parkedBytes := h.sm.blockPark.Bytes()

	require.NoError(t, h.deliver(t, 0))

	require.Equal(t, 1, h.sm.blockPark.Len(), "a block whose parent went missing again must stay parked")
	require.Equal(t, parkedBytes, h.sm.blockPark.Bytes(), "putting a block back must not lose or double its budget")
	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".msgBlock",
		"the blob must still be on disk for the retry")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "a block nobody could commit yet must not be written off as a failure")

	require.False(t, h.rec.wasRejected(child), "a reorg is not the peer's fault, so it must not be told the block was bad")
}

// TestSyncManager_AParkedBlockIsKeptWhenTheCommitIsCancelled covers the other
// one. On shutdown the commit is cancelled mid-flight; the block has not been
// judged, so it must be left where the restart scan will find it rather than
// deleted and re-downloaded.
func TestSyncManager_AParkedBlockIsKeptWhenTheCommitIsCancelled(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()
	h.client.On("GetBlockExists", mock.Anything, &child).
		Return(false, errors.NewContextCanceledError("shutting down", context.Canceled)).Once()
	h.client.On("GetBlockExists", mock.Anything, &parent).Return(true, nil)
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	require.NoError(t, h.deliver(t, 0))

	require.Equal(t, 1, h.sm.blockPark.Len(), "a cancelled commit must leave the block parked for the restart scan")
	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".msgBlock")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "a cancelled commit is not a verdict on the block")
}
