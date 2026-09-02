package netsync

import (
	"container/list"
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// parkWiringHarness is a sync manager with a real, file-backed park and a
// blockchain client that reports every block missing until told otherwise, so a
// block really does arrive before its parent.
type parkWiringHarness struct {
	sm      *SyncManager
	client  *blockchain2.Mock
	peer    *peerpkg.Peer
	rec     *peerMsgRecorder
	parkDir string
	blocks  []*bsvutil.Block
	// store sits between the park and the real file store so a test can make
	// reading a blob back fail the way a starved or shutting-down store fails,
	// without disturbing the blob itself. Pass-through until a test says
	// otherwise, so every other test in this file is unaffected.
	store *parkReadFaultStore
}

func newParkWiringHarness(t *testing.T, parkOn bool) *parkWiringHarness {
	t.Helper()

	return newParkWiringHarnessInState(t, parkOn, blockchain2.FSMStateCATCHINGBLOCKS)
}

// newParkWiringHarnessInState is the same harness with the FSM state chosen by
// the caller. It matters for one decision only: handleBlockMsg suppresses every
// reject while the node is catching blocks, so a test about who gets blamed has
// to be able to run on both sides of that.
func newParkWiringHarnessInState(t *testing.T, parkOn bool, fsmState blockchain2.FSMStateType) *parkWiringHarness {
	t.Helper()

	// The real constructor registers these; a struct-literal manager reaches the
	// same gauges on the commit path.
	initPrometheusMetrics()

	blocks := minedBlocks(t, 3)

	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
	client.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{{}}, nil)
	// Nothing is stored, so every parent lookup fails the way it does for a
	// block that arrives before its parent.
	client.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))

	root := t.TempDir()

	storeURL, err := url.Parse("file://" + root)
	require.NoError(t, err)

	realStore, err := blob.NewStore(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	store := &parkReadFaultStore{Store: realStore}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.TempStore = storeURL
	tSettings.Legacy.ParkOutOfOrderBlocks = parkOn

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.settings = tSettings
	sm.blockchainClient = client
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](100)
	sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](time.Minute)
	sm.blockPark = newBlockPark(ulogger.TestLogger{}, tSettings, store)

	t.Cleanup(func() { sm.recentlyFailedBlocks.Stop() })

	checkpointHash := chainhash.Hash{0xcc}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 1_000_000, Hash: &checkpointHash}

	syncPeer, _, rec := connectRecordingPeer(t, 71, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	// The header list a headers-first node has while it is fetching these
	// blocks: one node per block, in order, none of them requested yet.
	sm.headerMu.Lock()
	sm.headerList = list.New()
	sm.headerIndex = make(map[chainhash.Hash]*list.Element)

	for i, b := range blocks {
		hash := b.MsgBlock().BlockHash()
		node := &headerNode{height: int32(i + 1), hash: &hash}
		sm.indexHeaderLocked(sm.headerList.PushBack(node), hash)
	}

	// Everything in the list has already been asked for, which is the state a
	// node is in while blocks are arriving.
	sm.startHeader = nil
	sm.headerMu.Unlock()
	sm.headersFirstMode.Store(true)

	return &parkWiringHarness{sm: sm, client: client, peer: syncPeer, rec: rec, parkDir: parkDirectory(storeURL), blocks: blocks, store: store}
}

// deliver feeds one block through the block-queue consumer's own path.
func (h *parkWiringHarness) deliver(t *testing.T, index int) error {
	t.Helper()

	msgBlock := h.blocks[index].MsgBlock()
	hash := msgBlock.BlockHash()

	h.sm.blockDownloads.Add(h.peer, hash)

	return h.sm.processQueuedBlock(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   hash,
		blockHeight: int32(index + 1),
		peer:        h.peer,
	})
}

// deliverBlock feeds one arbitrary block through the block-queue consumer's own
// path, for the tests that need a block the harness did not mine.
func (h *parkWiringHarness) deliverBlock(t *testing.T, msgBlock *wire.MsgBlock, height int32) error {
	t.Helper()

	hash := msgBlock.BlockHash()

	h.sm.blockDownloads.Add(h.peer, hash)

	return h.sm.processQueuedBlock(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   hash,
		blockHeight: height,
		peer:        h.peer,
	})
}

// TestSyncManager_AParkedBlockIsCommittedWhenItsParentArrives is the whole
// commit in one test. A block arrives before its parent; today it is fully
// downloaded, fully decoded and then thrown away, and nothing ever asks for it
// again. It must instead be kept and committed once the parent lands — and the
// header list must move on with it, or headers-first sync wedges one block
// later.
func TestSyncManager_AParkedBlockIsCommittedWhenItsParentArrives(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	// The child arrives first and its parent is not stored.
	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))

	require.Equal(t, 1, h.sm.blockPark.Len(), "a block whose parent is missing must be kept, not thrown away")
	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".msgBlock")

	// Now the parent arrives and commits, and everything behind it must follow.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	require.NoError(t, h.deliver(t, 0))

	require.Zero(t, h.sm.blockPark.Len(), "the parked block must be committed once its parent is in the chain")
	require.Zero(t, h.sm.blockPark.Bytes(), "committing a parked block must give its budget back")

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, child.String(), "a committed block's blob must be deleted")
	}

	// The header list is only advanced by an arriving block that matches its
	// front. A block committed off disk never passes that code, so without an
	// explicit advance the front sticks on a block already in the chain and the
	// NEXT block never matches it.
	h.sm.headerMu.Lock()
	front := h.sm.headerList.Front().Value.(*headerNode)
	h.sm.headerMu.Unlock()

	require.Equal(t, h.blocks[2].MsgBlock().BlockHash().String(), front.hash.String(),
		"the header list must have moved past both the parent and the block drained behind it")
}

// TestSyncManager_AParkedBlockFromADepartedPeerStillCommits. The commit path
// dereferences the delivering peer on several routes, and by the time a parked
// block drains that peer may be long gone — every block recovered from disk
// after a restart has no peer at all. That must be a defined state, not a
// panic and not a disconnect aimed at somebody else.
func TestSyncManager_AParkedBlockFromADepartedPeerStillCommits(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// The peer that delivered it goes away, and is evicted from the manager
	// exactly as handleDonePeerMsg would evict it. Another peer delivers the
	// parent.
	h.peer.DisconnectWithInfo("test: peer left")
	h.sm.peerStates.Delete(h.peer)

	other, _, _ := connectRacePeer(t, 72, 1000)
	registerRacePeer(h.sm, other)
	h.sm.storeSyncPeer(other, &syncPeerState{})
	h.peer = other

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	require.NotPanics(t, func() {
		require.NoError(t, h.deliver(t, 0))
	}, "a parked block whose peer has gone must still commit")

	require.Zero(t, h.sm.blockPark.Len(), "losing the delivering peer must not lose the block")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "the block must have been committed, not written off as a failure")

	h.sm.headerMu.Lock()
	front := h.sm.headerList.Front().Value.(*headerNode)
	h.sm.headerMu.Unlock()

	require.Equal(t, h.blocks[2].MsgBlock().BlockHash().String(), front.hash.String(),
		"a block committed from the park must move the header list on, whoever delivered it")
}

// TestSyncManager_NothingIsDrainedAfterABlockThatDidNotCommit pins the guard
// that tells the two apart. handleBlockMsg returns nil from several paths that
// put nothing in the chain; draining after one of those would try to commit the
// children of a block that is not there, and each of them would be given up on.
func TestSyncManager_NothingIsDrainedAfterABlockThatDidNotCommit(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// The parent itself now arrives and is ALSO an orphan, so it commits
	// nothing. handleBlockMsg still returns nil.
	h.client.On("GetBlockExists", mock.Anything, &parent).Return(false, nil).Once()

	// And the child would now be judged bad if anything did try to commit it, so
	// a drain that should not have run destroys its blob and writes it off —
	// which is what makes the difference visible instead of merely wasteful. It
	// has to be a fault of the BLOCK and not a local one: a local fault leaves
	// the block parked either way, so it could not tell the two apart.
	h.client.On("GetBlockExists", mock.Anything, &child).
		Return(false, errors.NewBlockInvalidError("this block is not one we can take")).Once()

	require.NoError(t, h.deliver(t, 0))

	require.Equal(t, 2, h.sm.blockPark.Len(),
		"nothing may be drained behind a block that did not go into the chain")
	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".msgBlock",
		"the child's blob must still be on disk; a drain that should not have run would have given it up")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "a block nobody tried to commit must not be marked as having failed")
}

// TestSyncManager_WithTheParkOffTheBlockIsDiscardedAndAskedForAgain is the
// settings-only rollback. With legacy_parkOutOfOrderBlocks false there is no
// park at all and nothing reaches the disk — but the block is NOT simply
// forgotten, because the download walk is put back onto it. That rewind is not
// gated by the setting, and it is the half of the drop path that keeps
// headers-first sync from stopping on the first out-of-order block, so the test
// asserts it rather than only asserting the absence of a park.
func TestSyncManager_WithTheParkOffTheBlockIsDiscardedAndAskedForAgain(t *testing.T) {
	h := newParkWiringHarness(t, false)

	require.Nil(t, h.sm.blockPark, "legacy_parkOutOfOrderBlocks false must leave no park at all")

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))

	require.Empty(t, parkDirEntries(t, h.parkDir), "with the park off nothing may reach the disk")
	require.Zero(t, h.sm.blockPark.Len())

	h.sm.headerMu.Lock()
	startHeader := h.sm.startHeader
	h.sm.headerMu.Unlock()

	require.NotNil(t, startHeader, "a discarded block must go back into the download walk")
	require.Equal(t, child.String(), startHeader.Value.(*headerNode).hash.String())
}

// TestHandleBlockDirect_ToleratesANilPeer. Every block recovered from the park
// after a restart has no delivering peer, and (*Peer).String dereferences the
// peer's address and asks it whether it is the sync peer — so calling it on nil
// panics, on the block-queue goroutine, in production.
func TestHandleBlockDirect_ToleratesANilPeer(t *testing.T) {
	h := newParkWiringHarness(t, true)

	msgBlock := h.blocks[1].MsgBlock()
	hash := msgBlock.BlockHash()
	prev := msgBlock.Header.PrevBlock

	// The parent IS stored, so the block gets past the parent lookup and reaches
	// the tracing call that names the peer. It is stopped just after, on the
	// parent's mined status, so the test does not need the whole ingest pipeline.
	h.client = &blockchain2.Mock{}
	h.sm.blockchainClient = h.client
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	h.client.On("GetBlockHeader", mock.Anything, &prev).
		Return(&model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
			&model.BlockHeaderMeta{Height: 1}, nil)
	h.client.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(false, nil)

	h.sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = false
	h.sm.settings.BlockValidation.IsParentMinedRetryMaxRetry = 1
	h.sm.settings.BlockValidation.IsParentMinedRetryBackoffDuration = time.Millisecond

	require.NotPanics(t, func() {
		err := h.sm.HandleBlockDirect(context.Background(), nil, hash, msgBlock)
		require.Error(t, err, "the parent is not mined, so this must fail there — not on a nil peer")
	})
}

// TestSyncManager_TheSweepCommitsABlockWhoseParentTurnedUpQuietly. A block can
// be parked for a reason other than a genuinely absent parent, and a block
// recovered from disk after a restart never sees a commit event for a parent
// that was already in the chain. Without the sweep those sit until their TTL
// evicts them and the whole download is wasted.
func TestSyncManager_TheSweepCommitsABlockWhoseParentTurnedUpQuietly(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// The parent is in the chain, but nothing in this node committed it, so no
	// drain was ever triggered.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	require.Zero(t, h.sm.blockPark.Len(),
		"a parked block whose parent is in the chain must be committed by the sweep, not left to expire")
}

// TestSyncManager_TheSweepGivesUpOnABlockWhoseParentNeverArrives: the park is
// bounded in time as well as in bytes, and giving a block up must leave it
// re-requestable rather than simply losing it.
func TestSyncManager_TheSweepGivesUpOnABlockWhoseParentNeverArrives(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	h.sm.sweepParkedBlocks(time.Now().Add(parkEntryTTL + time.Second))

	require.Zero(t, h.sm.blockPark.Len())
	require.Zero(t, h.sm.blockPark.Bytes())

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, child.String(), "a block given up on must not leave its blob behind")
	}

	h.sm.headerMu.Lock()
	startHeader := h.sm.startHeader
	h.sm.headerMu.Unlock()

	require.NotNil(t, startHeader)
	require.Equal(t, child.String(), startHeader.Value.(*headerNode).hash.String(),
		"a block given up on must be back in front of the download walk, or nothing ever asks for it again")
}
