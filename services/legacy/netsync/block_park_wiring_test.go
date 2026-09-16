package netsync

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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
	// noSuchBlock is the catch-all GetBlockHeader expectation. testify matches
	// the first registered expectation whose arguments fit, so a per-hash answer
	// added later would never be reached while this one stands. chainHolds
	// unsets it, adds the specific answer, and puts it back behind.
	noSuchBlock *mock.Call
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
	// Height 0, not some other placeholder: committedTip reads this mock
	// directly now, and the header cache below is seeded starting at height 1
	// — genesis plus these mined blocks — so the two have to agree on where
	// the chain sits or the wanted range computed from them names nothing.
	client.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 0}, nil)
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{{}}, nil)
	// Nothing is stored, so every parent lookup fails the way it does for a
	// block that arrives before its parent.
	noSuchBlock := client.On("GetBlockHeader", mock.Anything, mock.Anything).
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

	syncPeer, _, rec := connectRecordingPeer(t, 71, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	sm.headersFirstMode.Store(true)

	// assignWantedBlocks reads the header cache, one node per block, in order,
	// none of them requested yet. The chain mock above reports height 0, which
	// is exactly the height these blocks (1, 2, 3, ...) sit above.
	sm.headerCache = newHeaderCache()
	headers := make([]*wire.BlockHeader, len(blocks))
	for i, b := range blocks {
		headers[i] = &b.MsgBlock().Header
	}
	require.True(t, sm.headerCache.Fill(blocks[0].MsgBlock().Header.PrevBlock, 1, headers))

	return &parkWiringHarness{sm: sm, client: client, peer: syncPeer, rec: rec, parkDir: parkDirectory(storeURL), blocks: blocks, store: store, noSuchBlock: noSuchBlock}
}

// chainHolds makes the blockchain answer that it has this block, and that the
// block is valid.
//
// The sweep asks GetBlockHeader rather than GetBlockExists because invalidation
// is a flag on the row and not a delete, so existence alone cannot say whether a
// parent is usable. The harness answers "no such block" for everything by
// default, and testify serves the first matching expectation, so making one hash
// resolve means taking the catch-all out and putting it back behind the specific
// answer.
func (h *parkWiringHarness) chainHolds(t *testing.T, hash chainhash.Hash) {
	t.Helper()

	h.noSuchBlock.Unset()

	h.client.On("GetBlockHeader", mock.Anything, &hash).
		Return(&model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
			&model.BlockHeaderMeta{Height: 1}, nil)

	h.noSuchBlock = h.client.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))
}

// chainHoldsInvalid is the same, for a parent this node has stored and rejected.
// A parked block behind one of those can never be committed, however long it is
// held, and committing it on the strength of the parent merely existing is the
// hole the pair closes.
func (h *parkWiringHarness) chainHoldsInvalid(t *testing.T, hash chainhash.Hash) {
	t.Helper()

	h.noSuchBlock.Unset()

	h.client.On("GetBlockHeader", mock.Anything, &hash).
		Return(&model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
			&model.BlockHeaderMeta{Height: 1, Invalid: true}, nil)

	h.noSuchBlock = h.client.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))
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

// requireBackInTheWalk asserts the end state a given-up block must reach: it is
// still wanted and unowed, so the next wanted-range pass asks the peer for it
// again. There is no header-list or cursor position to inspect any more — the
// pass recomputes what it wants and who owes it from the committed tip on every
// call, so "still wanted" is answered by the getdata that follows, not by
// where anything sits.
func (h *parkWiringHarness) requireBackInTheWalk(t *testing.T, hash chainhash.Hash, getDataBefore int) {
	t.Helper()

	h.sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(getDataBefore, hash) }, 5*time.Second),
		"a block given up on must be asked for again")
}

// TestSyncManager_AParkedBlockIsCommittedWhenItsParentArrives is the whole
// commit in one test. A block arrives before its parent; today it is fully
// downloaded, fully decoded and then thrown away, and nothing ever asks for it
// again. It must instead be kept and committed once the parent lands — both
// the parent and the block drained behind it.
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
// forgotten: it is still wanted and unowed, so the next wanted-range pass asks
// for it again. That is not gated by the setting, and it is the half of the
// drop path that keeps headers-first sync from stopping on the first
// out-of-order block, so the test asserts it rather than only asserting the
// absence of a park.
func TestSyncManager_WithTheParkOffTheBlockIsDiscardedAndAskedForAgain(t *testing.T) {
	h := newParkWiringHarness(t, false)

	require.Nil(t, h.sm.blockPark, "legacy_parkOutOfOrderBlocks false must leave no park at all")

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	before := h.rec.getDataCount()

	require.NoError(t, h.deliver(t, 1))

	require.Empty(t, parkDirEntries(t, h.parkDir), "with the park off nothing may reach the disk")
	require.Zero(t, h.sm.blockPark.Len())

	h.sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 5*time.Second),
		"a discarded block must be asked for again")
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
		err := h.sm.HandleBlockDirect(context.Background(), nil, hash, msgBlock, nil, blockRequestOrigin{headerProven: true})
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
	h.chainHolds(t, h.blocks[1].MsgBlock().Header.PrevBlock)

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	require.Zero(t, h.sm.blockPark.Len(),
		"a parked block whose parent is in the chain must be committed by the sweep, not left waiting")
}

// TestSyncManager_TheSweepKeepsABlockWhoseParentIsMerelyLate is the inversion of
// what this test used to assert, and the inversion is the change.
//
// The park used to give a block up after thirty minutes and re-request it. That
// answered the wrong question: it asked how long the block had been waiting,
// when what matters is whether it can still be used. A block whose parent is
// late can still be used, however long it has waited, so throwing it away only
// bought a second download of a block already on disk — 39 of them in one
// measured 19-minute window on mainnet.
//
// So a late parent is no longer a reason to drop anything. What bounds the park
// is legacy_blockDownloadMaxBytes, which stops the walk asking for more rather
// than discarding what it has, and the two rules that can actually be decided:
// the chain going past the block, and its parent turning out to be invalid.
func TestSyncManager_TheSweepKeepsABlockWhoseParentIsMerelyLate(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	held := h.sm.blockPark.Bytes()
	require.Positive(t, held)

	// Many ticks, well past the half hour the old timer allowed, and the parent
	// still absent throughout.
	for tick := 0; tick < 80; tick++ {
		h.sm.sweepParkedBlocks(time.Now().Add(time.Duration(tick) * parkSweepInterval))
	}

	require.Equal(t, 1, h.sm.blockPark.Len(),
		"a block whose parent is merely late must be kept, however long it waits")
	require.Equal(t, held, h.sm.blockPark.Bytes(), "and it keeps its place in the byte budget")

	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".msgBlock",
		"its blob stays on disk, because downloading it again is the cost this avoids")

	before := h.rec.getDataCount()

	h.sm.fetchHeaderBlocks()

	require.False(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, time.Second),
		"the block must not be asked for again, because we already have it")
}
