package netsync

import (
	"bytes"
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
	// noSuchBlock is the catch-all GetBlockHeader expectation. testify matches
	// the first registered expectation whose arguments fit, so a per-hash answer
	// added later would never be reached while this one stands. chainHolds
	// unsets it, adds the specific answer, and puts it back behind.
	noSuchBlock *mock.Call
	// genesis is the hash the header cache is filled from — height 0 there,
	// unlike every h.blocks entry (height index+1) — so chainHolds can answer
	// with the height the header cache already committed to, rather than a
	// flat guess that only happened to fit blocks[0]'s own parent.
	genesis chainhash.Hash
}

// heightOf resolves hash to the height the harness's own header cache gave
// it: 0 for genesis, index+1 for h.blocks[index]. chainHolds and
// chainHoldsInvalid use this so a mocked GetBlockHeader answer never
// disagrees with pipelineParentHeight's own header-cache resolution — a
// disagreement here is exactly what turned into a spurious "block height is
// not the correct height" commit failure the first time this harness reused
// a flat Height: 1 for genesis instead of genesis's real height, 0.
func (h *parkWiringHarness) heightOf(hash chainhash.Hash) uint32 {
	if hash.IsEqual(&h.genesis) {
		return 0
	}

	for i, b := range h.blocks {
		if bh := b.MsgBlock().BlockHash(); bh.IsEqual(&hash) {
			return uint32(i + 1) //nolint:gosec // a small fixture index, never negative
		}
	}

	return 1
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

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.settings = tSettings
	// chainParams stays newRaceManager's default (MainNetParams) deliberately:
	// several tests outside this file (e.g. TestParkedDispatchMayBeWindowed)
	// build their own manager against this package's default chainParams and
	// windowRoute reads sm.chainParams.Checkpoints, so switching it here would
	// silently change which heights those tests' preconditions see as
	// below-checkpoint. pipelineBlockSink itself checks no PoW (only the
	// merkle root against the header) and blockOrigin/quickValidationAllowed
	// answering false for these fixture heights under mainnet checkpoints is
	// harmless — none of this file's tests assert on the .subtree vs
	// .subtreeToCheck file suffix that decision drives.
	sm.blockchainClient = client
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](100)
	sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](time.Minute)
	if parkOn {
		sm.blockPark = mustNewBlockPark(t, ulogger.TestLogger{}, tSettings, store)
		// The pipeline sink writes a block's subtree files to this store before
		// the park ever sees it (deliver/deliverBlock below), same store as the
		// park's own — see newPipelineParkManager's "one store, not two" note.
		sm.subtreeStore = store
	}

	t.Cleanup(func() { sm.recentlyFailedBlocks.Stop() })

	syncPeer, _, rec := connectRecordingPeer(t, 71, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	// New always builds a real stream registry, and newDownloadAssigner's
	// per-peer depth floors an unmeasured peer at unmeasuredPeerDepth (2) until
	// its speed is known. A harness peer never streams a real block through
	// trackBlockStreams, so without a seeded rate it would stay "unmeasured"
	// for the harness's whole life and silently cap every test in this file at
	// two requests in flight, whatever legacy_maxBlocksInTransitPerPeer says.
	sm.streams = newStreamRegistry()
	sm.streams.rates[syncPeer] = 1

	sm.headersFirstMode.Store(true)

	// assignWantedBlocks reads the header cache, one node per block, in order,
	// none of them requested yet. The chain mock above reports height 0, which
	// is exactly the height these blocks (1, 2, 3, ...) sit above.
	//
	// Genesis itself is included as the run's own height-0 entry, not merely
	// the anchor it is filled from. Genesis is always in the chain in
	// reality, but the harness's blockchain mock answers every GetBlockHeader
	// with "no such block" by default, and the streaming route now checks a
	// block's parent for reachability the same way for every arrival
	// (parentIsReachable, streaming_install.go) — including the harness's own
	// height-1 blocks, whose parent is genesis. Naming genesis in the header
	// cache is what makes it reachable without also answering "yes, this is
	// committed" (that is parentIsInChain's separate GetBlockHeader check,
	// which stays false until a test's own chainHolds says otherwise) — so a
	// height-1 delivery parks like any other orphan instead of being
	// discarded, or committed on the strength of an unrelated mock.
	genesis := bsvutil.NewBlock(chaincfg.RegressionNetParams.GenesisBlock)
	genesisHeader := genesis.MsgBlock().Header

	sm.headerCache = newHeaderCache()
	headers := make([]*wire.BlockHeader, 0, len(blocks)+1)
	headers = append(headers, &genesisHeader)
	for _, b := range blocks {
		h := b.MsgBlock().Header
		headers = append(headers, &h)
	}
	require.True(t, sm.headerCache.Fill(genesisHeader.PrevBlock, 0, headers))

	return &parkWiringHarness{sm: sm, client: client, peer: syncPeer, rec: rec, parkDir: parkDirectory(storeURL), blocks: blocks, store: store, noSuchBlock: noSuchBlock, genesis: genesisHeader.BlockHash()}
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
			&model.BlockHeaderMeta{Height: h.heightOf(hash)}, nil)

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
			&model.BlockHeaderMeta{Height: h.heightOf(hash), Invalid: true}, nil)

	h.noSuchBlock = h.client.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))
}

// deliver streams one block through the pipeline sink and the on-disk
// consumer path — pipelineBlockSink converts it to subtree files plus a
// converted record, then handleBlockOnDiskMsg is what a real peer connection
// calls once those bytes are down (see streaming_install.go). That is the
// only route left: the decoded blockQueueMsg path this used to drive
// (processQueuedBlock) no longer exists now that the park is mandatory.
func (h *parkWiringHarness) deliver(t *testing.T, index int) error {
	t.Helper()

	return h.deliverBlock(t, h.blocks[index].MsgBlock(), int32(index+1))
}

// deliverBlock is the same arrival for a block the harness did not mine.
// height is unused by the on-disk route — pipelineBlockSink resolves the
// parent's height itself from the blockchain client — and is kept only so
// callers seeded from a fixed height (block_park_height_test.go) still read
// naturally; the argument is accepted and ignored rather than removed, to
// keep this a one-file change.
func (h *parkWiringHarness) deliverBlock(t *testing.T, msgBlock *wire.MsgBlock, height int32) error {
	t.Helper()

	_ = height

	hash := msgBlock.BlockHash()
	body := blockBodyBytes(t, bsvutil.NewBlock(msgBlock))

	h.sm.blockDownloads.Add(h.peer, hash)

	converted, err := h.sm.pipelineBlockSink(hash, &msgBlock.Header, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return err
	}

	h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{
		body: peerpkg.BlockBody{
			Header:    msgBlock.Header,
			TxCount:   uint64(len(msgBlock.Transactions)),
			Size:      int64(len(body)),
			Hash:      hash,
			Converted: converted,
		},
		peer: h.peer,
	})

	return nil
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

// drainOneParkCommit processes one posted parkCommit synchronously, exactly as
// dispatchBlocks' own parkCommits arm does (manager.go): restore the entry,
// then schedule a drain for its parent.
//
// It stands in for the consumer loop a running node has, so a test can post a
// commit and see it land without starting that loop.
func (h *parkWiringHarness) drainOneParkCommit(t *testing.T) {
	t.Helper()

	select {
	case commit := <-h.sm.parkCommits:
		h.sm.blockPark.Restore(commit.entry)
		h.sm.scheduleDrain(commit.entry.prevBlock, commit.parentHeight)
	default:
		t.Fatal("drainOneParkCommit: no parkCommit was posted")
	}
}

// TestSyncManager_AParkedBlockIsCommittedWhenItsParentArrives is the whole
// commit in one test. A block arrives before its parent; today it is fully
// downloaded, fully decoded and then thrown away, and nothing ever asks for it
// again. It must instead be kept and committed once the parent lands — both
// the parent and the block drained behind it.
func TestSyncManager_AParkedBlockIsCommittedWhenItsParentArrives(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	// The child arrives first; its parent (blocks[0]) is not stored, but it is
	// in the header cache the harness seeded, so the streaming route keeps it
	// rather than discarding it as unreachable.
	require.NoError(t, h.deliver(t, 1))

	require.Equal(t, 1, h.sm.blockPark.Len(), "a block whose parent is missing must be kept, not thrown away")
	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".block")

	// Now the parent arrives. Its own parent is genesis, which the streaming
	// route checks via GetBlockHeader (parentIsInChain, streaming_install.go)
	// before it will even attempt a commit — chainHolds is what makes that
	// check pass. GetBlockExists answering true for everything after this is
	// the existing "already exists" short-circuit HandleConvertedBlock and
	// HandleBlockDirect share, which is enough for the park's own bookkeeping
	// (this is a wiring test, not a validation test).
	h.chainHolds(t, h.blocks[0].MsgBlock().Header.PrevBlock)
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	// New() builds this channel unconditionally; wired by hand here so the
	// parent's own arrival — its parent (genesis) already resolvable — posts
	// rather than committing through handleBlockOnDiskMsg's own direct call,
	// which see drainOneParkCommit's own doc comment for why to avoid.
	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	require.NoError(t, h.deliver(t, 0))
	h.drainOneParkCommit(t)

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

	h.chainHolds(t, h.blocks[0].MsgBlock().Header.PrevBlock)
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	// See drainOneParkCommit's own doc comment for why this channel needs to
	// be wired by hand for the parent's own arrival to commit correctly.
	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	require.NotPanics(t, func() {
		require.NoError(t, h.deliver(t, 0))
	}, "a parked block whose peer has gone must still commit")

	h.drainOneParkCommit(t)

	require.Zero(t, h.sm.blockPark.Len(), "losing the delivering peer must not lose the block")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "the block must have been committed, not written off as a failure")
}

// TestSyncManager_NothingIsDrainedAfterABlockThatDidNotCommit pins the guard
// that tells the two apart: nothing may be drained behind a block that did not
// go into the chain.
//
// The mechanism for "the parent did not go into the chain" changed with the
// streaming route: there is no decoded handleBlockMsg any more to return nil
// from an orphan branch. Here the parent (blocks[0]) is itself an orphan
// relative to genesis, which the harness never puts in the header cache or
// the chain, so parentIsReachable (streaming_install.go) discards it outright
// before any commit is even attempted — a stronger guarantee than "attempted
// and failed", but the same property: the child parked behind it must be left
// alone.
func TestSyncManager_NothingIsDrainedAfterABlockThatDidNotCommit(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// The parent itself now arrives and is discarded as unreachable (its own
	// parent, genesis, is neither committed nor in the header list).
	require.NoError(t, h.deliver(t, 0))

	require.Equal(t, 1, h.sm.blockPark.Len(),
		"the parent never landed, so nothing may be drained behind it")
	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".block",
		"the child's blob must still be on disk; a drain that should not have run would have given it up")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "a block nobody tried to commit must not be marked as having failed")
}

// TestHandleConvertedBlock_ToleratesANilPeer. Every block recovered from the park
// after a restart has no delivering peer, and (*Peer).String dereferences the
// peer's address and asks it whether it is the sync peer — so calling it on nil
// panics, on the block-queue goroutine, in production.
//
// Ported from the deleted HandleBlockDirect route onto HandleConvertedBlock,
// which carries the identical nil-peer guard (handle_block.go).
func TestHandleConvertedBlock_ToleratesANilPeer(t *testing.T) {
	h := newParkWiringHarness(t, true)

	msgBlock := h.blocks[1].MsgBlock()
	hash := msgBlock.BlockHash()
	prev := msgBlock.Header.PrevBlock

	blk := bodyCommitment(t, bsvutil.NewBlock(msgBlock))

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
		err := h.sm.HandleConvertedBlock(context.Background(), nil, hash, blk)
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

	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".block",
		"its blob stays on disk, because downloading it again is the cost this avoids")

	before := h.rec.getDataCount()

	h.sm.fetchHeaderBlocks()

	require.False(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, time.Second),
		"the block must not be asked for again, because we already have it")
}
