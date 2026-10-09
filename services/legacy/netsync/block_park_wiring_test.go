package netsync

import (
	"bytes"
	"context"
	"net/url"
	"strconv"
	"sync/atomic"
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
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	chainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// parkWiringHarness is a sync manager with a real, file-backed park and a real
// blockchain client over a sqlitememory store, so a block really does arrive
// before its parent and really is committed once the parent is there.
//
// The store starts holding regtest genesis and nothing else. That is the one
// fact every test here builds on: blocks[0]'s parent is always in the chain, so
// a delivery of blocks[0] commits on arrival, and blocks[1] and blocks[2] park
// until something stores the block under them (chainHolds, or a commit).
type parkWiringHarness struct {
	sm      *SyncManager
	chain   *parkWiringChain
	peer    *peerpkg.Peer
	rec     *peerMsgRecorder
	parkDir string
	blocks  []*bsvutil.Block
	// store sits between the park and the real file store so a test can make
	// reading a blob back fail the way a starved or shutting-down store fails,
	// without disturbing the blob itself. Pass-through until a test says
	// otherwise, so every other test in this file is unaffected.
	store *parkReadFaultStore
	// validation is what HandleConvertedBlock commits through. It stores each
	// block it is handed in chain, so a drained block's parent lookup reads a
	// real row, and it is where a test injects a commit failure
	// (failOnce), the one place a production commit failure originates.
	validation *convertedRouteSpyValidation
	// genesis is the hash the header cache is filled from, height 0 there and
	// always in the store, unlike every h.blocks entry (height index+1).
	genesis chainhash.Hash
}

// parkWiringChain is the real LocalClient over a sqlitememory store with the
// two seams the park tests need and the real client cannot give: the FSM state
// (LocalClient hard-codes RUNNING, and handleBlockMsg's reject suppression
// turns on it) and a GetBlockHeader fault, for the test that proves a store
// fault is not read as a missing parent. It also counts GetBlockIsMined polls
// for the mined-wait test. Every other call reaches the real store.
type parkWiringChain struct {
	blockchain2.ClientI

	fsm        atomic.Pointer[blockchain2.FSMStateType]
	headerErr  atomic.Pointer[error]
	minedPolls atomic.Int32
}

func (c *parkWiringChain) GetFSMCurrentState(context.Context) (*blockchain2.FSMStateType, error) {
	return c.fsm.Load(), nil
}

func (c *parkWiringChain) IsFSMCurrentState(_ context.Context, state blockchain2.FSMStateType) (bool, error) {
	return *c.fsm.Load() == state, nil
}

func (c *parkWiringChain) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if e := c.headerErr.Load(); e != nil {
		return nil, nil, *e
	}

	return c.ClientI.GetBlockHeader(ctx, hash)
}

func (c *parkWiringChain) GetBlockIsMined(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	c.minedPolls.Add(1)

	return c.ClientI.GetBlockIsMined(ctx, hash)
}

// failHeaderReadsWith makes every GetBlockHeader fail with err, the way a store
// that is briefly not answering does.
func (c *parkWiringChain) failHeaderReadsWith(err error) {
	c.headerErr.Store(&err)
}

// parkWiringStoreCounter gives each harness its own sqlitememory database:
// the driver shares one in-memory database per name within the process.
var parkWiringStoreCounter atomic.Int64

func newParkWiringHarness(t *testing.T, parkOn bool) *parkWiringHarness {
	t.Helper()

	return newParkWiringHarnessInState(t, parkOn, blockchain2.FSMStateCATCHINGBLOCKS)
}

// parkWiringOption adjusts the harness at construction.
type parkWiringOption func(*parkWiringConfig)

type parkWiringConfig struct {
	txsPerBlock int
}

// withTransactions mines every harness block with n non-coinbase transactions,
// each spending a distinct outpoint nothing created, so the converted record
// the sink writes names a subtree whose structure and data files are on disk.
// The default harness mines coinbase-only blocks, whose records name no
// subtree at all, which is no use to a test about those files.
//
// The spy block validation reads none of them, and the sink needs no parent
// output to stream a transaction into a subtree file, so an outpoint nothing
// created is exactly the shape wireBlockWithTxs already converts in this
// package's sink tests.
func withTransactions(n int) parkWiringOption {
	return func(c *parkWiringConfig) { c.txsPerBlock = n }
}

// newParkWiringHarnessInState is the same harness with the FSM state chosen by
// the caller. It matters for one decision only: handleBlockMsg suppresses every
// reject while the node is catching blocks, so a test about who gets blamed has
// to be able to run on both sides of that.
func newParkWiringHarnessInState(t *testing.T, parkOn bool, fsmState blockchain2.FSMStateType, opts ...parkWiringOption) *parkWiringHarness {
	t.Helper()

	var cfg parkWiringConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	// The real constructor registers these; a struct-literal manager reaches the
	// same gauges on the commit path.
	initPrometheusMetrics()

	blocks := minedBlocksCarrying(t, 3, cfg.txsPerBlock)

	root := t.TempDir()

	storeURL, err := url.Parse("file://" + root)
	require.NoError(t, err)

	// A deletion scheduler because the sink stamps every subtree file it
	// writes with a delete-at-height, and the file store refuses such a write
	// without one; a coinbase-only block writes no subtree file, so the
	// default harness never needed it. The same construction
	// pipeline_park_recovery_test.go uses.
	realStore, err := file.New(ulogger.TestLogger{}, storeURL,
		options.WithBlobDeletionScheduler(&recordingDeletionScheduler{}),
		options.WithStoreType(storetypes.TEMPSTORE),
	)
	require.NoError(t, err)

	store := &parkReadFaultStore{Store: realStore}

	// Regtest params, so the store seeds the regtest genesis the header cache
	// below is filled from and minedBlocks builds on.
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.TempStore = storeURL

	// The harness has no utxoStore, so legacyOutpointOnly is false and every
	// block above height 1 waits on its parent's mined_set before it commits.
	// chainHolds and the spy both store with mined_set, so the wait is one
	// lookup; a test that stores a parent WITHOUT it (chainHoldsUnmined) wants
	// the wait to give up in milliseconds, not the default 45 retries.
	tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 1
	tSettings.BlockValidation.IsParentMinedRetryBackoffMultiplier = 1
	tSettings.BlockValidation.IsParentMinedRetryBackoffDuration = time.Millisecond

	ctx := context.Background()

	chainURL, err := url.Parse("sqlitememory:///park_wiring_" + strconv.FormatInt(parkWiringStoreCounter.Add(1), 10))
	require.NoError(t, err)

	bcStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, chainURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bcStore.Close(ctx) })

	local, err := blockchain2.NewLocalClient(ulogger.TestLogger{}, tSettings, bcStore, store, nil)
	require.NoError(t, err)

	chain := &parkWiringChain{ClientI: local}
	chain.fsm.Store(&fsmState)

	validation := &convertedRouteSpyValidation{chain: chain}

	sm := newRaceManager(t)
	sm.ctx = ctx
	sm.settings = tSettings
	// chainParams stays newRaceManager's default (MainNetParams) deliberately:
	// several tests outside this file (e.g. TestParkedDispatchMayBeWindowed)
	// build their own manager against this package's default chainParams and
	// windowRoute reads sm.chainParams.Checkpoints, so switching it here would
	// silently change which heights those tests' preconditions see as
	// below-checkpoint. pipelineBlockSink itself checks no PoW (only the
	// merkle root against the header) and reads no checkpoint for the file
	// type, which is .subtreeToCheck everywhere, so mainnet checkpoints over
	// these fixture heights change nothing this file's tests assert on.
	sm.blockchainClient = chain
	// The commit is real: HandleConvertedBlock ends in ProcessBlock, and with a
	// real chain every block whose parent is stored gets that far.
	sm.blockValidation = validation
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
	// per-peer depth floors an unmeasured peer at unmeasuredPeerDepth (1) until
	// its speed is known. A harness peer never streams a real block through
	// trackBlockStreams, so without a seeded rate it would stay "unmeasured"
	// for the harness's whole life and silently cap every test in this file at
	// one request in flight, whatever legacy_maxBlocksInTransitPerPeer says.
	sm.streams = newStreamRegistry()
	sm.streams.rates[syncPeer] = 1

	sm.headersFirstMode.Store(true)

	// assignWantedBlocks reads the header cache, one node per block, in order,
	// none of them requested yet. The fresh store's best block is genesis at
	// height 0, which is exactly the height these blocks (1, 2, 3, ...) sit
	// above.
	//
	// Genesis is included as the run's own height-0 entry, not merely the
	// anchor it is filled from, so the cache describes the same chain the
	// store holds. The store really does hold genesis, so blocks[0] is
	// committable the moment it is delivered (parentIsInChain,
	// streaming_install.go), while blocks[1] and blocks[2] park until the
	// block under them is stored.
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

	return &parkWiringHarness{sm: sm, chain: chain, peer: syncPeer, rec: rec, parkDir: parkDirectory(storeURL), blocks: blocks, store: store, validation: validation, genesis: genesisHeader.BlockHash()}
}

// fixtureBlock returns the harness block with this hash.
func (h *parkWiringHarness) fixtureBlock(t *testing.T, hash chainhash.Hash) *bsvutil.Block {
	t.Helper()

	for _, b := range h.blocks {
		if bh := b.MsgBlock().BlockHash(); bh.IsEqual(&hash) {
			return b
		}
	}

	t.Fatalf("fixtureBlock: %s is not one of the harness's blocks; a real store cannot hold a block at an arbitrary hash", hash)

	return nil
}

// storeBlock puts a harness block into the real chain store with the given
// options, as a commit that happened outside this test would have. Storing a
// block the chain already holds is a no-op, so a block the spy has already
// committed can be named again.
func (h *parkWiringHarness) storeBlock(t *testing.T, hash chainhash.Hash, opts ...chainoptions.StoreBlockOption) {
	t.Helper()

	if hash.IsEqual(&h.genesis) {
		return
	}

	blk, err := model.NewBlockFromMsgBlock(h.fixtureBlock(t, hash).MsgBlock(), h.sm.settings)
	require.NoError(t, err)

	err = h.chain.AddBlock(h.sm.ctx, blk, "", opts...)
	if err != nil && errors.Is(err, errors.ErrBlockExists) {
		return
	}

	require.NoError(t, err)
}

// chainHolds puts this block into the real chain, committed and mined, the way
// a commit this node made before the test started would have left it. For
// genesis it is a no-op: genesis is always stored.
//
// The sweep asks GetBlockHeader rather than GetBlockExists because invalidation
// is a flag on the row and not a delete, so existence alone cannot say whether a
// parent is usable; both read the same real row here.
func (h *parkWiringHarness) chainHolds(t *testing.T, hash chainhash.Hash) {
	t.Helper()

	h.storeBlock(t, hash, chainoptions.WithMinedSet(true), chainoptions.WithSubtreesSet(true))
}

// chainHoldsUnmined stores the block committed but with mined_set still clear,
// which is the state a parent is in between block validation storing it and
// setTxMined finishing.
func (h *parkWiringHarness) chainHoldsUnmined(t *testing.T, hash chainhash.Hash) {
	t.Helper()

	h.storeBlock(t, hash, chainoptions.WithSubtreesSet(true))
}

// parkedEntry returns the park's own entry for hash, failing the test if it is
// not parked.
func (h *parkWiringHarness) parkedEntry(t *testing.T, hash chainhash.Hash) parkedBlock {
	t.Helper()

	h.sm.blockPark.mu.Lock()
	defer h.sm.blockPark.mu.Unlock()

	entry, ok := h.sm.blockPark.entries[hash]
	require.True(t, ok, "%s is not parked", hash)

	return *entry
}

// requireCommitted asserts the end state a committed block must reach: it is in
// the real chain, and block validation was handed it exactly once.
func (h *parkWiringHarness) requireCommitted(t *testing.T, hash chainhash.Hash) {
	t.Helper()

	exists, err := h.chain.GetBlockExists(h.sm.ctx, &hash)
	require.NoError(t, err)
	require.True(t, exists, "%s must be in the chain, not merely gone from the park", hash)
	require.Equal(t, 1, h.validation.callsFor(hash), "block validation must have been handed %s exactly once", hash)
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

	converted, err := h.sm.pipelineBlockSink(hash, &msgBlock.Header, bytes.NewReader(body), sinkPayloadLen(body))
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

	// Now the parent arrives. Its own parent is genesis, which the real store
	// holds, so the streaming route's parentIsInChain check
	// (streaming_install.go) passes and the parent can be committed now.
	//
	// New() builds this channel unconditionally; wired by hand here so the
	// parent's own arrival posts rather than committing through
	// handleBlockOnDiskMsg's own direct call, which see drainOneParkCommit's
	// own doc comment for why to avoid.
	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	require.NoError(t, h.deliver(t, 0))
	h.drainOneParkCommit(t)

	// Both blocks are in the real chain, each handed to block validation once:
	// the parent by its own arrival, the child by the drain behind it.
	h.requireCommitted(t, h.blocks[0].MsgBlock().BlockHash())
	h.requireCommitted(t, child)

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

	// See drainOneParkCommit's own doc comment for why this channel needs to
	// be wired by hand for the parent's own arrival to commit correctly.
	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	require.NotPanics(t, func() {
		require.NoError(t, h.deliver(t, 0))
	}, "a parked block whose peer has gone must still commit")

	h.drainOneParkCommit(t)

	h.requireCommitted(t, h.blocks[0].MsgBlock().BlockHash())
	h.requireCommitted(t, child)

	require.Zero(t, h.sm.blockPark.Len(), "losing the delivering peer must not lose the block")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "the block must have been committed, not written off as a failure")
}

// TestSyncManager_NothingIsDrainedAfterABlockThatDidNotCommit pins the guard
// that tells the two apart: nothing may be drained behind a block that did not
// go into the chain.
//
// The parent (blocks[0]) arrives with its own parent, genesis, in the chain, so
// it is committed on arrival, and block validation refuses it. That is the
// mechanism a node meets: a block that reaches the commit and is judged
// invalid. The child parked behind it must be left exactly where it is, with
// its blob, unjudged and never offered to block validation. Draining it would
// either fail it for a missing parent or, worse, commit it on top of a block
// this node has just rejected.
func TestSyncManager_NothingIsDrainedAfterABlockThatDidNotCommit(t *testing.T) {
	h := newParkWiringHarness(t, true)

	parent := h.blocks[0].MsgBlock().BlockHash()
	child := h.blocks[1].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// Block validation's verdict on the parent, at the one place a production
	// commit failure originates.
	h.validation.failOnce(parent, errors.NewBlockInvalidError("refused"))

	require.NoError(t, h.deliver(t, 0))

	_, parentFailed := h.sm.recentlyFailedBlocks.Get(parent)
	require.True(t, parentFailed, "the parent was judged, so it is remembered as failed")

	exists, err := h.chain.GetBlockExists(h.sm.ctx, &parent)
	require.NoError(t, err)
	require.False(t, exists, "a refused block is not in the chain")

	require.Equal(t, 1, h.sm.blockPark.Len(),
		"the parent never landed, so nothing may be drained behind it")
	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".block",
		"the child's blob must still be on disk; a drain that should not have run would have given it up")
	require.Zero(t, h.validation.callsFor(child), "the child must never have been offered to block validation")

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
	// the tracing call that names the peer. It is stored without mined_set, so
	// the call is stopped just after, on the parent's mined status, and the
	// test does not need the whole ingest pipeline. The harness's retry
	// settings make that wait give up after one retry.
	h.chainHoldsUnmined(t, prev)

	h.sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = false

	require.NotPanics(t, func() {
		err := h.sm.HandleConvertedBlock(context.Background(), nil, hash, blk)
		require.Error(t, err, "the parent is not mined, so this must fail there, not on a nil peer")
		require.True(t, errors.Is(err, errors.ErrBlockParentNotMined), "the failure must be the mined wait's, not something earlier: %v", err)
	})

	require.Zero(t, h.validation.callsFor(hash), "a block whose parent is not mined yet never reaches block validation")
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
	h.chainHolds(t, h.blocks[1].MsgBlock().Header.PrevBlock)

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	h.requireCommitted(t, h.blocks[1].MsgBlock().BlockHash())

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
