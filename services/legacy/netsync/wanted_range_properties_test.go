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
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// propertyDepth is the read-ahead depth these tests run with. Each test sets
// legacy_blockDownloadWindow to this value directly: the wanted range's depth
// and the node-wide request budget are the same setting now, so binding one
// binds the other.
//
// It is deliberately smaller than every other bound a pass is subject to — the
// per-peer in-flight cap and the block-size ladder's 20 — so that when an
// assertion holds, the wanted range is the only thing that can be holding it
// up. A depth equal to the per-peer cap, which is what assignHarness runs
// with, would leave two candidate explanations for every pass that stopped
// where it did.
const propertyDepth = 4

// propertyPasses is how many times each test drives fetchHeaderBlocks. The
// number matters: on 2026-09-12 the runaway was not visible in one pass, it was
// a ratchet that lifted the ceiling a little on every arrival, so a property
// about read-ahead has to be measured over sustained passes rather than one.
const propertyPasses = 40

// heightOfRequested resolves a requested hash back to the height the header
// cache gave it, so a test can assert on POSITION rather than on a count.
// Counting is what failed twice on 2026-09-12: a block 5000 ahead and a block 1
// ahead count the same.
//
// The cache maps height to hash, not hash to height, so this walks every height
// it names — bounded by Top(), which is at most a few hundred in these
// harnesses — rather than adding a reverse index to the cache itself for a
// question only tests ask.
func heightOfRequested(t *testing.T, sm *SyncManager, hash chainhash.Hash) (int32, bool) {
	t.Helper()

	top, ok := sm.headerCache.Top()
	if !ok {
		return 0, false
	}

	best, _, _ := sm.committedTip()

	for height := best + 1; height <= top; height++ {
		if candidate, named := sm.headerCache.At(height); named && candidate == hash {
			return height, true
		}
	}

	return 0, false
}

// runPass drives one wanted-range pass and returns the hashes it asked for.
//
// How many it asked for is known synchronously: requestBlocks records a block in
// the download ledger before the getdata is built, so the ledger's growth over
// the call is exactly the number of hashes now on their way to the peer. The
// send itself is asynchronous, so the recorder is then waited on for that exact
// number rather than for "something", and a pass that placed nothing is not
// waited on at all. Waiting a fixed period on every pass instead would add two
// seconds to each of the passes that correctly ask for nothing, and would accept
// a short pass as a complete one.
func runPass(t *testing.T, sm *SyncManager, rec *getDataRecorder, seen int, pass int) []chainhash.Hash {
	t.Helper()

	owed := sm.blockDownloads.Len()

	sm.fetchHeaderBlocks()

	placed := sm.blockDownloads.Len() - owed
	if placed <= 0 {
		return nil
	}

	require.True(t, WaitUntil(func() bool { return rec.count() >= seen+placed }, 5*time.Second),
		"pass %d recorded %d requests in the download ledger but only %d of them reached the peer",
		pass, placed, rec.count()-seen)

	return rec.all()[seen:]
}

// TestWantedRange_TheDownloaderCannotOutrunTheCommitter is the whole of
// 2026-09-12 in one assertion. With the committer stopped, two attempts to bound
// this by COUNTING outstanding blocks failed, because a block 5000 ahead and a
// block 1 ahead count the same. Bounding by position makes it arithmetic: at
// most depth blocks exist above the best block processed, so at most depth can
// ever be outstanding.
//
// The committer is set once and then frozen, which is exactly the state mainnet
// was in: blocks were arriving at full speed and the chain was settled at 868
// with the download front at 4877. What let the front run was that every ARRIVAL
// raised the bound. So this drives arrivals — the ledger entry is released the
// way handleBlockMsg releases it when a block lands — and requires that
// releasing them buys the downloader no ground at all.
//
// Two assertions, and the second is the one that matters. The ledger's length is
// the symptom; the height ceiling is the property. A design that bounded only
// the count could satisfy the first while asking for height 5000, and that is
// the design this replaces.
func TestWantedRange_TheDownloaderCannotOutrunTheCommitter(t *testing.T) {
	// Headers run a long way past anything the node may ask for, so a short
	// header cache cannot be what stops the pass.
	sm := assignManager(t, 1, 400)
	sm.settings.Legacy.BlockDownloadWindow = propertyDepth

	// Every other bound lifted clear of the depth. The ladder caps this at 20
	// whatever is asked for, and 20 is five times the depth, which is the point:
	// a pass that stops at four blocks stopped because of the range.
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 20

	peer, rec := schedulerPeer(t, sm, 1, 5000)
	wireStreamingPath(sm, peer)

	require.Greater(t, schedulerPeerBudget(sm), propertyDepth,
		"the peer's own budget must exceed the depth or it, and not the wanted range, is what bounds a pass")

	// The committer's position, frozen for the whole test. Nothing in this test
	// ever writes it again.
	const best = int32(100)

	mockCommittedTip(t, sm, uint32(best), 0)

	seen := 0
	highest := int32(0)

	for pass := 0; pass < propertyPasses; pass++ {
		fresh := runPass(t, sm, rec, seen, pass)
		seen += len(fresh)

		require.LessOrEqual(t, sm.blockDownloads.Len(), propertyDepth,
			"pass %d left more blocks outstanding than the read-ahead depth allows", pass)

		for _, hash := range fresh {
			height, found := heightOfRequested(t, sm, hash)
			require.True(t, found, "pass %d asked for a hash the header list cannot name", pass)

			require.Greater(t, height, best,
				"pass %d asked for a block at or below the committed block", pass)
			require.LessOrEqual(t, height, best+propertyDepth,
				"pass %d asked for height %d, which is more than %d blocks above the frozen committer at %d",
				pass, height, propertyDepth, best)

			if height > highest {
				highest = height
			}

			// The arrival. handleBlockMsg discharges the obligation with exactly
			// this call when a block lands, and discharging it is the only thing
			// that lets the next pass ask for anything at all. Under the design
			// this replaces it was also what lifted the ceiling: the header list
			// front advanced on arrival and the ceiling followed the front.
			//
			// This is the ledger's own method, called with the owner the ledger
			// itself recorded, and it is the whole of what an arrival does to the
			// ledger. It is not a stand-in for handleBlockMsg, which does a great
			// deal more; the third test drives that path in full.
			for _, owner := range sm.blockDownloads.OwnersOf(hash) {
				sm.blockDownloads.RemoveOwner(owner, hash)
			}
		}
	}

	// Without this the loop above could be vacuously true — forty passes that
	// each asked for nothing would satisfy every assertion in it.
	require.Greater(t, seen, propertyDepth,
		"the passes must have kept asking for blocks, or the bound above was never tested")
	require.Equal(t, best+propertyDepth, highest,
		"the passes should have reached the top of the window and stopped there")
}

// TestWantedRange_ARestartingNodeRequestsOnItsFirstPass pins the case the old
// design could not serve. Before this, lastCommittedHeight was written only when
// a block committed and was never seeded, so it read zero on a node restarting
// mid-chain, and the wanted range derived from zero names heights no restarted
// node's header cache holds.
//
// The counter is set here the way New sets it, rather than asserted to be zero:
// zero is the bug, not the state under test.
func TestWantedRange_ARestartingNodeRequestsOnItsFirstPass(t *testing.T) {
	// A node a long way up the chain, with headers for the run it is about to
	// fetch, at the heights they really have.
	const restartHeight = int32(800_000)

	sm := assignManager(t, restartHeight, restartHeight+200)
	sm.settings.Legacy.BlockDownloadWindow = propertyDepth

	// committedTip reads the chain directly on every call now, so a restarting
	// node's first pass sees its real height with nothing to seed. Without this
	// mock the manager has no blockchain client at all and committedTip answers
	// height 0, which is the bug this pins the absence of.
	mockCommittedTip(t, sm, uint32(restartHeight), 0)

	_, rec := schedulerPeer(t, sm, 1, restartHeight+1000)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"a node restarting mid-chain must ask for blocks on its first pass")
}

// TestNew_CommittedTipReadsTheChainThroughTheConstructor is what is left of
// the old seeding regression guard now that there is nothing left to seed.
// Before this fix, lastCommittedHeight was written only when a block
// committed and was never primed at startup, so it read zero on a node
// restarting mid-chain until this process's own first commit — and New had to
// be caught making a seeding call to fix that. committedTip reads
// sm.blockchainClient directly on every call now, so there is no seeding step
// for New to own or for a regression to drop; the only thing left for this to
// guard is that New actually wires blockchainClient at all, which this proves
// by running the real constructor and reading the tip back through it.
func TestNew_CommittedTipReadsTheChainThroughTheConstructor(t *testing.T) {
	const chainHeight = uint32(800_000)

	// Cancellable rather than context.Background(): New starts a goroutine
	// (startKafkaListeners) that polls sm.blockchainClient.IsFSMCurrentState
	// once a second for as long as ctx is alive, and this test's mock has
	// nothing else it needs to keep answering once the test is done.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: chainHeight}, nil)
	// startKafkaListeners' ticker goroutine calls this every second regardless
	// of what this test is pinning; without a stub the mock panics the whole
	// test binary the first time it fires.
	client.Mock.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(false, nil)

	config := &Config{
		ChainParams: &chaincfg.MainNetParams,
		// Checkpoints are irrelevant to this test and pull in a real header walk
		// against bestHeader, which is a bare stand-in and not a chain the
		// checkpoint tables know about.
		DisableCheckpoints: true,
	}

	tSettings := &settings.Settings{}

	sm, err := New(
		ctx,
		ulogger.TestLogger{},
		tSettings,
		client,
		&validator.MockValidator{},
		&utxo.MockUtxostore{},
		blob_memory.New(),
		parkTempStore(t, tSettings),
		&subtreevalidation.MockSubtreeValidation{},
		&blockvalidation.MockBlockValidation{},
		blockassembly.NewMock(),
		config,
	)
	require.NoError(t, err)

	height, _, ok := sm.committedTip()
	require.True(t, ok, "New must wire a blockchain client committedTip can read")
	require.Equal(t, int32(chainHeight), height,
		"a node built against a chain at 800,000 must not believe its best block is 0")
}

// newParkPropertyManager builds a manager the wanted-range pass and the block
// queue's own delivery path can both run against: a real park over a real file
// blob store, a header list naming every one of the mined blocks at its true
// height, and a blockchain that holds nothing.
//
// Holding nothing is the point. Every parent lookup fails, so every block that
// arrives is an orphan and parks, and nothing ever commits — which is the
// committer stopped, the condition the park bound has to survive. It is also
// what mainnet looked like on 2026-09-12 during the genesis resync: blocks
// arriving at full speed with the chain settled far below them.
//
// Modelled on newParkWiringHarness, which mines three blocks; this needs enough
// of them for a runaway to have somewhere to run to.
func newParkPropertyManager(t *testing.T, blocks []*bsvutil.Block) (*SyncManager, *getDataRecorder) {
	t.Helper()

	// The real constructor registers these; a struct-literal manager reaches the
	// same gauges on the commit path.
	initPrometheusMetrics()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	client.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 0}, nil)
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{{}}, nil)
	client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	client.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	store, err := blob.NewStore(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.TempStore = storeURL
	tSettings.Legacy.BlockDownloadWindow = propertyDepth

	// As in the first test: every other bound lifted clear of the depth, so the
	// wanted range is the only candidate explanation for where a pass stops.
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 20

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.settings = tSettings
	// minedBlocks solves against RegressionNetParams; blockOrigin and
	// quickValidationAllowed read sm.chainParams.Checkpoints, so this must
	// agree with what the header cache below is filled from.
	sm.chainParams = &chaincfg.RegressionNetParams
	sm.blockchainClient = client
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](100)
	sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](time.Minute)
	sm.blockPark = mustNewBlockPark(t, ulogger.TestLogger{}, tSettings, store)
	require.NotNil(t, sm.blockPark, "the park must be built or this test measures nothing")
	// The pipeline sink writes a block's subtree files to this store before the
	// park ever sees it (deliverPropertyBlock below), same store as the park's
	// own — see newPipelineParkManager's "one store, not two" note.
	sm.subtreeStore = store

	t.Cleanup(func() { sm.recentlyFailedBlocks.Stop() })

	peer, rec := schedulerPeer(t, sm, 1, int32(len(blocks))+1000)
	sm.storeSyncPeer(peer, &syncPeerState{})

	// parkOrphanBlock's own top-up (fetchMoreHeaderBlocks) can also fire a pass
	// as each block below is delivered, alongside the explicit ones the loop
	// below drives. That is harmless to what is asserted here: blockDownloads.Len
	// and blockPark.Len are both read fresh around each call, not counted by how
	// many distinct calls fired, and every pass — this loop's or a top-up's —
	// answers to the same depth ceiling. So the park bound holds regardless of
	// which caller triggered a given pass.

	// The wanted-range pass reads the header cache, so that is what has to name
	// the same run — from the blocks' own real headers, which really
	// do link from genesis, rather than a synthetic chain: this harness delivers
	// the blocks themselves through the streaming pipeline (deliverPropertyBlock),
	// and a hash the cache named that did not match a delivered block's real hash
	// would test nothing.
	headers := make([]*wire.BlockHeader, 0, len(blocks))

	for _, b := range blocks {
		h := b.MsgBlock().Header
		headers = append(headers, &h)
	}

	regtestParams := chaincfg.RegressionNetParams

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(*regtestParams.GenesisHash, 1, headers),
		"the mined chain must genuinely link from genesis or this harness is not testing what it claims to")

	sm.headersFirstMode.Store(true)

	return sm, rec
}

// deliverPropertyBlock streams one block through the pipeline sink and the
// on-disk consumer path — the same route block_park_wiring_test.go's
// parkWiringHarness.deliver drives — since processQueuedBlock and the decoded
// blockQueueMsg it took no longer exist; every arrival now goes through
// pipelineBlockSink and handleBlockOnDiskMsg (streaming_install.go).
func deliverPropertyBlock(t *testing.T, sm *SyncManager, peer *peerpkg.Peer, blk *bsvutil.Block, hash chainhash.Hash) error {
	t.Helper()

	msgBlock := blk.MsgBlock()
	body := blockBodyBytes(t, blk)

	sm.blockDownloads.Add(peer, hash)

	converted, err := sm.pipelineBlockSink(hash, &msgBlock.Header, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return err
	}

	sm.handleBlockOnDiskMsg(&blockOnDiskMsg{
		body: peerpkg.BlockBody{
			Header:    msgBlock.Header,
			TxCount:   uint64(len(msgBlock.Transactions)),
			Size:      int64(len(body)),
			Hash:      hash,
			Converted: converted,
		},
		peer: peer,
	})

	return nil
}

// TestWantedRange_TheParkNeverExceedsTheReadAheadDepth pins the consequence. A
// park with a count-based entry cap could fill above a gap and refuse the one
// block that would extend the tip, the drain would find no child for the
// settled hash, and the node would idle with thousands of unusable blocks on
// disk. That is what the 4096-entry cap did on 2026-09-12, and that cap is gone
// now: this property is what bounds the park in its place.
//
// The park is bounded because the download is bounded by position: a block can
// only be parked if it was obtained, it can only be obtained if it was
// requested, and the wanted range will not name a height more than
// legacy_blockDownloadWindow above the last committed block. So the park
// cannot exceed the depth — 4 here, 1024 by default.
//
// Blocks are delivered highest-first so every one of them is an orphan on
// arrival, which is the case the park exists for and the case that filled it.
//
// Two honest limits, stated rather than buried.
//
// The committer is frozen ABOVE zero rather than at it. Mainnet on 2026-09-12
// was settled at 868 with the download front at 4877, so a stopped committer
// part-way up is the faithful reproduction — but there is a sharper reason.
// lastCommittedHeight reading zero makes wantedBlocks derive its depth from two
// different anchors: best is zero while the ceiling falls back to the front of
// the header list, so the effective depth becomes front plus the configured
// depth rather than the configured depth. The park then holds one more block
// than the depth allows. That is the same unseeded-counter defect
// TestWantedRange_ARestartingNodeRequestsOnItsFirstPass names, seen from the
// other side, and it belongs in that test rather than smuggled into this one.
//
// With nothing committing, the arrival of a block also removes its header from
// the list, so a second mechanism reinforces the first — the range cannot
// re-name a height whose header has gone. That is why the load-bearing assertion
// here is the POSITION one, that no block above the window is ever in the park at
// all. Header removal can only take away a block already obtained; it cannot be
// what stops a block twenty heights up being requested in the first place.
func TestWantedRange_TheParkNeverExceedsTheReadAheadDepth(t *testing.T) {
	// Six times the depth, so a pass that ran away would have somewhere to run.
	blocks := minedBlocks(t, propertyDepth*6)

	sm, rec := newParkPropertyManager(t, blocks)

	// The committer's position, frozen for the whole test. Nothing here ever
	// writes it again, and nothing can: the blockchain mock holds no block, so
	// every arrival is an orphan and no block ever commits.
	const parkBest = int32(propertyDepth)

	mockCommittedTip(t, sm, uint32(parkBest), 0)

	// Every block above the window, by hash, so the position assertion can be
	// made against the park directly.
	aboveWindow := make(map[chainhash.Hash]int32, len(blocks))
	for i, b := range blocks {
		if height := int32(i + 1); height > parkBest+propertyDepth {
			aboveWindow[b.MsgBlock().BlockHash()] = height
		}
	}

	byHash := make(map[chainhash.Hash]int, len(blocks))
	for i, b := range blocks {
		byHash[b.MsgBlock().BlockHash()] = i
	}

	seen := 0

	for pass := 0; pass < propertyPasses; pass++ {
		fresh := runPass(t, sm, rec, seen, pass)
		seen += len(fresh)

		// Highest first, so each block arrives before its parent and parks. The
		// pass hands runs out in ascending order, so reversing what it asked for
		// is the worst case the park can be given.
		for i := len(fresh) - 1; i >= 0; i-- {
			index, known := byHash[fresh[i]]
			require.True(t, known, "pass %d asked for a block the harness never mined", pass)

			require.NoError(t, deliverPropertyBlock(t, sm, sm.loadSyncPeer(), blocks[index], fresh[i]),
				"pass %d: delivering block %d", pass, index+1)
		}

		require.LessOrEqual(t, sm.blockPark.Len(), propertyDepth,
			"pass %d left the park holding more blocks than the read-ahead depth allows", pass)

		for hash, height := range aboveWindow {
			require.False(t, sm.blockPark.Has(hash),
				"pass %d parked the block at height %d, which is more than %d above the committer frozen at %d",
				pass, height, propertyDepth, parkBest)
		}
	}

	// Without this the loop could be vacuously true: a park that was never given
	// anything satisfies every bound in it.
	require.Positive(t, sm.blockPark.Len(), "the passes must actually have parked something")
}
