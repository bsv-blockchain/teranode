package blockvalidation

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

// forkHeadersFrom builds n mined regtest headers on parent, each bound to the coinbase a
// coinbase-only body at its height would carry. The first timestamp is skewed by tsSkew seconds so
// that two chains built on the same parent differ: CreateSyntheticChainFromAtHeight is deterministic
// in its parent and height, so it would rebuild a stored child byte for byte.
func forkHeadersFrom(t *testing.T, parent *model.BlockHeader, n int, startHeight, tsSkew uint32) []*model.BlockHeader {
	t.Helper()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	headers := make([]*model.BlockHeader, n)
	prevHash := parent.Hash()
	timestamp := parent.Timestamp + tsSkew

	for i := 0; i < n; i++ {
		coinbaseTx := testhelpers.CreateSimpleCoinbaseTx(startHeight + uint32(i)) //nolint:gosec // test height, small
		timestamp++

		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: coinbaseTx.TxIDChainHash(),
			Timestamp:      timestamp,
			Bits:           *nBits,
		}
		testhelpers.MineHeader(header)

		headers[i] = header
		prevHash = header.Hash()
	}

	return headers
}

// storeMinedBlock writes a coinbase-only block for header into the real store with mined_set true,
// as a block this node has fully processed.
func storeMinedBlock(ctx context.Context, t *testing.T, store blockchain_store.Store, header *model.BlockHeader, height uint32) {
	t.Helper()

	coinbaseTx := testhelpers.CreateSimpleCoinbaseTx(height)
	block := &model.Block{
		Header:           header,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		SizeInBytes:      uint64(80 + coinbaseTx.Size()), //nolint:gosec // small positive size
		Height:           height,
	}

	_, _, err := store.StoreBlock(ctx, block, "test", options.WithMinedSet(true))
	require.NoError(t, err)
}

// minedSetClearRecorder wraps the real sqlitememory-backed client and counts ClearBlockMinedSet
// calls, delegating each one. Reading mined_set back after catchup() is racy: step 9.6 sends a
// BlockMinedUnset notification per block, and block validation's background job re-marks those
// blocks mined on its own schedule. The call count is what step 9.6 actually did.
type minedSetClearRecorder struct {
	blockchain.ClientI

	clears atomic.Int64
}

func (r *minedSetClearRecorder) ClearBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	r.clears.Add(1)

	return r.ClientI.ClearBlockMinedSet(ctx, blockHash)
}

// storeSideBlock writes a coinbase-only block for header into the real store without mined_set, as
// a block this node validated but never adopted. opts are passed to StoreBlock, so
// options.WithInvalid(true) stores a side block this node rejected.
func storeSideBlock(ctx context.Context, t *testing.T, store blockchain_store.Store, header *model.BlockHeader, height uint32, opts ...options.StoreBlockOption) {
	t.Helper()

	coinbaseTx := testhelpers.CreateSimpleCoinbaseTx(height)
	block := &model.Block{
		Header:           header,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		SizeInBytes:      uint64(80 + coinbaseTx.Size()), //nolint:gosec // small positive size
		Height:           height,
	}

	_, _, err := store.StoreBlock(ctx, block, "test", opts...)
	require.NoError(t, err)
}

// ancestorLinkRun is what one catchup() run left behind: the error, how many header and /blocks/
// requests reached the peer, how many local blocks step 9.6 un-stamped, and the real client plus
// the two header chains, so the caller can read the end state from the store.
type ancestorLinkRun struct {
	err             error
	headersRequests int
	blocksRequests  int
	otherRequests   int // requests for anything but headers and /blocks/, such as /subtree_data
	minedClears     int64
	breakerFailures int    // failures counted on the peer's circuit breaker after the catchup
	errorType       string // previousCatchupAttempt.ErrorType, how releaseCatchupLock classified err
	client          blockchain.ClientI
	mainChain       []*model.BlockHeader // b1..b10, the local chain; b5 is the common ancestor
	offered         []*model.BlockHeader // every header the peer offers after the ancestor, all batches
}

// offerShape is the header offer the peer serves after the common ancestor b5.
type offerShape int

const (
	// offerLinked: h1..h5, h1 built on b5. An honest fork of depth 5.
	offerLinked offerShape = iota
	// offerFirstOnOther: h1..h5, h1 built on B, a stored block other than the ancestor.
	offerFirstOnOther
	// offerBreakAtSecond: h1 built on b5, then h2..h5 built on B, so the chain breaks after h1.
	offerBreakAtSecond
	// offerNothingNew: the peer serves b5 alone, nothing after the ancestor.
	offerNothingNew
	// offerBreakBetweenBatches: a full first batch b5, h1..h9999, then a second batch built on h1
	// rather than on h9999, the last header the first batch ended on.
	offerBreakBetweenBatches
	// offerReplayBetweenBatches: a full first batch b5, h1..h9999, then every later batch replays
	// h1..h9999 and adds one fresh header f1 on h9999. Each replayed batch builds on b5, not on the
	// header the batch before it ended on. The target is f2 on f1, which the peer never serves, so
	// reaching the target never ends the fetch.
	offerReplayBetweenBatches
	// offerWrongBits: h1..h5 linked on b5, where h1 is mined against bits other than its parent's.
	// Regtest never adjusts difficulty, so the bits every block must carry are its parent's.
	offerWrongBits
	// offerThroughStoredSideBranch: s6..s9 built on b5 and already stored, a side branch this node
	// validated but never adopted, then h10, h11 on s9. Every header up to s9 is stored, but only
	// b5 is on our current chain.
	offerThroughStoredSideBranch
	// offerThroughInvalidSideBranch: as offerThroughStoredSideBranch, but s6..s9 are stored marked
	// invalid, a side branch this node rejected.
	offerThroughInvalidSideBranch
	// offerThroughInvalidAboveTip: s6..s10 built on b5 and stored valid, s11 on s10 stored marked
	// invalid, then h12, h13 on s11. s11 is the only stored header above our tip at b10, so the
	// ancestor walk stops at its height ceiling before it reads s11's invalid flag.
	offerThroughInvalidAboveTip
	// offerThroughValidThenInvalidAboveTip: s6..s11 built on b5 and stored valid, s12 on s11 stored
	// marked invalid, then h13, h14 on s12. b10 is mined at a harder difficulty, so our tip stays
	// at b10 although s11 is higher, and the invalid block is not the first one above our tip.
	offerThroughValidThenInvalidAboveTip
	// offerSideBranchOnly: the peer serves s6..s9, h10, h11 without b5 in front. s6..s9 are stored
	// but none is on our current chain, and our tip does not move, so the reply cannot have come
	// from our locator.
	offerSideBranchOnly
	// offerSideBranchOnlyAfterReorg: as offerSideBranchOnly, but while the peer answers the header
	// request our own chain reorgs to c8..c11 on b7, away from b10, the block the locator was
	// built from.
	offerSideBranchOnlyAfterReorg
)

// runAncestorLinkCatchup runs catchup() against a real sqlitememory blockchain store holding the
// local chain genesis..b10, every block mined, plus B, a second stored block (a side block on
// genesis at height 1). The peer serves [b5] followed by the headers shape describes, and for the
// two between-batches shapes a different answer to every header request after the first.
func runAncestorLinkCatchup(t *testing.T, shape offerShape) ancestorLinkRun {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	params := *tSettings.ChainCfgParams
	params.CoinbaseMaturity = 100
	// Coinbase-only v1 fixtures: keep the BIP34/65/66 version floors and the coinbase-height rule off.
	params.BIP0034Height = 100000000
	params.BIP0065Height = 100000000
	params.BIP0066Height = 100000000
	params.Checkpoints = []chaincfg.Checkpoint{}
	tSettings.ChainCfgParams = &params
	tSettings.BlockValidation.SecretMiningThreshold = 1000 // keep the secret-mining gate out of the way
	tSettings.BlockValidation.GRPCListenAddress = ""

	store, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(logger, tSettings, store, nil, nil)
	require.NoError(t, err)

	recorder := &minedSetClearRecorder{ClientI: client}

	genesis, _, err := store.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	mainChain := forkHeadersFrom(t, genesis, 10, 1, 0)

	if shape == offerThroughValidThenInvalidAboveTip {
		// Each regtest block adds work 2, and these bits add 8. Above genesis our chain to b10 weighs
		// 26, the stored side branch to s11 weighs 22, and the offer to h14 weighs 28. So b10 stays
		// our tip, and the offer still passes the less-work gate that ends a lighter one as a no-op.
		mainChain[9].Bits = nBitsFrom(t, "201fffff")
		testhelpers.MineHeader(mainChain[9])
	}

	for i, h := range mainChain {
		storeMinedBlock(ctx, t, store, h, uint32(i+1)) //nolint:gosec // i < 10
	}

	otherBlockB := forkHeadersFrom(t, genesis, 1, 1, 500)[0]
	require.NotEqual(t, mainChain[0].Hash().String(), otherBlockB.Hash().String())
	storeMinedBlock(ctx, t, store, otherBlockB, 1)

	ancestor := mainChain[4] // b5

	var offered, served, later []*model.BlockHeader

	var unservedTarget *model.BlockHeader

	switch shape {
	case offerLinked, offerNothingNew:
		offered = forkHeadersFrom(t, ancestor, 5, 6, 1000)
	case offerFirstOnOther:
		offered = forkHeadersFrom(t, otherBlockB, 5, 6, 1000)
	case offerWrongBits:
		h1 := forkHeadersFrom(t, ancestor, 1, 6, 1000)[0]
		h1.Bits = nBitsFrom(t, "1f7fffff")
		testhelpers.MineHeader(h1)
		offered = append([]*model.BlockHeader{h1}, forkHeadersFrom(t, h1, 4, 7, 1000)...)
	case offerBreakAtSecond:
		offered = append(forkHeadersFrom(t, ancestor, 1, 6, 1000), forkHeadersFrom(t, otherBlockB, 4, 7, 2000)...)
	case offerThroughStoredSideBranch, offerThroughInvalidSideBranch, offerSideBranchOnly, offerSideBranchOnlyAfterReorg:
		var opts []options.StoreBlockOption
		if shape == offerThroughInvalidSideBranch {
			opts = append(opts, options.WithInvalid(true))
		}

		sideBranch := forkHeadersFrom(t, ancestor, 4, 6, 700)
		for i, h := range sideBranch {
			storeSideBlock(ctx, t, store, h, uint32(6+i), opts...) //nolint:gosec // i < 4
		}

		offered = append(sideBranch, forkHeadersFrom(t, sideBranch[len(sideBranch)-1], 2, 10, 700)...)
	case offerThroughInvalidAboveTip:
		sideBranch := forkHeadersFrom(t, ancestor, 6, 6, 700)
		for i, h := range sideBranch[:5] {
			storeSideBlock(ctx, t, store, h, uint32(6+i)) //nolint:gosec // i < 5
		}

		storeSideBlock(ctx, t, store, sideBranch[5], 11, options.WithInvalid(true))

		offered = append(sideBranch, forkHeadersFrom(t, sideBranch[5], 2, 12, 700)...)
	case offerThroughValidThenInvalidAboveTip:
		sideBranch := forkHeadersFrom(t, ancestor, 7, 6, 700)
		for i, h := range sideBranch[:6] {
			storeSideBlock(ctx, t, store, h, uint32(6+i)) //nolint:gosec // i < 6
		}

		storeSideBlock(ctx, t, store, sideBranch[6], 12, options.WithInvalid(true))

		offered = append(sideBranch, forkHeadersFrom(t, sideBranch[6], 2, 13, 700)...)
	case offerBreakBetweenBatches, offerReplayBetweenBatches:
		// With b5 in front this is a full batch, so the fetch asks again from h9999.
		offered = forkHeadersFrom(t, ancestor, maxBlockHeadersPerRequest-1, 6, 1000)
	}

	if shape != offerNothingNew {
		served = offered
	}

	// The side-branch-only shapes leave b5 out, so no served header is on our current chain.
	sideBranchOnly := shape == offerSideBranchOnly || shape == offerSideBranchOnlyAfterReorg

	switch shape {
	case offerBreakBetweenBatches:
		later = forkHeadersFrom(t, offered[0], 5, 7, 3000)
	case offerReplayBetweenBatches:
		fresh := forkHeadersFrom(t, offered[len(offered)-1], 2, uint32(6+len(offered)), 3000) //nolint:gosec // small test height
		later = append(append([]*model.BlockHeader{}, offered...), fresh[0])
		unservedTarget = fresh[1]
	}

	bestHeader, _, err := store.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, mainChain[9].Hash().String(), bestHeader.Hash().String(), "b10 must be the local tip")

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	if sideBranchOnly {
		// Without this the UTXO store reports height 0 and the header fetch caps the locator at
		// genesis, which no reorg can take off our current chain. At 10 the locator starts at b10.
		require.NoError(t, utxoStore.SetBlockHeight(10))
	}

	subtreeStore := memory.New()

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    recorder,
		utxoStore:           utxoStore,
		subtreeStore:        subtreeStore,
		forkManager:         NewForkManager(logger, tSettings),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		peerCircuitBreakers: catchup.NewPeerCircuitBreakers(catchup.DefaultCircuitBreakerConfig()),
		headerChainCache:    catchup.NewHeaderChainCache(logger),
	}
	server.fetchSubtreeDataForBlockFn = server.fetchSubtreeDataForBlock
	server.blockValidation = NewBlockValidation(ctx, logger, tSettings, client, subtreeStore, nil, utxoStore, nil, nil)

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)

	responseHeaders := make([]byte, 0, (len(served)+1)*model.BlockHeaderSize)
	if !sideBranchOnly {
		responseHeaders = append(responseHeaders, ancestor.Bytes()...)
	}

	for _, h := range served {
		responseHeaders = append(responseHeaders, h.Bytes()...)
	}

	laterHeaders := make([]byte, 0, len(later)*model.BlockHeaderSize)
	for _, h := range later {
		laterHeaders = append(laterHeaders, h.Bytes()...)
	}

	var headersRequests atomic.Int64

	// For offerSideBranchOnlyAfterReorg, c8..c11 on b7 are stored mined while the peer answers the
	// first header request, after the locator was built from b10. With more work than b10 they
	// become our current chain. The responder runs outside the test's own assertions, so a store
	// error is kept and checked after catchup returns.
	var (
		reorgOnFirstRequest func()
		reorgErr            error
	)

	if shape == offerSideBranchOnlyAfterReorg {
		reorgChain := forkHeadersFrom(t, mainChain[6], 4, 8, 900)
		reorgBlocks := make([]*model.Block, len(reorgChain))

		for i, h := range reorgChain {
			coinbaseTx := testhelpers.CreateSimpleCoinbaseTx(uint32(8 + i)) //nolint:gosec // i < 4
			reorgBlocks[i] = &model.Block{
				Header:           h,
				CoinbaseTx:       coinbaseTx,
				TransactionCount: 1,
				SizeInBytes:      uint64(80 + coinbaseTx.Size()), //nolint:gosec // small positive size
				Height:           uint32(8 + i),                  //nolint:gosec // i < 4
			}
		}

		var reorgOnce sync.Once

		reorgOnFirstRequest = func() {
			reorgOnce.Do(func() {
				for _, b := range reorgBlocks {
					if _, _, err := store.StoreBlock(ctx, b, "test", options.WithMinedSet(true)); err != nil {
						reorgErr = err
						return
					}
				}
			})
		}
	}

	httpmock.RegisterResponder("GET", `=~^http://peer/headers_from_common_ancestor/.*`,
		func(*http.Request) (*http.Response, error) {
			if reorgOnFirstRequest != nil {
				reorgOnFirstRequest()
			}

			if headersRequests.Add(1) > 1 && later != nil {
				return httpmock.NewBytesResponse(200, laterHeaders), nil
			}

			return httpmock.NewBytesResponse(200, responseHeaders), nil
		})
	httpmock.RegisterResponder("GET", `=~^http://peer/blocks/.*`, httpmock.NewBytesResponder(500, nil))

	var otherRequests atomic.Int64

	httpmock.RegisterNoResponder(func(req *http.Request) (*http.Response, error) {
		otherRequests.Add(1)
		return httpmock.ConnectionFailure(req)
	})

	// Add the headers only the later batches carry, so the target is the newest header offered and
	// the end-state check covers every offered block.
	switch shape {
	case offerBreakBetweenBatches:
		offered = append(append([]*model.BlockHeader{}, offered...), later...)
	case offerReplayBetweenBatches:
		offered = append(append([]*model.BlockHeader{}, later...), unservedTarget)
	}

	targetBlock := &model.Block{
		Header: offered[len(offered)-1],
		Height: 10,
	}

	err = server.catchup(ctx, targetBlock, "peer-ancestor-link", "http://peer")
	require.NoError(t, reorgErr, "the reorg the peer's reply races must be stored")

	blocksRequests := 0
	for k, v := range httpmock.GetCallCountInfo() {
		if strings.Contains(k, "http://peer/blocks/") {
			blocksRequests += v
		}
	}

	_, breakerFailures, _, _ := server.peerCircuitBreakers.GetBreaker("peer-ancestor-link").GetStats()

	errorType := ""
	if server.previousCatchupAttempt != nil {
		errorType = server.previousCatchupAttempt.ErrorType
	}

	return ancestorLinkRun{
		err:             err,
		breakerFailures: breakerFailures,
		errorType:       errorType,
		headersRequests: int(headersRequests.Load()),
		blocksRequests:  blocksRequests,
		otherRequests:   int(otherRequests.Load()),
		minedClears:     recorder.clears.Load(),
		client:          client,
		mainChain:       mainChain,
		offered:         offered,
	}
}

// minedAboveAncestor reads mined_set from the real store for b6..b10, the local blocks above the
// ancestor that step 9.6 un-stamps ahead of a reorg. Only meaningful when nothing was cleared: once
// step 9.6 runs, background re-marking makes the flags race (see minedSetClearRecorder).
func minedAboveAncestor(t *testing.T, run ancestorLinkRun) []bool {
	t.Helper()

	mined := make([]bool, 0, 5)

	for _, h := range run.mainChain[5:] {
		isMined, err := run.client.GetBlockIsMined(context.Background(), h.Hash())
		require.NoError(t, err)

		mined = append(mined, isMined)
	}

	return mined
}

// requireRejectedWithoutSideEffects asserts that catchup refused the offer and left the node as
// it was: no block body fetched, no local block un-stamped, b6..b10 still mined, nothing offered
// stored.
func requireRejectedWithoutSideEffects(t *testing.T, run ancestorLinkRun) {
	t.Helper()

	require.Zero(t, run.blocksRequests, "no block may be fetched for a rejected offer")
	require.Zero(t, run.minedClears, "a rejected offer must not un-stamp any local block (step 9.6)")
	require.Equal(t, []bool{true, true, true, true, true}, minedAboveAncestor(t, run),
		"a rejected offer must leave the local blocks above the ancestor mined")

	for _, h := range run.offered {
		exists, err := run.client.GetBlockExists(context.Background(), h.Hash())
		require.NoError(t, err)
		require.False(t, exists, "no offered block may be stored")
	}
}

// requireChargedToPeer asserts that the rejection is charged to the serving peer, not only coded
// as a peer fault: the header fetch recorded a failure on the peer's circuit breaker and no success
// after it (a success in the closed state resets the count), and releaseCatchupLock classified the
// error as network_error, the generic peer error it reports to the peer registry.
func requireChargedToPeer(t *testing.T, run ancestorLinkRun) {
	t.Helper()

	require.True(t, errors.Is(run.err, errors.ErrNetworkInvalidResponse), "a broken offer is a peer fault: %v", run.err)
	require.Equal(t, 1, run.breakerFailures, "the header fetch must record the broken offer as one failure on the peer's circuit breaker, and no success")
	require.Equal(t, "network_error", run.errorType, "releaseCatchupLock must classify the rejection as a peer network error")
}

// TestCatchup_FirstHeaderMustLinkToCommonAncestor covers bitcoin-sv/teranode#4975 against a real
// sqlitememory blockchain store. b5 is the stored common ancestor and b10 the local tip, so every
// offer here is a fork of depth 5. Catchup must reject an offer whose headers do not form one chain
// on b5, as a peer fault (NetworkInvalidResponse), before it un-stamps the local blocks above the
// ancestor and before it fetches any block body, because every height it would assign
// (ancestor + 1 + index) is wrong.
//
// The control subtest runs the same setup with a linked offer. It does un-stamp b6..b10 and does
// reach the block fetch, so the zero clear and fetch counts in the other subtests are not vacuous.
//
// The peer serves b5 in front of the offer, so the header fetch, which checks every link within a
// batch, rejects both broken offers before findCommonAncestor sees them, and charges the peer.
//
// Mutation proofs: moving the mined_set clear back to its old place straight after the
// secret-mining check fails nothing_new_after_ancestor on the clear count. Removing the link check
// in catchupGetBlockHeaders fails both broken-offer subtests on the error message and on the
// circuit-breaker count, because findCommonAncestor then rejects them after the fetch has recorded
// a success.
func TestCatchup_FirstHeaderMustLinkToCommonAncestor(t *testing.T) {
	t.Run("control_linked_chain_unstamps_and_reaches_block_fetch", func(t *testing.T) {
		run := runAncestorLinkCatchup(t, offerLinked)

		require.Positive(t, run.blocksRequests, "a correctly linked chain must reach the block fetch, else the setup is vacuous")
		require.Equal(t, int64(5), run.minedClears, "a correctly linked fork of depth 5 must un-stamp b6..b10, else the setup is vacuous")
	})

	t.Run("first_header_parent_is_other_stored_block", func(t *testing.T) {
		run := runAncestorLinkCatchup(t, offerFirstOnOther)

		require.Error(t, run.err, "catchup must abort when the first header does not link to the common ancestor")
		requireChargedToPeer(t, run)
		require.Contains(t, run.err.Error(), "header batch does not link as one chain")
		requireRejectedWithoutSideEffects(t, run)
	})

	t.Run("chain_breaks_after_first_header", func(t *testing.T) {
		run := runAncestorLinkCatchup(t, offerBreakAtSecond)

		require.Error(t, run.err, "catchup must abort when a later offered header does not link to the one before it")
		requireChargedToPeer(t, run)
		require.Contains(t, run.err.Error(), "header batch does not link as one chain")
		requireRejectedWithoutSideEffects(t, run)
	})

	t.Run("nothing_new_after_ancestor", func(t *testing.T) {
		// The fork depth is still 5, but there is nothing to reorg to, so there is nothing to
		// un-stamp. The old placement of the clear un-stamped b6..b10 here and then returned.
		run := runAncestorLinkCatchup(t, offerNothingNew)

		require.Error(t, run.err, "a peer with nothing after the ancestor and an unknown target is on a dead fork")
		requireRejectedWithoutSideEffects(t, run)
	})
}

// TestCatchup_AncestorMustBeOnCurrentChain covers a peer whose offer runs through a side branch we
// already store, against a real sqlitememory blockchain store. The local chain is b1..b10, and
// s6..s9 on b5 are stored but never adopted. The peer offers [b5, s6..s9, h10, h11]. Every header
// up to s9 is stored, so an ancestor walk that keeps the last stored header picks s9, measures a
// fork depth of 1 and un-stamps only b10, leaving b6..b9 mined while they are reorged out. The
// common ancestor must be b5, the last offered header on our current chain, so the fork depth is
// 5 and the clear un-stamps b6..b10.
//
// Mutation proof: dropping the lastOnCurrentChain call in findCommonAncestor fails the clear
// count with 1 instead of 5.
func TestCatchup_AncestorMustBeOnCurrentChain(t *testing.T) {
	run := runAncestorLinkCatchup(t, offerThroughStoredSideBranch)

	require.Positive(t, run.blocksRequests, "the offer must reach the block fetch, else the clear count is not what the fork depth decided")
	require.Equal(t, int64(5), run.minedClears, "a fork from b5 must un-stamp b6..b10, not only the blocks above the stored side branch")
}

// requireSideBranchOfferUntouched asserts that catchup refused an offer through the stored side
// branch s6..s9 before it touched the node: no block body fetched, no local block un-stamped,
// b6..b10 still mined, and h10, h11, the offered headers past the side branch, not stored. s6..s9
// are stored by construction, so requireRejectedWithoutSideEffects does not fit.
func requireSideBranchOfferUntouched(t *testing.T, run ancestorLinkRun) {
	t.Helper()

	require.Zero(t, run.blocksRequests, "no block may be fetched for a rejected offer")
	require.Zero(t, run.minedClears, "a rejected offer must not un-stamp any local block (step 9.6)")
	require.Equal(t, []bool{true, true, true, true, true}, minedAboveAncestor(t, run),
		"a rejected offer must leave the local blocks above the ancestor mined")

	for _, h := range run.offered[4:] {
		exists, err := run.client.GetBlockExists(context.Background(), h.Hash())
		require.NoError(t, err)
		require.False(t, exists, "no offered block past the stored side branch may be stored")
	}
}

// TestCatchup_InvalidStoredSideBlockAfterAncestorIsRejected covers an offer through a side branch
// this node stores marked invalid, against a real sqlitememory blockchain store. The local chain is
// b1..b10, s6..s9 on b5 are stored invalid, and the peer offers [b5, s6..s9, h10, h11]. When the
// ancestor walk kept the last stored header, s9 was the ancestor and its invalid flag aborted the
// catchup. Narrowed to our current chain the ancestor is b5, which is valid, so catchup must still
// find s6..s9 invalid and abort before it un-stamps b6..b10 or fetches any block body.
//
// Mutation proof: removing the invalid scan over the stored headers after the ancestor in
// findCommonAncestor lets the offer through, un-stamping b6..b10 and reaching the block fetch.
func TestCatchup_InvalidStoredSideBlockAfterAncestorIsRejected(t *testing.T) {
	run := runAncestorLinkCatchup(t, offerThroughInvalidSideBranch)

	_, s6Meta, err := run.client.GetBlockHeader(context.Background(), run.offered[0].Hash())
	require.NoError(t, err)
	require.True(t, s6Meta.Invalid, "s6 must be stored invalid, else the setup is vacuous")

	require.Error(t, run.err)
	require.True(t, errors.Is(run.err, errors.ErrBlockInvalid), "an offer through a stored invalid block must be rejected as invalid: %v", run.err)
	require.Contains(t, run.err.Error(), run.offered[0].Hash().String(), "the error must name s6, the first invalid offered block")
	requireSideBranchOfferUntouched(t, run)
}

// TestCatchup_InvalidStoredBlockAboveTipIsRejected covers a stored invalid block the ancestor walk
// never records, because it sits above our tip, against a real sqlitememory blockchain store. The
// local chain is b1..b10, s6..s10 on b5 are stored valid, s11 is stored invalid, and the peer
// offers [b5, s6..s11, h12, h13]. Narrowed to our current chain the ancestor is b5, so catchup must
// find s11 invalid and abort before it un-stamps b6..b10 or fetches any block body.
//
// Mutation proof: removing the rejectInvalidStoredHeader call from findCommonAncestor lets the
// offer through, un-stamping b6..b10 and reaching the block fetch.
func TestCatchup_InvalidStoredBlockAboveTipIsRejected(t *testing.T) {
	run := runAncestorLinkCatchup(t, offerThroughInvalidAboveTip)

	s11 := run.offered[5]
	_, s11Meta, err := run.client.GetBlockHeader(context.Background(), s11.Hash())
	require.NoError(t, err)
	require.True(t, s11Meta.Invalid, "s11 must be stored invalid, else the setup is vacuous")
	require.Equal(t, uint32(11), s11Meta.Height, "s11 must sit above the tip at b10, else the setup is vacuous")

	require.Error(t, run.err)
	require.True(t, errors.Is(run.err, errors.ErrBlockInvalid), "an offer through a stored invalid block must be rejected as invalid: %v", run.err)
	require.Contains(t, run.err.Error(), s11.Hash().String(), "the error must name s11, the first invalid offered block")
	require.Zero(t, run.blocksRequests, "no block may be fetched for a rejected offer")
	require.Zero(t, run.minedClears, "a rejected offer must not un-stamp any local block (step 9.6)")
	require.Equal(t, []bool{true, true, true, true, true}, minedAboveAncestor(t, run),
		"a rejected offer must leave the local blocks above the ancestor mined")
}

// TestCatchup_ValidThenInvalidStoredBlocksAboveTipAreRejected covers an invalid block that is not
// the first stored header above our tip. The local chain is b1..b10 with b10 at a harder difficulty,
// s6..s11 on b5 are stored valid, s12 is stored invalid, and the peer offers [b5, s6..s12, h13, h14].
// The walk stops at s11, so catchup must read past that valid block to find s12.
//
// Mutation proof: returning from rejectInvalidStoredHeader after the first header it reads lets the
// offer through, un-stamping b6..b10 and reaching the block fetch.
func TestCatchup_ValidThenInvalidStoredBlocksAboveTipAreRejected(t *testing.T) {
	run := runAncestorLinkCatchup(t, offerThroughValidThenInvalidAboveTip)

	s11 := run.offered[5]
	_, s11Meta, err := run.client.GetBlockHeader(context.Background(), s11.Hash())
	require.NoError(t, err)
	require.False(t, s11Meta.Invalid, "s11 must be stored valid, else the setup is vacuous")
	require.Equal(t, uint32(11), s11Meta.Height, "s11 must sit above the tip at b10, else the setup is vacuous")

	s12 := run.offered[6]
	_, s12Meta, err := run.client.GetBlockHeader(context.Background(), s12.Hash())
	require.NoError(t, err)
	require.True(t, s12Meta.Invalid, "s12 must be stored invalid, else the setup is vacuous")

	require.Error(t, run.err)
	require.True(t, errors.Is(run.err, errors.ErrBlockInvalid), "an offer through a stored invalid block must be rejected as invalid: %v", run.err)
	require.Contains(t, run.err.Error(), s12.Hash().String(), "the error must name s12, the first invalid offered block")
	require.Zero(t, run.blocksRequests, "no block may be fetched for a rejected offer")
	require.Zero(t, run.minedClears, "a rejected offer must not un-stamp any local block (step 9.6)")
	require.Equal(t, []bool{true, true, true, true, true}, minedAboveAncestor(t, run),
		"a rejected offer must leave the local blocks above the ancestor mined")
}

// failingHeaderLookupClient fails GetBlockHeader for one hash and defers to the real client otherwise.
type failingHeaderLookupClient struct {
	blockchain.ClientI
	failOn *chainhash.Hash
}

func (c *failingHeaderLookupClient) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if blockHash.IsEqual(c.failOn) {
		return nil, nil, errors.NewStorageError("injected header lookup failure")
	}

	return c.ClientI.GetBlockHeader(ctx, blockHash)
}

// TestRejectInvalidStoredHeader covers the outcomes the end-to-end test does not reach, against a
// real sqlitememory blockchain store holding s1 valid and s2 invalid on genesis, with u1 never stored.
//
// Mutation proofs: dropping the ErrBlockNotFound return fails stops_at_first_unstored, and reporting
// a failed lookup as a peer fault fails lookup_failure_is_not_charged_to_the_peer.
func TestRejectInvalidStoredHeader(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	store, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(logger, tSettings, store, nil, nil)
	require.NoError(t, err)

	genesis, _, err := store.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	side := forkHeadersFrom(t, genesis, 3, 1, 700)
	s1, s2, unstored := side[0], side[1], side[2]
	storeSideBlock(ctx, t, store, s1, 1)
	storeSideBlock(ctx, t, store, s2, 2, options.WithInvalid(true))

	catchupCtx := &CatchupContext{blockUpTo: &model.Block{Header: s2}}
	ancestorHash := genesis.Hash()

	reject := func(c blockchain.ClientI, headers ...*model.BlockHeader) error {
		server := &Server{logger: logger, blockchainClient: c}
		return server.rejectInvalidStoredHeader(ctx, catchupCtx, headers, ancestorHash)
	}

	t.Run("names_the_first_invalid", func(t *testing.T) {
		err := reject(client, s1, s2)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "got %v", err)
		require.Contains(t, err.Error(), s2.Hash().String())
	})

	t.Run("all_stored_valid", func(t *testing.T) {
		require.NoError(t, reject(client, s1))
	})

	t.Run("stops_at_first_unstored", func(t *testing.T) {
		require.NoError(t, reject(client, s1, unstored, s2), "a header past the first unstored one is not read")
	})

	t.Run("lookup_failure_is_not_charged_to_the_peer", func(t *testing.T) {
		err := reject(&failingHeaderLookupClient{ClientI: client, failOn: s1.Hash()}, s1, s2)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrServiceError), "got %v", err)
		require.False(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "got %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "got %v", err)
	})
}

// TestCatchup_NoAncestorOnCurrentChain covers an offer whose stored headers are all on a side
// branch, against a real sqlitememory blockchain store. The peer serves s6..s9, h10, h11 without
// b5 in front, so the ancestor walk finds s6..s9 stored but none on our current chain. The locator
// was built from b10, so whose fault that is depends on whether b10 is still on our current chain.
//
// tip_unmoved: it is, so the reply cannot have come from our locator, and catchup rejects it as a
// peer fault. tip_reorged_during_fetch: our chain reorged to c8..c11 on b7 while the peer
// answered, so the reply may be honest and catchup reports a ServiceError, which is not charged
// to the peer.
//
// Mutation proofs: deleting the -1 branch after lastOnCurrentChain panics on storedMetas[-1] in
// both subtests. Returning the peer-fault error unconditionally there fails tip_reorged_during_fetch.
func TestCatchup_NoAncestorOnCurrentChain(t *testing.T) {
	t.Run("tip_unmoved", func(t *testing.T) {
		run := runAncestorLinkCatchup(t, offerSideBranchOnly)

		require.Error(t, run.err)
		require.True(t, errors.Is(run.err, errors.ErrNetworkInvalidResponse), "a reply with nothing on our current chain is a peer fault: %v", run.err)
		require.Contains(t, run.err.Error(), "no common ancestor on our current chain")
		require.Equal(t, "network_error", run.errorType, "releaseCatchupLock must classify the rejection as a peer network error")
		requireSideBranchOfferUntouched(t, run)
	})

	t.Run("tip_reorged_during_fetch", func(t *testing.T) {
		run := runAncestorLinkCatchup(t, offerSideBranchOnlyAfterReorg)

		bestHeader, _, err := run.client.GetBestBlockHeader(context.Background())
		require.NoError(t, err)
		require.NotEqual(t, run.mainChain[9].Hash().String(), bestHeader.Hash().String(), "our chain must have reorged away from b10, else the setup is vacuous")

		require.Error(t, run.err)
		require.True(t, errors.Is(run.err, errors.ErrServiceError), "our own reorg is a local fault: %v", run.err)
		require.False(t, errors.Is(run.err, errors.ErrNetworkInvalidResponse), "the peer must not be charged for our own reorg: %v", run.err)
		require.Zero(t, run.blocksRequests, "no block may be fetched for a rejected offer")
		require.Zero(t, run.minedClears, "a rejected offer must not un-stamp any local block (step 9.6)")
	})
}

// TestCatchup_HeaderBatchMustLinkToPreviousBatch covers a peer whose second header batch does not
// build on the last header of the first, against a real sqlitememory blockchain store. Every
// request names one fixed peer tip and the peer serves the path back from it, so an honest peer's
// batches always link, reorg or not. A break between batches is a protocol violation: catchup must
// reject it as NetworkInvalidResponse in the header fetch, before it un-stamps b6..b10 and before
// it fetches any block body.
//
// replayed_batches_end_at_second_request also pins the bound. Its peer answers every request after
// the first with h1..h9999 plus one fresh header, so the fetch never moves forward. Spliced back
// onto b5, that answer kept the loop below the accumulated-header cap for all 1000 iterations.
// Rejected, it costs the peer one extra request.
//
// Mutation proofs: removing the link check in catchupGetBlockHeaders fails both subtests, and so
// does restoring the splice it replaced.
func TestCatchup_HeaderBatchMustLinkToPreviousBatch(t *testing.T) {
	t.Run("second_batch_builds_on_earlier_header", func(t *testing.T) {
		run := runAncestorLinkCatchup(t, offerBreakBetweenBatches)

		require.Error(t, run.err, "catchup must abort when a header batch does not build on the previous one")
		requireChargedToPeer(t, run)
		require.Contains(t, run.err.Error(), "header batch does not link as one chain")
		require.Equal(t, 2, run.headersRequests, "the second batch is rejected as it arrives")
		requireRejectedWithoutSideEffects(t, run)
	})

	t.Run("replayed_batches_end_at_second_request", func(t *testing.T) {
		run := runAncestorLinkCatchup(t, offerReplayBetweenBatches)

		require.Equal(t, 2, run.headersRequests, "a replaying peer must not keep the header fetch looping")
		require.Error(t, run.err, "catchup must abort when a header batch does not build on the previous one")
		requireChargedToPeer(t, run)
		require.Contains(t, run.err.Error(), "header batch does not link as one chain")
		requireRejectedWithoutSideEffects(t, run)
	})
}

// TestBuildHeaderCache_RejectsFirstHeaderNotLinkedToAncestor pins buildHeaderCache's own guard.
// On the catchup path findCommonAncestor rejects a mis-linked offer first, so the end-to-end test
// above cannot tell this guard from the warning it replaced. Here the context is built directly.
func TestBuildHeaderCache_RejectsFirstHeaderNotLinkedToAncestor(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	ancestor := &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}, Bits: *nBits, Timestamp: 1}
	testhelpers.MineHeader(ancestor)

	other := &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}, Bits: *nBits, Timestamp: 2}
	testhelpers.MineHeader(other)

	newServer := func() *Server {
		return &Server{
			logger:           ulogger.TestLogger{},
			settings:         tSettings,
			headerChainCache: catchup.NewHeaderChainCache(ulogger.TestLogger{}),
		}
	}

	t.Run("linked", func(t *testing.T) {
		headers := forkHeadersFrom(t, ancestor, 3, 1, 0)
		catchupCtx := &CatchupContext{
			blockUpTo:          &model.Block{Header: headers[len(headers)-1]},
			blockHeaders:       headers,
			commonAncestorHash: ancestor.Hash(),
		}

		require.NoError(t, newServer().buildHeaderCache(catchupCtx))
	})

	t.Run("first_header_on_other_block", func(t *testing.T) {
		headers := forkHeadersFrom(t, other, 3, 1, 0)
		catchupCtx := &CatchupContext{
			blockUpTo:          &model.Block{Header: headers[len(headers)-1]},
			blockHeaders:       headers,
			commonAncestorHash: ancestor.Hash(),
		}

		err := newServer().buildHeaderCache(catchupCtx)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "a mis-linked first header is a peer fault: %v", err)
		require.Contains(t, err.Error(), "does not link to common ancestor")
	})
}

// TestCatchup_HeaderWithWrongDifficultyIsRejectedBeforeAnyBodyFetch covers the #2084 fix
// against a real sqlitememory blockchain store. The peer offers a fork of depth 5 on b5 whose first
// header declares bits other than the ones the chain requires. Its whole adjustment window is in
// the store, which the header precheck used to skip, leaving the subtree prewarm and the block
// fetch to run first. Catchup must reject it at the header precheck, charge the peer as malicious,
// and request nothing but headers.
func TestCatchup_HeaderWithWrongDifficultyIsRejectedBeforeAnyBodyFetch(t *testing.T) {
	run := runAncestorLinkCatchup(t, offerWrongBits)

	require.Error(t, run.err)
	require.True(t, errors.IsMaliciousResponseError(run.err), "wrong difficulty bits are a malicious peer: %v", run.err)
	require.Contains(t, run.err.Error(), "incorrect difficulty bits")
	require.Zero(t, run.otherRequests, "nothing but headers may be requested for a rejected offer")
	requireRejectedWithoutSideEffects(t, run)
}

// TestLastOnCurrentChain covers the search findCommonAncestor uses to narrow the last stored
// offered header to the last one on our current chain, against a real sqlitememory blockchain
// store holding b1..b10 on the current chain and s6..s9, a stored side branch on b5. Each case is a
// stored prefix as the ancestor walk leaves it, and the want is the index of the header the search
// must return.
func TestLastOnCurrentChain(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	store, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(logger, tSettings, store, nil, nil)
	require.NoError(t, err)

	genesis, _, err := store.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	mainChain := forkHeadersFrom(t, genesis, 10, 1, 0)
	for i, h := range mainChain {
		storeMinedBlock(ctx, t, store, h, uint32(i+1)) //nolint:gosec // i < 10
	}

	sideBranch := forkHeadersFrom(t, mainChain[4], 4, 6, 700)
	for i, h := range sideBranch {
		storeSideBlock(ctx, t, store, h, uint32(6+i)) //nolint:gosec // i < 4
	}

	_, bestMeta, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	server := &Server{logger: logger, settings: tSettings, blockchainClient: client}
	catchupCtx := &CatchupContext{blockUpTo: &model.Block{Header: mainChain[9]}}

	concat := func(parts ...[]*model.BlockHeader) []*model.BlockHeader {
		var out []*model.BlockHeader
		for _, p := range parts {
			out = append(out, p...)
		}

		return out
	}

	tests := []struct {
		name    string
		headers []*model.BlockHeader
		want    int
	}{
		{name: "all_on_chain_up_to_tip", headers: mainChain, want: 9},
		{name: "all_on_chain_below_tip", headers: mainChain[2:7], want: 4},
		{name: "one_on_chain_then_side_branch", headers: concat(mainChain[4:5], sideBranch), want: 0},
		{name: "five_on_chain_then_side_branch", headers: concat(mainChain[:5], sideBranch), want: 4},
		{name: "three_on_chain_then_one_side_block", headers: concat(mainChain[2:5], sideBranch[:1]), want: 2},
		{name: "side_branch_only", headers: sideBranch, want: -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			metas := make([]*model.BlockHeaderMeta, len(tc.headers))
			for i, h := range tc.headers {
				_, meta, err := client.GetBlockHeader(ctx, h.Hash())
				require.NoError(t, err)

				metas[i] = meta
			}

			got, err := server.lastOnCurrentChain(ctx, catchupCtx, tc.headers, metas, bestMeta)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// A failed chain check is our own RPC failing, so it must come back as a ServiceError, which
	// the caller retries without charging the peer. The prefix b1..b5, s6..s9 is probed in the
	// order s9 (the last), b1 (the first), then b5, s7, s6 in the search, so failing s9, b1 and
	// s7 covers all three call sites. The tip short-circuits without an RPC, so no failing block is
	// the tip. The injected error is a StorageError, not a ServiceError, so a wrapper that charged
	// the peer instead would fail here.
	prefix := concat(mainChain[:5], sideBranch)

	prefixMetas := make([]*model.BlockHeaderMeta, len(prefix))
	for i, h := range prefix {
		_, meta, err := client.GetBlockHeader(ctx, h.Hash())
		require.NoError(t, err)

		prefixMetas[i] = meta
	}

	failures := []struct {
		name   string
		failAt *model.BlockHeaderMeta
	}{
		{name: "check_fails_on_first_probe", failAt: prefixMetas[8]},    // s9
		{name: "check_fails_on_second_probe", failAt: prefixMetas[0]},   // b1
		{name: "check_fails_inside_the_search", failAt: prefixMetas[6]}, // s7
	}

	for _, tc := range failures {
		t.Run(tc.name, func(t *testing.T) {
			failing := &failingChainCheckClient{ClientI: client, failID: tc.failAt.ID}
			failingServer := &Server{logger: logger, settings: tSettings, blockchainClient: failing}

			got, err := failingServer.lastOnCurrentChain(ctx, catchupCtx, prefix, prefixMetas, bestMeta)
			require.Error(t, err)
			require.Equal(t, -1, got)
			require.Positive(t, failing.failed.Load(), "the failing block must have been probed, else the case is vacuous")
			require.True(t, errors.Is(err, errors.ErrServiceError), "a failed chain check is a local fault: %v", err)
			require.False(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "a failed chain check must not be charged to the peer: %v", err)
		})
	}

	// findCommonAncestor, driven directly: a stored prefix with nothing on our current chain is the
	// peer's fault, and a failed chain check is ours and comes back unchanged. The fetch result
	// carries no locator start here, so the no-ancestor case cannot be excused as our own reorg
	// (TestCatchup_NoAncestorOnCurrentChain covers that).
	runFind := func(client blockchain.ClientI, headers []*model.BlockHeader) error {
		s := &Server{logger: logger, settings: tSettings, blockchainClient: client}

		return s.findCommonAncestor(ctx, &CatchupContext{
			blockUpTo:          &model.Block{Header: mainChain[9]},
			headersFetchResult: &catchup.Result{Headers: headers},
		})
	}

	err = runFind(client, sideBranch)
	require.True(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "an offer with no header on our current chain is a peer fault: %v", err)

	err = runFind(&failingChainCheckClient{ClientI: client, failID: prefixMetas[8].ID}, prefix)
	require.True(t, errors.Is(err, errors.ErrServiceError), "a failed chain check is a local fault: %v", err)
	require.False(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "a failed chain check must not be charged to the peer: %v", err)
}

// failingChainCheckClient wraps the real sqlitememory-backed client and fails
// CheckBlockIsInCurrentChain for one block ID, delegating every other call.
type failingChainCheckClient struct {
	blockchain.ClientI

	failID uint32
	failed atomic.Int64
}

func (c *failingChainCheckClient) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	if len(blockIDs) == 1 && blockIDs[0] == c.failID {
		c.failed.Add(1)

		return false, errors.NewStorageError("chain check failed for block %d", c.failID)
	}

	return c.ClientI.CheckBlockIsInCurrentChain(ctx, blockIDs)
}
