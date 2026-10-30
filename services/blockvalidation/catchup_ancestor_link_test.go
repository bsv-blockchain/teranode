package blockvalidation

import (
	"context"
	"net/http"
	"net/url"
	"strings"
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

// ancestorLinkRun is what one catchup() run left behind: the error, how many header and /blocks/
// requests reached the peer, how many local blocks step 9.6 un-stamped, and the real client plus
// the two header chains, so the caller can read the end state from the store.
type ancestorLinkRun struct {
	err             error
	headersRequests int
	blocksRequests  int
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
	for i, h := range mainChain {
		storeMinedBlock(ctx, t, store, h, uint32(i+1)) //nolint:gosec // i < 10
	}

	otherBlockB := forkHeadersFrom(t, genesis, 1, 1, 500)[0]
	require.NotEqual(t, mainChain[0].Hash().String(), otherBlockB.Hash().String())
	storeMinedBlock(ctx, t, store, otherBlockB, 1)

	_, bestMeta, err := store.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(10), bestMeta.Height, "b10 must be the local tip")

	ancestor := mainChain[4] // b5

	var offered, served, later []*model.BlockHeader

	var unservedTarget *model.BlockHeader

	switch shape {
	case offerLinked, offerNothingNew:
		offered = forkHeadersFrom(t, ancestor, 5, 6, 1000)
	case offerFirstOnOther:
		offered = forkHeadersFrom(t, otherBlockB, 5, 6, 1000)
	case offerBreakAtSecond:
		offered = append(forkHeadersFrom(t, ancestor, 1, 6, 1000), forkHeadersFrom(t, otherBlockB, 4, 7, 2000)...)
	case offerBreakBetweenBatches, offerReplayBetweenBatches:
		// With b5 in front this is a full batch, so the fetch asks again from h9999.
		offered = forkHeadersFrom(t, ancestor, maxBlockHeadersPerRequest-1, 6, 1000)
	}

	if shape != offerNothingNew {
		served = offered
	}

	switch shape {
	case offerBreakBetweenBatches:
		later = forkHeadersFrom(t, offered[0], 5, 7, 3000)
	case offerReplayBetweenBatches:
		fresh := forkHeadersFrom(t, offered[len(offered)-1], 2, uint32(6+len(offered)), 3000) //nolint:gosec // small test height
		later = append(append([]*model.BlockHeader{}, offered...), fresh[0])
		unservedTarget = fresh[1]
	}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

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

	responseHeaders := append([]byte{}, ancestor.Bytes()...)
	for _, h := range served {
		responseHeaders = append(responseHeaders, h.Bytes()...)
	}

	laterHeaders := make([]byte, 0, len(later)*model.BlockHeaderSize)
	for _, h := range later {
		laterHeaders = append(laterHeaders, h.Bytes()...)
	}

	var headersRequests atomic.Int64

	httpmock.RegisterResponder("GET", `=~^http://peer/headers_from_common_ancestor/.*`,
		func(*http.Request) (*http.Response, error) {
			if headersRequests.Add(1) > 1 && later != nil {
				return httpmock.NewBytesResponse(200, laterHeaders), nil
			}

			return httpmock.NewBytesResponse(200, responseHeaders), nil
		})
	httpmock.RegisterResponder("GET", `=~^http://peer/blocks/.*`, httpmock.NewBytesResponder(500, nil))

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
