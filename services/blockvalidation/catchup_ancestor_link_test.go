package blockvalidation

import (
	"context"
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
// calls, delegating each one. Reading mined_set back after catchup() is racy: step 5.5 sends a
// BlockMinedUnset notification per block, and block validation's background job re-marks those
// blocks mined on its own schedule. The call count is what step 5.5 actually did.
type minedSetClearRecorder struct {
	blockchain.ClientI

	clears atomic.Int64
}

func (r *minedSetClearRecorder) ClearBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	r.clears.Add(1)

	return r.ClientI.ClearBlockMinedSet(ctx, blockHash)
}

// ancestorLinkRun is what one catchup() run left behind: the error, how many /blocks/ requests
// reached the peer, how many local blocks step 5.5 un-stamped, and the real client plus the two
// header chains, so the caller can read the end state from the store.
type ancestorLinkRun struct {
	err            error
	blocksRequests int
	minedClears    int64
	client         blockchain.ClientI
	mainChain      []*model.BlockHeader // b1..b10, the local chain; b5 is the common ancestor
	offered        []*model.BlockHeader // h1..h5, the headers the peer offers after the ancestor
}

// runAncestorLinkCatchup runs catchup() against a real sqlitememory blockchain store holding the
// local chain genesis..b10, every block mined, plus B, a second stored block (a side block on
// genesis at height 1). The peer offers [b5, h1..h5]. When firstParentIsAncestor is true, h1 is
// built on b5, an honest fork of depth 5. When false, h1 is built on B, so it names a stored block
// other than the common ancestor as its parent.
func runAncestorLinkCatchup(t *testing.T, firstParentIsAncestor bool) ancestorLinkRun {
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

	firstParent := otherBlockB
	if firstParentIsAncestor {
		firstParent = ancestor
	}

	offered := forkHeadersFrom(t, firstParent, 5, 6, 1000)

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
	for _, h := range offered {
		responseHeaders = append(responseHeaders, h.Bytes()...)
	}

	httpmock.RegisterResponder("GET", `=~^http://peer/headers_from_common_ancestor/.*`,
		httpmock.NewBytesResponder(200, responseHeaders))
	httpmock.RegisterResponder("GET", `=~^http://peer/blocks/.*`, httpmock.NewBytesResponder(500, nil))

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

	return ancestorLinkRun{
		err:            err,
		blocksRequests: blocksRequests,
		minedClears:    recorder.clears.Load(),
		client:         client,
		mainChain:      mainChain,
		offered:        offered,
	}
}

// minedAboveAncestor reads mined_set from the real store for b6..b10, the local blocks above the
// ancestor that step 5.5 un-stamps ahead of a reorg. Only meaningful when nothing was cleared: once
// step 5.5 runs, background re-marking makes the flags race (see minedSetClearRecorder).
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

// TestCatchup_FirstHeaderMustLinkToCommonAncestor covers bitcoin-sv/teranode#4975 against a real
// sqlitememory blockchain store. The peer offers [b5, h1..h5]: b5 is the stored common ancestor,
// but h1's previous hash is B, a different stored block. Catchup must abort before step 5.5
// un-stamps the local blocks above the ancestor, and before it fetches any block body, because
// every height it would assign (ancestor + 1 + index) is wrong.
//
// The control subtest runs the same setup with h1 built on b5. It does un-stamp b6..b10 and does
// reach the block fetch, so the zero clear and fetch counts in the second subtest are not vacuous.
//
// Mutation proof: delete the link check in findCommonAncestor and the second subtest fails on the
// clear count, because step 5.5 un-stamps b6..b10 before buildHeaderCache rejects the offer.
func TestCatchup_FirstHeaderMustLinkToCommonAncestor(t *testing.T) {
	t.Run("control_linked_chain_unstamps_and_reaches_block_fetch", func(t *testing.T) {
		run := runAncestorLinkCatchup(t, true)

		require.Positive(t, run.blocksRequests, "a correctly linked chain must reach the block fetch, else the setup is vacuous")
		require.Equal(t, int64(5), run.minedClears, "a correctly linked fork of depth 5 must un-stamp b6..b10, else the setup is vacuous")
	})

	t.Run("first_header_parent_is_other_stored_block", func(t *testing.T) {
		run := runAncestorLinkCatchup(t, false)

		require.Error(t, run.err, "catchup must abort when the first header does not link to the common ancestor")
		require.True(t, errors.Is(run.err, errors.ErrProcessing), "a mis-linked offer is a generic peer fault: %v", run.err)
		require.Contains(t, run.err.Error(), "does not link to common ancestor")
		require.Zero(t, run.blocksRequests, "no block may be fetched once the link to the ancestor is broken")
		require.Zero(t, run.minedClears, "a rejected offer must not un-stamp any local block (step 5.5)")
		require.Equal(t, []bool{true, true, true, true, true}, minedAboveAncestor(t, run),
			"a rejected offer must leave the local blocks above the ancestor mined")

		for _, h := range run.offered {
			exists, err := run.client.GetBlockExists(context.Background(), h.Hash())
			require.NoError(t, err)
			require.False(t, exists, "no offered block may be stored")
		}
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
		require.True(t, errors.Is(err, errors.ErrProcessing), "a mis-linked first header is a generic peer fault: %v", err)
		require.Contains(t, err.Error(), "does not link to common ancestor")
	})
}
