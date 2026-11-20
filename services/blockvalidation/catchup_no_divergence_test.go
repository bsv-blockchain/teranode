package blockvalidation

import (
	"context"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// noDivergenceRun is what one catchup() run against a peer serving only headers we hold left
// behind.
type noDivergenceRun struct {
	err                error
	maturityViolations float64 // coinbase_maturity_violation metric increments for the peer
	minedClears        int64
	blocksRequests     int
	errorType          string
}

// noDivergenceCase sets up one runNoDivergenceCatchup run.
type noDivergenceCase struct {
	peerID         string
	servedFrom     int    // first main-chain height the peer serves
	servedTo       int    // last main-chain height the peer serves
	maxAccumulated int    // CatchupMaxAccumulatedHeaders; 0 keeps the default
	forkLen        int    // headers of a fork on b<servedTo> served after the main-chain range
	utxoHeight     uint32 // UTXO store height, where our block locator starts; 0 means our tip
}

// runNoDivergenceCatchup runs catchup() against a real sqlitememory blockchain store holding
// genesis..b120, every block mined. The peer serves b<servedFrom>..b<servedTo> followed by fork,
// and the target is the last header of fork, or a block on b120 the peer never serves when fork is
// empty. With a coinbase maturity of 100 and our tip at 120, any ancestor at b19 or below scores as
// a fork deeper than maturity. A UTXO height below 120 starts our block locator there, as it does
// while the UTXO store lags the chain.
func runNoDivergenceCatchup(t *testing.T, c noDivergenceCase) noDivergenceRun {
	t.Helper()

	peerID := c.peerID

	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	params := *tSettings.ChainCfgParams
	params.CoinbaseMaturity = 100
	params.BIP0034Height = 100000000
	params.BIP0065Height = 100000000
	params.BIP0066Height = 100000000
	params.Checkpoints = []chaincfg.Checkpoint{}
	tSettings.ChainCfgParams = &params
	// Below the 115 blocks an ancestor at b5 sits under our tip, so the secret-mining check, which
	// measures from the ancestor's height rather than from the fork depth, would act on that list.
	tSettings.BlockValidation.SecretMiningThreshold = 50
	tSettings.BlockValidation.GRPCListenAddress = ""

	if c.maxAccumulated > 0 {
		tSettings.BlockValidation.CatchupMaxAccumulatedHeaders = c.maxAccumulated
	}

	store, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(logger, tSettings, store, nil, nil)
	require.NoError(t, err)

	recorder := &minedSetClearRecorder{ClientI: client}

	genesis, _, err := store.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	const localTip = 120

	mainChain := forkHeadersFrom(t, genesis, localTip, 1, 0)
	for i, h := range mainChain {
		storeMinedBlock(ctx, t, store, h, uint32(i+1)) //nolint:gosec // i < 120
	}

	_, bestMeta, err := store.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(localTip), bestMeta.Height)

	servedHeaders := append([]*model.BlockHeader{}, mainChain[c.servedFrom-1:c.servedTo]...)

	var target *model.BlockHeader

	if c.forkLen > 0 {
		fork := forkHeadersFrom(t, mainChain[c.servedTo-1], c.forkLen, uint32(c.servedTo+1), 1000) //nolint:gosec // small test height
		servedHeaders = append(servedHeaders, fork...)
		target = fork[len(fork)-1]
	} else {
		target = forkHeadersFrom(t, mainChain[localTip-1], 1, localTip+1, 1000)[0]
	}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	utxoHeight := c.utxoHeight
	if utxoHeight == 0 {
		utxoHeight = localTip
	}

	require.NoError(t, utxoStore.SetBlockHeight(utxoHeight))

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

	responseHeaders := make([]byte, 0, len(servedHeaders)*model.BlockHeaderSize)
	for _, h := range servedHeaders {
		responseHeaders = append(responseHeaders, h.Bytes()...)
	}

	var blocksRequests atomic.Int64

	httpmock.RegisterResponder("GET", `=~^http://peer/headers_from_common_ancestor/.*`,
		httpmock.NewBytesResponder(200, responseHeaders))
	httpmock.RegisterResponder("GET", `=~^http://peer/blocks/.*`,
		func(req *http.Request) (*http.Response, error) {
			blocksRequests.Add(1)
			return httpmock.NewBytesResponse(500, nil), nil
		})
	httpmock.RegisterNoResponder(httpmock.ConnectionFailure)

	violations := prometheusCatchupErrors.WithLabelValues(peerID, "coinbase_maturity_violation")
	before := testutil.ToFloat64(violations)

	err = server.catchup(ctx, &model.Block{Header: target, Height: localTip + 1}, peerID, "http://peer")

	errorType := ""
	if server.previousCatchupAttempt != nil {
		errorType = server.previousCatchupAttempt.ErrorType
	}

	return noDivergenceRun{
		err:                err,
		maturityViolations: testutil.ToFloat64(violations) - before,
		minedClears:        recorder.clears.Load(),
		blocksRequests:     int(blocksRequests.Load()),
		errorType:          errorType,
	}
}

// TestCatchup_HeadersWeAllHoldAreNoFork covers a peer whose served header list ends below our tip
// with every header in it one we already hold. The common-ancestor walk then runs off the end of
// the list without finding a header we lack, which is no evidence of a fork. It used to take the
// last header as the divergence point anyway and score the distance to our tip as a fork depth,
// so a list ending more than coinbase maturity below our tip reported an honest peer malicious
// for a coinbase-maturity violation.
//
// served_in_full: the peer's list simply ends at b5, short of the block it announced. That is
// the existing "no new headers" outcome: the peer is charged for not serving the block it
// announced, as on a dead fork, but not reported malicious.
//
// truncated_by_our_cap: the UTXO store lags at height 2, so our locator starts at b2. The peer
// honours it and serves b2..b11, and our accumulated-header cap of 5 cuts the list at b6. The list
// ended because we stopped asking, so the peer is not charged at all.
//
// capped_list_ignoring_our_locator: the same locator, but the peer serves b1..b10, a list that does
// not start where our locator does. Our cap still cuts it at b5, but a peer that ignores the
// locator can trip the cap at will, so the list is charged to the peer as an invalid response.
//
// deep_fork_control: a real fork on b5. The walk stops on a header we lack, the depth of 115 is
// real, and the peer is still reported, so the zero counts above are not vacuous.
//
// The short-circuit in catchup() is what this test guards: without it the no-fork subtests fail on
// the secret-mining abort "deep fork with no offered headers", and without the zero depth as well
// they fail on the coinbase-maturity violation. The zero depth on its own is pinned by
// TestFindCommonAncestor_ListWeAllHoldMeasuresNoForkDepth. The secret-mining check measures from
// the ancestor's height, not from the depth, so the threshold here is set below 115 to put it in
// play. Dropping the locator-start check in handleExhaustedAncestorWalk fails
// capped_list_ignoring_our_locator.
func TestCatchup_HeadersWeAllHoldAreNoFork(t *testing.T) {
	t.Run("served_in_full", func(t *testing.T) {
		run := runNoDivergenceCatchup(t, noDivergenceCase{peerID: "peer-all-held", servedFrom: 1, servedTo: 5})

		require.Error(t, run.err, "the announced block is unknown and the peer served nothing past our chain")
		require.Zero(t, run.maturityViolations, "a list we hold entirely is no fork, so no coinbase-maturity violation")
		require.True(t, errors.Is(run.err, errors.ErrNetworkInvalidResponse), "the peer did not serve the block it announced: %v", run.err)
		require.Contains(t, run.err.Error(), "peer returned no new headers")
		require.Zero(t, run.minedClears, "no local block may be un-stamped")
		require.Zero(t, run.blocksRequests, "no block body may be fetched")
	})

	t.Run("truncated_by_our_cap", func(t *testing.T) {
		run := runNoDivergenceCatchup(t, noDivergenceCase{peerID: "peer-all-held-capped", servedFrom: 2, servedTo: 11, maxAccumulated: 5, utxoHeight: 2})

		require.Error(t, run.err)
		require.Zero(t, run.maturityViolations, "a list our own cap cut short is no fork, so no coinbase-maturity violation")
		require.True(t, isLocalCatchupFault(run.err), "our cap cut the list, so the peer must not be charged: %v", run.err)
		require.False(t, errors.Is(run.err, errors.ErrNetworkInvalidResponse), "must not carry a peer-fault code: %v", run.err)
		require.Contains(t, run.err.Error(), "stopped on our own header cap")
		require.Equal(t, "local_header_cap", run.errorType, "the catchup status must not file our own cap against the peer")
		require.Zero(t, run.minedClears, "no local block may be un-stamped")
		require.Zero(t, run.blocksRequests, "no block body may be fetched")
	})

	t.Run("capped_list_ignoring_our_locator", func(t *testing.T) {
		run := runNoDivergenceCatchup(t, noDivergenceCase{peerID: "peer-ignores-locator", servedFrom: 1, servedTo: 10, maxAccumulated: 5, utxoHeight: 2})

		require.Error(t, run.err)
		require.Zero(t, run.maturityViolations, "still no fork, so no coinbase-maturity violation")
		require.False(t, isLocalCatchupFault(run.err), "a list that does not start at our locator must not pass as our own cap: %v", run.err)
		require.True(t, errors.Is(run.err, errors.ErrNetworkInvalidResponse), "the peer is charged for the list it served: %v", run.err)
		require.Contains(t, run.err.Error(), "not at our block locator's first entry")
		require.Equal(t, "network_error", run.errorType, "filed against the peer")
		require.Zero(t, run.minedClears, "no local block may be un-stamped")
		require.Zero(t, run.blocksRequests, "no block body may be fetched")
	})

	t.Run("deep_fork_control", func(t *testing.T) {
		run := runNoDivergenceCatchup(t, noDivergenceCase{peerID: "peer-deep-fork", servedFrom: 1, servedTo: 5, forkLen: 3})

		require.Error(t, run.err)
		require.Equal(t, float64(1), run.maturityViolations, "a real fork 115 deep must still be reported, else the no-fork subtests are vacuous")
		require.Contains(t, run.err.Error(), "exceeds coinbase maturity")
		require.Equal(t, "coinbase_maturity_violation", run.errorType)
		require.Zero(t, run.minedClears)
	})
}
