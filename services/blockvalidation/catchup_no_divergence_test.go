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
	breakerFailures    int
}

// runNoDivergenceCatchup runs catchup() against a real sqlitememory blockchain store holding
// genesis..b120, every block mined. The peer serves b1..b<served> followed by fork, and the target
// is the last header of fork, or a block on b120 the peer never serves when fork is empty. With a
// coinbase maturity of 100 and our tip at 120, any ancestor at b19 or below scores as a fork deeper
// than maturity.
func runNoDivergenceCatchup(t *testing.T, peerID string, served int, maxAccumulated int, forkLen int) noDivergenceRun {
	t.Helper()

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

	if maxAccumulated > 0 {
		tSettings.BlockValidation.CatchupMaxAccumulatedHeaders = maxAccumulated
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

	servedHeaders := append([]*model.BlockHeader{}, mainChain[:served]...)

	var target *model.BlockHeader

	if forkLen > 0 {
		fork := forkHeadersFrom(t, mainChain[served-1], forkLen, uint32(served+1), 1000) //nolint:gosec // small test height
		servedHeaders = append(servedHeaders, fork...)
		target = fork[len(fork)-1]
	} else {
		target = forkHeadersFrom(t, mainChain[localTip-1], 1, localTip+1, 1000)[0]
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

	_, breakerFailures, _, _ := server.peerCircuitBreakers.GetBreaker(peerID).GetStats()

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
		breakerFailures:    breakerFailures,
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
// truncated_by_our_cap: the peer serves b1..b10 and our accumulated-header cap of 5 cut the list
// at b5. The list ended because we stopped asking, so the peer is not charged at all.
//
// deep_fork_control: the same setup with a real fork on b5. The walk stops on a header we lack,
// the depth of 115 is real, and the peer is still reported, so the zero counts above are not
// vacuous.
//
// Two changes guard this, and each is load-bearing. findCommonAncestor no longer measures a fork
// depth for a list that ran out, and catchup() skips every fork check for one. The secret-mining
// check measures from the ancestor's height, not from the depth, so the threshold here is set
// below 115 to put it in play.
//
// Mutation proofs: dropping the short-circuit in catchup() alone fails both no-fork subtests, on
// the secret-mining abort "deep fork with no offered headers". Restoring the depth measurement as
// well brings back the coinbase-maturity violation in both.
func TestCatchup_HeadersWeAllHoldAreNoFork(t *testing.T) {
	t.Run("served_in_full", func(t *testing.T) {
		run := runNoDivergenceCatchup(t, "peer-all-held", 5, 0, 0)

		require.Error(t, run.err, "the announced block is unknown and the peer served nothing past our chain")
		require.Zero(t, run.maturityViolations, "a list we hold entirely is no fork, so no coinbase-maturity violation")
		require.True(t, errors.Is(run.err, errors.ErrNetworkInvalidResponse), "the peer did not serve the block it announced: %v", run.err)
		require.Contains(t, run.err.Error(), "peer returned no new headers")
		require.Zero(t, run.minedClears, "no local block may be un-stamped")
		require.Zero(t, run.blocksRequests, "no block body may be fetched")
	})

	t.Run("truncated_by_our_cap", func(t *testing.T) {
		run := runNoDivergenceCatchup(t, "peer-all-held-capped", 10, 5, 0)

		require.Error(t, run.err)
		require.Zero(t, run.maturityViolations, "a list our own cap cut short is no fork, so no coinbase-maturity violation")
		require.True(t, isLocalCatchupFault(run.err), "our cap cut the list, so the peer must not be charged: %v", run.err)
		require.False(t, errors.Is(run.err, errors.ErrNetworkInvalidResponse), "must not carry a peer-fault code: %v", run.err)
		require.Contains(t, run.err.Error(), "stopped on our own header cap")
		require.Zero(t, run.breakerFailures, "the header fetch itself succeeded")
		require.Zero(t, run.minedClears, "no local block may be un-stamped")
		require.Zero(t, run.blocksRequests, "no block body may be fetched")
	})

	t.Run("deep_fork_control", func(t *testing.T) {
		run := runNoDivergenceCatchup(t, "peer-deep-fork", 5, 0, 3)

		require.Error(t, run.err)
		require.Equal(t, float64(1), run.maturityViolations, "a real fork 115 deep must still be reported, else the no-fork subtests are vacuous")
		require.Contains(t, run.err.Error(), "exceeds coinbase maturity")
		require.Equal(t, "coinbase_maturity_violation", run.errorType)
		require.Zero(t, run.minedClears)
	})
}
