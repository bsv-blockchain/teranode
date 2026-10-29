package blockvalidation

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// nettedCatchupServer is a Server on a netting store with checkpoints at heights 400 and 1,000.
func nettedCatchupServer(t *testing.T, dbName string) (*Server, *BlockValidation) {
	t.Helper()

	bv, _, cleanup := newNetRangeHarness(t, dbName, 65536)
	t.Cleanup(cleanup)

	bv.settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 400}, {Height: 1000}}

	return &Server{logger: ulogger.TestLogger{}, settings: bv.settings, blockValidation: bv, utxoStore: bv.utxoStore}, bv
}

// A fork whose first block is below the checkpoint on the netted route is refused at the header
// stage as the peer's fault, at any depth. verifyCheckpointsInHeaderChain does not check a fork,
// so before this refusal such a fork reached tryQuickValidation as a local fault and charged no
// peer.
func TestNettedRoute_ForkBelowTheCheckpointIsChargedToThePeer(t *testing.T) {
	u, bv := nettedCatchupServer(t, "netted_route_fork")
	target := newReleaseCatchupBlock(t)

	for _, depth := range []uint32{1, 200} {
		c := &CatchupContext{blockUpTo: target, forkDepth: depth, commonAncestorMeta: &model.BlockHeaderMeta{Height: 499}}

		err := u.refuseNettedRouteFork(c)
		require.Error(t, err, "depth %d", depth)
		require.True(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "depth %d: got %v", depth, err)
		require.False(t, isLocalCatchupFault(err), "depth %d: the terminal handler must charge the peer", depth)
	}

	// Not a fork, a fork from above the checkpoint, or a store that does not net: no refusal.
	require.NoError(t, u.refuseNettedRouteFork(&CatchupContext{blockUpTo: target, forkDepth: 0, commonAncestorMeta: &model.BlockHeaderMeta{Height: 499}}))
	require.NoError(t, u.refuseNettedRouteFork(&CatchupContext{blockUpTo: target, forkDepth: 1, commonAncestorMeta: &model.BlockHeaderMeta{Height: 1000}}))

	bv.utxoStore = bv.utxoStore.(nettingRecorder).applyRecorder
	require.NoError(t, u.refuseNettedRouteFork(&CatchupContext{blockUpTo: target, forkDepth: 1, commonAncestorMeta: &model.BlockHeaderMeta{Height: 499}}))
}

// A window on the netted route with no verified checkpoint at or above its first block is
// refused at the header stage, before a block body is fetched, as a local fault.
func TestNettedRoute_WindowWithoutACheckpointIsRefusedBeforeTheFetch(t *testing.T) {
	u, bv := nettedCatchupServer(t, "netted_route_window")
	target := newReleaseCatchupBlock(t)
	headers := []*model.BlockHeader{target.Header}

	window := func(useQuick bool, verified uint32) *CatchupContext {
		return &CatchupContext{
			blockUpTo:               target,
			commonAncestorMeta:      &model.BlockHeaderMeta{Height: 400},
			blockHeaders:            headers,
			useQuickValidation:      useQuick,
			highestCheckpointHeight: verified,
		}
	}

	for _, c := range []*CatchupContext{window(false, 0), window(true, 400)} {
		err := u.refuseNettedRouteWindowWithoutCheckpoint(c)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrServiceError), "got %v", err)
		require.True(t, isLocalCatchupFault(err), "an honest peer that is still syncing serves such a window")
	}

	require.NoError(t, u.refuseNettedRouteWindowWithoutCheckpoint(window(true, 1000)), "checkpoint 1,000 is verified")

	// A fork is refuseNettedRouteFork's work, and an empty window has nothing to fetch.
	fork := window(false, 0)
	fork.forkDepth = 1
	require.NoError(t, u.refuseNettedRouteWindowWithoutCheckpoint(fork))

	empty := window(false, 0)
	empty.blockHeaders = nil
	require.NoError(t, u.refuseNettedRouteWindowWithoutCheckpoint(empty))

	bv.utxoStore = bv.utxoStore.(nettingRecorder).applyRecorder
	require.NoError(t, u.refuseNettedRouteWindowWithoutCheckpoint(window(false, 0)), "a store that does not net validates normally")
}

// releaseCatchupLock scores the two netted-route refusals as Server.go does: the window without a
// checkpoint is local and charges no peer, and the fork is a checkpoint failure of the peer.
func TestNettedRoute_ReleaseCatchupLockScoresTheRefusals(t *testing.T) {
	u, _ := nettedCatchupServer(t, "netted_route_release")
	target := newReleaseCatchupBlock(t)

	release := func(t *testing.T, terminal error) (*Server, *catchupReportRecorder) {
		t.Helper()

		rec := &catchupReportRecorder{}
		s := &Server{logger: ulogger.TestLogger{}, p2pClient: rec}
		c := &CatchupContext{blockUpTo: target, baseURL: "http://peer", peerID: "peer", startTime: time.Now()}
		s.releaseCatchupLock(c, &terminal)
		require.NotNil(t, s.previousCatchupAttempt)

		return s, rec
	}

	t.Run("window without a checkpoint is local", func(t *testing.T) {
		err := u.refuseNettedRouteWindowWithoutCheckpoint(&CatchupContext{
			blockUpTo: target, commonAncestorMeta: &model.BlockHeaderMeta{Height: 400}, blockHeaders: []*model.BlockHeader{target.Header},
		})
		require.Error(t, err)

		s, rec := release(t, err)
		require.Equal(t, "local_no_verified_checkpoint", s.previousCatchupAttempt.ErrorType)
		require.Empty(t, rec.genericErrorReported(), "no peer error is reported")
		require.Empty(t, rec.maliciousReported())
	})

	t.Run("a wrapped cause with network text stays local", func(t *testing.T) {
		cause := errors.NewNetworkError("unexpected EOF from http://peer connection")
		err := newNettedRouteRefusal("[tryQuickValidation] quick validation of a block below the checkpoint on a netting UTXO store failed; it is retried, not validated another way", cause)

		s, rec := release(t, err)
		require.Equal(t, "local_no_verified_checkpoint", s.previousCatchupAttempt.ErrorType)
		require.Empty(t, rec.genericErrorReported())
	})

	t.Run("a refusal wrapped by another local error is still a refusal", func(t *testing.T) {
		refusal := u.refuseNettedRouteWindowWithoutCheckpoint(&CatchupContext{
			blockUpTo: target, commonAncestorMeta: &model.BlockHeaderMeta{Height: 400}, blockHeaders: []*model.BlockHeader{target.Header},
		})
		require.Error(t, refusal)

		err := errors.NewServiceError("[test] catch-up stopped", refusal)
		require.True(t, isNettedRouteRefusal(err))

		s, rec := release(t, err)
		require.Equal(t, "local_no_verified_checkpoint", s.previousCatchupAttempt.ErrorType)
		require.Empty(t, rec.genericErrorReported())
	})

	t.Run("the marker walk is bounded", func(t *testing.T) {
		// releaseCatchupLock runs this on every local catch-up error, under activeCatchupCtxMu,
		// and a chain can be tens of thousands of links deep. A marker past the bound is not read.
		refusal := u.refuseNettedRouteWindowWithoutCheckpoint(&CatchupContext{
			blockUpTo: target, commonAncestorMeta: &model.BlockHeaderMeta{Height: 400}, blockHeaders: []*model.BlockHeader{target.Header},
		})
		require.Error(t, refusal)

		within, beyond := refusal, refusal
		for i := 0; i < markerChainDepth-1; i++ {
			within = errors.NewServiceError("[test] wrap", within)
		}

		for i := 0; i < markerChainDepth; i++ {
			beyond = errors.NewServiceError("[test] wrap", beyond)
		}

		require.True(t, isNettedRouteRefusal(within), "a marker on the last link inside the bound is read")
		require.False(t, isNettedRouteRefusal(beyond), "a marker past the bound is not read")
	})

	t.Run("a local error with the refusal's wording but no marker is not a refusal", func(t *testing.T) {
		err := errors.NewServiceError("[test] block below the checkpoint on a netting UTXO store, from some other path")
		require.False(t, isNettedRouteRefusal(err), "the marker, not the wording, makes a refusal")

		s, _ := release(t, err)
		require.NotEqual(t, "local_no_verified_checkpoint", s.previousCatchupAttempt.ErrorType)
	})

	t.Run("a corrupt body that wraps a refusal is scored corrupt", func(t *testing.T) {
		refusal := u.refuseNettedRouteWindowWithoutCheckpoint(&CatchupContext{
			blockUpTo: target, commonAncestorMeta: &model.BlockHeaderMeta{Height: 400}, blockHeaders: []*model.BlockHeader{target.Header},
		})
		require.Error(t, refusal)

		s, _ := release(t, errors.NewBlockCorruptError("[test] subtree does not hash to its key", refusal))
		require.Equal(t, "corrupt_block_body", s.previousCatchupAttempt.ErrorType)
	})

	t.Run("a local error that only names the checkpoint is not a netted refusal", func(t *testing.T) {
		err := errors.NewServiceError("[test] checkpoint list unavailable")
		require.False(t, isNettedRouteRefusal(err))

		s, _ := release(t, err)
		require.NotEqual(t, "local_no_verified_checkpoint", s.previousCatchupAttempt.ErrorType)
	})

	t.Run("fork below the checkpoint is the peer's", func(t *testing.T) {
		err := u.refuseNettedRouteFork(&CatchupContext{blockUpTo: target, forkDepth: 1, commonAncestorMeta: &model.BlockHeaderMeta{Height: 499}})
		require.Error(t, err)

		s, rec := release(t, err)
		require.Equal(t, "network_error", s.previousCatchupAttempt.ErrorType)
		require.Equal(t, []string{"peer"}, rec.genericErrorReported(), "the peer error is reported")
	})
}

// On the netted route the header window that catchupGetBlockHeaders fetches ends exactly at the
// next checkpoint above the tip, whatever blockvalidation_max_accumulated_headers says and however
// far the UTXO store trails the tip. Checkpoints are at heights 10 and 30, and the tip is at
// checkpoint 10. With the UTXO store at 10 the window is headers 10 to 30. With it at 5 the locator
// starts at 5, and the window is headers 5 to 30. A setting of 1 used to stop the window short of
// checkpoint 30, and a setting of 1,000 used to run it past checkpoint 30 to the peer's tip.
func TestNettedRoute_HeaderWindowEndsAtTheNextCheckpoint(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_header_window", 65536)
	defer cleanup()

	s := bv.settings

	// The real sqlite chain store, not a mock: the locator and ancestor queries this test guards
	// are the ones production runs. It starts with the network's genesis, so the test chain
	// builds on it, and genesis is block 0.
	chainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, s)
	require.NoError(t, err)

	genesis, err := chainStore.GetBlockByHeight(t.Context(), 0)
	require.NoError(t, err)

	blocks := append([]*model.Block{genesis}, testhelpers.CreateTestBlocksWithPrev(t, 40, genesis.Header.Hash())...)
	for i, b := range blocks {
		b.Height = uint32(i) //nolint:gosec // test heights
	}

	s.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{
		{Height: 10, Hash: blocks[10].Header.Hash()},
		{Height: 30, Hash: blocks[30].Header.Hash()},
	}
	for _, b := range blocks[1:11] {
		_, _, err := chainStore.StoreBlock(t.Context(), b, "test")
		require.NoError(t, err)
	}

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, s, chainStore, nil, nil)
	require.NoError(t, err)

	bv.blockchainClient = client

	u := &Server{logger: ulogger.TestLogger{}, settings: s, blockValidation: bv, utxoStore: bv.utxoStore, blockchainClient: client}

	// The peer serves its chain from the first locator hash it holds, that header included.
	byHash := make(map[string]int, len(blocks))
	for i, b := range blocks {
		byHash[b.Header.Hash().String()] = i
	}

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `=~^http://netted-peer/headers_from_common_ancestor/.*`,
		func(req *http.Request) (*http.Response, error) {
			locator := req.URL.Query().Get("block_locator_hashes")
			start := 0

			for i := 0; i+64 <= len(locator); i += 64 {
				if idx, ok := byHash[strings.ToLower(locator[i:i+64])]; ok {
					start = idx
					break
				}
			}

			var body []byte
			for _, b := range blocks[start:] {
				body = append(body, b.Header.Bytes()...)
			}

			return httpmock.NewBytesResponse(http.StatusOK, body), nil
		})

	for _, tc := range []struct {
		setting    int
		utxoHeight uint32
	}{{1, 10}, {1000, 10}, {1, 5}, {1000, 5}} {
		s.BlockValidation.CatchupMaxAccumulatedHeaders = tc.setting
		require.NoError(t, bv.utxoStore.SetBlockHeight(tc.utxoHeight))

		result, _, err := u.catchupGetBlockHeaders(t.Context(), blocks[40], "netted-peer-id", "http://netted-peer")
		require.NoError(t, err)
		require.NotNil(t, result)

		require.Equal(t, blocks[tc.utxoHeight].Header.Hash(), result.Headers[0].Hash(), "setting %d, UTXO height %d: the window starts where the locator starts", tc.setting, tc.utxoHeight)
		require.Equal(t, blocks[30].Header.Hash(), result.Headers[len(result.Headers)-1].Hash(), "setting %d, UTXO height %d: the window ends at checkpoint 30", tc.setting, tc.utxoHeight)
		require.Len(t, result.Headers, int(30-tc.utxoHeight)+1)
	}
}

// On the netted route the DAA check runs before the refusal of a window without a verified
// checkpoint, so a peer that serves a self-consistent low-difficulty chain in such a window is
// still reported as malicious, and an honest window without a checkpoint stays a local fault.
func TestNettedRoute_DifficultyIsCheckedBeforeTheWindowRefusal(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_daa_first", 65536)
	defer cleanup()

	params := chaincfg.MainNetParams
	params.DaaForkHeight = 0
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 300_000}}
	bv.settings.ChainCfgParams = &params

	startBits, err := model.NewNBitFromString("180a097a")
	require.NoError(t, err)

	easyBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	run := func(t *testing.T, tamper bool) (*catchupReportRecorder, error) {
		t.Helper()

		anchor, headers := buildConstantChain(300, 600, startBits)
		if tamper {
			headers[200].Bits = *easyBits
		}

		rec := &catchupReportRecorder{}
		u := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, blockValidation: bv, utxoStore: bv.utxoStore, p2pClient: rec}
		c := &CatchupContext{blockUpTo: newReleaseCatchupBlock(t), peerID: "peer", commonAncestorMeta: anchor, blockHeaders: headers}

		_, netted := u.nettedRouteFirstBlock(c)
		require.True(t, netted, "the window starts below checkpoint 300,000 on a netting store")

		return rec, u.validateHeaderWindow(t.Context(), c)
	}

	rec, err := run(t, true)
	require.Error(t, err)
	require.True(t, errors.IsMaliciousResponseError(err), "got %v", err)
	require.Equal(t, []string{"peer"}, rec.maliciousReported(), "the low-difficulty chain is reported")

	rec, err = run(t, false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceError), "got %v", err)
	require.True(t, isLocalCatchupFault(err))
	require.Empty(t, rec.maliciousReported())
}

// nettedForkCatchup is a full catchup() run on the sqlitememory chain store: a local chain of
// tip blocks on genesis, every block above the fork point marked mined, and a peer that serves
// forkLen blocks off block ancestor. The store nets below a checkpoint at height 1,000, so every
// block in these tests can only be quick validated.
type nettedForkCatchup struct {
	server *Server
	client blockchain.ClientI
	local  []*model.Block
	target *model.Block
}

func newNettedForkCatchup(t *testing.T, ancestor, tip uint32, forkLen int) *nettedForkCatchup {
	t.Helper()

	server, _, mockUTXOStore, cleanup := setupTestCatchupServer(t)
	t.Cleanup(cleanup)

	params := chaincfg.RegressionNetParams
	params.CoinbaseMaturity = 100
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 1000}}
	server.settings.ChainCfgParams = &params
	server.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	server.settings.BlockValidation.SecretMiningThreshold = 1000

	chainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, server.settings)
	require.NoError(t, err)

	genesis, err := chainStore.GetBlockByHeight(t.Context(), 0)
	require.NoError(t, err)

	local := append([]*model.Block{genesis}, testhelpers.CreateTestBlocksWithPrev(t, int(tip), genesis.Header.Hash())...)
	for i, b := range local {
		b.Height = uint32(i) //nolint:gosec // test heights
	}

	for _, b := range local[1:] {
		_, _, err := chainStore.StoreBlock(t.Context(), b, "test")
		require.NoError(t, err)
	}

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, server.settings, chainStore, nil, nil)
	require.NoError(t, err)

	// Every block above the fork point is marked mined, so a mined_set clear shows in the store.
	for _, b := range local[ancestor+1:] {
		require.NoError(t, client.SetBlockMinedSet(t.Context(), b.Header.Hash()))
	}

	server.blockchainClient = client
	server.blockValidation.blockchainClient = client

	mockUTXOStore.On("GetBlockHeight").Return(tip)

	store := nettingMockStore{mockUTXOStore}
	server.blockValidation.utxoStore = store
	server.utxoStore = store

	fork := testhelpers.CreateSyntheticChainFromAtHeight(local[ancestor].Header, forkLen, ancestor+1)

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)

	body := local[ancestor].Header.Bytes()
	for _, h := range fork {
		body = append(body, h.Bytes()...)
	}

	httpmock.RegisterResponder("GET", `=~^http://peer/headers_from_common_ancestor/.*`, httpmock.NewBytesResponder(http.StatusOK, body))
	// Every other request the peer gets is counted, so assertNoBodyFetch sees a body fetch.
	httpmock.RegisterResponder("GET", `=~.*`, httpmock.NewStringResponder(http.StatusInternalServerError, ""))

	return &nettedForkCatchup{
		server: server,
		client: client,
		local:  local,
		target: &model.Block{Header: fork[len(fork)-1], Height: ancestor + uint32(forkLen)}, //nolint:gosec // test heights
	}
}

// assertMinedSetKept requires every local block above ancestor to be still marked mined: the
// refusal came before Step 5.5's mined_set clear.
func (n *nettedForkCatchup) assertMinedSetKept(t *testing.T, ancestor uint32) {
	t.Helper()

	for _, b := range n.local[ancestor+1:] {
		mined, err := n.client.GetBlockIsMined(t.Context(), b.Header.Hash())
		require.NoError(t, err)
		require.True(t, mined, "block %d lost its mined_set for a fork that was refused", b.Height)
	}
}

// assertNoBodyFetch requires that only the header request reached the peer: the refusal comes
// before any block body.
func assertNoBodyFetch(t *testing.T) {
	t.Helper()

	for call, n := range httpmock.GetCallCountInfo() {
		if n > 0 && !strings.Contains(call, "headers_from_common_ancestor") {
			t.Fatalf("the peer was asked for more than headers: %s (%d)", call, n)
		}
	}
}

// The netted-route refusals, through catchup() on a real chain store: each fork is refused before
// the mined_set clear and before a block body is fetched, a deep fork is still reported malicious,
// and a fork is charged to the peer at any depth.
func TestNettedRoute_CatchupRefusesForksOnTheChainStore(t *testing.T) {
	initPrometheusMetrics()

	t.Run("a fork is refused before the mined_set clear", func(t *testing.T) {
		// Depth 5 passes the fork-depth and secret-mining checks, so without the refusal catchup
		// reaches Step 5.5 and un-stamps this node's own blocks for a fork it cannot take.
		n := newNettedForkCatchup(t, 100, 105, 10)

		err := n.server.catchup(t.Context(), n.target, "peer-netted", "http://peer")

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "got %v", err)
		require.False(t, isLocalCatchupFault(err), "processCatchupChItem must charge and rotate the peer")
		n.assertMinedSetKept(t, 100)
		assertNoBodyFetch(t)
	})

	t.Run("a window without a checkpoint is refused before the fetch", func(t *testing.T) {
		// No fork: the peer extends our tip at 100 with blocks 101-105, and the window holds no
		// verified checkpoint (the next one is 1,000), so no block in it can be quick validated.
		n := newNettedForkCatchup(t, 100, 100, 5)

		err := n.server.catchup(t.Context(), n.target, "peer-netted", "http://peer")

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrServiceError), "got %v", err)
		require.True(t, isNettedRouteRefusal(err), "got %v", err)
		assertNoBodyFetch(t)
	})

	t.Run("a secretly mined netted fork keeps the secret-mining error", func(t *testing.T) {
		// Depth 80 passes the fork-depth check but exceeds a secret-mining threshold of 50, and the
		// offered fork is lighter. checkSecretMining's other returns are local faults that must not
		// charge the peer, so its error is never swapped for the netted refusal.
		n := newNettedForkCatchup(t, 100, 180, 10)
		n.server.settings.BlockValidation.SecretMiningThreshold = 50

		err := n.server.catchup(t.Context(), n.target, "peer-netted-secret", "http://peer")

		require.Error(t, err)
		require.Contains(t, err.Error(), "secretly mined")
		require.False(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "got %v", err)
		n.assertMinedSetKept(t, 100)
		assertNoBodyFetch(t)
	})

	t.Run("a deep fork is reported malicious and charged to the peer", func(t *testing.T) {
		// Depth 150 exceeds coinbase maturity. The fork-depth check must see it first, because it
		// is what reports the peer malicious (recordMaliciousAttempt, ban score 50). Its own
		// ServiceError is a local fault to processCatchupChItem, which would keep the same sync
		// peer and retry the same hash, so on the netted route the refusal is returned in its
		// place, as for a shallower netted fork.
		n := newNettedForkCatchup(t, 100, 250, 10)

		violations := prometheusCatchupErrors.WithLabelValues("peer-netted-deep", "coinbase_maturity_violation")
		before := testutil.ToFloat64(violations)

		err := n.server.catchup(t.Context(), n.target, "peer-netted-deep", "http://peer")

		require.Error(t, err)
		require.Equal(t, before+1, testutil.ToFloat64(violations),
			"validateForkDepth records the malicious attempt and its metric together")
		require.True(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "got %v", err)
		require.False(t, isLocalCatchupFault(err), "processCatchupChItem must charge and rotate the peer")
		require.NotNil(t, n.server.previousCatchupAttempt)
		require.Equal(t, "coinbase_maturity_violation", n.server.previousCatchupAttempt.ErrorType,
			"the dashboard names the real cause, not network_error")
		n.assertMinedSetKept(t, 100)
		assertNoBodyFetch(t)
	})
}
