package blockvalidation

import (
	"net/http"
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
		require.True(t, errors.Is(err, errors.ErrProcessing), "depth %d: got %v", depth, err)
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
		err := errors.NewServiceError("[tryQuickValidation] quick validation of a block below the checkpoint on a netting UTXO store failed; it is retried, not validated another way", cause)

		s, rec := release(t, err)
		require.Equal(t, "local_no_verified_checkpoint", s.previousCatchupAttempt.ErrorType)
		require.Empty(t, rec.genericErrorReported())
	})

	t.Run("fork below the checkpoint is the peer's", func(t *testing.T) {
		err := u.refuseNettedRouteFork(&CatchupContext{blockUpTo: target, forkDepth: 1, commonAncestorMeta: &model.BlockHeaderMeta{Height: 499}})
		require.Error(t, err)

		s, rec := release(t, err)
		require.Equal(t, "checkpoint_verification_failed", s.previousCatchupAttempt.ErrorType)
		require.Equal(t, []string{"peer"}, rec.genericErrorReported(), "the peer error is reported")
	})
}

// On the netted route the header window, built by catchupGetBlockHeaders with a UTXO height that
// trails the tip, holds the next checkpoint. Checkpoints at heights 10 and 30 make the window
// floor 21 headers (the gap of 20 plus the common ancestor). The tip is at checkpoint 10 and the
// UTXO store reports height 5. A locator from the tip gives headers 10 to 30. A locator capped at
// the UTXO height gives headers 5 to 25, which stop short of checkpoint 30, and then each block
// in the window is refused.
func TestNettedRoute_HeaderWindowFromATrailingUTXOHeightHoldsTheNextCheckpoint(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_header_window", 65536)
	defer cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 41)
	for i, b := range blocks {
		b.Height = uint32(i) //nolint:gosec // test heights
	}

	s := bv.settings
	s.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{
		{Height: 10, Hash: blocks[10].Header.Hash()},
		{Height: 30, Hash: blocks[30].Header.Hash()},
	}
	s.BlockValidation.CatchupMaxAccumulatedHeaders = 1
	require.Equal(t, 21, model.NettedRouteCatchupWindow(s, bv.utxoStore))

	chainStore := blockchain_store.NewMockStore()
	for _, b := range blocks[:11] {
		_, _, err := chainStore.StoreBlock(t.Context(), b, "test")
		require.NoError(t, err)
	}

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, s, chainStore, nil, nil)
	require.NoError(t, err)

	bv.blockchainClient = client
	require.NoError(t, bv.utxoStore.SetBlockHeight(5))

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

	result, _, err := u.catchupGetBlockHeaders(t.Context(), blocks[40], "netted-peer-id", "http://netted-peer")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Headers, 21)

	require.Equal(t, blocks[10].Header.Hash(), result.Headers[0].Hash(), "the window starts at the tip, the common ancestor")
	require.Equal(t, blocks[30].Header.Hash(), result.Headers[len(result.Headers)-1].Hash(), "the window holds checkpoint 30")
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
