package blockvalidation

import (
	"context"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// maliciousGateP2PClient reports one peer as malicious and every other peer as not.
type maliciousGateP2PClient struct {
	*subtreeAttributionP2PClient
	malicious string
}

func (c *maliciousGateP2PClient) IsPeerMalicious(_ context.Context, peerID string) (bool, string, error) {
	if peerID == c.malicious {
		return true, "marked by the test", nil
	}

	return false, "", nil
}

// TestBlockFoundChMaliciousGateAnswersStoredBlock: a peer marked malicious announces a block on a
// blockFoundCh worker. If the block is already stored, the WaitToComplete caller gets nil, as for
// any other peer. If it is not stored, the caller gets the malicious error. Either way nothing is
// fetched from the malicious peer.
func TestBlockFoundChMaliciousGateAnswersStoredBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	s.peerMaliciousCache = nil
	s.p2pClient = &maliciousGateP2PClient{subtreeAttributionP2PClient: &subtreeAttributionP2PClient{}, malicious: "peerM"}

	require.True(t, s.isPeerMalicious(ctx, "peerM"), "setup: peerM must be malicious")

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	fetchKey := "GET http://malicious-peer/block/" + block.Hash().String()
	httpmock.RegisterResponder("GET", "http://malicious-peer/block/"+block.Hash().String(), httpmock.NewStringResponder(http.StatusInternalServerError, ""))

	stored := s.blockValidation
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, &gap054ExistsChain{ClientI: s.blockchainClient}, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	require.NoError(t, gap054Deliver(ctx, t, s, block.Hash(), "http://malicious-peer", "peerM"), "a stored block must answer nil, also for a malicious peer")
	require.Equal(t, 0, httpmock.GetCallCountInfo()[fetchKey], "a stored block must not be fetched")

	s.blockValidation = stored

	err := gap054Deliver(ctx, t, s, block.Hash(), "http://malicious-peer", "peerM")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "got %v", err)
	require.Contains(t, err.Error(), "peer peerM is marked as malicious")
	require.Equal(t, 0, httpmock.GetCallCountInfo()[fetchKey], "nothing may be fetched from a malicious announcer")
}

// processBlockFoundGateRoute delivers a block from (peerID, baseURL) on one of the two routes into
// processBlockFound: the priority worker, which fetches the block, and the ProcessBlock gRPC call,
// which carries the block bytes.
type processBlockFoundGateRoute struct {
	name    string
	deliver func(ctx context.Context, t *testing.T, s *Server, block *model.Block, peerID, baseURL string) error
}

var processBlockFoundGateRoutes = []processBlockFoundGateRoute{
	{
		name: "priority worker",
		deliver: func(ctx context.Context, _ *testing.T, s *Server, block *model.Block, peerID, baseURL string) error {
			return s.processBlockWithPriorityRecovered(ctx, processBlockFound{hash: block.Hash(), baseURL: baseURL, peerID: peerID})
		},
	},
	{
		name: "ProcessBlock gRPC",
		deliver: func(ctx context.Context, t *testing.T, s *Server, block *model.Block, peerID, baseURL string) error {
			blockBytes, err := block.Bytes()
			require.NoError(t, err)

			_, err = s.ProcessBlock(ctx, &blockvalidation_api.ProcessBlockRequest{Block: blockBytes, Height: block.Height, PeerId: peerID, BaseUrl: baseURL})

			return err
		},
	},
}

// runProcessBlockFoundGateStoredBlock checks one peer gate of processBlockFound on both routes. The
// peer that stopPeer stops delivers a block that is not stored and gets wantErr. Then an honest peer
// stores the block, and the stopped peer delivers it again and gets nil, as any other peer does.
// Nothing is fetched from the stopped peer either time.
func runProcessBlockFoundGateStoredBlock(t *testing.T, stopPeer func(ctx context.Context, t *testing.T, s *Server, block *model.Block, peerID string), wantErr string) {
	t.Helper()

	for _, route := range processBlockFoundGateRoutes {
		t.Run(route.name, func(t *testing.T) {
			initPrometheusMetrics()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			s, _, block := newProcessBlockFoundHarness(ctx, t)

			const stoppedPeer = "peerStopped"

			stopPeer(ctx, t, s, block, stoppedPeer)

			httpmock.ActivateNonDefault(util.HTTPClient())
			defer httpmock.DeactivateAndReset()

			fetchKey := "GET http://stopped-peer/block/" + block.Hash().String()
			httpmock.RegisterResponder("GET", "http://stopped-peer/block/"+block.Hash().String(), httpmock.NewStringResponder(http.StatusInternalServerError, ""))

			err := route.deliver(ctx, t, s, block, stoppedPeer, "http://stopped-peer")
			require.Error(t, err, "a block that is not stored must be refused for this peer")
			require.Contains(t, err.Error(), wantErr)
			require.Equal(t, 0, httpmock.GetCallCountInfo()[fetchKey], "nothing may be fetched from this peer")

			// The block bytes travel with the call, so this stores the block without a fetch.
			require.NoError(t, s.processBlockFound(ctx, block.Hash(), "peer-honest", "http://honest-peer", false, block))

			exists, err := s.blockValidation.GetBlockExists(ctx, block.Hash())
			require.NoError(t, err)
			require.True(t, exists, "setup: the honest delivery must store the block")

			require.NoError(t, route.deliver(ctx, t, s, block, stoppedPeer, "http://stopped-peer"), "a stored block must answer nil, also for this peer")
			require.Equal(t, 0, httpmock.GetCallCountInfo()[fetchKey], "a stored block must not be fetched")
		})
	}
}

// TestProcessBlockFoundMaliciousGateAnswersStoredBlock: a peer that the P2P client reports as
// malicious delivers a block through processBlockFound, on the priority worker and on the
// ProcessBlock gRPC call. A stored block answers nil, a block that is not stored gets the malicious
// error, and nothing is fetched from the peer.
func TestProcessBlockFoundMaliciousGateAnswersStoredBlock(t *testing.T) {
	runProcessBlockFoundGateStoredBlock(t, func(ctx context.Context, t *testing.T, s *Server, _ *model.Block, peerID string) {
		s.peerMaliciousCache = nil
		s.p2pClient = &maliciousGateP2PClient{subtreeAttributionP2PClient: &subtreeAttributionP2PClient{}, malicious: peerID}

		require.True(t, s.isPeerMalicious(ctx, peerID), "setup: the peer must be malicious")
	}, "peer peerStopped is malicious")
}

// TestProcessBlockFoundPanicCapAnswersStoredBlock is the same check for a peer at its
// recovered-panic cap (BV-STARTUP-007).
func TestProcessBlockFoundPanicCapAnswersStoredBlock(t *testing.T) {
	runProcessBlockFoundGateStoredBlock(t, func(_ context.Context, t *testing.T, s *Server, block *model.Block, peerID string) {
		s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2
		s.recordPanicAttempt(block.Hash(), peerID)
		s.recordPanicAttempt(block.Hash(), peerID)

		require.True(t, s.panicAttemptsExhausted(block.Hash(), peerID), "setup: the peer must be at its cap")
	}, "recovered-panic cap reached")
}
