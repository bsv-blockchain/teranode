package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// maliciousAbortP2PClient is a minimal P2PClientI implementation used to verify
// that the catchup header loop aborts when a peer is flagged malicious by the
// P2P service. IsPeerMalicious consumes isMaliciousResults in order; once the
// slice is exhausted the last value repeats.
type maliciousAbortP2PClient struct {
	isMaliciousResults []bool
	calls              int
}

func (m *maliciousAbortP2PClient) IsPeerMalicious(_ context.Context, _ string) (bool, string, error) {
	idx := m.calls
	m.calls++

	if len(m.isMaliciousResults) == 0 {
		return false, "", nil
	}

	if idx >= len(m.isMaliciousResults) {
		idx = len(m.isMaliciousResults) - 1
	}

	if m.isMaliciousResults[idx] {
		return true, "banned", nil
	}

	return false, "", nil
}

// Remaining P2PClientI methods are no-ops; they are not exercised by these tests.
func (m *maliciousAbortP2PClient) RecordCatchupAttempt(_ context.Context, _ string) error { return nil }
func (m *maliciousAbortP2PClient) RecordCatchupSuccess(_ context.Context, _ string, _ int64) error {
	return nil
}
func (m *maliciousAbortP2PClient) RecordCatchupFailure(_ context.Context, _ string) error { return nil }
func (m *maliciousAbortP2PClient) RecordCatchupFailureWithKind(_ context.Context, _, _, _ string) error {
	return nil
}
func (m *maliciousAbortP2PClient) RecordCatchupMalicious(_ context.Context, _ string) error {
	return nil
}
func (m *maliciousAbortP2PClient) UpdateCatchupError(_ context.Context, _ string, _ string) error {
	return nil
}
func (m *maliciousAbortP2PClient) UpdateCatchupReputation(_ context.Context, _ string, _ float64) error {
	return nil
}
func (m *maliciousAbortP2PClient) GetPeersForCatchup(_ context.Context) ([]*p2p.PeerInfo, error) {
	return nil, nil
}
func (m *maliciousAbortP2PClient) GetPeer(_ context.Context, _ string) (*p2p.PeerInfo, error) {
	return nil, nil
}
func (m *maliciousAbortP2PClient) ReportValidBlock(_ context.Context, _ string, _ string) error {
	return nil
}
func (m *maliciousAbortP2PClient) ReportValidSubtree(_ context.Context, _ string, _ string) error {
	return nil
}
func (m *maliciousAbortP2PClient) ReportValidatedChainProgress(_ context.Context, _ string, _ uint32, _ string, _ []byte) error {
	return nil
}
func (m *maliciousAbortP2PClient) IsPeerUnhealthy(_ context.Context, _ string) (bool, string, float32, error) {
	return false, "", 0, nil
}
func (m *maliciousAbortP2PClient) RecordBytesDownloaded(_ context.Context, _ string, _ uint64) error {
	return nil
}

// TestCatchup_MaliciousPeerAbortsCatchup verifies that a peer flagged malicious
// by the P2P service aborts the catchup header loop instead of being treated
// like any other peer (the bug: both checks only logged and continued).
func TestCatchup_MaliciousPeerAbortsCatchup(t *testing.T) {
	t.Run("AbortsBeforeLoopWhenPeerMaliciousAtStart", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		server, _, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000)).Maybe()

		// Peer is malicious from the very first check.
		server.p2pClient = &maliciousAbortP2PClient{isMaliciousResults: []bool{true}}

		mainnetHeaders := testhelpers.GetMainnetHeaders(t, 1)
		targetBlock := &model.Block{Header: mainnetHeaders[0], Height: 1001}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "peer-malicious-001", "http://malicious-peer")

		require.Error(t, err)
		require.True(t, errors.IsMaliciousResponseError(err), "expected malicious-peer error, got: %v", err)
		require.NotNil(t, result)
		require.False(t, result.Success)
		require.Equal(t, 0, httpmock.GetTotalCallCount(), "no HTTP request should be made to a peer flagged malicious before the loop")
	})

	t.Run("AbortsInsideLoopWhenPeerFlaggedMidCatchup", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000)).Maybe()

		// First check (pre-loop) passes, second check (per-iteration) trips.
		server.p2pClient = &maliciousAbortP2PClient{isMaliciousResults: []bool{false, true}}

		mainnetHeaders := testhelpers.GetMainnetHeaders(t, 1)
		bestBlockHeader := mainnetHeaders[0]
		targetBlock := &model.Block{Header: mainnetHeaders[0], Height: 1001}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)
		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "peer-malicious-002", "http://malicious-peer")

		require.Error(t, err)
		require.True(t, errors.IsMaliciousResponseError(err), "expected malicious-peer error, got: %v", err)
		require.NotNil(t, result)
		require.False(t, result.Success)
		require.Equal(t, 0, httpmock.GetTotalCallCount(), "no header fetch should occur once the peer is flagged malicious mid-loop")
	})
}
