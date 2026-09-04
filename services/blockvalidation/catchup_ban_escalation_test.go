package blockvalidation

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// banScoreRecordingP2PClient records malicious reports and ban-score escalations
// so tests can assert that a confirmed consensus violation does both.
type banScoreRecordingP2PClient struct {
	P2PClientI

	mu             sync.Mutex
	maliciousPeers []string
	banCalls       []string // "peerID:reason"
	banErr         error
}

func (b *banScoreRecordingP2PClient) RecordCatchupMalicious(_ context.Context, peerID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.maliciousPeers = append(b.maliciousPeers, peerID)

	return nil
}

func (b *banScoreRecordingP2PClient) AddBanScore(_ context.Context, peerID string, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.banCalls = append(b.banCalls, peerID+":"+reason)

	return b.banErr
}

func (b *banScoreRecordingP2PClient) snapshot() ([]string, []string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]string(nil), b.maliciousPeers...), append([]string(nil), b.banCalls...)
}

// TestReportCatchupMaliciousAndBan verifies that confirmed catchup misbehaviour is
// escalated to the ban-score system, not just penalised in reputation (issue #1161).
func TestReportCatchupMaliciousAndBan(t *testing.T) {
	t.Run("EscalatesToBanScore", func(t *testing.T) {
		client := &banScoreRecordingP2PClient{}
		server := &Server{logger: ulogger.TestLogger{}, p2pClient: client}

		server.reportCatchupMaliciousAndBan(context.Background(), "peer-1", "invalid_block_validation", p2p.ReasonInvalidBlock)

		malicious, bans := client.snapshot()
		require.Equal(t, []string{"peer-1"}, malicious)
		require.Equal(t, []string{"peer-1:" + p2p.ReasonInvalidBlock}, bans)
	})

	t.Run("RepeatedInvalidBlocksAccumulateBanScore", func(t *testing.T) {
		client := &banScoreRecordingP2PClient{}
		server := &Server{logger: ulogger.TestLogger{}, p2pClient: client}

		for i := 0; i < 5; i++ {
			server.reportCatchupMaliciousAndBan(context.Background(), "peer-2", "invalid_block", p2p.ReasonInvalidBlock)
		}

		malicious, bans := client.snapshot()
		require.Len(t, malicious, 5)
		require.Len(t, bans, 5, "every confirmed invalid block must add ban score")
	})

	t.Run("BanScoreErrorIsNonFatal", func(t *testing.T) {
		client := &banScoreRecordingP2PClient{banErr: errors.NewServiceError("p2p down")}
		server := &Server{logger: ulogger.TestLogger{}, p2pClient: client}

		require.NotPanics(t, func() {
			server.reportCatchupMaliciousAndBan(context.Background(), "peer-3", "invalid_block", p2p.ReasonInvalidBlock)
		})

		malicious, bans := client.snapshot()
		require.Equal(t, []string{"peer-3"}, malicious)
		require.Len(t, bans, 1)
	})

	t.Run("EmptyPeerIDIsIgnored", func(t *testing.T) {
		client := &banScoreRecordingP2PClient{}
		server := &Server{logger: ulogger.TestLogger{}, p2pClient: client}

		server.reportCatchupMaliciousAndBan(context.Background(), "", "invalid_block", p2p.ReasonInvalidBlock)

		malicious, bans := client.snapshot()
		require.Empty(t, malicious)
		require.Empty(t, bans)
	})

	t.Run("SoftMaliciousReportDoesNotBan", func(t *testing.T) {
		// Ambiguous / transient failures must stay soft: reputation only.
		client := &banScoreRecordingP2PClient{}
		server := &Server{logger: ulogger.TestLogger{}, p2pClient: client}

		server.reportCatchupMalicious(context.Background(), "peer-4", "validation_failure")

		malicious, bans := client.snapshot()
		require.Equal(t, []string{"peer-4"}, malicious)
		require.Empty(t, bans, "transient failures must not accrue ban score")
	})

	t.Run("NilP2PClientIsSafe", func(t *testing.T) {
		server := &Server{logger: ulogger.TestLogger{}}

		require.NotPanics(t, func() {
			server.reportCatchupMaliciousAndBan(context.Background(), "peer-5", "invalid_block", p2p.ReasonInvalidBlock)
		})
	})
}
