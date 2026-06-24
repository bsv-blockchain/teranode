package p2p

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// freshTestServer wires a P2P Server backed by a real in-memory
// CentralizedPeerRegistry via NewLocalPeerRegistryClient — same code path the
// gRPC-mode daemon uses, just bypassing the wire round-trip.
func freshTestServer(t *testing.T) (*Server, *blockchain.CentralizedPeerRegistry, peer.ID) {
	t.Helper()

	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	client := blockchain.NewLocalPeerRegistryClient(reg)

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
	require.NoError(t, err)
	pid, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	s := &Server{
		peerRegistry: client,
		logger:       ulogger.TestLogger{},
	}
	return s, reg, pid
}

func TestRecordCatchupAttempt_RegistersSyncAttempt(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.RecordCatchupAttempt(context.Background(), &p2p_api.RecordCatchupAttemptRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int32(1), got.SyncAttemptCount)
	require.False(t, got.LastSyncAttempt.IsZero())
}

func TestRecordCatchupAttempt_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)

	_, err := s.RecordCatchupAttempt(context.Background(), &p2p_api.RecordCatchupAttemptRequest{PeerId: "not-a-peer-id"})
	require.Error(t, err)
}

func TestRecordCatchupSuccess_UpdatesInteractionMetrics(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.RecordCatchupSuccess(context.Background(), &p2p_api.RecordCatchupSuccessRequest{
		PeerId:     pid.String(),
		DurationMs: 250,
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int64(1), got.InteractionSuccesses)
	require.Equal(t, int64(250), got.AvgResponseTimeMs)
}

func TestRecordCatchupFailure_UpdatesInteractionMetrics(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.RecordCatchupFailure(context.Background(), &p2p_api.RecordCatchupFailureRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int64(1), got.InteractionFailures)
}

func TestRecordCatchupMalicious_PinsReputationLow(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	_, err := s.RecordCatchupMalicious(context.Background(), &p2p_api.RecordCatchupMaliciousRequest{PeerId: pid.String()})
	require.NoError(t, err)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int64(1), got.MaliciousCount)
	require.Equal(t, 5.0, got.ReputationScore)
}

func TestUpdateCatchupReputation_NoOp(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	r1, _ := reg.Get(pid.String())

	resp, err := s.UpdateCatchupReputation(context.Background(), &p2p_api.UpdateCatchupReputationRequest{
		PeerId: pid.String(),
		Score:  99.9,
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	r2, _ := reg.Get(pid.String())
	require.Equal(t, r1.ReputationScore, r2.ReputationScore, "manual reputation override is a no-op now")
}

func TestUpdateCatchupError_StoresMessageAndTime(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	_, err := s.UpdateCatchupError(context.Background(), &p2p_api.UpdateCatchupErrorRequest{
		PeerId:   pid.String(),
		ErrorMsg: "block 0xdead missing",
	})
	require.NoError(t, err)

	got, _ := reg.Get(pid.String())
	require.Equal(t, "block 0xdead missing", got.LastCatchupError)
	require.False(t, got.LastCatchupErrorTime.IsZero())
}

func TestGetPeersForCatchup_FiltersAndSorts(t *testing.T) {
	s, reg, _ := freshTestServer(t)

	// Three peers: full + http url, pruned + http url, banned + http url.
	reg.Register(&blockchain.PeerInfo{ID: "full", DataHubURL: "http://full", Storage: "full", Height: 100})
	reg.Register(&blockchain.PeerInfo{ID: "pruned", DataHubURL: "http://pruned", Storage: "pruned", Height: 100})
	reg.Register(&blockchain.PeerInfo{ID: "no-url", Storage: "full", Height: 100})
	reg.Register(&blockchain.PeerInfo{ID: "banned", DataHubURL: "http://banned", Storage: "full", Height: 100})
	reg.AddBanScore("banned", "spam", 0)
	reg.AddBanScore("banned", "spam", 0)

	resp, err := s.GetPeersForCatchup(context.Background(), &p2p_api.GetPeersForCatchupRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 2, "no-url and banned must be excluded")

	ids := []string{resp.Peers[0].Id, resp.Peers[1].Id}
	require.ElementsMatch(t, []string{"full", "pruned"}, ids)
	require.Equal(t, "full", resp.Peers[0].Id, "full must sort ahead of pruned")
}

func TestReportValidSubtree_HappyPath(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.ReportValidSubtree(context.Background(), &p2p_api.ReportValidSubtreeRequest{
		PeerId:      pid.String(),
		SubtreeHash: "abc",
	})
	require.NoError(t, err)
	require.True(t, resp.Success)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int64(1), got.SubtreesReceived)
}

func TestReportValidSubtree_RejectsEmpty(t *testing.T) {
	s, _, _ := freshTestServer(t)

	_, err := s.ReportValidSubtree(context.Background(), &p2p_api.ReportValidSubtreeRequest{})
	require.Error(t, err)

	_, err = s.ReportValidSubtree(context.Background(), &p2p_api.ReportValidSubtreeRequest{
		PeerId: "x",
	})
	require.Error(t, err)
}

func TestReportValidBlock_HappyPath(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.ReportValidBlock(context.Background(), &p2p_api.ReportValidBlockRequest{
		PeerId:    pid.String(),
		BlockHash: "abc",
	})
	require.NoError(t, err)
	require.True(t, resp.Success)

	got, _ := reg.Get(pid.String())
	require.Equal(t, int64(1), got.BlocksReceived)
}

func TestIsPeerMalicious_BannedPeerIsMalicious(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.AddBanScore(pid.String(), "spam", 0)
	reg.AddBanScore(pid.String(), "spam", 0)

	resp, err := s.IsPeerMalicious(context.Background(), &p2p_api.IsPeerMaliciousRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.IsMalicious)
}

func TestIsPeerMalicious_CleanPeer(t *testing.T) {
	s, _, pid := freshTestServer(t)

	resp, err := s.IsPeerMalicious(context.Background(), &p2p_api.IsPeerMaliciousRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.False(t, resp.IsMalicious)
}

func TestIsPeerMalicious_EmptyID(t *testing.T) {
	s, _, _ := freshTestServer(t)

	resp, err := s.IsPeerMalicious(context.Background(), &p2p_api.IsPeerMaliciousRequest{PeerId: ""})
	require.NoError(t, err)
	require.False(t, resp.IsMalicious)
}

func TestIsPeerUnhealthy_LowReputation(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	// Drive reputation below 40 by recording a malicious event.
	reg.UpdateMetrics(pid.String(), 0, 0, 0, false, false, true, 0)

	resp, err := s.IsPeerUnhealthy(context.Background(), &p2p_api.IsPeerUnhealthyRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.IsUnhealthy)
}

func TestIsPeerUnhealthy_UnknownPeer(t *testing.T) {
	s, _, pid := freshTestServer(t)

	resp, err := s.IsPeerUnhealthy(context.Background(), &p2p_api.IsPeerUnhealthyRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.IsUnhealthy)
}

func TestRecordCatchupSuccess_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.RecordCatchupSuccess(context.Background(), &p2p_api.RecordCatchupSuccessRequest{PeerId: "not-a-peer"})
	require.Error(t, err)
}

func TestRecordCatchupFailure_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.RecordCatchupFailure(context.Background(), &p2p_api.RecordCatchupFailureRequest{PeerId: "not-a-peer"})
	require.Error(t, err)
}

func TestRecordCatchupMalicious_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.RecordCatchupMalicious(context.Background(), &p2p_api.RecordCatchupMaliciousRequest{PeerId: "not-a-peer"})
	require.Error(t, err)
}

func TestUpdateCatchupError_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.UpdateCatchupError(context.Background(), &p2p_api.UpdateCatchupErrorRequest{PeerId: "not-a-peer"})
	require.Error(t, err)
}

func TestReportValidSubtree_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.ReportValidSubtree(context.Background(), &p2p_api.ReportValidSubtreeRequest{
		PeerId: "not-a-peer", SubtreeHash: "abc",
	})
	require.Error(t, err)
}

func TestReportValidBlock_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	_, err := s.ReportValidBlock(context.Background(), &p2p_api.ReportValidBlockRequest{
		PeerId: "not-a-peer", BlockHash: "abc",
	})
	require.Error(t, err)
}

func TestReportValidBlock_RejectsEmpty(t *testing.T) {
	s, _, _ := freshTestServer(t)

	_, err := s.ReportValidBlock(context.Background(), &p2p_api.ReportValidBlockRequest{})
	require.Error(t, err)

	_, err = s.ReportValidBlock(context.Background(), &p2p_api.ReportValidBlockRequest{PeerId: "x"})
	require.Error(t, err)
}

func TestIsPeerUnhealthy_InvalidPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	resp, err := s.IsPeerUnhealthy(context.Background(), &p2p_api.IsPeerUnhealthyRequest{PeerId: "not-a-peer"})
	require.NoError(t, err)
	require.True(t, resp.IsUnhealthy)
}

func TestIsPeerUnhealthy_LowSuccessRate(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	// Give 12 interactions: 4 success, 8 failure. Handler uses
	// `successes < total/2` (integer div), so 4 < 12/2 = 6 → unhealthy.
	for i := 0; i < 4; i++ {
		reg.UpdateMetrics(pid.String(), 0, 0, 0, true, false, false, 100)
	}
	for i := 0; i < 8; i++ {
		reg.UpdateMetrics(pid.String(), 0, 0, 0, false, true, false, 0)
	}

	resp, err := s.IsPeerUnhealthy(context.Background(), &p2p_api.IsPeerUnhealthyRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.IsUnhealthy)
	require.Contains(t, resp.Reason, "low")
}

func TestIsPeerUnhealthy_EmptyID(t *testing.T) {
	s, _, _ := freshTestServer(t)
	resp, err := s.IsPeerUnhealthy(context.Background(), &p2p_api.IsPeerUnhealthyRequest{PeerId: ""})
	require.NoError(t, err)
	require.True(t, resp.IsUnhealthy)
	require.Contains(t, resp.Reason, "empty")
}

func TestIsPeerUnhealthy_HealthyPeer(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	// Push reputation above the unhealthy threshold via successful interactions.
	for i := 0; i < 5; i++ {
		reg.UpdateMetrics(pid.String(), 0, 0, 0, true, false, false, 100)
	}

	resp, err := s.IsPeerUnhealthy(context.Background(), &p2p_api.IsPeerUnhealthyRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.False(t, resp.IsUnhealthy)
}
