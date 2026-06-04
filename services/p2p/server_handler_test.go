package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestServer_PeerInfoToP2PProto_RoundTripFields(t *testing.T) {
	now := time.Now()
	hash, err := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	require.NoError(t, err)

	bp := &blockchain.PeerInfo{
		ID:                     "peer-1",
		Height:                 100,
		BlockHash:              hash,
		DataHubURL:             "http://peer.example",
		BanScore:               42,
		IsBanned:               true,
		IsConnected:            true,
		ConnectedAt:            now,
		BytesReceived:          1024,
		LastBlockTime:          now,
		LastMessageTime:        now,
		InteractionAttempts:    5,
		InteractionSuccesses:   4,
		InteractionFailures:    1,
		LastInteractionAttempt: now,
		LastInteractionSuccess: now,
		LastInteractionFailure: now,
		ReputationScore:        80.0,
		MaliciousCount:         0,
		AvgResponseTimeMs:      150,
		Storage:                "full",
		ClientName:             "test/1.0",
		LastCatchupError:       "boom",
		LastCatchupErrorTime:   now,
	}

	out := peerInfoToP2PProto(bp)
	require.Equal(t, bp.ID, out.Id)
	require.Equal(t, bp.Height, out.Height)
	require.Equal(t, hash.String(), out.BlockHash)
	require.Equal(t, bp.DataHubURL, out.DataHubUrl)
	require.Equal(t, bp.BanScore, out.BanScore)
	require.Equal(t, bp.IsBanned, out.IsBanned)
	require.Equal(t, bp.IsConnected, out.IsConnected)
	require.Equal(t, bp.AvgResponseTimeMs, out.AvgResponseTimeMs)
	require.Equal(t, bp.Storage, out.Storage)
	require.Equal(t, bp.ClientName, out.ClientName)
	require.Equal(t, bp.LastCatchupError, out.LastCatchupError)
	require.Equal(t, now.Unix(), out.ConnectedAt)
}

func TestServer_PeerInfoToP2PProto_NilHashAndZeroTimes(t *testing.T) {
	out := peerInfoToP2PProto(&blockchain.PeerInfo{ID: "p"})
	require.Equal(t, "p", out.Id)
	require.Empty(t, out.BlockHash)
	require.Equal(t, int64(0), out.ConnectedAt, "zero time must serialise as 0 unix")
	require.Equal(t, int64(0), out.LastMessageTime)
}

func TestServer_AddBanScore_KnownReasonNoBanBelowThreshold(t *testing.T) {
	s, _, pid := freshTestServer(t)
	s.banList = noopBanList{}

	resp, err := s.AddBanScore(context.Background(), &p2p_api.AddBanScoreRequest{
		PeerId: pid.String(),
		Reason: "spam", // 50 points; threshold 100 → not banned
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	bResp, err := s.IsBanned(context.Background(), &p2p_api.IsBannedRequest{IpOrSubnet: pid.String()})
	require.NoError(t, err)
	require.False(t, bResp.IsBanned)
}

func TestServer_AddBanScore_CrossesThresholdBans(t *testing.T) {
	s, _, pid := freshTestServer(t)
	s.banList = noopBanList{}

	_, _ = s.AddBanScore(context.Background(), &p2p_api.AddBanScoreRequest{PeerId: pid.String(), Reason: "spam"})
	_, _ = s.AddBanScore(context.Background(), &p2p_api.AddBanScoreRequest{PeerId: pid.String(), Reason: "spam"})

	bResp, err := s.IsBanned(context.Background(), &p2p_api.IsBannedRequest{IpOrSubnet: pid.String()})
	require.NoError(t, err)
	require.True(t, bResp.IsBanned)
}

func TestServer_AddBanScore_UnknownReasonStillRecorded(t *testing.T) {
	s, _, pid := freshTestServer(t)

	resp, err := s.AddBanScore(context.Background(), &p2p_api.AddBanScoreRequest{
		PeerId: pid.String(),
		Reason: "made-up-reason",
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)
}

func TestServer_RecordBytesDownloaded_AccumulatesDelta(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	_, err := s.RecordBytesDownloaded(context.Background(), &p2p_api.RecordBytesDownloadedRequest{
		PeerId:          pid.String(),
		BytesDownloaded: 1500,
	})
	require.NoError(t, err)
	_, err = s.RecordBytesDownloaded(context.Background(), &p2p_api.RecordBytesDownloadedRequest{
		PeerId:          pid.String(),
		BytesDownloaded: 500,
	})
	require.NoError(t, err)

	got, _ := reg.Get(pid.String())
	require.Equal(t, uint64(2000), got.BytesReceived)
}

func TestServer_RecordBytesDownloaded_RejectsBadPeerID(t *testing.T) {
	s, _, _ := freshTestServer(t)

	_, err := s.RecordBytesDownloaded(context.Background(), &p2p_api.RecordBytesDownloadedRequest{
		PeerId:          "not-a-peer-id",
		BytesDownloaded: 1,
	})
	require.Error(t, err)
}

func TestServer_ResetReputation_ReportsResetCount(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	reg.UpdateMetrics(pid.String(), 0, 0, 0, false, false, true, 0) // malicious → low rep

	resp, err := s.ResetReputation(context.Background(), &p2p_api.ResetReputationRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.Equal(t, int32(1), resp.PeersReset)

	got, _ := reg.Get(pid.String())
	require.Equal(t, 50.0, got.ReputationScore)
}

func TestServer_ResetReputation_AllPeers(t *testing.T) {
	s, reg, _ := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: "a"})
	reg.Register(&blockchain.PeerInfo{ID: "b"})

	resp, err := s.ResetReputation(context.Background(), &p2p_api.ResetReputationRequest{PeerId: ""})
	require.NoError(t, err)
	require.Equal(t, int32(2), resp.PeersReset)
}

func TestServer_GetPeerRegistry_ReturnsAll(t *testing.T) {
	s, reg, _ := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: "a", Height: 100})
	reg.Register(&blockchain.PeerInfo{ID: "b", Height: 200})

	resp, err := s.GetPeerRegistry(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 2)
}

func TestServer_GetPeer_FoundAndNotFound(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String(), Height: 99})

	resp, err := s.GetPeer(context.Background(), &p2p_api.GetPeerRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.Found)
	require.Equal(t, uint32(99), resp.Peer.Height)

	other, _ := peer.Decode("12D3KooWAfBVdmphtMFPVq3GEpcg3QMiRbrwD9mpd6D6fc4CswRw")
	resp, err = s.GetPeer(context.Background(), &p2p_api.GetPeerRequest{PeerId: other.String()})
	require.NoError(t, err)
	require.False(t, resp.Found)
}

func TestServer_GetPeer_InvalidIDIsNotFound(t *testing.T) {
	s, _, _ := freshTestServer(t)

	resp, err := s.GetPeer(context.Background(), &p2p_api.GetPeerRequest{PeerId: "not-a-peer"})
	require.NoError(t, err)
	require.False(t, resp.Found)
}

func TestServer_ClearBanned_ClearsIPList(t *testing.T) {
	// Server.ClearBanned only clears the IP-based banList. The ban_list field
	// is not exercised in this lightweight test setup, but the call should not
	// panic and should return Ok=true even when banList is unset (we'll only
	// test the no-panic path here since BanList is a separate component).
	s, _, _ := freshTestServer(t)
	s.banList = noopBanList{}
	resp, err := s.ClearBanned(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.True(t, resp.Ok)
}

func TestServer_IsBanned_FallsThroughToRegistry(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	s.banList = noopBanList{}
	reg.AddBanScore(pid.String(), "spam", 0)
	reg.AddBanScore(pid.String(), "spam", 0)

	resp, err := s.IsBanned(context.Background(), &p2p_api.IsBannedRequest{IpOrSubnet: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.IsBanned)
}

// noopBanList satisfies BanListI for tests that exercise the registry-side
// ban path without dragging in the real banlist + interceptor wiring.
type noopBanList struct{}

func (noopBanList) Init(context.Context) error                         { return nil }
func (noopBanList) Add(_ context.Context, _ string, _ time.Time) error { return nil }
func (noopBanList) Remove(_ context.Context, _ string) error           { return nil }
func (noopBanList) Clear()                                             {}
func (noopBanList) IsBanned(string) bool                               { return false }
func (noopBanList) ListBanned() []string                               { return nil }
func (noopBanList) Subscribe() chan BanEvent                           { return make(chan BanEvent, 1) }
func (noopBanList) Unsubscribe(chan BanEvent)                          {}
