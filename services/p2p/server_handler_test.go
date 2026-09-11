package p2p

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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
		CatchupAttempts:        7,
		CatchupSuccesses:       5,
		CatchupFailures:        2,
		BlocksReceived:         11,
		SubtreesReceived:       12,
		TransactionsReceived:   13,
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
	require.Equal(t, bp.CatchupAttempts, out.CatchupAttempts)
	require.Equal(t, bp.CatchupSuccesses, out.CatchupSuccesses)
	require.Equal(t, bp.CatchupFailures, out.CatchupFailures)
	require.Equal(t, bp.BlocksReceived, out.BlocksReceived)
	require.Equal(t, bp.SubtreesReceived, out.SubtreesReceived)
	require.Equal(t, bp.TransactionsReceived, out.TransactionsReceived)
}

// TestServer_GetPeerRegistry_ReceivedCountersSurviveWire drives the path a
// gRPC consumer (the asset service's miner-tier cache) takes: the blockchain
// registry records received blocks/subtrees/txs, GetPeerRegistry builds the
// proto, the message is marshalled and unmarshalled as gRPC would, and the
// client-side converter maps it back. The asset service's tierMiner predicate
// requires BlocksReceived > 0, so a zero here silently demotes every miner to
// the ordinary per-peer rate limit.
func TestServer_GetPeerRegistry_ReceivedCountersSurviveWire(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	s.addConnectedPeer(pid, "miner/1.0", 10, nil, "")

	reg.RecordBlockReceived(pid.String(), 5)
	reg.RecordBlockReceived(pid.String(), 5)
	reg.RecordSubtreeReceived(pid.String(), 5)
	reg.RecordTransactionReceived(pid.String())

	resp, err := s.GetPeerRegistry(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 1)

	wire, err := proto.Marshal(resp.Peers[0])
	require.NoError(t, err)
	decoded := &p2p_api.PeerRegistryInfo{}
	require.NoError(t, proto.Unmarshal(wire, decoded))

	got, err := convertFromAPIPeerInfo(decoded)
	require.NoError(t, err)
	require.Equal(t, pid, got.ID)
	require.Equal(t, int64(2), got.BlocksReceived)
	require.Equal(t, int64(1), got.SubtreesReceived)
	require.Equal(t, int64(1), got.TransactionsReceived)
}

// TestServer_PeerRegistryInfo_CarriesEveryPeerInfoField is the guard against the
// class of bug fixed above: a field added to the public p2p.PeerInfo contract but
// never given a wire field, so gRPC consumers silently read a zero. Every
// exported p2p.PeerInfo field must have a counterpart in p2p_api.PeerRegistryInfo
// unless it is explicitly listed as intentionally in-process only.
func TestServer_PeerRegistryInfo_CarriesEveryPeerInfoField(t *testing.T) {
	// Fields deliberately not carried over gRPC. Adding a name here is a
	// contract decision: document the reason.
	notOnWire := map[string]string{
		"CatchupBlocks":        "no writer exists yet; add a wire field when one does",
		"LastSyncAttempt":      "sync backoff state is local to the p2p process",
		"SyncAttemptCount":     "sync backoff state is local to the p2p process",
		"LastReputationReset":  "sync backoff state is local to the p2p process",
		"ReputationResetCount": "sync backoff state is local to the p2p process",
	}
	// Domain fields whose wire name differs beyond letter case.
	renamed := map[string]string{
		"AvgResponseTime": "AvgResponseTimeMs",
	}

	wireFields := map[string]bool{}
	wt := reflect.TypeOf(p2p_api.PeerRegistryInfo{})
	for i := 0; i < wt.NumField(); i++ {
		wireFields[strings.ToLower(wt.Field(i).Name)] = true
	}

	dt := reflect.TypeOf(PeerInfo{})
	for i := 0; i < dt.NumField(); i++ {
		name := dt.Field(i).Name
		if !dt.Field(i).IsExported() {
			continue
		}
		if _, ok := notOnWire[name]; ok {
			continue
		}
		if alias, ok := renamed[name]; ok {
			name = alias
		}
		require.True(t, wireFields[strings.ToLower(name)],
			"p2p.PeerInfo.%s has no p2p_api.PeerRegistryInfo field; add one (and map it in peerInfoToP2PProto/convertFromAPIPeerInfo) or list it in notOnWire", dt.Field(i).Name)
	}

	for name := range notOnWire {
		_, ok := dt.FieldByName(name)
		require.True(t, ok, "notOnWire lists %s, which is no longer a p2p.PeerInfo field", name)
	}
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
