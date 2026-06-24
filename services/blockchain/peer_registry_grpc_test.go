package blockchain

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newTestBlockchain returns a minimal Blockchain with only the peerRegistry set,
// sufficient for exercising the gRPC peer-registry handlers.
func newTestBlockchain() *Blockchain {
	return &Blockchain{
		peerRegistry: NewCentralizedPeerRegistry(DefaultBanConfig()),
	}
}

// ---------------------------------------------------------------------------
// RegisterPeer
// ---------------------------------------------------------------------------

func TestGRPC_RegisterPeer_Valid(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	_, err := b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{
			PeerId:        "peer-1",
			TransportType: blockchain_api.TransportType_TRANSPORT_HTTP,
			ClientName:    "grpc-test",
			Height:        42,
			DataHubUrl:    "http://peer1.example.com",
		},
	})
	require.NoError(t, err)

	got, ok := b.peerRegistry.Get("peer-1")
	require.True(t, ok)
	require.Equal(t, "peer-1", got.ID)
	require.Equal(t, blockchain_api.TransportType_TRANSPORT_HTTP, got.TransportType)
	require.Equal(t, "grpc-test", got.ClientName)
	require.Equal(t, uint32(42), got.Height)
	require.Equal(t, "http://peer1.example.com", got.DataHubURL)
}

func TestGRPC_RegisterPeer_NilPeerReturnsError(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	_, err := b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{Peer: nil})
	require.Error(t, err)
	require.Contains(t, err.Error(), "peer info is required")
}

// ---------------------------------------------------------------------------
// UpdatePeerMetrics
// ---------------------------------------------------------------------------

func TestGRPC_UpdatePeerMetrics(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	// Register the peer first.
	_, err := b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{PeerId: "peer-1"},
	})
	require.NoError(t, err)

	_, err = b.UpdatePeerMetrics(ctx, &blockchain_api.UpdatePeerMetricsRequest{
		PeerId:             "peer-1",
		Height:             300,
		BytesSentDelta:     2048,
		BytesReceivedDelta: 1024,
		RecordSuccess:      true,
		ResponseTimeMs:     200,
	})
	require.NoError(t, err)

	got, ok := b.peerRegistry.Get("peer-1")
	require.True(t, ok)
	require.Equal(t, uint32(300), got.Height)
	require.Equal(t, uint64(2048), got.BytesSent)
	require.Equal(t, uint64(1024), got.BytesReceived)
	require.Equal(t, int64(1), got.InteractionSuccesses)
	require.Equal(t, int64(200), got.AvgResponseTimeMs)
}

// ---------------------------------------------------------------------------
// RemovePeer
// ---------------------------------------------------------------------------

func TestGRPC_RemovePeer(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	_, _ = b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{PeerId: "peer-1"},
	})
	require.Equal(t, 1, b.peerRegistry.Count())

	_, err := b.RemovePeer(ctx, &blockchain_api.RemovePeerRequest{PeerId: "peer-1"})
	require.NoError(t, err)
	require.Equal(t, 0, b.peerRegistry.Count())

	_, ok := b.peerRegistry.Get("peer-1")
	require.False(t, ok)
}

// ---------------------------------------------------------------------------
// ListPeers
// ---------------------------------------------------------------------------

func TestGRPC_ListPeers_NoFilter(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	_, _ = b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{
			PeerId:        "http-peer",
			TransportType: blockchain_api.TransportType_TRANSPORT_HTTP,
			Height:        100,
		},
	})
	_, _ = b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{
			PeerId:        "wire-peer",
			TransportType: blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
			Height:        50,
		},
	})

	resp, err := b.ListPeers(ctx, &blockchain_api.ListPeersRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 2)
}

func TestGRPC_ListPeers_TransportFilter(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	_, _ = b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{
			PeerId:        "http-peer",
			TransportType: blockchain_api.TransportType_TRANSPORT_HTTP,
			Height:        100,
		},
	})
	_, _ = b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{
			PeerId:        "wire-peer",
			TransportType: blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
			Height:        50,
		},
	})

	resp, err := b.ListPeers(ctx, &blockchain_api.ListPeersRequest{
		FilterTransport: true,
		TransportFilter: blockchain_api.TransportType_TRANSPORT_HTTP,
	})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 1)
	require.Equal(t, "http-peer", resp.Peers[0].PeerId)
}

func TestGRPC_ListPeers_MinHeight(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	_, _ = b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{PeerId: "low", Height: 10},
	})
	_, _ = b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{PeerId: "high", Height: 200},
	})

	resp, err := b.ListPeers(ctx, &blockchain_api.ListPeersRequest{MinHeight: 100})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 1)
	require.Equal(t, "high", resp.Peers[0].PeerId)
}

func TestGRPC_ListPeers_ExcludeBanned(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	_, _ = b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{PeerId: "good-peer", Height: 50},
	})
	_, _ = b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{PeerId: "bad-peer", Height: 50},
	})

	// Ban "bad-peer" via AddBanScore to push past threshold.
	_, _ = b.AddBanScore(ctx, &blockchain_api.AddBanScoreRequest{
		PeerId: "bad-peer", Reason: "test", Points: 100,
	})

	resp, err := b.ListPeers(ctx, &blockchain_api.ListPeersRequest{ExcludeBanned: true})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 1)
	require.Equal(t, "good-peer", resp.Peers[0].PeerId)
}

// ---------------------------------------------------------------------------
// GetPeer
// ---------------------------------------------------------------------------

func TestGRPC_GetPeer_Found(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	_, _ = b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{
			PeerId:     "peer-1",
			ClientName: "found-me",
			Height:     77,
		},
	})

	resp, err := b.GetPeer(ctx, &blockchain_api.GetPeerRequest{PeerId: "peer-1"})
	require.NoError(t, err)
	require.False(t, resp.NotFound)
	require.NotNil(t, resp.Peer)
	require.Equal(t, "peer-1", resp.Peer.PeerId)
	require.Equal(t, "found-me", resp.Peer.ClientName)
	require.Equal(t, uint32(77), resp.Peer.Height)
}

func TestGRPC_GetPeer_NotFound(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	resp, err := b.GetPeer(ctx, &blockchain_api.GetPeerRequest{PeerId: "nonexistent"})
	require.NoError(t, err)
	require.True(t, resp.NotFound)
	require.Nil(t, resp.Peer)
}

// ---------------------------------------------------------------------------
// AddBanScore
// ---------------------------------------------------------------------------

func TestGRPC_AddBanScore_Basic(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	resp, err := b.AddBanScore(ctx, &blockchain_api.AddBanScoreRequest{
		PeerId: "peer-1", Reason: "test", Points: 25,
	})
	require.NoError(t, err)
	require.Equal(t, int32(25), resp.Score)
	require.False(t, resp.Banned)
}

func TestGRPC_AddBanScore_ThresholdBan(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	// First call: below threshold.
	_, _ = b.AddBanScore(ctx, &blockchain_api.AddBanScoreRequest{
		PeerId: "peer-1", Reason: "test", Points: 99,
	})

	// Second call: crosses threshold.
	resp, err := b.AddBanScore(ctx, &blockchain_api.AddBanScoreRequest{
		PeerId: "peer-1", Reason: "test", Points: 1,
	})
	require.NoError(t, err)
	require.Equal(t, int32(100), resp.Score)
	require.True(t, resp.Banned)
}

// ---------------------------------------------------------------------------
// IsPeerBanned
// ---------------------------------------------------------------------------

func TestGRPC_IsPeerBanned_NotBanned(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	resp, err := b.IsPeerBanned(ctx, &blockchain_api.IsPeerBannedRequest{PeerId: "clean-peer"})
	require.NoError(t, err)
	require.False(t, resp.Banned)
}

func TestGRPC_IsPeerBanned_Banned(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	_, _ = b.AddBanScore(ctx, &blockchain_api.AddBanScoreRequest{
		PeerId: "peer-1", Reason: "test", Points: 100,
	})

	resp, err := b.IsPeerBanned(ctx, &blockchain_api.IsPeerBannedRequest{PeerId: "peer-1"})
	require.NoError(t, err)
	require.True(t, resp.Banned)
}

// ---------------------------------------------------------------------------
// ListBannedPeers
// ---------------------------------------------------------------------------

func TestGRPC_ListBannedPeers_Empty(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	resp, err := b.ListBannedPeers(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Empty(t, resp.PeerIds)
}

func TestGRPC_ListBannedPeers_WithBanned(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	_, _ = b.AddBanScore(ctx, &blockchain_api.AddBanScoreRequest{
		PeerId: "bad-1", Reason: "test", Points: 100,
	})
	_, _ = b.AddBanScore(ctx, &blockchain_api.AddBanScoreRequest{
		PeerId: "bad-2", Reason: "test", Points: 150,
	})
	// Leave one peer below threshold.
	_, _ = b.AddBanScore(ctx, &blockchain_api.AddBanScoreRequest{
		PeerId: "good-1", Reason: "test", Points: 10,
	})

	resp, err := b.ListBannedPeers(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, resp.PeerIds, 2)
	require.ElementsMatch(t, []string{"bad-1", "bad-2"}, resp.PeerIds)
}

// ---------------------------------------------------------------------------
// ClearBannedPeers
// ---------------------------------------------------------------------------

func TestGRPC_ClearBannedPeers(t *testing.T) {
	b := newTestBlockchain()
	ctx := context.Background()

	_, _ = b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{PeerId: "peer-1"},
	})
	_, _ = b.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: &blockchain_api.PeerRegistryInfo{PeerId: "peer-2"},
	})

	_, _ = b.AddBanScore(ctx, &blockchain_api.AddBanScoreRequest{
		PeerId: "peer-1", Reason: "test", Points: 100,
	})
	_, _ = b.AddBanScore(ctx, &blockchain_api.AddBanScoreRequest{
		PeerId: "peer-2", Reason: "test", Points: 100,
	})

	bannedResp, _ := b.ListBannedPeers(ctx, &emptypb.Empty{})
	require.Len(t, bannedResp.PeerIds, 2)

	_, err := b.ClearBannedPeers(ctx, &emptypb.Empty{})
	require.NoError(t, err)

	bannedResp, _ = b.ListBannedPeers(ctx, &emptypb.Empty{})
	require.Empty(t, bannedResp.PeerIds)

	// Verify PeerInfo ban status is also cleared.
	p1, ok := b.peerRegistry.Get("peer-1")
	require.True(t, ok)
	require.False(t, p1.IsBanned)
	require.Equal(t, int32(0), p1.BanScore)
}

// ---------------------------------------------------------------------------
// peerInfoToProto / protoToPeerInfo round-trip
// ---------------------------------------------------------------------------

func TestGRPC_PeerInfoProtoRoundTrip(t *testing.T) {
	hash, err := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	require.NoError(t, err)

	now := time.Now().Truncate(time.Microsecond) // proto timestamps have microsecond precision at best

	original := &PeerInfo{
		ID:                     "round-trip-peer",
		TransportType:          blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
		ClientName:             "test-client/1.0",
		Height:                 654321,
		DataHubURL:             "http://data.example.com",
		NetworkAddress:         "192.168.1.100:8333",
		IsBanned:               true,
		BanScore:               42,
		Storage:                "aerospike",
		BytesSent:              999999,
		BytesReceived:          888888,
		InteractionAttempts:    100,
		InteractionSuccesses:   90,
		InteractionFailures:    8,
		MaliciousCount:         2,
		ReputationScore:        73.5,
		AvgResponseTimeMs:      150,
		ConnectedAt:            now.Add(-1 * time.Hour),
		LastMessageTime:        now.Add(-30 * time.Second),
		LastInteractionAttempt: now.Add(-10 * time.Second),
		LastInteractionSuccess: now.Add(-10 * time.Second),
		LastInteractionFailure: now.Add(-5 * time.Minute),
		LastSeen:               now,
		BlockHash:              hash,
	}

	proto := peerInfoToProto(original)
	roundTripped := protoToPeerInfo(ulogger.TestLogger{}, proto)

	require.Equal(t, original.ID, roundTripped.ID)
	require.Equal(t, original.TransportType, roundTripped.TransportType)
	require.Equal(t, original.ClientName, roundTripped.ClientName)
	require.Equal(t, original.Height, roundTripped.Height)
	require.Equal(t, original.DataHubURL, roundTripped.DataHubURL)
	require.Equal(t, original.NetworkAddress, roundTripped.NetworkAddress)
	require.Equal(t, original.IsBanned, roundTripped.IsBanned)
	require.Equal(t, original.BanScore, roundTripped.BanScore)
	require.Equal(t, original.Storage, roundTripped.Storage)
	require.Equal(t, original.BytesSent, roundTripped.BytesSent)
	require.Equal(t, original.BytesReceived, roundTripped.BytesReceived)
	require.Equal(t, original.InteractionAttempts, roundTripped.InteractionAttempts)
	require.Equal(t, original.InteractionSuccesses, roundTripped.InteractionSuccesses)
	require.Equal(t, original.InteractionFailures, roundTripped.InteractionFailures)
	require.Equal(t, original.MaliciousCount, roundTripped.MaliciousCount)
	require.Equal(t, original.ReputationScore, roundTripped.ReputationScore)
	require.Equal(t, original.AvgResponseTimeMs, roundTripped.AvgResponseTimeMs)

	// Timestamps: protobuf round-trips lose sub-microsecond precision, but we
	// truncated above so direct comparison is safe within a second.
	require.WithinDuration(t, original.ConnectedAt, roundTripped.ConnectedAt, time.Microsecond)
	require.WithinDuration(t, original.LastMessageTime, roundTripped.LastMessageTime, time.Microsecond)
	require.WithinDuration(t, original.LastInteractionAttempt, roundTripped.LastInteractionAttempt, time.Microsecond)
	require.WithinDuration(t, original.LastInteractionSuccess, roundTripped.LastInteractionSuccess, time.Microsecond)
	require.WithinDuration(t, original.LastInteractionFailure, roundTripped.LastInteractionFailure, time.Microsecond)
	require.WithinDuration(t, original.LastSeen, roundTripped.LastSeen, time.Microsecond)

	// BlockHash should survive the round-trip.
	require.NotNil(t, roundTripped.BlockHash)
	require.Equal(t, original.BlockHash.String(), roundTripped.BlockHash.String())
}

func TestGRPC_PeerInfoProtoRoundTrip_NilBlockHash(t *testing.T) {
	original := &PeerInfo{
		ID:        "no-hash-peer",
		Height:    10,
		BlockHash: nil,
	}

	proto := peerInfoToProto(original)
	require.Nil(t, proto.BlockHash)

	roundTripped := protoToPeerInfo(ulogger.TestLogger{}, proto)
	require.Nil(t, roundTripped.BlockHash)
}

func TestGRPC_PeerInfoProtoRoundTrip_ZeroTimestamps(t *testing.T) {
	original := &PeerInfo{
		ID: "zero-times",
		// All time fields left at zero value.
	}

	proto := peerInfoToProto(original)
	roundTripped := protoToPeerInfo(ulogger.TestLogger{}, proto)

	require.Equal(t, original.ID, roundTripped.ID)

	// Zero time -> proto -> back should yield a time at or very near the zero value.
	// timestamppb.New(time.Time{}) creates year 0001 which round-trips consistently.
	require.True(t, roundTripped.ConnectedAt.IsZero() || roundTripped.ConnectedAt.Equal(time.Time{}))
	require.True(t, roundTripped.LastSeen.IsZero() || roundTripped.LastSeen.Equal(time.Time{}))
	require.True(t, roundTripped.LastMessageTime.IsZero() || roundTripped.LastMessageTime.Equal(time.Time{}))
	require.True(t, roundTripped.LastInteractionAttempt.IsZero() || roundTripped.LastInteractionAttempt.Equal(time.Time{}))
	require.True(t, roundTripped.LastInteractionSuccess.IsZero() || roundTripped.LastInteractionSuccess.Equal(time.Time{}))
	require.True(t, roundTripped.LastInteractionFailure.IsZero() || roundTripped.LastInteractionFailure.Equal(time.Time{}))
}

// ---------------------------------------------------------------------------
// blockHashToBytes / bytesToBlockHash round-trip
// ---------------------------------------------------------------------------

func TestGRPC_BlockHashBytesRoundTrip(t *testing.T) {
	hash, err := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	require.NoError(t, err)

	b := blockHashToBytes(hash)
	require.Len(t, b, chainhash.HashSize)

	roundTripped, err := bytesToBlockHash(b)
	require.NoError(t, err)
	require.NotNil(t, roundTripped)
	require.Equal(t, hash.String(), roundTripped.String())
}

func TestGRPC_BlockHashBytesRoundTrip_Nil(t *testing.T) {
	b := blockHashToBytes(nil)
	require.Nil(t, b)

	h, err := bytesToBlockHash(nil)
	require.NoError(t, err)
	require.Nil(t, h)
}

func TestGRPC_BlockHashBytesRoundTrip_Empty(t *testing.T) {
	h, err := bytesToBlockHash([]byte{})
	require.NoError(t, err)
	require.Nil(t, h)
}

func TestGRPC_BlockHashBytesRoundTrip_InvalidLength(t *testing.T) {
	// A slice that is not 32 bytes returns (nil, error) — caller logs and
	// continues with the rest of the entry.
	h, err := bytesToBlockHash([]byte{0x01, 0x02, 0x03})
	require.Error(t, err)
	require.Nil(t, h)
}

// ---------------------------------------------------------------------------
// protoTimeToTime
// ---------------------------------------------------------------------------

func TestGRPC_ProtoTimeToTime_Nil(t *testing.T) {
	result := protoTimeToTime(nil)
	require.True(t, result.IsZero())
}

func TestGRPC_ProtoTimeToTime_Valid(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	ts := timestamppb.New(now)
	result := protoTimeToTime(ts)
	require.WithinDuration(t, now, result, time.Microsecond)
}

// ---------------------------------------------------------------------------
// blockHashToBytes does not alias the input
// ---------------------------------------------------------------------------

func TestGRPC_BlockHashToBytes_NoCopy(t *testing.T) {
	hash, err := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	require.NoError(t, err)

	b := blockHashToBytes(hash)

	// Mutating the returned slice should not affect the original hash.
	b[0] = 0xFF
	require.NotEqual(t, byte(0xFF), hash[0], "blockHashToBytes must return a copy, not an alias")
}
