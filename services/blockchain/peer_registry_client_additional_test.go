package blockchain

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// setupRegistryGRPC starts a real gRPC server backed by a CentralizedPeerRegistry
// and returns a PeerRegistryClientI connected to it. The cleanup function stops
// the server and closes the client.
func setupRegistryGRPC(t *testing.T) (PeerRegistryClientI, func()) {
	t.Helper()

	registry := NewCentralizedPeerRegistry(DefaultBanConfig())
	bc := &Blockchain{peerRegistry: registry}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	blockchain_api.RegisterPeerRegistryServiceServer(srv, bc)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := NewPeerRegistryClientFromConn(conn)

	cleanup := func() {
		_ = client.Close()
		_ = conn.Close()
		srv.Stop()
	}
	return client, cleanup
}

// ---------------------------------------------------------------------------
// RegisterPeer -> GetPeer round-trip
// ---------------------------------------------------------------------------

func TestRegistryClient_RegisterThenGetPeer(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	err := client.RegisterPeer(ctx, &PeerInfo{
		ID:             "rpc-peer-1",
		TransportType:  blockchain_api.TransportType_TRANSPORT_HTTP,
		ClientName:     "test-client/1.0",
		Height:         42,
		DataHubURL:     "http://peer1.example.com",
		NetworkAddress: "10.0.0.1:8333",
		Storage:        "aerospike",
	})
	require.NoError(t, err)

	got, found, err := client.GetPeer(ctx, "rpc-peer-1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "rpc-peer-1", got.ID)
	require.Equal(t, blockchain_api.TransportType_TRANSPORT_HTTP, got.TransportType)
	require.Equal(t, "test-client/1.0", got.ClientName)
	require.Equal(t, uint32(42), got.Height)
	require.Equal(t, "http://peer1.example.com", got.DataHubURL)
	require.Equal(t, "10.0.0.1:8333", got.NetworkAddress)
	require.Equal(t, "aerospike", got.Storage)
}

// ---------------------------------------------------------------------------
// GetPeer - not found
// ---------------------------------------------------------------------------

func TestRegistryClient_GetPeer_NotFound(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	got, found, err := client.GetPeer(ctx, "nonexistent")
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, got)
}

// ---------------------------------------------------------------------------
// UpdatePeerMetrics -> verify via GetPeer
// ---------------------------------------------------------------------------

func TestRegistryClient_UpdatePeerMetrics(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	// Register first.
	err := client.RegisterPeer(ctx, &PeerInfo{
		ID:     "metrics-peer",
		Height: 10,
	})
	require.NoError(t, err)

	// Update metrics with success.
	err = client.UpdatePeerMetrics(ctx, "metrics-peer",
		500,   // height
		4096,  // bytesSentDelta
		2048,  // bytesRecvDelta
		true,  // recordSuccess
		false, // recordFailure
		false, // recordMalicious
		150,   // responseTimeMs
	)
	require.NoError(t, err)

	got, found, err := client.GetPeer(ctx, "metrics-peer")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint32(500), got.Height)
	require.Equal(t, uint64(4096), got.BytesSent)
	require.Equal(t, uint64(2048), got.BytesReceived)
	require.Equal(t, int64(1), got.InteractionSuccesses)
	require.Equal(t, int64(150), got.AvgResponseTimeMs)
}

func TestRegistryClient_UpdatePeerMetrics_Failure(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	err := client.RegisterPeer(ctx, &PeerInfo{ID: "fail-peer"})
	require.NoError(t, err)

	err = client.UpdatePeerMetrics(ctx, "fail-peer",
		0,     // height
		0,     // bytesSentDelta
		0,     // bytesRecvDelta
		false, // recordSuccess
		true,  // recordFailure
		false, // recordMalicious
		0,     // responseTimeMs
	)
	require.NoError(t, err)

	got, found, err := client.GetPeer(ctx, "fail-peer")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(1), got.InteractionFailures)
}

func TestRegistryClient_UpdatePeerMetrics_Malicious(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	err := client.RegisterPeer(ctx, &PeerInfo{ID: "evil-peer"})
	require.NoError(t, err)

	err = client.UpdatePeerMetrics(ctx, "evil-peer",
		0,     // height
		0,     // bytesSentDelta
		0,     // bytesRecvDelta
		false, // recordSuccess
		false, // recordFailure
		true,  // recordMalicious
		0,     // responseTimeMs
	)
	require.NoError(t, err)

	got, found, err := client.GetPeer(ctx, "evil-peer")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(1), got.MaliciousCount)
	// Malicious peers should have pinned low reputation.
	require.Equal(t, 5.0, got.ReputationScore)
}

// ---------------------------------------------------------------------------
// RemovePeer -> verify via GetPeer (not found)
// ---------------------------------------------------------------------------

func TestRegistryClient_RemovePeer(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	err := client.RegisterPeer(ctx, &PeerInfo{ID: "doomed-peer"})
	require.NoError(t, err)

	// Confirm it exists.
	_, found, err := client.GetPeer(ctx, "doomed-peer")
	require.NoError(t, err)
	require.True(t, found)

	// Remove.
	err = client.RemovePeer(ctx, "doomed-peer")
	require.NoError(t, err)

	// Should be gone.
	got, found, err := client.GetPeer(ctx, "doomed-peer")
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, got)
}

func TestRegistryClient_RemovePeer_Nonexistent(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	// Removing a peer that does not exist should not error.
	err := client.RemovePeer(ctx, "ghost")
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// ListPeers with various filters
// ---------------------------------------------------------------------------

func TestRegistryClient_ListPeers_NoFilter(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	err := client.RegisterPeer(ctx, &PeerInfo{
		ID:            "peer-a",
		TransportType: blockchain_api.TransportType_TRANSPORT_HTTP,
		Height:        100,
	})
	require.NoError(t, err)

	err = client.RegisterPeer(ctx, &PeerInfo{
		ID:            "peer-b",
		TransportType: blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
		Height:        200,
	})
	require.NoError(t, err)

	peers, err := client.ListPeers(ctx, nil, 0, 0, false, false)
	require.NoError(t, err)
	require.Len(t, peers, 2)
}

func TestRegistryClient_ListPeers_TransportFilter(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	err := client.RegisterPeer(ctx, &PeerInfo{
		ID:               "http-only",
		TransportType:    blockchain_api.TransportType_TRANSPORT_HTTP,
		TransportTypeSet: true,
		Height:           100,
	})
	require.NoError(t, err)

	err = client.RegisterPeer(ctx, &PeerInfo{
		ID:               "wire-only",
		TransportType:    blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
		TransportTypeSet: true,
		Height:           200,
	})
	require.NoError(t, err)

	filter := blockchain_api.TransportType_TRANSPORT_HTTP
	peers, err := client.ListPeers(ctx, &filter, 0, 0, false, false)
	require.NoError(t, err)
	require.Len(t, peers, 1)
	require.Equal(t, "http-only", peers[0].ID)
}

func TestRegistryClient_ListPeers_MinHeight(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	err := client.RegisterPeer(ctx, &PeerInfo{ID: "low", Height: 10})
	require.NoError(t, err)
	err = client.RegisterPeer(ctx, &PeerInfo{ID: "high", Height: 500})
	require.NoError(t, err)

	peers, err := client.ListPeers(ctx, nil, 0, 100, false, false)
	require.NoError(t, err)
	require.Len(t, peers, 1)
	require.Equal(t, "high", peers[0].ID)
}

func TestRegistryClient_ListPeers_ExcludeBanned(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	err := client.RegisterPeer(ctx, &PeerInfo{ID: "good-peer", Height: 50})
	require.NoError(t, err)
	err = client.RegisterPeer(ctx, &PeerInfo{ID: "bad-peer", Height: 50})
	require.NoError(t, err)

	// Ban "bad-peer" by pushing past threshold.
	_, _, err = client.AddBanScore(ctx, "bad-peer", "test", 100)
	require.NoError(t, err)

	peers, err := client.ListPeers(ctx, nil, 0, 0, true, false)
	require.NoError(t, err)
	require.Len(t, peers, 1)
	require.Equal(t, "good-peer", peers[0].ID)
}

// ---------------------------------------------------------------------------
// AddBanScore -> IsPeerBanned round-trip
// ---------------------------------------------------------------------------

func TestRegistryClient_AddBanScore_ThenIsBanned(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	// Below threshold — should not be banned.
	score, banned, err := client.AddBanScore(ctx, "peer-1", "minor", 50)
	require.NoError(t, err)
	require.Equal(t, int32(50), score)
	require.False(t, banned)

	isBanned, err := client.IsPeerBanned(ctx, "peer-1")
	require.NoError(t, err)
	require.False(t, isBanned)

	// Cross threshold — should become banned.
	score, banned, err = client.AddBanScore(ctx, "peer-1", "major", 50)
	require.NoError(t, err)
	require.Equal(t, int32(100), score)
	require.True(t, banned)

	isBanned, err = client.IsPeerBanned(ctx, "peer-1")
	require.NoError(t, err)
	require.True(t, isBanned)
}

func TestRegistryClient_IsPeerBanned_UnknownPeer(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	isBanned, err := client.IsPeerBanned(ctx, "never-seen")
	require.NoError(t, err)
	require.False(t, isBanned)
}

// ---------------------------------------------------------------------------
// ListBannedPeers -> ClearBannedPeers round-trip
// ---------------------------------------------------------------------------

func TestRegistryClient_ListBannedPeers(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	// No bans initially.
	banned, err := client.ListBannedPeers(ctx)
	require.NoError(t, err)
	require.Empty(t, banned)

	// Ban two peers.
	_, _, err = client.AddBanScore(ctx, "bad-1", "test", 100)
	require.NoError(t, err)
	_, _, err = client.AddBanScore(ctx, "bad-2", "test", 150)
	require.NoError(t, err)

	// Keep one below threshold.
	_, _, err = client.AddBanScore(ctx, "good-1", "test", 10)
	require.NoError(t, err)

	banned, err = client.ListBannedPeers(ctx)
	require.NoError(t, err)
	require.Len(t, banned, 2)
	require.ElementsMatch(t, []string{"bad-1", "bad-2"}, banned)
}

func TestRegistryClient_ClearBannedPeers(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	// Register and ban peers.
	err := client.RegisterPeer(ctx, &PeerInfo{ID: "peer-1"})
	require.NoError(t, err)
	err = client.RegisterPeer(ctx, &PeerInfo{ID: "peer-2"})
	require.NoError(t, err)

	_, _, err = client.AddBanScore(ctx, "peer-1", "test", 100)
	require.NoError(t, err)
	_, _, err = client.AddBanScore(ctx, "peer-2", "test", 100)
	require.NoError(t, err)

	banned, err := client.ListBannedPeers(ctx)
	require.NoError(t, err)
	require.Len(t, banned, 2)

	// Clear all bans.
	err = client.ClearBannedPeers(ctx)
	require.NoError(t, err)

	banned, err = client.ListBannedPeers(ctx)
	require.NoError(t, err)
	require.Empty(t, banned)

	// Verify peers themselves are no longer marked banned.
	isBanned, err := client.IsPeerBanned(ctx, "peer-1")
	require.NoError(t, err)
	require.False(t, isBanned)

	isBanned, err = client.IsPeerBanned(ctx, "peer-2")
	require.NoError(t, err)
	require.False(t, isBanned)
}

// ---------------------------------------------------------------------------
// Multiple operations in sequence — full lifecycle
// ---------------------------------------------------------------------------

func TestRegistryClient_FullLifecycle(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Register peer.
	err := client.RegisterPeer(ctx, &PeerInfo{
		ID:            "lifecycle-peer",
		TransportType: blockchain_api.TransportType_TRANSPORT_HTTP,
		Height:        100,
		ClientName:    "lifecycle-test/1.0",
	})
	require.NoError(t, err)

	// 2. Update metrics.
	err = client.UpdatePeerMetrics(ctx, "lifecycle-peer",
		200,   // new height
		1024,  // bytesSentDelta
		512,   // bytesRecvDelta
		true,  // recordSuccess
		false, // recordFailure
		false, // recordMalicious
		50,    // responseTimeMs
	)
	require.NoError(t, err)

	// 3. Verify via GetPeer.
	got, found, err := client.GetPeer(ctx, "lifecycle-peer")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint32(200), got.Height)
	require.Equal(t, uint64(1024), got.BytesSent)

	// 4. Add ban score (below threshold).
	score, banned, err := client.AddBanScore(ctx, "lifecycle-peer", "minor-infraction", 30)
	require.NoError(t, err)
	require.Equal(t, int32(30), score)
	require.False(t, banned)

	// 5. ListPeers should include this peer.
	peers, err := client.ListPeers(ctx, nil, 0, 0, false, false)
	require.NoError(t, err)
	require.Len(t, peers, 1)

	// 6. Remove peer.
	err = client.RemovePeer(ctx, "lifecycle-peer")
	require.NoError(t, err)

	// 7. Verify gone.
	_, found, err = client.GetPeer(ctx, "lifecycle-peer")
	require.NoError(t, err)
	require.False(t, found)
}

// ---------------------------------------------------------------------------
// Register updates existing peer fields
// ---------------------------------------------------------------------------

func TestRegistryClient_RegisterPeer_UpdateExisting(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	// Initial registration.
	err := client.RegisterPeer(ctx, &PeerInfo{
		ID:         "update-peer",
		ClientName: "v1",
		Height:     10,
	})
	require.NoError(t, err)

	// Update with new values.
	err = client.RegisterPeer(ctx, &PeerInfo{
		ID:         "update-peer",
		ClientName: "v2",
		Height:     20,
	})
	require.NoError(t, err)

	got, found, err := client.GetPeer(ctx, "update-peer")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "v2", got.ClientName)
	require.Equal(t, uint32(20), got.Height)
}

// ---------------------------------------------------------------------------
// ListPeers empty registry
// ---------------------------------------------------------------------------

func TestRegistryClient_ListPeers_EmptyRegistry(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	peers, err := client.ListPeers(ctx, nil, 0, 0, false, false)
	require.NoError(t, err)
	require.Empty(t, peers)
}

// ---------------------------------------------------------------------------
// AddBanScore accumulation
// ---------------------------------------------------------------------------

func TestRegistryClient_AddBanScore_Accumulates(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	// Three incremental additions.
	score, banned, err := client.AddBanScore(ctx, "score-peer", "first", 25)
	require.NoError(t, err)
	require.Equal(t, int32(25), score)
	require.False(t, banned)

	score, banned, err = client.AddBanScore(ctx, "score-peer", "second", 30)
	require.NoError(t, err)
	require.Equal(t, int32(55), score)
	require.False(t, banned)

	score, banned, err = client.AddBanScore(ctx, "score-peer", "third", 45)
	require.NoError(t, err)
	require.Equal(t, int32(100), score)
	require.True(t, banned)
}

// ---------------------------------------------------------------------------
// Context cancellation propagates correctly
// ---------------------------------------------------------------------------

func TestRegistryClient_ContextCancellation(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := client.RegisterPeer(ctx, &PeerInfo{ID: "cancelled"})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// ListPeers with MinReputation filter
// ---------------------------------------------------------------------------

func TestRegistryClient_ListPeers_MinReputation(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()

	// Register two peers — they start at reputation 50.
	err := client.RegisterPeer(ctx, &PeerInfo{ID: "normal-peer", Height: 100})
	require.NoError(t, err)

	// Register a peer and mark it malicious (reputation drops to 5).
	err = client.RegisterPeer(ctx, &PeerInfo{ID: "bad-peer", Height: 100})
	require.NoError(t, err)
	err = client.UpdatePeerMetrics(ctx, "bad-peer", 100, 0, 0, false, false, true, 0)
	require.NoError(t, err)

	// Filter for reputation >= 40 — should exclude the malicious peer.
	peers, err := client.ListPeers(ctx, nil, 40.0, 0, false, false)
	require.NoError(t, err)
	require.Len(t, peers, 1)
	require.Equal(t, "normal-peer", peers[0].ID)
}

// ---------------------------------------------------------------------------
// Timeout context
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Extended RPCs added in the move from p2p (UpdateConnectionState,
// UpdateLastMessageTime, UpdateStorage, RecordSyncAttempt, ClearAllSyncAttempts,
// RecordBlockReceived, RecordSubtreeReceived, RecordTransactionReceived,
// RecordCatchupError, ResetReputation, ReconsiderBadPeers).
// ---------------------------------------------------------------------------

func TestRegistryClient_UpdateConnectionState(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, client.RegisterPeer(ctx, &PeerInfo{ID: "p"}))

	require.NoError(t, client.UpdateConnectionState(ctx, "p", true))
	got, _, err := client.GetPeer(ctx, "p")
	require.NoError(t, err)
	require.True(t, got.IsConnected)

	require.NoError(t, client.UpdateConnectionState(ctx, "p", false))
	got, _, err = client.GetPeer(ctx, "p")
	require.NoError(t, err)
	require.False(t, got.IsConnected)
}

func TestRegistryClient_UpdateLastMessageTime(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, client.RegisterPeer(ctx, &PeerInfo{ID: "p"}))

	first, _, err := client.GetPeer(ctx, "p")
	require.NoError(t, err)

	time.Sleep(2 * time.Millisecond)
	require.NoError(t, client.UpdateLastMessageTime(ctx, "p"))

	second, _, err := client.GetPeer(ctx, "p")
	require.NoError(t, err)
	require.True(t, second.LastMessageTime.After(first.LastMessageTime))
}

func TestRegistryClient_UpdateStorage(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, client.RegisterPeer(ctx, &PeerInfo{ID: "p"}))

	require.NoError(t, client.UpdateStorage(ctx, "p", "full"))
	got, _, err := client.GetPeer(ctx, "p")
	require.NoError(t, err)
	require.Equal(t, "full", got.Storage)
}

func TestRegistryClient_RecordSyncAttemptAndClearAll(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, client.RegisterPeer(ctx, &PeerInfo{ID: "p"}))

	require.NoError(t, client.RecordSyncAttempt(ctx, "p"))
	require.NoError(t, client.RecordSyncAttempt(ctx, "p"))

	got, _, err := client.GetPeer(ctx, "p")
	require.NoError(t, err)
	require.Equal(t, int32(2), got.SyncAttemptCount)
	require.False(t, got.LastSyncAttempt.IsZero())

	cleared, err := client.ClearAllSyncAttempts(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(1), cleared)
}

func TestRegistryClient_RecordBlockSubtreeTransaction(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, client.RegisterPeer(ctx, &PeerInfo{ID: "p"}))

	require.NoError(t, client.RecordBlockReceived(ctx, "p", 250))
	require.NoError(t, client.RecordSubtreeReceived(ctx, "p", 0))
	require.NoError(t, client.RecordTransactionReceived(ctx, "p"))

	got, _, err := client.GetPeer(ctx, "p")
	require.NoError(t, err)
	require.Equal(t, int64(1), got.BlocksReceived)
	require.Equal(t, int64(1), got.SubtreesReceived)
	require.Equal(t, int64(1), got.TransactionsReceived)
	require.Equal(t, int64(3), got.InteractionSuccesses)
	require.Equal(t, int64(250), got.AvgResponseTimeMs)
}

func TestRegistryClient_RecordCatchupError(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, client.RegisterPeer(ctx, &PeerInfo{ID: "p"}))

	require.NoError(t, client.RecordCatchupError(ctx, "p", "block 0xdead missing"))

	got, _, err := client.GetPeer(ctx, "p")
	require.NoError(t, err)
	require.Equal(t, "block 0xdead missing", got.LastCatchupError)
	require.False(t, got.LastCatchupErrorTime.IsZero())
}

func TestRegistryClient_ResetReputationSingleAndAll(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, client.RegisterPeer(ctx, &PeerInfo{ID: "a"}))
	require.NoError(t, client.RegisterPeer(ctx, &PeerInfo{ID: "b"}))
	require.NoError(t, client.UpdatePeerMetrics(ctx, "a", 0, 0, 0, false, false, true, 0))

	count, err := client.ResetReputation(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, int32(1), count)

	count, err = client.ResetReputation(ctx, "")
	require.NoError(t, err)
	require.Equal(t, int32(2), count)
}

func TestRegistryClient_ReconsiderBadPeers(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, client.RegisterPeer(ctx, &PeerInfo{ID: "p"}))
	require.NoError(t, client.UpdatePeerMetrics(ctx, "p", 0, 0, 0, false, false, true, 0))

	// Peer reputation just dropped — recent failure, so the cooldown blocks recovery.
	count, err := client.ReconsiderBadPeers(ctx, time.Hour)
	require.NoError(t, err)
	require.Equal(t, int32(0), count)
}

func TestRegistryClient_ContextTimeout(t *testing.T) {
	client, cleanup := setupRegistryGRPC(t)
	defer cleanup()

	// Use a very generous timeout to verify the operation completes within it.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := client.RegisterPeer(ctx, &PeerInfo{ID: "timeout-peer"})
	require.NoError(t, err)

	got, found, err := client.GetPeer(ctx, "timeout-peer")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "timeout-peer", got.ID)
}
