package blockchain

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RegisterPeer adds or updates a peer in the centralized registry.
func (b *Blockchain) RegisterPeer(_ context.Context, req *blockchain_api.RegisterPeerRequest) (*emptypb.Empty, error) {
	if req.Peer == nil {
		return nil, status.Error(codes.InvalidArgument, "peer info is required")
	}
	info := protoToPeerInfo(b.logger, req.Peer)
	b.peerRegistry.Register(info)
	return &emptypb.Empty{}, nil
}

// UpdatePeerMetrics updates runtime counters for an existing peer without a full re-registration.
func (b *Blockchain) UpdatePeerMetrics(_ context.Context, req *blockchain_api.UpdatePeerMetricsRequest) (*emptypb.Empty, error) {
	b.peerRegistry.UpdateMetrics(
		req.PeerId,
		req.Height,
		req.BytesSentDelta,
		req.BytesReceivedDelta,
		req.RecordSuccess,
		req.RecordFailure,
		req.RecordMalicious,
		req.ResponseTimeMs,
	)
	return &emptypb.Empty{}, nil
}

// RemovePeer removes a peer from the registry on disconnect or explicit eviction.
func (b *Blockchain) RemovePeer(_ context.Context, req *blockchain_api.RemovePeerRequest) (*emptypb.Empty, error) {
	b.peerRegistry.Remove(req.PeerId)
	return &emptypb.Empty{}, nil
}

// ListPeers returns all peers matching the given filters, sorted by reputation descending.
func (b *Blockchain) ListPeers(_ context.Context, req *blockchain_api.ListPeersRequest) (*blockchain_api.ListPeersResponse, error) {
	// Only apply the transport filter when the caller has explicitly set one; a zero value
	// for a proto enum is indistinguishable from "not set" without the boolean sentinel.
	var tf *blockchain_api.TransportType
	if req.FilterTransport {
		t := req.TransportFilter
		tf = &t
	}

	infos := b.peerRegistry.List(tf, req.MinReputation, req.MinHeight, req.ExcludeBanned, req.SortByStorage)

	peers := make([]*blockchain_api.PeerRegistryInfo, 0, len(infos))
	for _, info := range infos {
		peers = append(peers, peerInfoToProto(info))
	}

	return &blockchain_api.ListPeersResponse{Peers: peers}, nil
}

// GetPeer retrieves a single peer by ID.
func (b *Blockchain) GetPeer(_ context.Context, req *blockchain_api.GetPeerRequest) (*blockchain_api.GetPeerResponse, error) {
	info, ok := b.peerRegistry.Get(req.PeerId)
	if !ok {
		return &blockchain_api.GetPeerResponse{NotFound: true}, nil
	}
	return &blockchain_api.GetPeerResponse{Peer: peerInfoToProto(info)}, nil
}

// AddBanScore adds penalty points to a peer's ban score.
func (b *Blockchain) AddBanScore(_ context.Context, req *blockchain_api.AddBanScoreRequest) (*blockchain_api.AddBanScoreResponse, error) {
	score, banned := b.peerRegistry.AddBanScore(req.PeerId, req.Reason, req.Points)
	return &blockchain_api.AddBanScoreResponse{Score: score, Banned: banned}, nil
}

// IsPeerBanned checks if a peer is currently banned.
func (b *Blockchain) IsPeerBanned(_ context.Context, req *blockchain_api.IsPeerBannedRequest) (*blockchain_api.IsPeerBannedResponse, error) {
	return &blockchain_api.IsPeerBannedResponse{Banned: b.peerRegistry.IsBannedPeer(req.PeerId)}, nil
}

// ListBannedPeers returns all currently banned peer IDs.
func (b *Blockchain) ListBannedPeers(_ context.Context, _ *emptypb.Empty) (*blockchain_api.ListBannedPeersResponse, error) {
	return &blockchain_api.ListBannedPeersResponse{PeerIds: b.peerRegistry.ListBannedPeers()}, nil
}

// ClearBannedPeers removes all bans.
func (b *Blockchain) ClearBannedPeers(_ context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	b.peerRegistry.ClearBannedPeers()
	return &emptypb.Empty{}, nil
}

// UpdateConnectionState flips the IsConnected flag on a peer entry.
func (b *Blockchain) UpdateConnectionState(_ context.Context, req *blockchain_api.UpdateConnectionStateRequest) (*emptypb.Empty, error) {
	b.peerRegistry.UpdateConnectionState(req.PeerId, req.Connected)
	return &emptypb.Empty{}, nil
}

// UpdateLastMessageTime sets a peer's LastMessageTime to now.
func (b *Blockchain) UpdateLastMessageTime(_ context.Context, req *blockchain_api.UpdateLastMessageTimeRequest) (*emptypb.Empty, error) {
	b.peerRegistry.UpdateLastMessageTime(req.PeerId)
	return &emptypb.Empty{}, nil
}

// UpdateStorage sets the peer's storage mode.
func (b *Blockchain) UpdateStorage(_ context.Context, req *blockchain_api.UpdateStorageRequest) (*emptypb.Empty, error) {
	b.peerRegistry.UpdateStorage(req.PeerId, req.Storage)
	return &emptypb.Empty{}, nil
}

// RecordSyncAttempt records a sync attempt against a peer.
func (b *Blockchain) RecordSyncAttempt(_ context.Context, req *blockchain_api.RecordSyncAttemptRequest) (*emptypb.Empty, error) {
	b.peerRegistry.RecordSyncAttempt(req.PeerId)
	return &emptypb.Empty{}, nil
}

// ClearAllSyncAttempts resets sync attempt counters across the registry.
func (b *Blockchain) ClearAllSyncAttempts(_ context.Context, _ *emptypb.Empty) (*blockchain_api.ClearAllSyncAttemptsResponse, error) {
	cleared := b.peerRegistry.ClearAllSyncAttempts()
	return &blockchain_api.ClearAllSyncAttemptsResponse{Cleared: int32(cleared)}, nil
}

// RecordBlockReceived increments the BlocksReceived counter and records a successful interaction.
func (b *Blockchain) RecordBlockReceived(_ context.Context, req *blockchain_api.RecordReceivedRequest) (*emptypb.Empty, error) {
	b.peerRegistry.RecordBlockReceived(req.PeerId, req.ResponseTimeMs)
	return &emptypb.Empty{}, nil
}

// RecordSubtreeReceived increments the SubtreesReceived counter and records a successful interaction.
func (b *Blockchain) RecordSubtreeReceived(_ context.Context, req *blockchain_api.RecordReceivedRequest) (*emptypb.Empty, error) {
	b.peerRegistry.RecordSubtreeReceived(req.PeerId, req.ResponseTimeMs)
	return &emptypb.Empty{}, nil
}

// RecordTransactionReceived increments the TransactionsReceived counter.
func (b *Blockchain) RecordTransactionReceived(_ context.Context, req *blockchain_api.RecordReceivedRequest) (*emptypb.Empty, error) {
	b.peerRegistry.RecordTransactionReceived(req.PeerId)
	return &emptypb.Empty{}, nil
}

// RecordCatchupError stores the peer's most recent catchup error.
func (b *Blockchain) RecordCatchupError(_ context.Context, req *blockchain_api.RecordCatchupErrorRequest) (*emptypb.Empty, error) {
	b.peerRegistry.RecordCatchupError(req.PeerId, req.ErrorMessage)
	return &emptypb.Empty{}, nil
}

// ResetReputation resets reputation for a peer (or all peers when peer_id is empty).
func (b *Blockchain) ResetReputation(_ context.Context, req *blockchain_api.ResetReputationRequest) (*blockchain_api.ResetReputationResponse, error) {
	reset := b.peerRegistry.ResetReputation(req.PeerId)
	return &blockchain_api.ResetReputationResponse{Reset_: int32(reset)}, nil
}

// ReconsiderBadPeers resets reputation for peers whose last failure is older than cooldown_seconds.
func (b *Blockchain) ReconsiderBadPeers(_ context.Context, req *blockchain_api.ReconsiderBadPeersRequest) (*blockchain_api.ReconsiderBadPeersResponse, error) {
	cooldown := time.Duration(req.CooldownSeconds) * time.Second
	count := b.peerRegistry.ReconsiderBadPeers(cooldown)
	return &blockchain_api.ReconsiderBadPeersResponse{Reconsidered: int32(count)}, nil
}

// peerInfoToProto converts the domain PeerInfo type to its protobuf representation.
func peerInfoToProto(info *PeerInfo) *blockchain_api.PeerRegistryInfo {
	return &blockchain_api.PeerRegistryInfo{
		PeerId:                 info.ID,
		TransportType:          info.TransportType,
		ClientName:             info.ClientName,
		Height:                 info.Height,
		DataHubUrl:             info.DataHubURL,
		NetworkAddress:         info.NetworkAddress,
		IsBanned:               info.IsBanned,
		BanScore:               info.BanScore,
		Storage:                info.Storage,
		BytesSent:              info.BytesSent,
		BytesReceived:          info.BytesReceived,
		InteractionAttempts:    info.InteractionAttempts,
		InteractionSuccesses:   info.InteractionSuccesses,
		InteractionFailures:    info.InteractionFailures,
		MaliciousCount:         info.MaliciousCount,
		ReputationScore:        info.ReputationScore,
		AvgResponseTimeMs:      info.AvgResponseTimeMs,
		ConnectedAt:            timestamppb.New(info.ConnectedAt),
		LastMessageTime:        timestamppb.New(info.LastMessageTime),
		LastInteractionAttempt: timestamppb.New(info.LastInteractionAttempt),
		LastInteractionSuccess: timestamppb.New(info.LastInteractionSuccess),
		LastInteractionFailure: timestamppb.New(info.LastInteractionFailure),
		LastSeen:               timestamppb.New(info.LastSeen),
		BlockHash:              blockHashToBytes(info.BlockHash),
		IsConnected:            info.IsConnected,
		LastBlockTime:          timestamppb.New(info.LastBlockTime),
		BlocksReceived:         info.BlocksReceived,
		SubtreesReceived:       info.SubtreesReceived,
		TransactionsReceived:   info.TransactionsReceived,
		CatchupBlocks:          info.CatchupBlocks,
		LastSyncAttempt:        timestamppb.New(info.LastSyncAttempt),
		SyncAttemptCount:       info.SyncAttemptCount,
		LastReputationReset:    timestamppb.New(info.LastReputationReset),
		ReputationResetCount:   info.ReputationResetCount,
		LastCatchupError:       info.LastCatchupError,
		LastCatchupErrorTime:   timestamppb.New(info.LastCatchupErrorTime),
	}
}

// protoToPeerInfo converts a protobuf PeerRegistryInfo to the domain PeerInfo type.
// TransportTypeSet is set to true unconditionally: the wire format always
// carries a transport_type value, so a deserialised PeerInfo has been
// "explicitly set" by virtue of crossing the registry boundary at all. This
// matches the semantics callers want — a peer registered via gRPC keeps its
// transport type sticky across subsequent updates that omit the field.
//
// An invalid (non-empty but unparseable) BlockHash is logged via the supplied
// logger and the resulting PeerInfo carries a nil BlockHash; this is
// non-fatal because the rest of the entry is salvageable. A nil logger is
// tolerated (rare; only some tests).
func protoToPeerInfo(logger ulogger.Logger, p *blockchain_api.PeerRegistryInfo) *PeerInfo {
	hash, err := bytesToBlockHash(p.BlockHash)
	if err != nil && logger != nil {
		logger.Errorf("peer registry: dropping invalid BlockHash for peer %s: %v", p.PeerId, err)
	}
	return &PeerInfo{
		ID:                     p.PeerId,
		TransportType:          p.TransportType,
		TransportTypeSet:       true,
		ClientName:             p.ClientName,
		Height:                 p.Height,
		DataHubURL:             p.DataHubUrl,
		NetworkAddress:         p.NetworkAddress,
		IsBanned:               p.IsBanned,
		BanScore:               p.BanScore,
		Storage:                p.Storage,
		BytesSent:              p.BytesSent,
		BytesReceived:          p.BytesReceived,
		InteractionAttempts:    p.InteractionAttempts,
		InteractionSuccesses:   p.InteractionSuccesses,
		InteractionFailures:    p.InteractionFailures,
		MaliciousCount:         p.MaliciousCount,
		ReputationScore:        p.ReputationScore,
		AvgResponseTimeMs:      p.AvgResponseTimeMs,
		ConnectedAt:            protoTimeToTime(p.ConnectedAt),
		LastMessageTime:        protoTimeToTime(p.LastMessageTime),
		LastInteractionAttempt: protoTimeToTime(p.LastInteractionAttempt),
		LastInteractionSuccess: protoTimeToTime(p.LastInteractionSuccess),
		LastInteractionFailure: protoTimeToTime(p.LastInteractionFailure),
		LastSeen:               protoTimeToTime(p.LastSeen),
		BlockHash:              hash,
		IsConnected:            p.IsConnected,
		LastBlockTime:          protoTimeToTime(p.LastBlockTime),
		BlocksReceived:         p.BlocksReceived,
		SubtreesReceived:       p.SubtreesReceived,
		TransactionsReceived:   p.TransactionsReceived,
		CatchupBlocks:          p.CatchupBlocks,
		LastSyncAttempt:        protoTimeToTime(p.LastSyncAttempt),
		SyncAttemptCount:       p.SyncAttemptCount,
		LastReputationReset:    protoTimeToTime(p.LastReputationReset),
		ReputationResetCount:   p.ReputationResetCount,
		LastCatchupError:       p.LastCatchupError,
		LastCatchupErrorTime:   protoTimeToTime(p.LastCatchupErrorTime),
	}
}

// protoTimeToTime converts a nullable proto timestamp to time.Time, returning the zero value when nil.
func protoTimeToTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// blockHashToBytes converts a *chainhash.Hash to a byte slice, returning nil if hash is nil.
// Returns a copy to avoid aliasing the caller's backing array.
func blockHashToBytes(h *chainhash.Hash) []byte {
	if h == nil {
		return nil
	}
	return append([]byte(nil), h[:]...)
}

// bytesToBlockHash converts a byte slice to *chainhash.Hash. An empty slice
// returns (nil, nil) — peers that have not advertised a tip yet legitimately
// arrive with no hash. A non-empty but invalid slice returns
// (nil, ProcessingError); the caller is expected to log via its structured
// logger and continue (the bad hash is recoverable; the rest of the PeerInfo
// is salvageable).
func bytesToBlockHash(b []byte) (*chainhash.Hash, error) {
	if len(b) == 0 {
		return nil, nil
	}
	h, err := chainhash.NewHash(b)
	if err != nil {
		return nil, errors.NewProcessingError(fmt.Sprintf("invalid block hash bytes (len=%d)", len(b)), err)
	}
	return h, nil
}
