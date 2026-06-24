package blockchain

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// PeerRegistryClientI is the client interface for the centralized peer registry.
// Services that register or query peers use this instead of the full ClientI,
// keeping the dependency surface minimal.
type PeerRegistryClientI interface {
	// RegisterPeer adds or updates a peer in the centralized registry.
	RegisterPeer(ctx context.Context, info *PeerInfo) error

	// UpdatePeerMetrics reports metrics changes for an existing peer.
	UpdatePeerMetrics(ctx context.Context, peerID string, height uint32, bytesSentDelta, bytesRecvDelta uint64, recordSuccess, recordFailure, recordMalicious bool, responseTimeMs int64) error

	// RemovePeer removes a peer from the centralized registry.
	RemovePeer(ctx context.Context, peerID string) error

	// GetPeer retrieves a single peer by ID. Returns nil and false if not found.
	GetPeer(ctx context.Context, peerID string) (*PeerInfo, bool, error)

	// ListPeers returns peers matching the given filters, sorted by reputation.
	// When sortByStorage is true, peers are sorted primarily by storage mode
	// (full > pruned > unknown) before reputation.
	ListPeers(ctx context.Context, transportFilter *blockchain_api.TransportType, minReputation float64, minHeight uint32, excludeBanned, sortByStorage bool) ([]*PeerInfo, error)

	// AddBanScore adds penalty points to a peer and returns updated score and ban status.
	AddBanScore(ctx context.Context, peerID string, reason string, defaultPoints int32) (int32, bool, error)

	// IsPeerBanned checks if a peer is currently banned.
	IsPeerBanned(ctx context.Context, peerID string) (bool, error)

	// ListBannedPeers returns all currently banned peer IDs.
	ListBannedPeers(ctx context.Context) ([]string, error)

	// ClearBannedPeers removes all bans.
	ClearBannedPeers(ctx context.Context) error

	// UpdateConnectionState flips the IsConnected flag on a peer entry.
	UpdateConnectionState(ctx context.Context, peerID string, connected bool) error

	// UpdateLastMessageTime sets the peer's last message time to now.
	UpdateLastMessageTime(ctx context.Context, peerID string) error

	// UpdateStorage sets the peer's storage mode (full, pruned, etc.).
	UpdateStorage(ctx context.Context, peerID, storage string) error

	// RecordSyncAttempt records a sync attempt against the peer for backoff tracking.
	RecordSyncAttempt(ctx context.Context, peerID string) error

	// ClearAllSyncAttempts resets sync attempt counters for all peers and returns the cleared count.
	ClearAllSyncAttempts(ctx context.Context) (int32, error)

	// RecordBlockReceived increments BlocksReceived, sets LastBlockTime, and
	// records a successful interaction with the given response time.
	RecordBlockReceived(ctx context.Context, peerID string, responseTimeMs int64) error

	// RecordSubtreeReceived increments SubtreesReceived and records a successful interaction.
	RecordSubtreeReceived(ctx context.Context, peerID string, responseTimeMs int64) error

	// RecordTransactionReceived increments TransactionsReceived.
	RecordTransactionReceived(ctx context.Context, peerID string) error

	// RecordCatchupError stores the peer's most recent catchup error.
	RecordCatchupError(ctx context.Context, peerID, errMsg string) error

	// ResetReputation resets reputation for a peer (or all peers when peerID is empty).
	// Returns the count of peers reset.
	ResetReputation(ctx context.Context, peerID string) (int32, error)

	// ReconsiderBadPeers resets reputation for peers whose last failure is older than cooldown.
	// Returns the count of peers reconsidered.
	ReconsiderBadPeers(ctx context.Context, cooldown time.Duration) (int32, error)

	// Close releases any resources held by the client.
	// For clients created with NewPeerRegistryClientFromConn, Close is a no-op
	// because the caller owns the underlying connection.
	Close() error
}

// PeerRegistryClient is the gRPC-backed implementation of PeerRegistryClientI.
type PeerRegistryClient struct {
	client   blockchain_api.PeerRegistryServiceClient
	conn     *grpc.ClientConn
	ownsConn bool // true when this client created the connection and is responsible for closing it
	// logger is used for non-fatal diagnostics during proto decoding (e.g.
	// invalid BlockHash bytes in a peer message). Optional; nil fallback is
	// handled in log(). Stored as atomic.Value because log() runs on every
	// GetPeer / ListPeers RPC and SetLogger may be called after construction
	// — matches the CentralizedPeerRegistry hardening pattern.
	logger atomic.Value // loggerHolder
}

// SetLogger installs a logger for non-fatal proto-decoding diagnostics.
// Idempotent and safe to call concurrently with log(). Passing nil is a
// no-op (the default fallback applies).
func (c *PeerRegistryClient) SetLogger(l ulogger.Logger) {
	if l == nil {
		return
	}
	c.logger.Store(loggerHolder{l})
}

func (c *PeerRegistryClient) log() ulogger.Logger {
	if v := c.logger.Load(); v != nil {
		return v.(loggerHolder).l
	}
	return ulogger.New("PeerRegistryClient")
}

// NewPeerRegistryClient connects to the blockchain service and returns a PeerRegistryClientI.
// It reuses the same address as the blockchain service since PeerRegistryService is served on the same port.
func NewPeerRegistryClient(ctx context.Context, address string, tSettings *settings.Settings) (PeerRegistryClientI, error) {
	conn, err := util.GetGRPCClient(ctx, address, &util.ConnectionOptions{}, tSettings)
	if err != nil {
		return nil, err
	}

	return &PeerRegistryClient{
		client:   blockchain_api.NewPeerRegistryServiceClient(conn),
		conn:     conn,
		ownsConn: true,
	}, nil
}

// RegisterPeer implements PeerRegistryClientI.
func (c *PeerRegistryClient) RegisterPeer(ctx context.Context, info *PeerInfo) error {
	_, err := c.client.RegisterPeer(ctx, &blockchain_api.RegisterPeerRequest{
		Peer: peerInfoToProto(info),
	})
	return err
}

// UpdatePeerMetrics implements PeerRegistryClientI.
func (c *PeerRegistryClient) UpdatePeerMetrics(ctx context.Context, peerID string, height uint32, bytesSentDelta, bytesRecvDelta uint64, recordSuccess, recordFailure, recordMalicious bool, responseTimeMs int64) error {
	_, err := c.client.UpdatePeerMetrics(ctx, &blockchain_api.UpdatePeerMetricsRequest{
		PeerId:             peerID,
		Height:             height,
		BytesSentDelta:     bytesSentDelta,
		BytesReceivedDelta: bytesRecvDelta,
		RecordSuccess:      recordSuccess,
		RecordFailure:      recordFailure,
		RecordMalicious:    recordMalicious,
		ResponseTimeMs:     responseTimeMs,
	})
	return err
}

// RemovePeer implements PeerRegistryClientI.
func (c *PeerRegistryClient) RemovePeer(ctx context.Context, peerID string) error {
	_, err := c.client.RemovePeer(ctx, &blockchain_api.RemovePeerRequest{PeerId: peerID})
	return err
}

// GetPeer implements PeerRegistryClientI.
func (c *PeerRegistryClient) GetPeer(ctx context.Context, peerID string) (*PeerInfo, bool, error) {
	resp, err := c.client.GetPeer(ctx, &blockchain_api.GetPeerRequest{PeerId: peerID})
	if err != nil {
		return nil, false, err
	}
	if resp.NotFound {
		return nil, false, nil
	}
	return protoToPeerInfo(c.log(), resp.Peer), true, nil
}

// ListPeers implements PeerRegistryClientI.
func (c *PeerRegistryClient) ListPeers(ctx context.Context, transportFilter *blockchain_api.TransportType, minReputation float64, minHeight uint32, excludeBanned, sortByStorage bool) ([]*PeerInfo, error) {
	req := &blockchain_api.ListPeersRequest{
		MinReputation: minReputation,
		MinHeight:     minHeight,
		ExcludeBanned: excludeBanned,
		SortByStorage: sortByStorage,
	}
	if transportFilter != nil {
		req.FilterTransport = true
		req.TransportFilter = *transportFilter
	}

	resp, err := c.client.ListPeers(ctx, req)
	if err != nil {
		return nil, err
	}

	peers := make([]*PeerInfo, 0, len(resp.Peers))
	for _, p := range resp.Peers {
		peers = append(peers, protoToPeerInfo(c.log(), p))
	}
	return peers, nil
}

// AddBanScore implements PeerRegistryClientI.
func (c *PeerRegistryClient) AddBanScore(ctx context.Context, peerID string, reason string, defaultPoints int32) (int32, bool, error) {
	resp, err := c.client.AddBanScore(ctx, &blockchain_api.AddBanScoreRequest{
		PeerId: peerID,
		Reason: reason,
		Points: defaultPoints,
	})
	if err != nil {
		return 0, false, err
	}
	return resp.Score, resp.Banned, nil
}

// IsPeerBanned implements PeerRegistryClientI.
func (c *PeerRegistryClient) IsPeerBanned(ctx context.Context, peerID string) (bool, error) {
	resp, err := c.client.IsPeerBanned(ctx, &blockchain_api.IsPeerBannedRequest{PeerId: peerID})
	if err != nil {
		return false, err
	}
	return resp.Banned, nil
}

// ListBannedPeers implements PeerRegistryClientI.
func (c *PeerRegistryClient) ListBannedPeers(ctx context.Context) ([]string, error) {
	resp, err := c.client.ListBannedPeers(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	return resp.PeerIds, nil
}

// ClearBannedPeers implements PeerRegistryClientI.
func (c *PeerRegistryClient) ClearBannedPeers(ctx context.Context) error {
	_, err := c.client.ClearBannedPeers(ctx, &emptypb.Empty{})
	return err
}

// UpdateConnectionState implements PeerRegistryClientI.
func (c *PeerRegistryClient) UpdateConnectionState(ctx context.Context, peerID string, connected bool) error {
	_, err := c.client.UpdateConnectionState(ctx, &blockchain_api.UpdateConnectionStateRequest{
		PeerId:    peerID,
		Connected: connected,
	})
	return err
}

// UpdateLastMessageTime implements PeerRegistryClientI.
func (c *PeerRegistryClient) UpdateLastMessageTime(ctx context.Context, peerID string) error {
	_, err := c.client.UpdateLastMessageTime(ctx, &blockchain_api.UpdateLastMessageTimeRequest{PeerId: peerID})
	return err
}

// UpdateStorage implements PeerRegistryClientI.
func (c *PeerRegistryClient) UpdateStorage(ctx context.Context, peerID, storage string) error {
	_, err := c.client.UpdateStorage(ctx, &blockchain_api.UpdateStorageRequest{
		PeerId:  peerID,
		Storage: storage,
	})
	return err
}

// RecordSyncAttempt implements PeerRegistryClientI.
func (c *PeerRegistryClient) RecordSyncAttempt(ctx context.Context, peerID string) error {
	_, err := c.client.RecordSyncAttempt(ctx, &blockchain_api.RecordSyncAttemptRequest{PeerId: peerID})
	return err
}

// ClearAllSyncAttempts implements PeerRegistryClientI.
func (c *PeerRegistryClient) ClearAllSyncAttempts(ctx context.Context) (int32, error) {
	resp, err := c.client.ClearAllSyncAttempts(ctx, &emptypb.Empty{})
	if err != nil {
		return 0, err
	}
	return resp.Cleared, nil
}

// RecordBlockReceived implements PeerRegistryClientI.
func (c *PeerRegistryClient) RecordBlockReceived(ctx context.Context, peerID string, responseTimeMs int64) error {
	_, err := c.client.RecordBlockReceived(ctx, &blockchain_api.RecordReceivedRequest{
		PeerId:         peerID,
		ResponseTimeMs: responseTimeMs,
	})
	return err
}

// RecordSubtreeReceived implements PeerRegistryClientI.
func (c *PeerRegistryClient) RecordSubtreeReceived(ctx context.Context, peerID string, responseTimeMs int64) error {
	_, err := c.client.RecordSubtreeReceived(ctx, &blockchain_api.RecordReceivedRequest{
		PeerId:         peerID,
		ResponseTimeMs: responseTimeMs,
	})
	return err
}

// RecordTransactionReceived implements PeerRegistryClientI.
func (c *PeerRegistryClient) RecordTransactionReceived(ctx context.Context, peerID string) error {
	_, err := c.client.RecordTransactionReceived(ctx, &blockchain_api.RecordReceivedRequest{PeerId: peerID})
	return err
}

// RecordCatchupError implements PeerRegistryClientI.
func (c *PeerRegistryClient) RecordCatchupError(ctx context.Context, peerID, errMsg string) error {
	_, err := c.client.RecordCatchupError(ctx, &blockchain_api.RecordCatchupErrorRequest{
		PeerId:       peerID,
		ErrorMessage: errMsg,
	})
	return err
}

// ResetReputation implements PeerRegistryClientI.
func (c *PeerRegistryClient) ResetReputation(ctx context.Context, peerID string) (int32, error) {
	resp, err := c.client.ResetReputation(ctx, &blockchain_api.ResetReputationRequest{PeerId: peerID})
	if err != nil {
		return 0, err
	}
	return resp.Reset_, nil
}

// ReconsiderBadPeers implements PeerRegistryClientI.
func (c *PeerRegistryClient) ReconsiderBadPeers(ctx context.Context, cooldown time.Duration) (int32, error) {
	resp, err := c.client.ReconsiderBadPeers(ctx, &blockchain_api.ReconsiderBadPeersRequest{
		CooldownSeconds: int64(cooldown / time.Second),
	})
	if err != nil {
		return 0, err
	}
	return resp.Reconsidered, nil
}

// Close releases the underlying gRPC connection if this client owns it.
// Clients created via NewPeerRegistryClientFromConn do not own the connection
// and Close is a no-op for them.
func (c *PeerRegistryClient) Close() error {
	if !c.ownsConn || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// NewPeerRegistryClientFromConn creates a PeerRegistryClient using an existing gRPC connection.
// The caller retains ownership of the connection — Close() on this client is a no-op.
func NewPeerRegistryClientFromConn(conn *grpc.ClientConn) PeerRegistryClientI {
	return &PeerRegistryClient{
		client:   blockchain_api.NewPeerRegistryServiceClient(conn),
		conn:     conn,
		ownsConn: false,
	}
}

// NewLocalPeerRegistryClient returns a PeerRegistryClientI backed by an
// in-process *CentralizedPeerRegistry. Methods bypass gRPC entirely and call
// the registry directly. Intended for unit tests in dependent packages and
// as a fallback when running blockchain in-process is acceptable.
func NewLocalPeerRegistryClient(reg *CentralizedPeerRegistry) PeerRegistryClientI {
	return &localPeerRegistryClient{reg: reg}
}

type localPeerRegistryClient struct {
	reg *CentralizedPeerRegistry
}

func (l *localPeerRegistryClient) RegisterPeer(_ context.Context, info *PeerInfo) error {
	l.reg.Register(info)
	return nil
}

func (l *localPeerRegistryClient) UpdatePeerMetrics(_ context.Context, peerID string, height uint32, bytesSentDelta, bytesRecvDelta uint64, recordSuccess, recordFailure, recordMalicious bool, responseTimeMs int64) error {
	l.reg.UpdateMetrics(peerID, height, bytesSentDelta, bytesRecvDelta, recordSuccess, recordFailure, recordMalicious, responseTimeMs)
	return nil
}

func (l *localPeerRegistryClient) RemovePeer(_ context.Context, peerID string) error {
	l.reg.Remove(peerID)
	return nil
}

func (l *localPeerRegistryClient) GetPeer(_ context.Context, peerID string) (*PeerInfo, bool, error) {
	info, ok := l.reg.Get(peerID)
	return info, ok, nil
}

func (l *localPeerRegistryClient) ListPeers(_ context.Context, transportFilter *blockchain_api.TransportType, minReputation float64, minHeight uint32, excludeBanned, sortByStorage bool) ([]*PeerInfo, error) {
	return l.reg.List(transportFilter, minReputation, minHeight, excludeBanned, sortByStorage), nil
}

func (l *localPeerRegistryClient) AddBanScore(_ context.Context, peerID, reason string, defaultPoints int32) (int32, bool, error) {
	score, banned := l.reg.AddBanScore(peerID, reason, defaultPoints)
	return score, banned, nil
}

func (l *localPeerRegistryClient) IsPeerBanned(_ context.Context, peerID string) (bool, error) {
	return l.reg.IsBannedPeer(peerID), nil
}

func (l *localPeerRegistryClient) ListBannedPeers(_ context.Context) ([]string, error) {
	return l.reg.ListBannedPeers(), nil
}

func (l *localPeerRegistryClient) ClearBannedPeers(_ context.Context) error {
	l.reg.ClearBannedPeers()
	return nil
}

func (l *localPeerRegistryClient) UpdateConnectionState(_ context.Context, peerID string, connected bool) error {
	l.reg.UpdateConnectionState(peerID, connected)
	return nil
}

func (l *localPeerRegistryClient) UpdateLastMessageTime(_ context.Context, peerID string) error {
	l.reg.UpdateLastMessageTime(peerID)
	return nil
}

func (l *localPeerRegistryClient) UpdateStorage(_ context.Context, peerID, storage string) error {
	l.reg.UpdateStorage(peerID, storage)
	return nil
}

func (l *localPeerRegistryClient) RecordSyncAttempt(_ context.Context, peerID string) error {
	l.reg.RecordSyncAttempt(peerID)
	return nil
}

func (l *localPeerRegistryClient) ClearAllSyncAttempts(_ context.Context) (int32, error) {
	return int32(l.reg.ClearAllSyncAttempts()), nil
}

func (l *localPeerRegistryClient) RecordBlockReceived(_ context.Context, peerID string, responseTimeMs int64) error {
	l.reg.RecordBlockReceived(peerID, responseTimeMs)
	return nil
}

func (l *localPeerRegistryClient) RecordSubtreeReceived(_ context.Context, peerID string, responseTimeMs int64) error {
	l.reg.RecordSubtreeReceived(peerID, responseTimeMs)
	return nil
}

func (l *localPeerRegistryClient) RecordTransactionReceived(_ context.Context, peerID string) error {
	l.reg.RecordTransactionReceived(peerID)
	return nil
}

func (l *localPeerRegistryClient) RecordCatchupError(_ context.Context, peerID, errMsg string) error {
	l.reg.RecordCatchupError(peerID, errMsg)
	return nil
}

func (l *localPeerRegistryClient) ResetReputation(_ context.Context, peerID string) (int32, error) {
	return int32(l.reg.ResetReputation(peerID)), nil
}

func (l *localPeerRegistryClient) ReconsiderBadPeers(_ context.Context, cooldown time.Duration) (int32, error) {
	return int32(l.reg.ReconsiderBadPeers(cooldown)), nil
}

func (l *localPeerRegistryClient) Close() error { return nil }
