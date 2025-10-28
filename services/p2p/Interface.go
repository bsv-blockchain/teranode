// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"

	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ClientI defines the interface for P2P client operations.
// This interface abstracts the communication with the P2P service, providing methods
// for querying peer information and managing peer bans. It serves as a contract for
// client implementations, whether they use gRPC, HTTP, or in-process communication.
//
// The interface methods correspond to operations exposed by the P2P service API
// and typically map directly to RPC endpoints. All methods accept a context for
// cancellation and timeout control.
type ClientI interface {
	// GetPeers retrieves a list of connected peers from the P2P network.
	// It provides information about all active peer connections including their
	// addresses, connection details, and network statistics.
	//
	// Parameters:
	// - ctx: Context for the operation, allowing for cancellation and timeouts
	//
	// Returns a GetPeersResponse containing peer information or an error if the operation fails.
	GetPeers(ctx context.Context) (*p2p_api.GetPeersResponse, error)

	// BanPeer adds a peer to the ban list to prevent future connections.
	// It can ban by peer ID, IP address, or subnet depending on the request parameters.
	//
	// Parameters:
	// - ctx: Context for the operation
	// - peer: Details about the peer to ban, including ban duration
	//
	// Returns confirmation of the ban operation or an error if it fails.
	BanPeer(ctx context.Context, peer *p2p_api.BanPeerRequest) (*p2p_api.BanPeerResponse, error)

	// UnbanPeer removes a peer from the ban list, allowing future connections.
	// It operates on peer ID, IP address, or subnet as specified in the request.
	//
	// Parameters:
	// - ctx: Context for the operation
	// - peer: Details about the peer to unban
	//
	// Returns confirmation of the unban operation or an error if it fails.
	UnbanPeer(ctx context.Context, peer *p2p_api.UnbanPeerRequest) (*p2p_api.UnbanPeerResponse, error)

	// IsBanned checks if a specific peer is currently banned.
	// This can be used to verify ban status before attempting connection.
	//
	// Parameters:
	// - ctx: Context for the operation
	// - peer: Details about the peer to check
	//
	// Returns ban status information or an error if the check fails.
	IsBanned(ctx context.Context, peer *p2p_api.IsBannedRequest) (*p2p_api.IsBannedResponse, error)

	// ListBanned returns all currently banned peers.
	// This provides a comprehensive view of all active bans in the system.
	//
	// Parameters:
	// - ctx: Context for the operation
	// - _: Empty placeholder parameter (not used)
	//
	// Returns a list of all banned peers or an error if the operation fails.
	ListBanned(ctx context.Context, _ *emptypb.Empty) (*p2p_api.ListBannedResponse, error)

	// ClearBanned removes all peer bans from the system.
	// This effectively resets the ban list to empty, allowing all peers to connect.
	//
	// Parameters:
	// - ctx: Context for the operation
	// - _: Empty placeholder parameter (not used)
	//
	// Returns confirmation of the clear operation or an error if it fails.
	ClearBanned(ctx context.Context, _ *emptypb.Empty) (*p2p_api.ClearBannedResponse, error)
	// AddBanScore adds to a peer's ban score with the specified reason.
	// Returns an AddBanScoreResponse indicating success or an error if the operation fails.
	AddBanScore(ctx context.Context, req *p2p_api.AddBanScoreRequest) (*p2p_api.AddBanScoreResponse, error)

	// ConnectPeer connects to a specific peer using the provided multiaddr
	// Returns an error if the connection fails.
	ConnectPeer(ctx context.Context, peerAddr string) error

	// DisconnectPeer disconnects from a specific peer using their peer ID
	// Returns an error if the disconnection fails.
	DisconnectPeer(ctx context.Context, peerID string) error

	// RecordCatchupAttempt records that a catchup attempt was made to a peer.
	// This is used by BlockValidation to track peer reliability during catchup operations.
	RecordCatchupAttempt(ctx context.Context, peerID string) error

	// RecordCatchupSuccess records a successful catchup from a peer.
	// The duration parameter indicates how long the catchup operation took.
	RecordCatchupSuccess(ctx context.Context, peerID string, durationMs int64) error

	// RecordCatchupFailure records a failed catchup attempt from a peer.
	RecordCatchupFailure(ctx context.Context, peerID string) error

	// RecordCatchupMalicious records malicious behavior detected during catchup.
	RecordCatchupMalicious(ctx context.Context, peerID string) error

	// UpdateCatchupReputation updates the reputation score for a peer.
	// Score should be between 0 and 100.
	UpdateCatchupReputation(ctx context.Context, peerID string, score float64) error

	// GetPeersForCatchup returns peers suitable for catchup operations.
	// Returns peers sorted by reputation (highest first).
	GetPeersForCatchup(ctx context.Context) (*p2p_api.GetPeersForCatchupResponse, error)

	// ReportValidSubtree reports that a subtree was successfully fetched and validated from a peer.
	// This increases the peer's reputation score for providing valid data.
	ReportValidSubtree(ctx context.Context, peerID string, subtreeHash string) error

	// ReportValidBlock reports that a block was successfully received and validated from a peer.
	// This increases the peer's reputation score for providing valid blocks.
	ReportValidBlock(ctx context.Context, peerID string, blockHash string) error

	// IsPeerMalicious checks if a peer is considered malicious based on their behavior.
	// A peer is considered malicious if they are banned or have a very low reputation score.
	IsPeerMalicious(ctx context.Context, peerID string) (bool, string, error)

	// IsPeerUnhealthy checks if a peer is considered unhealthy based on their performance.
	// A peer is considered unhealthy if they have poor performance metrics or low reputation.
	IsPeerUnhealthy(ctx context.Context, peerID string) (bool, string, float32, error)
}
