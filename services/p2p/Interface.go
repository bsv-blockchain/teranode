// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"

	"github.com/bitcoin-sv/teranode/services/p2p/p2p_api"
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
}
