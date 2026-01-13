// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerInfo holds all information about a peer in the P2P network.
// This struct represents the public API contract for peer data, decoupled from
// any transport-specific representations (like protobuf).
type PeerInfo struct {
	ID              peer.ID
	ClientName      string // Human-readable name of the client software
	Height          int32
	BlockHash       string
	DataHubURL      string
	BanScore        int
	IsBanned        bool
	IsConnected     bool // Whether this peer is directly connected (vs gossiped)
	ConnectedAt     time.Time
	BytesReceived   uint64
	LastBlockTime   time.Time
	LastMessageTime time.Time // Last time we received any message from this peer
	URLResponsive   bool      // Whether the DataHub URL is responsive
	LastURLCheck    time.Time // Last time we checked URL responsiveness
	Storage         string    // Storage mode: "full", "pruned", or empty (unknown/old version)

	// Interaction metrics - track peer reliability across all interactions (blocks, subtrees, catchup, etc.)
	InteractionAttempts    int64         // Total number of interactions with this peer
	InteractionSuccesses   int64         // Number of successful interactions
	InteractionFailures    int64         // Number of failed interactions
	LastInteractionAttempt time.Time     // Last time we interacted with this peer
	LastInteractionSuccess time.Time     // Last successful interaction
	LastInteractionFailure time.Time     // Last failed interaction
	ReputationScore        float64       // Reputation score (0-100) for overall reliability
	MaliciousCount         int64         // Count of malicious behavior detections
	AvgResponseTime        time.Duration // Average response time for all interactions

	// Interaction type breakdown (optional tracking)
	BlocksReceived       int64 // Number of blocks received from this peer
	SubtreesReceived     int64 // Number of subtrees received from this peer
	TransactionsReceived int64 // Number of transactions received from this peer
	CatchupBlocks        int64 // Number of blocks received during catchup

	// Sync attempt tracking for backoff and recovery
	LastSyncAttempt      time.Time // When we last attempted to sync with this peer
	SyncAttemptCount     int       // Number of sync attempts with this peer
	LastReputationReset  time.Time // When reputation was last reset for recovery
	ReputationResetCount int       // How many times reputation has been reset (for exponential cooldown)

	// Catchup error tracking
	LastCatchupError     string    // Last error message from catchup attempt with this peer
	LastCatchupErrorTime time.Time // When the last catchup error occurred
}

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
	// Returns a slice of PeerInfo or an error if the operation fails.
	GetPeers(ctx context.Context) ([]*PeerInfo, error)

	// GetPeer retrieves information about a specific peer from the P2P network.
	// Returns nil if the peer is not found in the registry.
	//
	// Parameters:
	// - ctx: Context for the operation
	// - peerID: The peer ID to look up
	//
	// Returns the peer information or nil if not found, or an error if the operation fails.
	GetPeer(ctx context.Context, peerID string) (*PeerInfo, error)

	// BanPeer adds a peer to the ban list to prevent future connections.
	// It can ban by peer ID, IP address, or subnet depending on the request parameters.
	//
	// Parameters:
	// - ctx: Context for the operation
	// - addr: Peer address (IP or subnet) to ban
	// - until: Unix timestamp when the ban expires
	//
	// Returns an error if the ban operation fails.
	BanPeer(ctx context.Context, addr string, until int64) error

	// UnbanPeer removes a peer from the ban list, allowing future connections.
	// It operates on peer ID, IP address, or subnet as specified in the request.
	//
	// Parameters:
	// - ctx: Context for the operation
	// - addr: Peer address (IP or subnet) to unban
	//
	// Returns an error if the unban operation fails.
	UnbanPeer(ctx context.Context, addr string) error

	// IsBanned checks if a specific peer is currently banned.
	// This can be used to verify ban status before attempting connection.
	//
	// Parameters:
	// - ctx: Context for the operation
	// - ipOrSubnet: IP address or subnet to check
	//
	// Returns true if banned, false otherwise, or an error if the check fails.
	IsBanned(ctx context.Context, ipOrSubnet string) (bool, error)

	// ListBanned returns all currently banned peers.
	// This provides a comprehensive view of all active bans in the system.
	//
	// Parameters:
	// - ctx: Context for the operation
	//
	// Returns a list of all banned peer IDs or an error if the operation fails.
	ListBanned(ctx context.Context) ([]string, error)

	// ClearBanned removes all peer bans from the system.
	// This effectively resets the ban list to empty, allowing all peers to connect.
	//
	// Parameters:
	// - ctx: Context for the operation
	//
	// Returns an error if the clear operation fails.
	ClearBanned(ctx context.Context) error

	// AddBanScore adds to a peer's ban score with the specified reason.
	//
	// Parameters:
	// - ctx: Context for the operation
	// - peerID: Peer ID to add ban score to
	// - reason: Reason for adding ban score
	//
	// Returns an error if the operation fails.
	AddBanScore(ctx context.Context, peerID string, reason string) error

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

	// UpdateCatchupError stores the last catchup error for a peer.
	// This helps track why catchup failed for specific peers.
	UpdateCatchupError(ctx context.Context, peerID string, errorMsg string) error

	// UpdateCatchupReputation updates the reputation score for a peer.
	// Score should be between 0 and 100.
	UpdateCatchupReputation(ctx context.Context, peerID string, score float64) error

	// GetPeersForCatchup returns peers suitable for catchup operations.
	// Returns peers sorted by reputation (highest first).
	GetPeersForCatchup(ctx context.Context) ([]*PeerInfo, error)

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

	// GetPeerRegistry retrieves the comprehensive peer registry data.
	// Returns all peers in the registry with their complete information.
	GetPeerRegistry(ctx context.Context) ([]*PeerInfo, error)

	// RecordBytesDownloaded records the number of bytes downloaded via HTTP from a peer.
	// This method is called after downloading data (blocks, subtrees, etc.) from a peer's
	// DataHub URL to track total network usage per peer.
	//
	// Parameters:
	// - ctx: Context for the operation
	// - peerID: Peer ID string that provided the data
	// - bytesDownloaded: Number of bytes downloaded in this operation
	//
	// Returns an error if the operation fails.
	RecordBytesDownloaded(ctx context.Context, peerID string, bytesDownloaded uint64) error
}
