package blockvalidation

import (
	"context"

	"github.com/bsv-blockchain/teranode/services/p2p"
)

// P2PClientI defines the interface for P2P client operations needed by BlockValidation.
// This interface is a subset of p2p.ClientI, containing only the catchup-related methods
// that BlockValidation needs for reporting peer metrics to the peer registry.
//
// This interface exists to avoid circular dependencies between blockvalidation and p2p packages.
type P2PClientI interface {
	// RecordCatchupAttempt records that a catchup attempt was made to a peer.
	RecordCatchupAttempt(ctx context.Context, peerID string) error

	// RecordCatchupSuccess records a successful catchup from a peer.
	RecordCatchupSuccess(ctx context.Context, peerID string, durationMs int64) error

	// RecordCatchupFailure records a failed catchup attempt from a peer.
	RecordCatchupFailure(ctx context.Context, peerID string) error

	// RecordCatchupMalicious records malicious behavior detected during catchup.
	RecordCatchupMalicious(ctx context.Context, peerID string) error

	// UpdateCatchupError stores the last catchup error for a peer.
	UpdateCatchupError(ctx context.Context, peerID string, errorMsg string) error

	// UpdateCatchupReputation updates the reputation score for a peer.
	UpdateCatchupReputation(ctx context.Context, peerID string, score float64) error

	// GetPeersForCatchup returns peers suitable for catchup operations.
	// Returns a slice of PeerInfo sorted by reputation (highest first).
	GetPeersForCatchup(ctx context.Context) ([]*p2p.PeerInfo, error)

	// GetPeer returns information about a specific peer.
	// Returns nil if the peer is not found.
	GetPeer(ctx context.Context, peerID string) (*p2p.PeerInfo, error)

	// ReportValidBlock reports that a block was successfully received and validated from a peer.
	ReportValidBlock(ctx context.Context, peerID string, blockHash string) error

	// ReportValidSubtree reports that a subtree was successfully received and validated from a peer.
	ReportValidSubtree(ctx context.Context, peerID string, subtreeHash string) error

	// IsPeerMalicious checks if a peer is considered malicious based on their behavior.
	// A peer is considered malicious if they are banned or have a very low reputation score.
	IsPeerMalicious(ctx context.Context, peerID string) (bool, string, error)

	// IsPeerUnhealthy checks if a peer is considered unhealthy based on their performance.
	// A peer is considered unhealthy if they have poor performance metrics or low reputation.
	IsPeerUnhealthy(ctx context.Context, peerID string) (bool, string, float32, error)

	// RecordBytesDownloaded records the number of bytes downloaded via HTTP from a peer.
	// This is called after downloading data (blocks, subtrees, etc.) from a peer's DataHub URL.
	RecordBytesDownloaded(ctx context.Context, peerID string, bytesDownloaded uint64) error
}
