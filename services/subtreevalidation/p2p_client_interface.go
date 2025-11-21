package subtreevalidation

import (
	"context"
)

// P2PClientI defines the interface for P2P client operations needed by SubtreeValidation.
// This interface is a subset of p2p.ClientI, containing only the methods
// that SubtreeValidation needs for reporting peer metrics to the peer registry.
//
// This interface exists to avoid circular dependencies between subtreevalidation and p2p packages.
type P2PClientI interface {
	// ReportValidSubtree reports that a subtree was successfully fetched and validated from a peer.
	ReportValidSubtree(ctx context.Context, peerID string, subtreeHash string) error

	// RecordBytesDownloaded records the number of bytes downloaded via HTTP from a peer.
	// This is called after downloading data (subtrees, etc.) from a peer's DataHub URL.
	RecordBytesDownloaded(ctx context.Context, peerID string, bytesDownloaded uint64) error
}
