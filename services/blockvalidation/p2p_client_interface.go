package blockvalidation

import (
	"context"

	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
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

	// UpdateCatchupReputation updates the reputation score for a peer.
	UpdateCatchupReputation(ctx context.Context, peerID string, score float64) error

	// GetPeersForCatchup returns peers suitable for catchup operations.
	GetPeersForCatchup(ctx context.Context) (*p2p_api.GetPeersForCatchupResponse, error)
}
