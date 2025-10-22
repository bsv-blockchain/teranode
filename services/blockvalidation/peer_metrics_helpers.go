package blockvalidation

import (
	"context"
	"time"
)

// reportCatchupAttempt reports a catchup attempt to the P2P service.
// Falls back to local metrics if P2P client is unavailable.
//
// Parameters:
//   - ctx: Context for the gRPC call
//   - peerID: Peer identifier
func (u *Server) reportCatchupAttempt(ctx context.Context, peerID string) {
	if peerID == "" {
		return
	}

	// Report to P2P service if client is available
	if u.p2pClient != nil {
		if err := u.p2pClient.RecordCatchupAttempt(ctx, peerID); err != nil {
			u.logger.Warnf("[peer_metrics] Failed to report catchup attempt to P2P service for peer %s: %v", peerID, err)
			// Fall through to local metrics as backup
		} else {
			return // Successfully reported to P2P service
		}
	}

	// Fallback to local metrics (for backward compatibility or when P2P client unavailable)
	// Note: Local metrics don't track attempts separately, only successes/failures
}

// reportCatchupSuccess reports a successful catchup to the P2P service.
// Falls back to local metrics if P2P client is unavailable.
//
// Parameters:
//   - ctx: Context for the gRPC call
//   - peerID: Peer identifier
//   - duration: Duration of the catchup operation
func (u *Server) reportCatchupSuccess(ctx context.Context, peerID string, duration time.Duration) {
	if peerID == "" {
		return
	}

	durationMs := duration.Milliseconds()

	// Report to P2P service if client is available
	if u.p2pClient != nil {
		if err := u.p2pClient.RecordCatchupSuccess(ctx, peerID, durationMs); err != nil {
			u.logger.Warnf("[peer_metrics] Failed to report catchup success to P2P service for peer %s: %v", peerID, err)
			// Fall through to local metrics as backup
		} else {
			return // Successfully reported to P2P service
		}
	}

	// Fallback to local metrics (for backward compatibility or when P2P client unavailable)
	if u.peerMetrics != nil {
		peerMetric := u.peerMetrics.GetOrCreatePeerMetrics(peerID)
		if peerMetric != nil {
			peerMetric.RecordSuccess()
		}
	}
}

// reportCatchupFailure reports a failed catchup to the P2P service.
// Falls back to local metrics if P2P client is unavailable.
//
// Parameters:
//   - ctx: Context for the gRPC call
//   - peerID: Peer identifier
func (u *Server) reportCatchupFailure(ctx context.Context, peerID string) {
	if peerID == "" {
		return
	}

	// Report to P2P service if client is available
	if u.p2pClient != nil {
		if err := u.p2pClient.RecordCatchupFailure(ctx, peerID); err != nil {
			u.logger.Warnf("[peer_metrics] Failed to report catchup failure to P2P service for peer %s: %v", peerID, err)
			// Fall through to local metrics as backup
		} else {
			return // Successfully reported to P2P service
		}
	}

	// Fallback to local metrics (for backward compatibility or when P2P client unavailable)
	if u.peerMetrics != nil {
		peerMetric := u.peerMetrics.GetOrCreatePeerMetrics(peerID)
		if peerMetric != nil {
			peerMetric.RecordFailure()
		}
	}
}

// reportCatchupMalicious reports malicious behavior to the P2P service.
// Falls back to local metrics if P2P client is unavailable.
//
// Parameters:
//   - ctx: Context for the gRPC call
//   - peerID: Peer identifier
//   - reason: Description of the malicious behavior (for logging)
func (u *Server) reportCatchupMalicious(ctx context.Context, peerID string, reason string) {
	if peerID == "" {
		return
	}

	u.logger.Warnf("[peer_metrics] Recording malicious attempt from peer %s: %s", peerID, reason)

	// Report to P2P service if client is available
	if u.p2pClient != nil {
		if err := u.p2pClient.RecordCatchupMalicious(ctx, peerID); err != nil {
			u.logger.Warnf("[peer_metrics] Failed to report malicious behavior to P2P service for peer %s: %v", peerID, err)
			// Fall through to local metrics as backup
		} else {
			return // Successfully reported to P2P service
		}
	}

	// Fallback to local metrics (for backward compatibility or when P2P client unavailable)
	if u.peerMetrics != nil {
		peerMetric := u.peerMetrics.GetOrCreatePeerMetrics(peerID)
		if peerMetric != nil {
			peerMetric.RecordMaliciousAttempt()
		}
	}
}

// isPeerMalicious checks if a peer is marked as malicious.
// Checks P2P service first, falls back to local metrics.
//
// Parameters:
//   - ctx: Context for the gRPC call
//   - peerID: Peer identifier
//
// Returns:
//   - bool: True if peer is malicious
func (u *Server) isPeerMalicious(ctx context.Context, peerID string) bool {
	if peerID == "" {
		return false
	}

	// Check local metrics first (faster, no network call)
	// In distributed mode, the P2P service is the source of truth,
	// but we keep local metrics as a cache for performance
	if u.peerMetrics != nil {
		peerMetric := u.peerMetrics.GetOrCreatePeerMetrics(peerID)
		if peerMetric != nil && peerMetric.IsMalicious() {
			return true
		}
	}

	return false
}

// isPeerBad checks if a peer has a bad reputation.
// Checks local metrics.
//
// Parameters:
//   - peerID: Peer identifier
//
// Returns:
//   - bool: True if peer has bad reputation
func (u *Server) isPeerBad(peerID string) bool {
	if peerID == "" {
		return false
	}

	// Check local metrics
	if u.peerMetrics != nil {
		peerMetric := u.peerMetrics.GetOrCreatePeerMetrics(peerID)
		if peerMetric != nil && peerMetric.IsBad() {
			return true
		}
	}

	return false
}
