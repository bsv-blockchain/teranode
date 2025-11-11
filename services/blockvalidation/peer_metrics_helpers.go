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

	// Fallback: No local metrics needed since we're using P2P service for all peer tracking
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
		}
	}
}

// reportCatchupError stores the catchup error message in the peer registry.
// This allows the UI to display why catchup failed for each peer.
//
// Parameters:
//   - ctx: Context for the operation
//   - peerID: Peer identifier
//   - errorMsg: Error message to store
func (u *Server) reportCatchupError(ctx context.Context, peerID string, errorMsg string) {
	if peerID == "" || errorMsg == "" {
		return
	}

	// Report to P2P service if client is available
	if u.p2pClient != nil {
		if err := u.p2pClient.UpdateCatchupError(ctx, peerID, errorMsg); err != nil {
			u.logger.Warnf("[peer_metrics] Failed to update catchup error for peer %s: %v", peerID, err)
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

	// Fallback: No local metrics needed since we're using P2P service for all peer tracking
}

// isPeerMalicious checks if a peer is marked as malicious.
// Queries the P2P service for the peer's status.
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

	// Query P2P service for peer status
	if u.p2pClient != nil {
		isMalicious, reason, err := u.p2pClient.IsPeerMalicious(ctx, peerID)
		if err != nil {
			u.logger.Warnf("[isPeerMalicious] Failed to check if peer %s is malicious: %v", peerID, err)
			// On error, assume peer is not malicious to avoid false positives
			return false
		}
		if isMalicious {
			u.logger.Debugf("[isPeerMalicious] Peer %s is malicious: %s", peerID, reason)
		}
		return isMalicious
	}

	return false
}

// isPeerBad checks if a peer has a bad reputation.
// Queries the P2P service for the peer's health status.
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

	// Query P2P service for peer health status
	if u.p2pClient != nil {
		// Use context.Background() since the old method didn't require context
		isUnhealthy, reason, reputationScore, err := u.p2pClient.IsPeerUnhealthy(context.Background(), peerID)
		if err != nil {
			u.logger.Warnf("[isPeerBad] Failed to check if peer %s is unhealthy: %v", peerID, err)
			// On error, assume peer is not bad to avoid false positives
			return false
		}
		if isUnhealthy {
			u.logger.Debugf("[isPeerBad] Peer %s is unhealthy (reputation: %.2f): %s", peerID, reputationScore, reason)
		}
		return isUnhealthy
	}

	return false
}
