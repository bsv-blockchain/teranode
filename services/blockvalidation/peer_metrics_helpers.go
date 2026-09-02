package blockvalidation

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

const (
	catchupFailureKindGeneric         = "generic"
	catchupFailureKindBlockIncomplete = "block_incomplete"
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

// reportValidBlockHeaders credits a peer for successfully serving a batch of
// block headers during catchup. This is a generic interaction success
// (reputation and response time) and does NOT count as a completed catchup —
// the catchup-operation outcome is reported separately by doCatchup. Keeping
// this credit prevents a peer that serves headers fine but keeps failing at the
// block-fetch stage from having its reputation collapse to the point where the
// only viable catchup peer is excluded as unhealthy.
func (u *Server) reportValidBlockHeaders(ctx context.Context, peerID string, duration time.Duration) {
	if peerID == "" {
		return
	}

	if u.p2pClient != nil {
		if err := u.p2pClient.ReportValidBlockHeaders(ctx, peerID, duration.Milliseconds()); err != nil {
			u.logger.Warnf("[peer_metrics] Failed to report valid block headers to P2P service for peer %s: %v", peerID, err)
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
	u.reportCatchupFailureWithKind(ctx, peerID, catchupFailureKindGeneric, "")
}

func (u *Server) reportCatchupFailureForError(ctx context.Context, peerID string, err error) {
	if errors.Is(err, errors.ErrBlockIncomplete) {
		return
	}
	if catchupFailureAlreadyReported(err) {
		// The layer where the failure occurred (e.g. the header-fetch stage)
		// already recorded it; reporting again would let CatchupFailures exceed
		// CatchupAttempts and double the reputation penalty for one attempt.
		return
	}
	u.reportCatchupFailure(ctx, peerID)
}

// catchupFailureReportedKey is the error-data key that marks an error chain
// whose catchup failure has already been recorded against the peer by the
// layer where it occurred, so upper layers that report failures for propagated
// errors can skip re-reporting.
const catchupFailureReportedKey = "catchup_failure_reported"

// markCatchupFailureReported wraps err to signal that its catchup failure has
// already been reported to the P2P service. The wrapper is a native *Error
// carrying a data-key marker: the errors package flattens foreign wrapper
// types into message-only links (breaking code matching downstream), so the
// marker must live in error data on a native link, which keeps the wrapped
// chain fully intact for errors.Is dispatch. Nil-safe.
func markCatchupFailureReported(err error) error {
	if err == nil {
		return nil
	}
	wrapped := errors.NewProcessingError("catchup failure reported at source", err)
	wrapped.SetData(catchupFailureReportedKey, true)
	return wrapped
}

// catchupFailureAlreadyReported reports whether err carries the
// markCatchupFailureReported marker anywhere in its wrapped chain. The walk is
// depth-bounded defensively, mirroring the errors package's own chain walks.
func catchupFailureAlreadyReported(err error) bool {
	var e *errors.Error
	if !errors.As(err, &e) {
		return false
	}

	for depth := 0; e != nil && depth < 32; depth++ {
		if v, ok := e.GetData(catchupFailureReportedKey).(bool); ok && v {
			return true
		}

		next := e.WrappedErr()
		if next == nil {
			return false
		}

		var nextErr *errors.Error
		if !errors.As(next, &nextErr) {
			return false
		}
		e = nextErr
	}

	return false
}

func (u *Server) reportCatchupFailureWithKind(ctx context.Context, peerID, failureKind, blockHash string) {
	if peerID == "" {
		return
	}

	// Report to P2P service if client is available
	if u.p2pClient != nil {
		if err := u.p2pClient.RecordCatchupFailureWithKind(ctx, peerID, failureKind, blockHash); err != nil {
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

// reportValidatedChainProgress reports locally validated header-chain progress
// to P2P. Reporting is advisory and must not affect catchup or block validation.
func (u *Server) reportValidatedChainProgress(ctx context.Context, peerID string, height uint32, blockHash string, chainWork []byte) {
	if peerID == "" || height == 0 || blockHash == "" || len(chainWork) == 0 {
		return
	}

	if u.p2pClient == nil {
		return
	}

	if err := u.p2pClient.ReportValidatedChainProgress(ctx, peerID, height, blockHash, chainWork); err != nil {
		u.logger.Warnf("[peer_metrics] Failed to report validated chain progress for peer %s at height %d: %v", peerID, height, err)
	}
}

// shouldReportConsensusMalicious decides whether a failed block validation is
// solid enough evidence against the serving peer to charge their reputation.
//
// A consensus code alone is not enough. errors.Is walks the whole wrap chain, so
// an error can carry both ErrBlockInvalid/ErrTxInvalid and ErrStorageError, and a
// storage fault is always this node's disk rather than anything the peer sent
// (issue 1439). Charging that combination would blame an honest peer for our own
// corruption, which is the failure this branch exists to remove.
//
// The exemption is defence in depth rather than a path known to be live:
// ValidateBlockWithOptions screens ErrStorageError out at its block.Valid failure
// site before wrapping in BlockInvalid, and validateBlockSubtrees wraps only on
// ErrTxInvalid. It is written down anyway because releaseCatchupLock already
// scores exactly that chain local_storage_fault, and two classifiers in one file
// reaching opposite verdicts on one error is the bug shape, not the safeguard.
//
// That argument is why the exemption delegates to isLocalCatchupFault rather than
// naming ErrStorageError alone. Screening one code while the sibling classifiers
// screen four plus context errors reproduces the disagreement one code over: a
// consensus code wrapping ErrServiceUnavailable or a context error would be scored
// local by releaseCatchupLock, exonerated by processCatchupChItem, and still
// reported malicious here. Since this is a reputation call, that is the exact
// failure the branch exists to remove.
func shouldReportConsensusMalicious(err error) bool {
	if isLocalCatchupFault(err) {
		return false
	}

	return errors.Is(err, errors.ErrBlockInvalid) || errors.Is(err, errors.ErrTxInvalid)
}

// isLocalCatchupFault reports whether a catchup failure is this node's own doing
// rather than anything the serving peer did, so no reputation charge is warranted.
//
// The predicate is the union of two errors-package helpers because neither covers
// this on its own, and they are disjoint on exactly the cases that matter here.
// IsLocalError - what recordCatchupPeerFailure uses - is context errors plus
// ErrStorageError, and misses the *Unavailable codes. IsTransientLocalError is
// {ServiceError, StorageError, ServiceUnavailable, StorageUnavailable}, and misses
// context errors entirely. Either one alone leaves a class of purely local failure
// charged to an honest primary: a shutdown or catchup-context deadline in the first
// case, an aerospike batch timeout (ErrServiceUnavailable) in the second.
//
// The explicit ErrContextCanceled test is not redundant with IsContextError. That
// helper resolves the chain with errors.As, which stops at the OUTERMOST *Error,
// so it reads the code of the wrapper rather than of the cause; a consensus code
// wrapping a context cancellation therefore misses the code test and falls through
// to a substring match on the rendered message, which matches only the exact
// wording "context canceled". errors.Is walks the chain by code, so it catches the
// wrapped case whatever the wrapper says. That chain is the one this predicate
// exists for, since shouldReportConsensusMalicious is only ever asked about errors
// that already carry a consensus code on the outside.
func isLocalCatchupFault(err error) bool {
	return errors.IsTransientLocalError(err) ||
		errors.IsContextError(err) ||
		errors.Is(err, errors.ErrContextCanceled)
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

// reportValidBlockForPeers reports a successfully validated block to the P2P service
// for the primary peer and all contributing secondary peers.
// This credits reputation to all peers that provided data for this block.
func (u *Server) reportValidBlockForPeers(ctx context.Context, primaryPeerID string, blockHash string, contributingPeers map[string]struct{}) {
	if u.p2pClient == nil {
		return
	}

	// Credit the primary peer
	if primaryPeerID != "" {
		if err := u.p2pClient.ReportValidBlock(ctx, primaryPeerID, blockHash); err != nil {
			u.logger.Warnf("[peer_metrics] Failed to report valid block %s for primary peer %s: %v", blockHash, primaryPeerID, err)
		}
	}

	// Credit each secondary peer that contributed subtree data
	secondaryCount := 0
	for contributingPeerID := range contributingPeers {
		if contributingPeerID == primaryPeerID {
			continue // already credited
		}
		if err := u.p2pClient.ReportValidBlock(ctx, contributingPeerID, blockHash); err != nil {
			u.logger.Warnf("[peer_metrics] Failed to report valid block %s for contributing peer %s: %v", blockHash, contributingPeerID, err)
		} else {
			secondaryCount++
		}
	}
	if secondaryCount > 0 {
		u.logger.Infof("[peer_metrics] Credited %d contributing peers for valid block %s", secondaryCount, blockHash)
	}
}
