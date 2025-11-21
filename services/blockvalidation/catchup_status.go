// This file contains catchup status reporting functionality for the dashboard.
package blockvalidation

import (
	"strconv"
	"time"
)

// PreviousAttempt represents a failed catchup attempt to a peer.
// This structure captures details about why a catchup attempt failed,
// allowing the dashboard to show what went wrong with each peer.
type PreviousAttempt struct {
	// PeerID is the P2P identifier of the peer we attempted to sync from
	PeerID string `json:"peer_id"`

	// PeerURL is the DataHub URL of the peer we attempted to sync from
	PeerURL string `json:"peer_url"`

	// TargetBlockHash is the hash of the block we were attempting to catch up to
	TargetBlockHash string `json:"target_block_hash,omitempty"`

	// TargetBlockHeight is the height of the block we were attempting to catch up to
	TargetBlockHeight uint32 `json:"target_block_height,omitempty"`

	// ErrorMessage is the error that caused us to switch peers
	ErrorMessage string `json:"error_message"`

	// ErrorType categorizes the error (e.g., "validation_failure", "network_error", "secret_mining")
	ErrorType string `json:"error_type"`

	// AttemptTime is when this attempt occurred (Unix timestamp in milliseconds)
	AttemptTime int64 `json:"attempt_time"`

	// DurationMs is how long this attempt ran before failing
	DurationMs int64 `json:"duration_ms"`

	// BlocksValidated is how many blocks were validated before failure
	BlocksValidated int64 `json:"blocks_validated,omitempty"`
}

// CatchupStatus represents the current state of an active catchup operation.
// This structure is designed for API consumption by the dashboard.
type CatchupStatus struct {
	// IsCatchingUp indicates whether a catchup operation is currently active
	IsCatchingUp bool `json:"is_catching_up"`

	// PeerID is the P2P identifier of the peer we're syncing from
	PeerID string `json:"peer_id,omitempty"`

	// PeerURL is the DataHub URL of the peer we're syncing from
	PeerURL string `json:"peer_url,omitempty"`

	// TargetBlockHash is the hash of the block we're catching up to
	TargetBlockHash string `json:"target_block_hash,omitempty"`

	// TargetBlockHeight is the height of the block we're catching up to
	TargetBlockHeight uint32 `json:"target_block_height,omitempty"`

	// CurrentHeight is our current blockchain height before catchup
	CurrentHeight uint32 `json:"current_height,omitempty"`

	// TotalBlocks is the total number of blocks to sync
	TotalBlocks int `json:"total_blocks,omitempty"`

	// BlocksFetched is the number of blocks fetched so far
	BlocksFetched int64 `json:"blocks_fetched,omitempty"`

	// BlocksValidated is the number of blocks validated so far
	BlocksValidated int64 `json:"blocks_validated,omitempty"`

	// StartTime is when the catchup started (Unix timestamp in milliseconds)
	StartTime int64 `json:"start_time,omitempty"`

	// DurationMs is how long the catchup has been running
	DurationMs int64 `json:"duration_ms,omitempty"`

	// ForkDepth indicates how many blocks behind the peer we were at start
	ForkDepth uint32 `json:"fork_depth,omitempty"`

	// CommonAncestorHash is the hash of the common ancestor block
	CommonAncestorHash string `json:"common_ancestor_hash,omitempty"`

	// CommonAncestorHeight is the height of the common ancestor block
	CommonAncestorHeight uint32 `json:"common_ancestor_height,omitempty"`

	// PreviousAttempt contains details about the last failed catchup attempt, if any
	PreviousAttempt *PreviousAttempt `json:"previous_attempt,omitempty"`
}

// getCatchupStatusInternal returns the current catchup status for API/dashboard consumption.
// This method is thread-safe and can be called from HTTP handlers.
//
// Returns:
//   - *CatchupStatus: Current catchup status, or a status with IsCatchingUp=false if no catchup is active
func (u *Server) getCatchupStatusInternal() *CatchupStatus {
	status := &CatchupStatus{
		IsCatchingUp: u.isCatchingUp.Load(),
	}

	// Get the active catchup context and previous attempt (thread-safe)
	u.activeCatchupCtxMu.RLock()
	ctx := u.activeCatchupCtx
	previousAttempt := u.previousCatchupAttempt
	u.activeCatchupCtxMu.RUnlock()

	// Include previous attempt if available (whether currently catching up or not)
	if previousAttempt != nil {
		status.PreviousAttempt = previousAttempt
	}

	// If no catchup is active, return status with only previous attempt info
	if !status.IsCatchingUp {
		return status
	}

	// If context is nil (race condition or clearing), return not catching up
	if ctx == nil {
		status.IsCatchingUp = false
		return status
	}

	// Populate status from catchup context
	status.PeerID = ctx.peerID
	status.PeerURL = ctx.baseURL
	status.TargetBlockHash = ctx.blockUpTo.Hash().String()
	status.TargetBlockHeight = ctx.blockUpTo.Height
	status.CurrentHeight = ctx.currentHeight
	status.TotalBlocks = len(ctx.blockHeaders)
	status.BlocksFetched = u.blocksFetched.Load()
	status.BlocksValidated = u.blocksValidated.Load()
	status.StartTime = ctx.startTime.UnixMilli()
	status.DurationMs = time.Since(ctx.startTime).Milliseconds()
	status.ForkDepth = ctx.forkDepth

	// Add common ancestor info if available
	if ctx.commonAncestorHash != nil {
		status.CommonAncestorHash = ctx.commonAncestorHash.String()
	}
	if ctx.commonAncestorMeta != nil {
		status.CommonAncestorHeight = ctx.commonAncestorMeta.Height
	}

	return status
}

// GetCatchupStatusSummary returns a brief summary of catchup status for logging.
// This method is useful for debug logging and troubleshooting.
//
// Returns:
//   - string: Human-readable summary of catchup status
func (u *Server) GetCatchupStatusSummary() string {
	status := u.getCatchupStatusInternal()

	if !status.IsCatchingUp {
		return "No active catchup"
	}

	return formatCatchupStatusSummary(status)
}

// formatCatchupStatusSummary formats a catchup status as a human-readable string.
func formatCatchupStatusSummary(status *CatchupStatus) string {
	if status == nil || !status.IsCatchingUp {
		return "No active catchup"
	}

	var summary string
	summary += "Catching up from peer " + status.PeerID
	summary += " to block " + shortHash(status.TargetBlockHash)
	if status.TotalBlocks > 0 {
		summary += " (" + formatProgress(status.BlocksValidated, int64(status.TotalBlocks)) + ")"
	}
	summary += " [" + formatDuration(status.DurationMs) + "]"

	return summary
}

// shortHash returns a shortened version of a block hash for display.
func shortHash(hash string) string {
	if len(hash) <= 16 {
		return hash
	}
	return hash[:8] + "..." + hash[len(hash)-8:]
}

// formatProgress returns a formatted progress string like "50/100 (50%)".
func formatProgress(current, total int64) string {
	if total == 0 {
		return "0/0"
	}
	percentage := float64(current) / float64(total) * 100
	return formatInt(current) + "/" + formatInt(total) + " (" + formatFloat(percentage, 1) + "%)"
}

// formatInt formats an int64 as a string.
func formatInt(n int64) string {
	return strconv.FormatInt(n, 10)
}

// formatFloat formats a float64 with specified precision.
func formatFloat(f float64, precision int) string {
	return strconv.FormatFloat(f, 'f', precision, 64)
}

// formatDuration formats a duration in milliseconds as a human-readable string.
func formatDuration(ms int64) string {
	switch {
	case ms < 1000:
		return formatInt(ms) + "ms"
	case ms < 60000:
		return formatInt(ms/1000) + "s"
	case ms < 3600000:
		return formatInt(ms/60000) + "m"
	default:
		return formatInt(ms/3600000) + "h"
	}
}
