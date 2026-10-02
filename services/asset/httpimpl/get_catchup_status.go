package httpimpl

import (
	"context"
	"net/http"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/labstack/echo/v4"
)

// GetCatchupStatus returns the current catchup status from the BlockValidation service
func (h *HTTP) GetCatchupStatus(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), 5*time.Second)
	defer cancel()

	// Get BlockValidation client from repository
	blockValidationClient := h.repository.GetBlockvalidationClient()
	if blockValidationClient == nil {
		h.logger.Errorf("[GetCatchupStatus] BlockValidation client not available")
		return c.JSON(http.StatusServiceUnavailable, map[string]interface{}{
			"error":          "BlockValidation service not available",
			"is_catching_up": false,
		})
	}

	// Get catchup status
	status, err := blockValidationClient.GetCatchupStatus(ctx)
	if err != nil {
		h.logger.Errorf("[GetCatchupStatus] Failed to get catchup status: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":          "Failed to get catchup status",
			"is_catching_up": false,
		})
	}

	// Convert to JSON response
	jsonResp := map[string]interface{}{
		"is_catching_up":         status.IsCatchingUp,
		"peer_id":                status.PeerID,
		"target_block_hash":      status.TargetBlockHash,
		"target_block_height":    status.TargetBlockHeight,
		"current_height":         status.CurrentHeight,
		"total_blocks":           status.TotalBlocks,
		"blocks_fetched":         status.BlocksFetched,
		"blocks_validated":       status.BlocksValidated,
		"start_time":             status.StartTime,
		"duration_ms":            status.DurationMs,
		"fork_depth":             status.ForkDepth,
		"common_ancestor_hash":   status.CommonAncestorHash,
		"common_ancestor_height": status.CommonAncestorHeight,
	}

	// Add previous attempt if available
	var (
		previousAttempt    map[string]interface{}
		previousPeerURL    string
		previousErrMessage string
	)

	if status.PreviousAttempt != nil {
		previousPeerURL = status.PreviousAttempt.PeerURL
		previousErrMessage = status.PreviousAttempt.ErrorMessage

		previousAttempt = map[string]interface{}{
			"peer_id":             status.PreviousAttempt.PeerID,
			"target_block_hash":   status.PreviousAttempt.TargetBlockHash,
			"target_block_height": status.PreviousAttempt.TargetBlockHeight,
			"error_type":          status.PreviousAttempt.ErrorType,
			"attempt_time":        status.PreviousAttempt.AttemptTime,
			"duration_ms":         status.PreviousAttempt.DurationMs,
			"blocks_validated":    status.PreviousAttempt.BlocksValidated,
		}

		jsonResp["previous_attempt"] = previousAttempt
	}

	applyCatchupStatusProjection(h.settings, jsonResp, previousAttempt, status.PeerURL, previousPeerURL, previousErrMessage)

	return c.JSON(http.StatusOK, jsonResp)
}

// applyCatchupStatusProjection adds back the fields that are only public when
// the operator leaves the corresponding detail switch on.
//
// error_message is the failed attempt's raw wrapped error, assigned verbatim in
// the block-validation catchup path, so it carries store URLs, local paths and
// internal host:port pairs; error_type stays public and is the coarse
// classification an operator actually reads. The peer URLs are the peer's own
// advertised DataHub address, but which one this node currently trusts is not
// something an anonymous caller needs.
func applyCatchupStatusProjection(tSettings *settings.Settings, resp, previousAttempt map[string]interface{},
	peerURL, previousPeerURL, previousErrorMessage string) {
	if tSettings.Asset.PublicPeersDetail {
		resp["peer_url"] = peerURL

		if previousAttempt != nil {
			previousAttempt["peer_url"] = previousPeerURL
		}
	}

	if tSettings.Asset.PublicErrorDetail && previousAttempt != nil {
		previousAttempt["error_message"] = previousErrorMessage
	}
}
