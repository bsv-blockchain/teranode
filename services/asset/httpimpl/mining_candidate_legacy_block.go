package httpimpl

import (
	"net/http"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

// handleMiningCandidateLegacyBlock handles requests for mining candidate blocks in standard
// Bitcoin wire format (SER_NETWORK serialization). Called from GetLegacyBlock when
// ?type=miningcandidate is set.
//
// The :hash parameter is interpreted as the mining candidate ID (hex-encoded, from a prior
// getminingcandidate RPC call). The block assembly service looks up the candidate, constructs
// a default coinbase, computes the merkle root, and returns the 80-byte header + metadata.
// The asset service then streams all transactions from the subtree store.
//
// The output is suitable for SVNode's getblocktemplate proposal mode:
//
//	Header (80 bytes) + VarInt(txCount) + coinbaseTx + remaining transactions
//
// Error Responses:
//   - 400 Bad Request: Invalid candidate ID format
//   - 404 Not Found: Candidate expired or not found
//   - 501 Not Implemented: Block assembly client not configured
//   - 500 Internal Server Error: Block assembly or streaming errors
func (h *HTTP) handleMiningCandidateLegacyBlock(c echo.Context) error {
	idStr := c.Param("hash")

	ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetMiningCandidateLegacyBlock_http",
		tracing.WithParentStat(AssetStat),
		tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetMiningCandidateLegacyBlock for %s: %s", c.RealIP(), idStr),
	)
	defer deferFn()

	if h.blockAssemblyClient == nil {
		return echo.NewHTTPError(http.StatusNotImplemented, "block assembly client not configured")
	}

	// The RPC returns candidate IDs in reversed hex (Bitcoin display convention).
	// We need to reverse back to internal byte order for the job store lookup.
	candidateID, err := util.DecodeAndReverseHexString(idStr)
	if err != nil || len(candidateID) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid candidate ID format").Error())
	}

	resp, err := h.blockAssemblyClient.GetCandidateBlock(ctx, candidateID)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "mining candidate not found or expired")
		}

		h.logger.Errorf("[Asset_http] GetMiningCandidateLegacyBlock error from block assembly: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	r, err := h.repository.GetMiningCandidateLegacyBlockReader(ctx, resp.Header, resp.CoinbaseTx, resp.SubtreeHashes, resp.TransactionCount)
	if err != nil {
		h.logger.Errorf("[Asset_http] GetMiningCandidateLegacyBlock streaming error: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.Stream(http.StatusOK, echo.MIMEOctetStream, r)
}
