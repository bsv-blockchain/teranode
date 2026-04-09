package httpimpl

import (
	"encoding/hex"
	"net/http"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

// GetMiningCandidateLegacyBlock creates an HTTP handler that streams a mining candidate's block
// in the standard Bitcoin wire format (SER_NETWORK serialization).
//
// This endpoint requires a valid mining candidate ID obtained from a prior getminingcandidate RPC call.
// It calls the block assembly service to look up the candidate, construct a default coinbase,
// compute the merkle root, and build the block header. Then it streams all transactions from
// the subtree store in the same format as GetLegacyBlock.
//
// The output is suitable for use with SVNode's getblocktemplate proposal mode:
//
//	Header (80 bytes) + VarInt(txCount) + coinbaseTx + remaining transactions
//
// URL Parameters:
//   - id: Mining candidate ID as hex string (from getminingcandidate RPC)
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/octet-stream
//	Body: Standard Bitcoin wire format block data
//
// Error Responses:
//   - 400 Bad Request: Invalid candidate ID format
//   - 404 Not Found: Candidate expired or not found
//   - 501 Not Implemented: Block assembly client not configured
//   - 500 Internal Server Error: Block assembly or streaming errors
//
// Example Usage:
//
//	GET /api/v1/block_legacy/miningcandidate/<hex_id>
func (h *HTTP) GetMiningCandidateLegacyBlock() func(c echo.Context) error {
	return func(c echo.Context) error {
		idStr := c.Param("id")

		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetMiningCandidateLegacyBlock_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetMiningCandidateLegacyBlock for %s: %s", c.Request().RemoteAddr, idStr),
		)
		defer deferFn()

		if h.blockAssemblyClient == nil {
			return echo.NewHTTPError(http.StatusNotImplemented, "block assembly client not configured")
		}

		candidateID, err := hex.DecodeString(idStr)
		if err != nil || len(candidateID) == 0 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid candidate ID format").Error())
		}

		// Call block assembly to get the candidate block metadata
		resp, err := h.blockAssemblyClient.GetCandidateBlock(ctx, candidateID)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				return echo.NewHTTPError(http.StatusNotFound, "mining candidate not found or expired")
			}

			h.logger.Errorf("[Asset_http] GetMiningCandidateLegacyBlock error from block assembly: %v", err)
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		// Stream the wire format block using the repository's streaming infrastructure
		r, err := h.repository.GetMiningCandidateLegacyBlockReader(ctx, resp.Header, resp.CoinbaseTx, resp.SubtreeHashes, resp.TransactionCount)
		if err != nil {
			h.logger.Errorf("[Asset_http] GetMiningCandidateLegacyBlock streaming error: %v", err)
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		return c.Stream(http.StatusOK, echo.MIMEOctetStream, r)
	}
}
