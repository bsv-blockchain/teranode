// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
package httpimpl

import (
	"net/http"

	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

// GetBlockStats handles HTTP GET requests to retrieve aggregate statistics
// about the blockchain.
//
// Parameters:
//   - c: Echo context containing the HTTP request and response
//
// Returns:
//   - error: Any error encountered during processing
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/json
//	Body: Blockchain statistics:
//	  {
//	    "block_count": <uint64>,             // Total number of blocks in chain
//	    "tx_count": <uint64>,                // Total number of transactions
//	    "max_height": <uint64>,              // Height of the latest block
//	    "avg_block_size": <double>,          // Average size of blocks
//	    "avg_tx_count_per_block": <double>,  // Average transactions per block
//	    "first_block_time": <uint32>,        // Unix timestamp of first block
//	    "last_block_time": <uint32>          // Unix timestamp of latest block
//	  }
//
// Error Responses:
//   - 500 Internal Server Error:
//   - Statistics calculation failure
//     Example: {"message": "error retrieving block statistics"}
//
// Monitoring:
//   - Execution time recorded in "GetBlockStats_http" statistic
//
// Example Usage:
//
//	# Get blockchain statistics
//	GET /blocks/stats
func (h *HTTP) GetBlockStats(c echo.Context) error {
	ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "GetBlockStats_http",
		tracing.WithParentStat(AssetStat),
		tracing.WithDebugLogMessage(h.logger, "GetBlockStats_http"),
	)

	defer deferFn()

	stats, err := h.repository.GetBlockStats(ctx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSONPretty(200, stats, "  ")
}
