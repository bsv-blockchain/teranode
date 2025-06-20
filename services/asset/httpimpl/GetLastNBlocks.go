// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
package httpimpl

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

// GetLastNBlocks handles HTTP GET requests to retrieve the most recent blocks
// in the blockchain. It supports filtering and optional inclusion of orphaned blocks.
//
// Parameters:
//   - c: Echo context containing the HTTP request and response
//
// Query Parameters:
//
//   - n: Number of blocks to retrieve (default: 10)
//     Example: ?n=20
//
//   - fromHeight: Starting block height for retrieval (default: 0)
//     Example: ?fromHeight=100000
//
//   - includeOrphans: Whether to include orphaned blocks (default: false)
//     Example: ?includeOrphans=true
//
// Returns:
//   - error: Any error encountered during processing
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/json
//	Body: Array of block information:
//	  [
//	    {
//	      "seen_at": "<timestamp>",        // When block was first seen
//	      "height": <uint32>,              // Block height
//	      "orphaned": <boolean>,           // Whether block is orphaned
//	      "block_header": "<bytes>",       // Block header in bytes
//	      "miner": "<string>",             // Miner information
//	      "coinbase_value": <uint64>,      // Coinbase reward in satoshis
//	      "transaction_count": <uint64>,    // Number of transactions
//	      "size": <uint64>                 // Block size in bytes
//	    },
//	    // ... additional blocks
//	  ]
//
// Error Responses:
//
//   - 400 Bad Request:
//
//   - Invalid 'n' parameter
//
//   - Invalid fromHeight parameter
//     Example: {"message": "strconv.ParseInt: parsing \"invalid\": invalid syntax"}
//
//   - 404 Not Found:
//
//   - No blocks found
//     Example: {"message": "not found"}
//
//   - 500 Internal Server Error:
//
//   - Repository errors
//
// Monitoring:
//   - Execution time recorded in "GetLastNBlocks_http" statistic
//   - Prometheus metric "asset_http_get_last_n_blocks" tracks successful responses
//   - Debug logging of request information
//
// Example Usage:
//
//	# Get last 10 blocks (default)
//	GET /blocks/last
//
//	# Get last 20 blocks
//	GET /blocks/last?n=20
//
//	# Get last 10 blocks starting from height 100000
//	GET /blocks/last?fromHeight=100000
//
//	# Get last 10 blocks including orphans
//	GET /blocks/last?includeOrphans=true
//
// Notes:
//   - Blocks are returned in descending order (newest first)
//   - When fromHeight is specified, counting starts from that height downward
//   - Response is pretty-printed JSON for readability
//   - When includeOrphans=true, orphaned blocks at the same height are included
func (h *HTTP) GetLastNBlocks(c echo.Context) error {
	queryN := c.QueryParam("n")
	queryFromHeight := c.QueryParam("fromHeight")

	ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetLastNBlocks_http",
		tracing.WithParentStat(AssetStat),
		tracing.WithDebugLogMessage(h.logger, "GetLastNBlocks_http for %s for last %s blocks from height %d", c.Request().RemoteAddr, queryN, queryFromHeight),
	)

	defer deferFn()

	var err error

	n := int64(10)
	if queryN != "" {
		n, err = strconv.ParseInt(queryN, 10, 64)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid 'n' parameter", err).Error())
		}
	}

	fromHeight := uint64(0)
	if queryFromHeight != "" {
		fromHeight, err = strconv.ParseUint(queryFromHeight, 10, 32)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid 'fromHeight' parameter", err).Error())
		}
	}

	includeOrphans := c.QueryParam("includeOrphans") == "true"

	fromHeightUint32, err := util.SafeUint64ToUint32(fromHeight)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid 'fromHeight' parameter", err).Error())
	}

	blocks, err := h.repository.GetLastNBlocks(ctx, n, includeOrphans, fromHeightUint32)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
	}

	prometheusAssetHTTPGetLastNBlocks.WithLabelValues("OK", "200").Inc()

	return c.JSONPretty(200, blocks, "  ")
}
