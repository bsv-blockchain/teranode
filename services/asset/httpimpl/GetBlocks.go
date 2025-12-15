// Package httpimpl provides HTTP handlers for blockchain data retrieval,
// including paginated block listings.
package httpimpl

import (
	"net/http"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

// GetBlocks handles HTTP GET requests for retrieving a paginated list of blocks.
// It supports pagination and optional inclusion of orphaned blocks.
//
// Parameters:
//   - c: Echo context containing the HTTP request and response
//
// Query Parameters:
//
//   - offset: Number of blocks to skip from the tip (default: 0)
//     Example: ?offset=100
//
//   - limit: Maximum number of blocks to return (default: 20, max: 100)
//     Example: ?limit=50
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
//	Body: Paginated list of blocks:
//	  {
//	    "data": [
//	      {
//	        "seen_at": "<timestamp>",        // When block was first seen
//	        "height": <uint32>,              // Block height
//	        "orphaned": <boolean>,           // Whether block is orphaned
//	        "block_header": "<bytes>",       // Block header in bytes
//	        "miner": "<string>",             // Miner information
//	        "coinbase_value": <uint64>,      // Coinbase reward in satoshis
//	        "transaction_count": <uint64>,    // Number of transactions
//	        "size": <uint64>                 // Block size in bytes
//	      },
//	      // ... additional blocks
//	    ],
//	    "pagination": {
//	      "offset": <int>,          // Current offset
//	      "limit": <int>,           // Current limit
//	      "total_records": <int>    // Total number of blocks available
//	    }
//	  }
//
// Error Responses:
//
//   - 400 Bad Request:
//
//   - Invalid offset parameter
//
//   - Invalid limit parameter
//     Example: {"message": "strconv.Atoi: parsing \"invalid\": invalid syntax"}
//
//   - 404 Not Found:
//
//   - No blocks found
//     Example: {"message": "not found"}
//
//   - 500 Internal Server Error:
//
//   - Best block header retrieval failure
//
//   - Block data retrieval errors
//
// Monitoring:
//   - Execution time recorded in "GetBlocks_http" statistic
//   - Prometheus metric "asset_http_get_last_n_blocks" tracks successful responses
//
// Example Usage:
//
//	# Get latest 20 blocks (default)
//	GET /blocks
//
//	# Get 50 blocks starting 100 blocks from tip
//	GET /blocks?offset=100&limit=50
//
//	# Include orphaned blocks
//	GET /blocks?limit=50&includeOrphans=true
//
// Notes:
//   - Blocks are returned in descending order (newest first)
//   - Offset is calculated from the chain tip
//   - Total records includes genesis block (height 0)
//   - Response is pretty-printed JSON for readability
//   - When includeOrphans=true, orphaned blocks at the same height are included
func (h *HTTP) GetBlocks(c echo.Context) error {
	ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetBlocks_http",
		tracing.WithParentStat(AssetStat),
	)

	defer deferFn()

	offset, limit, err := h.getLimitOffset(c)
	if err != nil {
		// err is already an echo.HTTPError
		return err
	}

	includeOrphans := c.QueryParam("includeOrphans") == "true"

	// First we find the latest block height
	_, blockMeta, err := h.repository.GetBestBlockHeader(ctx)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
			return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("best block header not found").Error())
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error getting best block header", err).Error())
		}
	}

	latestBlockHeight := blockMeta.Height

	// Validate offset doesn't exceed block height to prevent underflow
	if uint32(offset) > latestBlockHeight {
		return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("offset exceeds block height").Error())
	}

	fromHeight := latestBlockHeight - uint32(offset)

	h.logger.Debugf("[Asset_http] GetBlockChain for %s with offset = %d, limit = %d and fromHeight = %d", c.Request().RemoteAddr, offset, limit, fromHeight)

	blocks, err := h.repository.GetLastNBlocks(ctx, int64(limit), includeOrphans, fromHeight)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
			return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("blocks not found").Error())
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error getting last n blocks", err).Error())
		}
	}

	prometheusAssetHTTPGetLastNBlocks.WithLabelValues("OK", "200").Inc()

	response := ExtendedResponse{
		Data: blocks,
		Pagination: Pagination{
			Offset:       offset,
			Limit:        limit,
			TotalRecords: int(latestBlockHeight) + 1,
		},
	}

	h.logger.Infof("[GetBlocks][%d][%d] sending to client in json (%d nodes)", offset, limit, len(blocks))

	return c.JSONPretty(200, response, "  ")
}
