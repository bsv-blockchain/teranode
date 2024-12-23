// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
package httpimpl

import (
	"net/http"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

type SubtreeMeta struct {
	TxCount int    `json:"txCount"`
	Hash    string `json:"hash"`
	Index   int    `json:"index"`
	Fee     uint64 `json:"fee"`
	Size    uint64 `json:"size"`
}

// GetBlockSubtrees creates an HTTP handler for retrieving paginated subtree information
// for a specific block. While it accepts a ReadMode parameter, it only supports JSON output.
//
// Parameters:
//   - mode: ReadMode (only JSON mode is supported)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// URL Parameters:
//   - hash: Block hash (hex string)
//
// Query Parameters:
//
//   - offset: Number of subtrees to skip (default: 0)
//     Example: ?offset=10
//
//   - limit: Maximum number of subtrees to return (default: 20, max: 100)
//     Example: ?limit=50
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/json
//	Body: Paginated list of subtree metadata:
//	  {
//	    "data": [
//	      {
//	        "txCount": <int>,      // Number of transactions in subtree
//	        "hash": "<string>",    // Subtree hash
//	        "index": <int>,        // Index in block's subtree list
//	        "fee": <uint64>,       // Total fees for transactions in subtree
//	        "size": <uint64>       // Total size of subtree in bytes
//	      },
//	      // ... additional subtrees
//	    ],
//	    "pagination": {
//	      "offset": <int>,         // Current offset
//	      "limit": <int>,          // Current limit
//	      "total_records": <int>   // Total number of subtrees in block
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
//   - Block not found
//     Example: {"message": "block not found"}
//
//   - 500 Internal Server Error:
//
//   - Invalid block hash format
//
//   - Repository errors
//
//   - Unsupported read mode
//
// Monitoring:
//   - Prometheus metric "asset_http_get_block" tracks successful responses
//
// Example Usage:
//
//	# Get first 20 subtrees of a block
//	GET /block/subtrees/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f
//
//	# Get 50 subtrees starting from index 100
//	GET /block/subtrees/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f?offset=100&limit=50
//
// Notes:
//   - If a subtree's details cannot be retrieved, the response will include only its hash and index
//   - Subtrees are returned in their original order within the block
//   - Despite accepting a mode parameter, only JSON format is supported
//   - Each subtree represents a group of transactions with associated metadata
//   - The subtree count (total_records) comes from the block's subtree list length
func (h *HTTP) GetBlockSubtrees(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		hashStr := c.Param("hash")

		ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "GetBlockSubtrees_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetBlockSubtrees in %s for %s: %s", mode, c.Request().RemoteAddr, hashStr),
		)

		defer deferFn()

		if len(hashStr) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash string", err).Error())
		}

		block, err := h.repository.GetBlockByHash(ctx, hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		offset, limit, err := h.getLimitOffset(c)
		if err != nil {
			// error is already an echo error
			return err
		}

		result := ExtendedResponse{
			Pagination: Pagination{
				Offset:       offset,
				Limit:        limit,
				TotalRecords: len(block.Subtrees),
			},
		}

		// get all the subtrees for the block
		var (
			subtreeHead *util.Subtree
			numNodes    int
		)

		data := make([]SubtreeMeta, 0, limit)
		if len(block.Subtrees) > 0 {
			for i := offset; i < offset+limit; i++ {
				if i >= len(block.Subtrees) {
					break
				}

				subtreeHash := block.Subtrees[i]

				// do not check for error here, we will just return an empty row for the subtree
				subtreeHead, numNodes, _ = h.repository.GetSubtreeHead(ctx, subtreeHash)

				if subtreeHead != nil {
					data = append(data, SubtreeMeta{
						Index:   i,
						Hash:    subtreeHash.String(),
						TxCount: numNodes,
						Fee:     subtreeHead.Fees,
						Size:    subtreeHead.SizeInBytes,
					})
				} else {
					data = append(data, SubtreeMeta{
						Index: i,
						Hash:  subtreeHash.String(),
					})
				}
			}
		}

		result.Data = data

		prometheusAssetHttpGetBlock.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			return c.JSONPretty(200, result, "  ")
		}

		return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("bad read mode").Error())
	}
}
