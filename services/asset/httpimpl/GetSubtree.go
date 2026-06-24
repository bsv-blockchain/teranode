// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
package httpimpl

import (
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

// calculateSpeed takes the duration of the transfer and the size of the data transferred (in bytes)
// and returns the speed in kilobytes per second.
func calculateSpeed(duration time.Duration, sizeInKB float64) float64 {
	// Convert duration to seconds
	seconds := duration.Seconds()

	// Calculate speed in KB/s
	speed := sizeInKB / seconds

	return speed
}

// GetSubtree creates an HTTP handler for retrieving subtree data in multiple formats.
// Includes performance monitoring and response signing.
//
// Parameters:
//   - mode: ReadMode specifying the response format (JSON, BINARY_STREAM, or HEX)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// URL Parameters:
//   - hash: Subtree hash (hex string)
//
// Query Parameters (JSON mode only):
//   - offset: Starting index for pagination (default: 0)
//   - limit: Maximum number of nodes to return (default: 20, max: 100)
//
// HTTP Response Formats:
//
//  1. JSON (mode = JSON):
//     Status: 200 OK
//     Content-Type: application/json
//     Body:
//     {
//     "data": {
//     "Height": <int>,
//     "Fees": <uint64>,
//     "SizeInBytes": <uint64>,
//     "FeeHash": "<string>",
//     "Nodes": [
//     // Array of subtree nodes (paginated)
//     ],
//     "ConflictingNodes": [
//     // Array of conflicting node hashes
//     ]
//     },
//     "pagination": {
//     "offset": <int>,
//     "limit": <int>,
//     "totalRecords": <int>
//     }
//     }
//
//  2. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: Raw subtree node data (32 bytes per node)
//
//  3. Hex (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hexadecimal encoding of node data
//
// Error Responses:
//
//   - 404 Not Found:
//
//   - Subtree not found
//     Example: {"message": "not found"}
//
//   - 500 Internal Server Error:
//
//   - Invalid subtree hash format
//
//   - Subtree deserialization errors
//
//   - Invalid read mode
//
// Monitoring:
//   - Execution time recorded in "GetSubtree_http" statistic
//   - Prometheus metric "asset_http_get_subtree" tracks responses
//   - Performance logging including transfer speed (KB/sec)
//   - Response size logging in KB
//
// Security:
//   - Response includes cryptographic signature if private key is configured
//
// Notes:
//   - JSON mode requires full subtree deserialization
//   - Binary/Hex modes use more efficient streaming approach
//   - Includes performance metrics in logs
func (h *HTTP) GetSubtree(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetSubtree_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetSubtree in %s for %s: %s", mode, c.RealIP(), c.Param("hash")),
		)

		defer deferFn()

		if len(c.Param("hash")) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash string", err).Error())
		}

		prometheusAssetHTTPGetSubtree.WithLabelValues("OK", "200").Inc()

		// sign the response, if the private key is set, ignore error
		// do this before any output is sent to the client, this adds a signature to the response header
		_ = h.Sign(c.Response(), hash.CloneBytes())

		// At this point, the subtree contains all the fees and sizes for the transactions in the subtree.

		if mode == JSON {
			offset, limit, err := h.getLimitOffset(c)
			if err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, err.Error())
			}

			subtree, offset, totalNodes, err := h.repository.GetSubtreePage(ctx, hash, offset, limit)
			if err != nil {
				if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
					return echo.NewHTTPError(http.StatusNotFound, err.Error())
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}

			response := ExtendedResponse{
				Data: map[string]any{
					"Height":           subtree.Height,
					"Fees":             subtree.Fees,
					"SizeInBytes":      subtree.SizeInBytes,
					"FeeHash":          &subtree.FeeHash,
					"Nodes":            subtree.Nodes,
					"ConflictingNodes": subtree.ConflictingNodes,
				},
				Pagination: Pagination{
					Offset:       offset,
					Limit:        limit,
					TotalRecords: totalNodes,
				},
			}

			return c.JSONPretty(200, response, "  ")
		}

		if mode != BINARY_STREAM && mode != HEX {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("bad read mode").Error())
		}

		// Stream only the node hashes out of the serialized subtree file. This avoids
		// allocating 32 bytes for every node before the response can start.
		subtreeReader, err := h.repository.GetSubtreeNodeHashesReader(ctx, hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}
		defer subtreeReader.Close()

		if mode == BINARY_STREAM {
			return c.Stream(http.StatusOK, echo.MIMEOctetStream, subtreeReader)
		}

		// mode == HEX (validated above)
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextPlainCharsetUTF8)
		c.Response().WriteHeader(http.StatusOK)
		_, err = io.Copy(hex.NewEncoder(c.Response()), subtreeReader)
		return err
	}
}
