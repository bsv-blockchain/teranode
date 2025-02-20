// Package httpimpl provides HTTP handlers for blockchain data retrieval,
// including batch block header operations.
package httpimpl

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetBlockHeaders creates an HTTP handler for retrieving multiple consecutive block headers
// starting from a specific block hash. It supports multiple response formats
// and pagination.
//
// Parameters:
//   - mode: ReadMode specifying the response format (JSON, BINARY_STREAM, or HEX)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// URL Parameters:
//   - hash: Starting block hash (hex string)
//
// Query Parameters:
//   - n: Number of headers to retrieve (default: 100, max: 1000)
//     Example: ?n=50
//
// HTTP Response Formats:
//
//  1. JSON (mode = JSON):
//     Status: 200 OK
//     Content-Type: application/json
//     Body: Array of block headers:
//     [
//     {
//     "version": <uint32>,             // Block version
//     "hash_prev_block": "<string>",   // Previous block hash
//     "hash_merkle_root": "<string>",  // Merkle root hash
//     "timestamp": <uint32>,           // Block creation time
//     "bits": "<string>",              // 4-byte difficulty target (little-endian)
//     "nonce": <uint32>,               // PoW nonce
//     "hash": "<string>",              // Current block hash
//     "height": <uint32>               // Block height
//     },
//     // ... additional headers
//     ]
//
//  2. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: Concatenated 80-byte block headers, each containing:
//     - Version (4 bytes, little-endian uint32)
//     - Previous block hash (32 bytes)
//     - Merkle root (32 bytes)
//     - Timestamp (4 bytes, little-endian uint32)
//     - Bits (4 bytes, little-endian NBit)
//     - Nonce (4 bytes, little-endian uint32)
//     Total size = n * 80 bytes
//
//  3. Hex (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hexadecimal string of concatenated block headers
//     String length = n * 160 characters (80 bytes * 2)
//
// Error Responses:
//
//   - 404 Not Found:
//
//   - Starting block not found
//
//   - Headers not found
//
//   - 500 Internal Server Error:
//
//   - Invalid hash format
//
//   - Repository errors
//
//   - Invalid read mode
//
// Monitoring:
//   - Execution time recorded in "GetBlockHeaders_http" statistic
//   - Prometheus metric "asset_http_get_block_header" tracks successful responses
//   - Debug logging of request and completion
//
// Example Usage:
//
//	# Get 50 headers in JSON format
//	GET /block/headers/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f?n=50
//
//	# Get 100 headers in binary format
//	GET /block/headers/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f/raw
//
//	# Get 25 headers in hex format
//	GET /block/headers/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f/hex?n=25
//
// Notes:
//   - Headers are returned in consecutive order starting from the specified hash
//   - Binary response size can be calculated as n * 80 bytes
//   - Hex response size can be calculated as n * 160 characters
//   - Default limit of 100 headers can be adjusted via 'n' parameter
//   - Maximum limit of 1000 headers per request
func (h *HTTP) GetBlockHeaders(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		hashStr := c.Param("hash")
		nStr := c.QueryParam("n")

		ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "GetBlockHeaders_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithLogMessage(h.logger, "[GetBlockHeaders_http] Get %s Block Headers in %s for %s", mode, c.Request().RemoteAddr, hashStr),
		)

		defer deferFn()

		if len(hashStr) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash string", err).Error())
		}

		numberOfHeaders := 100
		if nStr != "" {
			numberOfHeaders, err = strconv.Atoi(nStr)
			if err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid number of headers").Error())
			}

			if numberOfHeaders == 0 {
				numberOfHeaders = 100
			}

			if numberOfHeaders > 1000 {
				numberOfHeaders = 1000
			}
		}

		var (
			headers     []*model.BlockHeader
			headerMetas []*model.BlockHeaderMeta
		)

		numberOfHeadersUint64, err := util.SafeIntToUint64(numberOfHeaders)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid number of headers", err).Error())
		}

		headers, headerMetas, err = h.repository.GetBlockHeaders(ctx, hash, numberOfHeadersUint64)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusAssetHttpGetBlockHeader.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			headerResponses := make([]*blockHeaderResponse, 0, len(headers))
			for idx, header := range headers {
				headerResponses = append(headerResponses, &blockHeaderResponse{
					BlockHeader: header,
					Hash:        header.String(),
					Height:      headerMetas[idx].Height,
					TxCount:     headerMetas[idx].TxCount,
					SizeInBytes: headerMetas[idx].SizeInBytes,
					Miner:       headerMetas[idx].Miner,
				})
			}

			return c.JSONPretty(200, headerResponses, "  ")
		}

		bytes := make([]byte, 0, len(headers)*model.BlockHeaderSize)

		for _, header := range headers {
			bytes = append(bytes, header.Bytes()...)
		}

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, bytes)
		case HEX:
			return c.String(200, hex.EncodeToString(bytes))
		default:
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("bad read mode").Error())
		}
	}
}
