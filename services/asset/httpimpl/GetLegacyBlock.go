package httpimpl

import (
	"net/http"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetLegacyBlock creates an HTTP handler that streams a block in the legacy Bitcoin protocol format.
//
// Parameters:
//   - c: Echo context containing the HTTP request and response
//
// URL Parameters:
//   - hash: Block hash (hex string)
//
// Returns:
//   - error: Any error encountered during processing
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/octet-stream
//	Body: Legacy format block data:
//	  - Magic number (4 bytes): 0xf9, 0xbe, 0xb4, 0xd9
//	  - Block size (4 bytes): little-endian uint32
//	  - Block header (80 bytes)
//	  - Transaction count (VarInt)
//	  - Transactions (variable length):
//	    * Coinbase transaction first
//	    * Followed by all other transactions if present
//
// Error Responses:
//   - 404 Not Found: Block not found
//   - 500 Internal Server Error:
//   - Invalid block hash format
//   - Block retrieval errors
//
// Monitoring:
//   - Prometheus metric "asset_http_get_block_legacy" tracks responses with status
//
// Example Usage:
//
//	GET /block/legacy/<hash>
func (h *HTTP) GetLegacyBlock() func(c echo.Context) error {
	return func(c echo.Context) error {
		hashStr := c.Param("hash")

		ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "GetLegacyBlock_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetLegacyBlock for %s: %s", c.Request().RemoteAddr, hashStr),
		)

		defer deferFn()

		if len(hashStr) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid block hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid block hash format", err).Error())
		}

		wireBlock := c.QueryParam("wire") != ""

		r, err := h.repository.GetLegacyBlockReader(ctx, hash, wireBlock)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				prometheusAssetHttpGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusNotFound)).Inc()
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				prometheusAssetHttpGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusAssetHttpGetBlockLegacy.WithLabelValues("OK", "200").Inc()

		return c.Stream(http.StatusOK, echo.MIMEOctetStream, r)
	}
}

// GetRestLegacyBlock creates an HTTP handler that streams a block in the legacy Bitcoin protocol format,
// using a ".bin" extension in the URL. Functionally identical to GetLegacyBlock but with different URL format.
//
// URL Parameters:
//   - hash: Block hash followed by ".bin" extension
//     Example: "000000...hash.bin"
//
// All other aspects (response format, errors, monitoring) are identical to GetLegacyBlock.
//
// Example Usage:
//
//	GET /block/legacy/<hash>.bin
func (h *HTTP) GetRestLegacyBlock() func(c echo.Context) error {
	return func(c echo.Context) error {
		resource := c.Param("hash.bin")
		if resource == "" {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid block hash extension").Error())
		}

		hashString := strings.Replace(resource, ".bin", "", 1)

		ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "GetRestLegacyBlock_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetBlockGetRestLegacyBlockByHash for %s: %s", c.Request().RemoteAddr, resource),
		)

		defer deferFn()

		if len(hashString) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid block hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(hashString)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid block hash string", err).Error())
		}

		r, err := h.repository.GetLegacyBlockReader(ctx, hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				prometheusAssetHttpGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusNotFound)).Inc()

				return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("block not found", err).Error())
			} else {
				prometheusAssetHttpGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()

				return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error getting block", err).Error())
			}
		}

		prometheusAssetHttpGetBlockLegacy.WithLabelValues("OK", "200").Inc()

		return c.Stream(http.StatusOK, echo.MIMEOctetStream, r)
	}
}
