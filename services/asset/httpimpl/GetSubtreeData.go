package httpimpl

import (
	"net/http"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

// GetSubtreeData creates an HTTP handler that streams all transactions of a subtree
//
// Parameters:
//   - c: Echo context containing the HTTP request and response
//
// URL Parameters:
//   - hash: Subtree hash (hex string)
//
// Returns:
//   - error: Any error encountered during processing
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/octet-stream
//	Body:
//	  - Concatenated binary transaction data
//
// Error Responses:
//   - 404 Not Found: Subtree not found
//   - 500 Internal Server Error:
//   - Invalid subtree hash format
//   - Subtree retrieval errors
//
// Monitoring:
//   - Prometheus metric "asset_http_get_subtree_data" tracks responses with status
//
// Example Usage:
//
//	GET /subtree_data/<hash>
func (h *HTTP) GetSubtreeData() func(c echo.Context) error {
	return func(c echo.Context) error {
		hashStr := c.Param("hash")

		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetSubtreeData_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetSubtreeData for %s: %s", c.Request().RemoteAddr, hashStr),
		)

		defer deferFn()

		if len(hashStr) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid subtree hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid subtree hash format", err).Error())
		}

		r, err := h.repository.GetSubtreeDataReader(ctx, hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				prometheusAssetHTTPGetSubtreeData.WithLabelValues("ERROR", http.StatusText(http.StatusNotFound)).Inc()
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				prometheusAssetHTTPGetSubtreeData.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}
		defer r.Close()

		prometheusAssetHTTPGetSubtreeData.WithLabelValues("OK", "200").Inc()

		return c.Stream(http.StatusOK, echo.MIMEOctetStream, r)
	}
}
