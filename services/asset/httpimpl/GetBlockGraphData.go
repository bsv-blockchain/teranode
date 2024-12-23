// Package httpimpl provides HTTP handlers for blockchain data retrieval and visualization.
package httpimpl

import (
	"net/http"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/labstack/echo/v4"
)

// GetBlockGraphData retrieves time-series data points showing transaction count
// over time. It supports various time periods for data aggregation.
//
// Parameters:
//   - c: Echo context containing the HTTP request and response
//
// URL Parameters:
//   - period: Time range for data aggregation. Supported values:
//   - "2h"  - Last 2 hours
//   - "6h"  - Last 6 hours
//   - "12h" - Last 12 hours
//   - "24h" - Last 24 hours
//   - "1w"  - Last week
//   - "1m"  - Last month (30 days)
//   - "3m"  - Last 3 months (90 days)
//
// Returns:
//   - error: Any error encountered during processing
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/json
//	Body: Array of time-series data points:
//	  {
//	    "data_points": [
//	      {
//	        "timestamp": <uint32>,  // Unix timestamp
//	        "tx_count": <uint64>    // Number of transactions
//	      },
//	      // ... additional data points
//	    ]
//	  }
//
// Error Responses:
//
//   - 400 Bad Request:
//
//   - Invalid or missing period parameter
//     Example: {"message": "period is required"}
//
//   - 500 Internal Server Error:
//
//   - Repository errors
//     Example: {"message": "internal server error"}
//
// Monitoring:
//   - Execution time recorded in "GetBlockGraphData_http" statistic
//
// Example Usage:
//
//	# Get transaction data for last 24 hours
//	GET /blocks/graph/24h
//
//	# Get transaction data for last week
//	GET /blocks/graph/1w
func (h *HTTP) GetBlockGraphData(c echo.Context) error {
	ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "GetBlockGraphData_http",
		tracing.WithParentStat(AssetStat),
	)

	defer deferFn()

	periodMillis := int64(0)
	switch c.Param("period") {
	case "2h":
		periodMillis = time.Now().Add(-2*time.Hour).UnixNano() / int64(time.Millisecond)
	case "6h":
		periodMillis = time.Now().Add(-6*time.Hour).UnixNano() / int64(time.Millisecond)
	case "12h":
		periodMillis = time.Now().Add(-12*time.Hour).UnixNano() / int64(time.Millisecond)
	case "24h":
		periodMillis = time.Now().Add(-24*time.Hour).UnixNano() / int64(time.Millisecond)
	case "1w":
		periodMillis = time.Now().Add(-7*24*time.Hour).UnixNano() / int64(time.Millisecond)
	case "1m":
		periodMillis = time.Now().Add(-30*24*time.Hour).UnixNano() / int64(time.Millisecond)
	case "3m":
		periodMillis = time.Now().Add(-90*24*time.Hour).UnixNano() / int64(time.Millisecond)
	default:
		return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("a valid period is required").Error())
	}

	//nolint:gosec
	dataPoints, err := h.repository.GetBlockGraphData(ctx, uint64(periodMillis))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSONPretty(200, dataPoints, "  ")
}
