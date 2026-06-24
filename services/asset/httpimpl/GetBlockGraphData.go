// Package httpimpl provides HTTP handlers for blockchain data retrieval and visualization.
package httpimpl

import (
	"math"
	"net/http"
	"sort"
	"time"

	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

const (
	bucket1h  = int64(3600)
	bucket6h  = int64(21600)
	bucket1d  = int64(86400)
	bucket1w  = int64(604800)
	bucket30d = int64(2592000)
)

// pickBucketSeconds returns the bucket size in seconds for a given time range.
// Returns 0 (no bucketing) for ranges ≤ 24h.
func pickBucketSeconds(rangeSeconds int64) int64 {
	switch {
	case rangeSeconds <= 86400: // ≤ 24h: per-block resolution
		return 0
	case rangeSeconds <= 604800: // ≤ 1w: 1h buckets (~168)
		return bucket1h
	case rangeSeconds <= 7776000: // ≤ 3m (90d): 6h buckets (~360)
		return bucket6h
	case rangeSeconds <= 31536000: // ≤ 1y: 1d buckets (~365)
		return bucket1d
	case rangeSeconds <= 157680000: // ≤ 5y: 1w buckets (~260)
		return bucket1w
	default: // > 5y: 30d buckets
		return bucket30d
	}
}

// aggregateDataPoints groups data points into fixed-size time buckets, summing TxCount per bucket.
// Returns the input unchanged if bucketSeconds is 0 or the input is empty.
// Output is always sorted by timestamp ASC regardless of input order.
// The input is not modified.
func aggregateDataPoints(in *model.BlockDataPoints, bucketSeconds int64) *model.BlockDataPoints {
	if bucketSeconds == 0 || len(in.DataPoints) == 0 {
		return in
	}
	// Copy the slice before sorting so the caller's order is not disturbed.
	sorted := make([]*model.DataPoint, len(in.DataPoints))
	copy(sorted, in.DataPoints)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})
	bs := uint64(bucketSeconds)
	buckets := make(map[uint32]uint64)
	order := make([]uint32, 0)
	for _, dp := range sorted {
		// uint64 arithmetic avoids overflow on uint32 timestamps near year 2106.
		b := uint32((uint64(dp.Timestamp) / bs) * bs)
		if _, ok := buckets[b]; !ok {
			order = append(order, b)
		}
		buckets[b] += dp.TxCount
	}
	out := &model.BlockDataPoints{
		DataPoints: make([]*model.DataPoint, 0, len(order)),
	}
	for _, b := range order {
		out.DataPoints = append(out.DataPoints, &model.DataPoint{
			Timestamp: b,
			TxCount:   buckets[b],
		})
	}
	return out
}

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
//   - "all" - All available data
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
	ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetBlockGraphData_http",
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
	case "all":
		periodMillis = 0
	default:
		return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("a valid period is required").Error())
	}

	periodMillisUint64, err := safeconversion.Int64ToUint64(periodMillis)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid period parameter", err).Error())
	}

	dataPoints, err := h.repository.GetBlockGraphData(ctx, periodMillisUint64)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	if len(dataPoints.DataPoints) > 1 {
		var minTS uint32 = math.MaxUint32
		var maxTS uint32
		for _, dp := range dataPoints.DataPoints {
			if dp.Timestamp < minTS {
				minTS = dp.Timestamp
			}
			if dp.Timestamp > maxTS {
				maxTS = dp.Timestamp
			}
		}
		rangeSeconds := int64(maxTS - minTS)
		dataPoints = aggregateDataPoints(dataPoints, pickBucketSeconds(rangeSeconds))
	}

	return c.JSONPretty(200, dataPoints, "  ")
}
