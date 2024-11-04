// Package http_impl provides HTTP handlers for the blockchain service API endpoints.
package http_impl

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
)

// GetBalance handles HTTP GET requests to retrieve the current balance information.
// It returns the number of UTXOs and their total value in satoshis.
//
// The handler:
//  1. Tracks execution time for performance monitoring
//  2. Retrieves balance information from the repository
//  3. Returns a JSON response with balance details
//
// Parameters:
//   - c: Echo context containing the HTTP request and response
//
// Returns:
//   - error: Any error encountered during processing
//
// HTTP Response:
//   - Status: 200 OK on success
//   - Body: JSON object containing:
//     {
//     "numberOfUtxos": <number>,
//     "totalSatoshis": <number>
//     }
//
// Error Responses:
//   - 500 Internal Server Error: If balance retrieval fails
//
// Example Response:
//
//	{
//	  "numberOfUtxos": 42,
//	  "totalSatoshis": 1000000000
//	}
//
// Usage:
//
//	GET /balance
//
// Timing:
//   - Execution time is recorded in the "GetBalance_http" statistic
func (h *HTTP) GetBalance(c echo.Context) error {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetBalance_http").AddTime(start)
	}()

	numberOfUtxos, totalSatoshis, err := h.repository.GetBalance(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	type balance struct {
		NumberOfUtxos uint64 `json:"numberOfUtxos"`
		TotalSatoshis uint64 `json:"totalSatoshis"`
	}

	return c.JSONPretty(200, balance{
		NumberOfUtxos: numberOfUtxos,
		TotalSatoshis: totalSatoshis,
	}, "  ")

}
