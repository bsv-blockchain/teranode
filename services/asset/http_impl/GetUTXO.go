// Package http_impl provides HTTP handlers for blockchain data retrieval and analysis.
package http_impl

import (
	"net/http"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

// GetUTXO creates an HTTP handler for retrieving unspent transaction output (UTXO) information.
// Supports multiple response formats.
//
// Parameters:
//   - mode: ReadMode specifying the response format (JSON, BINARY_STREAM, or HEX)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// URL Parameters:
//   - hash: UTXO hash (hex string)
//
// HTTP Response Formats:
//
//  1. JSON (mode = JSON):
//     Status: 200 OK
//     Content-Type: application/json
//     Body:
//     {
//     "status": <int>,                    // Status code
//     "spendingTxId": "<string>",         // Hash of spending transaction (if spent)
//     "lockTime": <uint32>                // Optional lock time
//     }
//
//  2. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: Raw bytes of spending transaction ID
//
//  3. Hex (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hex string of spending transaction ID
//
// Error Responses:
//
//   - 404 Not Found:
//
//   - UTXO not found
//
//   - UTXO status is NOT_FOUND
//     Example: {"message": "UTXO not found"}
//
//   - 500 Internal Server Error:
//
//   - Invalid UTXO hash format
//
//   - Repository errors
//
//   - Invalid read mode
//
// Monitoring:
//   - Execution time recorded in "GetUTXO_http" statistic
//   - Prometheus metric "asset_http_get_utxo" tracks successful responses
//   - Debug logging of request handling
//
// Example Usage:
//
//	# Get UTXO info in JSON format
//	GET /utxo/<hash>
//
//	# Get spending transaction ID in binary format
//	GET /utxo/<hash>/raw
//
//	# Get spending transaction ID in hex format
//	GET /utxo/<hash>/hex
func (h *HTTP) GetUTXO(mode ReadMode) func(c echo.Context) error {

	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetUTXO_http").AddTime(start)
		}()

		h.logger.Debugf("[Asset_http] GetUTXO in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		utxoResponse, err := h.repository.GetUtxo(c.Request().Context(), &utxo.Spend{
			TxID:         nil,
			Vout:         0,
			UTXOHash:     hash,
			SpendingTxID: nil,
		})
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		if utxoResponse == nil || utxoResponse.Status == int(utxo.Status_NOT_FOUND) {
			return echo.NewHTTPError(http.StatusNotFound, "UTXO not found")
		}

		prometheusAssetHttpGetUTXO.WithLabelValues("OK", "200").Inc()

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, utxoResponse.SpendingTxID.CloneBytes())
		case HEX:
			return c.String(200, utxoResponse.SpendingTxID.String())
		case JSON:
			return c.JSONPretty(200, utxoResponse, "  ")
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
