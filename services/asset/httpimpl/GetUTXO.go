// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
package httpimpl

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
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
		hashStr := c.Param("hash")
		voutStr := c.QueryParam("vout")

		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetUTXO_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetUTXO in %s for %s: %s?vout=%s", mode, c.Request().RemoteAddr, hashStr, voutStr),
		)

		defer deferFn()

		if len(hashStr) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash length").Error())
		}

		txHash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash format", err).Error())
		}

		if voutStr == "" {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("missing required query parameter: vout").Error())
		}

		vout, err := strconv.ParseUint(voutStr, 10, 32)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid vout format", err).Error())
		}

		voutUint32, err := safeconversion.Uint64ToUint32(vout)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("vout out of range", err).Error())
		}

		// Fetch the transaction to get the output for UTXO hash calculation
		txBytes, err := h.repository.GetTransaction(ctx, txHash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || errors.Is(err, errors.ErrTxNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("transaction not found").Error())
			}

			return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error retrieving transaction", err).Error())
		}

		tx, err := bt.NewTxFromBytes(txBytes)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error parsing transaction", err).Error())
		}

		if voutUint32 >= uint32(len(tx.Outputs)) {
			return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("UTXO not found: output index out of range").Error())
		}

		output := tx.Outputs[voutUint32]
		utxoHash, err := util.UTXOHashFromOutput(txHash, output, voutUint32)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error calculating UTXO hash", err).Error())
		}

		utxoResponse, err := h.repository.GetUtxo(ctx, &utxo.Spend{
			TxID:         txHash,
			Vout:         voutUint32,
			UTXOHash:     utxoHash,
			SpendingData: nil,
		})
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		if utxoResponse == nil || utxoResponse.Status == int(utxo.Status_NOT_FOUND) {
			return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("UTXO not found").Error())
		}

		prometheusAssetHTTPGetUTXO.WithLabelValues("OK", "200").Inc()

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, utxoResponse.Bytes())
		case HEX:
			return c.String(200, hex.EncodeToString(utxoResponse.Bytes()))
		case JSON:
			return c.JSONPretty(200, utxoResponse, "  ")
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, errors.NewInvalidArgumentError("bad read mode").Error())
		}
	}
}
