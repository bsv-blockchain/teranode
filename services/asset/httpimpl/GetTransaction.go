package httpimpl

import (
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetTransaction creates an HTTP handler for retrieving transaction data in multiple formats.
// The transaction data is retrieved from either the UTXO store or transaction store.
//
// Parameters:
//   - mode: ReadMode specifying the response format (JSON, BINARY_STREAM, or HEX)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// URL Parameters:
//   - hash: Transaction hash (hex string)
//
// HTTP Response Formats:
//
//  1. JSON (mode = JSON):
//     Status: 200 OK
//     Content-Type: application/json
//     Body: Bitcoin transaction:
//     {
//     "inputs": [                     // Array of transaction inputs
//     {
//     "previousTxId": "<string>",
//     "previousTxIndex": <uint32>,
//     "unlockingScript": "<string>",
//     "sequenceNumber": <uint32>
//     }
//     ],
//     "outputs": [                    // Array of transaction outputs
//     {
//     "satoshis": <uint64>,
//     "lockingScript": "<string>"
//     }
//     ],
//     "version": <uint32>,            // Transaction version
//     "locktime": <uint32>            // Transaction locktime
//     }
//
//  2. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: Raw Bitcoin transaction format:
//     - Version (4 bytes)
//     - Input count (VarInt)
//     - Inputs (variable length)
//     - Output count (VarInt)
//     - Outputs (variable length)
//     - Locktime (4 bytes)
//
//  3. Hex (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hexadecimal string of the binary format
//
// Error Responses:
//
//   - 404 Not Found:
//
//   - Transaction not found
//     Example: {"message": "not found"}
//
//   - 500 Internal Server Error:
//
//   - Invalid transaction hash
//
//   - Repository errors
//
//   - Invalid read mode
//
//   - Transaction parsing errors
//
// Security:
//   - Response includes cryptographic signature if private key is configured
//
// Monitoring:
//   - Execution time recorded in "GetTransaction_http" statistic
//   - Prometheus metric "asset_http_get_transaction" tracks responses
//
// Example Usage:
//
//	# Get transaction in JSON format
//	GET /tx/hash/<txid>
//
//	# Get raw transaction
//	GET /tx/hash/<txid>/raw
//
//	# Get transaction in hex format
//	GET /tx/hash/<txid>/hex
func (h *HTTP) GetTransaction(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		hashStr := c.Param("hash")

		ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "GetTransaction_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithLogMessage(h.logger, "[Asset_http] GetTransaction in %s for %s: %s", mode, c.Request().RemoteAddr, hashStr),
		)

		defer deferFn()

		if len(hashStr) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash string", err).Error())
		}

		b, err := h.repository.GetTransaction(ctx, hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		// sign the response, if the private key is set, ignore error
		// do this before any output is sent to the client, this adds a signature to the response header
		_ = h.Sign(c.Response(), hash.CloneBytes())

		prometheusAssetHTTPGetTransaction.WithLabelValues("OK", "200").Inc()

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, b)

		case HEX:
			return c.String(200, hex.EncodeToString(b))

		case JSON:
			tx, err := bt.NewTxFromBytes(b)
			if err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error parsing transaction", err).Error())
			}

			return c.JSONPretty(200, tx, "  ")

		default:
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("bad read mode").Error())
		}
	}
}
