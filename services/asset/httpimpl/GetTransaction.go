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
// The transaction data is retrieved from either the UTXO store or transaction store,
// providing a unified interface for transaction retrieval regardless of its storage location.
//
// This handler function implements content negotiation through the ReadMode parameter,
// allowing clients to request the same transaction in different representation formats
// without requiring separate API endpoints. It supports standard Bitcoin transaction formats
// including raw binary, hexadecimal, and structured JSON representations.
//
// The implementation includes proper error handling for various failure scenarios,
// translating internal errors to appropriate HTTP status codes and messages.
// It also implements optional response signing for data verification when configured.
//
// Performance considerations:
// - Binary mode provides the most efficient transfer for large transactions
// - JSON mode provides human-readable format at the cost of increased size and parsing overhead
// - Hex mode strikes a balance between human-readability and parsing simplicity
//
// Parameters:
//   - mode: ReadMode specifying the response format (JSON, BINARY_STREAM, or HEX)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function that processes the HTTP request
//
// URL Parameters:
//   - hash: Transaction hash (64-character hex string)
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
//     "previousTxId": "<string>",  // Hash of the transaction containing the output being spent
//     "previousTxIndex": <uint32>,  // Output index in the previous transaction
//     "unlockingScript": "<string>", // Script that satisfies the conditions of the output's locking script
//     "sequenceNumber": <uint32>    // Sequence number for the input
//     }
//     ],
//     "outputs": [                    // Array of transaction outputs
//     {
//     "satoshis": <uint64>,         // Value of the output in satoshis
//     "lockingScript": "<string>"  // Script specifying the conditions to spend this output
//     }
//     ],
//     "version": <uint32>,            // Transaction version number
//     "locktime": <uint32>            // The block height or timestamp when transaction is final
//     }
//
//  2. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: Raw Bitcoin transaction format:
//     - Version (4 bytes): Transaction version number as a little-endian uint32
//     - Input count (VarInt): Number of inputs in the transaction
//     - Inputs (variable length): Each consisting of:
//       * Previous transaction hash (32 bytes, little-endian)
//       * Previous output index (4 bytes, little-endian uint32)
//       * Script length (VarInt)
//       * Unlocking script (variable length)
//       * Sequence number (4 bytes, little-endian uint32)
//     - Output count (VarInt): Number of outputs in the transaction
//     - Outputs (variable length): Each consisting of:
//       * Value (8 bytes, little-endian uint64, in satoshis)
//       * Script length (VarInt)
//       * Locking script (variable length)
//     - Locktime (4 bytes): Transaction locktime as a little-endian uint32
//
//  3. Hex (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hexadecimal string representation of the raw binary transaction format
//           Each byte is encoded as two hex characters (0-9, a-f)
//
// Error Responses:
//
//   - 400 Bad Request:
//     Returned when the transaction hash is invalid (wrong format or length)
//     Example: {"message": "invalid hash length"}
//     Example: {"message": "invalid hash string"}
//
//   - 404 Not Found:
//     Returned when the transaction doesn't exist in any of the storage systems
//     Example: {"message": "not found"}
//
//   - 500 Internal Server Error:
//     Returned for various internal errors:
//     - Repository access failures
//     - Transaction deserialization errors for JSON mode
//     - Storage system unavailability
//     Example: {"message": "error parsing transaction"}
//
// Security Considerations:
//   - Response includes cryptographic signature if private key is configured
//   - Transaction hashes are validated for length and format before processing
//   - Raw binary responses should be handled carefully by clients to prevent buffer overflows
//
// Monitoring and Metrics:
//   - Execution time recorded in "GetTransaction_http" statistic for performance tracking
//   - Prometheus metric "asset_http_get_transaction" tracks response counts by status
//   - Request logging includes client IP and requested transaction hash
//
// Example Usage:
//
//   # Get transaction in JSON format (for human-readable exploration)
//   GET /tx/{hash}/json
//
//   # Get raw binary transaction (for efficient machine processing)
//   GET /tx/{hash}
//
//   # Get transaction in hex format (for verification or debugging)
//   GET /tx/{hash}/hex
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
