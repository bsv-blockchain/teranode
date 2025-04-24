// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
package httpimpl

import (
	"net/http"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetTransactionMeta creates an HTTP handler for retrieving transaction metadata.
// Only supports JSON response format.
//
// Parameters:
//   - mode: ReadMode (only JSON mode is supported)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// URL Parameters:
//   - hash: Transaction hash (hex string)
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/json
//	Body: Transaction metadata:
//	  {
//	    "tx": {                       // Complete transaction data
//	      // see bt.Tx structure
//	    },
//	    "parentTxHashes": [           // Array of parent transaction hashes
//	      "<hash>", ...
//	    ],
//	    "blockIDs": [                 // Array of block IDs containing this transaction
//	      <uint32>, ...
//	    ],
//	    "fee": <uint64>,             // Transaction fee in satoshis
//	    "sizeInBytes": <uint64>,     // Transaction size in bytes
//	    "isCoinbase": <boolean>,     // Whether this is a coinbase transaction
//	    "lockTime": <uint32>         // Transaction lock time
//	  }
//
// Error Responses:
//
//   - 404 Not Found:
//
//   - Transaction metadata not found
//     Example: {"message": "not found"}
//
//   - 500 Internal Server Error:
//
//   - Invalid transaction hash format
//
//   - UTXO store errors
//
//   - Invalid read mode (non-JSON)
//
// Monitoring:
//   - Execution time recorded in "GetTransactionMeta_http" statistic
//   - Prometheus metric "asset_http_get_transaction" tracks responses
//   - Debug logging of request handling
//
// Example Usage:
//
//	# Get transaction metadata
//	GET /tx/meta/<txid>
//
// Notes:
//   - Only JSON format is supported
//   - Metadata is retrieved from UTXO store
//   - Includes complete transaction data along with metadata
func (h *HTTP) GetTransactionMeta(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		hashStr := c.Param("hash")

		ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "GetTransactionMeta_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetTransactionMeta in %s for %s: %s", mode, c.Request().RemoteAddr, hashStr),
		)

		defer deferFn()

		if len(hashStr) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash string").Error())
		}

		meta, err := h.repository.GetTxMeta(ctx, hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusAssetHTTPGetTransaction.WithLabelValues("OK", "200").Inc()

		switch mode {
		case JSON:
			return c.JSONPretty(200, meta, "  ")
		default:
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("bad read mode").Error())
		}
	}
}
