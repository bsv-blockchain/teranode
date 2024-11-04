// Package http_impl provides HTTP handlers for blockchain data retrieval and analysis.
package http_impl

import (
	"net/http"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
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
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetTransactionMeta_http").AddTime(start)
		}()

		h.logger.Debugf("[Asset_http] GetTransactionMeta in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		meta, err := h.repository.UtxoStore.Get(c.Request().Context(), hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusAssetHttpGetTransaction.WithLabelValues("OK", "200").Inc()

		switch mode {
		case JSON:
			return c.JSONPretty(200, meta, "  ")
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
