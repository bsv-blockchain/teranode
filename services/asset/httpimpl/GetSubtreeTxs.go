// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
package httpimpl

import (
	"net/http"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type SubtreeTx struct {
	Index        int    `json:"index"`
	TxID         string `json:"txid"`
	InputsCount  int    `json:"inputsCount"`
	OutputsCount int    `json:"outputsCount"`
	Size         int    `json:"size"`
	Fee          int    `json:"fee"`
}

// GetSubtreeTxs creates an HTTP handler for retrieving transaction details from a subtree
// with pagination support. Only supports JSON output format.
//
// Parameters:
//   - mode: ReadMode (only JSON mode is supported)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// URL Parameters:
//   - hash: Subtree hash (hex string)
//
// Query Parameters:
//
//   - offset: Number of transactions to skip (default: 0)
//     Example: ?offset=100
//
//   - limit: Maximum number of transactions to return (default: 20, max: 100)
//     Example: ?limit=50
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/json
//	Body:
//	  {
//	    "data": [
//	      {
//	        "index": <int>,         // Position in subtree
//	        "txid": "<string>",     // Transaction ID
//	        "inputsCount": <int>,   // Number of inputs
//	        "outputsCount": <int>,  // Number of outputs
//	        "size": <int>,          // Transaction size in bytes
//	        "fee": <int>            // Transaction fee
//	      },
//	      // ... additional transactions
//	    ],
//	    "pagination": {
//	      "offset": <int>,          // Current offset
//	      "limit": <int>,           // Current limit
//	      "total_records": <int>    // Total number of transactions in subtree
//	    }
//	  }
//
// Error Responses:
//
//   - 400 Bad Request:
//
//   - Invalid pagination parameters
//     Example: {"message": "invalid offset or limit"}
//
//   - 404 Not Found:
//
//   - Subtree not found
//     Example: {"message": "not found"}
//
//   - 500 Internal Server Error:
//
//   - Invalid subtree hash format
//
//   - Subtree retrieval errors
//
//   - Invalid read mode (non-JSON)
//
// Monitoring:
//   - Execution time recorded in "GetSubtree_http" statistic
//   - Prometheus metric "asset_http_get_subtree" tracks responses
//   - Performance logging including transfer speed (KB/sec)
//   - Response size logging in KB
//
// Notes:
//   - Only JSON format is supported
//   - Missing transactions are skipped in the response
//   - Coinbase transactions are marked with IsCoinbase flag
//   - Response is pretty-printed for readability
//   - Transaction metadata is fetched individually for each transaction
//
// Example Usage:
//
//	# Get first 20 transactions from subtree
//	GET /subtree/txs/<hash>
//
//	# Get 50 transactions starting at offset 100
//	GET /subtree/txs/<hash>?offset=100&limit=50
func (h *HTTP) GetSubtreeTxs(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		hashStr := c.Param("hash")

		ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "GetSubtree_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetSubtree in %s for %s: %s", mode, c.Request().RemoteAddr, hashStr),
		)

		defer deferFn()

		if len(hashStr) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash string", err).Error())
		}

		offset, limit, err := h.getLimitOffset(c)
		if err != nil {
			// Invalid offset or limit already returns an http error
			return err
		}

		prometheusAssetHttpGetSubtree.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			// get subtree is much less efficient than get subtree reader and then only deserializing the nodes
			// this is only needed for the json response
			subtree, err := h.repository.GetSubtree(ctx, hash)
			if err != nil {
				if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
					return echo.NewHTTPError(http.StatusNotFound, err.Error())
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}

			data := make([]SubtreeTx, 0, limit)

			var txMeta *meta.Data

			for i := offset; i < offset+limit; i++ {
				if i >= subtree.Length() {
					break
				}

				node := subtree.Nodes[i]

				subtreeData := SubtreeTx{
					Index: i,
					TxID:  node.Hash.String(),
				}

				if util.CoinbasePlaceholderHash.Equal(node.Hash) {
					txMeta = &meta.Data{
						Tx:         bt.NewTx(),
						IsCoinbase: true,
					}
					txMeta.Tx.SetTxHash(util.CoinbasePlaceholderHash)
				} else {
					txMeta, err = h.repository.GetTransactionMeta(ctx, &node.Hash)
					if err != nil {
						// NewTxNotFoundError
						if errors.Is(err, errors.ErrTxNotFound) {
							h.logger.Infof("[GetSubtreeTxs][%s] not found in utxo store", node.Hash.String())
						} else {
							h.logger.Warnf("[GetSubtreeTxs][%s] error getting transaction meta: %s", node.Hash.String(), err.Error())
						}

						continue
					}

					if txMeta == nil {
						h.logger.Warnf("[GetSubtreeTxs][%s] txMeta is nil", node.Hash.String())
						continue
					}

					if txMeta.Tx == nil {
						h.logger.Warnf("[GetSubtreeTxs][%s] txMeta.Tx is nil", node.Hash.String())
						continue
					}

					subtreeData.InputsCount = len(txMeta.Tx.Inputs)
					subtreeData.OutputsCount = len(txMeta.Tx.Outputs)
					subtreeData.Size = int(txMeta.SizeInBytes)
					subtreeData.Fee = int(txMeta.Fee)
				}

				data = append(data, subtreeData)
			}

			response := ExtendedResponse{
				Data: data,
				Pagination: Pagination{
					Offset:       offset,
					Limit:        limit,
					TotalRecords: subtree.Length(),
				},
			}

			return c.JSONPretty(200, response, "  ")
		}

		return echo.NewHTTPError(http.StatusInternalServerError, errors.NewInvalidArgumentError("bad read mode").Error())
	}
}
