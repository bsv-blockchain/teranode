// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
package httpimpl

import (
	"net/http"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/labstack/echo/v4"
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

		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetTransactionMeta_http",
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

		// Fetch block hashes and subtree hashes
		// Note: We assume BlockIDs, SubtreeIdxs, and BlockHeights have the same length and correspond to each other
		h.logger.Debugf("Transaction %s has BlockIDs: %v, SubtreeIdxs: %v", hash.String(), meta.BlockIDs, meta.SubtreeIdxs)

		numEntries := len(meta.BlockIDs)
		if len(meta.SubtreeIdxs) != numEntries {
			h.logger.Warnf("Mismatch in array lengths: %d BlockIDs, %d SubtreeIdxs", numEntries, len(meta.SubtreeIdxs))
			// Use the minimum length to avoid index out of bounds
			if len(meta.SubtreeIdxs) < numEntries {
				numEntries = len(meta.SubtreeIdxs)
			}
		}

		blockHashes := make([]string, numEntries)
		subtreeHashes := make([]string, numEntries)

		for i := 0; i < numEntries; i++ {

			blockID := meta.BlockIDs[i]
			block, err := h.repository.GetBlockByID(ctx, uint64(blockID))
			if err != nil {
				h.logger.Warnf("Failed to get block for ID %d: %v", blockID, err)
				blockHashes[i] = ""
				subtreeHashes[i] = ""
			} else if block != nil && block.Header != nil {
				blockHashes[i] = block.Header.Hash().String()

				// Get the subtree hash using the subtree index
				subtreeIdx := meta.SubtreeIdxs[i]
				if subtreeIdx >= 0 && int(subtreeIdx) < len(block.Subtrees) && block.Subtrees[subtreeIdx] != nil {
					subtreeHashes[i] = block.Subtrees[subtreeIdx].String()
					h.logger.Debugf("Transaction in block %d (hash: %s), subtree index %d (hash: %s)",
						blockID, blockHashes[i], subtreeIdx, subtreeHashes[i])
				} else {
					h.logger.Warnf("Subtree index %d out of range for block %d (has %d subtrees)",
						subtreeIdx, blockID, len(block.Subtrees))
					subtreeHashes[i] = ""
				}
			}
		}

		// Create enhanced response with block hashes and subtree hashes
		response := map[string]interface{}{
			"tx":             meta.Tx,
			"parentTxHashes": meta.TxInpoints.ParentTxHashes,
			"blockIDs":       meta.BlockIDs,
			"blockHashes":    blockHashes,
			"blockHeights":   meta.BlockHeights,
			"subtreeIdxs":    meta.SubtreeIdxs,
			"subtreeHashes":  subtreeHashes,
			"fee":            meta.Fee,
			"sizeInBytes":    meta.SizeInBytes,
			"isCoinbase":     meta.IsCoinbase,
			"lockTime":       meta.LockTime,
		}

		prometheusAssetHTTPGetTransaction.WithLabelValues("OK", "200").Inc()

		switch mode {
		case JSON:
			return c.JSONPretty(200, response, "  ")
		default:
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("bad read mode").Error())
		}
	}
}
