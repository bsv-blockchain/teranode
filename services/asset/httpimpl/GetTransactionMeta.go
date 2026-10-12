// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
package httpimpl

import (
	"net/http"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
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
//	    "tx": {                       // Complete transaction data (see bt.Tx)
//	      // ...
//	    },
//	    "parentTxHashes": [           // Array of parent transaction hashes
//	      "<hash>", ...
//	    ],
//	    "blockIDs": [                 // Internal block IDs containing this tx
//	      <uint32>, ...
//	    ],
//	    "blockHashes": [              // Block hash for each entry
//	      "<hex>", ...
//	    ],
//	    "blockHeights": [             // Block height for each entry
//	      <uint32>, ...
//	    ],
//	    "subtreeIdxs": [              // Subtree index within each block
//	      <int>, ...
//	    ],
//	    "subtreeHashes": [            // Subtree hash for each entry
//	      "<hex>", ...
//	    ],
//	    "mainChainIndex": <int>,      // Index into the parallel arrays for the
//	                                   // main-chain entry, or -1 if the tx is only
//	                                   // in orphans/mempool. -1 is NOT authoritative:
//	                                   // a transient blockchain-client error during
//	                                   // the per-block check also yields -1, so it
//	                                   // means "not on main chain or undetermined".
//	    "fee": <uint64>,              // Transaction fee in satoshis
//	    "sizeInBytes": <uint64>,      // Transaction size in bytes
//	    "isCoinbase": <boolean>,      // Whether this is a coinbase transaction
//	    "lockTime": <uint32>          // Transaction lock time
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
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetTransactionMeta in %s for %s: %s", mode, c.RealIP(), hashStr),
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

		// The store can return no metadata and no error — the aerospike batch
		// path does exactly that for the coinbase placeholder hash. Treat it as
		// not-found rather than dereferencing nil below.
		if meta == nil {
			return echo.NewHTTPError(http.StatusNotFound, errors.NewTxNotFoundError("%s not found", hash.String()).Error())
		}

		// Fetch block hashes and subtree hashes
		// Note: We assume BlockIDs, SubtreeIdxs, and BlockHeights have the same length and correspond to each other
		h.logger.Debugf("Transaction %s has BlockIDs: %v, SubtreeIdxs: %v", hash.String(), meta.BlockIDs, meta.SubtreeIdxs)

		// The three arrays are parallel: index i across BlockIDs, SubtreeIdxs and
		// BlockHeights describes one (block, subtree) placement. A length mismatch
		// means the stored tx meta is corrupt — fail loudly rather than serving a
		// silently-truncated response built from misaligned data. (Matching the
		// guard in merkleproof.ConstructMerkleProof.)
		numEntries := len(meta.BlockIDs)
		if len(meta.SubtreeIdxs) != numEntries || len(meta.BlockHeights) != numEntries {
			h.logger.Errorf("Corrupt tx meta for %s: %d BlockIDs, %d SubtreeIdxs, %d BlockHeights",
				hash.String(), numEntries, len(meta.SubtreeIdxs), len(meta.BlockHeights))
			return echo.NewHTTPError(http.StatusInternalServerError,
				errors.NewProcessingError("malformed tx meta: parallel arrays length mismatch").Error())
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

		// Per-block check (not batched) because we need the index of the main-chain
		// entry, which CheckBlockIsInCurrentChain's batched form does not expose.
		// Backed by mainChainCache to avoid a gRPC round-trip per request when wired
		// (production path); falls back to direct gRPC when the cache is unavailable
		// (e.g. unit tests that construct HTTP without going through New()).
		mainChainIndex := -1
		// Cap at numEntries like the loop above: guards against malformed parallel
		// arrays so mainChainIndex can never index beyond their common length.
		for i := 0; i < numEntries; i++ {
			blockID := meta.BlockIDs[i]
			var (
				onChain bool
				err     error
			)
			if h.mainChainCache != nil {
				onChain, err = h.mainChainCache.IsOnMainChain(ctx, blockID, meta.BlockHeights[i])
			} else {
				onChain, err = h.repository.GetBlockchainClient().CheckBlockIsInCurrentChain(ctx, []uint32{blockID})
			}
			if err != nil {
				// Best-effort: tolerate transient blockchain client errors here. mainChainIndex
				// stays -1 if no main-chain entry is found; matches the GetBlockByID loop above
				// which also swallows per-block failures rather than 500'ing the whole response.
				h.logger.Warnf("Failed main-chain check for block %d: %v", blockID, err)
				continue
			}
			if onChain {
				mainChainIndex = i
				break
			}
		}

		// Create enhanced response with block hashes and subtree hashes.
		//
		// "tx" is null when this node does not retain the full transaction — a
		// node bootstrapped from a UTXO-set snapshot keeps only the live outputs,
		// which cannot reproduce the transaction. Every other field is still
		// accurate, so the response stays a 200 and txAvailable tells the caller
		// which case it is looking at without having to inspect "tx".
		response := map[string]interface{}{
			"tx":             meta.Tx,
			"txAvailable":    meta.Tx != nil,
			"parentTxHashes": meta.TxInpoints.ParentTxHashes,
			"blockIDs":       meta.BlockIDs,
			"blockHashes":    blockHashes,
			"blockHeights":   meta.BlockHeights,
			"subtreeIdxs":    meta.SubtreeIdxs,
			"subtreeHashes":  subtreeHashes,
			"mainChainIndex": mainChainIndex,
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
