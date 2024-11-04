// Package http_impl provides HTTP handlers for blockchain data retrieval and analysis.
package http_impl

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type res struct {
	Type string `json:"type"`
	Hash string `json:"hash"`
}

// Search creates an HTTP handler that searches for blockchain entities by hash or block height.
// It supports finding blocks, transactions, subtrees, and UTXOs.
//
// Parameters:
//   - c: Echo context containing the HTTP request and response
//
// Query Parameters:
//   - q: Search query string, can be either:
//   - 64-character hex string (hash search)
//   - Numeric value (block height search)
//     Example: ?q=000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f
//     Example: ?q=0
//
// Returns:
//   - error: Any error encountered during processing
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/json
//	Body: Entity type and hash:
//	  {
//	    "type": "<string>",  // One of: "block", "tx", "subtree", "utxo"
//	    "hash": "<string>"   // Hash of the found entity
//	  }
//
// Search Process:
//
//	For hash searches (64-character hex):
//	1. Tries to find as block hash
//	2. If not found, tries as transaction hash
//	3. If not found, tries as subtree hash
//	4. If not found, tries as UTXO hash
//
//	For numeric searches:
//	1. Validates block height is within range
//	2. Returns block hash at that height if found
//
// Error Responses:
//
//   - 400 Bad Request (with error codes):
//
//   - Code 1: Missing query parameter
//
//   - Code 2: Invalid hash format
//
//   - Code 3: Block search error
//
//   - Code 4: Subtree search error
//
//   - Code 5: Transaction search error
//
//   - Code 6: UTXO search error
//
//   - Code 7: Invalid query format
//
//   - 404 Not Found:
//
//   - No matching entity found
//
//   - 500 Internal Server Error:
//
//   - Repository errors
//
// Monitoring:
//   - Execution time recorded in "Search" statistic
//
// Example Usage:
//
//	# Search by block hash
//	GET /search?q=000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f
//
//	# Search by block height
//	GET /search?q=0
func (h *HTTP) Search(c echo.Context) error {
	start := gocore.CurrentTime()
	stat := AssetStat.NewStat("Search")
	defer func() {
		stat.AddTime(start)
	}()

	q := c.QueryParam("q")

	if q == "" {
		return sendError(c, http.StatusBadRequest, 1, errors.NewInvalidArgumentError("missing query parameter"))
	}

	if len(q) == 64 {
		// This is a hash
		hash, err := chainhash.NewHashFromStr(q)
		if err != nil {
			return sendError(c, http.StatusBadRequest, 2, errors.NewProcessingError("error reading hash", err))
		}

		// Check if the hash is a block...
		header, _, err := h.repository.GetBlockHeader(c.Request().Context(), hash)
		if err != nil && !errors.Is(err, errors.ErrNotFound) { // We return an error except if it's a not found error
			return sendError(c, http.StatusBadRequest, 3, errors.NewServiceError("error searching for block", err))
		}

		if header != nil {
			// It's a block
			return c.JSONPretty(200, &res{"block", hash.String()}, "  ")
		}

		// Check if it's a transaction
		tx, err := h.repository.GetTransactionMeta(c.Request().Context(), hash)
		if err != nil && !errors.Is(err, errors.ErrTxNotFound) {
			return sendError(c, http.StatusBadRequest, 5, errors.NewServiceError("error searching for tx", err))
		}

		if tx != nil {
			// It's a transaction
			return c.JSONPretty(200, &res{"tx", hash.String()}, "  ")
		}

		// Check if it's a subtree
		subtree, err := h.repository.GetSubtreeBytes(c.Request().Context(), hash)
		// TODO error handling is still a bit messy, not all implementations are throwing the ErrNotFound correctly
		if err != nil && !errors.Is(err, errors.ErrNotFound) && !strings.Contains(err.Error(), "not found") {
			return sendError(c, http.StatusBadRequest, 4, errors.NewServiceError("error searching for subtree", err))
		}

		if subtree != nil {
			// It's a subtree
			return c.JSONPretty(200, &res{"subtree", hash.String()}, "  ")
		}

		// Check if it's a utxo
		u, err := h.repository.GetUtxo(c.Request().Context(), &utxo.Spend{
			UTXOHash: hash,
		})
		if err != nil && !errors.Is(err, errors.ErrNotFound) {
			return sendError(c, http.StatusBadRequest, 6, errors.NewServiceError("error searching for utxo", err))
		}

		if u != nil {
			// It's a utxo
			return c.JSONPretty(http.StatusOK, &res{"utxo", hash.String()}, "  ")
		}

		return c.String(http.StatusNotFound, "not found")
	}

	if blockHeight, err := strconv.Atoi(q); err == nil {
		// We are searching a number, get latest block height
		_, blockMeta, err := h.repository.GetBestBlockHeader(c.Request().Context())
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}
		latestBlockHeight := blockMeta.Height

		// Return matching block if height within valid range
		if blockHeight >= 0 && uint32(blockHeight) <= latestBlockHeight {
			block, err := h.repository.GetBlockByHeight(c.Request().Context(), uint32(blockHeight))
			if err != nil {
				if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
					return echo.NewHTTPError(http.StatusNotFound, err.Error())
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}
			return c.JSONPretty(200, &res{"block", (*block.Hash()).String()}, "  ")
		}
	}

	return sendError(c, http.StatusBadRequest, 7, errors.NewUnknownError("query must be a valid hash or block height"))
}
