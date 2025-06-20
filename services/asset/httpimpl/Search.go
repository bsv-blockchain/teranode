// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
package httpimpl

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
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
	q := c.QueryParam("q")

	ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "Search",
		tracing.WithParentStat(AssetStat),
		tracing.WithDebugLogMessage(h.logger, "[Asset_http] Search for %s: %s", c.Request().RemoteAddr, q),
	)

	defer deferFn()

	if q == "" {
		return sendError(c, http.StatusBadRequest, int32(errors.ERR_INVALID_ARGUMENT), errors.NewInvalidArgumentError("missing query parameter"))
	}

	if len(q) == 64 {
		// This is a hash
		hash, err := chainhash.NewHashFromStr(q)
		if err != nil {
			return sendError(c, http.StatusBadRequest, int32(errors.ERR_INVALID_ARGUMENT), errors.NewInvalidArgumentError("error reading hash", err))
		}

		// Check if the hash is a block...
		header, _, err := h.repository.GetBlockHeader(ctx, hash)
		if err != nil && !errors.Is(err, errors.ErrNotFound) && !errors.Is(err, errors.ErrBlockNotFound) { // We return an error except if it's a not found error
			return sendError(c, http.StatusInternalServerError, int32(errors.ERR_SERVICE_ERROR), errors.NewServiceError("error searching for block", err))
		}

		if header != nil {
			// It's a block
			return c.JSONPretty(200, &res{"block", hash.String()}, "  ")
		}

		// Check if it's a transaction
		txMeta, err := h.repository.GetTransactionMeta(ctx, hash)
		if err != nil && !errors.Is(err, errors.ErrNotFound) && !errors.Is(err, errors.ErrTxNotFound) {
			return sendError(c, http.StatusInternalServerError, int32(errors.ERR_SERVICE_ERROR), errors.NewServiceError("error searching for tx", err))
		}

		if txMeta != nil {
			// It's a transaction
			return c.JSONPretty(200, &res{"tx", hash.String()}, "  ")
		}

		// Check if it's a subtree
		subtreeExists, err := h.repository.GetSubtreeExists(ctx, hash)
		if err != nil && !errors.Is(err, errors.ErrNotFound) && !strings.Contains(err.Error(), "not found") {
			return sendError(c, http.StatusInternalServerError, int32(errors.ERR_SERVICE_ERROR), errors.NewServiceError("error searching for subtree", err))
		}

		if subtreeExists {
			// It's a subtree
			return c.JSONPretty(200, &res{"subtree", hash.String()}, "  ")
		}

		// Check if it's a utxo
		u, err := h.repository.GetUtxo(ctx, &utxo.Spend{UTXOHash: hash})
		if err != nil && !errors.Is(err, errors.ErrNotFound) {
			return sendError(c, http.StatusInternalServerError, int32(errors.ERR_SERVICE_ERROR), errors.NewServiceError("error searching for utxo", err))
		}

		if u != nil {
			// It's a utxo
			return c.JSONPretty(http.StatusOK, &res{"utxo", hash.String()}, "  ")
		}

		return sendError(c, http.StatusNotFound, int32(errors.ERR_NOT_FOUND), errors.NewNotFoundError("no matching entity found"))
	}

	if blockHeight, err := strconv.Atoi(q); err == nil {
		if blockHeight < 0 {
			return sendError(c, http.StatusBadRequest, int32(errors.ERR_INVALID_ARGUMENT), errors.NewInvalidArgumentError("block height must be greater than or equal to 0"))
		}

		blockHeightUint32, err := util.SafeIntToUint32(blockHeight)
		if err != nil {
			return sendError(c, http.StatusBadRequest, int32(errors.ERR_INVALID_ARGUMENT), errors.NewInvalidArgumentError("invalid block height", err))
		}

		// Return matching block if height within valid range
		block, err := h.repository.GetBlockByHeight(ctx, blockHeightUint32)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return sendError(c, http.StatusNotFound, int32(errors.ERR_NOT_FOUND), errors.NewNotFoundError("block not found"))
			} else {
				return sendError(c, http.StatusInternalServerError, int32(errors.ERR_SERVICE_ERROR), err)
			}
		}

		return c.JSONPretty(200, &res{"block", block.Hash().String()}, "  ")
	}

	return sendError(c, http.StatusBadRequest, int32(errors.ERR_INVALID_ARGUMENT), errors.NewInvalidArgumentError("query must be a valid hash or block height"))
}
