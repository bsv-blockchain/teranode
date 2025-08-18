// Package httpimpl provides HTTP handlers for blockchain data retrieval,
// including block locator generation for chain synchronization.
package httpimpl

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/labstack/echo/v4"
)

// GetBlockLocator handles HTTP GET requests for retrieving a block locator.
// A block locator is an array of block hashes used for efficient chain
// synchronization between nodes.
//
// Parameters:
//   - c: Echo context containing the HTTP request and response
//
// Query Parameters:
//   - hash: Optional block hash to start from (default: best block)
//   - height: Optional block height to start from (ignored if hash provided)
//
// Returns:
//   - error: Any error encountered during processing
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/json
//	Body: Block locator array:
//	  {
//	    "block_locator": [
//	      "<hash1>",  // Most recent block hash
//	      "<hash2>",  // Previous block (exponential backoff)
//	      // ... additional hashes
//	      "<genesis>" // Always includes genesis block
//	    ]
//	  }
//
// Error Responses:
//
//   - 400 Bad Request:
//
//   - Invalid hash parameter
//
//   - 500 Internal Server Error:
//
//   - Best block header retrieval failure
//
//   - Blockchain client communication error
//
// Example Usage:
//
//	# Get block locator from current best block
//	GET /block_locator
//
//	# Get block locator from specific block
//	GET /block_locator?hash=<block_hash>
//
// Notes:
//   - Uses exponential backoff algorithm (first 10 blocks, then doubles)
//   - Always includes genesis block as the last element
//   - Calls blockchain service's GetBlockLocator gRPC method
func (h *HTTP) GetBlockLocator(c echo.Context) error {
	ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetBlockLocator_http",
		tracing.WithParentStat(AssetStat),
	)
	defer deferFn()

	// Get optional hash parameter
	hashParam := c.QueryParam("hash")
	heightParam := c.QueryParam("height")

	var blockHash []byte
	var blockHeight uint32

	// If no hash provided, use the best block
	if hashParam == "" {
		bestHeader, bestMeta, err := h.repository.GetBestBlockHeader(ctx)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("best block header not found").Error())
			}
			return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error getting best block header", err).Error())
		}
		blockHash = bestHeader.Hash().CloneBytes()
		blockHeight = bestMeta.Height
	} else {
		// Parse the provided hash
		hash, err := chainhash.NewHashFromStr(hashParam)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash parameter", err).Error())
		}
		blockHash = hash.CloneBytes()

		// If height was also provided, use it
		if heightParam != "" {
			var height uint32
			if _, err := fmt.Sscanf(heightParam, "%d", &height); err == nil {
				blockHeight = height
			}
		}
	}

	h.logger.Debugf("[Asset_http] GetBlockLocator for %s with hash = %x, height = %d",
		c.Request().RemoteAddr, blockHash, blockHeight)

	// Convert byte array to chainhash.Hash for the repository call
	var blockHeaderHash *chainhash.Hash
	var err error
	if len(blockHash) > 0 {
		blockHeaderHash, err = chainhash.NewHash(blockHash)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,
				errors.NewProcessingError("error creating hash", err).Error())
		}
	}

	// Call the repository to get the block locator
	locator, err := h.repository.GetBlockLocator(ctx, blockHeaderHash, blockHeight)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError,
			errors.NewProcessingError("error getting block locator", err).Error())
	}

	// Convert chainhash.Hash array to hex strings for JSON response
	locatorHexStrings := make([]string, len(locator))
	for i, hash := range locator {
		locatorHexStrings[i] = hash.String()
	}

	response := map[string]interface{}{
		"block_locator": locatorHexStrings,
	}

	h.logger.Debugf("[GetBlockLocator] sending block locator with %d hashes to client", len(locatorHexStrings))

	return c.JSONPretty(200, response, "  ")
}
