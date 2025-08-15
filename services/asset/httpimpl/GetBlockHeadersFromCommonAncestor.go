package httpimpl

import (
	"net/http"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/labstack/echo/v4"
)

// GetBlockHeadersFromCommonAncestor creates an HTTP handler for retrieving multiple consecutive block headers
// starting from a given block hash, up to a specified number of headers.
//
// Parameters:
//   - mode: ReadMode specifying the response format (JSON, BINARY_STREAM, or HEX)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// URL Parameters:
//   - hash: Starting block hash (hex string)
//
// Query Parameters: (either n or block_locator_hashes must be provided but not both)
//   - n: Number of headers to retrieve (default: 100, max: 1000)
//     Example: ?n=50
//   - block_locator_hashes: Block locator hashes (hex string)
//     Example: ?block_locator_hashes=000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f
//
// HTTP Response Formats:
//
//  1. JSON (mode = JSON):
//     Status: 200 OK
//     Content-Type: application/json
//     Body: Array of block headers:
//     [
//     {
//     "version": <uint32>,             // Block version
//     "hash_prev_block": "<string>",   // Previous block hash
//     "hash_merkle_root": "<string>",  // Merkle root hash
//     "timestamp": <uint32>,           // Block creation time
//     "bits": "<string>",              // 4-byte difficulty target (little-endian)
//     "nonce": <uint32>,               // PoW nonce
//     "hash": "<string>",              // Current block hash
//     "height": <uint32>               // Block height
//     },
//     // ... additional headers
//     ]
//
//  2. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: Concatenated 80-byte block headers, each containing:
//     - Version (4 bytes, little-endian uint32)
//     - Previous block hash (32 bytes)
//     - Merkle root (32 bytes)
//     - Timestamp (4 bytes, little-endian uint32)
//     - Bits (4 bytes, little-endian NBit)
//     - Nonce (4 bytes, little-endian uint32)
//     Total size = n * 80 bytes
//
//  3. Hex (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hexadecimal string of concatenated block headers
//     String length = n * 160 characters (80 bytes * 2)
//
// Error Responses:
//
//   - 404 Not Found:
//
//   - Starting block not found
//
//   - Headers not found
//
//   - 500 Internal Server Error:
//
//   - Invalid hash format
//
//   - Repository errors
//
//   - Invalid read mode
//
// Monitoring:
//   - Execution time recorded in "GetBlockHeaders_http" statistic
//   - Prometheus metric "asset_http_get_block_header" tracks successful responses
//   - Debug logging of request and completion
//
// Example Usage:
//
//	# Get 50 headers in JSON format
//	GET /block/headersToCommonAncestor/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f?n=50
//
//	# Get 100 headers in binary format
//	GET /block/headersToCommonAncestor/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f/raw
//
//	# Get 25 headers in hex format
//	GET /block/headersToCommonAncestor/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f/hex?n=25
//
// Notes:
//   - Headers are returned in consecutive order starting from the specified hash
//   - Binary response size can be calculated as n * 80 bytes
//   - Hex response size can be calculated as n * 160 characters
//   - Default limit of 100 headers can be adjusted via 'n' parameter
//   - Maximum limit of 10_000 headers per request
func (h *HTTP) GetBlockHeadersFromCommonAncestor(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		hashStr := c.Param("hash")

		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetBlockHeadersFromCommonAncestor_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithLogMessage(h.logger, "[GetBlockHeadersFromCommonAncestor_http] Get %s Block Headers in %s for %s", mode, c.Request().RemoteAddr, hashStr),
		)
		defer deferFn()

		chainTipHash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash string", err).Error())
		}

		hashes, err := h.parseBlockLocatorHashValues(c.QueryParam("block_locator_hashes"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		numberOfHeaders, err := h.parseNumberOfHeaders(c.QueryParam("n"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		var (
			headers     []*model.BlockHeader
			headerMetas []*model.BlockHeaderMeta
		)

		headers, headerMetas, err = h.repository.GetBlockHeadersFromCommonAncestor(ctx, chainTipHash, hashes, uint32(numberOfHeaders)) // nolint:gosec
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			}

			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		prometheusAssetHTTPGetBlockHeader.WithLabelValues("OK", "200").Inc()

		return h.formatResponse(c, mode, headers, headerMetas)
	}
}

// parseBlockLocatorHashes parses a string of concatenated hashes into an array of chainhash.Hash
func (h *HTTP) parseBlockLocatorHashValues(hashesStr string) ([]chainhash.Hash, error) {
	if len(hashesStr) == 0 {
		return nil, errors.NewInvalidArgumentError("block locator hashes cannot be empty")
	}

	if len(hashesStr)%64 != 0 {
		return nil, errors.NewInvalidArgumentError("block locator hashes length must be a multiple of 64")
	}

	numHashes := len(hashesStr) / 64
	hashes := make([]chainhash.Hash, numHashes)

	for i := 0; i < numHashes; i++ {
		hashPart := hashesStr[i*64 : (i+1)*64]

		hash, err := chainhash.NewHashFromStr(hashPart)
		if err != nil {
			return nil, errors.NewInvalidArgumentError("invalid hash string in block locator", err)
		}

		hashes[i] = *hash
	}

	return hashes, nil
}
