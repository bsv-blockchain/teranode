// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
// It implements a comprehensive REST API for accessing blocks, transactions, and other
// blockchain data through standardized interfaces with multiple response formats.
package httpimpl

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

// BlockExtended represents a block with additional information about the next block
// in the blockchain. It embeds the base Block model and adds the next block hash.
//
// This structure extends the standard Block model to provide additional blockchain traversal
// capabilities by including a reference to the next block in the chain. This is particularly
// useful for blockchain explorers and analysis tools that need to navigate the chain in
// a forward direction.
//
// The structure preserves all fields from the base Block model through embedding, while
// adding the NextBlock field that can be used for chain traversal. This approach maintains
// compatibility with existing Block-based code while extending functionality.
//
// Fields:
// - Block: Embedded Block model containing all standard block data including header, transactions, etc.
// - NextBlock: Hash of the next block in the chain, or nil if this is the tip of the chain
type BlockExtended struct {
	*model.Block                 // Embedded Block model containing basic block data
	NextBlock    *chainhash.Hash `json:"nextblock"` // Hash of the next block in the chain, if available
}

// GetBlockByHeight creates an HTTP handler for retrieving blocks by their height in the blockchain.
// The functionality mirrors GetBlockByHash but uses block height for lookup instead of hash.
// This is particularly useful for clients that need to retrieve blocks based on their position
// in the blockchain rather than their identifying hash.
//
// The handler performs the following operations:
// 1. Validates the height parameter from the URL path
// 2. Retrieves the block from the repository using its height
// 3. Fetches the next block hash (if available) to enhance the response with chain traversal data
// 4. Signs the response if configured
// 5. Formats and returns the block according to the specified read mode
//
// This endpoint is commonly used by blockchain explorers, analytics systems, and applications
// that need to traverse the blockchain sequentially or access blocks at specific heights.
//
// Parameters:
//   - mode: ReadMode specifying the response format (JSON, BINARY_STREAM, or HEX)
//     JSON provides a structured representation with all block fields
//     BINARY_STREAM offers the most efficient transfer format
//     HEX provides a human-readable representation of the binary data
//
// Returns:
//   - func(c echo.Context) error: Echo handler function that processes the request
//
// URL Parameters:
//   - height: Block height (uint32) representing the position in the blockchain
//
// Query Parameters:
//   - format: Optional parameter to override the default response format
//
// HTTP Response Formats:
//
//  1. JSON (mode = JSON):
//     Status: 200 OK
//     Content-Type: application/json
//     Body: BlockExtended object:
//     {
//     "header": {
//     // BlockHeader fields
//     "version": <uint32>,
//     "hash_prev_block": "<string>",
//     "hash_merkle_root": "<string>",
//     "timestamp": <uint32>,
//     "bits": "<string>",
//     "nonce": <uint32>
//     },
//     "coinbase_tx": <transaction>,
//     "transaction_count": <uint64>,
//     "size_in_bytes": <uint64>,
//     "subtrees": ["<hash>", ...],
//     "height": <uint32>,
//     "id": <uint32>,
//     "nextblock": "<hash or null>"
//     }
//
//  2. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: Serialized block in the following format:
//     - Block header (80 bytes)
//     - Transaction count (varint)
//     - Size in bytes (varint)
//     - Subtree list
//     - Coinbase transaction
//     - Height (varint)
//
//  3. Hex (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hex-encoded version of the binary format
//
// Error Responses:
//   - 400 Bad Request:
//   - Invalid height parameter (non-integer)
//   - Block serialization error
//   - 404 Not Found: No block exists at specified height
//   - 500 Internal Server Error: Repository errors or invalid read mode
//
// Security:
//   - Response includes a cryptographic signature in the header if private key is configured
//
// Monitoring:
//   - Execution time recorded in "GetBlock_http" statistic
//   - Prometheus metric "asset_http_get_block" tracks successful responses
//   - Detailed logging of request handling and completion time
//
// Example Usage:
//
//	// Get block in JSON format
//	GET /block/height/0
//
//	// Get block in raw binary format
//	GET /block/height/0/raw
//
//	// Get block in hex format
//	GET /block/height/0/hex
func (h *HTTP) GetBlockByHeight(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "GetBlockByHeight_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithLogMessage(h.logger, "[Asset_http] GetBlockByHeight in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("height")),
		)

		defer deferFn()

		height, err := strconv.ParseUint(c.Param("height"), 10, 64)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid height parameter", err).Error())
		}

		heightUint32, err := util.SafeUint64ToUint32(height)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid height parameter", err).Error())
		}

		block, err := h.repository.GetBlockByHeight(ctx, heightUint32)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		// sign the response, if the private key is set, ignore error
		// do this before any output is sent to the client, this adds a signature to the response header
		_ = h.Sign(c.Response(), block.Header.Hash().CloneBytes())

		prometheusAssetHTTPGetBlock.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			// get next hash to include in response
			var nextBlockHash *chainhash.Hash

			nextHeight, err := util.SafeUint64ToUint32(height + 1)
			if err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid height parameter", err).Error())
			}

			nextBlock, _ := h.repository.GetBlockByHeight(ctx, nextHeight)
			if nextBlock != nil {
				nextBlockHash = nextBlock.Hash()
			}

			blockExtended := BlockExtended{
				Block:     block,
				NextBlock: nextBlockHash,
			}

			return c.JSONPretty(200, blockExtended, "  ")
		}

		b, err := block.Bytes()
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, b)
		case HEX:
			return c.String(200, hex.EncodeToString(b))
		default:
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("bad read mode").Error())
		}
	}
}

// GetBlockByHash creates an HTTP handler for retrieving blocks by their hash.
// It supports multiple response formats and includes response signing capabilities.
//
// Parameters:
//   - mode: ReadMode specifying the response format (JSON, BINARY_STREAM, or HEX)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// URL Parameters:
//   - hash: Block hash (hex string)
//
// HTTP Response Formats:
//
//  1. JSON (mode = JSON):
//     Status: 200 OK
//     Content-Type: application/json
//     Body: BlockExtended object:
//     {
//     "header": {
//     // BlockHeader fields
//     "version": <uint32>,
//     "hash_prev_block": "<string>",
//     "hash_merkle_root": "<string>",
//     "timestamp": <uint32>,
//     "bits": "<string>",
//     "nonce": <uint32>
//     },
//     "coinbase_tx": <transaction>,
//     "transaction_count": <uint64>,
//     "size_in_bytes": <uint64>,
//     "subtrees": ["<hash>", ...],
//     "height": <uint32>,
//     "id": <uint32>,
//     "nextblock": "<hash or null>"
//     }
//
//  2. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: Serialized block in the following format:
//     - Block header (80 bytes)
//     - Transaction count (varint)
//     - Size in bytes (varint)
//     - Subtree list
//     - Coinbase transaction
//     - Height (varint)
//
//  3. Hex (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hex-encoded version of the binary format
//
// Error Responses:
//   - 400 Bad Request: Invalid hash format or block serialization error
//   - 404 Not Found: Block not found with specified hash
//   - 500 Internal Server Error: Repository errors or invalid read mode
//
// Security:
//   - Response includes a cryptographic signature in the header if private key is configured
//
// Monitoring:
//   - Prometheus metric "asset_http_get_block" tracks successful responses
//   - Debug logging of request handling
//
// Example Usage:
//
//	// Get block in JSON format
//	GET /block/hash/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f
//
//	// Get block in raw binary format
//	GET /block/hash/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f/raw
//
//	// Get block in hex format
//	GET /block/hash/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f/hex
func (h *HTTP) GetBlockByHash(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		hashStr := c.Param("hash")

		ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "GetBlockByHash_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetBlockByHash in %s for %s: %s", mode, c.Request().RemoteAddr, hashStr),
		)

		defer deferFn()

		if len(hashStr) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash string", err).Error())
		}

		block, err := h.repository.GetBlockByHash(ctx, hash)
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

		prometheusAssetHTTPGetBlock.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			height := block.Height
			if height == 0 {
				_, blockMeta, _ := h.repository.GetBlockHeader(c.Request().Context(), hash)
				if blockMeta != nil {
					height = blockMeta.Height
				}
			}

			// get next hash to include in response
			var nextBlockHash *chainhash.Hash

			nextBlock, _ := h.repository.GetBlockByHeight(ctx, height+1)
			if nextBlock != nil {
				nextBlockHash = nextBlock.Hash()
			}

			blockExtended := BlockExtended{
				Block:     block,
				NextBlock: nextBlockHash,
			}

			return c.JSONPretty(200, blockExtended, "  ")
		}

		b, err := block.Bytes()
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, b)
		case HEX:
			return c.String(200, hex.EncodeToString(b))
		default:
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("bad read mode").Error())
		}
	}
}
