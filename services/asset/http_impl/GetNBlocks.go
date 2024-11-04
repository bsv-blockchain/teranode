// Package http_impl provides HTTP handlers for blockchain data retrieval and analysis.
package http_impl

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

// GetNBlocks creates an HTTP handler for retrieving multiple consecutive blocks
// starting from a specific block hash. It supports multiple response formats
// and pagination.
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
// Query Parameters:
//   - n: Number of blocks to retrieve (default: 100, max: 1000)
//     Example: ?n=50
//
// HTTP Response Formats:
//
//  1. JSON (mode = JSON):
//     Status: 200 OK
//     Content-Type: application/json
//     Body: Array of blocks:
//     [
//     {
//     "header": {
//     // BlockHeader fields
//     },
//     "coinbase_tx": <transaction>,
//     "transaction_count": <uint64>,
//     "size_in_bytes": <uint64>,
//     "subtrees": ["<hash>", ...],
//     "height": <uint32>,
//     "id": <uint32>
//     },
//     // ... additional blocks
//     ]
//
//  2. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: Concatenated block bytes, each containing:
//     - Block header
//     - Transaction count (VarInt)
//     - Size in bytes (VarInt)
//     - Subtree list
//     - Coinbase transaction
//     - Height (VarInt)
//
//  3. Hex (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hexadecimal string of concatenated block bytes
//
// Error Responses:
//
//   - 400 Bad Request:
//
//   - Invalid block hash format
//
//   - 404 Not Found:
//
//   - Starting block not found
//     Example: {"message": "not found"}
//
//   - 500 Internal Server Error:
//
//   - Block serialization errors
//
//   - Repository errors
//
//   - Invalid read mode
//
// Monitoring:
//   - Execution time recorded in "GetNBlocks_http" statistic
//   - Prometheus metric "asset_http_get_block_header" tracks successful responses
//   - Debug logging of request parameters
//
// Example Usage:
//
//	# Get 50 blocks in JSON format
//	GET /blocks/n/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f?n=50
//
//	# Get 100 blocks in binary format
//	GET /blocks/n/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f/raw
//
//	# Get 25 blocks in hex format
//	GET /blocks/n/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f/hex?n=25
//
// Notes:
//   - Blocks are returned in consecutive order starting from the specified hash
//   - Default limit of 100 blocks can be adjusted via 'n' parameter
//   - Maximum limit of 1000 blocks per request
//   - Binary and hex responses are concatenated block data
func (h *HTTP) GetNBlocks(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetNBlocks_http").AddTime(start)
		}()

		hashStr := c.Param("hash")
		nStr := c.QueryParam("n")

		numberOfBlocks := 100
		if nStr != "" {
			numberOfBlocks, _ = strconv.Atoi(nStr)
			if numberOfBlocks == 0 {
				numberOfBlocks = 100
			}
			if numberOfBlocks > 1000 {
				numberOfBlocks = 1000
			}
		}

		h.logger.Debugf("[Asset_http] Get %s Blocks in %s for %s", mode, c.Request().RemoteAddr, hashStr)

		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return err
		}

		// get all the blocks from the repository
		blocks, err := h.repository.GetBlocks(c.Request().Context(), hash, uint32(numberOfBlocks))
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusAssetHttpGetBlockHeader.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			return c.JSONPretty(200, blocks, "  ")
		}

		bytes := make([]byte, 0, len(blocks)*32*1024)
		for _, block := range blocks {
			blockBytes, err := block.Bytes()
			if err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
			bytes = append(bytes, blockBytes...)
		}

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, bytes)
		case HEX:
			return c.String(200, hex.EncodeToString(bytes))
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
