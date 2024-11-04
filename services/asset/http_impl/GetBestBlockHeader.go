// Package http_impl provides HTTP handlers for the blockchain service API endpoints.
package http_impl

import (
	"encoding/hex"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
)

// GetBestBlockHeader creates an HTTP handler for retrieving the most recent block header
// in the blockchain. It supports multiple response formats through the mode parameter.
//
// Parameters:
//   - mode: ReadMode specifying the response format (JSON, BINARY_STREAM, or HEX)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// HTTP Response Formats:
//
//  1. JSON (mode = JSON):
//     Status: 200 OK
//     Content-Type: application/json
//     Body: Block header with metadata:
//     {
//     "version": <uint32>,            // Block version
//     "hash_prev_block": "<string>",  // Previous block hash
//     "hash_merkle_root": "<string>", // Merkle root hash
//     "timestamp": <uint32>,          // Block creation time (Unix timestamp)
//     "bits": "<string>",             // Target difficulty threshold
//     "nonce": <uint32>,              // Proof of work nonce
//     "hash": "<string>",             // Current block hash
//     "height": <uint32>,             // Block height in the chain
//     "tx_count": <uint64>,           // Number of transactions
//     "size_in_bytes": <uint64>,      // Block size in bytes
//     "miner": "<string>"             // Miner information
//     }
//
//  2. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: 80-byte block header in Bitcoin protocol format:
//     - Version (4 bytes, little-endian)
//     - Previous block hash (32 bytes)
//     - Merkle root (32 bytes)
//     - Timestamp (4 bytes, little-endian)
//     - Bits (4 bytes)
//     - Nonce (4 bytes, little-endian)
//
//  3. Hex (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hexadecimal string representation of the 80-byte block header
//
// Error Responses:
//   - 500 Internal Server Error:
//   - Repository errors
//   - Invalid read mode
//
// Monitoring:
//   - Execution time recorded in "GetBestBlockHeader_http" statistic
//   - Prometheus metric "asset_http_get_best_block_header" tracks successful responses
//
// Example Usage:
//
//	# Get JSON format
//	GET /best-block-header
//
//	# Get binary format
//	GET /best-block-header/raw
//
//	# Get hex format
//	GET /best-block-header/hex
//
// Note:
//   - The binary and hex formats contain only the core block header fields (80 bytes)
//   - The JSON format includes additional metadata from BlockHeaderMeta
//   - All little-endian fields are converted to standard integers in JSON format
func (h *HTTP) GetBestBlockHeader(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetBestBlockHeader_http").AddTime(start)
		}()

		h.logger.Debugf("[Asset_http] GetBestBlockHeader in %s for %s", mode, c.Request().RemoteAddr)
		defer h.logger.Debugf("[Asset_http] GetBestBlockHeader completed in %s for %s", mode, c.Request().RemoteAddr)

		blockHeader, meta, err := h.repository.GetBestBlockHeader(c.Request().Context())
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
		prometheusAssetHttpGetBestBlockHeader.WithLabelValues("OK", "200").Inc()

		r := &blockHeaderResponse{
			BlockHeader: blockHeader,
			Hash:        blockHeader.String(),
			Height:      meta.Height,
			TxCount:     meta.TxCount,
			SizeInBytes: meta.SizeInBytes,
			Miner:       meta.Miner,
		}

		switch mode {
		case JSON:
			return c.JSONPretty(200, r, "  ")
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, blockHeader.Bytes())
		case HEX:
			return c.String(200, hex.EncodeToString(blockHeader.Bytes()))
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
