// Package httpimpl provides HTTP handlers for blockchain data retrieval,
// including block header information in various formats.
package httpimpl

import (
	"encoding/hex"
	"net/http"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetBlockHeader creates an HTTP handler for retrieving block header information
// by block hash. It supports multiple response formats.
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
//     Body: Block header with metadata:
//     {
//     "version": <uint32>,             // Block version
//     "hash_prev_block": "<string>",   // Previous block hash
//     "hash_merkle_root": "<string>",  // Merkle root hash
//     "timestamp": <uint32>,           // Block creation time
//     "bits": "<string>",              // 4-byte difficulty target (little-endian)
//     "nonce": <uint32>,               // PoW nonce
//     "hash": "<string>",              // Current block hash
//     "height": <uint32>,              // Block height
//     "tx_count": <uint64>,            // Number of transactions
//     "size_in_bytes": <uint64>,       // Block size in bytes
//     "miner": "<string>"              // Miner information
//     }
//
//  2. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: 80-byte block header with fields in order:
//     - Version (4 bytes, little-endian uint32)
//     - Previous block hash (32 bytes)
//     - Merkle root (32 bytes)
//     - Timestamp (4 bytes, little-endian uint32)
//     - Bits (4 bytes, little-endian NBit)
//     - Nonce (4 bytes, little-endian uint32)
//
//  3. Hex (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hexadecimal string of the 80-byte block header
//
// Error Responses:
//   - 404 Not Found: Block header not found
//   - 500 Internal Server Error:
//   - Invalid hash format
//   - Repository errors
//   - Invalid read mode
//
// Monitoring:
//   - Execution time recorded in "GetBlockHeader_http" statistic
//   - Prometheus metric "asset_http_get_block_header" tracks successful responses
//
// Example Usage:
//
//	# Get header in JSON format
//	GET /block/header/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f
//
//	# Get header in binary format
//	GET /block/header/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f/raw
//
//	# Get header in hex format
//	GET /block/header/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f/hex
//
// Notes:
//   - The binary format is exactly 80 bytes following the Bitcoin protocol
//   - NBit (bits) is a 4-byte array stored in little-endian format
//   - All integer values in the binary format are little-endian
func (h *HTTP) GetBlockHeader(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		hashParam := c.Param("hash")

		ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "GetBlockHeader_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithLogMessage(h.logger, "[Asset_http] GetBlockHeader in %s for %s: %s", mode, c.Request().RemoteAddr, hashParam),
		)

		defer deferFn()

		if len(hashParam) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(hashParam)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewProcessingError("invalid hash string", err).Error())
		}

		var (
			header *model.BlockHeader
			meta   *model.BlockHeaderMeta
		)

		header, meta, err = h.repository.GetBlockHeader(ctx, hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusAssetHttpGetBlockHeader.WithLabelValues("OK", "200").Inc()

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, header.Bytes())
		case HEX:
			return c.String(200, hex.EncodeToString(header.Bytes()))
		case JSON:
			headerResponse := &blockHeaderResponse{
				BlockHeader: header,
				Hash:        header.String(),
				Height:      meta.Height,
				TxCount:     meta.TxCount,
				SizeInBytes: meta.SizeInBytes,
				Miner:       meta.Miner,
			}
			return c.JSONPretty(200, headerResponse, "  ")
		default:
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("bad read mode").Error())
		}
	}
}
