// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
package httpimpl

import (
	"net/http"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/bump"
	"github.com/bsv-blockchain/teranode/util/merkleproof"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

// NOTE: This response format is deprecated in favor of the BUMP format.
// The endpoint now returns BSV Unified Merkle Path (BUMP) format data
// as defined in BRC-74: https://github.com/bitcoin-sv/BRCs/blob/master/transactions/0074.md

// LegacyMerkleProofResponse is kept for reference but no longer used.
type LegacyMerkleProofResponse struct {
	TxID             string   `json:"txid"`
	BlockHash        string   `json:"blockHash"`
	BlockHeight      uint32   `json:"blockHeight"`
	MerkleRoot       string   `json:"merkleRoot"`
	SubtreeIndex     int      `json:"subtreeIndex"`
	TxIndexInSubtree int      `json:"txIndexInSubtree"`
	SubtreeRoot      string   `json:"subtreeRoot"`
	SubtreeProof     []string `json:"subtreeProof"`
	BlockProof       []string `json:"blockProof"`
	CompletePath     []string `json:"completePath"`
	Flags            []int    `json:"flags"`
}

// GetMerkleProof creates an HTTP handler for retrieving a merkle proof for a transaction
// in BSV Unified Merkle Path (BUMP) format as defined in BRC-74.
// The proof can be used for SPV (Simplified Payment Verification) to verify that a
// transaction is included in a specific block without downloading the entire block.
//
// This implementation converts Teranode's internal merkle proof structure to the
// standardized BUMP format, ensuring compatibility with BSV ecosystem tools.
//
// Parameters:
//   - mode: ReadMode specifying the response format:
//   - BINARY_STREAM: Returns BUMP data as binary (application/octet-stream)
//   - HEX: Returns BUMP data as hexadecimal string (text/plain)
//   - JSON: Returns BUMP data as JSON (application/json)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function that processes the HTTP request
//
// URL Parameters:
//   - hash: Transaction hash or subtree hash (64-character hex string)
//     The API will attempt to find the hash as a transaction first,
//     then as a subtree if not found as a transaction
//
// HTTP Response Formats:
//
//	JSON (Content-Type: application/json):
//	{
//	  "blockHeight": <number>,          // Height of the block
//	  "path": [                         // Array of merkle tree levels
//	    [                               // Level 0 (closest to transaction)
//	      {
//	        "offset": <number>,         // Position within level
//	        "hash": "<hex>",            // Hash value (optional)
//	        "txid": <boolean>,          // Is client txid (optional)
//	        "duplicate": <boolean>      // Duplicate flag (optional)
//	      }
//	    ]
//	  ]
//	}
//
//	HEX (Content-Type: text/plain):
//	Returns the binary BUMP data encoded as a hexadecimal string
//
//	BINARY (Content-Type: application/octet-stream):
//	Returns the raw binary BUMP data following the specification:
//	- Block height (VarInt)
//	- Tree height (1 byte)
//	- Level data with offsets, flags, and hash values
//
// Error Responses:
//
//   - 400 Bad Request:
//     Returned when the transaction hash is invalid (wrong format or length)
//     Example: {"message": "invalid hash length"}
//     Example: {"message": "invalid hash string"}
//
//   - 404 Not Found:
//     Returned when the hash doesn't exist as a transaction or subtree
//     Example: {"message": "hash not found as transaction or subtree"}
//
//   - 500 Internal Server Error:
//     Returned for various internal errors:
//
//   - Repository access failures
//
//   - Block data retrieval errors
//
//   - Subtree data retrieval errors
//
//   - Merkle proof generation errors
//     Example: {"message": "error retrieving block data"}
//
// Security Considerations:
//   - Response includes cryptographic signature if private key is configured
//   - Transaction hashes are validated for length and format before processing
//   - The proof can be independently verified by clients using standard SPV algorithms
//
// Monitoring and Metrics:
//   - Execution time recorded in "GetMerkleProof_http" statistic for performance tracking
//   - Prometheus metric "asset_http_get_merkle_proof" tracks response counts by status
//   - Request logging includes client IP and requested transaction hash
//
// Example Usage:
//
//	# Get merkle proof for a transaction
//	GET /api/v1/merkle_proof/{txid}/json
//
//	# Example response:
//	{
//	  "txid": "abc123...",
//	  "blockHash": "def456...",
//	  "blockHeight": 800000,
//	  "merkleRoot": "789abc...",
//	  "subtreeIndex": 5,
//	  "txIndexInSubtree": 42,
//	  "subtreeRoot": "111222...",
//	  "subtreeProof": ["aaa...", "bbb...", "ccc..."],
//	  "blockProof": ["ddd...", "eee..."],
//	  "completePath": ["aaa...", "bbb...", "ccc...", "ddd...", "eee..."],
//	  "flags": [0, 1, 1, 0, 1]
//	}
//
// BUMP Format Compatibility:
//   - Follows BRC-74 specification for BSV ecosystem compatibility
//   - Binary format uses Bitcoin VarInt encoding for block height
//   - Flag values: 0x00=hash data, 0x01=duplicate, 0x02=client txid
//   - If a transaction appears in multiple blocks, the proof for the first block is returned
//   - Optimized for size and validation speed while maintaining SPV compatibility
func (h *HTTP) GetMerkleProof(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		hashStr := c.Param("hash")

		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetMerkleProof_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithLogMessage(h.logger, "[Asset_http] GetMerkleProof in %s for %s: %s", mode, c.Request().RemoteAddr, hashStr),
		)

		defer deferFn()

		if len(hashStr) != 64 {
			prometheusAssetHTTPGetMerkleProof.WithLabelValues("BadRequest", "400").Inc()
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash length").Error())
		}

		txHash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			prometheusAssetHTTPGetMerkleProof.WithLabelValues("BadRequest", "400").Inc()
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid hash string", err).Error())
		}

		// Create adapter to use merkleproof helper functions
		adapter := newMerkleProofAdapter(ctx, h.repository)

		// Try to construct merkle proof - first as transaction, then as subtree
		var proof *merkleproof.MerkleProof

		// First, try as transaction hash
		proof, err = merkleproof.ConstructMerkleProof(txHash, adapter)
		if err != nil {
			// If transaction not found, try as subtree hash
			if strings.Contains(err.Error(), "not in any block") || strings.Contains(err.Error(), "not found") {
				proof, err = merkleproof.ConstructSubtreeMerkleProof(txHash, adapter)
				if err != nil {
					// Neither transaction nor subtree found
					errStr := err.Error()
					if strings.Contains(errStr, "not found") {
						prometheusAssetHTTPGetMerkleProof.WithLabelValues("NotFound", "404").Inc()
						return echo.NewHTTPError(http.StatusNotFound, "hash not found as transaction or subtree")
					}
					prometheusAssetHTTPGetMerkleProof.WithLabelValues("InternalError", "500").Inc()
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			} else {
				// Some other error occurred
				prometheusAssetHTTPGetMerkleProof.WithLabelValues("InternalError", "500").Inc()
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		// Convert to BUMP format
		bumpProof, err := bump.ConvertToBUMP(proof)
		if err != nil {
			prometheusAssetHTTPGetMerkleProof.WithLabelValues("InternalError", "500").Inc()
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to convert to BUMP format: "+err.Error())
		}

		// Validate BUMP format
		if err = bump.Validate(bumpProof); err != nil {
			prometheusAssetHTTPGetMerkleProof.WithLabelValues("InternalError", "500").Inc()
			return echo.NewHTTPError(http.StatusInternalServerError, "invalid BUMP format: "+err.Error())
		}

		// Sign the response if configured
		_ = h.Sign(c.Response(), txHash.CloneBytes())

		prometheusAssetHTTPGetMerkleProof.WithLabelValues("OK", "200").Inc()

		switch mode {
		case JSON:
			return c.JSONPretty(200, bumpProof, "  ")
		case HEX:
			hexData, err := bumpProof.EncodeHex()
			if err != nil {
				prometheusAssetHTTPGetMerkleProof.WithLabelValues("InternalError", "500").Inc()
				return echo.NewHTTPError(http.StatusInternalServerError, "failed to encode BUMP hex: "+err.Error())
			}
			return c.String(200, hexData)
		case BINARY_STREAM:
			binaryData, err := bumpProof.EncodeBinary()
			if err != nil {
				prometheusAssetHTTPGetMerkleProof.WithLabelValues("InternalError", "500").Inc()
				return echo.NewHTTPError(http.StatusInternalServerError, "failed to encode BUMP binary: "+err.Error())
			}
			return c.Blob(200, "application/octet-stream", binaryData)
		default:
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("bad read mode").Error())
		}
	}
}
