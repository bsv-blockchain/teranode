package httpimpl

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

// legacyInternalTokenHeader carries the shared secret (asset_legacyPeerPoolToken)
// the legacy peer server presents on its internal ?wire=1 request to
// GET /block_legacy/:hash, so Asset can tell that call apart from an anonymous
// caller who also sets ?wire=1.
const legacyInternalTokenHeader = "X-Teranode-Internal-Token"

// GetLegacyBlock creates an HTTP handler that streams a block in the legacy Bitcoin protocol format.
//
// Parameters:
//   - c: Echo context containing the HTTP request and response
//
// URL Parameters:
//   - hash: Block hash (hex string)
//
// Returns:
//   - error: Any error encountered during processing
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/octet-stream
//	Body: Legacy format block data:
//	  - Magic number (4 bytes): 0xf9, 0xbe, 0xb4, 0xd9
//	  - Block size (4 bytes): little-endian uint32
//	  - Block header (80 bytes)
//	  - Transaction count (VarInt)
//	  - Transactions (variable length):
//	    * Coinbase transaction first
//	    * Followed by all other transactions if present
//
// Error Responses:
//   - 404 Not Found: Block not found
//   - 500 Internal Server Error:
//   - Invalid block hash format
//   - Block retrieval errors
//
// Monitoring:
//   - Prometheus metric "asset_http_get_block_legacy" tracks responses with status
//
// Example Usage:
//
//	GET /block/legacy/<hash>
func (h *HTTP) GetLegacyBlock() func(c echo.Context) error {
	return func(c echo.Context) error {
		// Dispatch to mining candidate handler when ?type=miningcandidate
		if c.QueryParam("type") == "miningcandidate" {
			return h.handleMiningCandidateLegacyBlock(c)
		}

		hashStr := c.Param("hash")

		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetLegacyBlock_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetLegacyBlock for %s: %s", c.RealIP(), hashStr),
		)

		defer deferFn()

		if len(hashStr) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid block hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid block hash format", err).Error())
		}

		wireBlock := c.QueryParam("wire") != ""

		// The internal legacy-peer-server pool is only for this node's own legacy
		// peer server (pushBlockMsg). wire=1 alone proves nothing: any anonymous
		// caller can set it. asset_legacyPeerPoolToken is a shared secret configured
		// identically on the legacy and Asset services; only a request that presents
		// it, in addition to wire=1, is trusted with the peer pool. An unset
		// (empty) token makes the pool unreachable by anyone, preserving today's
		// single-pool behaviour.
		if wireBlock && h.usesLegacyPeerPool(c) {
			ctx = repository.WithLegacyBlockReaderPeerPool(ctx, true)
		}

		r, err := h.repository.GetLegacyBlockReader(ctx, hash, wireBlock)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				prometheusAssetHTTPGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusNotFound)).Inc()
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				prometheusAssetHTTPGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		// The reader is a *io.PipeReader fed by a goroutine. Closing it unblocks that
		// goroutine with io.ErrClosedPipe, which it already handles. Without this, a
		// stream that fails because the write to the response failed (client gone) leaves
		// the producer blocked in Write forever, holding its arena and any file-store read
		// permit — streamOrAbort hijacks and returns nil in that case, so nothing else
		// ever closes the read end.
		defer r.Close()

		// Outcome counted after streaming — see the note in GetSubtreeData for why, and
		// for which cases this still labels as OK.
		if err := streamOrAbort(c, http.StatusOK, echo.MIMEOctetStream, r); err != nil {
			prometheusAssetHTTPGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()

			return err
		}

		prometheusAssetHTTPGetBlockLegacy.WithLabelValues("OK", "200").Inc()

		return nil
	}
}

// GetRestLegacyBlock creates an HTTP handler that streams a block in the legacy Bitcoin protocol format,
// using a ".bin" extension in the URL. Functionally identical to GetLegacyBlock but with different URL format.
//
// URL Parameters:
//   - hash: Block hash followed by ".bin" extension
//     Example: "000000...hash.bin"
//
// All other aspects (response format, errors, monitoring) are identical to GetLegacyBlock.
//
// Example Usage:
//
//	GET /block/legacy/<hash>.bin
func (h *HTTP) GetRestLegacyBlock() func(c echo.Context) error {
	return func(c echo.Context) error {
		resource := c.Param("hash.bin")
		if resource == "" {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid block hash extension").Error())
		}

		hashString := strings.Replace(resource, ".bin", "", 1)

		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetRestLegacyBlock_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http] GetBlockGetRestLegacyBlockByHash for %s: %s", c.RealIP(), resource),
		)

		defer deferFn()

		if len(hashString) != 64 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid block hash length").Error())
		}

		hash, err := chainhash.NewHashFromStr(hashString)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid block hash string", err).Error())
		}

		r, err := h.repository.GetLegacyBlockReader(ctx, hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				prometheusAssetHTTPGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusNotFound)).Inc()

				return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("block not found", err).Error())
			} else {
				prometheusAssetHTTPGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()

				return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error getting block", err).Error())
			}
		}

		// See GetLegacyBlock: closing the pipe read end is what unblocks the producer
		// goroutine when the response write failed and streamOrAbort hijacked.
		defer r.Close()

		// Outcome counted after streaming — see the note in GetSubtreeData.
		if err := streamOrAbort(c, http.StatusOK, echo.MIMEOctetStream, r); err != nil {
			prometheusAssetHTTPGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()

			return err
		}

		prometheusAssetHTTPGetBlockLegacy.WithLabelValues("OK", "200").Inc()

		return nil
	}
}

// usesLegacyPeerPool reports whether c's request is trusted to draw from the
// internal legacy-peer-server GetLegacyBlockReader pool, based on
// asset_legacyPeerPoolToken: a shared secret, never on network origin (RemoteAddr,
// X-Forwarded-For, etc. are all caller-controlled or topology-dependent, neither
// of which anonymous-vs-internal separation can safely rest on across every
// deployment shape, e.g. a multi-container asset_httpAddress).
//
// An empty configured token means the pool is unreachable: the zero value, and
// the shipped default, is today's single-pool behaviour. Otherwise the caller
// must present the same value in the X-Teranode-Internal-Token header, compared
// with a constant-time comparison so response timing cannot be used to guess it
// byte-by-byte.
func (h *HTTP) usesLegacyPeerPool(c echo.Context) bool {
	configured := h.settings.Asset.LegacyPeerPoolToken
	if configured == "" {
		return false
	}

	presented := c.Request().Header.Get(legacyInternalTokenHeader)
	if presented == "" {
		return false
	}

	// Compare fixed-length digests: ConstantTimeCompare returns early on a length
	// mismatch, which would leak the token's length.
	presentedSum := sha256.Sum256([]byte(presented))
	configuredSum := sha256.Sum256([]byte(configured))

	return subtle.ConstantTimeCompare(presentedSum[:], configuredSum[:]) == 1
}
