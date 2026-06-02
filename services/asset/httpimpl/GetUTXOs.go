package httpimpl

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net/http"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
	"golang.org/x/sync/errgroup"
)

const (
	// utxosRequestRecordSize is the on-wire size of one (txid, vout) lookup
	// in the POST /api/v1/utxos request body.
	utxosRequestRecordSize = 36 // 32 bytes txid + 4 bytes vout (little-endian)

	// utxosResponseRecordSize is the on-wire size of one fixed-length response
	// record in the binary and hex output modes. Binary clients can index by
	// `i*utxosResponseRecordSize`.
	utxosResponseRecordSize = 48 // 8 bytes status + 4 bytes lockTime + 4 bytes spendingVin + 32 bytes spendingTxID

	// utxosFanoutLimit caps in-flight per-record store lookups. Mirrors the
	// limit used by GetTransactions (POST /api/v1/subtree/:hash/txs).
	utxosFanoutLimit = 1024
)

// GetUTXOs creates an HTTP handler for bulk UTXO spend-status lookups.
//
// HTTP Method:
//   - POST
//
// Request (identical for all three response modes):
//
//	Content-Type: application/octet-stream
//	Body: Concatenated 36-byte records, each containing
//	      [32 bytes txid][4 bytes vout (little-endian)].
//	      The body length must be a multiple of 36.
//
// Response modes (selected by the route, mirroring GetUTXO):
//
//  1. BINARY_STREAM (POST /api/v1/utxos)
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: Concatenated fixed-length 48-byte records in input order:
//     [8 bytes status LE][4 bytes lockTime LE][4 bytes vin LE][32 bytes spendingTxID].
//     For unspent UTXOs the trailing 36 bytes (vin + spendingTxID) are zero-filled.
//     For records not found in the store, status is utxo.Status_NOT_FOUND and
//     the remaining bytes are zero.
//
//  2. HEX (POST /api/v1/utxos/hex)
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Lowercase hex-encoding of the BINARY_STREAM body. Decoded length is
//     numRecords * 48 bytes; clients can decode then index as above.
//
//  3. JSON (POST /api/v1/utxos/json)
//     Status: 200 OK
//     Content-Type: application/json
//     Body: JSON array of utxo.SpendResponse objects, in input order, e.g.
//     [{"status":1,"spendingData":{"txId":"...","vin":0},"lockTime":1234567},
//     {"status":3}, ...]
//
// The whole-request HTTP status is unchanged by per-record outcomes.
//
// Error Responses (all modes):
//
//   - 400 Bad Request: body length is not a multiple of 36 bytes.
//   - 413 Request Entity Too Large: body exceeds the global asset_httpBodyLimit setting.
//   - 429 Too Many Requests: heavy-route tiered rate limiter rejected the request
//     (shared with /subtree/:hash/txs and other fan-out endpoints).
//   - 500 Internal Server Error: transport error reading the body, an unrecoverable
//     repository error from at least one record, or a per-record panic recovered
//     inside the fan-out (the panic is logged but the response body does not
//     include the panic value, to avoid leaking internal driver details).
//
// Performance notes:
//   - Bypasses the per-record full-transaction fetch that GET /api/v1/utxo/:hash
//     used to do (see GetUTXO.go). Each lookup is a single Aerospike record read.
//   - Concurrent lookups are bounded by utxosFanoutLimit.
//   - Each goroutine writes its own slot in a preallocated []*SpendResponse, so
//     no mutex is needed.
//
// Monitoring:
//   - Execution time recorded under "GetUTXOs_http" via the asset tracer.
//   - prometheusAssetHTTPGetUTXOs is incremented by the number of records served.
func (h *HTTP) GetUTXOs(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetUTXOs_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithLogMessage(h.logger, "[Asset_http:GetUTXOs] in %s for %s", mode, c.Request().RemoteAddr),
		)

		defer deferFn()

		reqBytes, err := readUTXOsBody(c)
		if err != nil {
			return err
		}

		numRecords := len(reqBytes) / utxosRequestRecordSize
		if numRecords == 0 {
			return writeUTXOsResponse(c, mode, nil)
		}

		// Each goroutine writes its own slot — slice indexing is the
		// happens-before barrier, no mutex required.
		results := make([]*utxo.SpendResponse, numRecords)

		g, gCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(g, utxosFanoutLimit)

		for i := 0; i < numRecords; i++ {
			recOffset := i * utxosRequestRecordSize

			var txHash chainhash.Hash
			copy(txHash[:], reqBytes[recOffset:recOffset+32])

			vout := binary.LittleEndian.Uint32(reqBytes[recOffset+32 : recOffset+36])

			g.Go(func() (retErr error) {
				// Echo's middleware.Recover only protects the request goroutine —
				// not the ones errgroup spawns. Without this defer, a per-record
				// panic (e.g. an index-out-of-range deep in a store driver) would
				// crash the asset process.
				defer func() {
					if r := recover(); r != nil {
						h.logger.Errorf("[Asset_http:GetUTXOs] recovered panic on %s:%d: %v", txHash.String(), vout, r)
						retErr = echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("internal error getting utxo %s:%d", txHash.String(), vout).Error())
					}
				}()

				resp, err := h.fetchOneUTXO(gCtx, &txHash, vout)
				if err != nil {
					return err
				}

				results[i] = resp

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			h.logger.Errorf("[Asset_http:GetUTXOs] fan-out failed: %s", err.Error())

			return err
		}

		prometheusAssetHTTPGetUTXOs.WithLabelValues("OK", "200").Add(float64(numRecords))

		h.logger.Infof("[Asset_http:GetUTXOs] served %d records in %s", numRecords, mode)

		return writeUTXOsResponse(c, mode, results)
	}
}

// readUTXOsBody consumes the request body, maps the body-limit-exceeded error
// to 413 (so streaming clients without Content-Length get the documented
// status), and validates that the body length is a non-negative multiple of
// utxosRequestRecordSize. Returns the raw bytes or an echo.HTTPError ready to
// be returned from the handler.
func readUTXOsBody(c echo.Context) ([]byte, error) {
	body := c.Request().Body
	defer func() { _ = body.Close() }()

	reqBytes, err := io.ReadAll(body)
	if err != nil {
		if errors.Is(err, echo.ErrStatusRequestEntityTooLarge) {
			return nil, echo.NewHTTPError(http.StatusRequestEntityTooLarge, errors.NewInvalidArgumentError("request body exceeds asset_httpBodyLimit").Error())
		}

		return nil, echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error reading request body", err).Error())
	}

	if len(reqBytes)%utxosRequestRecordSize != 0 {
		return nil, echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("body length %d is not a multiple of %d bytes", len(reqBytes), utxosRequestRecordSize).Error())
	}

	return reqBytes, nil
}

// fetchOneUTXO performs the bulk endpoint's per-record store lookup.
// ErrNotFound (and a nil resp) become a per-record Status_NOT_FOUND; any
// other store error is wrapped as an echo HTTPError so the whole request
// fails (preferred over silently zero-padding every other record when the
// store is unreachable). Panics are NOT recovered here — the caller's
// goroutine wrapper handles that.
func (h *HTTP) fetchOneUTXO(ctx context.Context, txHash *chainhash.Hash, vout uint32) (*utxo.SpendResponse, error) {
	resp, err := h.repository.GetUtxo(ctx, &utxo.Spend{
		TxID:         txHash,
		Vout:         vout,
		UTXOHash:     nil,
		SpendingData: nil,
	})
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			return &utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil
		}

		return nil, echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error getting utxo %s:%d", txHash.String(), vout, err).Error())
	}

	if resp == nil {
		return &utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil
	}

	return resp, nil
}

// writeUTXOsResponse serializes results in the requested mode and writes the
// HTTP response. nil results means zero records (empty body).
func writeUTXOsResponse(c echo.Context, mode ReadMode, results []*utxo.SpendResponse) error {
	switch mode {
	case JSON:
		// nil is fine — echo writes "null", which is the natural JSON for an
		// absent payload. Tests prefer an empty array; use that.
		if results == nil {
			results = []*utxo.SpendResponse{}
		}

		return c.JSON(http.StatusOK, results)

	case HEX:
		buf := encodeUTXOsBinary(results)

		return c.String(http.StatusOK, hex.EncodeToString(buf))

	case BINARY_STREAM:
		buf := encodeUTXOsBinary(results)

		return c.Blob(http.StatusOK, echo.MIMEOctetStream, buf)

	default:
		return echo.NewHTTPError(http.StatusInternalServerError, errors.NewInvalidArgumentError("bad read mode").Error())
	}
}

// encodeUTXOsBinary serializes results into the on-wire fixed-length binary
// format: each record is utxosResponseRecordSize bytes, in input order.
func encodeUTXOsBinary(results []*utxo.SpendResponse) []byte {
	buf := make([]byte, len(results)*utxosResponseRecordSize)

	for i, r := range results {
		slotStart := i * utxosResponseRecordSize
		writeUTXOsRecord(buf[slotStart:slotStart+utxosResponseRecordSize], r)
	}

	return buf
}

// writeUTXOsRecord encodes a SpendResponse into dst as a fixed 48-byte record.
// Layout: [8 bytes status LE][4 bytes lockTime LE][4 bytes spendingVin LE][32 bytes spendingTxID].
// dst must have len >= utxosResponseRecordSize. Caller-zeroed slots stay zero
// when SpendingData is nil.
func writeUTXOsRecord(dst []byte, resp *utxo.SpendResponse) {
	binary.LittleEndian.PutUint64(dst[0:8], uint64(resp.Status)) //nolint:gosec
	binary.LittleEndian.PutUint32(dst[8:12], resp.LockTime)

	if resp.SpendingData != nil {
		binary.LittleEndian.PutUint32(dst[12:16], uint32(resp.SpendingData.Vin)) //nolint:gosec

		if resp.SpendingData.TxID != nil {
			copy(dst[16:48], resp.SpendingData.TxID[:])
		}
	}
}
