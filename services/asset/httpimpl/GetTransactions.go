package httpimpl

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
	"golang.org/x/sync/errgroup"
)

// GetTransactions creates an HTTP handler for retrieving multiple transactions in a single request.
// It accepts a stream of transaction hashes and returns concatenated transaction data.
//
// HTTP Method:
//   - POST
//
// Request:
//
//	Content-Type: application/octet-stream
//	Body: Concatenated 32-byte transaction hashes
//	Note: Each hash must be exactly 32 bytes
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/octet-stream
//	Body: Concatenated transaction data for all found transactions
//
// Error Responses:
//
//   - 404 Not Found:
//
//   - One or more transactions not found
//     Example: {"message": "not found"}
//
//   - 500 Internal Server Error:
//
//   - Error reading request body
//
//   - Invalid hash format
//
//   - Repository errors
//
// Performance:
//   - Concurrent transaction retrieval (up to 1024 goroutines)
//   - The response buffer is sized from the serialized transactions, not from
//     the requested record count
//   - POST /transactions (no subtree hash) dispatches each hash to a lookup as
//     it is read from the request body, rather than reading the whole body
//     before any lookup starts, and the per-record result slice grows one slot
//     per hash read, not presized from a request-supplied count
//   - POST /subtree/:hash/txs reads the whole (bounded) body before dispatching
//     any lookup, because GetSubtreeTransactions takes a node-wide permit
//     (asset_concurrency_get_subtree_transactions): dispatching while reading
//     would hold that shared permit for the client-paced upload, not just the
//     lookup fan-out
//
// Monitoring:
//   - Execution time recorded in "GetTransactions_http" statistic
//   - Prometheus metric "asset_http_get_transactions" tracks number of transactions
//   - Debug logging includes:
//   - Number of transactions processed
//   - Total response size
//   - Processing duration
//
// Example Usage:
//
//	# Request multiple transactions
//	POST /transactions
//	Body: <32-byte-hash1><32-byte-hash2>...
//
// Notes:
//   - Each transaction hash in the request must be exactly 32 bytes
//   - Response contains only found transactions
//   - Transactions are retrieved concurrently for better performance
//   - Response order matches request order
func (h *HTTP) GetTransactions() func(c echo.Context) error {
	return func(c echo.Context) error {
		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetTransactions_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http:GetTransactions] for %s", c.RealIP()),
		)

		defer deferFn()

		var subtreeHash *chainhash.Hash

		// this if statement is temporary and will be removed when the subtree data is implemented
		if len(c.Param("hash")) > 0 {
			if len(c.Param("hash")) != 64 {
				return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid subtree hash length").Error())
			}

			hash, err := chainhash.NewHashFromStr(c.Param("hash"))
			if err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid subtree hash string", err).Error())
			}

			// check whether the subtree exists
			subtreeExists, err := h.repository.GetSubtreeExists(ctx, hash)
			if err != nil {
				if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "no such file") {
					return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("subtree %s not found", hash.String(), err).Error())
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error getting subtree", hash.String(), err).Error())
				}
			}

			if !subtreeExists {
				return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("subtree %s not found", hash.String()).Error())
			}

			subtreeHash = hash
		}

		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEOctetStream)

		body := c.Request().Body
		defer func() {
			_ = body.Close()
		}()

		if subtreeHash != nil {
			return h.getTransactionsFromSubtree(ctx, c, subtreeHash, body)
		}

		return h.getTransactionsStreaming(ctx, c, body)
	}
}

// lookupAndStoreTransaction resolves hash — via transactionFromSubtreeData when
// present, otherwise the UTXO store — reserves the response-byte budget, and
// stores the serialized result in (*parts)[idx]. Shared by both GetTransactions
// code paths below.
//
// parts is a pointer, not a slice, and every access to *parts here happens under
// partsMu: on the streaming path the caller keeps growing the underlying slice
// with append after some lookups are already dispatched, so reading the slice
// header itself (not just indexing into it) must be synchronised, or a goroutine
// here can race the caller's append.
func (h *HTTP) lookupAndStoreTransaction(gCtx context.Context, hash chainhash.Hash, idx int,
	transactionFromSubtreeData map[chainhash.Hash]*bt.Tx, parts *[][]byte, partsMu *sync.Mutex,
	responseSize *atomic.Int64, maxBytes int64) (retErr error) {
	// Echo's middleware.Recover only protects the request goroutine — not the
	// ones errgroup spawns. The hashes come straight from the request body, so
	// without this defer a single caller-chosen txid that panics on
	// serialization (e.g. a tx reconstructed from an .outputs-only external
	// blob, which carries nil *bt.Output holes) would crash the asset process.
	defer func() {
		if r := recover(); r != nil {
			h.logger.Errorf("[Asset_http:GetTransactions] recovered panic on %s: %v", hash.String(), r)
			retErr = echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("internal error getting transaction %s", hash.String()).Error())
		}
	}()

	// A sibling lookup has already failed, or the client went away: skip the
	// work rather than serializing a transaction nobody will read.
	if gCtx.Err() != nil {
		return gCtx.Err()
	}

	var b []byte

	if tx, ok := transactionFromSubtreeData[hash]; ok {
		// Reserve the response-byte budget from the computed size before
		// serializing, so a batch that trips the budget never allocates this
		// record's bytes. tx.Size() is the non-extended size tx.Bytes() writes.
		if err := h.enforceBatchResponseBytes("GetTransactions", responseSize.Add(int64(tx.Size())), maxBytes); err != nil {
			return err
		}

		// always write the non-extended normal bytes as a response !
		// our peer node should extend the transactions if needed
		b = tx.Bytes()
	} else {
		var err error

		b, err = h.repository.GetTransaction(gCtx, &hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "no such file") {
				return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("transaction not found", err).Error())
			}

			return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error getting transaction", err).Error())
		}

		// The store hands back already-allocated bytes, so the budget can only be
		// checked after the fetch; a batch that trips it on this record still
		// doesn't retain the bytes in parts.
		if err := h.enforceBatchResponseBytes("GetTransactions", responseSize.Add(int64(len(b))), maxBytes); err != nil {
			return err
		}
	}

	partsMu.Lock()
	(*parts)[idx] = b
	partsMu.Unlock()

	return nil
}

// subtreeBatchHashHintCap bounds the initial capacity readSubtreeBatchHashes takes
// up front, whatever the hint came from: Content-Length is client-supplied, and
// asset_maxBatchRecords can be large while the request sends no Content-Length at
// all. Either way a slow client could otherwise make every connection hold a large
// allocation before sending a single hash. 1024 hashes is 32 KiB; a larger body
// grows the slice as it is actually read.
const subtreeBatchHashHintCap = 1024

// readSubtreeBatchHashes reads every 32-byte txid from body into a hash slice,
// growing it as it reads rather than presizing it from a value the request
// supplies: the slice's initial capacity is a hint taken from contentLength and
// maxRecords, always capped by subtreeBatchHashHintCap, and never determines its
// final size. maxRecords is
// enforced as hashes are read, exactly as the dispatch-while-reading path
// enforces it: an over-budget request is rejected here, before
// GetSubtreeTransactions (and the permit it takes) is ever reached.
func readSubtreeBatchHashes(body io.Reader, contentLength int64, maxRecords int) ([]chainhash.Hash, error) {
	hint := 0

	if contentLength > 0 {
		hint = int(contentLength / chainhash.HashSize)
	}

	if maxRecords > 0 && (hint == 0 || hint > maxRecords) {
		hint = maxRecords
	}

	// Cap last, so neither a forged Content-Length nor a large operator
	// maxRecords reached through a request with no Content-Length can size the
	// up-front allocation.
	if hint > subtreeBatchHashHintCap {
		hint = subtreeBatchHashHintCap
	}

	hashes := make([]chainhash.Hash, 0, hint)

	for {
		var hash chainhash.Hash

		if _, err := io.ReadFull(body, hash[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error reading request body", err).Error())
		}

		if maxRecords > 0 && len(hashes) >= maxRecords {
			return nil, errBatchRecords(maxRecords)
		}

		hashes = append(hashes, hash)
	}

	return hashes, nil
}

// getTransactionsFromSubtree serves POST /subtree/:hash/txs.
// GetSubtreeTransactions takes a node-wide permit
// (asset_concurrency_get_subtree_transactions, default 2) to bound the
// underlying subtree-data deserialization. Dispatching lookups while reading the
// request body — as the plain POST /transactions path does — would hold that
// permit for the whole client-paced upload: a couple of slow anonymous uploads
// can then pin both permits for up to the HTTP read timeout, starving every
// other caller of this route, including honest peer catchup. So on this path the
// body is read to completion, entirely off that permit, before the permit is
// ever taken: its lifetime is bounded to the lookup fan-out below, never the
// upload.
func (h *HTTP) getTransactionsFromSubtree(ctx context.Context, c echo.Context, subtreeHash *chainhash.Hash, body io.Reader) error {
	maxRecords := h.maxBatchRecords()

	hashes, err := readSubtreeBatchHashes(body, c.Request().ContentLength, maxRecords)
	if err != nil {
		return err
	}

	h.observeBatchRecords("GetTransactions", len(hashes))

	if len(hashes) == 0 {
		return c.Blob(http.StatusOK, echo.MIMEOctetStream, nil)
	}

	transactionFromSubtreeData := make(map[chainhash.Hash]*bt.Tx)

	// The permit covers the map, so it is held for as long as the map is read -
	// which ends with the fan-out below, not with the response. Holding it across
	// the write would pace a node-wide semaphore off the client's read speed.
	releaseSubtreeMap := func() {}
	defer func() { releaseSubtreeMap() }()

	// read the data from the subtreeData file and create a map of transaction hashes to transactions
	txMap, release, err := h.repository.GetSubtreeTransactions(ctx, subtreeHash)

	releaseSubtreeMap = release

	if err != nil {
		// this should not be an ERROR, but a warning, because it is not critical if the subtree data is not available
		// it just means that the transactions will be fetched from the utxo store
		h.logger.Debugf("[Asset_http:GetTransactions][%s] error getting transactions from subtree data: %s", subtreeHash.String(), err.Error())
	} else {
		transactionFromSubtreeData = txMap
	}

	maxBytes := h.maxBatchResponseBytes()

	parts := make([][]byte, len(hashes))

	var (
		partsMu      sync.Mutex
		responseSize atomic.Int64
	)

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(h.logger, g, 1024)

	for idx, hash := range hashes {
		idx, hash := idx, hash

		g.Go(func() error {
			return h.lookupAndStoreTransaction(gCtx, hash, idx, transactionFromSubtreeData, &parts, &partsMu, &responseSize, maxBytes)
		})
	}

	waitErr := g.Wait()

	// The map is no longer read past this point, so drop the permit before the
	// client-paced response write. release is sync.Once-guarded, so the deferred
	// call above stays safe.
	releaseSubtreeMap()

	if waitErr != nil {
		h.logger.Errorf("failed to get txs from repository: %s", waitErr.Error())
		return waitErr
	}

	responseBytes := concatTransactionBytes(parts)

	prometheusAssetHTTPGetTransactions.WithLabelValues("OK", "200").Add(float64(len(hashes)))

	h.logger.Debugf("[Asset_http:GetTransactions] sending %d txs to client (%d bytes)", len(hashes), len(responseBytes))

	return c.Blob(http.StatusOK, echo.MIMEOctetStream, responseBytes)
}

// getTransactionsStreaming serves POST /transactions (no subtree hash): each hash
// is dispatched to a lookup as it is read from body, and the read loop stops
// once a dispatched lookup has failed or the client's context is cancelled. This
// path takes no node-wide permit, so pacing dispatch to the client's upload
// speed does not pin a shared budget the way the subtree path's
// GetSubtreeTransactions permit would.
func (h *HTTP) getTransactionsStreaming(ctx context.Context, c echo.Context, body io.Reader) error {
	maxRecords := h.maxBatchRecords()
	maxBytes := h.maxBatchResponseBytes()

	var (
		partsMu      sync.Mutex
		parts        [][]byte
		responseSize atomic.Int64
		recordsRead  int
	)

	// A derived, explicitly cancellable context: once the read loop stops on a
	// read/budget error (firstErr below), cancel cuts short any lookup already
	// dispatched but not yet observing the errgroup's own cancellation (which
	// only fires once a goroutine itself returns an error), instead of letting
	// up to 1024 of them run to completion after the request is already known
	// to fail.
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gCtx := errgroup.WithContext(cancelCtx)
	util.SafeSetLimit(h.logger, g, 1024)

	// dispatch budgets and launches the lookup for a single hash, appending it
	// its own slot in parts. parts grows one slot per hash actually read, so
	// it is never presized from a request-supplied count; the mutex guards
	// both the append and every goroutine's indexed write, since a later
	// append can reallocate the backing array while an earlier goroutine is
	// still writing to its slot.
	dispatch := func(hash chainhash.Hash) error {
		if maxRecords > 0 && recordsRead >= maxRecords {
			return errBatchRecords(maxRecords)
		}

		recordsRead++

		partsMu.Lock()
		idx := len(parts)
		parts = append(parts, nil)
		partsMu.Unlock()

		g.Go(func() error {
			return h.lookupAndStoreTransaction(gCtx, hash, idx, nil, &parts, &partsMu, &responseSize, maxBytes)
		})

		return nil
	}

	var hash chainhash.Hash

	if _, err := io.ReadFull(body, hash[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return c.Blob(http.StatusOK, echo.MIMEOctetStream, nil)
		}

		return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error reading request body", err).Error())
	}

	// firstErr is the read/budget error that stopped the read loop, if any.
	// It takes priority over waitErr: it is the reason no more hashes were
	// read, whereas waitErr is whatever a dispatched lookup returned.
	var firstErr error

	if err := dispatch(hash); err != nil {
		firstErr = err
	}

	for firstErr == nil {
		if gCtx.Err() != nil {
			// A dispatched lookup already failed, or the client's context was
			// cancelled: stop reading more hashes from a body that can no
			// longer produce a successful response.
			break
		}

		if _, err := io.ReadFull(body, hash[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			firstErr = echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error reading request body", err).Error())

			break
		}

		if err := dispatch(hash); err != nil {
			firstErr = err
			break
		}
	}

	if firstErr != nil {
		cancel()
	}

	h.observeBatchRecords("GetTransactions", recordsRead)

	waitErr := g.Wait()

	if firstErr != nil {
		return firstErr
	}

	if waitErr != nil {
		h.logger.Errorf("failed to get txs from repository: %s", waitErr.Error())
		return waitErr
	}

	responseBytes := concatTransactionBytes(parts)

	prometheusAssetHTTPGetTransactions.WithLabelValues("OK", "200").Add(float64(recordsRead))

	h.logger.Debugf("[Asset_http:GetTransactions] sending %d txs to client (%d bytes)", recordsRead, len(responseBytes))

	return c.Blob(http.StatusOK, echo.MIMEOctetStream, responseBytes)
}

// concatTransactionBytes joins the per-record serialized transactions into a
// single buffer whose capacity is exactly the serialized length. The handler
// used to reserve a flat 32MB before reading the first body byte, so every
// in-flight request on this unauthenticated route cost 32MB regardless of how
// much data it actually asked for. Callers build parts by growing it one slot
// per hash read from the request body, not by presizing it from a
// request-supplied record count, so this function never sees more capacity
// than records actually dispatched.
func concatTransactionBytes(parts [][]byte) []byte {
	total := 0

	for _, p := range parts {
		total += len(p)
	}

	buf := make([]byte, 0, total)

	for _, p := range parts {
		buf = append(buf, p...)
	}

	return buf
}

// batchBudgetWarnInterval rate-limits the "would have been rejected" warnings so
// a flood of oversized batches cannot itself become a logging DoS.
const batchBudgetWarnInterval = time.Minute

// batchResponseWarnBytes is the observation threshold for the response-byte
// budget while asset_maxBatchResponseBytes is unset. 32MiB is the size the old
// unconditional preallocation implicitly treated as a normal batch response.
const batchResponseWarnBytes = 32 * 1024 * 1024

// batchBudgetWarner emits at most one warning per batchBudgetWarnInterval.
type batchBudgetWarner struct {
	lastUnixNano atomic.Int64
}

func (w *batchBudgetWarner) allow() bool {
	now := time.Now().UnixNano()
	last := w.lastUnixNano.Load()

	if now-last < int64(batchBudgetWarnInterval) {
		return false
	}

	return w.lastUnixNano.CompareAndSwap(last, now)
}

//nolint:gochecknoglobals // rate-limit state for the budget warnings, shared by the batch handlers
var (
	batchRecordWarner   batchBudgetWarner
	batchResponseWarner batchBudgetWarner
)

// maxBatchRecords returns the record cap enforced on POST /subtree/:hash/txs.
//
// asset_maxBatchRecords is the operator knob, but this route is the peer-catchup
// path: subtree validation posts subtreevalidation_missingTransactionsBatchSize
// txids (16384 by default) in one request, and a rejection there is not retried
// — it feeds publishInvalidSubtree and demotes an honest peer. The cap is
// therefore never enforced below the configured client batch size, so a
// mis-sized asset_maxBatchRecords cannot silently break synchronisation.
func (h *HTTP) maxBatchRecords() int {
	configured := h.settings.Asset.MaxBatchRecords
	if configured <= 0 {
		return 0
	}

	if floor := h.settings.SubtreeValidation.MissingTransactionsBatchSize; configured < floor {
		return floor
	}

	return configured
}

// errBatchRecords builds the rejection returned once the record budget is set
// and exceeded.
func errBatchRecords(maxRecords int) error {
	return echo.NewHTTPError(http.StatusRequestEntityTooLarge, errors.NewInvalidArgumentError("batch exceeds asset_maxBatchRecords (%d)", maxRecords).Error())
}

// observeBatchRecords logs a rate-limited warning when a batch would have
// breached a record budget the operator has not set yet, so the setting can be
// sized from real traffic before it is turned on.
func (h *HTTP) observeBatchRecords(route string, records int) {
	if h.settings.Asset.MaxBatchRecords > 0 {
		return
	}

	threshold := h.settings.SubtreeValidation.MissingTransactionsBatchSize
	if threshold <= 0 || records <= threshold {
		return
	}

	if batchRecordWarner.allow() {
		h.logger.Warnf("[Asset_http:%s] batch of %d records exceeds the catchup-sized threshold %d, asset_maxBatchRecords is unset (unlimited)", route, records, threshold)
	}
}

// catchupResponseBytesPerTx is the per-transaction size used to floor
// asset_maxBatchResponseBytes at a legitimate catchup batch. It matches the ratio
// batchResponseWarnBytes already assumes for the default batch size (32MiB / 16384
// = 2KiB/tx) — an average for small transactions, NOT a conservative (i.e.
// worst-case) figure. A subtree whose transactions run well above 2KiB (e.g. a
// chain carrying large data-carrier transactions) can still produce a batch larger
// than this floor. The floor only guarantees a configured value can't be set below
// small-transaction traffic; it does not guarantee a configured value is safe for
// this node's actual transaction sizes. See asset_maxBatchResponseBytes's longdesc.
const catchupResponseBytesPerTx = 2048

// maxBatchResponseBytes returns the response-byte cap enforced on POST
// /subtree/:hash/txs, floored the same way maxBatchRecords is.
//
// asset_maxBatchResponseBytes is the operator knob, but /subtree/:hash/txs is the
// peer-catchup path: subtree validation posts subtreevalidation_missingTransactionsBatchSize
// txids (16384 by default) in one request, each resolving to a full transaction. A
// 413 there is not retried — it feeds publishInvalidSubtree and demotes an honest
// peer. The cap is therefore never enforced below catchupResponseBytesPerTx's
// small-transaction estimate; see that constant's doc for why this floor is not a
// substitute for sizing the setting itself against this chain's actual transaction
// sizes.
//
// POST /utxos is not on the peer-catchup path (see GetUTXOs.go), so it enforces
// the operator's configured value directly, unfloored, the same as its record
// budget.
func (h *HTTP) maxBatchResponseBytes() int64 {
	configured := h.settings.Asset.MaxBatchResponseBytes
	if configured <= 0 {
		return 0
	}

	if floor := int64(h.settings.SubtreeValidation.MissingTransactionsBatchSize) * catchupResponseBytesPerTx; configured < floor {
		return floor
	}

	return configured
}

// enforceBatchResponseBytes rejects a batch whose accumulated response exceeds
// maxBytes, and otherwise records a rate-limited observation against the
// unfloored asset_maxBatchResponseBytes setting. A record budget alone does not
// bound output: duplicate txids in one batch each expand into a full transaction.
func (h *HTTP) enforceBatchResponseBytes(route string, total, maxBytes int64) error {
	if maxBytes > 0 {
		if total > maxBytes {
			return echo.NewHTTPError(http.StatusRequestEntityTooLarge, errors.NewInvalidArgumentError("batch response exceeds asset_maxBatchResponseBytes (%d)", maxBytes).Error())
		}

		return nil
	}

	if total > batchResponseWarnBytes && batchResponseWarner.allow() {
		h.logger.Warnf("[Asset_http:%s] batch response of %d bytes exceeds %d, asset_maxBatchResponseBytes is unset (unlimited)", route, total, batchResponseWarnBytes)
	}

	return nil
}
