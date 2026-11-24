package httpimpl

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
	"golang.org/x/sync/errgroup"
)

const (
	// utxoStatusMaxRecords is the hard per-request cap for POST /api/v1/utxos/status.
	// Per record the worst case is one output-status read, one funding-record read
	// (the full transaction body), one spender block-reference read (block IDs and
	// heights only, no transaction body) and one main-chain check per block the
	// transaction sits in (a remote call when the main-chain cache does not cover the
	// height). Funding and spender records are memoised per transaction, and each
	// distinct block height costs one header lookup per request. The cap is far below
	// the asset_maxBatchRecords default used by the 48-byte route. The configured
	// asset_maxBatchRecords still applies when it is lower.
	utxoStatusMaxRecords = 1024

	// utxoStatusBodyReads bounds how many funding-record reads (each loads a full
	// transaction body, possibly from an external blob) this route runs at once
	// across the whole process, over all requests. Each read also takes a
	// GetTransactionMeta permit (asset_concurrency_get_transaction_meta), shared
	// with the other routes that read transaction records. Only the requested
	// outputs' value, script hash and UTXO hash are kept after each read. A store
	// that batches reads in a time window (Aerospike's get batcher, 10 ms by
	// default) answers about this many per window, so a lower bound costs latency
	// on large batches: 1024 distinct funding transactions take up to
	// 1024/utxoStatusBodyReads windows.
	utxoStatusBodyReads = 32

	// utxoStatusRecordMaxBytes is an upper bound on the JSON size of one result,
	// used to charge asset_maxBatchResponseBytes before any lookup runs. It holds
	// for the indented ?pretty form too, where a record with every field at its
	// longest is under 800 bytes (a test measures it).
	utxoStatusRecordMaxBytes = 1024

	// UTXO status values reported by POST /api/v1/utxos/status.
	utxoStatusUnspent  = "unspent"
	utxoStatusSpent    = "spent"
	utxoStatusUnknown  = "unknown"
	utxoReasonPruned   = "not_found_may_be_pruned"
	utxoReasonStore    = "store_unavailable"
	utxoReasonVout     = "output_index_out_of_range"
	utxoReasonFrozen   = "frozen"
	utxoReasonConflict = "conflicting"
	utxoReasonLocked   = "locked"
	utxoReasonOther    = "unrecognised_store_status"
	utxoReasonNoOutput = "output_not_found"
	utxoReasonUnspend  = "unspendable"
	utxoReasonMaturity = "maturity_unknown"
	utxoReasonHeld     = "held"
	utxoReasonReassign = "reassigned"
	utxoReasonScript   = "script_rules_unknown"
)

// utxoStatusBodySem bounds the funding-record reads of every
// POST /api/v1/utxos/status request in this process together.
var utxoStatusBodySem = make(chan struct{}, utxoStatusBodyReads)

// UTXOStatusBlockRef names the best block the batch was read against.
type UTXOStatusBlockRef struct {
	Hash   string `json:"hash"`
	Height uint32 `json:"height"`
}

// UTXOStatusFunding describes the transaction that created the output.
type UTXOStatusFunding struct {
	// Mined is true when the transaction is in a block on the current main chain.
	Mined bool `json:"mined"`
	// Unconfirmed is true when the node holds the transaction but it is not in a
	// main-chain block: it is unmined, or only in blocks that were reorged out. It
	// does not claim the transaction sits in any mempool.
	Unconfirmed bool   `json:"unconfirmed"`
	BlockHeight uint32 `json:"blockHeight,omitempty"`
	BlockHash   string `json:"blockHash,omitempty"`
}

// UTXOStatusSpender describes the input that spent the output.
type UTXOStatusSpender struct {
	TxID string `json:"txid"`
	Vin  int    `json:"vin"`
	// Mined is true when the spending transaction is in a main-chain block whose
	// ID matches the stored block at that height, and false when the spender is
	// known to be unmined (or only in blocks off the main chain). It is absent
	// when that cannot be established: the spender's record is not held or cannot
	// be read, or a main-chain or block check failed. BlockHeight is only set when
	// Mined is true.
	Mined       *bool  `json:"mined,omitempty"`
	BlockHeight uint32 `json:"blockHeight,omitempty"`
}

// UTXOStatusRecord is the answer for one requested outpoint.
type UTXOStatusRecord struct {
	TxID   string `json:"txid"`
	Vout   uint32 `json:"vout"`
	Status string `json:"status"`
	// Reason is set only when Status is "unknown".
	Reason string `json:"reason,omitempty"`
	// Satoshis and LockingScriptSha256 are set when the node still holds the
	// funding transaction body. Absent means "not known", not zero.
	Satoshis            *uint64 `json:"satoshis,omitempty"`
	LockingScriptSha256 string  `json:"lockingScriptSha256,omitempty"`
	// Immature is true when the output is not yet spendable: coinbase maturity,
	// or a spend-height hold set by the alert system or a reassignment. A coinbase
	// output whose maturity cannot be checked, and a hold that may hide another
	// state, are reported as unknown instead.
	Immature bool `json:"immature,omitempty"`
	// Funding is absent when the node cannot establish where the funding
	// transaction is (a failed main-chain check, no block evidence, or a block
	// whose ID and height disagree). Absent means unknown, not "unconfirmed".
	Funding *UTXOStatusFunding `json:"funding,omitempty"`
	Spender *UTXOStatusSpender `json:"spender,omitempty"`
}

// UTXOStatusResponse is the body of POST /api/v1/utxos/status.
type UTXOStatusResponse struct {
	// BestBlock is the best block read before any outpoint was looked up.
	BestBlock UTXOStatusBlockRef `json:"bestBlock"`
	// BestBlockChanged is true when the best block read after the lookups differs
	// from BestBlock, i.e. the chain moved while the batch was being answered.
	// Callers that need a single consistent view should retry.
	BestBlockChanged bool               `json:"bestBlockChanged"`
	Results          []UTXOStatusRecord `json:"results"`
}

// GetUTXOStatuses creates an HTTP handler for POST /api/v1/utxos/status, a
// bulk outpoint lookup that is richer than POST /api/v1/utxos.
//
// Request body: concatenated 36-byte records [32 bytes txid][4 bytes vout LE],
// the same as POST /api/v1/utxos. At least one and at most
// min(utxoStatusMaxRecords, asset_maxBatchRecords) records.
//
// Response (200, application/json): UTXOStatusResponse, results in input order.
//
// Each result has status "unspent", "spent" or "unknown". "unknown" always
// carries a reason and is never a guess: a transaction the store does not hold
// may never have existed or may have been spent and pruned, so it is reported
// as unknown (not_found_may_be_pruned), never as spent or unspent. A
// per-record store failure is reported as unknown (store_unavailable) instead
// of failing the batch.
//
// An output the store never keeps as a UTXO (OP_FALSE OP_RETURN, and before
// Genesis a bare OP_RETURN or an oversized script) is reported as unknown
// (unspendable), because some stores answer OK for it. That check needs the
// funding transaction body; without the body it cannot run. A record seeded
// from a UTXO snapshot holds only the outputs that were unspent then, with a
// hole at each spent one, and Aerospike answers OK for a hole; a hole is
// reported as unknown (output_not_found), and no value or script hash is taken
// from a body that is not the complete requested transaction. An output the
// store has no entry for while it still holds the transaction's record (with or
// without a body) is unknown (output_not_found); not_found_may_be_pruned is kept
// for a transaction whose record the store does not hold. An output the store
// holds whose funding record cannot be read is unknown (store_unavailable), and
// one whose record has gone is unknown (not_found_may_be_pruned), because
// neither that check nor the coinbase maturity check could run.
//
// The script rules differ before and after Genesis. Where they give different
// answers for an output, the funding block must be placed on the main chain
// (or the transaction known to be unmined); otherwise the output is unknown
// (script_rules_unknown).
//
// When the value and script hash come from the complete requested transaction,
// the output's UTXO hash is computed from that body and the store's commitment
// is checked against it. A mismatch, for example after an alert-system
// reassignment gave the output a new owner, is unknown (reassigned): the stored
// output no longer belongs to the script the body names.
//
// A transaction marked conflicting or locked is unknown with that reason even
// where the store answers OK for the output: on Aerospike those flags live on
// the transaction's master record, and an output held on a child page
// (vout >= utxoBatchSize) reads OK without them.
//
// Coinbase maturity is derived from the funding block height and the best
// height, because Aerospike's GetSpend does not apply it. A coinbase output
// whose funding block is not placed on the main chain (a failed main-chain
// check, a block ID that does not match the block at that height, or a block
// that was reorged out) is unknown (maturity_unknown).
//
// A store's IMMATURE status can replace the output's own state: Aerospike
// applies a spend-height hold over SPENT, and SQL and pebble apply it after
// frozen, locked and conflicting. A held output whose store answer carries
// spending data (spent, or the frozen marker) is unknown (held); one whose
// transaction is marked conflicting or locked is unknown with that reason.
//
// Consistency: the best block is read once before the lookups and once after.
// Records are read one by one from the store at slightly different moments, so
// the answer is not an atomic snapshot. bestBlockChanged=false does not prove a
// consistent view: the main-chain cache window can lag the best block, the
// store's own height (used for IMMATURE and spend-height holds) is separate from
// the blockchain best block, and a reorg back to the same tip is not detected.
//
// Error responses: 400 (empty body or length not a multiple of 36), 413 (batch
// too large or body over asset_httpBodyLimit), 429 (heavy-route rate limit),
// 503 (best block cannot be read), 500 (transport error).
func (h *HTTP) GetUTXOStatuses() func(c echo.Context) error {
	return func(c echo.Context) error {
		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetUTXOStatuses_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithDebugLogMessage(h.logger, "[Asset_http:GetUTXOStatuses] for %s", c.Request().RemoteAddr),
		)

		defer deferFn()

		reqBytes, err := readUTXOsBody(c)
		if err != nil {
			return err
		}

		numRecords := len(reqBytes) / utxosRequestRecordSize
		if numRecords == 0 {
			return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("request body is empty").Error())
		}

		maxRecords := utxoStatusMaxRecords
		if configured := h.settings.Asset.MaxBatchRecords; configured > 0 && configured < maxRecords {
			maxRecords = configured
		}

		if numRecords > maxRecords {
			if maxRecords == h.settings.Asset.MaxBatchRecords {
				return errBatchRecords(maxRecords)
			}

			return echo.NewHTTPError(http.StatusRequestEntityTooLarge, errors.NewInvalidArgumentError("batch of %d exceeds the limit of %d records", numRecords, maxRecords).Error())
		}

		h.observeBatchRecords("GetUTXOStatuses", numRecords)

		if err := h.enforceBatchResponseBytes("GetUTXOStatuses", int64(numRecords)*utxoStatusRecordMaxBytes, h.settings.Asset.MaxBatchResponseBytes); err != nil {
			return err
		}

		best, err := h.utxoStatusBestBlock(ctx)
		if err != nil {
			return echo.NewHTTPError(http.StatusServiceUnavailable, errors.NewServiceUnavailableError("best block unavailable", err).Error())
		}

		lk := newUTXOStatusLookup(h, best.Height, utxoStatusWanted(reqBytes, numRecords))
		results := make([]UTXOStatusRecord, numRecords)

		g, gCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(h.logger, g, utxosFanoutLimit)

		for i := 0; i < numRecords; i++ {
			off := i * utxosRequestRecordSize

			var txHash chainhash.Hash
			copy(txHash[:], reqBytes[off:off+32])

			vout := binary.LittleEndian.Uint32(reqBytes[off+32 : off+36])

			g.Go(func() (retErr error) {
				defer func() {
					if r := recover(); r != nil {
						h.logger.Errorf("[Asset_http:GetUTXOStatuses] recovered panic on %s:%d: %v", txHash.String(), vout, r)
						retErr = echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("internal error getting utxo %s:%d", txHash.String(), vout).Error())
					}
				}()

				rec, err := lk.record(gCtx, &txHash, vout)
				if err != nil {
					return err
				}

				results[i] = rec

				return nil
			})
		}

		err = g.Wait()

		lk.logFailures()

		if err != nil {
			h.logger.Errorf("[Asset_http:GetUTXOStatuses] fan-out failed: %s", err.Error())

			return err
		}

		after, err := h.utxoStatusBestBlock(ctx)
		if err != nil {
			return echo.NewHTTPError(http.StatusServiceUnavailable, errors.NewServiceUnavailableError("best block unavailable", err).Error())
		}

		prometheusAssetHTTPGetUTXOStatuses.WithLabelValues("OK", "200").Add(float64(numRecords))

		return c.JSON(http.StatusOK, UTXOStatusResponse{
			BestBlock:        *best,
			BestBlockChanged: *best != *after,
			Results:          results,
		})
	}
}

// isStoreNotFound reports whether a store error means "no such record". Stores
// differ on whether they return ErrNotFound or ErrTxNotFound for that.
func isStoreNotFound(err error) bool {
	return errors.Is(err, errors.ErrNotFound) || errors.Is(err, errors.ErrTxNotFound)
}

func (h *HTTP) utxoStatusBestBlock(ctx context.Context) (*UTXOStatusBlockRef, error) {
	header, headerMeta, err := h.repository.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	if header == nil || headerMeta == nil {
		return nil, errors.NewProcessingError("best block header is empty")
	}

	return &UTXOStatusBlockRef{Hash: header.Hash().String(), Height: headerMeta.Height}, nil
}

// utxoStatusWanted maps each requested transaction to the output indexes the
// request asks about, so a funding read keeps only those outputs.
func utxoStatusWanted(reqBytes []byte, numRecords int) map[chainhash.Hash][]uint32 {
	wanted := make(map[chainhash.Hash][]uint32)

	for i := 0; i < numRecords; i++ {
		off := i * utxosRequestRecordSize

		var txHash chainhash.Hash
		copy(txHash[:], reqBytes[off:off+32])

		wanted[txHash] = append(wanted[txHash], binary.LittleEndian.Uint32(reqBytes[off+32:off+36]))
	}

	return wanted
}

// utxoStatusMetaEntry memoises one transaction-record read for the request.
type utxoStatusMetaEntry struct {
	once sync.Once
	data *meta.Data
	err  error
}

// utxoStatusOutput is what an answer needs from one funding output.
type utxoStatusOutput struct {
	// storablePre and storablePost are the script rules' answers before and
	// after Genesis: false for an output the store never keeps as a UTXO.
	storablePre, storablePost bool
	// verified is true when the output comes from the complete requested
	// transaction (serializable, and its txid matches); only then are
	// satoshis, scriptHash and utxoHash set.
	verified   bool
	satoshis   uint64
	scriptHash string
	// utxoHash is the store's commitment for this output as computed from the
	// body; the store's own copy is checked against it.
	utxoHash chainhash.Hash
}

// utxoStatusFundingRecord is a funding-transaction record with the body dropped: the
// requested outputs are summarised and the transaction itself is not kept.
type utxoStatusFundingRecord struct {
	// meta holds IsCoinbase, BlockIDs, BlockHeights, UnminedSince, Conflicting
	// and Locked. Its Tx is always nil.
	meta *meta.Data
	// hasBody is true when the store returned the complete requested
	// transaction, so its output count is the real one.
	hasBody bool
	// partial is true when the store returned a body that is not the complete
	// requested transaction, such as the outputs-only reconstruction of a
	// record seeded from a UTXO snapshot.
	partial bool
	// numOutputs is the body's output count, set only when hasBody.
	numOutputs int
	// outputs holds the requested outputs present in the body.
	outputs map[uint32]utxoStatusOutput
	// holes holds the requested positions where the body has no output: in a
	// seeded record, an output that was already spent when the snapshot was
	// taken. On Aerospike its UTXO slot is nil and GetSpend reads it as OK.
	holes map[uint32]bool
}

// utxoStatusFundingEntry memoises one funding-record read for the request.
type utxoStatusFundingEntry struct {
	once sync.Once
	rec  *utxoStatusFundingRecord
	err  error
}

// utxoStatusBlockEntry memoises one main-chain header lookup (by height) for
// the request: the block's ID and hash, without the block body.
type utxoStatusBlockEntry struct {
	once sync.Once
	id   uint32
	hash string
	err  error
}

type utxoStatusLookup struct {
	h        *HTTP
	bestH    uint32
	wanted   map[chainhash.Hash][]uint32
	mu       sync.Mutex
	fundings map[chainhash.Hash]*utxoStatusFundingEntry
	refs     map[chainhash.Hash]*utxoStatusMetaEntry
	blocks   map[uint32]*utxoStatusBlockEntry

	// Store failures are logged once per request, not once per record.
	failures  atomic.Int64
	firstFail atomic.Pointer[string]
}

func newUTXOStatusLookup(h *HTTP, bestH uint32, wanted map[chainhash.Hash][]uint32) *utxoStatusLookup {
	return &utxoStatusLookup{
		h:        h,
		bestH:    bestH,
		wanted:   wanted,
		fundings: make(map[chainhash.Hash]*utxoStatusFundingEntry),
		refs:     make(map[chainhash.Hash]*utxoStatusMetaEntry),
		blocks:   make(map[uint32]*utxoStatusBlockEntry),
	}
}

// failed counts a per-record store failure; the first one is kept for the log.
func (l *utxoStatusLookup) failed(txHash *chainhash.Hash, vout uint32, what string, err error) {
	if l.failures.Add(1) == 1 {
		msg := fmt.Sprintf("%s:%d %s: %v", txHash.String(), vout, what, err)
		l.firstFail.Store(&msg)
	}
}

// logFailures writes one line for all the store failures of the request.
func (l *utxoStatusLookup) logFailures() {
	n := l.failures.Load()
	if n == 0 {
		return
	}

	first := ""
	if p := l.firstFail.Load(); p != nil {
		first = *p
	}

	l.h.logger.Warnf("[Asset_http:GetUTXOStatuses] %d record(s) answered store_unavailable; first: %s", n, first)
}

// fundingRecord returns the (memoised) funding-transaction record, with the
// requested outputs summarised and the body dropped. At most
// utxoStatusBodyReads of these reads run at once in the whole process. A nil
// record with a nil error means the store does not hold the transaction.
func (l *utxoStatusLookup) fundingRecord(ctx context.Context, hash *chainhash.Hash) (*utxoStatusFundingRecord, error) {
	l.mu.Lock()

	e, ok := l.fundings[*hash]
	if !ok {
		e = &utxoStatusFundingEntry{}
		l.fundings[*hash] = e
	}

	l.mu.Unlock()

	e.once.Do(func() {
		select {
		case utxoStatusBodySem <- struct{}{}:
		case <-ctx.Done():
			e.err = ctx.Err()
			return
		}

		defer func() { <-utxoStatusBodySem }()

		data, err := l.h.repository.GetTxFundingRecord(ctx, hash)

		switch {
		case err != nil && isStoreNotFound(err):
			return
		case err != nil:
			e.err = err
			return
		case data == nil:
			return
		}

		e.rec = l.summarise(hash, data, l.wanted[*hash])
	})

	return e.rec, e.err
}

// summarise keeps what the answers need from a funding record and drops the
// transaction body. A value, script hash or UTXO hash is taken only from the
// complete requested transaction. A body that is not (a seeded, outputs-only
// reconstruction) still shows where outputs are missing and still gets the
// script check, since both can only turn an answer into unknown. The script
// rules are evaluated for both eras; which applies is decided once the funding
// block is placed on the main chain.
func (l *utxoStatusLookup) summarise(hash *chainhash.Hash, data *meta.Data, vouts []uint32) *utxoStatusFundingRecord {
	rec := &utxoStatusFundingRecord{
		meta: &meta.Data{
			IsCoinbase:   data.IsCoinbase,
			BlockIDs:     data.BlockIDs,
			BlockHeights: data.BlockHeights,
			UnminedSince: data.UnminedSince,
			Conflicting:  data.Conflicting,
			Locked:       data.Locked,
		},
	}

	if data.Tx == nil {
		return rec
	}

	verified := data.TxIsSerializable() && data.Tx.TxIDChainHash().IsEqual(hash)
	stored := len(data.Tx.Outputs)

	if verified {
		rec.hasBody, rec.numOutputs = true, stored
	} else {
		rec.partial = true
	}

	rec.outputs = make(map[uint32]utxoStatusOutput, len(vouts))

	genesis := l.h.settings.ChainCfgParams.GenesisActivationHeight

	preGenesis := uint32(0)
	if genesis > 0 {
		preGenesis = genesis - 1
	}

	for _, vout := range vouts {
		if int(vout) >= stored {
			continue
		}

		out := data.Tx.Outputs[vout]
		if out == nil || out.LockingScript == nil {
			if rec.holes == nil {
				rec.holes = make(map[uint32]bool)
			}

			rec.holes[vout] = true

			continue
		}

		if _, done := rec.outputs[vout]; done {
			continue
		}

		o := utxoStatusOutput{
			storablePre:  utxo.ShouldStoreOutputAsUTXO(out, preGenesis, genesis),
			storablePost: utxo.ShouldStoreOutputAsUTXO(out, genesis, genesis),
		}

		if verified {
			if uh, err := util.UTXOHashFromOutput(hash, out, vout); err == nil {
				sum := sha256.Sum256(*out.LockingScript)
				o.verified = true
				o.satoshis, o.scriptHash, o.utxoHash = out.Satoshis, hex.EncodeToString(sum[:]), *uh
			}
		}

		rec.outputs[vout] = o
	}

	return rec
}

// blockRefs returns the (memoised) block IDs and heights recorded for a
// transaction, without its body: the spender path only needs where the
// spender was mined. A nil record with a nil error means the store does not
// hold the transaction.
func (l *utxoStatusLookup) blockRefs(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	l.mu.Lock()

	e, ok := l.refs[*hash]
	if !ok {
		e = &utxoStatusMetaEntry{}
		l.refs[*hash] = e
	}

	l.mu.Unlock()

	e.once.Do(func() {
		e.data, e.err = l.h.repository.GetTxBlockRefs(ctx, hash)
		if e.err != nil && isStoreNotFound(e.err) {
			e.data, e.err = nil, nil
		}
	})

	return e.data, e.err
}

// blockAtHeight returns the (memoised) ID and hash of the main-chain block at a
// height, from a header-only lookup.
func (l *utxoStatusLookup) blockAtHeight(ctx context.Context, height uint32) (uint32, string, error) {
	l.mu.Lock()

	e, ok := l.blocks[height]
	if !ok {
		e = &utxoStatusBlockEntry{}
		l.blocks[height] = e
	}

	l.mu.Unlock()

	e.once.Do(func() {
		headers, metas, err := l.h.repository.GetBlockchainClient().GetBlockHeadersByHeight(ctx, height, height)

		switch {
		case err != nil:
			e.err = err
		case len(headers) != 1 || len(metas) != 1 || headers[0] == nil || metas[0] == nil || metas[0].Height != height:
			e.err = errors.NewProcessingError("no single main-chain header at height %d", height)
		default:
			e.id, e.hash = metas[0].ID, headers[0].Hash().String()
		}
	})

	return e.id, e.hash, e.err
}

func (l *utxoStatusLookup) record(ctx context.Context, txHash *chainhash.Hash, vout uint32) (UTXOStatusRecord, error) {
	rec := UTXOStatusRecord{TxID: txHash.String(), Vout: vout}

	unknown := func(reason string) (UTXOStatusRecord, error) {
		rec.Status, rec.Reason = utxoStatusUnknown, reason
		return rec, nil
	}

	// The funding record is read first: its body supplies the UTXO hash the
	// store's commitment is checked against.
	fr, metaErr := l.fundingRecord(ctx, txHash)
	if metaErr != nil && ctx.Err() != nil {
		return rec, ctx.Err()
	}

	var (
		out     utxoStatusOutput
		haveOut bool
	)

	spend := &utxo.Spend{TxID: txHash, Vout: vout}

	if metaErr == nil && fr != nil {
		// A seeded record has no output where one was already spent, and on
		// Aerospike that empty UTXO slot reads as OK. Whatever the store says,
		// there is no live output here.
		if fr.holes[vout] {
			return unknown(utxoReasonNoOutput)
		}

		out, haveOut = fr.outputs[vout]
		if haveOut && out.verified {
			uh := out.utxoHash
			spend.UTXOHash = &uh
		}
	}

	sr, err := l.h.repository.GetUtxo(ctx, spend)
	if err != nil && !isStoreNotFound(err) {
		if ctx.Err() != nil {
			return rec, ctx.Err()
		}

		// The stored commitment is not the one the body gives for this output,
		// for example after an alert-system reassignment. The value and script
		// hash from the body would describe the old owner.
		if spend.UTXOHash != nil && errors.Is(err, errors.ErrUtxoHashMismatch) {
			return unknown(utxoReasonReassign)
		}

		l.failed(txHash, vout, "output", err)

		return unknown(utxoReasonStore)
	}

	notFound := err != nil || sr == nil || sr.Status == int(utxo.Status_NOT_FOUND)

	// funding places the transaction on the main chain once per record.
	var (
		funding     *UTXOStatusFunding
		fundingDone bool
	)

	placeFunding := func() *UTXOStatusFunding {
		if !fundingDone {
			funding, fundingDone = l.funding(ctx, fr.meta), true
		}

		return funding
	}

	if haveOut {
		// Some stores keep no UTXO slot for an output that can never be spent
		// and then answer OK for it.
		storable, known := l.storable(out, placeFunding)
		if !known {
			return unknown(utxoReasonScript)
		}

		if !storable {
			return unknown(utxoReasonUnspend)
		}
	} else if metaErr == nil && fr != nil && fr.hasBody && int(vout) >= fr.numOutputs && notFound {
		return unknown(utxoReasonVout)
	}

	if notFound {
		// This must never be read as spent or unspent.
		switch {
		case metaErr != nil:
			l.failed(txHash, vout, "funding record", metaErr)

			return unknown(utxoReasonStore)
		case fr != nil:
			// The transaction's record is held (with or without its body) but
			// the store has no live entry for the output (for example after
			// journal retention passed, or, in a seeded record, a trailing
			// output that was already spent).
			return unknown(utxoReasonNoOutput)
		default:
			// The transaction record may be gone (pruned after spend, or never seen).
			return unknown(utxoReasonPruned)
		}
	}

	// The store holds the output, but without its funding record neither the
	// script check above nor the coinbase maturity check below can run, so the
	// answer would be a guess.
	switch {
	case metaErr != nil:
		l.failed(txHash, vout, "funding record", metaErr)

		return unknown(utxoReasonStore)
	case fr == nil:
		// The record was gone by the time it was read (pruned after a spend).
		return unknown(utxoReasonPruned)
	}

	rec.Funding = placeFunding()

	// The value and script hash are given only for an output the store holds, whose
	// commitment GetSpend checked against this body.
	if haveOut && out.verified {
		sats := out.satoshis
		rec.Satoshis = &sats
		rec.LockingScriptSha256 = out.scriptHash
	}

	switch utxo.Status(sr.Status) { //nolint:gosec
	case utxo.Status_OK:
		// Conflicting and locked are flags on the transaction. On Aerospike they
		// sit on the master record only, so an output on a child page reads OK.
		// They are taken from the funding-record read, which is a separate read
		// from this one (a flag set between the two is not seen).
		switch {
		case fr.meta.Conflicting:
			return unknown(utxoReasonConflict)
		case fr.meta.Locked:
			return unknown(utxoReasonLocked)
		}

		rec.Status = utxoStatusUnspent
	case utxo.Status_IMMATURE:
		// A spend-height hold can replace the output's own state: Aerospike sets
		// IMMATURE over SPENT, and SQL and pebble set it after frozen, locked and
		// conflicting. Spent and frozen outputs carry spending data (frozen as the
		// frozen marker).
		switch {
		case sr.SpendingData != nil:
			return unknown(utxoReasonHeld)
		case fr.meta.Conflicting:
			return unknown(utxoReasonConflict)
		case fr.meta.Locked:
			return unknown(utxoReasonLocked)
		}

		rec.Status, rec.Immature = utxoStatusUnspent, true
	case utxo.Status_SPENT:
		rec.Status = utxoStatusSpent

		if sr.SpendingData == nil || sr.SpendingData.TxID == nil {
			return unknown(utxoReasonOther)
		}

		rec.Spender = &UTXOStatusSpender{TxID: sr.SpendingData.TxID.String(), Vin: sr.SpendingData.Vin}

		if sd, sErr := l.blockRefs(ctx, sr.SpendingData.TxID); sErr == nil && sd != nil {
			// The same placement as the funding transaction, including the
			// block-ID check at the height: the chain check alone can report a
			// swept reservation's ID as on the main chain.
			if f := l.funding(ctx, sd); f != nil {
				mined := f.Mined
				rec.Spender.Mined = &mined

				if mined {
					rec.Spender.BlockHeight = f.BlockHeight
				}
			}
		}
	case utxo.Status_FROZEN:
		return unknown(utxoReasonFrozen)
	case utxo.Status_CONFLICTING:
		return unknown(utxoReasonConflict)
	case utxo.Status_LOCKED:
		return unknown(utxoReasonLocked)
	default:
		return unknown(utxoReasonOther)
	}

	// Aerospike's GetSpend ignores coinbase maturity, so derive it here as well.
	// It needs the funding block on the main chain; without that a coinbase
	// output's maturity is not known, and it is not reported as unspent.
	if rec.Status == utxoStatusUnspent && fr.meta.IsCoinbase {
		if rec.Funding == nil || !rec.Funding.Mined {
			return unknown(utxoReasonMaturity)
		}

		if uint64(rec.Funding.BlockHeight)+uint64(l.h.settings.ChainCfgParams.CoinbaseMaturity) > uint64(l.bestH) { //nolint:gosec
			rec.Immature = true
		}
	}

	return rec, nil
}

// storable applies the script rules of the era the output was created in.
// When both eras agree, no placement is needed. Otherwise the era comes from
// the funding block on the main chain, or, for a transaction known to be
// unmined, from the next block height; known is false when neither is
// established.
func (l *utxoStatusLookup) storable(out utxoStatusOutput, place func() *UTXOStatusFunding) (storable, known bool) {
	if out.storablePre == out.storablePost {
		return out.storablePost, true
	}

	f := place()

	var height uint32

	switch {
	case f == nil:
		return false, false
	case f.Mined:
		height = f.BlockHeight
	default:
		height = l.bestH + 1
	}

	if height >= l.h.settings.ChainCfgParams.GenesisActivationHeight {
		return out.storablePost, true
	}

	return out.storablePre, true
}

// funding reports where the transaction record sits. It returns nil (unknown)
// when that cannot be established: a main-chain check failed, the record has no
// block evidence and is not marked unmined, or a block's ID does not match the
// main-chain block at its recorded height. Unconfirmed is set only when every
// block was checked and found off the main chain, or the record says the
// transaction is unmined. The main-chain header at the recorded height is looked
// up once per height per request; it confirms the (ID, height) pair and supplies
// the hash.
func (l *utxoStatusLookup) funding(ctx context.Context, data *meta.Data) *UTXOStatusFunding {
	n := len(data.BlockIDs)
	if len(data.BlockHeights) < n {
		n = len(data.BlockHeights)
	}

	unknown := false

	for i := 0; i < n; i++ {
		id, height := data.BlockIDs[i], data.BlockHeights[i]

		// Block ID 0 is the genesis block. A record seeded with ID 0 at a
		// later height pairs the wrong block with the height.
		if id == model.GenesisBlockID && height != 0 {
			unknown = true
			continue
		}

		var (
			onChain bool
			err     error
		)

		if l.h.mainChainCache != nil {
			onChain, err = l.h.mainChainCache.IsOnMainChain(ctx, id, height)
		} else {
			onChain, err = l.h.repository.GetBlockchainClient().CheckBlockIsInCurrentChain(ctx, []uint32{id})
		}

		if err != nil {
			unknown = true
			continue
		}

		if !onChain {
			continue
		}

		blockID, blockHash, bErr := l.blockAtHeight(ctx, height)
		if bErr != nil || blockID != id {
			unknown = true
			continue
		}

		return &UTXOStatusFunding{Mined: true, BlockHeight: height, BlockHash: blockHash}
	}

	if unknown {
		return nil
	}

	if n > 0 || data.UnminedSince > 0 {
		return &UTXOStatusFunding{Unconfirmed: true}
	}

	return nil
}
