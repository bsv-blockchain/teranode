package httpimpl

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"sync"

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
	// distinct funding height costs one block fetch per request. The cap is far below
	// the asset_maxBatchRecords default used by the 48-byte route. The configured
	// asset_maxBatchRecords still applies when it is lower.
	utxoStatusMaxRecords = 1024

	// utxoStatusBodyReads bounds how many funding-record reads (each loads a full
	// transaction body, possibly from an external blob) run at once per request.
	// Only the requested outputs' value and script hash are kept after each read.
	utxoStatusBodyReads = 8

	// utxoStatusRecordMaxBytes is an upper bound on the JSON size of one result,
	// used to charge asset_maxBatchResponseBytes before any lookup runs.
	utxoStatusRecordMaxBytes = 600

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
)

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
	// Mined is true when the spending transaction is in a main-chain block.
	// It is false both for an unmined spender and when the spender's record is
	// not available; BlockHeight is only set when Mined is true.
	Mined       bool   `json:"mined"`
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
// funding transaction body; without the body it cannot run. An output the store
// holds whose funding record cannot be read is unknown (store_unavailable), and
// one whose record has gone is unknown (not_found_may_be_pruned), because
// neither that check nor the coinbase maturity check could run.
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

		lk := &utxoStatusLookup{
			h:        h,
			bestH:    best.Height,
			wanted:   utxoStatusWanted(reqBytes, numRecords),
			bodySem:  make(chan struct{}, utxoStatusBodyReads),
			fundings: make(map[chainhash.Hash]*utxoStatusFundingEntry),
			refs:     make(map[chainhash.Hash]*utxoStatusMetaEntry),
			blocks:   make(map[uint32]*utxoStatusBlockEntry),
		}
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

		if err := g.Wait(); err != nil {
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
	satoshis   uint64
	scriptHash string
	// storable is false for an output the store never keeps as a UTXO.
	storable bool
}

// utxoStatusFundingRecord is a funding-transaction record with the body dropped: the
// requested outputs are summarised and the transaction itself is not kept.
type utxoStatusFundingRecord struct {
	// meta holds IsCoinbase, BlockIDs, BlockHeights, UnminedSince, Conflicting
	// and Locked. Its Tx is always nil.
	meta *meta.Data
	// hasBody is true when the store returned the transaction body.
	hasBody bool
	// numOutputs is the body's output count, set only when hasBody.
	numOutputs int
	// outputs holds the requested outputs that exist in the body.
	outputs map[uint32]utxoStatusOutput
}

// utxoStatusFundingEntry memoises one funding-record read for the request.
type utxoStatusFundingEntry struct {
	once sync.Once
	rec  *utxoStatusFundingRecord
	err  error
}

// utxoStatusBlockEntry memoises one block fetch (by height) for the request.
type utxoStatusBlockEntry struct {
	once  sync.Once
	block *model.Block
	err   error
}

type utxoStatusLookup struct {
	h        *HTTP
	bestH    uint32
	wanted   map[chainhash.Hash][]uint32
	bodySem  chan struct{}
	mu       sync.Mutex
	fundings map[chainhash.Hash]*utxoStatusFundingEntry
	refs     map[chainhash.Hash]*utxoStatusMetaEntry
	blocks   map[uint32]*utxoStatusBlockEntry
}

// fundingRecord returns the (memoised) funding-transaction record, with the
// requested outputs summarised and the body dropped. At most utxoStatusBodyReads
// of these reads run at once. A nil record with a nil error means the store does
// not hold the transaction.
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
		case l.bodySem <- struct{}{}:
		case <-ctx.Done():
			e.err = ctx.Err()
			return
		}

		defer func() { <-l.bodySem }()

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

		e.rec = l.summarise(data, l.wanted[*hash])
	})

	return e.rec, e.err
}

// summarise keeps what the answers need from a funding record and drops the
// transaction body.
func (l *utxoStatusLookup) summarise(data *meta.Data, vouts []uint32) *utxoStatusFundingRecord {
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

	rec.hasBody = true
	rec.numOutputs = len(data.Tx.Outputs)
	rec.outputs = make(map[uint32]utxoStatusOutput, len(vouts))

	// The script rules depend on the height: the funding block's when known,
	// else the best block.
	height := l.bestH
	if len(data.BlockHeights) > 0 {
		height = data.BlockHeights[0]
	}

	for _, vout := range vouts {
		if int(vout) >= rec.numOutputs {
			continue
		}

		if _, done := rec.outputs[vout]; done {
			continue
		}

		out := data.Tx.Outputs[vout]
		sum := sha256.Sum256(*out.LockingScript)

		rec.outputs[vout] = utxoStatusOutput{
			satoshis:   out.Satoshis,
			scriptHash: hex.EncodeToString(sum[:]),
			storable:   utxo.ShouldStoreOutputAsUTXO(out, height, l.h.settings.ChainCfgParams.GenesisActivationHeight),
		}
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

// blockAtHeight returns the (memoised) main-chain block at a height.
func (l *utxoStatusLookup) blockAtHeight(ctx context.Context, height uint32) (*model.Block, error) {
	l.mu.Lock()

	e, ok := l.blocks[height]
	if !ok {
		e = &utxoStatusBlockEntry{}
		l.blocks[height] = e
	}

	l.mu.Unlock()

	e.once.Do(func() {
		e.block, e.err = l.h.repository.GetBlockByHeight(ctx, height)
	})

	return e.block, e.err
}

func (l *utxoStatusLookup) record(ctx context.Context, txHash *chainhash.Hash, vout uint32) (UTXOStatusRecord, error) {
	rec := UTXOStatusRecord{TxID: txHash.String(), Vout: vout}

	unknown := func(reason string) (UTXOStatusRecord, error) {
		rec.Status, rec.Reason = utxoStatusUnknown, reason
		return rec, nil
	}

	sr, err := l.h.repository.GetUtxo(ctx, &utxo.Spend{TxID: txHash, Vout: vout})
	if err != nil && !isStoreNotFound(err) {
		if ctx.Err() != nil {
			return rec, ctx.Err()
		}

		l.h.logger.Warnf("[Asset_http:GetUTXOStatuses] %s:%d: %v", txHash.String(), vout, err)

		return unknown(utxoReasonStore)
	}

	notFound := err != nil || sr == nil || sr.Status == int(utxo.Status_NOT_FOUND)

	fr, metaErr := l.fundingRecord(ctx, txHash)
	if metaErr != nil && ctx.Err() != nil {
		return rec, ctx.Err()
	}

	if metaErr == nil && fr != nil && fr.hasBody {
		if int(vout) < fr.numOutputs {
			// Some stores keep no UTXO slot for an output that can never be
			// spent and then answer OK for it. summarise applied the script rules.
			if out, ok := fr.outputs[vout]; ok {
				if !out.storable {
					return unknown(utxoReasonUnspend)
				}

				sats := out.satoshis
				rec.Satoshis = &sats
				rec.LockingScriptSha256 = out.scriptHash
			}
		} else if notFound {
			return unknown(utxoReasonVout)
		}
	}

	if notFound {
		// This must never be read as spent or unspent.
		switch {
		case metaErr != nil:
			l.h.logger.Warnf("[Asset_http:GetUTXOStatuses] %s:%d meta: %v", txHash.String(), vout, metaErr)

			return unknown(utxoReasonStore)
		case rec.Satoshis != nil:
			// The record and body are held but the store has no live entry for
			// the output (for example after journal retention passed).
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
		l.h.logger.Warnf("[Asset_http:GetUTXOStatuses] %s:%d funding record: %v", txHash.String(), vout, metaErr)

		return unknown(utxoReasonStore)
	case fr == nil:
		// The record was gone by the time it was read (pruned after a spend).
		return unknown(utxoReasonPruned)
	}

	rec.Funding = l.funding(ctx, fr.meta, true)

	switch utxo.Status(sr.Status) { //nolint:gosec
	case utxo.Status_OK:
		rec.Status = utxoStatusUnspent
	case utxo.Status_IMMATURE:
		// A spend-height hold can replace the output's own state: Aerospike sets
		// IMMATURE over SPENT, and SQL and pebble set it after frozen, locked and
		// conflicting. Spent and frozen outputs carry spending data (frozen as the
		// frozen marker); locked and conflicting are flags on the transaction.
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
			if f := l.funding(ctx, sd, false); f != nil && f.Mined {
				rec.Spender.Mined, rec.Spender.BlockHeight = true, f.BlockHeight
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

// funding reports where the transaction record sits. It returns nil (unknown)
// when that cannot be established: a main-chain check failed, the record has no
// block evidence and is not marked unmined, or a block's ID and height disagree.
// Unconfirmed is set only when every block was checked and found off the main
// chain, or the record says the transaction is unmined. With wantHash the block
// at the recorded height is fetched (once per height per request), which both
// confirms the (ID, height) pair and supplies the hash; the spender path skips
// that fetch and uses only the cheap checks.
func (l *utxoStatusLookup) funding(ctx context.Context, data *meta.Data, wantHash bool) *UTXOStatusFunding {
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

		f := &UTXOStatusFunding{Mined: true, BlockHeight: height}

		if wantHash {
			block, bErr := l.blockAtHeight(ctx, height)
			if bErr != nil || block == nil || block.Header == nil || block.ID != id {
				unknown = true
				continue
			}

			f.BlockHash = block.Header.Hash().String()
		}

		return f
	}

	if unknown {
		return nil
	}

	if n > 0 || data.UnminedSince > 0 {
		return &UTXOStatusFunding{Unconfirmed: true}
	}

	return nil
}
