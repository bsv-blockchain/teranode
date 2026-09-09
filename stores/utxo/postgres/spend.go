package postgres

import (
	"bytes"
	"context"
	"math"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/jackc/pgx/v5"
)

// ---------------------------------------------------------------------------
// Batch types
// ---------------------------------------------------------------------------

// batchSpendItem represents a single spend queued into the batcher.
type batchSpendItem struct {
	spend             *utxo.Spend
	blockHeight       uint32
	errCh             chan error
	ignoreConflicting bool
	ignoreLocked      bool
	skipUTXOHashCheck bool
}

// voutToInt32 guards the uint32 → INT4 narrowing at the point a vout enters a
// SQL binding: spends.prev_output_idx is an INT4 column, and a protocol-valid
// transaction can never carry 2^31 outputs, so a vout above math.MaxInt32 is
// corrupt input and must be rejected with a typed error rather than failing
// deep inside pgx parameter encoding.
func voutToInt32(vout uint32) (int32, error) {
	if vout > math.MaxInt32 {
		return 0, errors.NewProcessingError("vout %d exceeds int32 range for INT prev_output_idx", vout)
	}
	return int32(vout), nil
}

// blockHeightToInt32 guards the uint32 → INT4 narrowing for block heights, mirroring
// voutToInt32. Heights land in INT4 columns (spent_at_height, mined_at_height,
// unmined_since, block_heights, delete_at_height); a height above MaxInt32 is far
// beyond any real chain (millennia out) but must be rejected at the write entry
// points rather than silently wrapping negative inside pgx encoding. Validating once
// at the entry makes the downstream int32(height) casts provably safe.
func blockHeightToInt32(height uint32) (int32, error) {
	if height > math.MaxInt32 {
		return 0, errors.NewProcessingError("blockHeight %d exceeds int32 range for INT height columns", height)
	}
	return int32(height), nil
}

// ---------------------------------------------------------------------------
// Direct-mode SQL (used when batcher is not active)
// ---------------------------------------------------------------------------

// spendValidationSQL is the CTE used to validate a spend attempt and insert into
// the append-only spends table. spent_at_height is recorded for deferred DAH
// computation by Worker 2.
//
// Packed form: per-output access is O(1) byte arithmetic — utxo_hash is a
// 16-byte substr at offset $2*16 (substr is 1-based; the stored 128-bit prefix
// is compared against the caller hash's first 16 bytes), frozen/spendable flags
// are get_bit() bitmap probes, coinbase_spending_height is a scalar. The WHERE
// clause requires $2 < t.out_count, so a missing tx OR an OOB index both
// produce 0 RETURNING rows — diagnosed by diagnoseSpendFailure. get_bit is
// only reached when $2 < out_count (bitmap is sized to out_count bits rounded
// up), so it can never raise an out-of-range error.
const spendValidationSQL = `
WITH validation AS (
    SELECT
        substr(t.utxo_hashes, $2::int * 16 + 1, 16) AS utxo_hash,
        (t.out_frozens IS NOT NULL AND get_bit(t.out_frozens, $2::int) = 1) AS output_frozen,
        -- An output is only spendable if its out_spendables bit is set. A non-spendable
        -- output (e.g. OP_RETURN) still has a utxo_hash stored, so the hash match alone
        -- is not enough to authorise a spend — without this gate a caller presenting the
        -- correct hash of an OP_RETURN output could insert a spend row. Matches the
        -- aerospike (UTXO_NOT_FOUND) and sql (no output row) stores.
        (t.out_spendables IS NOT NULL AND get_bit(t.out_spendables, $2::int) = 1) AS output_spendable,
        t.coinbase_spending_height AS coinbase_spending_height,
        CASE WHEN array_length(t.spendable_ins, 1) >= $2::int + 1 THEN t.spendable_ins[$2::int + 1] END AS spendable_in,
        t.locked AS tx_locked, t.conflicting AS tx_conflicting, t.frozen AS tx_frozen
    FROM txs t
    WHERE t.hash = $1 AND $2::int < t.out_count
),
inserted AS (
    INSERT INTO spends (prev_tx_hash, prev_output_idx, spending_data, spent_at_height)
    SELECT $1, $2, $3, $5 FROM validation v
    WHERE v.utxo_hash = $4 AND v.output_spendable AND NOT v.output_frozen AND NOT v.tx_frozen
      AND ($6 OR NOT v.tx_locked) AND ($7 OR NOT v.tx_conflicting)
      -- coinbase_spending_height is the FIRST height at which the coinbase output is
      -- spendable (inclusive): a spend at exactly that height is allowed, so the
      -- immaturity test is strictly greater-than. Do not change to >=.
      AND NOT (v.coinbase_spending_height > 0 AND v.coinbase_spending_height > $5)
      AND NOT (COALESCE(v.spendable_in, 0) > 0 AND $5 < COALESCE(v.spendable_in, 0))
    ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING
    RETURNING prev_tx_hash
)
SELECT 1 FROM inserted
`

// spendValidationSQLNoHash is identical to spendValidationSQL except the
// utxo_hash comparison predicate ("v.utxo_hash = $4 AND") is omitted and
// parameters are renumbered accordingly: $4=blockHeight, $5=ignoreLocked,
// $6=ignoreConflicting. Used for the below-checkpoint outpoint-only fast path
// (IgnoreFlags.SkipUTXOHashCheck=true) where the caller has no parent scripts
// to derive the UTXO hash from. Every other guard — output_spendable, frozen,
// locked, conflicting, coinbase maturity, spendable_in, and the ON CONFLICT
// double-spend barrier — is kept intact.
const spendValidationSQLNoHash = `
WITH validation AS (
    SELECT
        (t.out_frozens IS NOT NULL AND get_bit(t.out_frozens, $2::int) = 1) AS output_frozen,
        (t.out_spendables IS NOT NULL AND get_bit(t.out_spendables, $2::int) = 1) AS output_spendable,
        t.coinbase_spending_height AS coinbase_spending_height,
        CASE WHEN array_length(t.spendable_ins, 1) >= $2::int + 1 THEN t.spendable_ins[$2::int + 1] END AS spendable_in,
        t.locked AS tx_locked, t.conflicting AS tx_conflicting, t.frozen AS tx_frozen
    FROM txs t
    WHERE t.hash = $1 AND $2::int < t.out_count
),
inserted AS (
    INSERT INTO spends (prev_tx_hash, prev_output_idx, spending_data, spent_at_height)
    SELECT $1, $2, $3, $4 FROM validation v
    WHERE v.output_spendable AND NOT v.output_frozen AND NOT v.tx_frozen
      AND ($5 OR NOT v.tx_locked) AND ($6 OR NOT v.tx_conflicting)
      AND NOT (v.coinbase_spending_height > 0 AND v.coinbase_spending_height > $4)
      AND NOT (COALESCE(v.spendable_in, 0) > 0 AND $4 < COALESCE(v.spendable_in, 0))
    ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING
    RETURNING prev_tx_hash
)
SELECT 1 FROM inserted
`

// spendDiagnosticSQL re-queries when the validation INSERT returned 0 rows so
// we can determine the exact reason the spend failed.
//
// Packed form: reads the flat per-output columns on txs. Returns NOT FOUND
// (0 rows) when the tx is missing OR the index is OOB — both are TxNotFound.
const spendDiagnosticSQL = `
SELECT
    substr(t.utxo_hashes, $2::int * 16 + 1, 16) AS utxo_hash,
    (t.out_frozens IS NOT NULL AND get_bit(t.out_frozens, $2::int) = 1) AS output_frozen,
    (t.out_spendables IS NOT NULL AND get_bit(t.out_spendables, $2::int) = 1) AS output_spendable,
    CASE WHEN array_length(t.spendable_ins, 1) >= $2::int + 1 THEN t.spendable_ins[$2::int + 1] END AS spendable_in,
    t.coinbase_spending_height AS coinbase_spending_height,
    t.locked AS tx_locked, t.conflicting AS tx_conflicting, t.frozen AS tx_frozen,
    sp.spending_data AS existing_spend
FROM txs t
LEFT JOIN spends sp ON sp.prev_tx_hash = t.hash AND sp.prev_output_idx = $2
WHERE t.hash = $1 AND $2::int < t.out_count
`

// ---------------------------------------------------------------------------
// Spend — public API
// ---------------------------------------------------------------------------

// Spend marks UTXOs consumed by the given transaction as spent.
func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	if prometheusDirectSpend != nil {
		prometheusDirectSpend.Inc()
	}

	if blockHeight == 0 {
		return nil, errors.NewProcessingError("blockHeight must be greater than zero")
	}
	if _, err := blockHeightToInt32(blockHeight); err != nil {
		return nil, err
	}

	useIgnoreConflicting := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreConflicting
	useIgnoreLocked := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreLocked
	useSkip := len(ignoreFlags) > 0 && ignoreFlags[0].SkipUTXOHashCheck

	var spends []*utxo.Spend
	var err error
	if useSkip {
		spends, err = utxo.GetSpendsOutpointOnly(tx)
	} else {
		spends, err = utxo.GetSpends(tx)
	}
	if err != nil {
		return nil, err
	}

	if len(spends) == 0 {
		return nil, errors.NewProcessingError("No spends provided", nil)
	}

	// prev_output_idx is INT4 — reject out-of-range vouts before any spend is
	// attempted, so a corrupt input can never partially apply a batch.
	for _, spend := range spends {
		if spend == nil {
			continue
		}
		if _, err := voutToInt32(spend.Vout); err != nil {
			return nil, err
		}
	}

	if s.spendBatcher != nil {
		return s.spendBatched(ctx, tx, spends, blockHeight, useIgnoreLocked, useIgnoreConflicting, useSkip)
	}

	return s.spendDirect(ctx, spends, blockHeight, useIgnoreLocked, useIgnoreConflicting, useSkip)
}

// ---------------------------------------------------------------------------
// spendBatched — enqueue each input into the batcher
// ---------------------------------------------------------------------------

// spendBatched enqueues each input into the spend batcher and waits for the
// per-input result. The batcher runs the DB write on context.Background() (see
// sendSpendBatch), so cancelling the request ctx only stops US WAITING — a spend the
// batcher has already committed stays committed. That is intentional and matches the
// sql and aerospike stores: a committed spend is rolled back ONLY for a genuine
// validation failure (see needsSpendRollback), never for cancellation or timeout,
// because a spend is idempotent for the same spender — a retry re-issues the same
// spending_data and the ON CONFLICT DO NOTHING in spendValidationSQL makes it a
// no-op. Rolling back on cancellation would diverge from the other stores and break
// that idempotent-retry contract.
//
// The wait is bounded by SpendWaitTimeout (parity with sql/aerospike) so a stalled
// batcher cannot block a caller whose ctx carries no deadline.
func (s *Store) spendBatched(ctx context.Context, tx *bt.Tx, spends []*utxo.Spend, blockHeight uint32, ignoreLocked, ignoreConflicting, skipUTXOHashCheck bool) ([]*utxo.Spend, error) {
	spentSpends := make([]*utxo.Spend, 0, len(spends))

	// Bound the wait so a stalled batcher cannot hang a deadline-less caller. This
	// bounds only OUR wait; the batch still completes on context.Background().
	spendTimeout := s.settings.UtxoStore.SpendWaitTimeout
	if spendTimeout <= 0 {
		spendTimeout = 30 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, spendTimeout)
	defer cancel()

	// Enqueue each spend into the batcher and wait for results. PutCtx (not Put) links
	// the request span onto the batch span for tracing, matching sql/aerospike.
	errChs := make([]chan error, len(spends))
	for idx, spend := range spends {
		if spend == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}
		errCh := make(chan error, 1)
		errChs[idx] = errCh
		s.spendBatcher.PutCtx(ctx, &batchSpendItem{
			spend:             spend,
			blockHeight:       blockHeight,
			errCh:             errCh,
			ignoreConflicting: ignoreConflicting,
			ignoreLocked:      ignoreLocked,
			skipUTXOHashCheck: skipUTXOHashCheck,
		})
	}

	// Wait for all results.
	for idx, spend := range spends {
		var batchErr error
		select {
		case batchErr = <-errChs[idx]:
		case <-waitCtx.Done():
			// Distinguish our timeout from an upstream cancellation, but in BOTH cases
			// fall through without rolling back any already-committed spend (see doc).
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				spends[idx].Err = errors.NewServiceUnavailableError("[Spend] batch operation timed out after %s for %s:%d", spendTimeout, spend.TxID, spend.Vout)
			} else {
				spends[idx].Err = errors.NewContextCanceledError("[Spend] context cancelled for %s:%d", spend.TxID, spend.Vout)
			}
			continue
		}

		if batchErr != nil {
			spends[idx].Err = batchErr

			var errSpent *errors.UtxoSpentErrData
			if errors.AsData(batchErr, &errSpent) {
				spends[idx].ConflictingTxID = errSpent.SpendingData.TxID
				if prometheusDirectConflicts != nil {
					prometheusDirectConflicts.Inc()
				}
			}
			continue
		}
		spentSpends = append(spentSpends, spend)
	}

	if len(spends) != len(spentSpends) {
		// Roll back already-committed spends ONLY for genuine validation failures.
		// Cancellation/timeout errors deliberately fall through without a rollback —
		// the committed spend is idempotent on retry (see the spendBatched doc). This
		// matches the sql and aerospike stores.
		if needsSpendRollback(spends) {
			if unspendErr := s.Unspend(context.Background(), spentSpends); unspendErr != nil {
				s.logger.Errorf("error in postgres unspend (rollback): %v", unspendErr)
			}
		}

		var spendErrors error
		for _, spend := range spends {
			if spend.Err != nil {
				if spendErrors != nil {
					spendErrors = errors.Join(spendErrors, spend.Err)
				} else {
					spendErrors = spend.Err
				}
			}
		}
		return spends, errors.NewUtxoError("error in postgres spend - errors", spendErrors)
	}

	return spends, nil
}

// ---------------------------------------------------------------------------
// spendDirect — per-input validation CTE (no batcher)
// ---------------------------------------------------------------------------

func (s *Store) spendDirect(ctx context.Context, spends []*utxo.Spend, blockHeight uint32, ignoreLocked, ignoreConflicting, skipUTXOHashCheck bool) ([]*utxo.Spend, error) {
	spentSpends := make([]*utxo.Spend, 0, len(spends))

	for idx, spend := range spends {
		if spend == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}

		spendingDataBytes := spend.SpendingData.Bytes()
		inputStart := time.Now()

		// Try the atomic INSERT with validation CTE.
		// When skipUTXOHashCheck is set, use the NoHash variant which omits the
		// utxo_hash predicate and uses renumbered parameters ($4=blockHeight,
		// $5=ignoreLocked, $6=ignoreConflicting).
		var inserted int
		var err error
		if skipUTXOHashCheck {
			err = s.pool.QueryRow(ctx, spendValidationSQLNoHash,
				spend.TxID[:],      // $1 prev_tx_hash
				int32(spend.Vout),  // $2 prev_output_idx (INT4; range-checked in Spend)
				spendingDataBytes,  // $3 spending_data
				int32(blockHeight), // $4 blockHeight (also written to INT4 spent_at_height)
				ignoreLocked,       // $5 ignoreLocked
				ignoreConflicting,  // $6 ignoreConflicting
			).Scan(&inserted)
		} else {
			err = s.pool.QueryRow(ctx, spendValidationSQL,
				spend.TxID[:],       // $1 prev_tx_hash
				int32(spend.Vout),   // $2 prev_output_idx (INT4; range-checked in Spend)
				spendingDataBytes,   // $3 spending_data
				spend.UTXOHash[:16], // $4 expected_utxo_hash (16-byte prefix — utxo_hashes stores 16)
				int32(blockHeight),  // $5 blockHeight (also written to INT4 spent_at_height)
				ignoreLocked,        // $6 ignoreLocked
				ignoreConflicting,   // $7 ignoreConflicting
			).Scan(&inserted)
		}

		if prometheusDirectSpendDuration != nil {
			prometheusDirectSpendDuration.Observe(time.Since(inputStart).Seconds())
		}

		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			spends[idx].Err = errors.NewStorageError("[Spend] query failed for %s:%d", spend.TxID, spend.Vout, err)
			continue
		}

		if err == nil {
			spentSpends = append(spentSpends, spend)
			continue
		}

		// INSERT returned 0 rows — run diagnostic query.
		diagErr := s.diagnoseSpendFailure(ctx, spend, spendingDataBytes, blockHeight, ignoreLocked, ignoreConflicting, skipUTXOHashCheck)
		if diagErr == nil {
			spentSpends = append(spentSpends, spend)
			continue
		}

		spends[idx].Err = diagErr

		var errSpent *errors.UtxoSpentErrData
		if errors.AsData(diagErr, &errSpent) {
			spends[idx].ConflictingTxID = errSpent.SpendingData.TxID
			if prometheusDirectConflicts != nil {
				prometheusDirectConflicts.Inc()
			}
		}
	}

	if len(spends) != len(spentSpends) {
		if needsSpendRollback(spends) {
			if unspendErr := s.Unspend(context.Background(), spentSpends); unspendErr != nil {
				s.logger.Errorf("error in postgres unspend (rollback): %v", unspendErr)
			}
		}

		var spendErrors error
		for _, spend := range spends {
			if spend.Err != nil {
				if spendErrors != nil {
					spendErrors = errors.Join(spendErrors, spend.Err)
				} else {
					spendErrors = spend.Err
				}
			}
		}
		return spends, errors.NewUtxoError("error in postgres spend - errors", spendErrors)
	}

	return spends, nil
}

// ---------------------------------------------------------------------------
// sendSpendBatch — batch callback for the go-batcher
// ---------------------------------------------------------------------------

func (s *Store) sendSpendBatch(batch []*batchSpendItem) {
	s.batchStats.spendItems.Add(int64(len(batch)))
	s.batchStats.spendBatches.Add(1)
	// Reject later same-outpoint spend claims with different spending data in
	// memory before the batch reaches the DB, exactly as the aerospike
	// (sendSpendBatchLua) and sql stores do. Two concurrent spends of the same
	// UTXO that coalesce into one batch otherwise both map to the single
	// ON CONFLICT-inserted row in bulkSpendSQL's result LEFT JOIN and both
	// return success — a double-spend. Filtering here gives first-seen
	// exactly-one-error semantics and parity with the aerospike store; it is a
	// no-op for batches of distinct outpoints (the hot path) and preserves
	// idempotent retries (identical spending data is kept).
	batch = utxo.FilterConflictingDuplicateSpendClaims(batch,
		func(item *batchSpendItem) *utxo.Spend {
			if item == nil {
				return nil
			}
			return item.spend
		},
		func(item *batchSpendItem, err error) {
			item.errCh <- err
		},
	)
	if len(batch) == 0 {
		return
	}

	s.trySendSpendBatch(batch)
}

func (s *Store) trySendSpendBatch(batch []*batchSpendItem) (retryable bool) {
	ctx := context.Background()

	// Single-item fast path: use the direct validation CTE instead of bulk UNNEST.
	if len(batch) == 1 {
		item := batch[0]
		var inserted int
		var err error
		if item.skipUTXOHashCheck {
			err = s.pool.QueryRow(ctx, spendValidationSQLNoHash,
				item.spend.TxID[:], int32(item.spend.Vout),
				item.spend.SpendingData.Bytes(),
				int32(item.blockHeight), item.ignoreLocked, item.ignoreConflicting,
			).Scan(&inserted)
		} else {
			err = s.pool.QueryRow(ctx, spendValidationSQL,
				item.spend.TxID[:], int32(item.spend.Vout),
				item.spend.SpendingData.Bytes(), item.spend.UTXOHash[:16],
				int32(item.blockHeight), item.ignoreLocked, item.ignoreConflicting,
			).Scan(&inserted)
		}
		if err == nil {
			item.errCh <- nil
			return false
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			item.errCh <- errors.NewStorageError("[Spend] query failed for %s:%d", item.spend.TxID, item.spend.Vout, err)
			return false
		}
		diagErr := s.diagnoseSpendFailure(ctx, item.spend, item.spend.SpendingData.Bytes(),
			item.blockHeight, item.ignoreLocked, item.ignoreConflicting, item.skipUTXOHashCheck)
		if diagErr == nil {
			item.errCh <- nil
		} else {
			item.errCh <- diagErr
		}
		return false
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] failed to acquire connection", err)
		}
		return false
	}
	defer conn.Release()

	// Single bulk query: UNNEST parallel arrays → 3-table JOIN validation →
	// INSERT valid spends → return per-item results with diagnostic flags.
	// One parse/plan/execute for the entire batch (§6.1 multi-row INSERT).
	prevTxHashes := make([][]byte, len(batch))
	prevIdxs := make([]int32, len(batch)) // prev_output_idx is INT4 (vout range-checked in Spend)
	spendingDatas := make([][]byte, len(batch))
	utxoHashes := make([][]byte, len(batch))
	blockHeights := make([]int32, len(batch)) // spent_at_height is INT4
	ignLockeds := make([]bool, len(batch))
	ignConflictings := make([]bool, len(batch))
	batchIdxs := make([]int32, len(batch))
	skipHashChecks := make([]bool, len(batch)) // per-row SkipUTXOHashCheck flag ($9)

	for i, item := range batch {
		prevTxHashes[i] = item.spend.TxID[:]
		prevIdxs[i] = int32(item.spend.Vout)
		spendingDatas[i] = item.spend.SpendingData.Bytes()
		utxoHashes[i] = item.spend.UTXOHash[:16] // utxo_hashes stores a 16-byte prefix
		blockHeights[i] = int32(item.blockHeight)
		ignLockeds[i] = item.ignoreLocked
		ignConflictings[i] = item.ignoreConflicting
		batchIdxs[i] = int32(i)
		skipHashChecks[i] = item.skipUTXOHashCheck
	}

	rows, err := conn.Query(ctx, bulkSpendSQL,
		prevTxHashes, prevIdxs, spendingDatas, utxoHashes,
		blockHeights, ignLockeds, ignConflictings, batchIdxs, skipHashChecks,
	)
	if err != nil {
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] bulk query failed", err)
		}
		return false
	}

	type bulkResult struct {
		inserted        bool
		found           bool
		outputFrozen    bool
		txFrozen        bool
		txLocked        bool
		txConflicting   bool
		utxoHashMatch   bool
		coinbaseBlock   bool
		spendableBlock  bool
		slotExists      bool   // true when the array subscript is in-bounds
		outputSpendable bool   // false for non-spendable outputs (e.g. OP_RETURN)
		existingSpend   []byte // conflicting row's spending_data (nil if none visible)
	}
	resultMap := make(map[int]*bulkResult, len(batch))
	for rows.Next() {
		var bIdx int32
		r := &bulkResult{}
		if err := rows.Scan(&bIdx, &r.inserted,
			&r.outputFrozen, &r.txFrozen, &r.txLocked, &r.txConflicting,
			&r.utxoHashMatch, &r.coinbaseBlock, &r.spendableBlock, &r.slotExists, &r.outputSpendable,
			&r.existingSpend); err != nil {
			rows.Close()
			for _, item := range batch {
				item.errCh <- errors.NewStorageError("[Spend] scan", err)
			}
			return false
		}
		r.found = true
		resultMap[int(bIdx)] = r
	}
	rows.Close()

	// A mid-stream error (statement_timeout, server reset, network drop) makes
	// rows.Next() stop early with the error parked in rows.Err(). Without this
	// check the partially-filled resultMap would dispatch TxNotFound to every
	// not-yet-received item — silently turning a transient DB failure into a false
	// "output not found" success. Surface it as a storage error to all items
	// instead (the caller decides whether to retry), matching the scan-error path.
	if err := rows.Err(); err != nil {
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] bulk rows iteration", err)
		}
		return false
	}

	for i, item := range batch {
		spend := item.spend
		r, found := resultMap[i]

		if !found {
			// Tx hash not found in txs table.
			item.errCh <- errors.NewTxNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
			continue
		}
		if !r.slotExists {
			// Tx found but output index is OOB — treat as not found (no such output).
			item.errCh <- errors.NewTxNotFoundError("output %s:%d not found (index OOB)", spend.TxID, spend.Vout)
			continue
		}
		if !r.outputSpendable {
			// Output exists but is not a spendable UTXO (e.g. OP_RETURN). Treat as
			// not found, matching the aerospike/sql stores.
			item.errCh <- errors.NewTxNotFoundError("output %s:%d not found (not a spendable UTXO)", spend.TxID, spend.Vout)
			continue
		}
		if r.inserted {
			item.errCh <- nil
			continue
		}
		// Not inserted — classify by visible validation columns first; if none of
		// them rejected the spend, the only remaining reason is "row already
		// existed in spends" (ON CONFLICT DO NOTHING fired). Fall through to
		// diagnoseSpendFailure to fetch the existing spending_data for the
		// idempotent-vs-double-spend distinction. This is the rare path.
		if r.outputFrozen || r.txFrozen {
			item.errCh <- errors.NewUtxoFrozenError("[Spend] utxo is frozen for %s:%d", spend.TxID, spend.Vout)
		} else if r.txLocked && !item.ignoreLocked {
			item.errCh <- errors.NewTxLockedError("[Spend] utxo is not spendable for %s:%d", spend.TxID, spend.Vout)
		} else if r.txConflicting && !item.ignoreConflicting {
			item.errCh <- errors.NewTxConflictingError("[Spend] tx is conflicting for %s:%d", spend.TxID, spend.Vout)
		} else if !r.utxoHashMatch {
			item.errCh <- errors.NewUtxoHashMismatchError("[Spend] utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
		} else if r.coinbaseBlock {
			item.errCh <- errors.NewTxCoinbaseImmatureError("[Spend] coinbase utxo not ready for %s:%d", spend.TxID, spend.Vout)
		} else if r.spendableBlock {
			item.errCh <- errors.NewTxLockedError("[Spend] utxo %s:%d is not spendable yet", spend.TxID, spend.Vout)
		} else if r.existingSpend != nil {
			// All validation passed and the conflicting spends row is visible in
			// the bulk statement's snapshot: classify idempotent-vs-double-spend
			// right here, saving diagnoseSpendFailure's second full-parent fetch.
			// This is the dominant duplicate path under IBD replay.
			if bytes.Equal(r.existingSpend, item.spend.SpendingData.Bytes()) {
				item.errCh <- nil // idempotent retry — same spender
			} else if existingSD, parseErr := spendpkg.NewSpendingDataFromBytes(r.existingSpend); parseErr != nil {
				item.errCh <- errors.NewProcessingError("failed to parse existing spending data", parseErr)
			} else {
				item.errCh <- errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, existingSD)
			}
		} else {
			// All validation passed, INSERT skipped, yet no existing row is
			// visible to the bulk statement's snapshot: a concurrent writer or a
			// concurrent Unspend. Only the fresh-snapshot diagnosis can classify
			// this safely — never assume success here, or a concurrently
			// committed double-spend would be reported as spent-OK.
			diagErr := s.diagnoseSpendFailure(ctx, item.spend, item.spend.SpendingData.Bytes(),
				item.blockHeight, item.ignoreLocked, item.ignoreConflicting, item.skipUTXOHashCheck)
			if diagErr == nil {
				item.errCh <- nil // idempotent retry — same spender
			} else {
				item.errCh <- diagErr
			}
		}
	}

	return false
}

// bulkSpendSQL processes an entire batch in ONE query: UNNEST → validate → INSERT → results.
// spent_at_height is recorded per row for deferred DAH computation by Worker 2.
//
// Packed form: per-output access is O(1) byte arithmetic on the txs row. A tx
// with a missing hash produces no row in validated (JOIN miss → no result row
// for that batch_idx). An OOB index produces a validated row but utxo_hash IS
// NULL (slot_exists=false). The CASE guards on prev_idx < out_count are
// load-bearing: get_bit() ERRORS on an out-of-range bit index (unlike an array
// subscript, which returns NULL), and substr() past the end returns an EMPTY
// bytea (not NULL) which would corrupt the slot_exists classification. The
// result dispatch handles these cases:
//   - no row in resultMap → TxNotFound (tx missing)
//   - row exists, slot_exists=false → TxNotFound (OOB index)
//   - row exists, !utxo_match → UtxoHashMismatch (wrong UTXO hash)
//
// $9 (skip_hash[]) carries the per-row IgnoreFlags.SkipUTXOHashCheck flag.
// When skip_hash is true for a row the utxo_hash = expected_utxo_hash predicate
// is bypassed in to_insert and utxo_match is reported as true; all other guards
// (double-spend ON CONFLICT, output_spendable, frozen, locked, conflicting,
// coinbase maturity) remain enforced regardless.
const bulkSpendSQL = `
WITH items AS (
    SELECT unnest($1::bytea[])   AS prev_tx_hash,
           unnest($2::int[])     AS prev_idx,
           unnest($3::bytea[])   AS spending_data,
           unnest($4::bytea[])   AS expected_utxo_hash,
           unnest($5::int[])     AS block_height,
           unnest($6::boolean[]) AS ign_locked,
           unnest($7::boolean[]) AS ign_conflicting,
           unnest($8::int[])     AS batch_idx,
           unnest($9::boolean[]) AS skip_hash
),
validated AS (
    SELECT i.batch_idx, i.prev_tx_hash, i.prev_idx, i.spending_data, i.expected_utxo_hash,
           i.block_height, i.ign_locked, i.ign_conflicting, i.skip_hash,
           -- utxo_hash is computed ONLY when the row actually compares it: with
           -- skip_hash the 16-byte slice (and its detoast on wide parents) is
           -- dead work on every below-checkpoint spend. Bounds classification
           -- moved to slot_in_bounds so this can be NULL without ambiguity.
           CASE WHEN NOT i.skip_hash AND i.prev_idx::int < t.out_count THEN substr(t.utxo_hashes, i.prev_idx::int * 16 + 1, 16) END AS utxo_hash,
           (i.prev_idx::int < t.out_count AND t.utxo_hashes IS NOT NULL) AS slot_in_bounds,
           CASE WHEN i.prev_idx::int < t.out_count AND t.out_frozens IS NOT NULL THEN get_bit(t.out_frozens, i.prev_idx::int) = 1 ELSE false END AS out_frozen,
           -- Spendable bit: a non-spendable output (OP_RETURN) carries a utxo_hash but
           -- must not be spendable. See spendValidationSQL for the rationale.
           CASE WHEN i.prev_idx::int < t.out_count AND t.out_spendables IS NOT NULL THEN get_bit(t.out_spendables, i.prev_idx::int) = 1 ELSE false END AS out_spendable,
           CASE WHEN array_length(t.spendable_ins,1) >= i.prev_idx::int+1 THEN t.spendable_ins[i.prev_idx::int+1] END AS spendable_in,
           t.coinbase_spending_height AS coinbase_spending_height,
           t.locked AS tx_locked, t.conflicting AS tx_conflicting, t.frozen AS tx_frozen
    FROM items i JOIN txs t ON t.hash = i.prev_tx_hash
),
to_insert AS (
    SELECT prev_tx_hash, prev_idx, spending_data, block_height, batch_idx FROM validated
    WHERE (skip_hash OR utxo_hash = expected_utxo_hash) AND out_spendable AND NOT out_frozen AND NOT tx_frozen
      AND (ign_locked OR NOT tx_locked) AND (ign_conflicting OR NOT tx_conflicting)
      -- coinbase_spending_height is the FIRST spendable height (inclusive): a spend
      -- at exactly that height is valid, so immaturity is strictly >. Do not use >=.
      AND NOT (coinbase_spending_height > 0 AND coinbase_spending_height > block_height)
      AND NOT (COALESCE(spendable_in,0) > 0 AND block_height < COALESCE(spendable_in,0))
),
inserted AS (
    INSERT INTO spends (prev_tx_hash, prev_output_idx, spending_data, spent_at_height)
    SELECT prev_tx_hash, prev_idx, spending_data, block_height FROM to_insert
    ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING
    RETURNING prev_tx_hash, prev_output_idx
)
SELECT v.batch_idx, (i.prev_tx_hash IS NOT NULL) AS inserted,
       v.out_frozen, v.tx_frozen, v.tx_locked, v.tx_conflicting,
       (v.skip_hash OR (v.utxo_hash IS NOT NULL AND v.utxo_hash = v.expected_utxo_hash)) AS utxo_match,
       (v.coinbase_spending_height > 0 AND v.coinbase_spending_height > v.block_height) AS coinbase_block,
       (COALESCE(v.spendable_in,0) > 0 AND v.block_height < COALESCE(v.spendable_in,0)) AS spendable_block,
       v.slot_in_bounds AS slot_exists,
       v.out_spendable AS output_spendable,
       -- For rows the INSERT skipped, fetch the existing spender IN this
       -- statement (CASE is lazy: the subquery runs only for skipped rows, and
       -- its index probe hits the leaf the conflict check just warmed). This
       -- classifies the dominant already-spent case (idempotent IBD-replay
       -- retry vs double-spend) without diagnoseSpendFailure's second
       -- full-parent fetch. NULL when the conflicting row is not visible to
       -- this statement's snapshot (concurrent writer/Unspend) — the dispatcher
       -- MUST fall back to the fresh-snapshot diagnosis then, never assume.
       CASE WHEN i.prev_tx_hash IS NULL THEN
            (SELECT s2.spending_data FROM spends s2
              WHERE s2.prev_tx_hash = v.prev_tx_hash AND s2.prev_output_idx = v.prev_idx)
       END AS existing_spending_data
FROM validated v LEFT JOIN inserted i ON i.prev_tx_hash = v.prev_tx_hash AND i.prev_output_idx = v.prev_idx
ORDER BY v.batch_idx
`

// diagnoseSpendFailure queries the output + txs + spends to determine
// why a spend INSERT failed. skipUTXOHashCheck suppresses the hash mismatch
// check for the below-checkpoint outpoint-only fast path.
func (s *Store) diagnoseSpendFailure(ctx context.Context, spend *utxo.Spend, spendingDataBytes []byte,
	blockHeight uint32, ignoreLocked, ignoreConflicting, skipUTXOHashCheck bool) error {

	var (
		utxoHashBytes          []byte
		outputFrozen           bool
		outputSpendable        bool
		spendableIn            *int32
		coinbaseSpendingHeight int64
		txLocked               bool
		txConflicting          bool
		txFrozen               bool
		existingSpendBytes     []byte
	)

	err := s.pool.QueryRow(ctx, spendDiagnosticSQL,
		spend.TxID[:],     // $1
		int32(spend.Vout), // $2 (INT4; range-checked at the Spend entry point)
	).Scan(&utxoHashBytes, &outputFrozen, &outputSpendable, &spendableIn,
		&coinbaseSpendingHeight, &txLocked, &txConflicting, &txFrozen, &existingSpendBytes)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.NewTxNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
		}
		return errors.NewStorageError("[Spend] diagnostic query failed for %s:%d", spend.TxID, spend.Vout, err)
	}

	// Check existing spend (double-spend or idempotent). Honour a recorded spend
	// regardless of the spendable bit so an already-spent UTXO is still reported.
	if existingSpendBytes != nil {
		if bytes.Equal(existingSpendBytes, spendingDataBytes) {
			// Idempotent: same spending data already recorded.
			return nil
		}
		// Different spender: double-spend error.
		existingSD, parseErr := spendpkg.NewSpendingDataFromBytes(existingSpendBytes)
		if parseErr != nil {
			return errors.NewProcessingError("failed to parse existing spending data", parseErr)
		}
		return errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, existingSD)
	}

	// Non-spendable output (e.g. OP_RETURN): exists with a utxo_hash but is not a
	// UTXO, so it can never be spent. Report not-found, matching aerospike/sql.
	if !outputSpendable {
		return errors.NewTxNotFoundError("output %s:%d not found (not a spendable UTXO)", spend.TxID, spend.Vout)
	}

	// Check frozen.
	if outputFrozen || txFrozen {
		return errors.NewUtxoFrozenError("[Spend] utxo is frozen for %s:%d", spend.TxID, spend.Vout)
	}

	// Check locked (when not ignored).
	if txLocked && !ignoreLocked {
		return errors.NewTxLockedError("[Spend] utxo is not spendable for %s:%d", spend.TxID, spend.Vout)
	}

	// Check conflicting (when not ignored).
	if txConflicting && !ignoreConflicting {
		return errors.NewTxConflictingError("[Spend] tx is conflicting for %s:%d", spend.TxID, spend.Vout)
	}

	// Check UTXO hash mismatch (skipped on below-checkpoint outpoint-only path).
	// utxo_hashes stores a 16-byte prefix, so compare the caller hash's first 16.
	if !skipUTXOHashCheck && !bytes.Equal(utxoHashBytes, spend.UTXOHash[:16]) {
		return errors.NewUtxoHashMismatchError("[Spend] utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
	}

	// Check coinbase maturity.
	if coinbaseSpendingHeight > 0 && coinbaseSpendingHeight > int64(blockHeight) {
		return errors.NewTxCoinbaseImmatureError("[Spend] coinbase utxo not ready to spend for %s:%d, requires height %d, current %d",
			spend.TxID, spend.Vout, coinbaseSpendingHeight, blockHeight)
	}

	// Check spendable_in.
	if spendableIn != nil && *spendableIn > 0 && blockHeight < uint32(*spendableIn) {
		return errors.NewTxLockedError("[Spend] utxo %s:%d is not spendable until %d", spend.TxID, spend.Vout, *spendableIn)
	}

	// We reach here only when the INSERT recorded 0 rows yet the diagnostic read
	// finds a spendable, unfrozen, unlocked, non-conflicting, hash-matching, mature
	// output with NO existing spend row. The only way to produce that is the
	// ON CONFLICT having fired against a row that a concurrent Unspend then deleted
	// before this read (separate pool connection, separate snapshot). The UTXO is
	// therefore back to unspent and the caller's retry will re-insert cleanly — so
	// this is an idempotent no-op, NOT a hard error. Returning a StorageError here
	// (the old behaviour) spuriously failed a valid retry and, in a batch, left
	// already-committed sibling inputs un-rolled-back (StorageError is not in
	// needsSpendRollback). Treat it as success.
	return nil
}

// needsSpendRollback returns true if any spend failed due to a validation error
// that indicates the transaction is genuinely invalid.
func needsSpendRollback(spends []*utxo.Spend) bool {
	for _, spend := range spends {
		if spend.Err == nil {
			continue
		}
		if errors.Is(spend.Err, errors.ErrSpent) ||
			errors.Is(spend.Err, errors.ErrTxConflicting) ||
			errors.Is(spend.Err, errors.ErrFrozen) ||
			errors.Is(spend.Err, errors.ErrUtxoHashMismatch) {
			return true
		}
	}
	return false
}

// Unspend reverses a previous spend by deleting the owning spend rows, clearing the
// corresponding spent_bits bits and the now-invalid deferred-prune stamp on the
// affected parents, and enqueueing those parents onto the dah_dirty_parents heal
// queue — all in ONE transaction so a reorg never observes a torn state (some
// outputs unspent while the parent still carries a stale delete_at_height the
// pruner could act on, or a cleared bit with no queued heal for the fold's
// stale-snapshot race).
//
// Ownership (matches the aerospike and sql stores): a spend row is removed only when
// the caller's SpendingData token equals the stored spender. A non-owning caller — a
// stale reorg record whose output has since been re-spent by a different tx — is a
// no-op for that row (it must NOT wipe the live spender), but its parent still takes
// part in the DAH housekeeping below. SpendingData is therefore mandatory: a nil
// token is a hard error rather than an unconditional delete.
func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	if len(spends) == 0 {
		return nil
	}

	// SpendingData is the ownership token — reject up front rather than risk
	// deleting a spend we do not own. Vout must also fit the INT4
	// prev_output_idx column (separate entry point from Spend).
	for _, spend := range spends {
		if spend == nil {
			continue
		}
		if spend.SpendingData == nil {
			return errors.NewProcessingError("[Unspend] SpendingData (ownership token) required for %s:%d", spend.TxID, spend.Vout)
		}
		if _, err := voutToInt32(spend.Vout); err != nil {
			return err
		}
	}

	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[Unspend] begin", err)
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	parentSet := make(map[chainhash.Hash]struct{}, len(spends))

	// One bulk statement for the whole batch: a batch touching the same parent
	// folds all its clears into a SINGLE per-parent mask (GROUP BY parent), so
	// the txs row is written once instead of once per input.
	prevTxHashes := make([][]byte, 0, len(spends))
	prevIdxs := make([]int32, 0, len(spends))
	spendingDatas := make([][]byte, 0, len(spends))

	for _, spend := range spends {
		if spend == nil {
			continue
		}
		prevTxHashes = append(prevTxHashes, spend.TxID[:])
		prevIdxs = append(prevIdxs, int32(spend.Vout)) // INT4; range-checked above
		spendingDatas = append(spendingDatas, spend.SpendingData.Bytes())
		if spend.TxID != nil {
			parentSet[*spend.TxID] = struct{}{}
		}
	}

	// Ownership-checked delete + bitmap clear: the spending_data predicate ensures
	// only this caller's spend rows are removed. A mismatching (non-owning) caller
	// deletes 0 rows — intentional — but its parent still drives the DAH
	// housekeeping below.
	//
	// Setter-C reconcile (spent-bitmap, proc v15): the delete must CLEAR the
	// parent's spent_bits bits for exactly the OWNED, SPENDABLE spend rows it
	// actually removed — the inverse of the fold's OR. The DELETE ... RETURNING
	// yields (prev_tx_hash, prev_output_idx) per removed row (0 rows for a
	// non-owning no-op, so no clear); the outpoint is joined back to
	// txs.out_count / out_spendables so ONLY a spendable output contributes,
	// mirroring EXACTLY the spendable filter the fold-forward sweep proc
	// (dah_sweep_proc.go) applies when it SETS bits. That keeps the subset
	// invariant (spent_bits ⊆ out_spendables) in both directions. Unlike the old
	// arithmetic counter this needs no defensive floor: clearing a bit that was
	// never set (spend never folded, or a double-Unspend replay) is naturally a
	// no-op — AND NOT of a zero bit changes nothing, by construction.
	//
	// Mask build is byte-wise and SET-WISE (PG has no |/& on bytea): byte_agg
	// bit_or's per (parent, byte index) with the LSB-first get_bit encoding
	// (byte i/8, bit 1<<(i%8) — same as out_spendables); mask_agg zero-fills gap
	// bytes via LATERAL generate_series + LEFT JOIN (never a correlated subquery
	// per parent). The clear ANDs the complement in over the parent's OWN
	// spent_bits width — a mask byte beyond that width can only cover never-set
	// bits, so ignoring it is the required no-op; the mask get_byte is
	// bounds-guarded (mask may be narrower too) and COALESCE keeps spent_bits
	// unchanged when the series is empty (octet_length 0) so the NOT NULL column
	// can never be written NULL.
	//
	// last_spend_height is deliberately LEFT UNCHANGED (see the Unspend doc
	// comment): it only ever raises the completion height and the stamp is
	// re-gated by bit_count(spent_bits) = spendable_count, so a stale-high value
	// on a no-longer-fully-spent tx is harmless (delete_at_height is cleared
	// below and cannot re-stamp until the bitmap re-completes, at which point
	// the fold re-advances last_spend_height via GREATEST).
	if len(prevTxHashes) > 0 {
		if _, err := pgxTx.Exec(ctx, `
			WITH items AS (
				SELECT unnest($1::bytea[]) AS prev_tx_hash,
				       unnest($2::int[])   AS prev_idx,
				       unnest($3::bytea[]) AS spending_data
			),
			del AS (
				DELETE FROM spends s
				 USING items i
				 WHERE s.prev_tx_hash = i.prev_tx_hash
				   AND s.prev_output_idx = i.prev_idx
				   AND s.spending_data = i.spending_data
				RETURNING s.prev_tx_hash, s.prev_output_idx
			),
			spendable AS (
				SELECT d.prev_tx_hash AS hash, d.prev_output_idx AS idx
				  FROM del d
				  JOIN txs t ON t.hash = d.prev_tx_hash
				 WHERE d.prev_output_idx < t.out_count
				   AND t.out_spendables IS NOT NULL
				   AND get_bit(t.out_spendables, d.prev_output_idx) = 1
			),
			byte_agg AS (
				SELECT hash, idx / 8 AS byte_idx, bit_or(1 << (idx % 8)) AS byte_val
				  FROM spendable
				 GROUP BY hash, idx / 8
			),
			mask_agg AS (
				SELECT hb.hash,
				       decode(string_agg(lpad(to_hex(COALESCE(ba.byte_val, 0)), 2, '0'), '' ORDER BY gs.i), 'hex') AS mask
				  FROM (SELECT hash, max(byte_idx) AS max_byte FROM byte_agg GROUP BY hash) hb
				 CROSS JOIN LATERAL generate_series(0, hb.max_byte) gs(i)
				  LEFT JOIN byte_agg ba ON ba.hash = hb.hash AND ba.byte_idx = gs.i
				 GROUP BY hb.hash
			)
			UPDATE txs t
			   SET spent_bits = COALESCE((
			       -- spent_bits AND NOT mask, byte-wise. The series spans t.spent_bits'
			       -- own width, so its get_byte is in-bounds by construction; 255 # x
			       -- complements within the byte. Empty series → NULL → COALESCE keeps
			       -- the current value.
			       SELECT decode(string_agg(lpad(to_hex(
			                  get_byte(t.spent_bits, gi.i)
			                & (255 # CASE WHEN gi.i < octet_length(m.mask) THEN get_byte(m.mask, gi.i) ELSE 0 END)
			              ), 2, '0'), '' ORDER BY gi.i), 'hex')
			         FROM generate_series(0, octet_length(t.spent_bits) - 1) gi(i)
			       ), t.spent_bits)
			  FROM mask_agg m
			 WHERE t.hash = m.hash`,
			prevTxHashes, prevIdxs, spendingDatas,
		); err != nil {
			return errors.NewStorageError("[Unspend] bulk delete+clear failed", err)
		}
	}

	// Affected parents are no longer fully spent, so any deferred-DAH stamp they
	// carry is now invalid. Clear it in the SAME transaction as the spend deletion
	// (the reorg-clear) so the pruner can never see a deleted-spend / stale-DAH
	// torn state. Housekeeping runs for every affected parent, including the
	// non-owning no-op deletes above. (The spent_bits clear above is per removed
	// spend row; this DAH clear is per-parent — a parent with a partly-cleared
	// bitmap is no longer fully spent, so clearing its stamp is always correct.)
	if len(parentSet) > 0 {
		parentHashes := make([][]byte, 0, len(parentSet))
		for h := range parentSet {
			hb := h
			parentHashes = append(parentHashes, hb[:])
		}
		if _, err := pgxTx.Exec(ctx,
			`UPDATE txs SET delete_at_height = NULL WHERE hash = ANY($1)`, parentHashes,
		); err != nil {
			return errors.NewStorageError("[Unspend] failed to clear delete_at_height", err)
		}

		// C6: remove the parent hashes from the pending_deletes side-table in the same
		// pgxTx so the clear is atomic with the DAH null above. A revived (unspent)
		// parent is no longer a prune candidate; leaving it in the list would let the
		// pruner wrongly delete it after the retention period. DELETE WHERE hash = ANY($1)
		// is a harmless no-op for hashes not in the table.
		if _, err := pgxTx.Exec(ctx,
			`DELETE FROM pending_deletes WHERE hash = ANY($1)`, parentHashes,
		); err != nil {
			return errors.NewStorageError("[Unspend] failed to delete pending_deletes (C6)", err)
		}

		// Dirty-parents heal queue: a concurrent fold whose band snapshot predates
		// this transaction's commit can re-set a just-cleared bit from that stale
		// snapshot (the one residual v15 hazard — see dah_sweep_proc.go). Enqueue
		// every affected parent IN THE SAME TRANSACTION so the reconciler
		// (dah_reconcile.go) recomputes spent_bits from spends with a fresh
		// snapshot within ~one tick. The partition number is the txs_pNN leaf the
		// row lives in — PG's hash routing is not reproducible client-side, so it
		// is recovered from the row's tableoid (the trailing digit run of the
		// txs_pNN leaf name, width-agnostic so it holds beyond 99 partitions). A parent with no txs row has nothing to heal
		// and drops out of the join; ON CONFLICT DO NOTHING keeps re-enqueues of a
		// still-queued parent idempotent.
		if _, err := pgxTx.Exec(ctx, `
			INSERT INTO dah_dirty_parents (hash, partition)
			SELECT t.hash, substring(t.tableoid::regclass::text from '(\d+)$')::int
			  FROM txs t
			 WHERE t.hash = ANY($1)
			ON CONFLICT (hash) DO NOTHING`, parentHashes,
		); err != nil {
			return errors.NewStorageError("[Unspend] failed to enqueue dah_dirty_parents", err)
		}

		// If requested, lock the parents within the same transaction. SetLocked(true)
		// semantics also clear DAH (already cleared above); keeping it in-tx preserves
		// all-or-nothing with the spend reversal.
		if len(flagAsLocked) > 0 && flagAsLocked[0] {
			if _, err := pgxTx.Exec(ctx,
				`UPDATE txs SET locked = true, delete_at_height = NULL WHERE hash = ANY($1)`, parentHashes,
			); err != nil {
				return errors.NewStorageError("[Unspend] failed to lock parent txs", err)
			}
		}
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[Unspend] commit", err)
	}

	return nil
}
