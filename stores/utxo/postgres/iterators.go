package postgres

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

// unminedTxIterator implements utxo.UnminedTxIterator for the postgres store.
type unminedTxIterator struct {
	store *Store
	rows  pgx.Rows
	err   error
	done  bool
}

const iteratorBatchSize = 1024

// newUnminedTxIterator returns every unmined, non-conflicting tx for block
// assembly's reload on (re)start. It drives from the pending_unmined side-table
// via a JOIN to txs — NOT a direct scan of txs — because txs is PARTITION BY
// HASH with only a BRIN on unmined_since (a btree there is HOT-path-forbidden,
// see the txsIndexesDDLBase comment in schema.go), so a WHERE unmined_since IS
// NOT NULL over txs is a full seq scan of every partition. That scan is slow
// enough on a mid-IBD restart to deadlock against validation's coinbase-maturity
// wait. Reading from pending_unmined avoids it entirely.
//
// This mirrors newPrunableUnminedTxIterator MINUS the cutoff: a reload has no
// height bound, so there is no $1 param, no pu.unmined_since <= $1 clause, and
// NO lazy-cleanup DELETE (a reload must not mutate the side-table — cleanup of
// stale pending_unmined rows is left to the pruner path exclusively).
func newUnminedTxIterator(store *Store) (*unminedTxIterator, error) {
	ctx := context.Background()

	// Read-your-writes barrier: pending_unmined is populated by the write-behind
	// projector (see pending_unmined_projector.go) with up to puFlushInterval of
	// lag, so a reload issued shortly after Create calls returned would miss
	// those txs entirely (the EXISTS probe below would even short-circuit to an
	// empty iterator). Drain the projector buffer synchronously first — the
	// reload path is cold (block-assembly (re)start), so one batched INSERT is
	// negligible, and a flush failure must surface as an error rather than a
	// silently incomplete reload (flushPendingUnmined re-queues the buffer, so
	// the caller can retry).
	if err := store.flushPendingUnmined(ctx); err != nil {
		return nil, errors.NewStorageError("failed to flush pending_unmined projector before unmined reload", err)
	}

	// Fast path: during IBD pending_unmined is typically empty, yet the
	// partition-wise JOIN below still costs planning + parallel-worker startup
	// while returning zero rows. A sub-ms EXISTS probe skips it when there is
	// nothing to reload. No WHERE clause: probing on the unindexed unmined_since
	// could seq-scan a populated table, while bare EXISTS stops at the first row.
	var hasRows bool
	if err := store.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pending_unmined)`).Scan(&hasRows); err != nil {
		return nil, err
	}

	if !hasRows {
		return &unminedTxIterator{store: store, done: true}, nil
	}

	// Read filter (correctness): AND t.unmined_since IS NOT NULL excludes any
	// stale pending_unmined row whose tx has since been mined or pruned but whose
	// side-table row has not yet been reclaimed by the pruner. t.conflicting =
	// false excludes conflicting txs. The row shape and ORDER BY t.hash match
	// newPrunableUnminedTxIterator exactly so readOne's Scan is byte-identical.
	q := `
		SELECT t.hash, t.fee, t.size_in_bytes, t.inserted_at, t.coinbase,
		       t.locked, pu.unmined_since, t.raw_tx, t.mined_info
		FROM pending_unmined pu
		JOIN txs t ON t.hash = pu.hash
		WHERE t.conflicting = false
		  AND t.unmined_since IS NOT NULL
		ORDER BY t.hash
	`

	rows, err := store.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}

	return &unminedTxIterator{store: store, rows: rows}, nil
}

func newPrunableUnminedTxIterator(store *Store, cutoffBlockHeight uint32) (*unminedTxIterator, error) {
	ctx := context.Background()
	cutoff := int32(cutoffBlockHeight) // unmined_since is INT4

	// Fast path: during IBD pending_unmined is typically empty, yet the cleanup
	// DELETE and the partition-wise JOIN below still cost ~50ms per call in
	// planning + parallel-worker startup alone (measured ~10% of mainnet DB time
	// while returning zero rows). A sub-ms EXISTS probe skips both when there is
	// nothing to do. No WHERE clause: probing on the unindexed unmined_since
	// could seq-scan a populated table, while bare EXISTS stops at the first row.
	var hasRows bool
	if err := store.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pending_unmined)`).Scan(&hasRows); err != nil {
		return nil, err
	}

	if !hasRows {
		return &unminedTxIterator{store: store, done: true}, nil
	}

	// Lazy cleanup (bounding): remove stale pending_unmined rows AT OR BELOW the cutoff
	// that are no longer truly unmined (mined, conflicting, or pruned). This only
	// reclaims STALE rows AT OR BELOW the cutoff; stale rows ABOVE the cutoff
	// (mined/conflicting txs more recent than the cutoff) are deliberately left for a
	// future pruner cycle when the cutoff advances to reach them — they cost nothing
	// because the read filter (t.unmined_since IS NOT NULL) already excludes them from
	// results. This is best-effort — a failure here must not abort the pruner read.
	// The mine-path DELETE was removed from SetMinedMulti (lever 1), so stale rows
	// accumulate and are reclaimed here once per pruner cycle (infrequent, off the hot path).
	_, cleanErr := store.pool.Exec(ctx, `
		DELETE FROM pending_unmined pu
		WHERE pu.unmined_since <= $1
		  AND NOT EXISTS (
		      SELECT 1 FROM txs t
		      WHERE t.hash = pu.hash
		        AND t.unmined_since IS NOT NULL
		        AND NOT t.conflicting
		  )
	`, cutoff)
	if cleanErr != nil {
		store.logger.Warnf("[GetPrunableUnminedTxIterator] lazy cleanup failed (continuing): %v", cleanErr)
	}

	// Read filter (correctness): AND t.unmined_since IS NOT NULL ensures that any
	// stale row that survived the cleanup above (e.g. written after the cleanup ran
	// but before the read) is silently excluded. The invariant is preserved: only
	// { unmined_since IS NOT NULL AND NOT conflicting } rows are returned.
	q := `
		SELECT t.hash, t.fee, t.size_in_bytes, t.inserted_at, t.coinbase,
		       t.locked, pu.unmined_since, t.raw_tx, t.mined_info
		FROM pending_unmined pu
		JOIN txs t ON t.hash = pu.hash
		WHERE pu.unmined_since <= $1
		  AND t.conflicting = false
		  AND t.unmined_since IS NOT NULL
		ORDER BY t.hash
	`

	rows, err := store.pool.Query(ctx, q, cutoff)
	if err != nil {
		return nil, err
	}

	return &unminedTxIterator{store: store, rows: rows}, nil
}

func (it *unminedTxIterator) Next(ctx context.Context) ([]*utxo.UnminedTransaction, error) {
	if it.done || it.err != nil || it.rows == nil {
		return nil, it.err
	}

	batch := make([]*utxo.UnminedTransaction, 0, iteratorBatchSize)
	for i := 0; i < iteratorBatchSize; i++ {
		if err := ctx.Err(); err != nil {
			// Cancelled mid-iteration: the rows were opened on context.Background(),
			// so cancelling ctx does NOT release the pinned pool connection. Close it
			// here and latch the error so a caller that stops on this error without
			// also calling Close() cannot leak the connection.
			if cerr := it.Close(); cerr != nil {
				it.store.logger.Warnf("failed to close iterator: %v", cerr)
			}

			it.err = err

			return nil, err
		}
		tx, err := it.readOne(ctx)
		if err != nil {
			return nil, err
		}
		if tx == nil {
			break
		}
		batch = append(batch, tx)
	}

	if len(batch) == 0 {
		return nil, nil
	}

	return batch, nil
}

func (it *unminedTxIterator) readOne(_ context.Context) (*utxo.UnminedTransaction, error) {
	if it.done || it.err != nil || it.rows == nil {
		return nil, it.err
	}

	if !it.rows.Next() {
		// rows.Next() also returns false on a mid-stream failure (connection
		// reset, statement timeout). Capture rows.Err() before Close() so a
		// truncated result set surfaces as an error rather than being mistaken
		// for a clean end-of-iteration.
		rowsErr := it.rows.Err()
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}
		if rowsErr != nil {
			it.err = rowsErr
			return nil, rowsErr
		}
		return nil, nil
	}

	var (
		txHashBytes  []byte
		fee          int64
		sizeInBytes  int64
		insertedAt   time.Time
		isCoinbase   bool
		locked       bool
		unminedSince *int64 // nullable: conflicting txs may have unmined_since = NULL
		rawTx        []byte
		minedInfo    []byte
	)

	if err := it.rows.Scan(&txHashBytes, &fee, &sizeInBytes, &insertedAt, &isCoinbase,
		&locked, &unminedSince, &rawTx, &minedInfo); err != nil {
		if closeErr := it.Close(); closeErr != nil {
			it.store.logger.Warnf("failed to close iterator: %v", closeErr)
		}
		it.err = err
		return nil, it.err
	}

	txHash, err := chainhash.NewHash(txHashBytes)
	if err != nil {
		if closeErr := it.Close(); closeErr != nil {
			it.store.logger.Warnf("failed to close iterator: %v", closeErr)
		}
		it.err = err
		return nil, it.err
	}

	if isCoinbase {
		return &utxo.UnminedTransaction{Skip: true}, nil
	}

	// Deserialize raw_tx for inputs.
	var inputs []*bt.Input
	if rawTx != nil {
		parsedTx, parseErr := bt.NewTxFromBytes(rawTx)
		if parseErr != nil {
			if closeErr := it.Close(); closeErr != nil {
				it.store.logger.Warnf("failed to close iterator: %v", closeErr)
			}
			it.err = parseErr
			return nil, it.err
		}
		inputs = parsedTx.Inputs
	}

	txInpoints, err := subtree.NewTxInpointsFromInputs(inputs)
	if err != nil {
		if closeErr := it.Close(); closeErr != nil {
			it.store.logger.Warnf("failed to close iterator: %v", closeErr)
		}
		it.err = errors.NewProcessingError("failed to create tx inpoints from inputs", err)
		return nil, it.err
	}

	// Decode block IDs from the packed mined_info bytea.
	bidResult := decodeMinedBlockIDs(minedInfo)

	var unminedSinceVal int
	if unminedSince != nil {
		unminedSinceVal = int(*unminedSince)
	}

	return &utxo.UnminedTransaction{
		Node: &subtree.Node{
			Hash:        *txHash,
			Fee:         uint64(fee),
			SizeInBytes: uint64(sizeInBytes),
		},
		TxInpoints:   &txInpoints,
		CreatedAt:    int(insertedAt.UnixMilli()),
		Locked:       locked,
		BlockIDs:     bidResult,
		UnminedSince: unminedSinceVal,
	}, nil
}

func (it *unminedTxIterator) Err() error {
	return it.err
}

func (it *unminedTxIterator) Close() error {
	it.done = true
	if it.rows != nil {
		it.rows.Close()
	}
	return nil
}

// GetUnminedTxIterator returns an iterator for all unmined, non-conflicting transactions.
func (s *Store) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	return newUnminedTxIterator(s)
}

// GetConflictingTxIterator returns an iterator over transactions currently
// marked conflicting=true. Reuses the unminedTxIterator row-mapping machinery
// with the filter flipped.
func (s *Store) GetConflictingTxIterator() (utxo.UnminedTxIterator, error) {
	return newConflictingTxIterator(s)
}

func newConflictingTxIterator(store *Store) (*unminedTxIterator, error) {
	q := `
		SELECT hash, fee, size_in_bytes, inserted_at, coinbase,
		       locked, unmined_since, raw_tx, mined_info
		FROM txs
		WHERE conflicting = true
		ORDER BY hash
	`

	rows, err := store.pool.Query(context.Background(), q)
	if err != nil {
		return nil, err
	}

	return &unminedTxIterator{store: store, rows: rows}, nil
}

// ScanInconsistentUnminedTxs is a no-op for Postgres — the Postgres store always uses
// index-based queries on unmined_since, so there's no fullScan inconsistency to fix.
func (s *Store) ScanInconsistentUnminedTxs() (utxo.ConsistencyScanIterator, error) {
	return nil, nil
}

// GetPrunableUnminedTxIterator returns an iterator for unmined transactions
// older than the cutoff height.
func (s *Store) GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (utxo.UnminedTxIterator, error) {
	return newPrunableUnminedTxIterator(s, cutoffBlockHeight)
}

// QueryOldUnminedTransactions returns transaction hashes for unmined transactions
// older than the cutoff height.
func (s *Store) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT hash FROM txs
		WHERE unmined_since IS NOT NULL AND unmined_since <= $1
		ORDER BY unmined_since LIMIT 1000
	`, int32(cutoffBlockHeight))
	if err != nil {
		return nil, errors.NewStorageError("failed to query old unmined transactions", err)
	}
	defer rows.Close()

	var txHashes []chainhash.Hash
	for rows.Next() {
		var hashBytes []byte
		if err := rows.Scan(&hashBytes); err != nil {
			// Best-effort by design: a per-row scan failure (near-impossible for a
			// BYTEA hash column) drops only that one row, not the whole batch. The
			// caller (pruner preserve/prune) re-runs every cycle, so a skipped old
			// unmined tx is picked up next pass — failing the whole call here would be
			// strictly worse (one bad row would stall preservation each cycle). A
			// stream-level failure is still surfaced via rows.Err() below.
			s.logger.Errorf("[QueryOldUnminedTransactions] error scanning row (skipped): %v", err)
			continue
		}
		var txHash chainhash.Hash
		copy(txHash[:], hashBytes)
		txHashes = append(txHashes, txHash)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("error iterating unmined transactions", err)
	}

	return txHashes, nil
}
