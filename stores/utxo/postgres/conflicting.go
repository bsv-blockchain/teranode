package postgres

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// GetCounterConflicting delegates to the store-agnostic implementation which
// walks inputs to find counter-conflicting transactions.
func (s *Store) GetCounterConflicting(ctx context.Context, hash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetCounterConflictingTxHashes(ctx, s, hash, 0)
}

// GetConflictingChildren delegates to the store-agnostic implementation which
// recursively walks spending children and conflicting_children records.
func (s *Store) GetConflictingChildren(ctx context.Context, hash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetConflictingChildren(ctx, s, hash, s.settings.UtxoStore.ConflictingChildrenMaxNodes)
}

// SetConflicting marks transactions as conflicting or not conflicting.
// It updates txs.conflicting and txs.delete_at_height, appends to the
// conflicting_children array on parent txs, and returns affected parent spends
// and spending child tx hashes.
func (s *Store) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	if len(txHashes) == 0 {
		return nil, nil, nil
	}

	// Compute delete_at_height when setting conflicting=true.
	var deleteAtHeight *int32
	if s.settings.GetUtxoStoreBlockHeightRetention() > 0 && setValue {
		// Widen to int64 before adding so the sum cannot wrap in uint32
		// arithmetic, then narrow to int32 for the INT4 delete_at_height column
		// (heights and retention are far below 2^31).
		v64 := int64(s.GetBlockHeight()) + 1 + int64(s.settings.GetUtxoStoreBlockHeightRetention())
		v := int32(v64)
		deleteAtHeight = &v
	}

	affectedParentSpends := make([]*utxo.Spend, 0, len(txHashes))
	spendingTxHashes := make([]chainhash.Hash, 0, len(txHashes))
	// Dedup spending children across the whole call: a multi-output parent has one
	// spends row per spent output, all recording the same spender, so the raw scan
	// can yield the same spender hash many times. Callers recurse on these, so
	// duplicates would double-process.
	seenSpenders := make(map[chainhash.Hash]struct{})

	// All writes go through one transaction so the flag/DAH updates and the
	// conflicting_children appends across every hash commit atomically — a
	// mid-loop failure rolls the whole set back rather than leaving a torn state.
	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, errors.NewStorageError("[SetConflicting] begin", err)
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	for _, conflictingTxHash := range txHashes {
		txHash := conflictingTxHash

		// Read the tx's raw bytes INSIDE this transaction with FOR UPDATE so the
		// read-then-write is atomic under one snapshot and the row is locked against a
		// concurrent prune/modify. (Previously this read the row via s.Get on a
		// separate pool connection — a TOCTOU window that was safe only because raw_tx
		// is immutable.) raw_tx is an inline, immutable BYTEA column (never
		// externalised), and we only need it to recover the tx's inputs/parents.
		var rawTx []byte
		if err := pgxTx.QueryRow(ctx, `SELECT raw_tx FROM txs WHERE hash = $1 FOR UPDATE`, txHash[:]).Scan(&rawTx); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, nil, errors.NewTxNotFoundError("[SetConflicting] transaction not found: %s", txHash)
			}
			return nil, nil, errors.NewStorageError("[SetConflicting] read tx %s", txHash, err)
		}
		tx, err := bt.NewTxFromBytes(rawTx)
		if err != nil {
			return nil, nil, errors.NewProcessingError("[SetConflicting] parse raw_tx for %s", txHash, err)
		}

		// Update txs: set conflicting flag + delete_at_height.
		var tag pgconn.CommandTag
		if setValue {
			// When setting conflicting=true: set DAH only if not already set, AND only when the row
			// is not preserved. The preserve_until guard mirrors the DAH sweep (dah_sweep.go:97,
			// dah_sweep_proc.go:162), which skips preserved rows. Without it a preserved parent marked
			// conflicting would be stamped for deletion while still inside its preservation window —
			// violating the DAH-setter invariant in pruner_provider.deleteTombstonedPartition (the
			// pruner deletes purely on the stamp and never re-checks preserve_until). The conflicting
			// flag itself is always applied; only the DAH is gated. ProcessExpiredPreservations stamps
			// the DAH (conflicting branch) once the preservation expires.
			tag, err = pgxTx.Exec(ctx, `
				UPDATE txs SET
				  conflicting = $2,
				  delete_at_height = CASE
				      WHEN preserve_until IS NOT NULL THEN delete_at_height
				      ELSE COALESCE(delete_at_height, $3)
				  END
				WHERE hash = $1`,
				txHash[:], setValue, deleteAtHeight,
			)
		} else {
			// When clearing conflicting: clear DAH.
			tag, err = pgxTx.Exec(ctx, `
				UPDATE txs SET
				  conflicting = $2,
				  delete_at_height = $3
				WHERE hash = $1`,
				txHash[:], setValue, deleteAtHeight,
			)
		}
		if err != nil {
			return nil, nil, errors.NewStorageError("failed to set conflicting flag for %s", txHash, err)
		}
		// Defensive: the FOR UPDATE read above holds this row's lock for the whole
		// transaction, so a concurrent prune can no longer delete it between the read
		// and this UPDATE. Kept as a guard — a 0-row update would still mean the flag
		// was never set, which the caller must not read as success.
		if tag.RowsAffected() == 0 {
			return nil, nil, errors.NewTxNotFoundError("[SetConflicting] transaction not found (concurrently removed?): %s", txHash)
		}

		// C1: when clearing conflicting (setValue=false), remove the hash from
		// pending_deletes — the tx is no longer a prune candidate. Harmless no-op
		// if the hash was never stamped (DELETE on a missing row returns 0 rows affected).
		// Runs inside the existing pgxTx so it is atomic with the UPDATE above.
		if !setValue {
			if _, err = pgxTx.Exec(ctx,
				`DELETE FROM pending_deletes WHERE hash = $1`, txHash[:]); err != nil {
				return nil, nil, errors.NewStorageError("failed to delete pending_deletes for %s (C1)", txHash, err)
			}
		}

		// U4: maintain pending_unmined invariant for both setValue directions.
		//   - setValue=true (unmined→conflicting): the tx leaves the set
		//     {unmined AND NOT conflicting}, so its pending_unmined row must be removed.
		//     DELETE is correct here.
		//   - setValue=false (clearing conflicting): the tx may now re-enter the set.
		//     If txs.unmined_since IS NOT NULL (tx is still unmined), INSERT/UPSERT into
		//     pending_unmined so the invariant == {unmined AND NOT conflicting} holds.
		//     If txs.unmined_since IS NULL (tx is mined), it must NOT be in pending_unmined
		//     — DELETE any stale row.
		// pending_unmined is ALWAYS-ON (no flag gate).
		if setValue {
			// setValue=true: tx becomes conflicting → remove from pending_unmined.
			if _, err = pgxTx.Exec(ctx,
				`DELETE FROM pending_unmined WHERE hash = $1`, txHash[:]); err != nil {
				return nil, nil, errors.NewStorageError("failed to delete pending_unmined for %s (U4-true)", txHash, err)
			}
		} else {
			// setValue=false: tx is no longer conflicting.
			// Read unmined_since from the same transaction (already updated above).
			// If unmined_since IS NOT NULL → INSERT/UPSERT into pending_unmined.
			// If unmined_since IS NULL (mined) → DELETE any stale row.
			if _, err = pgxTx.Exec(ctx, `
				WITH cur AS (
					SELECT unmined_since FROM txs WHERE hash = $1
				)
				INSERT INTO pending_unmined (hash, unmined_since)
				SELECT $1, cur.unmined_since FROM cur WHERE cur.unmined_since IS NOT NULL
				ON CONFLICT (hash) DO UPDATE SET unmined_since = EXCLUDED.unmined_since`, txHash[:]); err != nil {
				return nil, nil, errors.NewStorageError("failed to upsert pending_unmined for %s (U4-false)", txHash, err)
			}
			// Also remove the row if the tx is mined (unmined_since IS NULL); the INSERT
			// above skips that case, so a stale row from a previous reorg must be removed.
			if _, err = pgxTx.Exec(ctx, `
				DELETE FROM pending_unmined
				WHERE hash = $1
				  AND NOT EXISTS (SELECT 1 FROM txs WHERE hash = $1 AND unmined_since IS NOT NULL)`,
				txHash[:]); err != nil {
				return nil, nil, errors.NewStorageError("failed to delete stale pending_unmined for %s (U4-false mined)", txHash, err)
			}
		}

		// When we just set conflicting=true, upsert the hash into pending_deletes if
		// the row now has a delete_at_height. We use a SELECT-based INSERT (reading the
		// current DAH from the row, inside the same transaction snapshot) so preserved
		// rows — whose DAH was left NULL by the CASE above — are correctly excluded.
		// This runs inside the existing pgxTx so the upsert is always atomic with the
		// UPDATE above.
		if setValue {
			if _, err = pgxTx.Exec(ctx,
				`INSERT INTO pending_deletes (hash, delete_at_height)
				 SELECT $1, delete_at_height FROM txs WHERE hash = $1 AND delete_at_height IS NOT NULL
				 ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height`,
				txHash[:]); err != nil {
				return nil, nil, errors.NewStorageError("failed to upsert pending_deletes for %s", txHash, err)
			}
		}

		// Append this tx as a conflicting child of each parent (array on txs).
		// The @> guard makes the append idempotent: a replay/retry must not push a
		// duplicate child hash into the parent's array (matches the sql store's
		// ON CONFLICT DO NOTHING).
		if tx != nil {
			seen := make(map[chainhash.Hash]struct{}, len(tx.Inputs))
			for _, input := range tx.Inputs {
				parentHash := *input.PreviousTxIDChainHash()
				if _, ok := seen[parentHash]; ok {
					continue
				}
				seen[parentHash] = struct{}{}

				_, insertErr := pgxTx.Exec(ctx, `
					UPDATE txs SET conflicting_children = COALESCE(conflicting_children, '{}') || $2::bytea[]
					WHERE hash = $1
					  AND NOT (COALESCE(conflicting_children, '{}') @> $2::bytea[])`,
					parentHash[:], [][]byte{txHash[:]},
				)
				if insertErr != nil {
					return nil, nil, errors.NewStorageError("failed to update conflicting_children for %s", txHash, insertErr)
				}
			}
		}

		// Build affected parent spends (inputs of the conflicting tx).
		if tx != nil {
			for i, input := range tx.Inputs {
				utxoHash, hashErr := util.UTXOHashFromInput(input)
				if hashErr != nil {
					return nil, nil, hashErr
				}

				affectedParentSpends = append(affectedParentSpends, &utxo.Spend{
					TxID:         input.PreviousTxIDChainHash(),
					Vout:         input.PreviousTxOutIndex,
					UTXOHash:     utxoHash,
					SpendingData: spendpkg.NewSpendingData(&txHash, i),
				})
			}
		}

		// Find spending child transactions by querying the spends table directly.
		if tx != nil {
			rows, queryErr := pgxTx.Query(ctx, `
				SELECT sp.spending_data
				FROM spends sp
				WHERE sp.prev_tx_hash = $1`,
				txHash[:],
			)
			if queryErr != nil {
				return nil, nil, queryErr
			}

			for rows.Next() {
				var spendingDataBytes []byte
				if scanErr := rows.Scan(&spendingDataBytes); scanErr != nil {
					rows.Close()
					return nil, nil, scanErr
				}
				if len(spendingDataBytes) >= 32 {
					sd, parseErr := spendpkg.NewSpendingDataFromBytes(spendingDataBytes)
					if parseErr != nil {
						rows.Close()
						return nil, nil, parseErr
					}
					if sd.TxID != nil {
						if _, dup := seenSpenders[*sd.TxID]; !dup {
							seenSpenders[*sd.TxID] = struct{}{}
							spendingTxHashes = append(spendingTxHashes, *sd.TxID)
						}
					}
				}
			}
			rows.Close()
			if rowsErr := rows.Err(); rowsErr != nil {
				return nil, nil, rowsErr
			}
		}
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return nil, nil, errors.NewStorageError("[SetConflicting] commit", err)
	}

	return affectedParentSpends, spendingTxHashes, nil
}

// batchUnlockItem represents a single SetLocked(false) request.
type batchUnlockItem struct {
	hash chainhash.Hash
	done chan error
}

// sendUnlockBatch pipelines N UPDATE queries via SendBatch.
func (s *Store) sendUnlockBatch(batch []*batchUnlockItem) {
	s.batchStats.unlockItems.Add(int64(len(batch)))
	s.batchStats.unlockBatches.Add(1)
	ctx := context.Background()

	// Single-item fast path: direct query.
	if len(batch) == 1 {
		_, err := s.pool.Exec(ctx, `UPDATE txs SET locked = false WHERE hash = $1`, batch[0].hash[:])
		if err != nil {
			batch[0].done <- errors.NewStorageError("[Unlock] update", err)
		} else {
			batch[0].done <- nil
		}
		return
	}

	// Single bulk UPDATE — all hashes in one query.
	hashBytes := make([][]byte, len(batch))
	for i, item := range batch {
		hashBytes[i] = item.hash[:]
	}

	// AND locked = true: skip rows already unlocked so the UPDATE never writes a
	// redundant row version (dead tuple) for a no-op unlock. The end state is
	// identical (locked=false); this only avoids needless churn that competes with
	// the reclaim path for autovacuum.
	_, err := s.pool.Exec(ctx, `UPDATE txs SET locked = false FROM unnest($1::bytea[]) AS h(v) WHERE txs.hash = h.v AND locked = true`, hashBytes)
	if err != nil {
		for _, item := range batch {
			item.done <- errors.NewStorageError("[Unlock] bulk update", err)
		}
		return
	}
	for _, item := range batch {
		item.done <- nil
	}
}

// SetLocked marks transactions as locked or unlocked.
// When locking (setValue=true), delete_at_height is cleared so the tx is not pruned.
// When unlocking (setValue=false), delete_at_height is left as-is.
// Single-hash unlock calls are batched when unlockBatcher is active.
func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	// Single-hash unlock → use batcher.
	if s.unlockBatcher != nil && !setValue && len(txHashes) == 1 {
		done := make(chan error, 1)
		s.unlockBatcher.Put(&batchUnlockItem{hash: txHashes[0], done: done})
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for i := 0; i < len(txHashes); i += maxINClauseSize {
		end := i + maxINClauseSize
		if end > len(txHashes) {
			end = len(txHashes)
		}
		chunk := txHashes[i:end]

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		hashBytes := make([][]byte, len(chunk))
		for j := range chunk {
			hashBytes[j] = chunk[j][:]
		}

		// unnest-JOIN instead of a generated IN-list: one stable prepared plan for
		// every batch size (an IN-list re-plans per arity) and per-row runtime
		// partition pruning (an IN/= ANY probe descends all 8 leaf pkey btrees).
		if setValue {
			// C2: when locking (delete_at_height cleared), also remove the hashes from
			// pending_deletes in the same statement via a CTE. The DELETE is a harmless
			// no-op for hashes that were never stamped.
			const q = `WITH upd AS (
				UPDATE txs SET locked = true, delete_at_height = NULL
				  FROM unnest($1::bytea[]) AS h(v) WHERE txs.hash = h.v
				RETURNING txs.hash
			)
			DELETE FROM pending_deletes WHERE hash IN (SELECT hash FROM upd)`
			// NOTE: the CTE above makes the outer statement a DELETE, so
			// CommandTag.RowsAffected() reflects rows deleted from pending_deletes —
			// NOT rows updated in txs. Do not add accounting logic here that relies on
			// the returned row count.
			if _, err := s.pool.Exec(ctx, q, hashBytes); err != nil {
				return errors.NewStorageError("failed to set locked flag", err)
			}
		} else {
			const q = `UPDATE txs SET locked = false FROM unnest($1::bytea[]) AS h(v) WHERE txs.hash = h.v AND locked = true`
			if _, err := s.pool.Exec(ctx, q, hashBytes); err != nil {
				return errors.NewStorageError("failed to clear locked flag", err)
			}
		}
	}

	return nil
}

// MarkTransactionsOnLongestChain updates unmined_since for transactions.
// onLongestChain=true: clears unmined_since (transaction is mined on main chain).
// onLongestChain=false: sets unmined_since to current block height (transaction is unmined).
func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	// Deduplicate the input. The accounting below compares rows-updated against
	// attempted, but a SQL IN (...) clause collapses duplicate hashes to a single
	// match, so a caller passing the same hash twice would otherwise trip the
	// spurious "N/M transactions not found" error even though every tx updated.
	seen := make(map[chainhash.Hash]struct{}, len(txHashes))
	allHashBytes := make([][]byte, 0, len(txHashes))

	for i := range txHashes {
		if _, dup := seen[txHashes[i]]; dup {
			continue
		}

		seen[txHashes[i]] = struct{}{}
		allHashBytes = append(allHashBytes, txHashes[i][:])
	}

	currentBlockHeight := s.GetBlockHeight()

	attempted := len(allHashBytes)
	totalUpdated := 0
	allErrors := make([]error, 0, 10)
	errorCount := 0

	for i := 0; i < len(allHashBytes); i += maxINClauseSize {
		end := i + maxINClauseSize
		if end > len(allHashBytes) {
			end = len(allHashBytes)
		}
		chunk := allHashBytes[i:end]

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if onLongestChain {
			inClause, inArgs := buildINClauseLocal(chunk, 1)
			// U2: delete from pending_unmined atomically with the unmined_since NULL
			// clear. pending_unmined is ALWAYS-ON (no flag). SELECT count(*) FROM upd
			// returns the txs-updated count so totalUpdated accounting is correct.
			q := fmt.Sprintf(`WITH upd AS (
				UPDATE txs SET unmined_since = NULL WHERE hash IN %s RETURNING hash
			),
			_pu AS (
				DELETE FROM pending_unmined WHERE hash IN (SELECT hash FROM upd)
			)
			SELECT count(*) FROM upd`, inClause)
			var rowsAffected int
			if err := s.pool.QueryRow(ctx, q, inArgs...).Scan(&rowsAffected); err != nil {
				errorCount += len(chunk)
				if len(allErrors) < 10 {
					s.logger.Errorf("[MarkTransactionsOnLongestChain] chunk %d-%d error: %v", i, end-1, err)
					allErrors = append(allErrors, errors.NewStorageError("failed to mark chunk %d-%d", i, end-1, err))
				}
				continue
			}
			totalUpdated += rowsAffected
		} else {
			// Leaving the longest chain: also CLEAR any deferred-prune stamp. The
			// stamp was computed for the tx as mined-and-fully-spent ON THE OLD chain;
			// it must not survive to let the pruner delete a tx that is no longer on
			// the longest chain. This clear is LOAD-BEARING, not belt-and-braces: the
			// pruner trusts delete_at_height and does NOT re-check on-longest-chain at
			// delete time (see the DESIGN CONTRACT in deleteTombstonedPartition), so a
			// stale stamp left here would let a reorged-out tx be deleted. The sweep
			// re-stamps if/when it rejoins the longest chain.
			inClause, inArgs := buildINClauseLocal(chunk, 2)
			chunkArgs := append([]interface{}{int32(currentBlockHeight)}, inArgs...) // unmined_since is INT4

			// DO NOT "fix" this by wrapping the chunk loop in a single transaction
			// and rolling back on error. This branch is a CLEAR (delete_at_height = NULL)
			// that makes a reorged-out tx safe from the pruner, so partial application is
			// SAFER than none: rolling back on a chunk failure would un-clear the chunks
			// already made safe, enlarging the stale-stamp set from just the failed chunk
			// to the ENTIRE batch. (Atomicity is the right property for a SET, where partial
			// application is the hazard — not for a clear.) Instead we continue, clearing
			// every remaining chunk, and return the joined error so the caller retries the
			// whole, idempotent set. A failed chunk's stamp is a FUTURE height
			// (completion + retention), so the pruner cannot act on it before a retry
			// (or the next reorg pass) re-clears it.
			var rowsAffected int
			// C3 + U3: remove the hashes from pending_deletes in the SAME statement as
			// the DAH clear via CTEs.
			// U3 (into-set direction): this call sets txs.unmined_since = $1 (non-NULL)
			// and the tx is NOT conflicting, so per the == invariant it must be PRESENT
			// in pending_unmined. INSERT/UPSERT atomically with the UPDATE.
			// SELECT count(*) FROM upd returns the txs-updated count so totalUpdated
			// accounting is correct. pending_unmined is ALWAYS-ON (no flag).
			q := fmt.Sprintf(`WITH upd AS (
				UPDATE txs SET unmined_since = $1, delete_at_height = NULL WHERE hash IN %s RETURNING hash, unmined_since
			),
			_del AS (
				DELETE FROM pending_deletes WHERE hash IN (SELECT hash FROM upd)
			),
			_pu AS (
				INSERT INTO pending_unmined (hash, unmined_since)
				SELECT hash, unmined_since FROM upd WHERE NOT EXISTS (
					SELECT 1 FROM txs WHERE txs.hash = upd.hash AND txs.conflicting
				)
				ON CONFLICT (hash) DO UPDATE SET unmined_since = EXCLUDED.unmined_since
			)
			SELECT count(*) FROM upd`, inClause)
			if err := s.pool.QueryRow(ctx, q, chunkArgs...).Scan(&rowsAffected); err != nil {
				errorCount += len(chunk)
				if len(allErrors) < 10 {
					s.logger.Errorf("[MarkTransactionsOnLongestChain] chunk %d-%d error: %v", i, end-1, err)
					allErrors = append(allErrors, errors.NewStorageError("failed to mark chunk %d-%d", i, end-1, err))
				}
				continue
			}
			totalUpdated += rowsAffected
		}
	}

	if errorCount > 0 {
		s.logger.Errorf("[MarkTransactionsOnLongestChain] completed with errors: attempted=%d, succeeded=%d, failed=%d",
			attempted, totalUpdated, errorCount)
		return errors.Join(allErrors...)
	}

	if totalUpdated < attempted {
		missing := attempted - totalUpdated
		s.logger.Errorf("[MarkTransactionsOnLongestChain] %d/%d transactions not found in txs", missing, attempted)
		// A missing tx is a data-integrity problem the caller must see: the sql
		// store treats it as fatal. Surface it as an error rather than returning
		// nil and silently reporting success.
		return errors.NewStorageError("[MarkTransactionsOnLongestChain] %d/%d transactions not found in txs", missing, attempted)
	}

	return nil
}
