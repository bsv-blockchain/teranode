package packedsql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

func encodeIntentHashes(hashes []chainhash.Hash) []byte {
	buf := make([]byte, 0, len(hashes)*chainhash.HashSize)
	for i := range hashes {
		buf = append(buf, hashes[i][:]...)
	}

	return buf
}

func decodeIntentHashes(buf []byte) ([]chainhash.Hash, error) {
	if len(buf)%chainhash.HashSize != 0 {
		return nil, errors.NewStorageError("packedsql: conflict_intents tx_hashes blob length %d is not a multiple of %d", len(buf), chainhash.HashSize)
	}

	hashes := make([]chainhash.Hash, 0, len(buf)/chainhash.HashSize)
	for off := 0; off < len(buf); off += chainhash.HashSize {
		var h chainhash.Hash

		copy(h[:], buf[off:off+chainhash.HashSize])
		hashes = append(hashes, h)
	}

	return hashes, nil
}

func (s *Store) BeginConflictIntent(ctx context.Context, intent utxo.ConflictIntent) error {
	intentID := intent.IntentID()

	var recoveryData []byte

	if intent.Recovery != nil {
		var err error

		if recoveryData, err = utxo.EncodeConflictRecovery(intent.Recovery); err != nil {
			return err
		}
	}

	if _, err := s.pool.Exec(ctx,
		`INSERT INTO conflict_intents (intent_id, kind, block_height, block_hash, tx_hashes, started_at, recovery)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (intent_id) DO NOTHING`,
		intentID[:], string(intent.Kind), int64(intent.BlockHeight), intent.BlockHash[:],
		encodeIntentHashes(intent.TxHashes), intent.StartedAt, recoveryData); err != nil {
		return errors.NewStorageError("packedsql: failed to record conflict intent %s", intentID, err)
	}

	return nil
}

func (s *Store) CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM conflict_intents WHERE intent_id = $1`, intentID[:]); err != nil {
		return errors.NewStorageError("packedsql: failed to remove conflict intent %s", intentID, err)
	}

	return nil
}

func (s *Store) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT intent_id, kind, block_height, block_hash, tx_hashes, started_at FROM conflict_intents`)
	if err != nil {
		return nil, errors.NewStorageError("packedsql: pending conflict intents query failed", err)
	}

	defer rows.Close()

	var intents []utxo.ConflictIntent

	for rows.Next() {
		var (
			intentID    []byte
			kind        string
			blockHeight int64
			blockHash   []byte
			txHashes    []byte
			startedAt   int64
		)

		if err = rows.Scan(&intentID, &kind, &blockHeight, &blockHash, &txHashes, &startedAt); err != nil {
			return nil, errors.NewStorageError("packedsql: pending conflict intents scan failed", err)
		}

		id, err := chainhash.NewHash(intentID)
		if err != nil {
			return nil, errors.NewStorageError("packedsql: corrupt conflict intent id", err)
		}

		bh, err := chainhash.NewHash(blockHash)
		if err != nil {
			return nil, errors.NewStorageError("packedsql: corrupt conflict intent block_hash", err)
		}

		hashes, err := decodeIntentHashes(txHashes)
		if err != nil {
			return nil, err
		}

		intents = append(intents, utxo.ConflictIntent{
			Kind:        utxo.ConflictIntentKind(kind),
			BlockHeight: uint32(blockHeight), //nolint:gosec
			BlockHash:   *bh,
			TxHashes:    hashes,
			StartedAt:   startedAt,
			ID:          *id,
		})
	}

	if err = rows.Err(); err != nil {
		return nil, errors.NewStorageError("packedsql: pending conflict intents iteration failed", err)
	}

	return intents, nil
}

func (s *Store) SaveConflictRecovery(ctx context.Context, intentID chainhash.Hash, recovery *utxo.ConflictRecovery) error {
	data, err := utxo.EncodeConflictRecovery(recovery)
	if err != nil {
		return err
	}

	ct, err := s.pool.Exec(ctx, `UPDATE conflict_intents SET recovery = $2 WHERE intent_id = $1`, intentID[:], data)
	if err != nil {
		return errors.NewStorageError("packedsql: failed to save conflict recovery checklist", err)
	}

	if ct.RowsAffected() != 1 {
		return errors.NewStorageError("packedsql: conflict recovery intent %s is missing", intentID)
	}

	return nil
}

func (s *Store) LoadConflictRecovery(ctx context.Context, intentID chainhash.Hash) (*utxo.ConflictRecovery, error) {
	var data []byte

	err := s.pool.QueryRow(ctx, `SELECT recovery FROM conflict_intents WHERE intent_id = $1`, intentID[:]).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, errors.NewStorageError("packedsql: failed to load conflict recovery checklist", err)
	}

	return utxo.DecodeConflictRecovery(data)
}

// RestoreConflictSpends writes each spend synchronously on ctx through the ordinary spend
// writer, ignoring the conflicting and locked flags. A different existing spender is refused
// and the same spender is accepted. Cancelling ctx stops the write, so no queued work can land
// after the caller gave up.
func (s *Store) RestoreConflictSpends(ctx context.Context, spends []*utxo.Spend, blockHeight uint32) error {
	ig := utxo.IgnoreFlags{IgnoreConflicting: true, IgnoreLocked: true}

	for _, sp := range spends {
		if err := ctx.Err(); err != nil {
			return err
		}

		if sp == nil || sp.SpendingData == nil {
			return errors.NewProcessingError("packedsql: invalid conflict restoration spend")
		}

		if err := s.spendOnQuerier(ctx, s.pool, []*utxo.Spend{sp}, blockHeight, ig); err != nil {
			return err
		}
	}

	return nil
}

// SetConflictLocks takes or releases the parent locks this operation owns. conflict_owner
// holds the durable operation id, and every ordinary lock writer clears it, so an operation
// never releases a lock another holder took.
func (s *Store) SetConflictLocks(ctx context.Context, intentID chainhash.Hash, parents []chainhash.Hash, locked bool) error {
	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.NewStorageError("packedsql: failed to begin conflict lock transaction", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	for _, h := range parents {
		if locked {
			ct, execErr := dbTx.Exec(ctx,
				`UPDATE packed_txs SET flags = flags | 8, conflict_owner = $2, delete_at_height = NULL
				 WHERE hash = $1 AND ((flags & 8) = 0 OR conflict_owner = $2)`, h[:], intentID[:])
			if execErr != nil {
				return errors.NewStorageError("packedsql: failed to lock conflict parent %s", h, execErr)
			}

			if ct.RowsAffected() == 0 {
				return errors.NewProcessingError("packedsql: conflict operation %s does not own parent lock %s", intentID, h)
			}

			continue
		}

		released, relErr := s.releaseConflictLock(ctx, dbTx, intentID, h)
		if relErr != nil {
			return relErr
		}

		if released {
			continue
		}

		var alreadyUnlocked bool

		if qErr := dbTx.QueryRow(ctx, `SELECT (flags & 8) = 0 FROM packed_txs WHERE hash = $1`, h[:]).Scan(&alreadyUnlocked); qErr != nil {
			if errors.Is(qErr, pgx.ErrNoRows) {
				return errors.NewTxNotFoundError("packedsql: conflict parent %s not found", h, qErr)
			}

			return errors.NewStorageError("packedsql: failed to read conflict parent lock %s", h, qErr)
		}

		if !alreadyUnlocked {
			return errors.NewProcessingError("packedsql: conflict operation %s does not own parent lock %s", intentID, h)
		}
	}

	if err = dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("packedsql: failed to commit conflict lock transaction", err)
	}

	return nil
}

// ReleaseConflictLocks releases only the locks conflict_owner shows this operation still owns.
// A lock another holder took, or one this operation never took, is left as it is.
func (s *Store) ReleaseConflictLocks(ctx context.Context, intentID chainhash.Hash, parents []chainhash.Hash) error {
	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.NewStorageError("packedsql: failed to begin conflict lock release", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	for _, h := range parents {
		if _, err = s.releaseConflictLock(ctx, dbTx, intentID, h); err != nil {
			return err
		}
	}

	if err = dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("packedsql: failed to commit conflict lock release", err)
	}

	return nil
}

func (s *Store) releaseConflictLock(ctx context.Context, q pgxQuerier, intentID, parent chainhash.Hash) (bool, error) {
	ct, err := q.Exec(ctx,
		`UPDATE packed_txs SET flags = flags & ~8, conflict_owner = NULL, `+unlockDAHSQL+`
		 WHERE hash = $1 AND conflict_owner = $3`, parent[:], s.unlockDAH(), intentID[:])
	if err != nil {
		return false, errors.NewStorageError("packedsql: failed to release conflict lock %s", parent, err)
	}

	return ct.RowsAffected() > 0, nil
}
