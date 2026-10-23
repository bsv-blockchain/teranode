package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

func (s *Store) SaveConflictRecovery(ctx context.Context, id chainhash.Hash, r *utxo.ConflictRecovery) error {
	data, err := utxo.EncodeConflictRecovery(r)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE conflict_intents SET recovery = $2 WHERE intent_id = $1`, id[:], data)
	if err != nil {
		return errors.NewStorageError("saving conflict recovery checklist", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.NewStorageError("conflict recovery intent %s is missing", id.String())
	}
	return nil
}

func (s *Store) LoadConflictRecovery(ctx context.Context, id chainhash.Hash) (*utxo.ConflictRecovery, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT recovery FROM conflict_intents WHERE intent_id = $1`, id[:]).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.NewStorageError("loading conflict recovery checklist", err)
	}
	return utxo.DecodeConflictRecovery(data)
}

func (s *Store) RestoreConflictSpends(ctx context.Context, spends []*utxo.Spend, height uint32) error {
	for _, sp := range spends {
		if sp == nil || sp.SpendingData == nil {
			return errors.NewProcessingError("invalid conflict restoration spend")
		}
		// Run the existing conditional spend writer synchronously. No queued write
		// may arrive after a timeout and undo a later recovery phase.
		done := make(chan error, 1)
		s.sendSpendBatch([]*batchSpend{{spend: sp, blockHeight: height, errCh: done, ignoreConflicting: true, ignoreLocked: true}})
		if err := <-done; err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

// The owner is the durable operation ID, not the process ID. Ordinary lock
// writers clear conflict_owner atomically when they change locked. Recovery
// therefore cannot release a lock acquired by another operation after a crash.
func (s *Store) SetConflictLocks(ctx context.Context, id chainhash.Hash, parents []chainhash.Hash, locked bool) error {
	txn, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()
	for _, h := range parents {
		var rowID int
		if locked {
			err = txn.QueryRowContext(ctx, `UPDATE transactions SET locked = true, conflict_owner = $2, delete_at_height = NULL WHERE hash = $1 AND (locked = false OR conflict_owner = $2) RETURNING id`, h[:], id[:]).Scan(&rowID)
		} else {
			err = txn.QueryRowContext(ctx, `UPDATE transactions SET locked = false, conflict_owner = NULL WHERE hash = $1 AND conflict_owner = $2 RETURNING id`, h[:], id[:]).Scan(&rowID)
			if errors.Is(err, sql.ErrNoRows) {
				var alreadyUnlocked bool
				if e := txn.QueryRowContext(ctx, `SELECT NOT locked FROM transactions WHERE hash = $1`, h[:]).Scan(&alreadyUnlocked); e != nil {
					return e
				}
				if alreadyUnlocked {
					continue
				}
			}
		}
		if err != nil {
			return errors.NewProcessingError("conflict operation %s does not own parent lock %s", id.String(), h.String(), err)
		}
		if !locked {
			if err := s.setDAH(ctx, txn, rowID); err != nil {
				return err
			}
		}
	}
	return txn.Commit()
}
