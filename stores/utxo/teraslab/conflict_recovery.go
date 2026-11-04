package teraslab

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teraslab "github.com/icellan/teraslab/client/go"
)

// This file implements the conflict-recovery methods of utxo.Store for the
// TeraSlab backend. The recovery checklist is persisted in the recovery column
// of the WAL's conflict_intents table (s.walDB — see conflict_wal.go), so Save /
// Load mirror the SQL backend. RestoreConflictSpends re-applies recorded spends
// against the TeraSlab server, mirroring the Aerospike backend (an external store
// with no local SQL UTXO table). The lock methods are no-ops; see below.

// SaveConflictRecovery atomically publishes the recovery checklist and phase onto
// the already-begun intent. The intent must exist (BeginConflictIntent ran first).
func (s *Store) SaveConflictRecovery(ctx context.Context, id chainhash.Hash, r *utxo.ConflictRecovery) error {
	data, err := utxo.EncodeConflictRecovery(r)
	if err != nil {
		return err
	}

	result, err := s.walDB.ExecContext(ctx, `UPDATE conflict_intents SET recovery = $2 WHERE intent_id = $1`, id[:], data)
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

// LoadConflictRecovery reads the recovery checklist for an intent. It returns nil
// for an absent or legacy intent (no row, or a NULL/empty recovery column —
// DecodeConflictRecovery treats empty input as nil).
func (s *Store) LoadConflictRecovery(ctx context.Context, id chainhash.Hash) (*utxo.ConflictRecovery, error) {
	var data []byte

	err := s.walDB.QueryRowContext(ctx, `SELECT recovery FROM conflict_intents WHERE intent_id = $1`, id[:]).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, errors.NewStorageError("loading conflict recovery checklist", err)
	}

	return utxo.DecodeConflictRecovery(data)
}

// RestoreConflictSpends re-applies the recorded spends against the TeraSlab
// server. The spends are applied with IgnoreConflicting + IgnoreLocked so a
// winner's inputs are re-spent regardless of the conflict/lock flags set during
// the operation being rolled back. The client call is synchronous (it does not
// go through the spend batcher), so no queued write can land after a later
// recovery phase and undo it, matching the Aerospike backend.
func (s *Store) RestoreConflictSpends(ctx context.Context, spends []*utxo.Spend, height uint32) error {
	for _, sp := range spends {
		if sp == nil || sp.SpendingData == nil {
			return errors.NewProcessingError("invalid conflict restoration spend")
		}
	}

	batchSize := s.settings.UtxoStore.SpendBatcherSize
	if batchSize <= 0 {
		batchSize = len(spends)
	}

	params := teraslab.SpendBatchParams{
		IgnoreConflicting:    true,
		IgnoreLocked:         true,
		CurrentBlockHeight:   height,
		BlockHeightRetention: s.settings.GetUtxoStoreBlockHeightRetention(),
	}

	for start := 0; start < len(spends); start += batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch := spends[start:min(start+batchSize, len(spends))]

		items := make([]teraslab.SpendItem, len(batch))
		for i, sp := range batch {
			items[i] = teraslab.SpendItem{
				TxID:         hashToTxID(sp.TxID),
				Vout:         sp.Vout,
				UtxoHash:     hashToUtxoHash(sp.UTXOHash),
				SpendingData: spendingDataToWire(sp.SpendingData),
			}
		}

		if _, err := s.client.SpendBatch(ctx, params, items); err != nil {
			// Any per-item error during restore is a real fault. The server omits
			// an idempotent same-spender re-spend (missing = success), so an item
			// that does come back is a different spender or a genuine failure.
			// TxNotFound included: TeraSlab has no "already blessed" fallback (the
			// Aerospike requireParentOutput path), so a not-found parent here halts.
			if pe, ok := err.(*teraslab.PartialError); ok {
				return partialErrorToError("RestoreConflictSpends", pe)
			}

			return err
		}
	}

	return nil
}

// SetConflictLocks is a no-op for TeraSlab. The SQL backend owns conflict locks
// per operation via a conflict_owner column; TeraSlab's lock is an un-owned
// boolean (the record's LOCKED flag), so recovery cannot tell a lock this
// conflict operation took from one a concurrent validation 2PC holds. Releasing
// or re-acquiring blindly would clobber the latter (issue 1291). The accepted
// post-crash cost — conflict-locked parents may stay locked until re-mined — is
// the same trade-off the Aerospike backend makes. Unlike Aerospike (whose Unspend
// does not touch parent locks), TeraSlab's Unspend sets the lock state
// symmetrically (spend.go), so the non-crash ReverseProcessConflicting path is
// still covered; only the crash-replay re-acquire is skipped here.
func (s *Store) SetConflictLocks(_ context.Context, _ chainhash.Hash, _ []chainhash.Hash, _ bool) error {
	return nil
}

// ReleaseConflictLocks is a no-op for the same reason as SetConflictLocks.
func (s *Store) ReleaseConflictLocks(_ context.Context, _ chainhash.Hash, _ []chainhash.Hash) error {
	return nil
}
