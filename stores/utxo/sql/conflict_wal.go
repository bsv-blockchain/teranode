package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// This file implements the conflict-resolution write-ahead log (WAL) for the
// SQL backend — crash safety for ProcessConflicting / ReverseProcessConflicting
// (see #861). Intents live in the conflict_intents table (DDL in
// createPostgresSchemaImpl / createSqliteSchema).

// encodeIntentHashes flattens the intent's tx hashes into a single byte slice
// (32 bytes each) for storage in the tx_hashes column.
func encodeIntentHashes(hashes []chainhash.Hash) []byte {
	buf := make([]byte, 0, len(hashes)*chainhash.HashSize)
	for i := range hashes {
		buf = append(buf, hashes[i][:]...)
	}

	return buf
}

// decodeIntentHashes splits a stored tx_hashes blob back into chainhash values.
func decodeIntentHashes(buf []byte) ([]chainhash.Hash, error) {
	if len(buf)%chainhash.HashSize != 0 {
		return nil, errors.NewStorageError("conflict_intents tx_hashes blob length %d is not a multiple of %d", len(buf), chainhash.HashSize)
	}

	hashes := make([]chainhash.Hash, 0, len(buf)/chainhash.HashSize)
	for off := 0; off < len(buf); off += chainhash.HashSize {
		var h chainhash.Hash
		copy(h[:], buf[off:off+chainhash.HashSize])
		hashes = append(hashes, h)
	}

	return hashes, nil
}

// BeginConflictIntent durably records an intent before the operation's first
// state mutation. Idempotent on the deterministic intent id: re-inserting the
// same intent is a no-op rather than a duplicate-key error.
func (s *Store) BeginConflictIntent(ctx context.Context, intent utxo.ConflictIntent) error {
	intentID := intent.IntentID()

	var q string
	if s.engine == "postgres" {
		q = `INSERT INTO conflict_intents (intent_id, kind, block_height, block_hash, tx_hashes, started_at)
		     VALUES ($1, $2, $3, $4, $5, $6)
		     ON CONFLICT (intent_id) DO NOTHING`
	} else {
		q = `INSERT OR IGNORE INTO conflict_intents (intent_id, kind, block_height, block_hash, tx_hashes, started_at)
		     VALUES ($1, $2, $3, $4, $5, $6)`
	}

	if _, err := s.db.ExecContext(ctx, q,
		intentID[:],
		string(intent.Kind),
		int64(intent.BlockHeight),
		intent.BlockHash[:],
		encodeIntentHashes(intent.TxHashes),
		intent.StartedAt,
	); err != nil {
		return errors.NewStorageError("[BeginConflictIntent] failed to record intent %s", intentID.String(), err)
	}

	return nil
}

// CompleteConflictIntent removes the intent record once the terminal step
// committed. Removing an absent intent is idempotent (no error).
func (s *Store) CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM conflict_intents WHERE intent_id = $1`, intentID[:]); err != nil {
		return errors.NewStorageError("[CompleteConflictIntent] failed to remove intent %s", intentID.String(), err)
	}

	return nil
}

// PendingConflictIntents returns every begun-but-not-completed intent.
func (s *Store) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT kind, block_height, block_hash, tx_hashes, started_at FROM conflict_intents`)
	if err != nil {
		return nil, errors.NewStorageError("[PendingConflictIntents] query failed", err)
	}
	defer rows.Close()

	var intents []utxo.ConflictIntent

	for rows.Next() {
		var (
			kind        string
			blockHeight int64
			blockHash   []byte
			txHashes    []byte
			startedAt   int64
		)

		if err := rows.Scan(&kind, &blockHeight, &blockHash, &txHashes, &startedAt); err != nil {
			return nil, errors.NewStorageError("[PendingConflictIntents] scan failed", err)
		}

		bh, err := chainhash.NewHash(blockHash)
		if err != nil {
			return nil, errors.NewStorageError("[PendingConflictIntents] corrupt block_hash (kind=%s height=%d startedAt=%d)", kind, blockHeight, startedAt, err)
		}

		hashes, err := decodeIntentHashes(txHashes)
		if err != nil {
			return nil, errors.NewStorageError("[PendingConflictIntents] corrupt intent row (kind=%s height=%d startedAt=%d)", kind, blockHeight, startedAt, err)
		}

		intents = append(intents, utxo.ConflictIntent{
			Kind:        utxo.ConflictIntentKind(kind),
			BlockHeight: uint32(blockHeight),
			BlockHash:   *bh,
			TxHashes:    hashes,
			StartedAt:   startedAt,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[PendingConflictIntents] row iteration failed", err)
	}

	return intents, nil
}
