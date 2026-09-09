package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// The conflict-resolution write-ahead log. ProcessConflicting and
// ReverseProcessConflicting move several transactions' state in sequence, and a process
// killed between two of those steps leaves the set half-moved. The intent row is written
// before the first mutation and removed after the last, so anything still in the table at
// startup is work a crash interrupted; block assembly reads it once and replays it.
//
// The table is in schemaSQL, unpartitioned and keyed on the intent id alone. Sizing it is
// not a judgement call: an in-flight resolution exists only for the duration of one call,
// so the steady-state row count is the number of resolutions running right now.

// intentInsertSQL records one intent, idempotently.
//
// ON CONFLICT DO NOTHING rather than an upsert, and the difference matters. The id is a
// hash over the operation's own inputs, so a row already carrying that id describes THIS
// operation, begun before the crash that is now being retried. Overwriting it would move
// started_at forward and lose the original attempt's timestamp, which is the only thing
// that says how long the interrupted work has been outstanding.
const intentInsertSQL = `
INSERT INTO conflict_intents (intent_id, kind, block_height, block_hash, tx_hashes, started_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (intent_id) DO NOTHING`

const intentDeleteSQL = `DELETE FROM conflict_intents WHERE intent_id = $1`

// intentSelectSQL reads every unfinished intent, oldest first.
//
// The order is by started_at because replay is replaying history: two intents that
// overlap were begun in that order, and applying the later one first would leave the
// earlier one's decisions on top of state it was never computed against.
const intentSelectSQL = `
SELECT kind, block_height, block_hash, tx_hashes, started_at
  FROM conflict_intents
 ORDER BY started_at`

// encodeIntentHashes flattens the intent's hashes into one 32-byte-per-hash blob.
//
// In the order the caller gave, NOT sorted. The id is computed over a sorted copy so that
// re-beginning the same operation is idempotent whatever order it arrives in, but the list
// itself is the operation's own argument and the caller gets it back unchanged.
func encodeIntentHashes(hashes []chainhash.Hash) []byte {
	buf := make([]byte, 0, len(hashes)*chainhash.HashSize)
	for i := range hashes {
		buf = append(buf, hashes[i][:]...)
	}

	return buf
}

// decodeIntentHashes splits a stored blob back into hashes.
//
// A length that is not a multiple of 32 is an error rather than a truncation. The blob is
// the set of transactions a replay is about to act on; serving a short read of it would
// hand block assembly a subset and let it declare the resolution complete.
func decodeIntentHashes(buf []byte) ([]chainhash.Hash, error) {
	if len(buf)%chainhash.HashSize != 0 {
		return nil, errors.NewStorageError("[utxoset] conflict_intents tx_hashes blob length %d is not a multiple of %d",
			len(buf), chainhash.HashSize)
	}

	hashes := make([]chainhash.Hash, 0, len(buf)/chainhash.HashSize)

	for off := 0; off < len(buf); off += chainhash.HashSize {
		var h chainhash.Hash

		copy(h[:], buf[off:off+chainhash.HashSize])
		hashes = append(hashes, h)
	}

	return hashes, nil
}

// BeginConflictIntent durably records an intent before the operation's first mutation.
//
// The caller MUST abort on an error from this: proceeding without the record is what
// leaves a half-applied resolution nothing can find. Idempotent on the deterministic id.
func (s *Store) BeginConflictIntent(ctx context.Context, intent utxo.ConflictIntent) error {
	intentID := intent.IntentID()

	if _, err := s.pool.Exec(ctx, intentInsertSQL,
		intentID[:],
		string(intent.Kind),
		int32(intent.BlockHeight), //nolint:gosec // block heights are far below 2^31
		intent.BlockHash[:],
		encodeIntentHashes(intent.TxHashes),
		intent.StartedAt,
	); err != nil {
		return errors.NewStorageError("[utxoset][BeginConflictIntent] record intent %s", intentID.String(), err)
	}

	return nil
}

// CompleteConflictIntent removes the record once the terminal step has committed.
//
// Zero rows is a success, not a miss. Completing twice is how a retried operation ends,
// and the only fact this needs to establish is that the row is gone.
func (s *Store) CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error {
	if _, err := s.pool.Exec(ctx, intentDeleteSQL, intentID[:]); err != nil {
		return errors.NewStorageError("[utxoset][CompleteConflictIntent] remove intent %s", intentID.String(), err)
	}

	return nil
}

// PendingConflictIntents returns every intent begun but not completed.
//
// Block assembly calls this once at startup and treats an error as fatal, which is right:
// a store that cannot say whether a conflict resolution was interrupted must not be
// allowed to start assembling blocks on top of the half-applied state.
func (s *Store) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	rows, err := s.pool.Query(ctx, intentSelectSQL)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][PendingConflictIntents] query", err)
	}

	defer rows.Close()

	var intents []utxo.ConflictIntent

	for rows.Next() {
		var (
			kind        string
			blockHeight int32
			blockHash   []byte
			txHashes    []byte
			startedAt   int64
		)

		if err := rows.Scan(&kind, &blockHeight, &blockHash, &txHashes, &startedAt); err != nil {
			return nil, errors.NewStorageError("[utxoset][PendingConflictIntents] scan", err)
		}

		bh, bhErr := chainhash.NewHash(blockHash)
		if bhErr != nil {
			return nil, errors.NewStorageError("[utxoset][PendingConflictIntents] corrupt block_hash (kind=%s height=%d startedAt=%d)",
				kind, blockHeight, startedAt, bhErr)
		}

		hashes, hErr := decodeIntentHashes(txHashes)
		if hErr != nil {
			return nil, errors.NewStorageError("[utxoset][PendingConflictIntents] corrupt intent row (kind=%s height=%d startedAt=%d)",
				kind, blockHeight, startedAt, hErr)
		}

		intents = append(intents, utxo.ConflictIntent{
			Kind:        utxo.ConflictIntentKind(kind),
			BlockHeight: uint32(blockHeight), //nolint:gosec // written from a uint32 block height
			BlockHash:   *bh,
			TxHashes:    hashes,
			StartedAt:   startedAt,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][PendingConflictIntents] rows", err)
	}

	return intents, nil
}
