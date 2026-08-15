package packedsql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
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

	if _, err := s.pool.Exec(ctx,
		`INSERT INTO conflict_intents (intent_id, kind, block_height, block_hash, tx_hashes, started_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (intent_id) DO NOTHING`,
		intentID[:], string(intent.Kind), int64(intent.BlockHeight), intent.BlockHash[:],
		encodeIntentHashes(intent.TxHashes), intent.StartedAt); err != nil {
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
		`SELECT kind, block_height, block_hash, tx_hashes, started_at FROM conflict_intents`)
	if err != nil {
		return nil, errors.NewStorageError("packedsql: pending conflict intents query failed", err)
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

		if err = rows.Scan(&kind, &blockHeight, &blockHash, &txHashes, &startedAt); err != nil {
			return nil, errors.NewStorageError("packedsql: pending conflict intents scan failed", err)
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
		})
	}

	if err = rows.Err(); err != nil {
		return nil, errors.NewStorageError("packedsql: pending conflict intents iteration failed", err)
	}

	return intents, nil
}
