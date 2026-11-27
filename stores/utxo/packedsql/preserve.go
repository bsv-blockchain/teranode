package packedsql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

func (s *Store) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT hash FROM packed_txs
		 WHERE unmined_since IS NOT NULL AND unmined_since <= $1
		 ORDER BY unmined_since LIMIT 1000`, int64(cutoffBlockHeight))
	if err != nil {
		return nil, errors.NewStorageError("packedsql: failed to query old unmined transactions", err)
	}

	defer rows.Close()

	var txHashes []chainhash.Hash

	for rows.Next() {
		var hb []byte
		if err = rows.Scan(&hb); err != nil {
			return nil, errors.NewStorageError("packedsql: failed to scan old unmined transaction", err)
		}

		txHashes = append(txHashes, chainhash.Hash(hb))
	}

	if err = rows.Err(); err != nil {
		return nil, errors.NewStorageError("packedsql: error iterating old unmined transactions", err)
	}

	return txHashes, nil
}

func (s *Store) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	if len(txIDs) == 0 {
		return nil
	}

	for start := 0; start < len(txIDs); start += setMinedChunkSize {
		end := min(start+setMinedChunkSize, len(txIDs))
		chunk := txIDs[start:end]

		hashBytes := make([][]byte, len(chunk))
		for i := range chunk {
			hashBytes[i] = chunk[i][:]
		}

		if _, err := s.pool.Exec(ctx,
			`UPDATE packed_txs SET preserve_until = $1, delete_at_height = NULL
			 WHERE hash = ANY($2::bytea[])
			 AND (delete_at_height IS NOT NULL OR preserve_until IS NOT NULL)`,
			int64(preserveUntilHeight), hashBytes); err != nil {
			return errors.NewStorageError("packedsql: failed to preserve transactions", err)
		}
	}

	return nil
}

func (s *Store) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	if retention == 0 {
		if _, err := s.pool.Exec(ctx,
			`UPDATE packed_txs SET preserve_until = NULL WHERE preserve_until IS NOT NULL AND preserve_until <= $1`,
			int64(currentHeight)); err != nil {
			return errors.NewStorageError("packedsql: failed to process expired preservations", err)
		}

		return nil
	}

	deleteAtHeight := int64(currentHeight) + int64(retention)

	if _, err := s.pool.Exec(ctx,
		`UPDATE packed_txs SET
		   delete_at_height = CASE
		     WHEN (flags & 4) <> 0 THEN $1::bigint
		     WHEN unmined_since IS NULL
		          AND octet_length(coalesce(block_refs, ''::bytea)) > 0
		          AND spent_count >= page0_count AND pages_spent >= pages_total
		     THEN $1::bigint
		     ELSE NULL
		   END,
		   preserve_until = NULL
		 WHERE preserve_until IS NOT NULL AND preserve_until <= $2`,
		deleteAtHeight, int64(currentHeight)); err != nil {
		return errors.NewStorageError("packedsql: failed to process expired preservations", err)
	}

	return nil
}
