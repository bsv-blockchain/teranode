package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// GetConflictingTxIterator returns an iterator over transactions currently
// marked conflicting=true. The iterator reuses the unminedTxIterator machinery
// (same row-to-UnminedTransaction mapping) but flips the filter.
func (s *Store) GetConflictingTxIterator() (utxo.UnminedTxIterator, error) {
	return newConflictingTxIterator(s)
}

func newConflictingTxIterator(store *Store) (*unminedTxIterator, error) {
	it := &unminedTxIterator{
		store: store,
	}

	q := `
		SELECT
		 t.id
		,t.hash
		,t.fee
		,t.size_in_bytes
		,t.inserted_at
		,t.locked
		,t.coinbase
		,COALESCE(t.unmined_since, 0)
		FROM transactions t
		WHERE t.conflicting = true
		ORDER BY t.id ASC
	`

	rows, err := store.db.Query(q)
	if err != nil {
		return nil, err
	}

	it.rows = rows

	return it, nil
}

// ConflictingTxHashes returns the hash of every transaction flagged conflicting,
// read through the px_transactions_conflicting partial index. SQL always answers
// from the index, so indexed is true whenever err is nil.
func (s *Store) ConflictingTxHashes(ctx context.Context) ([]chainhash.Hash, bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT hash FROM transactions WHERE conflicting = true`)
	if err != nil {
		return nil, false, errors.NewStorageError("[ConflictingTxHashes] query failed", err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var hashes []chainhash.Hash

	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, false, errors.NewStorageError("[ConflictingTxHashes] scan failed", err)
		}

		hash, hashErr := chainhash.NewHash(raw)
		if hashErr != nil {
			return nil, false, errors.NewStorageError("[ConflictingTxHashes] bad hash in transactions row", hashErr)
		}

		hashes = append(hashes, *hash)
	}

	if err = rows.Err(); err != nil {
		return nil, false, errors.NewStorageError("[ConflictingTxHashes] row iteration failed", err)
	}

	return hashes, true, nil
}
