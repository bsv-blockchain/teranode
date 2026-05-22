package sql

import (
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
