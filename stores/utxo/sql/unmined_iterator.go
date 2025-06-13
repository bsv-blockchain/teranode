package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// unminedTxIterator implements utxo.UnminedTxIterator for SQL
type unminedTxIterator struct {
	store *Store
	err   error
	done  bool
	rows  *sql.Rows
}

func newUnminedTxIterator(store *Store) (*unminedTxIterator, error) {
	it := &unminedTxIterator{
		store: store,
	}

	q := `
		SELECT
		 t.id
		,t.hash
		,t.fee
		,t.size_in_bytes
		FROM transactions t
		WHERE t.not_mined = TRUE
	`

	rows, err := store.db.Query(q)
	if err != nil {
		return nil, err
	}

	it.rows = rows

	return it, nil
}

func (it *unminedTxIterator) Next(ctx context.Context) (*utxo.UnminedTransaction, error) {
	if it.done || it.err != nil || it.rows == nil {
		return nil, it.err
	}

	more := it.rows.Next()
	if !more {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		return nil, nil
	}

	var (
		id          uint64
		txID        *chainhash.Hash
		fee         uint64
		sizeInBytes uint64
	)

	if err := it.rows.Scan(&id, &txID, &fee, &sizeInBytes); err != nil {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		it.err = err

		return nil, it.err
	}

	q2 := `
		SELECT
		 previous_transaction_hash
		,previous_tx_idx
		,previous_tx_satoshis
		,previous_tx_script
		,unlocking_script
		,sequence_number
		FROM inputs
		WHERE transaction_id = $1
		ORDER BY idx
	`

	rows, err := it.store.db.QueryContext(ctx, q2, id)
	if err != nil {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		it.err = err

		return nil, it.err
	}

	defer rows.Close()

	tx := bt.Tx{}

	for rows.Next() {
		input := &bt.Input{}

		var previousTxHashBytes []byte

		if err := rows.Scan(&previousTxHashBytes, &input.PreviousTxOutIndex, &input.PreviousTxSatoshis, &input.PreviousTxScript, &input.UnlockingScript, &input.SequenceNumber); err != nil {
			if err := it.Close(); err != nil {
				it.store.logger.Warnf("failed to close iterator: %v", err)
			}

			return nil, err
		}

		previousTxHash, err := chainhash.NewHash(previousTxHashBytes)
		if err != nil {
			if err := it.Close(); err != nil {
				it.store.logger.Warnf("failed to close iterator: %v", err)
			}

			return nil, err
		}

		if err := input.PreviousTxIDAdd(previousTxHash); err != nil {
			if err := it.Close(); err != nil {
				it.store.logger.Warnf("failed to close iterator: %v", err)
			}

			return nil, err
		}

		tx.Inputs = append(tx.Inputs, input)
	}

	txInpoints, err := meta.NewTxInpointsFromInputs(tx.Inputs)
	if err != nil {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		return nil, errors.NewProcessingError("failed to create tx inpoints from inputs", err)
	}

	return &utxo.UnminedTransaction{
		Hash:       txID,
		Fee:        fee,
		Size:       sizeInBytes,
		TxInpoints: txInpoints,
	}, nil
}

func (it *unminedTxIterator) Err() error {
	return it.err
}

func (it *unminedTxIterator) Close() error {
	it.done = true

	return it.rows.Close()
}

func (s *Store) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	return newUnminedTxIterator(s)
}
