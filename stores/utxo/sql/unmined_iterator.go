package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model/time"
	"github.com/bsv-blockchain/teranode/stores/utxo"
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
		,t.inserted_at
		,t.locked
		,t.coinbase
		,t.unmined_since
		FROM transactions t
		WHERE t.unmined_since IS NOT NULL
		  AND t.conflicting = false
		ORDER BY t.id ASC
	`

	rows, err := store.db.Query(q)
	if err != nil {
		return nil, err
	}

	it.rows = rows

	return it, nil
}

func (it *unminedTxIterator) Next(ctx context.Context) ([]*utxo.UnminedTransaction, error) {
	if it.done || it.err != nil || it.rows == nil {
		return nil, it.err
	}

	// Read a batch of transactions (up to 16K to match Aerospike iterator batch size)
	const batchSize = 1024
	batch := make([]*utxo.UnminedTransaction, 0, batchSize)

	for i := 0; i < batchSize; i++ {
		tx, err := it.readOne(ctx)
		if err != nil {
			return nil, err
		}
		if tx == nil {
			break
		}
		batch = append(batch, tx)
	}

	if len(batch) == 0 {
		return nil, nil
	}

	return batch, nil
}

func (it *unminedTxIterator) readOne(ctx context.Context) (*utxo.UnminedTransaction, error) {
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
		id           uint64
		txID         *chainhash.Hash
		fee          uint64
		sizeInBytes  uint64
		insertedAt   time.CustomTime
		locked       bool
		isCoinbase   bool
		unminedSince uint32
	)

	if err := it.rows.Scan(&id, &txID, &fee, &sizeInBytes, &insertedAt, &locked, &isCoinbase, &unminedSince); err != nil {
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

	if isCoinbase {
		// skip coinbase transactions
		return &utxo.UnminedTransaction{
			Skip: true,
		}, nil
	}

	tx := bt.Tx{}

	var (
		previousTxHashBytes []byte
		previousTxHash      *chainhash.Hash
	)

	for rows.Next() {
		input := &bt.Input{}

		if err = rows.Scan(&previousTxHashBytes, &input.PreviousTxOutIndex, &input.PreviousTxSatoshis, &input.PreviousTxScript, &input.UnlockingScript, &input.SequenceNumber); err != nil {
			if err = it.Close(); err != nil {
				it.store.logger.Warnf("failed to close iterator: %v", err)
			}

			return nil, err
		}

		previousTxHash, err = chainhash.NewHash(previousTxHashBytes)
		if err != nil {
			if err = it.Close(); err != nil {
				it.store.logger.Warnf("failed to close iterator: %v", err)
			}

			return nil, err
		}

		if err = input.PreviousTxIDAdd(previousTxHash); err != nil {
			if err = it.Close(); err != nil {
				it.store.logger.Warnf("failed to close iterator: %v", err)
			}

			return nil, err
		}

		tx.Inputs = append(tx.Inputs, input)
	}

	txInpoints, err := subtree.NewTxInpointsFromInputs(tx.Inputs)
	if err != nil {
		if err = it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		return nil, errors.NewProcessingError("failed to create tx inpoints from inputs", err)
	}

	blockIds := make([]uint32, 0, 2)

	q3 := `
			SELECT
			    block_id
			FROM block_ids
			WHERE transaction_id = $1
			ORDER BY block_id
		`

	rows2, err := it.store.db.QueryContext(ctx, q3, id)
	if err != nil {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		it.err = err

		return nil, it.err
	}

	defer rows2.Close()

	for rows2.Next() {
		var blockId uint32

		if err = rows2.Scan(&blockId); err != nil {
			if err = it.Close(); err != nil {
				it.store.logger.Warnf("failed to close iterator: %v", err)
			}

			return nil, err
		}

		blockIds = append(blockIds, blockId)
	}

	return &utxo.UnminedTransaction{
		Node: &subtree.Node{
			Hash:        *txID,
			Fee:         fee,
			SizeInBytes: sizeInBytes,
		},
		TxInpoints:   &txInpoints,
		CreatedAt:    int(insertedAt.UnixMilli()),
		Locked:       locked,
		BlockIDs:     blockIds,
		UnminedSince: int(unminedSince),
	}, nil
}

func (it *unminedTxIterator) Err() error {
	return it.err
}

func (it *unminedTxIterator) Close() error {
	it.done = true

	return it.rows.Close()
}

func (s *Store) GetUnminedTxIterator(bool) (utxo.UnminedTxIterator, error) {
	return newUnminedTxIterator(s)
}
