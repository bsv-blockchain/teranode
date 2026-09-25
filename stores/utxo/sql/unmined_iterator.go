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

const errFailedCloseIterator = "failed to close iterator: %v"

// unminedTxIterator implements utxo.UnminedTxIterator for SQL
type unminedTxIterator struct {
	store *Store
	err   error
	done  bool
	rows  *sql.Rows
}

func newUnminedTxIterator(store *Store) (*unminedTxIterator, error) {
	return newUnminedTxIteratorContext(context.Background(), store)
}

func newUnminedTxIteratorContext(ctx context.Context, store *Store) (*unminedTxIterator, error) {
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

	rows, err := store.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}

	it.rows = rows

	return it, nil
}

func (it *unminedTxIterator) Next(ctx context.Context) ([]*utxo.UnminedTransaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
		it.err = it.rows.Err()
		if err := it.Close(); err != nil {
			it.store.logger.Warnf(errFailedCloseIterator, err)
		}

		return nil, it.err
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
			it.store.logger.Warnf(errFailedCloseIterator, err)
		}

		it.err = err

		return nil, it.err
	}

	// Outpoint columns only. Everything this iterator produces from the inputs is
	// subtree.NewTxInpointsFromInputs below, which reads each input's parent hash
	// and vout; the returned UnminedTransaction carries no transaction body. The
	// extended and unlocking columns were being read and discarded once per
	// unmined transaction, on the path block assembly walks at every restart.
	q2 := inputsQuerySQL(inputsQueryOutpoints, "transaction_id = $1")

	rows, err := it.store.db.QueryContext(ctx, q2, id)
	if err != nil {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf(errFailedCloseIterator, err)
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

	// Built once per transaction rather than once per input row: this runs for
	// every input of every unmined transaction at each block assembly restart.
	var scanRow inputScanRow

	scanTargets := scanRow.scanTargets(inputsQueryOutpoints)

	for rows.Next() {
		if err = rows.Scan(scanTargets...); err != nil {
			if err = it.Close(); err != nil {
				it.store.logger.Warnf(errFailedCloseIterator, err)
			}

			return nil, err
		}

		input, inputErr := scanRow.toInput(inputsQueryOutpoints)
		if inputErr != nil {
			if err = it.Close(); err != nil {
				it.store.logger.Warnf(errFailedCloseIterator, err)
			}

			return nil, inputErr
		}

		tx.Inputs = append(tx.Inputs, input)
	}

	// A truncated read here hands block assembly a transaction with fewer parents
	// than it really has, which is a silent wrong answer rather than a failure.
	if err = rows.Err(); err != nil {
		if err2 := it.Close(); err2 != nil {
			it.store.logger.Warnf(errFailedCloseIterator, err2)
		}

		return nil, err
	}

	txInpoints, err := subtree.NewTxInpointsFromInputs(tx.Inputs)
	if err != nil {
		if err = it.Close(); err != nil {
			it.store.logger.Warnf(errFailedCloseIterator, err)
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
			it.store.logger.Warnf(errFailedCloseIterator, err)
		}

		it.err = err

		return nil, it.err
	}

	defer rows2.Close()

	for rows2.Next() {
		var blockId uint32

		if err = rows2.Scan(&blockId); err != nil {
			if err = it.Close(); err != nil {
				it.store.logger.Warnf(errFailedCloseIterator, err)
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

func (s *Store) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	return newUnminedTxIterator(s)
}

// GetUnminedTxIteratorContext binds both query admission and row iteration to
// ctx. Online recovery uses this optional API to bound its serialized scan.
func (s *Store) GetUnminedTxIteratorContext(ctx context.Context) (utxo.UnminedTxIterator, error) {
	iterator, err := newUnminedTxIteratorContext(ctx, s)
	if err != nil {
		return nil, err
	}
	return iterator, nil
}

// ScanInconsistentUnminedTxs is a no-op for SQL — the SQL store always uses
// index-based queries, so there's no fullScan inconsistency to fix.
func (s *Store) ScanInconsistentUnminedTxs() (utxo.ConsistencyScanIterator, error) {
	return nil, nil
}

func (s *Store) GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (utxo.UnminedTxIterator, error) {
	return newPrunableUnminedTxIterator(s, cutoffBlockHeight)
}

func newPrunableUnminedTxIterator(store *Store, cutoffBlockHeight uint32) (*unminedTxIterator, error) {
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
		  AND t.unmined_since <= $1
		  AND t.conflicting = false
		ORDER BY t.id ASC
	`

	rows, err := store.db.Query(q, cutoffBlockHeight)
	if err != nil {
		return nil, err
	}

	it.rows = rows

	return it, nil
}
