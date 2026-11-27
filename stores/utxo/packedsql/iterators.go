package packedsql

import (
	"bytes"
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

const iteratorBatchSize = 1000

type packedTxIterator struct {
	store    *Store
	where    string
	extraArg *int64
	lastHash []byte
	done     bool
	err      error
}

func (s *Store) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	return &packedTxIterator{
		store: s,
		where: `unmined_since IS NOT NULL AND (flags & 4) = 0`,
	}, nil
}

func (s *Store) GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (utxo.UnminedTxIterator, error) {
	cutoff := int64(cutoffBlockHeight)

	return &packedTxIterator{
		store:    s,
		where:    `unmined_since IS NOT NULL AND (flags & 4) = 0 AND unmined_since <= $2`,
		extraArg: &cutoff,
	}, nil
}

func (s *Store) GetConflictingTxIterator() (utxo.UnminedTxIterator, error) {
	return &packedTxIterator{
		store: s,
		where: `(flags & 4) <> 0`,
	}, nil
}

func (it *packedTxIterator) Next(ctx context.Context) ([]*utxo.UnminedTransaction, error) {
	if it.done || it.err != nil {
		return nil, it.err
	}

	query := `SELECT hash, fee, size_in_bytes, created_at, coalesce(unmined_since, 0), flags, inputs, block_refs
		 FROM packed_txs WHERE hash > $1 AND ` + it.where + ` ORDER BY hash LIMIT 1000`

	args := []any{it.lastHash}
	if it.extraArg != nil {
		args = append(args, *it.extraArg)
	}

	if it.lastHash == nil {
		args[0] = []byte{}
	}

	rows, err := it.store.pool.Query(ctx, query, args...)
	if err != nil {
		it.err = errors.NewStorageError("packedsql: iterator query failed", err)
		return nil, it.err
	}

	defer rows.Close()

	batch := make([]*utxo.UnminedTransaction, 0, iteratorBatchSize)

	for rows.Next() {
		var (
			hb           []byte
			fee          int64
			sizeInBytes  int64
			createdAt    int64
			unminedSince int64
			flags        int16
			inputsBlob   []byte
			blockRefs    []byte
		)

		if err = rows.Scan(&hb, &fee, &sizeInBytes, &createdAt, &unminedSince, &flags, &inputsBlob, &blockRefs); err != nil {
			it.err = errors.NewStorageError("packedsql: iterator scan failed", err)
			return nil, it.err
		}

		it.lastHash = append(it.lastHash[:0], hb...)

		if flags&flagCoinbase != 0 {
			batch = append(batch, &utxo.UnminedTransaction{Skip: true})
			continue
		}

		txInpoints, err := txInpointsFromBlob(inputsBlob)
		if err != nil {
			it.err = err
			return nil, it.err
		}

		blockIDs, _, _ := unpackBlockRefs(blockRefs)

		batch = append(batch, &utxo.UnminedTransaction{
			Node: &subtree.Node{
				Hash:        chainhash.Hash(hb),
				Fee:         uint64(fee),         //nolint:gosec
				SizeInBytes: uint64(sizeInBytes), //nolint:gosec
			},
			TxInpoints:   txInpoints,
			CreatedAt:    int(createdAt),
			Locked:       flags&flagLocked != 0,
			BlockIDs:     blockIDs,
			UnminedSince: int(unminedSince),
		})
	}

	if err = rows.Err(); err != nil {
		it.err = errors.NewStorageError("packedsql: iterator rows failed", err)
		return nil, it.err
	}

	if len(batch) == 0 {
		it.done = true
		return nil, nil
	}

	return batch, nil
}

func (it *packedTxIterator) Err() error {
	return it.err
}

func (it *packedTxIterator) Close() error {
	it.done = true
	return nil
}

func txInpointsFromBlob(inputsBlob []byte) (*subtree.TxInpoints, error) {
	n := offsetBlobCount(inputsBlob)
	inputs := make([]*bt.Input, n)

	for i := 0; i < n; i++ {
		item, err := offsetBlobItem(inputsBlob, i)
		if err != nil {
			return nil, err
		}

		inputs[i] = &bt.Input{}
		if _, err = inputs[i].ReadFromExtended(bytes.NewReader(item)); err != nil {
			return nil, errors.NewTxInvalidError("packedsql: could not read input %d", i, err)
		}
	}

	txInpoints, err := subtree.NewTxInpointsFromInputs(inputs)
	if err != nil {
		return nil, errors.NewProcessingError("packedsql: failed to create tx inpoints from inputs", err)
	}

	return &txInpoints, nil
}

type consistencyScanIterator struct {
	store    *Store
	lastHash []byte
	scanned  int64
	done     bool
	err      error
}

func (s *Store) ScanInconsistentUnminedTxs() (utxo.ConsistencyScanIterator, error) {
	return &consistencyScanIterator{store: s}, nil
}

func (it *consistencyScanIterator) Next(ctx context.Context) ([]*utxo.InconsistentTxRecord, error) {
	if it.done || it.err != nil {
		return nil, it.err
	}

	last := it.lastHash
	if last == nil {
		last = []byte{}
	}

	rows, err := it.store.pool.Query(ctx,
		`SELECT hash, block_refs, coalesce(unmined_since, 0) FROM packed_txs WHERE hash > $1 ORDER BY hash LIMIT 5000`,
		last)
	if err != nil {
		it.err = errors.NewStorageError("packedsql: consistency scan query failed", err)
		return nil, it.err
	}

	defer rows.Close()

	records := make([]*utxo.InconsistentTxRecord, 0, 8)

	rowCount := 0

	for rows.Next() {
		var (
			hb           []byte
			blockRefs    []byte
			unminedSince int64
		)

		if err = rows.Scan(&hb, &blockRefs, &unminedSince); err != nil {
			it.err = errors.NewStorageError("packedsql: consistency scan failed", err)
			return nil, it.err
		}

		rowCount++
		it.scanned++
		it.lastHash = append(it.lastHash[:0], hb...)

		if unminedSince != 0 && len(blockRefs) > 0 {
			blockIDs, _, _ := unpackBlockRefs(blockRefs)
			records = append(records, &utxo.InconsistentTxRecord{
				Hash:         chainhash.Hash(hb),
				BlockIDs:     blockIDs,
				UnminedSince: int(unminedSince),
			})
		}
	}

	if err = rows.Err(); err != nil {
		it.err = errors.NewStorageError("packedsql: consistency scan rows failed", err)
		return nil, it.err
	}

	if rowCount == 0 {
		it.done = true
		return nil, nil
	}

	return records, nil
}

func (it *consistencyScanIterator) TotalScanned() int64 {
	return it.scanned
}

func (it *consistencyScanIterator) Err() error {
	return it.err
}

func (it *consistencyScanIterator) Close() error {
	it.done = true
	return nil
}
