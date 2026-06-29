package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	teraslab "github.com/icellan/teraslab/client/go"
)

// Create stores a new transaction's outputs as UTXOs and returns associated metadata.
func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	options := utxo.CreateOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	item, err := txToCreateItem(tx, blockHeight, uint32(s.settings.ChainCfgParams.CoinbaseMaturity), s.settings.ChainCfgParams.GenesisActivationHeight, options) //nolint:gosec
	if err != nil {
		return nil, err
	}

	done := make(chan batchStoreResult, 1)
	s.storeBatcher.Put(&batchStoreItem{
		ctx:        ctx,
		createItem: item,
		done:       done,
	})

	var result batchStoreResult
	select {
	case result = <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if result.err != nil {
		// TeraSlab returns ErrTxExists for all duplicates; return it verbatim,
		// matching the Aerospike backend (its create recovery path returns
		// ErrTxExists for an already-existing tx regardless of whether the
		// outputs are spent). Do NOT upgrade ErrTxExists→ErrSpent based on the
		// existing record's spend state: Block Assembly's processCoinbaseUtxos
		// re-creates every coinbase during catch-up and tolerates ONLY
		// ErrTxExists. By the time catch-up replays an old block, that coinbase
		// can already be mature and spent by a later (already-validated) block,
		// so upgrading here aborts catch-up with "error processing coinbase
		// utxos -> UTXO_SPENT". No Create caller relies on ErrSpent (only Spend
		// results drive double-spend/conflicting handling).
		return nil, result.err
	}

	// Build the meta.Data response
	data := result.data
	if data == nil {
		data = &meta.Data{}
	}

	data.Tx = tx
	data.Fee = item.Fee
	data.SizeInBytes = item.SizeInBytes
	data.LockTime = tx.LockTime
	data.IsCoinbase = item.IsCoinbase
	data.Conflicting = options.Conflicting
	data.Locked = options.Locked
	data.Frozen = options.Frozen

	if len(options.MinedBlockInfos) > 0 {
		data.BlockIDs = make([]uint32, 0, len(options.MinedBlockInfos))
		data.BlockHeights = make([]uint32, 0, len(options.MinedBlockInfos))
		data.SubtreeIdxs = make([]int, 0, len(options.MinedBlockInfos))
		for _, mbi := range options.MinedBlockInfos {
			data.BlockIDs = append(data.BlockIDs, mbi.BlockID)
			data.BlockHeights = append(data.BlockHeights, mbi.BlockHeight)
			data.SubtreeIdxs = append(data.SubtreeIdxs, mbi.SubtreeIdx)
		}
	}

	return data, nil
}

// Delete removes a UTXO and its associated metadata from the store.
//
// Delete is idempotent: deleting a transaction that does not exist is a
// successful no-op. The server does not always report a clean TX_NOT_FOUND for
// a missing record (it attempts parent-prune before confirming existence and
// can surface a storage error instead), so any delete error is reconciled
// against a positive existence check: the error is suppressed only when the
// record is confirmed absent — a real error on an existing record still
// propagates.
func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	txid := hashToTxID(hash)

	_, err := s.client.DeleteBatch(ctx, []teraslab.TxID{txid})
	if err == nil {
		return nil
	}

	// Clean not-found responses are idempotent successes.
	if _, ok := err.(*teraslab.NotFoundError); ok {
		return nil
	}

	if pe, ok := err.(*teraslab.PartialError); ok {
		mapped := error(nil)
		for _, e := range pe.Errors {
			if e.Code == teraslab.ErrCodeTxNotFound {
				continue
			}
			mapped = mapErrorCode(e.Code)
			break
		}
		if mapped == nil {
			return nil // only not-found items
		}
		// Idempotency guard: suppress the error only if the record is
		// confirmed absent; otherwise surface it.
		if found, checkErr := s.recordExists(ctx, txid); checkErr == nil && !found {
			return nil
		}
		return mapped
	}

	// Non-partial transport/other error: same idempotency guard.
	if found, checkErr := s.recordExists(ctx, txid); checkErr == nil && !found {
		return nil
	}
	return err
}

// recordExists reports whether a record for txid is present. The bool is only
// meaningful when the returned error is nil.
func (s *Store) recordExists(ctx context.Context, txid teraslab.TxID) (bool, error) {
	records, err := s.client.GetRecordBatch(ctx, teraslab.FieldUtxoCount, []teraslab.TxID{txid})
	if err != nil {
		if _, ok := err.(*teraslab.NotFoundError); ok {
			return false, nil
		}
		return false, err
	}
	if len(records) == 0 {
		return false, nil
	}
	return records[0].Found, nil
}
