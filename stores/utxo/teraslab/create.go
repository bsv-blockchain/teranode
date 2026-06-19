package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// Create stores a new transaction's outputs as UTXOs and returns associated metadata.
func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	options := utxo.CreateOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	item, err := txToCreateItem(tx, blockHeight, options)
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
		// TeraSlab returns ErrTxExists for all duplicates. The shared tests expect
		// ErrSpent when re-creating a tx whose UTXOs have been spent. Disambiguate
		// by checking the existing record's spending state.
		if errors.Is(result.err, errors.ErrTxExists) {
			txHash := tx.TxIDChainHash()
			if options.TxID != nil {
				txHash = options.TxID
			}
			if hasSpentUTXOs(ctx, s, txHash) {
				return nil, errors.ErrSpent
			}
		}
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

// hasSpentUTXOs checks if any UTXO of the given transaction has been spent.
func hasSpentUTXOs(ctx context.Context, s *Store, txHash *chainhash.Hash) bool {
	txid := hashToTxID(txHash)
	records, err := s.client.GetRecordBatch(ctx, teraslab.FieldUtxoSlots, []teraslab.TxID{txid})
	if err != nil || len(records) == 0 || !records[0].Found {
		return false
	}

	for _, slot := range records[0].Slots {
		if slot.Status == teraslab.SlotSpent {
			return true
		}
	}

	return false
}

// Delete removes a UTXO and its associated metadata from the store.
func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	txid := hashToTxID(hash)
	_, err := s.client.DeleteBatch(ctx, []teraslab.TxID{txid})
	if err != nil {
		// Treat not-found as success (idempotent delete)
		if _, ok := err.(*teraslab.NotFoundError); ok {
			return nil
		}
		if pe, ok := err.(*teraslab.PartialError); ok {
			for _, e := range pe.Errors {
				if e.Code == teraslab.ErrCodeTxNotFound {
					continue
				}
				return mapErrorCode(e.Code)
			}
			return nil
		}
		return err
	}
	return nil
}
