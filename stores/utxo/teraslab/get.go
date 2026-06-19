package teraslab

import (
	"context"
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// Get retrieves UTXO metadata for a given transaction hash.
func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, requestedFields ...fields.FieldName) (*meta.Data, error) {
	done := make(chan batchGetResult, 1)
	s.getBatcher.Put(&batchGetItem{
		ctx:       ctx,
		hash:      *hash,
		fieldMask: buildFieldMask(requestedFields),
		done:      done,
	})

	var result batchGetResult
	select {
	case result = <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if result.err != nil {
		return nil, result.err
	}

	return result.data, nil
}

// GetMeta retrieves only transaction metadata without the full transaction data.
// Uses utxo.MetaFields (excludes Tx) for efficiency.
func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	done := make(chan batchGetResult, 1)
	s.getBatcher.Put(&batchGetItem{
		ctx:       ctx,
		hash:      *hash,
		fieldMask: defaultGetMetaMask,
		done:      done,
	})

	var result batchGetResult
	select {
	case result = <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if result.err != nil {
		return result.err
	}

	*data = *result.data
	return nil
}

// GetSpend checks the spend status of a UTXO.
func (s *Store) GetSpend(ctx context.Context, sp *utxo.Spend) (*utxo.SpendResponse, error) {
	txid := hashToTxID(sp.TxID)

	// Use GetRecordBatch with FieldUtxoSlots to get slot hashes for validation.
	// The wire-level GetSpendBatch does not return slot hashes, so we cannot
	// validate that the requested UTXOHash matches the stored hash.
	records, err := s.client.GetRecordBatch(ctx, teraslab.FieldUtxoSlots, []teraslab.TxID{txid})
	if err != nil {
		if _, ok := err.(*teraslab.NotFoundError); ok {
			return nil, errors.ErrTxNotFound
		}
		return nil, err
	}

	if len(records) == 0 || !records[0].Found {
		return nil, errors.ErrTxNotFound
	}

	if int(sp.Vout) >= len(records[0].Slots) {
		return nil, errors.NewUtxoError("vout out of range")
	}

	slot := records[0].Slots[sp.Vout]

	// Validate UTXO hash — if the caller provided a hash and it doesn't match
	// the stored hash, the UTXO doesn't exist (e.g. after reassignment).
	if sp.UTXOHash != nil {
		storedHash := chainhash.Hash(slot.Hash)
		if !storedHash.IsEqual(sp.UTXOHash) {
			return nil, errors.ErrTxNotFound
		}
	}

	resp := &utxo.SpendResponse{}

	switch slot.Status {
	case teraslab.SlotUnspent:
		spendableHeight := binary.LittleEndian.Uint32(slot.SpendingData[0:4])
		if spendableHeight > 0 {
			resp.Status = int(utxo.Status_IMMATURE)
		} else {
			resp.Status = int(utxo.Status_OK)
		}
	case teraslab.SlotSpent:
		resp.Status = int(utxo.Status_SPENT)
		resp.SpendingData = wireToSpendingData(slot.SpendingData)
	case teraslab.SlotFrozen:
		resp.Status = int(utxo.Status_FROZEN)
		resp.SpendingData = spend.NewSpendingData(&subtree.FrozenBytesTxHash, int(sp.Vout))
	default:
		resp.Status = int(utxo.Status_OK)
	}

	return resp, nil
}
