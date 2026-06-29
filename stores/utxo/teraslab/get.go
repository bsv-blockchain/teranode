package teraslab

import (
	"context"
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	teraslab "github.com/icellan/teraslab/client/go"
)

// Get retrieves UTXO metadata for a given transaction hash.
func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, requestedFields ...fields.FieldName) (*meta.Data, error) {
	done := make(chan batchGetResult, 1)
	s.getBatcher.Put(&batchGetItem{
		ctx:       ctx,
		hash:      *hash,
		fieldMask: buildFieldMask(requestedFields),
		includeTx: true,
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
		includeTx: false,
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
	// GetMeta returns metadata only, not the full transaction body. The metadata
	// fetch pulls cold data (TxInpoints lives there), which also reconstructs
	// data.Tx as a side effect — clear it so GetMeta honors its contract, matching
	// the SQL backend (stores/utxo/sql/sql.go GetMeta). TxInpoints is retained.
	data.Tx = nil
	return nil
}

// GetSpend checks the spend status of a UTXO.
func (s *Store) GetSpend(ctx context.Context, sp *utxo.Spend) (*utxo.SpendResponse, error) {
	txid := hashToTxID(sp.TxID)

	// Use GetRecordBatch with FieldUtxoSlots to get slot hashes for validation.
	// The wire-level GetSpendBatch does not return slot hashes, so we cannot
	// validate that the requested UTXOHash matches the stored hash. FieldFlags is
	// also requested so the tx-level conflicting/locked flags can override the
	// per-slot status (matching the Aerospike backend).
	records, err := s.client.GetRecordBatch(ctx, teraslab.FieldUtxoSlots|teraslab.FieldFlags, []teraslab.TxID{txid})
	if err != nil {
		// A missing record is a status, not an error: the Store contract (and the
		// Aerospike/SQL backends) returns NOT_FOUND with a nil error so callers can
		// distinguish "absent" from a real failure. Only genuine errors propagate.
		if _, ok := err.(*teraslab.NotFoundError); ok {
			return &utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil
		}
		return nil, err
	}

	if len(records) == 0 || !records[0].Found {
		return &utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil
	}

	if int(sp.Vout) >= len(records[0].Slots) {
		return nil, errors.NewUtxoError("vout out of range")
	}

	slot := records[0].Slots[sp.Vout]

	// A stored slot with an all-zero hash is a data output (OP_RETURN): Create
	// stores a zero hash and never computes a real one for these. A genuine
	// SHA-based UTXO hash is never all-zero, so a zero hash uniquely identifies
	// a non-spendable data slot. Report it as absent — matching Aerospike, which
	// returns a NOT_FOUND status with nil error rather than a Go error. This is
	// distinct from a missing record (handled above with ErrTxNotFound).
	if isZeroHash(slot.Hash) {
		return &utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil
	}

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
		// For an unspent slot the server stores the absolute spendable-at block
		// height in SpendingData[0:4] (block_height + spendable_after; 0 means
		// immediately spendable — see teraslab record.rs UtxoSlot docs). The UTXO
		// is only immature while the current chain height has not yet reached that
		// height, matching Aerospike's maturity check (spendableIn > blockHeight).
		spendableHeight := binary.LittleEndian.Uint32(slot.SpendingData[0:4])
		if spendableHeight > s.blockHeight.Load() {
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

	// The conflicting and locked flags are tx-level metadata, not per-slot status.
	// Apply them as overrides after the slot status, matching the Aerospike
	// backend's precedence (conflicting, then locked — locked wins).
	if records[0].Metadata != nil {
		if records[0].Metadata.Flags&0x02 != 0 { // CONFLICTING
			resp.Status = int(utxo.Status_CONFLICTING)
		}
		if records[0].Metadata.Flags&0x04 != 0 { // LOCKED
			resp.Status = int(utxo.Status_LOCKED)
		}
	}

	return resp, nil
}

// isZeroHash reports whether a stored slot hash is all-zero bytes, which the
// store uses to mark a data (OP_RETURN) output. A real SHA-based UTXO hash is
// never all-zero, so this unambiguously distinguishes a data slot.
func isZeroHash(h teraslab.UtxoHash) bool {
	for _, b := range h {
		if b != 0 {
			return false
		}
	}
	return true
}
