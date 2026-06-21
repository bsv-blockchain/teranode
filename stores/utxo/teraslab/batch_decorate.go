package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// BatchDecorate efficiently fetches metadata for multiple transactions.
func (s *Store) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, requestedFields ...fields.FieldName) error {
	if len(unresolvedMetaDataSlice) == 0 {
		return nil
	}

	txids := make([]teraslab.TxID, len(unresolvedMetaDataSlice))
	for i, umd := range unresolvedMetaDataSlice {
		txids[i] = hashToTxID(&umd.Hash)
	}

	records, err := s.client.GetRecordBatch(ctx, buildFieldMask(requestedFields), txids)
	if err != nil {
		return err
	}

	for i, umd := range unresolvedMetaDataSlice {
		if i >= len(records) || !records[i].Found {
			// Mark not-found items so callers can distinguish "not found" from
			// "not processed" — mirrors the Aerospike backend, which sets a
			// TxNotFound error on the item when the key is missing.
			umd.Data = nil
			umd.Err = errors.NewTxNotFoundError("%s not found", umd.Hash.String())
			continue
		}

		data, err := recordToMetaData(records[i])
		if err != nil {
			umd.Err = err
			continue
		}

		umd.Data = data
	}

	return nil
}

// BatchPreviousOutputsDecorate fetches previous output information for inputs across
// multiple transactions in bulk.
//
// It collects the distinct, still-undecorated parent txids across ALL txs in one
// pass and resolves them with a SINGLE GetRecordBatch (instead of one batch per
// tx), then decorates every input across all txs from that shared result. The
// per-input decode/decoration logic mirrors PreviousOutputsDecorate exactly.
func (s *Store) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	if len(txs) == 0 {
		return nil
	}

	// Distinct parent txids for inputs that still need decoration, across all txs.
	seen := make(map[teraslab.TxID]bool)
	txids := make([]teraslab.TxID, 0)

	for _, tx := range txs {
		if tx == nil {
			continue
		}
		for _, input := range tx.Inputs {
			if input.PreviousTxScript != nil {
				continue // already decorated
			}

			prevTxID := input.PreviousTxIDChainHash()
			if prevTxID == nil {
				continue
			}

			tid := hashToTxID(prevTxID)
			if !seen[tid] {
				seen[tid] = true
				txids = append(txids, tid)
			}
		}
	}

	if len(txids) == 0 {
		return nil
	}

	// Cold data carries the parents' outputs; version/locktime round out the tx.
	mask := teraslab.FieldColdData | teraslab.FieldTxVersion | teraslab.FieldLocktime
	records, err := s.client.GetRecordBatch(ctx, mask, txids)
	if err != nil {
		return err
	}

	prevTxs := make(map[teraslab.TxID]*bt.Tx, len(txids))
	for i, tid := range txids {
		if i >= len(records) || !records[i].Found {
			continue
		}

		data, err := recordToMetaData(records[i])
		if err != nil {
			return err
		}

		if data != nil && data.Tx != nil {
			prevTxs[tid] = data.Tx
		}
	}

	for _, tx := range txs {
		if tx == nil {
			continue
		}
		for _, input := range tx.Inputs {
			if input.PreviousTxScript != nil {
				continue
			}

			prevTxID := input.PreviousTxIDChainHash()
			if prevTxID == nil {
				continue
			}

			prevTx, ok := prevTxs[hashToTxID(prevTxID)]
			if !ok {
				return errors.NewTxNotFoundError("previous tx not found: %s", prevTxID.String())
			}

			vout := input.PreviousTxOutIndex
			if int(vout) >= len(prevTx.Outputs) || prevTx.Outputs[vout] == nil {
				return errors.NewTxInvalidError("previous tx %s has no output at index %d", prevTxID.String(), vout)
			}

			input.PreviousTxSatoshis = prevTx.Outputs[vout].Satoshis
			input.PreviousTxScript = prevTx.Outputs[vout].LockingScript
		}
	}

	return nil
}

// PreviousOutputsDecorate populates each input's PreviousTxSatoshis and
// PreviousTxScript from the referenced parent transaction's stored outputs.
//
// Inputs that are already decorated (PreviousTxScript != nil) are skipped (per
// the interface contract). The distinct parent txids are fetched in one
// GetRecordBatch with cold data (which carries the parents' outputs), then each
// input is filled from parent.Outputs[vout] — mirroring the Aerospike backend.
//
// (The previous implementation called GetSpendBatch and discarded the response;
// it never decorated anything, and GetSpendBatch returns spend status, not the
// output script/satoshis, so it could not have.)
func (s *Store) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	if tx == nil || len(tx.Inputs) == 0 {
		return nil
	}

	// Distinct parent txids for inputs that still need decoration.
	seen := make(map[teraslab.TxID]bool, len(tx.Inputs))
	txids := make([]teraslab.TxID, 0, len(tx.Inputs))

	for _, input := range tx.Inputs {
		if input.PreviousTxScript != nil {
			continue // already decorated
		}

		prevTxID := input.PreviousTxIDChainHash()
		if prevTxID == nil {
			continue
		}

		tid := hashToTxID(prevTxID)
		if !seen[tid] {
			seen[tid] = true
			txids = append(txids, tid)
		}
	}

	if len(txids) == 0 {
		return nil
	}

	// Cold data carries the parents' outputs; version/locktime round out the tx.
	mask := teraslab.FieldColdData | teraslab.FieldTxVersion | teraslab.FieldLocktime
	records, err := s.client.GetRecordBatch(ctx, mask, txids)
	if err != nil {
		return err
	}

	prevTxs := make(map[teraslab.TxID]*bt.Tx, len(txids))
	for i, tid := range txids {
		if i >= len(records) || !records[i].Found {
			continue
		}

		data, err := recordToMetaData(records[i])
		if err != nil {
			return err
		}

		if data != nil && data.Tx != nil {
			prevTxs[tid] = data.Tx
		}
	}

	for _, input := range tx.Inputs {
		if input.PreviousTxScript != nil {
			continue
		}

		prevTxID := input.PreviousTxIDChainHash()
		if prevTxID == nil {
			continue
		}

		prevTx, ok := prevTxs[hashToTxID(prevTxID)]
		if !ok {
			return errors.NewTxNotFoundError("previous tx not found: %s", prevTxID.String())
		}

		vout := input.PreviousTxOutIndex
		if int(vout) >= len(prevTx.Outputs) || prevTx.Outputs[vout] == nil {
			return errors.NewTxInvalidError("previous tx %s has no output at index %d", prevTxID.String(), vout)
		}

		input.PreviousTxSatoshis = prevTx.Outputs[vout].Satoshis
		input.PreviousTxScript = prevTx.Outputs[vout].LockingScript
	}

	return nil
}
