package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	teraslab "github.com/icellan/teraslab/client/go"

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
func (s *Store) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	for _, tx := range txs {
		if err := s.PreviousOutputsDecorate(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

// PreviousOutputsDecorate fetches information about transaction inputs' previous outputs.
func (s *Store) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	if tx == nil || len(tx.Inputs) == 0 {
		return nil
	}

	items := make([]teraslab.GetSpendItem, 0, len(tx.Inputs))
	for _, input := range tx.Inputs {
		prevTxID := input.PreviousTxIDChainHash()
		if prevTxID == nil {
			continue
		}
		items = append(items, teraslab.GetSpendItem{
			TxID: hashToTxID(prevTxID),
			Vout: input.PreviousTxOutIndex,
		})
	}

	if len(items) == 0 {
		return nil
	}

	_, err := s.client.GetSpendBatch(ctx, items)
	return err
}
