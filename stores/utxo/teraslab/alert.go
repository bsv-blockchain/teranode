package teraslab

import (
	"context"

	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// FreezeUTXOs marks UTXOs as frozen, preventing them from being spent.
func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	if len(spends) == 0 {
		return nil
	}

	items := make([]teraslab.FreezeItem, len(spends))
	for i, sp := range spends {
		items[i] = teraslab.FreezeItem{
			TxID:     hashToTxID(sp.TxID),
			Vout:     sp.Vout,
			UtxoHash: hashToUtxoHash(sp.UTXOHash),
		}
	}

	_, err := s.client.FreezeBatch(ctx, items)
	if err != nil {
		if pe, ok := err.(*teraslab.PartialError); ok {
			return partialErrorToError("FreezeUTXOs", pe)
		}
		return err
	}

	return nil
}

// UnFreezeUTXOs removes the frozen status from UTXOs, allowing them to be spent again.
func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	if len(spends) == 0 {
		return nil
	}

	items := make([]teraslab.FreezeItem, len(spends))
	for i, sp := range spends {
		items[i] = teraslab.FreezeItem{
			TxID:     hashToTxID(sp.TxID),
			Vout:     sp.Vout,
			UtxoHash: hashToUtxoHash(sp.UTXOHash),
		}
	}

	_, err := s.client.UnfreezeBatch(ctx, items)
	if err != nil {
		if pe, ok := err.(*teraslab.PartialError); ok {
			return partialErrorToError("UnFreezeUTXOs", pe)
		}
		return err
	}

	return nil
}

// ReAssignUTXO reassigns a UTXO to a new transaction output.
func (s *Store) ReAssignUTXO(ctx context.Context, utxoSpend *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	items := []teraslab.ReassignItem{
		{
			TxID:        hashToTxID(utxoSpend.TxID),
			Vout:        utxoSpend.Vout,
			UtxoHash:    hashToUtxoHash(utxoSpend.UTXOHash),
			NewUtxoHash: hashToUtxoHash(newUtxo.UTXOHash),
		},
	}

	params := teraslab.ReassignBatchParams{
		BlockHeight:    s.blockHeight.Load(),
		SpendableAfter: utxo.ReAssignedUtxoSpendableAfterBlocks,
	}

	_, err := s.client.ReassignBatch(ctx, params, items)
	return err
}
