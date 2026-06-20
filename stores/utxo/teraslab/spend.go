package teraslab

import (
	"context"
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
)

// Spend marks UTXOs as spent based on the transaction's inputs.
func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	var flags utxo.IgnoreFlags
	if len(ignoreFlags) > 0 {
		flags = ignoreFlags[0]
	}

	spendingTxID := tx.TxIDChainHash()

	items := make([]teraslab.SpendItem, 0, len(tx.Inputs))
	spendResults := make([]*utxo.Spend, 0, len(tx.Inputs))

	for i, input := range tx.Inputs {
		prevTxID := input.PreviousTxIDChainHash()
		if prevTxID == nil {
			continue
		}

		// Derive the UTXO hash from the (decorated) input. UTXOHashFromInput
		// returns a clear error when PreviousTxScript is nil, so an
		// undecorated input fails loudly here instead of silently hashing
		// over a zero satoshi / nil script and producing the wrong UTXO hash.
		utxoHash, err := util.UTXOHashFromInput(input)
		if err != nil {
			return nil, err
		}

		var sd teraslab.SpendingData
		copy(sd[:32], spendingTxID[:])
		binary.LittleEndian.PutUint32(sd[32:36], uint32(i)) //nolint:gosec

		item := teraslab.SpendItem{
			TxID:         hashToTxID(prevTxID),
			Vout:         input.PreviousTxOutIndex,
			UtxoHash:     hashToUtxoHash(utxoHash),
			SpendingData: sd,
		}

		items = append(items, item)
		spendResults = append(spendResults, &utxo.Spend{
			TxID:         prevTxID,
			Vout:         input.PreviousTxOutIndex,
			UTXOHash:     utxoHash,
			SpendingData: spend.NewSpendingData(spendingTxID, i),
		})
	}

	if len(items) == 0 {
		return spendResults, nil
	}

	params := teraslab.SpendBatchParams{
		IgnoreConflicting:    flags.IgnoreConflicting,
		IgnoreLocked:         flags.IgnoreLocked,
		CurrentBlockHeight:   blockHeight,
		BlockHeightRetention: s.settings.GetUtxoStoreBlockHeightRetention(),
	}

	resp, err := s.client.SpendBatch(ctx, params, items)
	if err != nil {
		if pe, ok := err.(*teraslab.PartialError); ok {
			// Map partial errors back to spend results
			hasError := false
			for _, itemErr := range pe.Errors {
				if int(itemErr.ItemIndex) < len(spendResults) {
					spendErr := mapErrorCode(itemErr.Code)
					spendResults[itemErr.ItemIndex].Err = spendErr

					// For AlreadySpent, extract the conflicting txid from error data
					if itemErr.Code == teraslab.ErrCodeAlreadySpent && len(itemErr.Data) >= 36 {
						conflictingSD := wireToSpendingData(teraslab.SpendingData(itemErr.Data[:36]))
						if conflictingSD != nil {
							spendResults[itemErr.ItemIndex].ConflictingTxID = conflictingSD.TxID
						}
					}
					hasError = true
				}
			}

			// Populate block IDs from successes
			if resp != nil {
				for _, success := range resp.Successes {
					if int(success.ItemIndex) < len(spendResults) {
						spendResults[success.ItemIndex].BlockIDs = success.BlockIDs
					}
				}
			}

			if hasError {
				return spendResults, errors.ErrUtxoError
			}
			return spendResults, nil
		}

		return nil, err
	}

	// Populate block IDs from successes
	if resp != nil {
		for _, success := range resp.Successes {
			if int(success.ItemIndex) < len(spendResults) {
				spendResults[success.ItemIndex].BlockIDs = success.BlockIDs
			}
		}
	}

	return spendResults, nil
}

// Unspend reverses a previous spend operation, marking UTXOs as unspent.
func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	if len(spends) == 0 {
		return nil
	}

	items := make([]teraslab.UnspendItem, 0, len(spends))
	for _, sp := range spends {
		if sp.TxID == nil {
			continue
		}
		items = append(items, teraslab.UnspendItem{
			TxID:     hashToTxID(sp.TxID),
			Vout:     sp.Vout,
			UtxoHash: hashToUtxoHash(sp.UTXOHash),
			// The server's unspend is ownership-checked: it only clears the
			// slot when the supplied spending data matches what is stored
			// (otherwise it is a no-op, per the Lua reference contract).
			// Omitting it made every unspend a silent no-op.
			SpendingData: spendingDataToWire(sp.SpendingData),
		})
	}

	params := teraslab.UnspendBatchParams{
		CurrentBlockHeight:   s.blockHeight.Load(),
		BlockHeightRetention: s.settings.GetUtxoStoreBlockHeightRetention(),
	}

	_, err := s.client.UnspendBatch(ctx, params, items)
	if err != nil {
		if _, ok := err.(*teraslab.PartialError); ok {
			// Surface partial failures to the caller so reorg / restore flows can
			// react. Keep a warn log to preserve the existing operational signal.
			s.logger.Warnf("[TeraSlab] partial error during unspend: %v", err)
		}
		return err
	}

	// If flagAsLocked is true, lock the transactions after unspending
	if len(flagAsLocked) > 0 && flagAsLocked[0] {
		txHashes := make([]chainhash.Hash, 0, len(spends))
		seen := make(map[chainhash.Hash]bool)
		for _, sp := range spends {
			if sp.TxID != nil && !seen[*sp.TxID] {
				txHashes = append(txHashes, *sp.TxID)
				seen[*sp.TxID] = true
			}
		}
		if len(txHashes) > 0 {
			return s.SetLocked(ctx, txHashes, true)
		}
	}

	return nil
}
