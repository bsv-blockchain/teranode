package teraslab

import (
	"context"
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	teraslab "github.com/icellan/teraslab/client/go"
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

	// Submit the whole transaction to the spend batcher. sendSpendBatch coalesces
	// transactions that share these params into one SpendBatch RPC (one server
	// redo fsync), and routes the per-input results — and any all-or-nothing
	// rollback — back here scoped to this transaction. The per-tx error mapping,
	// conflicting-txid extraction and atomic rollback live in finalizeSpendResults
	// / flushSpendGroup.
	done := make(chan batchSpendResult, 1)
	s.spendBatcher.Put(&batchSpendItem{
		ctx:          ctx,
		items:        items,
		results:      spendResults,
		spendingTxID: spendingTxID,
		params: teraslab.SpendBatchParams{
			IgnoreConflicting:    flags.IgnoreConflicting,
			IgnoreLocked:         flags.IgnoreLocked,
			CurrentBlockHeight:   blockHeight,
			BlockHeightRetention: s.settings.GetUtxoStoreBlockHeightRetention(),
		},
		done: done,
	})

	select {
	case res := <-done:
		return res.spends, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// spendCodeNeedsRollback reports whether a per-item spend error code means the
// transaction is genuinely invalid (so its already-succeeded sibling spends must
// be rolled back) rather than a transient/internal server fault (where the
// successful spends can safely remain for an idempotent retry). Mirrors the
// Aerospike backend's needsSpendRollback.
func spendCodeNeedsRollback(code uint16) bool {
	switch code {
	case teraslab.ErrCodeAlreadySpent,
		teraslab.ErrCodeConflicting,
		teraslab.ErrCodeFrozen,
		teraslab.ErrCodeAlreadyFrozen,
		teraslab.ErrCodeFrozenUntil,
		teraslab.ErrCodeLocked,
		teraslab.ErrCodeUtxoHashMismatch,
		teraslab.ErrCodeInvalidSpend,
		teraslab.ErrCodeCoinbaseImmature,
		teraslab.ErrCodeVoutOutOfRange:
		return true
	default:
		return false
	}
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
		if pe, ok := err.(*teraslab.PartialError); ok {
			// Surface partial failures to the caller so reorg / restore flows can
			// react. Keep a warn log to preserve the existing operational signal,
			// but return the mapped Teranode error so callers' errors.Is checks
			// (e.g. ErrTxNotFound) match instead of the raw client error type.
			s.logger.Warnf("[TeraSlab] partial error during unspend: %v", err)
			return partialErrorToError("Unspend", pe)
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
