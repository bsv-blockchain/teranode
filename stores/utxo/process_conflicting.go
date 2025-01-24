// //go:build aerospike

package utxo

import (
	"context"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

// ProcessConflicting is a method to process conflicting transactions
// We got a txp (parent), txa and txb. txa and txb are both spending txp[5].
//
// tx_original is in block 102a
// - tx_original.input[0] = tx_parent1[5]
// - tx_original.input[1] = tx_parent2[6]
//
// tx_original_child1 is in block 102a
// - tx_original_child1.input[0] = tx_parent4[0]
//
// tx_double_spend is in block 102b --> block 103b --> block 104b
// - tx_double_spend.input[0] = tx_parent1[5] - spent by tx_original
// - tx_double_spend.input[1] = tx_parent3[1] - unspent
//
// ReplaceSpend(txb, []chainhash.Hash{txa})
//
// What happens with tx_parent2[6]?
//
/*
5 phase commit
 - 1: mark tx_original and all it's children as conflicting
 - 2: un-spend tx_original (update tx_parent1 & tx_parent2 utxos) and all it's children (tx_parent4 from tx_original_child1),
      marking all unspent txs as not spendable (tx_parent1 & tx_parent2 & tx_parent4)
 - 3: spend tx_double_spend as normal (ignoring the not spendable flag)
 - 4: mark tx_double_spend as not conflicting
 - 5: mark tx_parent1 & tx_parent2 & tx_parent4 as spendable again
*/
func ProcessConflicting(ctx context.Context, s Store, conflictingTxHashes []chainhash.Hash) (err error) {
	// 0. Get the transactions, check they are conflicting
	winningTxs := make([]*bt.Tx, len(conflictingTxHashes))

	// losingTxHashesPerConflictingTx is a slice of slices, each slice contains the hashes of the transactions that are conflicting
	// with the winning transaction at the same index in the winningTxs slice
	losingTxHashesPerConflictingTx := make([][]chainhash.Hash, len(conflictingTxHashes))

	g, gCtx := errgroup.WithContext(ctx)

	for idx, txHash := range conflictingTxHashes {
		idx := idx
		txHash := txHash

		g.Go(func() error {
			txMeta, err := s.Get(gCtx, &txHash, []string{"tx", "blockIDs", "conflicting"})
			if err != nil {
				return err
			}

			// the transaction should be marked as conflicting, otherwise it shouldn't be in this process
			if !txMeta.Conflicting {
				return errors.NewError("tx is not conflicting")
			}

			// get the counter conflicting transactions for the current transaction
			if losingTxHashesPerConflictingTx[idx], err = getLosingTxHashes(gCtx, s, txMeta.Tx); err != nil {
				return err
			}

			// if there are no conflicting transactions, return an error
			if len(losingTxHashesPerConflictingTx[idx]) == 0 {
				return errors.NewError("no conflicting spend found")
			}

			winningTxs[idx] = txMeta.Tx

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	// create a unique list of all the losing tx hashes
	losingTxHashesMap := make(map[chainhash.Hash]struct{})

	for _, hashes := range losingTxHashesPerConflictingTx {
		for _, hash := range hashes {
			losingTxHashesMap[hash] = struct{}{}
		}
	}

	losingTxHashes := make([]chainhash.Hash, 0, len(losingTxHashesMap))
	for hash := range losingTxHashesMap {
		losingTxHashes = append(losingTxHashes, hash)
	}

	// - 1: mark all losingTxHashesPerConflictingTx as conflicting + all its spending transactions recursively
	affectedParentSpends, err := markConflictingRecursively(ctx, s, losingTxHashes)
	if err != nil {
		return err
	}

	// - 2: un-spend txa, marking the input txs as not spendable (txp & txq)
	err = s.Unspend(ctx, affectedParentSpends, true)
	if err != nil {
		return errors.NewTxUnspendableError("error unspending affected parent spends", err)
	}

	// get the unique hashes of the transactions that were marked as not spendable
	markedAsNotSpendableHashesUnique := make(map[chainhash.Hash]struct{})
	for _, spend := range affectedParentSpends {
		markedAsNotSpendableHashesUnique[*spend.TxID] = struct{}{}
	}

	markedAsNotSpendableHashes := make([]chainhash.Hash, 0, len(markedAsNotSpendableHashesUnique))
	for hash := range markedAsNotSpendableHashesUnique {
		markedAsNotSpendableHashes = append(markedAsNotSpendableHashes, hash)
	}

	// - 3: spend tx_double_spend as normal (ignoring the not spendable flag)
	var tErr *errors.Error

	for _, tx := range winningTxs {
		spends, err := s.Spend(ctx, tx, true)
		if err != nil {
			if errors.As(err, &tErr) {
				// add all the spend errors to the error chain
				for _, spend := range spends {
					if spend.Err != nil {
						tErr.SetWrappedErr(spend.Err)
					}
				}
			}

			return err
		}
	}

	// - 4: mark txb as not conflicting
	if _, _, err = s.SetConflicting(ctx, conflictingTxHashes, false); err != nil {
		return err
	}

	// - 5: mark txp & txq as spendable again
	if err = s.SetUnspendable(ctx, markedAsNotSpendableHashes, false); err != nil {
		return err
	}

	return nil
}

func markConflictingRecursively(ctx context.Context, s Store, hashes []chainhash.Hash) ([]*Spend, error) {
	// mark tx as conflicting
	affectedParentSpends, spendingChildTxs, err := s.SetConflicting(ctx, hashes, true)
	if err != nil {
		return nil, err
	}

	if len(spendingChildTxs) > 0 {
		childSpends, err := markConflictingRecursively(ctx, s, spendingChildTxs)
		if err != nil {
			return nil, err
		}

		affectedParentSpends = append(affectedParentSpends, childSpends...)
	}

	return affectedParentSpends, nil
}

func getLosingTxHashes(gCtx context.Context, s Store, tx *bt.Tx) ([]chainhash.Hash, error) {
	// 1. try to spend and get back the counter conflicting transactions
	// this is a shortcut to get the conflicting transactions without having to
	// iterate over all the inputs of the transaction and check if they are spent
	// TODO refactor into a function that just gets the conflicting tx hashes
	spends, err := s.Spend(gCtx, tx)
	if err != nil {
		if !errors.Is(err, errors.ErrTxInvalid) {
			return nil, err
		}
	}

	// create a slice for the conflicting tx hashes if it doesn't exist
	losingTxHashes := make([]chainhash.Hash, 0, len(tx.Inputs))

	for _, spend := range spends {
		if spend.ConflictingTxID == nil {
			continue
		}

		// append the conflicting tx hash to the slice
		losingTxHashes = append(losingTxHashes, *spend.ConflictingTxID)
	}

	// get the losing txs themselves

	return losingTxHashes, nil
}
