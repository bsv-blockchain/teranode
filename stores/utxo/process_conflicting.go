// //go:build aerospike

// Package utxo provides UTXO (Unspent Transaction Output) management for the Bitcoin SV Teranode implementation.
//
// This file implements conflicting transaction processing functionality for handling double-spend scenarios
// and transaction conflicts in the UTXO store. It requires the aerospike build tag.
package utxo

import (
	"context"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/tracing"
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
func ProcessConflicting(ctx context.Context, s Store, blockHeight uint32, conflictingTxHashes []chainhash.Hash,
	processedConflictingHashesMap map[chainhash.Hash]bool) (losingTxHashesMap txmap.TxMap, err error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "ProcessConflicting")

	defer deferFn()

	// 0. Get the transactions, check they are conflicting
	winningTxs := make([]*bt.Tx, len(conflictingTxHashes))

	// losingTxHashesPerConflictingTx is a slice of slices, each slice contains the hashes of the transactions that are conflicting
	// with the winning transaction at the same index in the winningTxs slice
	losingTxHashesPerConflictingTx := make([][]chainhash.Hash, len(conflictingTxHashes))
	losingTxHashesPerConflictingTxCount := atomic.Int64{}

	g, gCtx := errgroup.WithContext(ctx)

	for idx, txHash := range conflictingTxHashes {
		idx := idx
		txHash := txHash

		if txHash.Equal(subtree.CoinbasePlaceholderHashValue) {
			// the counter-conflicting tx is frozen, we should not process anything further
			return nil, errors.NewProcessingError("[ProcessConflicting][%s] tx is frozen", txHash.String())
		}

		g.Go(func() error {
			txMeta, err := s.Get(gCtx, &txHash, fields.Tx, fields.BlockIDs, fields.Conflicting)
			if err != nil {
				return errors.NewProcessingError("[ProcessConflicting][%s] error getting tx", txHash.String(), err)
			}

			// the transaction should be marked as conflicting, otherwise it shouldn't be in this process
			// unless it was already processed in this run, then it will be in the processedConflictingHashesMap.
			// This can occur when a transaction is in multiple forks, and we are moving back from one fork to another
			// and the transaction was already processed in the previous fork.
			if !txMeta.Conflicting && !processedConflictingHashesMap[txHash] {
				return errors.NewProcessingError("[ProcessConflicting][%s] tx is not conflicting", txHash.String())
			}

			// get the counter conflicting transactions for the current transaction
			// this includes all the children of the conflicting transaction
			if losingTxHashesPerConflictingTx[idx], err = s.GetCounterConflicting(gCtx, txHash); err != nil {
				return errors.NewProcessingError("[ProcessConflicting][%s] error getting counter conflicting txs", txHash.String(), err)
			}

			winningTxs[idx] = txMeta.Tx

			losingTxHashesPerConflictingTxCount.Add(int64(len(losingTxHashesPerConflictingTx[idx])))

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, err
	}

	// create a unique list of all the losing tx hashes
	losingTxHashesMap = txmap.NewSplitSwissMap(int(losingTxHashesPerConflictingTxCount.Load()))

	for _, hashes := range losingTxHashesPerConflictingTx {
		for _, hash := range hashes {
			// an error will be returned if the hash already exists in the map
			// we don't really care, we just need the unique hashes
			_ = losingTxHashesMap.Put(hash, 1)
		}
	}

	losingTxHashes := losingTxHashesMap.Keys()

	// - 1: mark all losingTxHashesPerConflictingTx as conflicting + all its spending transactions recursively
	affectedParentSpends, err := markConflictingRecursively(ctx, s, losingTxHashes)
	if err != nil {
		return nil, err
	}

	// - 2: un-spend txa, marking the input txs as not spendable (txp & txq)
	if err = s.Unspend(ctx, affectedParentSpends, true); err != nil {
		return nil, errors.NewTxLockedError("error unspending affected parent spends", err)
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
		spends, err := s.Spend(ctx, tx, blockHeight, IgnoreFlags{
			IgnoreConflicting: true,
			IgnoreLocked:      true,
		})
		if err != nil {
			if errors.As(err, &tErr) {
				// add all the spend errors to the error chain
				for _, spend := range spends {
					if spend.Err != nil {
						tErr.SetWrappedErr(spend.Err)
					}
				}
			}

			return nil, err
		}
	}

	// - 4: mark txb as not conflicting
	if _, _, err = s.SetConflicting(ctx, conflictingTxHashes, false); err != nil {
		return nil, err
	}

	// - 5: mark txp & txq as spendable again
	if err = s.SetLocked(ctx, markedAsNotSpendableHashes, false); err != nil {
		return nil, err
	}

	return losingTxHashesMap, nil
}

// markConflictingRecursively marks the given transactions as conflicting, and recursively marks all their spending
// children as conflicting too.
//
// Parameters:
//   - ctx: The context for managing request-scoped values, cancellation signals, and deadlines.
//   - s: The UTXO store interface used to interact with the underlying data store.
//   - hashes: A slice of transaction hashes to be marked as conflicting.
//
// Returns:
//   - A slice of pointers to Spend structs representing the affected parent spends.
//   - An error if any issues occur during the process.
func markConflictingRecursively(ctx context.Context, s Store, hashes []chainhash.Hash) ([]*Spend, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "markConflictingRecursively")

	defer deferFn()

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

func GetAndLockChildren(ctx context.Context, s Store, hash chainhash.Hash) ([]chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "GetAndLockChildren")

	defer deferFn()

	if hash.Equal(subtree.CoinbasePlaceholderHashValue) {
		// skip the coinbase placeholder hash
		return nil, errors.NewProcessingError("[GetAndLockChildren][%s] tx is frozen", hash.String())
	}

	// lock the transaction so it can't be modified while we are processing it
	if err := s.SetLocked(ctx, []chainhash.Hash{hash}, true); err != nil {
		return nil, err
	}

	// get the transaction utxos, which will include the spends
	txMeta, err := s.Get(ctx, &hash, fields.Utxos)
	if err != nil {
		return nil, err
	}

	childrenMap := make(map[chainhash.Hash]struct{})

	// set the conflicting children from the utxos bin spends
	if txMeta.SpendingDatas != nil {
		for _, spendingData := range txMeta.SpendingDatas {
			if spendingData != nil {
				childrenMap[*spendingData.TxID] = struct{}{}
			}
		}
	}

	// get and lock the children of the spends
	for child := range childrenMap {
		children, err := GetAndLockChildren(ctx, s, child)
		if err != nil {
			return nil, err
		}

		for _, c := range children {
			childrenMap[c] = struct{}{}
		}
	}

	children := make([]chainhash.Hash, 0, len(childrenMap))

	for child := range childrenMap {
		children = append(children, child)
	}

	return children, nil
}

func GetConflictingChildren(ctx context.Context, s Store, hash chainhash.Hash) ([]chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "GetConflictingChildren")

	defer deferFn()

	if hash.Equal(subtree.CoinbasePlaceholderHashValue) {
		// skip the coinbase placeholder hash
		return nil, nil
	}

	txMeta, err := s.Get(ctx, &hash, fields.Utxos, fields.ConflictingChildren)
	if err != nil {
		return nil, err
	}

	conflictingChildrenMap := make(map[chainhash.Hash]struct{})

	// set the conflicting children from the conflictingChildren bin
	if txMeta.ConflictingChildren != nil {
		for _, child := range txMeta.ConflictingChildren {
			conflictingChildrenMap[child] = struct{}{}
		}
	}

	// set the conflicting children from the utxos bin spends
	if txMeta.SpendingDatas != nil {
		for _, spendingData := range txMeta.SpendingDatas {
			if spendingData != nil {
				conflictingChildrenMap[*spendingData.TxID] = struct{}{}
			}
		}
	}

	// get the conflicting children of the conflicting children
	for conflictingChild := range conflictingChildrenMap {
		conflictingChildren, err := s.GetConflictingChildren(ctx, conflictingChild)
		if err != nil {
			return nil, err
		}

		for _, child := range conflictingChildren {
			conflictingChildrenMap[child] = struct{}{}
		}
	}

	conflictingChildren := make([]chainhash.Hash, 0, len(conflictingChildrenMap))

	for child := range conflictingChildrenMap {
		conflictingChildren = append(conflictingChildren, child)
	}

	return conflictingChildren, nil
}

func GetCounterConflictingTxHashes(ctx context.Context, s Store, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "GetCounterConflictingTxHashes")

	defer deferFn()

	txMeta, err := s.Get(ctx, &txHash, fields.Tx)
	if err != nil {
		return nil, err
	}

	counterConflictingMap := make(map[chainhash.Hash]struct{})
	counterConflictingMap[txHash] = struct{}{}

	// get the unique parent txs
	parentTxs := make(map[chainhash.Hash][]*chainhash.Hash)

	for _, input := range txMeta.Tx.Inputs {
		// get the parent tx
		parentTxs[*input.PreviousTxIDChainHash()] = nil
	}

	for parentTx := range parentTxs {
		parentTxHash := &parentTx

		parentTxMeta, err := s.Get(ctx, parentTxHash, fields.Utxos)
		if err != nil {
			return nil, err
		}

		spendingTxIDs := make([]*chainhash.Hash, len(parentTxMeta.SpendingDatas))

		for idx, spendingData := range parentTxMeta.SpendingDatas {
			if spendingData == nil {
				continue
			}

			spendingTxIDs[idx] = spendingData.TxID
		}

		parentTxs[*parentTxHash] = spendingTxIDs
	}

	for _, input := range txMeta.Tx.Inputs {
		parenTxIDS, ok := parentTxs[*input.PreviousTxIDChainHash()]
		if ok {
			// check the length of the spending txs, if it's less than the index, then the input is not spent
			if len(parenTxIDS) <= int(input.PreviousTxOutIndex) {
				// throw an error
				return nil, errors.NewProcessingError("[GetCounterConflictingTxHashes][%s] cannot process counter conflicting, input %d of %s is out of range (len: %d, %v)", txHash.String(), input.PreviousTxOutIndex, input.PreviousTxIDChainHash().String(), len(parenTxIDS), parenTxIDS)
			}

			spendingTxID := parenTxIDS[input.PreviousTxOutIndex]
			if spendingTxID != nil {
				counterConflictingMap[*spendingTxID] = struct{}{}

				childHashes, err := s.GetConflictingChildren(ctx, *spendingTxID)
				if err != nil {
					return nil, err
				}

				for _, childHash := range childHashes {
					if childHash.Equal(subtree.FrozenBytesTxHash) {
						return nil, errors.NewProcessingError("[GetCounterConflictingTxHashes][%s] tx has frozen child", spendingTxID.String())
					}

					counterConflictingMap[childHash] = struct{}{}
				}
			}
		}
	}

	counterConflicting := make([]chainhash.Hash, 0, len(counterConflictingMap))

	for child := range counterConflictingMap {
		counterConflicting = append(counterConflicting, child)
	}

	// fmt.Printf("counterConflicting: %v\n", counterConflicting)

	return counterConflicting, nil
}
