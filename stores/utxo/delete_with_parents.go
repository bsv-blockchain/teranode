package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
)

// DeleteWithParents deletes a transaction record and clears every reference to it in surviving
// parent records. It unspends the outputs the transaction spent, deletes the record, and removes
// the transaction from the conflictingChildren list of each parent when it was marked conflicting.
//
// A parent that is already gone is not an error. A transaction that is already gone is not an error.
func DeleteWithParents(ctx context.Context, s Store, txHash *chainhash.Hash) error {
	if err := ClearParentRefs(ctx, s, txHash); err != nil {
		return err
	}

	if err := s.Delete(ctx, txHash); err != nil && !isNotFoundErr(err) {
		return errors.NewStorageError("[DeleteWithParents][%s] failed to delete tx", txHash.String(), err)
	}

	return nil
}

// ClearParentRefs removes every reference surviving parent records hold to a transaction, without
// deleting the transaction. It unspends the outputs the transaction spent and removes the
// transaction from the conflictingChildren list of each parent when it was marked conflicting.
//
// A parent that is already gone is not an error. A transaction that is already gone is not an error.
func ClearParentRefs(ctx context.Context, s Store, txHash *chainhash.Hash) error {
	txMeta, err := s.Get(ctx, txHash, fields.Tx, fields.Conflicting)
	if err != nil {
		if isNotFoundErr(err) {
			return nil
		}

		return errors.NewStorageError("[ClearParentRefs][%s] failed to get tx", txHash.String(), err)
	}

	if txMeta.Tx == nil {
		return errors.NewProcessingError("[ClearParentRefs][%s] store returned no tx, cannot unspend inputs", txHash.String())
	}

	if err = s.PreviousOutputsDecorate(ctx, txMeta.Tx); err != nil {
		return errors.NewStorageError("[ClearParentRefs][%s] failed to decorate previous outputs", txHash.String(), err)
	}

	spends := make([]*Spend, 0, len(txMeta.Tx.Inputs))

	for i, input := range txMeta.Tx.Inputs {
		utxoHash, hashErr := util.UTXOHashFromInput(input)
		if hashErr != nil {
			return errors.NewProcessingError("[ClearParentRefs][%s] failed to hash input %d", txHash.String(), i, hashErr)
		}

		spends = append(spends, &Spend{
			TxID:         input.PreviousTxIDChainHash(),
			Vout:         input.PreviousTxOutIndex,
			UTXOHash:     utxoHash,
			SpendingData: spendpkg.NewSpendingData(txHash, i),
		})
	}

	if err = UnspendTolerantOfMissingParents(ctx, s, spends); err != nil {
		return errors.NewStorageError("[ClearParentRefs][%s] failed to unspend parents", txHash.String(), err)
	}

	if !txMeta.Conflicting {
		return nil
	}

	removals := make([]ConflictingChildRemoval, 0, len(txMeta.Tx.Inputs))
	seen := make(map[chainhash.Hash]struct{}, len(txMeta.Tx.Inputs))

	for _, input := range txMeta.Tx.Inputs {
		parentHash := input.PreviousTxIDChainHash()
		if parentHash == nil {
			continue
		}

		if _, dup := seen[*parentHash]; dup {
			continue
		}

		seen[*parentHash] = struct{}{}
		removals = append(removals, ConflictingChildRemoval{ParentHash: parentHash, ChildHash: txHash})
	}

	if len(removals) == 0 {
		return nil
	}

	if err = s.RemoveFromConflictingChildren(ctx, removals); err != nil {
		return errors.NewStorageError("[ClearParentRefs][%s] failed to remove conflicting child refs", txHash.String(), err)
	}

	return nil
}

// UnspendTolerantOfMissingParents unspends the outputs, skipping any whose parent is gone. A store may
// run one Unspend call as one database transaction, so a missing parent would undo the unspends of the
// surviving parents in the same call. On not found, it repeats one spend at a time.
func UnspendTolerantOfMissingParents(ctx context.Context, s SpendReverser, spends []*Spend) error {
	if len(spends) == 0 {
		return nil
	}

	err := s.Unspend(ctx, spends)
	if err == nil {
		return nil
	}

	if !isNotFoundErr(err) {
		return err
	}

	for _, spend := range spends {
		if err = s.Unspend(ctx, []*Spend{spend}); err != nil && !isNotFoundErr(err) {
			return err
		}
	}

	return nil
}

func isNotFoundErr(err error) bool {
	return errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound)
}
