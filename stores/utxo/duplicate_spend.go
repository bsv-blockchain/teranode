package utxo

import (
	"bytes"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

type spendOutpointKey struct {
	txID chainhash.Hash
	vout uint32
}

// FilterConflictingDuplicateSpendClaims removes later same-outpoint spend claims
// with different spending data from a backend batch and rejects them as ErrSpent.
func FilterConflictingDuplicateSpendClaims[T any](
	items []T,
	spendForItem func(T) *Spend,
	rejectItem func(T, error),
) []T {
	firstByOutpoint := make(map[spendOutpointKey]*Spend, len(items))
	filtered := make([]T, 0, len(items))

	for _, item := range items {
		spend := spendForItem(item)
		if spend == nil || spend.TxID == nil {
			filtered = append(filtered, item)
			continue
		}

		key := spendOutpointKey{
			txID: *spend.TxID,
			vout: spend.Vout,
		}
		first, ok := firstByOutpoint[key]
		if !ok {
			firstByOutpoint[key] = spend
			filtered = append(filtered, item)
			continue
		}

		if sameSpendingData(first, spend) {
			filtered = append(filtered, item)
			continue
		}

		rejectItem(item, errors.NewUtxoSpentError(key.txID, key.vout, spendHashValue(spend.UTXOHash), first.SpendingData))
	}

	return filtered
}

func sameSpendingData(first *Spend, next *Spend) bool {
	var firstBytes []byte
	if first != nil && first.SpendingData != nil {
		firstBytes = first.SpendingData.Bytes()
	}

	var nextBytes []byte
	if next != nil && next.SpendingData != nil {
		nextBytes = next.SpendingData.Bytes()
	}

	return bytes.Equal(firstBytes, nextBytes)
}

func spendHashValue(hash *chainhash.Hash) chainhash.Hash {
	if hash == nil {
		return chainhash.Hash{}
	}
	return *hash
}
