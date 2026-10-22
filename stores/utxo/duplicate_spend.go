package utxo

import (
	"bytes"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

type spendOutpointKey struct {
	txID chainhash.Hash
	vout uint32
}

// ProcessSpendBatch submits contiguous parts of items in order. A part never
// contains different spending data for the same outpoint. Identical claims can
// share a part. Every item reaches process exactly once, including invalid items
// whose validation belongs to the backend.
//
// process must finish the backend operation before returning. The backend then
// decides each claim against stored state, so a failed competing claim cannot
// reject an idempotent re-spend or invent a conflict owner. No preliminary read
// is needed. The backend must still enforce ownership atomically against other
// concurrent batches; this function only orders claims within this batch.
//
// Batches without conflicting claims use one call and no copied item slices.
func ProcessSpendBatch[T any](items []T, spendForItem func(T) *Spend, process func([]T)) {
	firstByOutpoint := make(map[spendOutpointKey]*spendpkg.SpendingData, len(items))
	start := 0
	for i, item := range items {
		spend := spendForItem(item)
		if spend == nil || spend.TxID == nil {
			continue
		}
		key := spendOutpointKey{txID: *spend.TxID, vout: spend.Vout}
		if first, found := firstByOutpoint[key]; found && !sameSpendingData(first, spend.SpendingData) {
			process(items[start:i])
			start = i
			clear(firstByOutpoint)
		}
		firstByOutpoint[key] = spend.SpendingData
	}
	if start < len(items) {
		process(items[start:])
	}
}

func sameSpendingData(first, next *spendpkg.SpendingData) bool {
	return bytes.Equal(first.Bytes(), next.Bytes())
}
