package utxo

import (
	"bytes"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

type spendOutpointKey struct {
	txID chainhash.Hash
	vout uint32
}

// PersistedSpender reports the spending data a backend currently holds for an
// outpoint. ok is true only when the lookup succeeded and the answer is
// authoritative; spendingData is nil for an outpoint known to be unspent.
// A false ok means "unknown", and callers must not read a winner into it.
type PersistedSpender func(txID *chainhash.Hash, vout uint32) (spendingData *spendpkg.SpendingData, ok bool)

// duplicateGroup collects the claims one batch makes on a single outpoint.
type duplicateGroup struct {
	// indexes holds item positions, first-seen first.
	indexes []int
	// distinct holds one claim per distinct spending data, first-seen first.
	distinct []*Spend
}

// FilterConflictingDuplicateSpendClaims removes later same-outpoint spend claims
// with different spending data from a backend batch and rejects them as ErrSpent.
//
// Which of two colliding claims wins is decided by persisted state, not by
// batch position. An outpoint already spent by S0 must keep accepting S0's
// idempotent re-submits even when a double-spend attempt by another tx sits
// earlier in the same batch: batch arrival order carries no consensus meaning,
// and refusing the legitimate spender makes Store.Spend un-spend the inputs that
// re-spend had just successfully re-applied (bitcoin-sv/teranode#4934).
//
// With no persisted spender to consult, the first claim wins. That keeps the
// guarantee this filter was added for (#825): when two claims genuinely
// conflict and nothing is persisted yet, exactly one of them reaches the
// backend, so the backend never arbitrates two writes to one row in a batch.
//
// Claims whose spending data equals the winner's are all kept, so idempotency is
// never narrowed.
func FilterConflictingDuplicateSpendClaims[T any](
	items []T,
	spendForItem func(T) *Spend,
	rejectItem func(T, error),
	persistedSpender PersistedSpender,
) []T {
	groups := make(map[spendOutpointKey]*duplicateGroup, len(items))
	keys := make([]spendOutpointKey, 0, len(items))
	spendOf := make([]*Spend, len(items))

	for i, item := range items {
		spend := spendForItem(item)
		spendOf[i] = spend
		if spend == nil || spend.TxID == nil {
			continue
		}

		key := spendOutpointKey{txID: *spend.TxID, vout: spend.Vout}
		group, found := groups[key]
		if !found {
			group = &duplicateGroup{}
			groups[key] = group
			keys = append(keys, key)
		}
		group.indexes = append(group.indexes, i)

		if !holdsSpendingData(group.distinct, spend.SpendingData) {
			group.distinct = append(group.distinct, spend)
		}
	}

	// Resolve a winner only for outpoints that actually collide, so an ordinary
	// batch never pays for a backend read.
	winners := make(map[spendOutpointKey]*spendpkg.SpendingData, len(keys))
	for _, key := range keys {
		group := groups[key]
		if len(group.distinct) < 2 {
			continue
		}

		// A claim matching what the backend already holds is that spender
		// re-submitting, and outranks whoever happened to arrive first.
		if persistedSpender != nil {
			txID := key.txID
			if persisted, ok := persistedSpender(&txID, key.vout); ok && persisted != nil {
				for _, claim := range group.distinct {
					if sameSpendingData(claim.SpendingData, persisted) {
						winners[key] = claim.SpendingData

						break
					}
				}
			}
		}

		if _, decided := winners[key]; !decided {
			winners[key] = group.distinct[0].SpendingData
		}
	}

	filtered := make([]T, 0, len(items))
	for i, item := range items {
		spend := spendOf[i]
		if spend == nil || spend.TxID == nil {
			filtered = append(filtered, item)
			continue
		}

		key := spendOutpointKey{txID: *spend.TxID, vout: spend.Vout}
		winner, decided := winners[key]
		if !decided || sameSpendingData(spend.SpendingData, winner) {
			filtered = append(filtered, item)
			continue
		}

		rejectItem(item, errors.NewUtxoSpentError(key.txID, key.vout,
			spendHashValue(spend.UTXOHash), winner))
	}

	return filtered
}

// holdsSpendingData reports whether list already carries a claim with sd.
func holdsSpendingData(list []*Spend, sd *spendpkg.SpendingData) bool {
	for _, claim := range list {
		if sameSpendingData(claim.SpendingData, sd) {
			return true
		}
	}

	return false
}

func sameSpendingData(first *spendpkg.SpendingData, next *spendpkg.SpendingData) bool {
	var firstBytes []byte
	if first != nil {
		firstBytes = first.Bytes()
	}

	var nextBytes []byte
	if next != nil {
		nextBytes = next.Bytes()
	}

	return bytes.Equal(firstBytes, nextBytes)
}

func spendHashValue(hash *chainhash.Hash) chainhash.Hash {
	if hash == nil {
		return chainhash.Hash{}
	}

	return *hash
}
