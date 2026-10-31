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

// SpendBatchHandler connects ProcessSpendBatch to one backend.
type SpendBatchHandler[T any] struct {
	// Start optionally reserves an item for dispatch immediately before its part
	// runs. An error rejects only that item and is never reused for another caller.
	Start func(T) error
	// Spend returns the claim of an item, or nil for an item the backend must reject.
	Spend func(T) *Spend
	// Process runs one part against the backend and answers every item in it
	// before it returns.
	Process func([]T)
	// Result returns the answer Process gave an item, and false when there is none.
	Result func(T) (error, bool)
	// Reject answers an item that ProcessSpendBatch decides without the backend.
	Reject func(T, error)
	// SameChecks reports whether the backend runs the same checks for two claims
	// on one outpoint, apart from the spending data: the same UTXO hash, block
	// height and ignore flags. If nil, backend results are not reused locally.
	SameChecks func(a, b T) bool
}

// spendFailure is a backend failure reusable for claims with identical checks.
type spendFailure[T any] struct {
	err     error
	decider T
}

// spendOwner is the stored owner and the claim whose answer showed it.
type spendOwner[T any] struct {
	data    *spendpkg.SpendingData
	decider T
}

// ProcessSpendBatch submits items to the backend in order, so the stored owner
// decides competing claims on the same outpoint.
//
// A batch without different claims on one outpoint sends its admitted items
// to Process in one call. An entirely abandoned batch makes no backend call.
// Otherwise a part ends before the first claim that competes with a pending claim
// whose result is unknown. After each part, the answers tell the stored owner:
//   - success names the claim itself;
//   - UTXO_SPENT names the owner in its data, unless that data is the claim's own;
//   - any other error leaves the owner unknown.
//
// A later claim with different data than a known owner is rejected locally with
// UTXO_SPENT naming that owner, but only when SameChecks holds against the claim
// that showed the owner. That claim passed every check before the spent check,
// so the backend would answer the later claim with the same UTXO_SPENT. A claim
// with other checks, or one that matches the owner, still goes to the backend.
// Non-UTXO_SPENT backend failures are also reused for later claims with the
// same checks. This keeps an outage from turning N competing claims into N
// serial backend calls. Different checks still require a backend decision;
// unknown-owner UTXO_SPENT errors are never copied to another spending claim.
// Start is checked at dispatch, including for items queued before a previous
// part blocked. Admission failures belong to one caller and are never reused.
//
// The local answer reflects the stored state when its part finished. It is an
// answer the backend could also give, because the claim was waiting the whole
// time. The backend must still enforce ownership atomically against concurrent
// batches. In particular, Aerospike does not promise an order for batch
// operations on one key (including Lua calls with different heights or flags),
// and PostgreSQL UPDATE ... FROM does not choose predictably between source
// rows matching the same output. Do not rely on observed backend ordering to
// combine competing claims into one part.
func ProcessSpendBatch[T any](items []T, h SpendBatchHandler[T]) {
	if len(items) == 0 {
		return
	}

	process := func(part []T) []T {
		if h.Start != nil {
			var ready []T
			filtered := false
			for i, item := range part {
				if err := h.Start(item); err != nil {
					if !filtered {
						ready = append(make([]T, 0, len(part)), part[:i]...)
						filtered = true
					}
					h.Reject(item, err)
				} else if filtered {
					ready = append(ready, item)
				}
			}
			if filtered {
				part = ready
			}
		}
		if len(part) > 0 {
			h.Process(part)
		}
		return part
	}

	if !hasCompetingClaims(items, h.Spend) {
		process(items)
		return
	}

	owners := make(map[spendOutpointKey]spendOwner[T])
	failures := make(map[spendOutpointKey][]spendFailure[T])
	pending := make(map[spendOutpointKey]*spendpkg.SpendingData)
	var part []T

	flush := func() {
		if len(part) == 0 {
			return
		}

		for _, item := range process(part) {
			learnOwner(owners, item, h)
			spend := h.Spend(item)
			if spend == nil || spend.TxID == nil || spend.SpendingData == nil || spend.SpendingData.TxID == nil {
				continue
			}
			if err, answered := h.Result(item); answered && err != nil && !errors.Is(err, errors.ErrSpent) {
				key := spendOutpointKey{txID: *spend.TxID, vout: spend.Vout}
				failures[key] = append(failures[key], spendFailure[T]{err: err, decider: item})
			}
		}

		part = nil
		clear(pending)
	}

	rejectedByResult := func(item T, spend *Spend, key spendOutpointKey) bool {
		if h.SameChecks != nil && spend.SpendingData != nil && spend.SpendingData.TxID != nil {
			for _, failure := range failures[key] {
				if h.SameChecks(failure.decider, item) {
					h.Reject(item, failure.err)
					return true
				}
			}
		}
		owner, known := owners[key]
		if !known || sameSpendingData(owner.data, spend.SpendingData) || h.SameChecks == nil || !h.SameChecks(owner.decider, item) {
			return false
		}

		h.Reject(item, errors.NewUtxoSpentError(key.txID, key.vout, hashValue(spend.UTXOHash), owner.data))

		return true
	}

	for _, item := range items {
		spend := h.Spend(item)
		if spend == nil || spend.TxID == nil {
			part = append(part, item)
			continue
		}

		key := spendOutpointKey{txID: *spend.TxID, vout: spend.Vout}
		if rejectedByResult(item, spend, key) {
			continue
		}

		if claim, found := pending[key]; found && !sameSpendingData(claim, spend.SpendingData) {
			flush()

			if rejectedByResult(item, spend, key) {
				continue
			}
		}

		pending[key] = spend.SpendingData
		part = append(part, item)
	}

	flush()
}

func hasCompetingClaims[T any](items []T, spendForItem func(T) *Spend) bool {
	claims := make(map[spendOutpointKey]*spendpkg.SpendingData, len(items))

	for _, item := range items {
		spend := spendForItem(item)
		if spend == nil || spend.TxID == nil {
			continue
		}

		key := spendOutpointKey{txID: *spend.TxID, vout: spend.Vout}
		if claim, found := claims[key]; found && !sameSpendingData(claim, spend.SpendingData) {
			return true
		}

		claims[key] = spend.SpendingData
	}

	return false
}

func learnOwner[T any](owners map[spendOutpointKey]spendOwner[T], item T, h SpendBatchHandler[T]) {
	spend := h.Spend(item)
	if spend == nil || spend.TxID == nil || spend.SpendingData == nil {
		return
	}

	err, answered := h.Result(item)
	if !answered {
		return
	}

	key := spendOutpointKey{txID: *spend.TxID, vout: spend.Vout}

	if err == nil {
		owners[key] = spendOwner[T]{data: spend.SpendingData, decider: item}
		return
	}

	var spentData *errors.UtxoSpentErrData
	if errors.Is(err, errors.ErrSpent) && errors.AsData(err, &spentData) &&
		spentData.SpendingData != nil && !sameSpendingData(spentData.SpendingData, spend.SpendingData) {
		owners[key] = spendOwner[T]{data: spentData.SpendingData, decider: item}
	}
}

func sameSpendingData(first, next *spendpkg.SpendingData) bool {
	return bytes.Equal(first.Bytes(), next.Bytes())
}

// SameUTXOHash reports whether two claims carry the same UTXO hash.
func SameUTXOHash(a, b *Spend) bool {
	return hashValue(a.UTXOHash) == hashValue(b.UTXOHash)
}

func hashValue(hash *chainhash.Hash) chainhash.Hash {
	if hash == nil {
		return chainhash.Hash{}
	}

	return *hash
}
