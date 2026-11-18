package utxo

import (
	"github.com/bsv-blockchain/teranode/errors"
)

// IsPrunedReplayRejection reports whether a spend-phase error carries the
// pruner's marker rejection, ErrUtxoSpendingTxPruned, which names the spending
// transaction as a replay of one the pruner removed. Its one production caller
// is the SQL store's bulk spend, which gives a duplicate input the same answer
// as its refused representative.
func IsPrunedReplayRejection(err error) bool {
	return errors.Is(err, errors.ErrUtxoSpendingTxPruned)
}

// RollbackSet decides which of a failed Spend's successful inputs must be
// reversed: everything this call actually wrote, plus, in most cases, the
// inputs whose output already recorded exactly this spend.
//
// An idempotent match is ambiguous on the record. "This output already records
// this spender" is byte-identical whether the spend was committed by a mined
// transaction long ago or by an earlier failed attempt at this very call, so
// the record cannot tell the two apart and the answer has to come from why the
// call is failing now.
//
// One rejection makes the match historical: the marker answer a pruned replay
// gets (IsReplayAnswer). The marker fires only for a transaction this store
// pruned. A mined transaction is pruned once it is fully spent and buried, so
// an output recording it as spender is recording a confirmed spend, and
// reversing it would hand a confirmed output to the next spender, which is the
// double-spend this exclusion was added to prevent. The pruner also deletes
// conflicting records that were never mined, and one whose parent claims were
// never released is marked like a mined one. Its match is not a confirmed
// spend, but it is held back all the same, so the output stays spent by a
// transaction that never confirmed.
//
// Every other rejection leaves the match reversible, and reversing it is what
// keeps the store self-healing. The store leaves partial spends committed when
// a call fails on a transient error, so an attempt that wrote one input and
// then failed on another leaves that input spent by a transaction it never
// created. On the next attempt that input reads as an idempotent match. Holding
// it back unconditionally made the orphan permanent: the output stayed spent by
// a transaction the store does not hold, and its next legitimate spender was
// refused with a txid this node has never seen.
//
// historical is the caller's answer to "might this call have been rejected as a
// pruned replay?". It must be true when any input was, and also whenever
// the caller does not know every input's answer, as on a spend call aborted
// while some inputs were still in flight: an input not yet answered may be the
// marker hit, and reading an in-flight slot is a data race besides. Holding an
// idempotent match back fails towards an output left spent by a transaction the
// store does not hold, which the next attempt heals; reversing it wrongly hands
// a confirmed output to anyone.
func RollbackSet(written, idempotent []*Spend, historical bool) []*Spend {
	if len(idempotent) == 0 || historical {
		return written
	}

	// A fresh slice, not append(written, ...): with spare capacity, append would
	// write the idempotent matches into the caller's backing array.
	rollback := make([]*Spend, 0, len(written)+len(idempotent))
	rollback = append(rollback, written...)

	return append(rollback, idempotent...)
}

// AnyPrunedReplay reports whether any spend failed with an answer that can mean
// a pruned replay (IsReplayAnswer). Only for spends whose Err slot is safe to
// read.
func AnyPrunedReplay(spends []*Spend) bool {
	for _, spend := range spends {
		if spend != nil && IsReplayAnswer(spend.Err) {
			return true
		}
	}

	return false
}

// IsSpendRollbackError reports whether a per-input spend error says the
// spending transaction is invalid, so the inputs the same Spend call did write
// must be reversed (RollbackSet decides which). An infrastructure error
// (DEVICE_OVERLOAD, timeout, cancellation) does not: the spend is idempotent for
// the same spender, so a retry matches the written inputs instead.
//
// Both stores decide with it only for RequireParentOutputs callers, the second
// wave of a below-checkpoint list, where a missing parent output fails the
// spend. Every other caller rolls back by the stores' own record-existence probe
// and transient-window gate, which differ between the stores.
//
// A missing parent counts only when requireParentOutputs is set, because only
// there is it a failure of the spend. Any other caller keeps the behaviour from
// before replay protection: a missing parent is not a validation failure, the
// fresh sibling spends stay, and a retry once the parent arrives matches them
// idempotently.
func IsSpendRollbackError(err error, requireParentOutputs bool) bool {
	return errors.Is(err, errors.ErrSpent) ||
		errors.Is(err, errors.ErrTxConflicting) ||
		errors.Is(err, errors.ErrFrozen) ||
		errors.Is(err, errors.ErrUtxoHashMismatch) ||
		errors.Is(err, errors.ErrUtxoSpendingTxPruned) ||
		(requireParentOutputs && errors.Is(err, errors.ErrTxNotFound))
}

// NeedsSpendRollback reports whether any spend failed with an error
// IsSpendRollbackError accepts.
func NeedsSpendRollback(spends []*Spend, requireParentOutputs bool) bool {
	for _, spend := range spends {
		if spend != nil && spend.Err != nil && IsSpendRollbackError(spend.Err, requireParentOutputs) {
			return true
		}
	}

	return false
}

// IsReplayAnswer reports whether a per-input spend error is the answer a
// replay of a pruned transaction gets: the marker rejection. The stores use it
// to decide that an idempotent match in the same call is historical and must
// not be reversed (RollbackSet).
//
// A missing parent does not count. Before the quick-validation cleanup was
// removed it stood in for a chain the pruner removed end to end, but on its own
// it is also what an ordinary retry sees: an earlier attempt that wrote one
// input and failed left that input spent by a transaction with no record, and
// treating the missing sibling parent as a replay kept that orphan spent for
// good. For a child this code pruned, the surviving parent carries the marker,
// so the marker answer alone covers it. A child pruned before this upgrade, or
// one whose marker write failed under the old pruner, has no marker: a replay
// of it beside a missing sibling parent still reverses the confirmed spend, as
// on main (see the LIMITATION in stores/utxo/sql/pruner/pruner_service.go). A
// prune that lands
// after an input's marker read is caught by the rollback instead, which reads
// the marker again once the spender is gone (withoutMarkedMatches in each
// store).
func IsReplayAnswer(err error) bool {
	return err != nil && errors.Is(err, errors.ErrUtxoSpendingTxPruned)
}

// ReplayRejectionsFirst reorders per-input spend errors so the marker
// rejection comes first, then a missing parent, then the rest. The Aerospike
// batched spend aggregates these with errors.JoinCapped, which keeps only the
// first few links. On a wide transaction whose marker hit sat past the cap the
// rejection vanished from the error, and the block paths could not recognise
// the replay. A missing parent ranks second not as a replay signal
// (IsReplayAnswer counts only the marker) but because the legacy block path
// treats it as a fault of this node's store (parkCommitFailure), so that link
// must survive the cap too. The order within each group is preserved.
func ReplayRejectionsFirst(errs []error) []error {
	ordered := make([]error, 0, len(errs))

	for _, rank := range []func(error) bool{
		func(err error) bool { return errors.Is(err, errors.ErrUtxoSpendingTxPruned) },
		func(err error) bool {
			return !errors.Is(err, errors.ErrUtxoSpendingTxPruned) && errors.Is(err, errors.ErrTxNotFound)
		},
		func(err error) bool {
			return !errors.Is(err, errors.ErrUtxoSpendingTxPruned) && !errors.Is(err, errors.ErrTxNotFound)
		},
	} {
		for _, err := range errs {
			if err != nil && rank(err) {
				ordered = append(ordered, err)
			}
		}
	}

	return ordered
}
