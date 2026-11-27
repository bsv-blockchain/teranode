package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// HistoricalSpendAge is how many blocks older than the incoming spend a
// recorded spend must be to count as history: half the pruning retention, 144
// at the default of 288.
//
// The pruner deletes a mined transaction no sooner than the retention after its
// block. A spend is usually recorded at or just above that block, so a replay
// of a pruned transaction usually meets a spend about the retention old. That
// is not a bound: a mempool spend is recorded at the tip plus one, and a reorg
// to a heavier but shorter chain can mine it lower without clearing that
// height. Half the retention is the margin for that gap. A spend recorded more
// than half the retention above the block that mined it is still young when the
// pruner deletes the transaction, so a replay at that moment is accepted; once
// the chain is half the retention past the recorded height, the replay is
// refused. An honest retry after a failed attempt usually comes back within
// minutes. One that comes back half the retention or more later is refused as a replay when its transaction has no record, including
// in a block: an earlier attempt that left a spend behind (a rollback skipped
// for a transient lock, one that failed, or on Aerospike an input still in
// flight when its call timed out that committed afterwards) and never created
// the transaction reads the same as a pruned one.
func HistoricalSpendAge(retention uint32) uint32 {
	if age := retention / 2; age > 0 {
		return age
	}

	return 1
}

// IsHistoricalSpend reports whether an idempotent match is a spend from
// history rather than an earlier attempt at the current call. recorded is false
// for a spend written before spend heights were stored; such a spend is not
// history. Counting it as history would refuse, for good, a block holding a
// transaction whose earlier attempt left a spend behind before the upgrade. The
// cost is a window of about the retention after the upgrade: a child spent
// before it and pruned after it has no height and no marker, so its replay is
// accepted as on a store without this check. A recorded height above the
// incoming one (a re-applied block below the tip) is not history. With retention 0 nothing is ever pruned, so no match
// is history: HistoricalSpendAge would otherwise clamp to one block and refuse
// an honest retry that follows its failed attempt by a block.
func IsHistoricalSpend(recordedHeight uint32, recorded bool, incomingHeight, retention uint32) bool {
	if retention == 0 {
		return false
	}

	if !recorded {
		return false
	}

	if incomingHeight < recordedHeight {
		return false
	}

	return incomingHeight-recordedHeight >= HistoricalSpendAge(retention)
}

// RollbackSet decides which of a failed Spend's successful inputs must be
// reversed: everything this call actually wrote, plus, in most cases, the
// inputs whose output already recorded exactly this spend.
//
// An idempotent match is ambiguous on the record: "this output already records
// this spender" reads the same whether a mined transaction committed it long
// ago or an earlier failed attempt at this very call did. The recorded spend
// height settles most cases. A match at least HistoricalSpendAge blocks older
// than the incoming spend is history (IsHistoricalSpend). The stores never pass such a match in
// idempotent, so Spend's own rollback never reverses it: reversing a confirmed
// spend would hand a confirmed output to the next spender. Spend still returns
// such a match among its successful inputs, and SequentialSpendAndCreate
// unspends every returned input when Create then fails with an error other than
// ErrTxExists or an unknown outcome, historical matches included.
//
// A young match is reversible, and reversing it is what keeps the store
// self-healing. The store leaves partial spends committed when a call fails on
// a transient error, so an attempt that wrote one input and then failed on
// another leaves that input spent by a transaction it never created. On the
// next attempt that input reads as a young idempotent match. Holding it back
// would make the orphan permanent: the output stays spent by a transaction the
// store does not hold, and its next legitimate spender is refused.
//
// historical holds back the young matches too. The caller sets it when an input
// was refused as a pruned replay (IsReplayAnswer), and whenever it does not know
// every input's answer, as on a spend call aborted while some inputs were still
// in flight: an unanswered input may be the refusal, and reading an in-flight
// slot is a data race besides. Holding a match back fails towards an output
// left spent by a transaction the store does not hold, which the next attempt
// heals; reversing it wrongly hands a confirmed output to anyone.
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

// AnyPrunedReplay reports whether any spend was refused as a pruned replay
// (IsReplayAnswer). Only for spends whose Err slot is safe to read.
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
// A missing parent selects this error-class rollback only for
// RequireParentOutputs calls. Ordinary calls use the stores' record-existence
// probe and transient-error gate, which can reverse fresh sibling spends and
// unmarked idempotent matches.
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

// IsReplayAnswer reports whether a per-input spend error is the refusal a replay
// of a pruned transaction gets, ErrUtxoSpendingTxPruned: a historical
// idempotent match whose spender has no record, or a spender an older pruner
// marked in the parent's replay markers. The stores use it to hold back every
// idempotent match of the call (RollbackSet).
//
// A missing parent does not count. On its own it is also what an ordinary
// retry sees: an earlier attempt that wrote one input and failed left that
// input spent by a transaction with no record, and treating the missing
// sibling parent as a replay would keep that orphan spent for good.
func IsReplayAnswer(err error) bool {
	return err != nil && errors.Is(err, errors.ErrUtxoSpendingTxPruned)
}

// ReplayRejectionsFirst reorders per-input spend errors so the pruned-replay
// refusal comes first, then a missing parent, then the rest. The Aerospike
// batched spend aggregates these with errors.JoinCapped, which keeps only the
// first few links. On a wide transaction whose refusal sat past the cap it
// vanished from the error, and the block paths could not recognise the replay.
// A missing parent ranks second not as a replay signal but because the legacy
// block path treats it as a fault of this node's store (parkCommitFailure), so
// that link must survive the cap too. The order within each group is preserved.
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

// KeepBelowHeight is the height below which a create-first reversal leaves a spend in place. A
// reversal undoes the spends of a tentative record, and the record owns only the spends recorded at
// or after the height it was created at. A spend recorded before it is kept when it is also history
// (IsHistoricalSpend): a replay of a transaction the pruner removed matches such a spend, and
// reversing it hands a confirmed output to anyone. A spend that is old but young by that rule is a
// leftover of an earlier attempt, and the reversal still clears it.
//
// recordHeight is the unminedSince of the record, 0 when it is not known, and the bound is then
// the age rule alone. currentHeight is the height the historical age is counted from. The result is
// 0 when no spend is kept: a store with retention 0 never prunes, so nothing is history, and a chain
// younger than HistoricalSpendAge has none either. A spend recorded at h is kept exactly when
// h < KeepBelowHeight, which is IsHistoricalSpend(h, true, currentHeight, retention) && h < recordHeight.
func KeepBelowHeight(recordHeight, currentHeight, retention uint32) uint32 {
	if retention == 0 {
		return 0
	}

	age := HistoricalSpendAge(retention)
	if currentHeight < age {
		return 0
	}

	bound := currentHeight - age + 1
	if recordHeight != 0 && recordHeight < bound {
		bound = recordHeight
	}

	return bound
}

type reversalGuardKey struct{}

// ReversalGuard is what a create-first reversal tells the store that reverses its spends.
type ReversalGuard struct {
	// Record is the txid of the tentative record whose spends are reversed. The store reads the
	// height of the record, and keeps the spends that predate it (KeepBelowHeight). Nil: no guard.
	Record *chainhash.Hash
	// CallHeight is the block height of the call that failed, 0 when not known. The store counts
	// history from the larger of it and its own height, so a lagging tip never reverses a spend that
	// the age rule refused as a replay.
	CallHeight uint32
}

// WithReversalCallHeight returns ctx carrying the block height of the call whose spends a
// create-first reversal may undo.
func WithReversalCallHeight(ctx context.Context, height uint32) context.Context {
	guard, _ := ReversalGuardFrom(ctx)
	guard.CallHeight = height

	return context.WithValue(ctx, reversalGuardKey{}, guard)
}

// WithReversedRecord returns ctx telling a store to keep, in the Unspend calls that run under it,
// the historical spends that predate the tentative record of hash. Only the create-first reversals
// set it; every other Unspend clears what it is told to, as before.
func WithReversedRecord(ctx context.Context, hash *chainhash.Hash) context.Context {
	guard, _ := ReversalGuardFrom(ctx)
	guard.Record = hash

	return context.WithValue(ctx, reversalGuardKey{}, guard)
}

// ReversalGuardFrom returns the guard ctx carries and whether it carries one.
func ReversalGuardFrom(ctx context.Context) (ReversalGuard, bool) {
	guard, ok := ctx.Value(reversalGuardKey{}).(ReversalGuard)

	return guard, ok
}
