package utxo

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// unspendRetryBackoffBase is the base delay for the exponential backoff between
// rollback attempts. A variable so tests can shorten it.
var unspendRetryBackoffBase = time.Second

// defaultSequentialRollbackTimeout bounds the rollback of SequentialSpendAndCreate
// for a store that does not name its own bound: the settings default of
// utxostore_spendRollbackTimeout.
const defaultSequentialRollbackTimeout = 120 * time.Second

// spendRollbackBounded is implemented by a SequentialStore that bounds its
// rollbacks itself. The SQL and Aerospike stores return
// utxostore_spendRollbackTimeout, the bound their own partial-spend rollbacks
// already use.
type spendRollbackBounded interface {
	SpendRollbackTimeout() time.Duration
}

// createOutcomeUnknownKey is the error-data key MarkCreateOutcomeUnknown sets.
const createOutcomeUnknownKey = "createOutcomeUnknown"

// MarkCreateOutcomeUnknown marks err as returned by a Create that stopped
// waiting for its write while the write could still commit: the store's own
// wait for its batch ran out, not the batch failing. SequentialSpendAndCreate
// keeps the spends for such an error, as it does for a context error. It
// returns err.
func MarkCreateOutcomeUnknown(err *errors.Error) *errors.Error {
	if err != nil {
		err.SetData(createOutcomeUnknownKey, true)
	}

	return err
}

// IsCreateOutcomeUnknown reports whether an error from Create leaves the
// create's outcome unknown: a context error, or an error the store marked with
// MarkCreateOutcomeUnknown anywhere in its chain.
func IsCreateOutcomeUnknown(err error) bool {
	if err == nil {
		return false
	}

	if errors.IsContextError(err) {
		return true
	}

	for cur := err; cur != nil; {
		if te, ok := cur.(*errors.Error); ok && te.GetData(createOutcomeUnknownKey) == true {
			return true
		}

		u, ok := cur.(interface{ Unwrap() error })
		if !ok {
			return false
		}

		cur = u.Unwrap()
	}

	return false
}

// ParseCreateOptions applies opts to a fresh CreateOptions and validates the
// combination. Every Store implementation of SpendAndCreate should use this so
// option validation stays identical across backends.
//
// Combinations that are ignored rather than rejected: with SpendOnly the
// create-side fields (MinedBlockInfos, TxID, IsCoinbase, Frozen, Conflicting,
// Locked, SkipExtendedInputs) are unused; with CreateOnly the IgnoreFlags are
// unused.
func ParseCreateOptions(opts ...CreateOption) (*CreateOptions, error) {
	options := &CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if options.CreateOnly && options.SpendOnly {
		return nil, errors.NewInvalidArgumentError("SpendAndCreate: WithCreateOnly and WithSpendOnly are mutually exclusive")
	}

	return options, nil
}

// SequentialStore is the subset of a concrete store's methods needed by
// SequentialSpendAndCreate. Out-of-tree backends that want the sequential
// behaviour implement these three methods and delegate.
type SequentialStore interface {
	Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error)
	Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...IgnoreFlags) ([]*Spend, error)
	Unspend(ctx context.Context, spends []*Spend, flagAsLocked ...bool) error
}

// SequentialSpendAndCreate implements the Store.SpendAndCreate contract as the
// original two-call sequence: spend the inputs, create the outputs, and roll the
// spends back (with retries) when the create definitely failed. Concrete stores
// delegate to it until they implement SpendAndCreate atomically (Postgres
// transactions, Aerospike-specific logic).
//
// A create that fails with ErrTxExists, or with an error that leaves its
// outcome unknown (IsCreateOutcomeUnknown), keeps the spends. ErrTxExists means
// the record is there. The SQL and Aerospike creates go through a batcher, and
// a caller that stops waiting for its item returns at once, while the batch can
// still commit the record. That happens when the caller's context ends, and on
// Aerospike also when the store's own batcher wait runs out, which returns a
// ServiceUnavailable error marked with MarkCreateOutcomeUnknown. Unspending then would leave a record whose
// inputs are unspent, which a repeat never heals, since a repeat drops a
// transaction whose record exists. Keeping the spends leaves either a written
// transaction or a parent naming a spender with no record yet, and a repeat of
// the same SpendAndCreate heals the second: the spend is accepted as the same
// spend and the create runs.
//
// Returned spends: nil when the create failed and the rollback succeeded (the
// spends are no longer in effect); the live spends when the create failed with
// ErrTxExists or an unknown outcome, or the rollback itself failed.
//
// The rollback does not run under ctx, so a ctx that ends during it cannot
// leave it half done. It runs under context.WithoutCancel(ctx), bounded by the
// store's SpendRollbackTimeout (defaultSequentialRollbackTimeout when the store
// names none), with the same three attempts, so a dead store still cannot hold
// the caller past that bound.
func SequentialSpendAndCreate(ctx context.Context, logger ulogger.Logger, s SequentialStore,
	tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, []*Spend, error) {
	options, err := ParseCreateOptions(opts...)
	if err != nil {
		return nil, nil, err
	}

	var spends []*Spend

	if !options.CreateOnly {
		spends, err = s.Spend(ctx, tx, blockHeight, options.IgnoreFlags)
		if err != nil {
			return nil, spends, err
		}

		if options.SpendOnly {
			return nil, spends, nil
		}
	}

	md, err := s.Create(ctx, tx, blockHeight, opts...)
	if err == nil && anyMatched(spends) {
		if err = recheckRecreated(ctx, logger, s, tx, blockHeight, options.IgnoreFlags); err != nil {
			return nil, spends, err
		}
	}

	if err != nil {
		if errors.Is(err, errors.ErrTxExists) {
			// the tx already exists; leave the spends in place for the caller to decide
			return nil, spends, err
		}

		if IsCreateOutcomeUnknown(err) {
			// the create may still commit; leave the spends in place, as for ErrTxExists
			return nil, spends, err
		}

		if len(spends) > 0 {
			if rollbackErr := unspendDetached(ctx, logger, s, spends); rollbackErr != nil {
				return nil, spends, errors.NewProcessingError("SpendAndCreate: error reversing utxo spends: %v", rollbackErr, err)
			}
		}

		return nil, nil, err
	}

	return md, spends, nil
}

// anyMatched reports whether any spend wrote nothing because the store already
// answered for it: an output that recorded exactly that spend (Idempotent), or
// a missing parent accepted on the strength of the spender's own record
// (Blessed).
func anyMatched(spends []*Spend) bool {
	for _, sp := range spends {
		if sp != nil && (sp.Idempotent || sp.Blessed) {
			return true
		}
	}

	return false
}

// completeDeleter is the store method recheckRecreated removes a recreated
// record with. Both in-tree stores implement it.
type completeDeleter interface {
	DeleteComplete(ctx context.Context, hash *chainhash.Hash) error
}

// recheckRecreated closes the window between the spend and the create of a
// replayed transaction, after a create that succeeded over a spend the store
// had already answered for.
//
// A rebroadcast of a mined, fully spent transaction C has its spends accepted
// without writing anything: as idempotent, since every surviving parent output
// already names C, or as blessed, since a parent is gone but C's record is
// there. If the pruner deletes C after that spend and before the create, the
// create finds no record and writes C again, with outputs a confirmed
// descendant already consumed. Nothing rejects, so no caller compensates.
//
// The pruner writes a child's markers before it deletes the child, so a create
// that found C gone can see them now. Spending again asks the store exactly
// that: the spend answers a marker ahead of an idempotent match. The recheck
// sets RequireParentOutputs, so a missing parent is not blessed by the record
// this call just wrote, which is what lets a C whose parents were pruned too be
// caught, and so the recheck never reverses an idempotent match.
//
// Only a replay answer (IsReplayAnswer: the marker, or a parent that is gone)
// deletes the record this call created. A parent that existed at the spend and
// is gone at the recheck was pruned in between, and is pruned only once fully
// spent and buried, so the transaction is the pruned one. The record is deleted
// detached and bounded, like the rollback, and the answer returned. The
// historical spends stay, as a normal prune leaves them.
//
// The recheck itself runs detached and bounded too, with the delete's three
// attempts, so a caller's deadline that the spend and the create used up does
// not stop it. Any other failure says nothing about a replay, and a concurrent
// caller may already have accepted the record as existing, so the record and
// the spends are kept. The failure is still returned, as a ServiceUnavailable
// error marked with MarkCreateOutcomeUnknown, because whether the record should
// stand is unknown: a caller that read success would never look again. A retry
// heals it either way: a replay meets the marker at the spend, and anything
// else finds the record and gets ErrTxExists.
//
// Only a transaction whose spend the store had already answered for is
// rechecked, so a transaction seen for the first time pays nothing.
func recheckRecreated(ctx context.Context, logger ulogger.Logger, s SequentialStore, tx *bt.Tx, blockHeight uint32, ignoreFlags IgnoreFlags) error {
	ignoreFlags.RequireParentOutputs = true

	bound := defaultSequentialRollbackTimeout

	if b, ok := s.(spendRollbackBounded); ok && b.SpendRollbackTimeout() > 0 {
		bound = b.SpendRollbackTimeout()
	}

	recheckCtx, cancelRecheck := context.WithTimeout(context.WithoutCancel(ctx), bound)
	defer cancelRecheck()

	var recheckErr error

	if err := retryStoreCall(recheckCtx, func() error {
		_, recheckErr = s.Spend(recheckCtx, tx, blockHeight, ignoreFlags)
		if recheckErr == nil || IsReplayAnswer(recheckErr) {
			return nil
		}

		return recheckErr
	}); err != nil {
		logger.Errorf("[SpendAndCreate] %s: recheck after create could not answer, keeping the created record: %v", tx.TxIDChainHash().String(), err)

		return MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("SpendAndCreate: recheck of %s after create could not answer, keeping the created record", tx.TxIDChainHash().String(), err))
	}

	if recheckErr == nil {
		return nil
	}

	deleter, ok := s.(completeDeleter)
	if !ok {
		return errors.NewStorageError("SpendAndCreate: %s recreated a pruned transaction and the store cannot remove it", tx.TxIDChainHash().String(), recheckErr)
	}

	delCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bound)
	defer cancel()

	if err := retryStoreCall(delCtx, func() error {
		err := deleter.DeleteComplete(delCtx, tx.TxIDChainHash())
		if err != nil && (errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound)) {
			return nil
		}

		return err
	}); err != nil {
		logger.Errorf("[SpendAndCreate] %s recreated a pruned transaction and could not remove it; it remains in the store: %v", tx.TxIDChainHash().String(), err)

		return errors.NewStorageError("SpendAndCreate: could not remove recreated pruned transaction %s", tx.TxIDChainHash().String(), errors.Join(recheckErr, err))
	}

	return recheckErr
}

// unspendDetached runs unspendWithRetry under a context that ignores ctx's
// cancellation and deadline and carries the rollback's own bound instead.
func unspendDetached(ctx context.Context, logger ulogger.Logger, s SequentialStore, spends []*Spend) error {
	bound := defaultSequentialRollbackTimeout

	if b, ok := s.(spendRollbackBounded); ok && b.SpendRollbackTimeout() > 0 {
		bound = b.SpendRollbackTimeout()
	}

	rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bound)
	defer cancel()

	return unspendWithRetry(rbCtx, logger, s, spends)
}

// unspendWithRetry reverses spends with up to 3 attempts and exponential backoff,
// ported from the validator's reverseSpends. The backoff aborts early when ctx
// is cancelled.
func unspendWithRetry(ctx context.Context, logger ulogger.Logger, s SequentialStore, spends []*Spend) error {
	for retries := uint(0); retries < 3; retries++ {
		if errReset := s.Unspend(ctx, spends); errReset != nil {
			if retries < 2 {
				backoff := time.Duration(1<<retries) * unspendRetryBackoffBase
				logger.Errorf("error resetting utxos, retrying in %s: %v", backoff.String(), errReset)

				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return errors.NewProcessingError("error resetting utxos, context cancelled during retry backoff", errors.Join(errReset, ctx.Err()))
				case <-timer.C:
				}
			} else {
				return errors.NewProcessingError("error resetting utxos", errReset)
			}
		} else {
			break
		}
	}

	return nil
}
