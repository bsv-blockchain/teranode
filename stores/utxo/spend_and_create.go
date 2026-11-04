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
			if errors.Is(err, errors.ErrUtxoSpendingTxPruned) {
				removeMarkedRecreation(ctx, logger, s, tx.TxIDChainHash())
			}

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

	if IsCreateOutcomeUnknown(err) && anyMatched(spends) {
		return nil, spends, recheckUnknownCreate(ctx, logger, s, tx, blockHeight, options.IgnoreFlags, err)
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

// completeDeleter is the store method the recheck removes a recreated record
// with, and metaReader the one it reads a record's block ids with. Both in-tree
// stores implement them.
type completeDeleter interface {
	DeleteComplete(ctx context.Context, hash *chainhash.Hash) error
}

type metaReader interface {
	GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error
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
// settles it either way. A replay meets the marker at the spend, and the spend
// arm of SequentialSpendAndCreate removes the unmined record this call kept
// (removeMarkedRecreation); anything else finds the record and gets
// ErrTxExists. One case a retry does not settle: when every parent of the
// replay was pruned as well, no marker survives, so the retry's spend is
// blessed on the kept record and the create answers ErrTxExists. That record
// then stands.
//
// Only a transaction whose spend the store had already answered for is
// rechecked, so a transaction seen for the first time pays nothing.
func recheckRecreated(ctx context.Context, logger ulogger.Logger, s SequentialStore, tx *bt.Tx, blockHeight uint32, ignoreFlags IgnoreFlags) error {
	answer, err := recheckSpend(ctx, s, tx, blockHeight, ignoreFlags)
	if err != nil {
		logger.Errorf("[SpendAndCreate] %s: recheck after create could not answer, keeping the created record: %v", tx.TxIDChainHash().String(), err)

		return MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("SpendAndCreate: recheck of %s after create could not answer, keeping the created record", tx.TxIDChainHash().String(), err))
	}

	if answer == nil {
		return nil
	}

	deleter, ok := s.(completeDeleter)
	if !ok {
		return errors.NewStorageError("SpendAndCreate: %s recreated a pruned transaction and the store cannot remove it", tx.TxIDChainHash().String(), answer)
	}

	if err := removeRecreated(ctx, s, deleter, tx.TxIDChainHash()); err != nil {
		logger.Errorf("[SpendAndCreate] %s recreated a pruned transaction and could not remove it; it remains in the store: %v", tx.TxIDChainHash().String(), err)

		return errors.NewStorageError("SpendAndCreate: could not remove recreated pruned transaction %s", tx.TxIDChainHash().String(), errors.Join(answer, err))
	}

	return answer
}

// recheckUnknownCreate is the recheck after a create whose outcome is unknown
// (IsCreateOutcomeUnknown) over a spend the store had already answered for. The
// write may still land, so a pruned C can be recreated with no error to say so.
//
// Unlike after a successful create, nothing here proves C's record was absent,
// so a replay answer alone does not license a delete. A live C can answer one:
// a C whose parent was pruned has its rebroadcast blessed and then answers the
// missing parent, and a C the pruner held back after writing one parent's
// marker answers that marker. Deleting either would remove a mined transaction
// with unspent outputs. So the record is removed only when the marker answers
// and the record carries no block id: the pruner marks only a mined child, so
// an unmined record behind a marker is a recreation, never the original. A
// record with block ids is left alone, and so is a create from a block path,
// which writes block ids. Those paths fail the attempt, and their compensation
// (removePrunedReplayGhosts in blockvalidation, on the list path and the
// one-at-a-time fallback alike) removes the record on the retry as a leftover
// of the block. It finds a leftover only while it is locked, so with
// blockvalidation_quick_validate_skip_utxo_lock set such a record stands.
//
// Every outcome returns an error still marked MarkCreateOutcomeUnknown, so the
// spends are kept. One gap stays: a write still in flight can land after the
// delete, or after a read that found nothing.
func recheckUnknownCreate(ctx context.Context, logger ulogger.Logger, s SequentialStore, tx *bt.Tx, blockHeight uint32, ignoreFlags IgnoreFlags, createErr error) error {
	hash := tx.TxIDChainHash()

	answer, err := recheckSpend(ctx, s, tx, blockHeight, ignoreFlags)
	if err != nil {
		logger.Errorf("[SpendAndCreate] %s: recheck after a create with an unknown outcome could not answer, keeping any record the create wrote: %v", hash.String(), err)

		return createErr
	}

	if !errors.Is(answer, errors.ErrUtxoSpendingTxPruned) {
		return createErr
	}

	if !removeMarkedRecreation(ctx, logger, s, hash) {
		return createErr
	}

	logger.Warnf("[SpendAndCreate] %s: removed a recreated pruned transaction after a create with an unknown outcome; a write still in flight could land after this", hash.String())

	return MarkCreateOutcomeUnknown(errors.NewUtxoSpendingTxPrunedError("SpendAndCreate: %s recreated a pruned transaction after a create with an unknown outcome and it was removed", hash.String(), createErr))
}

// removeMarkedRecreation removes the record of a transaction whose spend has
// just met a pruner marker, when that record carries no block id, and reports
// whether it is now gone. The pruner marks only a mined child, so an unmined
// record behind a marker is a recreation, never the original: a create that
// landed after the pruner removed the original, whose recheck could not
// answer or whose outcome was unknown. Without this, a retry of that replay
// would be rejected at the spend and never look at the record again, and the
// record would stand for good, locked and unmined, where the block-assembly
// reload picks it up as a transaction to mine.
//
// A record with block ids is left alone: it is the original, held back by the
// pruner after writing one parent's marker, or a block path's write, which that
// path's own compensation removes on the failed attempt. No record at all is
// the normal case and costs one read. A read or delete that fails is logged
// and leaves the record, and the caller's answer is the same either way.
func removeMarkedRecreation(ctx context.Context, logger ulogger.Logger, s SequentialStore, hash *chainhash.Hash) bool {
	deleter, canDelete := s.(completeDeleter)
	reader, canRead := s.(metaReader)

	if !canDelete || !canRead {
		logger.Errorf("[SpendAndCreate] %s: spend met a pruned transaction's marker and the store cannot check or remove a recreated record", hash.String())

		return false
	}

	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackBound(s))
	defer cancel()

	var (
		record meta.Data
		absent bool
	)

	if err := retryStoreCall(readCtx, func() error {
		err := reader.GetMeta(readCtx, hash, &record)
		if err != nil && (errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound)) {
			absent = true

			return nil
		}

		return err
	}); err != nil {
		logger.Errorf("[SpendAndCreate] %s: spend met a pruned transaction's marker and the record could not be read: %v", hash.String(), err)

		return false
	}

	if absent {
		return true
	}

	if len(record.BlockIDs) > 0 {
		return false
	}

	if err := removeRecreated(ctx, s, deleter, hash); err != nil {
		logger.Errorf("[SpendAndCreate] %s: spend met a pruned transaction's marker and the recreated record could not be removed: %v", hash.String(), err)

		return false
	}

	logger.Warnf("[SpendAndCreate] %s: removed an unmined record of a pruned transaction whose spend met the pruner's marker", hash.String())

	return true
}

// recheckSpend spends tx again with RequireParentOutputs, detached from ctx and
// bounded by the rollback timeout, with up to three attempts. answer is nil or a
// replay answer; err is set when no attempt gave either.
func recheckSpend(ctx context.Context, s SequentialStore, tx *bt.Tx, blockHeight uint32, ignoreFlags IgnoreFlags) (answer error, err error) {
	ignoreFlags.RequireParentOutputs = true

	recheckCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackBound(s))
	defer cancel()

	err = retryStoreCall(recheckCtx, func() error {
		_, answer = s.Spend(recheckCtx, tx, blockHeight, ignoreFlags)
		if answer == nil || IsReplayAnswer(answer) {
			return nil
		}

		return answer
	})
	if err != nil {
		return nil, err
	}

	return answer, nil
}

// removeRecreated deletes a recreated record, detached from ctx and bounded by
// the rollback timeout, with up to three attempts. A record already gone
// counts as removed.
func removeRecreated(ctx context.Context, s SequentialStore, deleter completeDeleter, hash *chainhash.Hash) error {
	delCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackBound(s))
	defer cancel()

	return retryStoreCall(delCtx, func() error {
		err := deleter.DeleteComplete(delCtx, hash)
		if err != nil && (errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound)) {
			return nil
		}

		return err
	})
}

// rollbackBound is the store's SpendRollbackTimeout, or
// defaultSequentialRollbackTimeout when the store names none.
func rollbackBound(s SequentialStore) time.Duration {
	if b, ok := s.(spendRollbackBounded); ok && b.SpendRollbackTimeout() > 0 {
		return b.SpendRollbackTimeout()
	}

	return defaultSequentialRollbackTimeout
}

// unspendDetached runs unspendWithRetry under a context that ignores ctx's
// cancellation and deadline and carries the rollback's own bound instead.
func unspendDetached(ctx context.Context, logger ulogger.Logger, s SequentialStore, spends []*Spend) error {
	rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackBound(s))
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
