package utxo

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

	return hasErrorData(err, createOutcomeUnknownKey)
}

// rolledForwardKey is the error-data key MarkRolledForward sets.
const rolledForwardKey = "rolledForward"

// MarkRolledForward marks err as the ErrTxExists a create-first SpendAndCreate returns after
// it found a record still creating and rolled it forward itself (re-spend, finalize). The
// record is then final and its spends are in effect, so the caller hands the transaction on
// without spending again. It returns err.
func MarkRolledForward(err *errors.Error) *errors.Error {
	if err != nil {
		err.SetData(rolledForwardKey, true)
	}

	return err
}

// IsRolledForward reports whether an error carries the MarkRolledForward marker anywhere in
// its chain. It walks Unwrap because callers wrap the error before they test it.
func IsRolledForward(err error) bool {
	return hasErrorData(err, rolledForwardKey)
}

// hasErrorData reports whether any *errors.Error in the Unwrap chain of err carries key set to true.
func hasErrorData(err error, key string) bool {
	for cur := err; cur != nil; {
		if te, ok := cur.(*errors.Error); ok && te.GetData(key) == true {
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

// prometheusCreateFirstFinalizeFailed counts create-first FinalizeTransaction failures
// that occurred after the spends succeeded (the tx is left in the creating state for
// recovery). A steady non-zero rate means finalize is failing systemically.
var prometheusCreateFirstFinalizeFailed = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "teranode",
	Subsystem: "utxo",
	Name:      "create_first_finalize_failed_total",
	Help:      "Number of create-first FinalizeTransaction failures after successful spends (tx left creating for recovery)",
})

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
// behaviour implement these methods and delegate.
type SequentialStore interface {
	Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error)
	Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...IgnoreFlags) ([]*Spend, error)
	Unspend(ctx context.Context, spends []*Spend, flagAsLocked ...bool) error
	// FinalizeTransaction clears the tentative creating state; used by the create-first path.
	FinalizeTransaction(ctx context.Context, tx *bt.Tx) error
	// DeleteComplete removes a record entirely; used by the create-first path to drop the
	// tentative record when its spends fail.
	DeleteComplete(ctx context.Context, hash *chainhash.Hash) error
	// BeginCreateFirstRollback marks a still-creating record as rolling back, so no concurrent
	// roll-forward can finalize it; see Store.BeginCreateFirstRollback.
	BeginCreateFirstRollback(ctx context.Context, hash *chainhash.Hash) (bool, error)
	// GetMeta reads the record's flags; used to tell a creating, rolling-back or final
	// record apart when Create reports ErrTxExists.
	GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error
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
	tx *bt.Tx, blockHeight uint32, createFirst bool, opts ...CreateOption) (*meta.Data, []*Spend, error) {
	options, err := ParseCreateOptions(opts...)
	if err != nil {
		return nil, nil, err
	}

	// WithCreating is managed exclusively by the create-first path; a caller must never
	// set it on SpendAndCreate. Allowing it would let a caller-supplied WithCreating(false)
	// defeat the forced tentative create (last-write-wins on the option slice), producing a
	// spendable record before its inputs are spent — the exact invariant this design rests
	// on. Reject it rather than silently letting ordering decide.
	if options.Creating {
		return nil, nil, errors.NewInvalidArgumentError("SpendAndCreate: WithCreating is managed by the create-first path and must not be passed by callers")
	}

	// Create-first ordering (create tentative → spend → finalize) applies only to the
	// combined path. Single-phase callers (CreateOnly / SpendOnly, e.g. coinbase, seeding,
	// reorg/conflict helpers) keep the direct behaviour — there is nothing to sequence.
	if createFirst && !options.CreateOnly && !options.SpendOnly {
		return sequentialCreateFirst(ctx, logger, s, tx, blockHeight, options.IgnoreFlags, opts...)
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

// rollbackContext returns a context that ignores ctx's cancellation and deadline
// and carries the rollback's own bound instead.
func rollbackContext(ctx context.Context, s any) (context.Context, context.CancelFunc) {
	bound := defaultSequentialRollbackTimeout

	if b, ok := s.(spendRollbackBounded); ok && b.SpendRollbackTimeout() > 0 {
		bound = b.SpendRollbackTimeout()
	}

	return context.WithTimeout(context.WithoutCancel(ctx), bound)
}

// unspendDetached runs unspendWithRetry under rollbackContext.
func unspendDetached(ctx context.Context, logger ulogger.Logger, s SpendReverser, spends []*Spend) error {
	rbCtx, cancel := rollbackContext(ctx, s)
	defer cancel()

	return unspendWithRetry(rbCtx, logger, s, spends)
}

// finalizeDetached runs finalizeWithRetry under rollbackContext.
func finalizeDetached(ctx context.Context, logger ulogger.Logger, s SequentialStore, tx *bt.Tx) error {
	fCtx, cancel := rollbackContext(ctx, s)
	defer cancel()

	return finalizeWithRetry(fCtx, logger, s, tx)
}

// SpendReverser is the store subset that reverses spends.
type SpendReverser interface {
	Unspend(ctx context.Context, spends []*Spend, flagAsLocked ...bool) error
}

// CreatingRollBacker is the store subset RollBackCreating needs.
type CreatingRollBacker interface {
	SpendReverser
	BeginCreateFirstRollback(ctx context.Context, hash *chainhash.Hash) (bool, error)
	DeleteComplete(ctx context.Context, hash *chainhash.Hash) error
}

// succeededSpends returns the spends that carry no per-input error.
func succeededSpends(spends []*Spend) []*Spend {
	var succeeded []*Spend

	for _, sp := range spends {
		if sp != nil && sp.Err == nil {
			succeeded = append(succeeded, sp)
		}
	}

	return succeeded
}

// tolerantReverser unspends through UnspendTolerantOfMissingParents, so a parent that is
// already gone does not fail a rollback.
type tolerantReverser struct {
	SpendReverser
}

func (r tolerantReverser) Unspend(ctx context.Context, spends []*Spend, flagAsLocked ...bool) error {
	return UnspendTolerantOfMissingParents(ctx, r.SpendReverser, spends)
}

// RollBackCreating undoes a create-first transition whose spend failed: it marks the record
// rolling back, unspends every input of tx, then deletes the record. It runs under one
// detached context bounded like the spend-first rollback (SpendRollbackTimeout).
//
// The unspend covers the whole input set, not only the spends that succeeded in the caller's
// pass. An input that an earlier attempt spent may fail in this pass for another reason, for
// example a locked parent, and a reversal limited to this pass would leave it spent by the
// deleted record. The store's ownership check makes an unspend of an input that tx does not
// own a no-op, so a rival's spend is never cleared.
//
// The marker is what makes the order safe. While the record carries it, no roll-forward
// can finalize the record, so the rollback owner is the only writer of its final outcome.
// The store's own partial-spend rollback cannot do this work, because it probes for the
// spender record and the tentative record makes that probe succeed.
//
//   - The marker call fails: the record is left creating and nothing is reversed. The error
//     is returned, and the sweep re-spends the record and either finalizes or rolls it back.
//   - The record is absent or already final: a concurrent roll-forward finalized it and
//     relies on the spends, so nothing is reversed. The return is nil.
//   - The reversal fails: the record is left rolling back and the error is returned. Nobody
//     can finalize it, and the sweep finishes the rollback.
//   - The delete fails: same, the record is left rolling back for the sweep.
//
// Residual: an unspend cannot tell two attempts of the same txid apart, because
// SpendingData is the txid and the input index. The marker stops a finalize while a rollback
// runs, and the sweep finishes an abandoned rollback only after CreatingTxSweepMinAgeBlocks,
// which must outlast utxostore_spendRollbackTimeout in wall-clock time. A rollback owner
// that stalls past that bound and resumes after a fresh create of the same txid can still
// clear that create's spends. That is the attempt-identity gap tracked in issue 1291.
func RollBackCreating(ctx context.Context, logger ulogger.Logger, s CreatingRollBacker, tx *bt.Tx) error {
	rbCtx, cancel := rollbackContext(ctx, s)
	defer cancel()

	hash := tx.TxIDChainHash()

	began, err := s.BeginCreateFirstRollback(rbCtx, hash)
	if err != nil {
		logger.Errorf("SpendAndCreate create-first: marking %s as rolling back failed, tx left creating for recovery: %v", hash.String(), err)

		return err
	}

	if !began {
		logger.Warnf("SpendAndCreate create-first: tentative record for %s was finalized by a concurrent roll-forward, spends left in place", hash.String())

		return nil
	}

	inputs, err := GetSpends(tx)
	if err != nil {
		logger.Errorf("SpendAndCreate create-first: building the input spends failed for %s, tx left rolling back for the sweep: %v", hash.String(), err)

		return err
	}

	if len(inputs) > 0 {
		if err := unspendWithRetry(rbCtx, logger, tolerantReverser{s}, inputs); err != nil {
			logger.Errorf("SpendAndCreate create-first: reversing spends failed for %s, tx left rolling back for the sweep: %v", hash.String(), err)

			return err
		}
	}

	if err := s.DeleteComplete(rbCtx, hash); err != nil {
		logger.Errorf("SpendAndCreate create-first: deleting the rolled back record failed for %s, tx left rolling back for the sweep: %v", hash.String(), err)

		return err
	}

	return nil
}

// sequentialCreateFirst persists a tx create-first: the record is created in the
// tentative "creating" state, the inputs are spent, then the flag is cleared. Because
// the record exists (spend-gated) before its inputs are spent, a crash in the
// spend→finalize window leaves an inspectable, recoverable record rather than a
// dangling spender reference. This addresses the spend-before-create crash window (one
// mechanism related to #1214); it does not by itself satisfy #1214's delete-path audit.
//
// Failure handling:
//   - Create returns ErrTxExists: the record is read back. A final record is an ordinary
//     duplicate and ErrTxExists is returned unchanged. A rolling-back record is a transient
//     ProcessingError, because its owner deletes it within the rollback bound or the sweep
//     does. A still-creating record is rolled forward here, by the rule RollForwardCreating
//     defines, under the caller's ignore flags and without RequireParentOutputs: that
//     record exists by design, and a parent is pruned only once every output is spent. On
//     success the call returns the spends and ErrTxExists marked with MarkRolledForward.
//     On failure it returns the roll-forward's error and its spends, and the record is
//     left for the sweep.
//   - The first spend sets RequireParentOutputs. The record it would otherwise find is this
//     call's own tentative one, which proves nothing about a missing parent.
//   - Spend fails with no per-input result, with a context error or with ErrServiceUnavailable:
//     the spend batch may still commit after the caller returned, so nothing is reversed or
//     deleted. The creating record stays as the recovery journal and the sweep re-spends
//     idempotently.
//   - Spend fails otherwise: RollBackCreating marks the record rolling back, unspends every
//     input of the tx and deletes the record, under a detached context bounded like the
//     spend-first rollback. Without the rollback a partially spent tx would leave its free
//     inputs naming a record that is later pruned (the #1214 shape), and every rejected
//     double-spend would leave a durable record. The per-input spends and the error are
//     returned, with nil meta, so the caller can classify them. If the rollback fails, the
//     returned error wraps both failures.
//   - FinalizeTransaction fails after successful spends: it is retried up to 3 times with
//     exponential backoff under a detached context. If every attempt fails, return an
//     error. The inputs are durably spent but the tentative flag could not be cleared, so
//     the outputs are still unspendable; reporting success would make the validator
//     announce the tx to block assembly (and unlock it) while its children fail with an
//     unhandled ErrTxCreating. The record stays creating and is recovered by the
//     ErrTxExists roll-forward on re-encounter, the pruner sweep, or setMined.
func sequentialCreateFirst(ctx context.Context, logger ulogger.Logger, s SequentialStore,
	tx *bt.Tx, blockHeight uint32, ignoreFlags IgnoreFlags, opts ...CreateOption) (*meta.Data, []*Spend, error) {
	// Force WithCreating(true) last so it cannot be overridden by a caller option (belt and
	// suspenders alongside the caller-supplied WithCreating rejection in SequentialSpendAndCreate).
	createOpts := append(append([]CreateOption{}, opts...), WithCreating(true))

	hash := tx.TxIDChainHash()

	md, err := s.Create(ctx, tx, blockHeight, createOpts...)
	if err != nil {
		if !errors.Is(err, errors.ErrTxExists) {
			return nil, nil, err
		}

		existing := &meta.Data{}
		if getErr := s.GetMeta(ctx, hash, existing); getErr != nil {
			return nil, nil, errors.NewProcessingError("SpendAndCreate create-first: tx %s exists but its state could not be read", hash.String(), getErr)
		}

		switch {
		case !existing.Creating:
			return nil, nil, err
		case existing.RollingBack:
			return nil, nil, errors.NewProcessingError("SpendAndCreate create-first: tx %s is rolling back; retry after it completes", hash.String())
		}

		spends, rfErr := rollForwardCreating(ctx, logger, s, tx, func() ([]*Spend, error) {
			return s.Spend(ctx, tx, blockHeight, ignoreFlags)
		})
		if rfErr != nil {
			return nil, spends, rfErr
		}

		return nil, spends, MarkRolledForward(errors.NewTxExistsError("SpendAndCreate create-first: tx %s was creating and has been rolled forward", hash.String()))
	}

	freshFlags := ignoreFlags
	freshFlags.RequireParentOutputs = true

	spends, err := s.Spend(ctx, tx, blockHeight, freshFlags)
	if err != nil {
		if spends == nil || errors.IsContextError(err) || errors.Is(err, errors.ErrServiceUnavailable) {
			return nil, spends, err
		}

		if rollbackErr := RollBackCreating(ctx, logger, s, tx); rollbackErr != nil {
			return nil, spends, errors.NewProcessingError("SpendAndCreate: error reversing utxo spends: %v", rollbackErr, err)
		}

		return nil, spends, err
	}

	if finErr := finalizeDetached(ctx, logger, s, tx); finErr != nil {
		logger.Errorf("SpendAndCreate create-first: FinalizeTransaction failed for %s, tx left creating for recovery: %v", hash.String(), finErr)
		prometheusCreateFirstFinalizeFailed.Inc()

		return md, spends, errors.NewProcessingError("SpendAndCreate create-first: finalize failed for %s, tx left creating for recovery", hash.String(), finErr)
	}

	md.Creating = false

	return md, spends, nil
}

// AllInputsSpent reports whether spends covered every input of tx with no per-input
// error — the completeness check every create-first roll-forward site must pass before
// finalizing. A nil top-level error is not sufficient proof on its own: a per-input error
// can be swallowed (e.g. by an "already blessed" fallback), which would otherwise let a
// caller finalize a tx whose input was never actually spent.
func AllInputsSpent(tx *bt.Tx, spends []*Spend) bool {
	if len(spends) != len(tx.Inputs) {
		return false
	}

	for _, sp := range spends {
		if sp == nil || sp.Err != nil {
			return false
		}
	}

	return true
}

// CreatingRollForwarder is the store subset needed to roll a creating record forward:
// re-run the spend-only phase, clear the tentative flag, and reverse this call's spends
// when the record turns out not to be completable.
type CreatingRollForwarder interface {
	SpendReverser
	SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, []*Spend, error)
	FinalizeTransaction(ctx context.Context, tx *bt.Tx) error
}

// RollForwardCreating completes a create-first transition abandoned in the tentative
// "creating" state: it re-runs the input spends (idempotent for the same spender) and
// finalizes the record. It is FAIL-CLOSED — it returns an error unless every input was
// actually spent and the finalize succeeded, so a caller can never mistake a partial
// roll-forward for a completed one. It returns the spends so the caller can hand the tx
// to block assembly. It does NOT unlock the record: the caller unlocks only after block
// assembly holds the tx, the order loadUnminedTransactions enforces, so a child cannot
// be mined ahead of a parent that is in no template. Shared by the create-first
// SpendAndCreate on ErrTxExists, the setMined pre-flight, and the pruner sweeper so the
// "is this really spent?" rule has a single definition.
//
// The outcomes:
//   - A spend error, or a spend that did not cover every input: the error is returned and the
//     spends stay in place. The record is still creating and remains the recovery journal.
//   - FinalizeTransaction fails with ErrTxNotFound: the record is gone or rolling back, so it
//     will not be completed and the spends this call made are its own to remove. They are
//     reversed best effort under a detached context bounded by the store's rollback bound,
//     and the error is returned.
//   - FinalizeTransaction fails otherwise: the error is returned and the spends stay in place,
//     because the record is still creating.
//
// opts thread the caller's IgnoreFlags into the re-spend. A roll-forward is COMPLETING a
// spend the node already committed to, so recovery callers that are in block context may
// pass at least as permissive a set as the original spend (e.g. WithIgnoreLocked). A caller
// that is not in block context must not bypass the lock: a locked parent is the handoff
// barrier, and the roll-forward then fails with ErrTxLocked, which is a retry. The re-spend
// does not set RequireParentOutputs, because the record it finds exists by design and a
// parent is pruned only once every output is spent.
func RollForwardCreating(ctx context.Context, logger ulogger.Logger, s CreatingRollForwarder, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) ([]*Spend, error) {
	return rollForwardCreating(ctx, logger, s, tx, func() ([]*Spend, error) {
		_, spends, err := s.SpendAndCreate(ctx, tx, blockHeight, append([]CreateOption{WithSpendOnly()}, opts...)...)

		return spends, err
	})
}

// creatingFinisher is the store subset the roll-forward rule needs after the re-spend.
type creatingFinisher interface {
	SpendReverser
	FinalizeTransaction(ctx context.Context, tx *bt.Tx) error
}

// rollForwardCreating is the single definition of the roll-forward rule; spend is the
// re-spend of the caller's store.
func rollForwardCreating(ctx context.Context, logger ulogger.Logger, s creatingFinisher, tx *bt.Tx, spend func() ([]*Spend, error)) ([]*Spend, error) {
	spends, err := spend()
	if err != nil {
		return spends, err
	}

	if !AllInputsSpent(tx, spends) {
		return spends, errors.NewProcessingError("RollForwardCreating: re-spend of %s did not cover all inputs (%d spends for %d inputs)",
			tx.TxIDChainHash().String(), len(spends), len(tx.Inputs))
	}

	if err := s.FinalizeTransaction(ctx, tx); err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			rbCtx, cancel := rollbackContext(ctx, s)
			defer cancel()

			if unspendErr := unspendWithRetry(rbCtx, logger, s, succeededSpends(spends)); unspendErr != nil {
				logger.Errorf("RollForwardCreating: %s will not be completed and reversing its spends failed: %v", tx.TxIDChainHash().String(), unspendErr)
			}
		}

		return spends, err
	}

	return spends, nil
}

// unspendWithRetry reverses spends with up to 3 attempts and exponential backoff,
// ported from the validator's reverseSpends. The backoff aborts early when ctx
// is cancelled.
func unspendWithRetry(ctx context.Context, logger ulogger.Logger, s SpendReverser, spends []*Spend) error {
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

// finalizeWithRetry finalizes tx with up to 3 attempts and the same exponential backoff
// as unspendWithRetry. The backoff aborts early when ctx is cancelled.
func finalizeWithRetry(ctx context.Context, logger ulogger.Logger, s SequentialStore, tx *bt.Tx) error {
	var finErr error

	for retries := uint(0); retries < 3; retries++ {
		if finErr = s.FinalizeTransaction(ctx, tx); finErr == nil {
			return nil
		}

		if retries < 2 {
			backoff := time.Duration(1<<retries) * unspendRetryBackoffBase
			logger.Errorf("error finalizing %s, retrying in %s: %v", tx.TxIDChainHash().String(), backoff.String(), finErr)

			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return errors.NewProcessingError("error finalizing, context cancelled during retry backoff", errors.Join(finErr, ctx.Err()))
			case <-timer.C:
			}
		}
	}

	return finErr
}
