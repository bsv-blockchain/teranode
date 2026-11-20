package utxo

import (
	"bytes"
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

// MarkCreateOutcomeUnknown marks err as returned by a Create whose write may
// have committed: the store stopped waiting for a write that could still
// commit, as when its own wait for its batch ran out, or the write failed in a
// way that leaves its commit unknown, as when a reply was lost after the write
// was sent. A store marks an error only when the write may exist; a failure the
// store knows wrote nothing stays unmarked. SequentialSpendAndCreate keeps the
// spends for a marked error, as it does for a context error. It returns err.
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

// alreadyFinalKey is the error-data key MarkAlreadyFinal sets.
const alreadyFinalKey = "alreadyFinal"

// MarkAlreadyFinal marks err as returned by a FinalizeTransaction that found the record already
// final under another actor's write. The store records the caller of the write that clears the
// creating state, so exactly one caller of an attempt gets nil, and that caller owns what follows,
// the hand-off to block assembly and any unwind of it. A call that gets this error treats the
// transaction as stored and takes neither step. It returns err.
func MarkAlreadyFinal(err *errors.Error) *errors.Error {
	if err != nil {
		err.SetData(alreadyFinalKey, true)
	}

	return err
}

// IsAlreadyFinal reports whether an error carries the MarkAlreadyFinal marker anywhere in its chain.
func IsAlreadyFinal(err error) bool {
	return hasErrorData(err, alreadyFinalKey)
}

// spendLeaseLiveKey is the error-data key MarkSpendLeaseLive sets.
const spendLeaseLiveKey = "spendLeaseLive"

// MarkSpendLeaseLive marks err as returned by a BeginCreateFirstRollback that the store refused
// because a spender of the attempt holds a live lease. Nothing was written and the record is still
// creating. It returns err.
func MarkSpendLeaseLive(err *errors.Error) *errors.Error {
	if err != nil {
		err.SetData(spendLeaseLiveKey, true)
	}

	return err
}

// IsSpendLeaseLive reports whether an error carries the MarkSpendLeaseLive marker anywhere in its
// chain.
func IsSpendLeaseLive(err error) bool {
	return hasErrorData(err, spendLeaseLiveKey)
}

// rollBackAttemptKey is the error-data key MarkRollBackAttempt sets.
const rollBackAttemptKey = "rollBackAttempt"

// MarkRollBackAttempt marks err as returned by a create-first Create that published its master
// record and then failed. The record stays creating under attempt, because another actor may have
// read it and begun a roll-forward, and only the rollback protocol can undo it without leaving that
// actor's spends behind. The caller rolls the attempt back with RollBackCreating. It returns err.
func MarkRollBackAttempt(err *errors.Error, attempt []byte) *errors.Error {
	if err != nil {
		err.SetData(rollBackAttemptKey, attempt)
	}

	return err
}

// RollBackAttempt returns the token MarkRollBackAttempt set anywhere in the Unwrap chain of err, and
// nil when there is none.
func RollBackAttempt(err error) []byte {
	var attempt []byte

	anyErrorInChain(err, func(te *errors.Error) bool {
		attempt, _ = te.GetData(rollBackAttemptKey).([]byte)

		return len(attempt) > 0
	})

	return attempt
}

// hasErrorData reports whether any *errors.Error in the Unwrap chain of err carries key set to true.
func hasErrorData(err error, key string) bool {
	return anyErrorInChain(err, func(te *errors.Error) bool { return te.GetData(key) == true })
}

// anyErrorInChain reports whether match accepts any *errors.Error in the Unwrap chain of err. It
// stops at the first one match accepts.
func anyErrorInChain(err error, match func(*errors.Error) bool) bool {
	for cur := err; cur != nil; {
		if te, ok := cur.(*errors.Error); ok && match(te) {
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
	// FinalizeTransaction clears the tentative creating state of the attempt that carries the
	// token; used by the create-first path.
	FinalizeTransaction(ctx context.Context, tx *bt.Tx, attempt, lease []byte) error
	// DeleteComplete removes a record entirely; used by the create-first path to drop the
	// tentative record when its spends fail.
	DeleteComplete(ctx context.Context, hash *chainhash.Hash) error
	// BeginCreateFirstRollback claims the rollback of a still-creating record exclusively, so
	// no concurrent roll-forward can finalize it; see Store.BeginCreateFirstRollback.
	BeginCreateFirstRollback(ctx context.Context, hash *chainhash.Hash, attempt []byte) (bool, error)
	// EndCreateFirstRollback ends the rollback of the attempt that carries the token, leaving a
	// hold for the txid; see Store.EndCreateFirstRollback.
	EndCreateFirstRollback(ctx context.Context, hash *chainhash.Hash, attempt []byte) error
	// TakeRollbackHold and ReleaseRollbackHold bracket a reversal of spends after the mark; see
	// Store.TakeRollbackHold.
	TakeRollbackHold(ctx context.Context, hash *chainhash.Hash, attempt []byte) (bool, error)
	ReleaseRollbackHold(ctx context.Context, hash *chainhash.Hash, attempt []byte) error
	// TakeSpendLease, ReleaseSpendLease and SpendLeasesLive fence a spend of a creating record's
	// inputs against the rollback of the record; see Store.TakeSpendLease.
	TakeSpendLease(ctx context.Context, hash *chainhash.Hash, attempt []byte) ([]byte, bool, error)
	ReleaseSpendLease(ctx context.Context, hash *chainhash.Hash, attempt, lease []byte) error
	SpendLeasesLive(ctx context.Context, hash *chainhash.Hash, attempt []byte) (bool, error)
	// ClearCreateFirstHold removes an expired hold, or with adopt any hold that is not busy; see
	// Store.ClearCreateFirstHold.
	ClearCreateFirstHold(ctx context.Context, hash *chainhash.Hash, adopt bool) (bool, error)
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
// ServiceUnavailable error marked with MarkCreateOutcomeUnknown. A write that
// failed after it was sent can also have committed: an Aerospike write in
// doubt, timed out or cut by a network error, or a Postgres write whose
// connection was lost before the commit was confirmed. The stores mark those
// errors too. Unspending then would leave a record whose
// inputs are unspent, which a repeat never heals, since a repeat drops a
// transaction whose record exists. Keeping the spends leaves either a written
// transaction or a parent naming a spender with no record yet, and a repeat of
// the same SpendAndCreate heals the second: the spend is accepted as the same
// spend and the create runs.
//
// A create that fails with ErrTxExists on the record of a create-first rollback,
// a hold included, is not a duplicate. The call reads the record after
// ErrTxExists to tell the two apart; see rollbackInTheWay.resolve for what it
// does then. This is the path a node takes after utxostore_useCreateFirstOrder
// is turned off while such records remain.
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

	// The position is taken before the first spend, so a create that cannot get one fails having
	// spent nothing, rather than spending, failing and rolling the spends back.
	if !options.SpendOnly {
		if taker, ok := s.(LanePositionTaker); ok {
			position, takeErr := taker.TakeLanePosition(ctx, options)
			if takeErr != nil {
				return nil, nil, takeErr
			}

			if position != options.LanePosition {
				opts = WithTakenLanePosition(opts, position)
			}
		}
	}

	// Create-first ordering (create tentative → spend → finalize) applies only to the
	// combined path. Single-phase callers (CreateOnly / SpendOnly, e.g. coinbase, seeding,
	// reorg/conflict helpers) keep the direct behaviour — there is nothing to sequence.
	if createFirst && !options.CreateOnly && !options.SpendOnly {
		blockContext := options.IgnoreFlags.IgnoreLocked || len(options.MinedBlockInfos) > 0

		return sequentialCreateFirst(ctx, logger, s, tx, blockHeight, options.IgnoreFlags, blockContext, opts...)
	}

	blockContext := options.IgnoreFlags.IgnoreLocked || len(options.MinedBlockInfos) > 0
	inTheWay := rollbackInTheWay{deadline: time.Now().Add(2 * rollbackBound(s))}

	for {
		var spends []*Spend

		if !options.CreateOnly {
			spends, err = s.Spend(ctx, tx, blockHeight, options.IgnoreFlags)
			if err != nil {
				inTheWay.reverseCarried(ctx, logger, s, tx)

				return nil, spends, err
			}

			if options.SpendOnly {
				return nil, spends, nil
			}
		}

		md, err := s.Create(ctx, tx, blockHeight, opts...)
		if err == nil {
			return md, spends, nil
		}

		if errors.Is(err, errors.ErrTxExists) {
			again, keptSpends, resolveErr := inTheWay.resolve(ctx, logger, s, tx, blockContext, options.CreateOnly, spends, err)
			if again {
				continue
			}

			return nil, keptSpends, resolveErr
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
}

// rollbackInTheWay is the state of one spend-first SpendAndCreate across the rounds it needs to
// get past the record of a create-first rollback at its txid.
type rollbackInTheWay struct {
	// deadline ends the wait for the hold of the rollback. It is twice the rollback bound: the
	// owner of a rollback reverses and cleans up within one bound, and a holder that crashed is
	// treated as gone after two.
	deadline time.Time
	// carried holds the spends of an earlier round that may still be in effect: the round ended
	// with the record out of the way, and the call went around to spend and create again.
	carried []*Spend
}

// besideRollback is what a spend-first call found, or did, at the record of a rollback.
type besideRollback int

const (
	// rollbackUndecided: the record is still that of a rollback and the call could not act.
	rollbackUndecided besideRollback = iota
	// rollbackGone: there is no record at the txid any more.
	rollbackGone
	// rollbackReplaced: the record is not that of a rollback: a later attempt wrote it.
	rollbackReplaced
	// rollbackHoldCleared: the call removed the hold.
	rollbackHoldCleared
	// rollbackReversed: the call reversed its spends under the hold of the attempt.
	rollbackReversed
)

// resolve handles the ErrTxExists of a spend-first create. It reports again when the caller must
// run its spend and its create once more, and otherwise the spends still in effect and the error
// to return.
//
// A record that is not rolling back is a duplicate: existsErr is returned with the spends, as it
// is when the record cannot be read. A record that is rolling back, a hold included, is not a
// stored transaction, and its rollback reverses the inputs of the txid. Spends are the txid and
// the input index, so that reversal also clears the spends this call made, and this call's own
// reversal would clear the spends of a create that adopted the hold. Two rules follow.
//
//   - The call never creates over spends it made before the record was out of the way. After it
//     has cleared a hold, at once in block context and otherwise only an expired one, or has seen
//     the record go, it spends again and then creates. The hold is written after the reversal of
//     its attempt, so no reversal is left to run once it is gone.
//   - The call reverses its spends only while it holds the hold of the attempt
//     (TakeRollbackHold), which no create can adopt and no other actor can reverse under. It then
//     returns a retryable error and no spends. While the take is refused, because the owner of the
//     rollback is busy, the call waits at the record and does not spend again (settle).
//
// A create-only call made no spend: it creates again over a hold it may clear and otherwise
// returns existsErr.
func (r *rollbackInTheWay) resolve(ctx context.Context, logger ulogger.Logger, s SequentialStore, tx *bt.Tx, blockContext, createOnly bool, spends []*Spend, existsErr error) (bool, []*Spend, error) {
	hash := tx.TxIDChainHash()

	existing := &meta.Data{}
	if getErr := s.GetMeta(ctx, hash, existing); getErr != nil || !existing.RollingBack {
		return false, spends, existsErr
	}

	clearHold := func() bool {
		cleared, clearErr := s.ClearCreateFirstHold(ctx, hash, blockContext)

		return clearErr == nil && cleared
	}

	// Past the deadline the call goes around no more: it neither clears nor takes a hold.
	expired := !time.Now().Before(r.deadline)

	if createOnly {
		if existing.Hold && !expired && clearHold() {
			return true, nil, nil
		}

		return false, spends, existsErr
	}

	if expired {
		return false, spends, errors.NewProcessingError("SpendAndCreate: tx %s is being rolled back by a create-first attempt and the spends of this call could not be reversed under its hold within twice the rollback bound; they are left in place", hash.String())
	}

	outcome, err := r.settle(ctx, logger, s, hash, spends, clearHold)

	switch outcome {
	case rollbackGone, rollbackHoldCleared:
		r.carried = spends

		return true, nil, nil
	case rollbackReplaced:
		return false, spends, existsErr
	case rollbackReversed:
		return false, nil, errors.NewProcessingError("SpendAndCreate: tx %s is held by a rolled back create-first attempt; retry after the hold expires", hash.String())
	default:
		return false, spends, errors.NewProcessingError("SpendAndCreate: tx %s is being rolled back by a create-first attempt and the spends of this call could not be reversed under its hold; they are left in place", hash.String(), err)
	}
}

// reverseCarried runs when the spend of a later round failed. The spends of the earlier round are
// then spends of a txid that this call will not create, and the store's own rollback covers only
// the inputs that the failed spend took. They are reversed when no record owns them: at once when
// the txid has no record, and under the hold when the record of a rollback is back at it. A
// record of a later attempt owns them, and they stay.
func (r *rollbackInTheWay) reverseCarried(ctx context.Context, logger ulogger.Logger, s SequentialStore, tx *bt.Tx) {
	if len(r.carried) == 0 {
		return
	}

	hash := tx.TxIDChainHash()

	outcome, err := r.settle(ctx, logger, s, hash, r.carried, nil)
	if outcome == rollbackGone {
		outcome, err = rollbackReversed, unspendDetached(ctx, logger, tolerantReverser{s}, r.carried)
	}

	if outcome == rollbackUndecided || err != nil {
		logger.Errorf("SpendAndCreate: the spends of an earlier round for %s could not be reversed after its spend failed, they are left in place: %v", hash.String(), err)
	}
}

// settle waits at the record of a rollback until the call can act on it, and acts:
//   - the record is gone, or is not that of a rollback any more: nothing is done;
//   - the record is a hold that clear removes (clear may be nil): nothing else is done;
//   - the hold of the attempt is free: the call takes it, reverses spends under it and releases
//     it. The reversal tolerates an input that is not spent by this txid, because the owner of the
//     rollback may have reversed it already.
//
// While the hold is taken by the owner of the rollback, its cleanup or another holder, the call
// looks again, at a slower pace than a lease poll, until the deadline. Each of those ends within
// the rollback bound, and then the record is a hold that is free, or it is gone.
func (r *rollbackInTheWay) settle(ctx context.Context, logger ulogger.Logger, s SequentialStore, hash *chainhash.Hash, spends []*Spend, clear func() bool) (besideRollback, error) {
	wait := time.Duration(0)

	for {
		existing := &meta.Data{}

		getErr := s.GetMeta(ctx, hash, existing)

		switch {
		case errors.Is(getErr, errors.ErrTxNotFound):
			return rollbackGone, nil
		case getErr != nil:
			return rollbackUndecided, getErr
		case !existing.RollingBack:
			return rollbackReplaced, nil
		}

		if clear != nil && existing.Hold && clear() {
			return rollbackHoldCleared, nil
		}

		took, takeErr := s.TakeRollbackHold(ctx, hash, existing.Attempt)
		if takeErr != nil {
			return rollbackUndecided, takeErr
		}

		if took {
			unspendErr := unspendDetached(ctx, logger, tolerantReverser{s}, spends)

			releaseRollbackHold(ctx, logger, s, hash, existing.Attempt)

			if unspendErr != nil {
				return rollbackUndecided, unspendErr
			}

			return rollbackReversed, nil
		}

		if !time.Now().Before(r.deadline) {
			return rollbackUndecided, errors.NewProcessingError("the hold of %s was not free within twice the rollback bound", hash.String())
		}

		wait = min(max(wait*2, leasePollMin), holdPollMax)

		timer := time.NewTimer(wait)

		select {
		case <-ctx.Done():
			timer.Stop()

			return rollbackUndecided, ctx.Err()
		case <-timer.C:
		}
	}
}

// rollbackBound returns the store's SpendRollbackTimeout, or the default when it names none.
func rollbackBound(s any) time.Duration {
	if b, ok := s.(spendRollbackBounded); ok && b.SpendRollbackTimeout() > 0 {
		return b.SpendRollbackTimeout()
	}

	return defaultSequentialRollbackTimeout
}

// rollbackContext returns a context that ignores ctx's cancellation and deadline
// and carries the rollback's own bound instead.
func rollbackContext(ctx context.Context, s any) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), rollbackBound(s))
}

// boundedContext returns a context that ends with ctx or after the rollback bound, whichever
// comes first. A roll-forward runs under it, so that its spend and finalize cannot outlast the
// hold a rollback leaves for the txid.
func boundedContext(ctx context.Context, s any) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, rollbackBound(s))
}

// unspendDetached runs unspendWithRetry under rollbackContext.
func unspendDetached(ctx context.Context, logger ulogger.Logger, s SpendReverser, spends []*Spend) error {
	rbCtx, cancel := rollbackContext(ctx, s)
	defer cancel()

	return unspendWithRetry(rbCtx, logger, s, spends)
}

// finalizeDetached runs finalizeWithRetry under rollbackContext.
func finalizeDetached(ctx context.Context, logger ulogger.Logger, s SequentialStore, tx *bt.Tx, attempt, lease []byte) error {
	fCtx, cancel := rollbackContext(ctx, s)
	defer cancel()

	return finalizeWithRetry(fCtx, logger, s, tx, attempt, lease)
}

// SpendReverser is the store subset that reverses spends.
type SpendReverser interface {
	Unspend(ctx context.Context, spends []*Spend, flagAsLocked ...bool) error
}

// CreatingRollBacker is the store subset RollBackCreating needs.
type CreatingRollBacker interface {
	SpendReverser
	BeginCreateFirstRollback(ctx context.Context, hash *chainhash.Hash, attempt []byte) (bool, error)
	EndCreateFirstRollback(ctx context.Context, hash *chainhash.Hash, attempt []byte) error
	ReleaseRollbackHold(ctx context.Context, hash *chainhash.Hash, attempt []byte) error
}

// tolerantReverser unspends through UnspendTolerantOfMissingParents, so a parent that is
// already gone, a vout that does not exist or a UTXO hash that does not match does not fail
// a rollback.
type tolerantReverser struct {
	SpendReverser
}

func (r tolerantReverser) Unspend(ctx context.Context, spends []*Spend, flagAsLocked ...bool) error {
	return UnspendTolerantOfMissingParents(ctx, r.SpendReverser, spends)
}

// RollBackCreating undoes a create-first transition whose spend failed: it marks the record
// rolling back, which the store allows only while no spender of the attempt holds a live lease,
// unspends every input of tx, then ends the rollback, which leaves a hold for the txid. attempt is the token of the attempt
// being rolled back, read from the record or returned by its create. A caller that holds a spend
// lease of the attempt releases it before this call. It runs under one detached context bounded
// like the spend-first rollback (SpendRollbackTimeout).
//
// The unspend covers the whole input set, not only the spends that succeeded in the caller's
// pass. An input that an earlier attempt spent may fail in this pass for another reason, for
// example a locked parent, and a reversal limited to this pass would leave it spent by the
// rolled back record. The store's ownership check makes an unspend of an input that tx does not
// own a no-op, so a rival's spend is never cleared.
//
// The marker and the leases are what make the order safe. Every actor that spends the inputs of a
// creating record holds a lease on it: the attempt that created it from its create on, and a
// roll-forward from TakeSpendLease on, which refuses a record that carries the marker. The marker
// is claimed exclusively and only for the attempt that carries the token, and the store refuses
// the claim while a lease is live. Once it is set no new spender can start and nothing can
// finalize the record. A spend of the attempt therefore cannot land after the reversal, the hold
// that ends the rollback needs no transaction data to undo one, and a lease holder that is
// clearing the creating state of the pagination children is never overtaken by a rollback that
// deletes them.
//
// The call that sets the marker also takes the hold of the attempt, so the owner holds it for its
// whole unspend: a pruner sweep, which takes the hold before it reverses spends, finds the record
// busy and does nothing, and a block-context create cannot adopt the record while the unspend is
// in flight. The owner releases the hold when the unspend is done and before it ends the
// rollback. The end of the rollback does not delete the master record. It turns it into a hold
// that keeps the txid taken until it expires, so no new attempt can create records while the
// rollback removes the children and blobs of this one.
// The store's own partial-spend rollback cannot do this work, because it probes for the
// spender record and the tentative record makes that probe succeed.
//
//   - The marker call fails: the record is left creating and nothing is reversed. The error
//     is returned, and the sweep re-spends the record and either finalizes or rolls it back.
//   - The record is absent, already final, owned by another attempt or already rolling back
//     under another caller: a concurrent roll-forward finalized it and relies on the spends, or
//     the other caller owns the reversal, so nothing is reversed. The return is nil.
//   - A lease stays live past the wait (spendLeaseWait, the longest one spend may run), because
//     its spender returned with an unknown outcome or crashed: the claim stays refused, nothing is
//     written or reversed and the error is returned. The record is left creating. A roll-forward
//     completes it, or the sweep rolls it back once the lease is stale.
//   - The reversal fails: the record is left rolling back and the error is returned. Nobody
//     can finalize it. The hold is released best effort, so the sweep can take it at once, and
//     otherwise once it has aged out, and finishes the rollback.
//   - The release of the hold fails: the error is returned and the record is left rolling back
//     and busy. The sweep takes it once the busy marker has aged out.
//   - The end of the rollback fails: same, the record is left rolling back for the sweep.
//
// Residual: an unspend still cannot tell two attempts of the same txid apart, because
// SpendingData is the txid and the input index. The token and the hold stop every actor that
// proves it still owns the txid before it unspends. A rollback owner that unspends without that
// proof and stalls past the hold, and past CreatingTxSweepMinAgeBlocks, which must outlast
// utxostore_spendRollbackTimeout in wall-clock time, can still clear the spends of a fresh create
// of the same txid. That is the attempt-identity gap tracked in issue 1291.
func RollBackCreating(ctx context.Context, logger ulogger.Logger, s CreatingRollBacker, tx *bt.Tx, attempt []byte) error {
	rbCtx, cancel := rollbackContext(ctx, s)
	defer cancel()

	hash := tx.TxIDChainHash()

	began, err := beginAfterSpendLeases(rbCtx, s, hash, attempt)
	if err != nil {
		logger.Errorf("SpendAndCreate create-first: marking %s as rolling back failed, tx left creating for recovery: %v", hash.String(), err)

		return err
	}

	if !began {
		logger.Warnf("SpendAndCreate create-first: %s is not rolled back by this call: it is final, gone, owned by another attempt or already rolling back under another caller, spends left in place", hash.String())

		return nil
	}

	inputs, err := GetSpends(tx)
	if err != nil {
		logger.Errorf("SpendAndCreate create-first: building the input spends failed for %s, tx left rolling back for the sweep: %v", hash.String(), err)
		releaseRollbackHold(ctx, logger, s, hash, attempt)

		return err
	}

	if len(inputs) > 0 {
		if err := unspendWithRetry(rbCtx, logger, tolerantReverser{s}, inputs); err != nil {
			logger.Errorf("SpendAndCreate create-first: reversing spends failed for %s, tx left rolling back for the sweep: %v", hash.String(), err)
			releaseRollbackHold(ctx, logger, s, hash, attempt)

			return err
		}
	}

	if err := releaseRollbackHoldStrict(ctx, s, hash, attempt); err != nil {
		logger.Errorf("SpendAndCreate create-first: releasing the hold of %s failed, tx left rolling back for the sweep: %v", hash.String(), err)

		return err
	}

	if err := s.EndCreateFirstRollback(rbCtx, hash, attempt); err != nil {
		logger.Errorf("SpendAndCreate create-first: ending the rollback failed for %s, tx left rolling back for the sweep: %v", hash.String(), err)

		return err
	}

	return nil
}

// CreatingRollbackFinisher is the store subset FinishRollBackCreating needs.
type CreatingRollbackFinisher interface {
	SpendReverser
	TakeRollbackHold(ctx context.Context, hash *chainhash.Hash, attempt []byte) (bool, error)
	ReleaseRollbackHold(ctx context.Context, hash *chainhash.Hash, attempt []byte) error
	EndCreateFirstRollback(ctx context.Context, hash *chainhash.Hash, attempt []byte) error
	SpendLeasesLive(ctx context.Context, hash *chainhash.Hash, attempt []byte) (bool, error)
}

// FinishRollBackCreating completes a rollback that its owner left marked: the owner crashed, or
// gave up because a spend lease stayed live. reversed says that the record already carries the
// reversed marker, and the call then only ends the rollback. Otherwise it takes the hold of the
// attempt, which is exclusive and proves that the record still belongs to that attempt. With the
// hold it checks that no spend lease is live, unspends every input of tx under the rollback bound,
// skipping inputs the tx can never have spent, releases the hold and ends the rollback, which
// leaves a hold. Every step is idempotent.
//
// It reports true only when it ended the rollback. It reports false with no error, and reverses
// nothing, when the take fails (the record is gone, owned by another attempt, busy or already
// reversed) or a lease is still live: a spend of the attempt may still land, and the caller tries
// again later. The pruner sweep calls it, and so does a create-first SpendAndCreate that finds the
// record in its way.
func FinishRollBackCreating(ctx context.Context, logger ulogger.Logger, s CreatingRollbackFinisher, tx *bt.Tx, attempt []byte, reversed bool) (bool, error) {
	hash := tx.TxIDChainHash()

	if !reversed {
		took, err := s.TakeRollbackHold(ctx, hash, attempt)
		if err != nil || !took {
			return false, err
		}

		live, err := s.SpendLeasesLive(ctx, hash, attempt)
		if err != nil || live {
			releaseRollbackHold(ctx, logger, s, hash, attempt)

			return false, err
		}

		inputs, err := GetSpends(tx)
		if err == nil && len(inputs) > 0 {
			rbCtx, cancel := rollbackContext(ctx, s)
			err = UnspendTolerantOfMissingParents(rbCtx, s, inputs)

			cancel()
		}

		if err != nil {
			releaseRollbackHold(ctx, logger, s, hash, attempt)

			return false, err
		}

		if err = releaseRollbackHoldStrict(ctx, s, hash, attempt); err != nil {
			return false, err
		}
	}

	if err := s.EndCreateFirstRollback(ctx, hash, attempt); err != nil {
		return false, err
	}

	return true, nil
}

// leasePollMin and leasePollMax bound the wait between two tries of a write that a live spend
// lease, or a busy holder, refuses.
const (
	leasePollMin = 10 * time.Millisecond
	leasePollMax = 500 * time.Millisecond
)

// holdPollMax bounds the wait between two looks of a spend-first call at the hold of a rollback
// that is busy.
const holdPollMax = 5 * time.Second

// defaultSpendLeaseWait is how long a rollback waits for the spenders of its attempt when the store
// names no spend wait: the settings default of utxostore_spendWaitTimeout.
const defaultSpendLeaseWait = 30 * time.Second

// spendWaitBounded is implemented by a store that names the longest one spend may run,
// utxostore_spendWaitTimeout.
type spendWaitBounded interface {
	SpendWaitTimeout() time.Duration
}

// spendLeaseWait returns how long a rollback waits for the spenders of its attempt before it
// gives up: the longest one healthy spend may run. A spender that is still live after that either
// returned with an unknown outcome or crashed. It releases nothing, and its lease is live for
// longer than a rollback may run, so a longer wait would only hold the caller.
func spendLeaseWait(s any) time.Duration {
	if b, ok := s.(spendWaitBounded); ok && b.SpendWaitTimeout() > 0 {
		return b.SpendWaitTimeout()
	}

	return defaultSpendLeaseWait
}

// rollbackBeginner is the store subset beginAfterSpendLeases needs.
type rollbackBeginner interface {
	BeginCreateFirstRollback(ctx context.Context, hash *chainhash.Hash, attempt []byte) (bool, error)
}

// beginAfterSpendLeases claims the rollback of the attempt. The store refuses the claim while a
// spender of the attempt holds a live lease (IsSpendLeaseLive), and the call then tries again until
// the claim is decided, spendLeaseWait has passed or ctx ends. A claim that is still refused then
// returns the store's error: nothing was written, the record is still creating, and a roll-forward
// or the sweep completes or rolls back the attempt later.
func beginAfterSpendLeases(ctx context.Context, s rollbackBeginner, hash *chainhash.Hash, attempt []byte) (bool, error) {
	waitCtx, cancel := context.WithTimeout(ctx, spendLeaseWait(s))
	defer cancel()

	wait := leasePollMin

	for {
		began, err := s.BeginCreateFirstRollback(ctx, hash, attempt)
		if err == nil || !IsSpendLeaseLive(err) {
			return began, err
		}

		timer := time.NewTimer(wait)

		select {
		case <-waitCtx.Done():
			timer.Stop()

			return false, err
		case <-timer.C:
		}

		wait = min(wait*2, leasePollMax)
	}
}

// rollbackHoldReleaser is the store subset that releases the hold of an attempt.
type rollbackHoldReleaser interface {
	ReleaseRollbackHold(ctx context.Context, hash *chainhash.Hash, attempt []byte) error
}

// releaseRollbackHoldStrict releases the hold that BeginCreateFirstRollback set, under a fresh
// context bounded by the store's rollback bound, so a bound that the reversal used up does not
// stop the release.
func releaseRollbackHoldStrict(ctx context.Context, s rollbackHoldReleaser, hash *chainhash.Hash, attempt []byte) error {
	relCtx, cancel := rollbackContext(ctx, s)
	defer cancel()

	return s.ReleaseRollbackHold(relCtx, hash, attempt)
}

// releaseRollbackHold releases the hold best effort and logs a failure: the record stays rolling
// back and the sweep takes it once the hold has aged out.
func releaseRollbackHold(ctx context.Context, logger ulogger.Logger, s rollbackHoldReleaser, hash *chainhash.Hash, attempt []byte) {
	if err := releaseRollbackHoldStrict(ctx, s, hash, attempt); err != nil {
		logger.Errorf("SpendAndCreate create-first: releasing the hold of %s failed, it expires on its own: %v", hash.String(), err)
	}
}

// sequentialCreateFirst persists a tx create-first: the record is created in the
// tentative "creating" state, the inputs are spent, then the flag is cleared. Because
// the record exists (spend-gated) before its inputs are spent, a crash in the
// spend→finalize window leaves an inspectable, recoverable record rather than a
// dangling spender reference. This addresses the spend-before-create crash window (one
// mechanism related to #1214); it does not by itself satisfy #1214's delete-path audit.
//
// The create writes this call's spend lease on the record, under the token the create returns.
// The lease names the time by which the spend must end, the store's rollback bound after the
// create, and the spend runs under a context that ends then. A rollback of the record waits for
// the lease before it reverses a spend; see RollBackCreating.
//
// Failure handling:
//   - Create returns ErrTxExists: the record is read back. A final record is an ordinary
//     duplicate and ErrTxExists is returned unchanged. A rolling-back record is a transient
//     ProcessingError, because its owner ends the rollback within the rollback bound or the
//     sweep does, and the hold it leaves expires. A rollback that its owner left open is
//     finished here when no holder is busy and no spend lease is live
//     (FinishRollBackCreating). Before it returns that error the call asks
//     the store to clear a hold: an expired one goes, and in block context (the caller ignores
//     locks or the create carries mined block info) a hold goes unless a holder is busy, so a
//     block that contains a transaction that lost a double-spend locally does not wait out the
//     hold. When the hold is cleared the create runs once more. A still-creating record is
//     rolled forward here, by the rule RollForwardCreating defines, for the attempt that wrote
//     it, under the caller's ignore flags and without RequireParentOutputs: that record exists
//     by design, and a parent is pruned only once every output is spent. On success the call
//     returns the spends and ErrTxExists marked with MarkRolledForward. On failure it returns
//     the roll-forward's error and its spends, and the record is left for the sweep.
//   - Create fails after it published the master record (MarkRollBackAttempt): the lease is
//     released and the attempt is rolled back.
//   - The first spend sets RequireParentOutputs. The record it would otherwise find is this
//     call's own tentative one, which proves nothing about a missing parent.
//   - Spend fails with no per-input result, with a context error or with ErrServiceUnavailable:
//     the spend batch may still commit after the caller returned, so nothing is reversed or
//     deleted and the lease is not released. The creating record stays as the recovery journal
//     and the sweep re-spends idempotently, or rolls back once the lease is stale.
//   - Spend fails otherwise: the lease is released and RollBackCreating marks the record rolling
//     back, unspends every input of the tx and ends the rollback, which leaves a hold for the
//     txid, under a detached context bounded like the spend-first rollback. Without the rollback
//     a partially spent tx would leave its free inputs naming a record that is later pruned (the
//     #1214 shape), and every rejected double-spend would leave a durable record. The per-input
//     spends and the error are returned, with nil meta, so the caller can classify them. If the
//     rollback fails, the returned error wraps both failures.
//   - FinalizeTransaction fails after successful spends: it is retried up to 3 times with
//     exponential backoff under a detached context. It clears only the record this call created,
//     identified by the token Create returned. If every attempt fails, return an
//     error. The inputs are durably spent but the tentative flag could not be cleared, so
//     the outputs are still unspendable; reporting success would make the validator
//     announce the tx to block assembly (and unlock it) while its children fail with an
//     unhandled ErrTxCreating. The record stays creating and is recovered by the
//     ErrTxExists roll-forward on re-encounter, the pruner sweep, or setMined. The lease is
//     released, because the spends it covers are complete.
func sequentialCreateFirst(ctx context.Context, logger ulogger.Logger, s SequentialStore,
	tx *bt.Tx, blockHeight uint32, ignoreFlags IgnoreFlags, blockContext bool, opts ...CreateOption) (*meta.Data, []*Spend, error) {
	// Force WithCreating(true) last so it cannot be overridden by a caller option (belt and
	// suspenders alongside the caller-supplied WithCreating rejection in SequentialSpendAndCreate).
	createOpts := append(append([]CreateOption{}, opts...), WithCreating(true))

	hash := tx.TxIDChainHash()

	var (
		md            *meta.Data
		spendDeadline time.Time
	)

	for round := 0; ; round++ {
		// Read before the create, so the time the store writes on the lease, which is in whole
		// seconds, is at most a second earlier. A lease is live for a further rollback bound.
		spendDeadline = time.Now().Add(rollbackBound(s))

		created, err := s.Create(ctx, tx, blockHeight, createOpts...)
		if err == nil {
			md = created

			break
		}

		if attempt := RollBackAttempt(err); attempt != nil {
			releaseSpendLease(ctx, logger, s, hash, attempt, attempt)

			if rollbackErr := RollBackCreating(ctx, logger, s, tx, attempt); rollbackErr != nil {
				return nil, nil, errors.NewProcessingError("SpendAndCreate create-first: rolling back the failed create of %s failed, tx left for the sweep: %v", hash.String(), rollbackErr, err)
			}

			return nil, nil, err
		}

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
			if round == 0 {
				// A rollback that its owner left open, because it crashed or a spend lease stayed
				// live, would otherwise wait for the pruner sweep, and the sweep waits for the chain
				// to advance. A block that contains the transaction must not depend on that, so the
				// call finishes the rollback when it can. A failure changes nothing: the transient
				// error below is returned and the sweep remains.
				if !existing.Hold {
					if _, finishErr := FinishRollBackCreating(ctx, logger, s, tx, existing.Attempt, existing.Reversed); finishErr != nil {
						logger.Warnf("SpendAndCreate create-first: finishing the open rollback of %s failed, left for the sweep: %v", hash.String(), finishErr)
					}
				}

				cleared, clearErr := s.ClearCreateFirstHold(ctx, hash, blockContext)
				if clearErr != nil {
					return nil, nil, errors.NewProcessingError("SpendAndCreate create-first: tx %s is rolling back and its hold could not be cleared", hash.String(), clearErr)
				}

				if cleared {
					continue
				}
			}

			return nil, nil, errors.NewProcessingError("SpendAndCreate create-first: tx %s is rolling back; retry after it completes", hash.String())
		}

		spends, completed, rfErr := rollForwardCreating(ctx, logger, s, tx, existing.Attempt, func(rfCtx context.Context) ([]*Spend, error) {
			return s.Spend(rfCtx, tx, blockHeight, ignoreFlags)
		})
		if rfErr != nil {
			return nil, spends, rfErr
		}

		if !completed {
			// Another actor finalized the record after the read above: an ordinary duplicate.
			return nil, nil, err
		}

		return nil, spends, MarkRolledForward(errors.NewTxExistsError("SpendAndCreate create-first: tx %s was creating and has been rolled forward", hash.String()))
	}

	freshFlags := ignoreFlags
	freshFlags.RequireParentOutputs = true

	spendCtx, cancelSpend := context.WithDeadline(ctx, spendDeadline)
	spends, err := s.Spend(spendCtx, tx, blockHeight, freshFlags)

	cancelSpend()

	if err != nil {
		if spendOutcomeUnknown(spends, err) {
			return nil, spends, err
		}

		releaseSpendLease(ctx, logger, s, hash, md.Attempt, md.Attempt)

		if rollbackErr := RollBackCreating(ctx, logger, s, tx, md.Attempt); rollbackErr != nil {
			return nil, spends, errors.NewProcessingError("SpendAndCreate: error reversing utxo spends: %v", rollbackErr, err)
		}

		return nil, spends, err
	}

	// The lease of the call that creates a record is its attempt token.
	if finErr := finalizeDetached(ctx, logger, s, tx, md.Attempt, md.Attempt); finErr != nil {
		if IsAlreadyFinal(finErr) {
			// A roll-forward of this attempt cleared the creating state. That actor owns the
			// hand-off of the transaction, so this call reports an ordinary duplicate. Its lease is
			// still on the record, because a finalize removes only the lease of its caller.
			releaseSpendLease(ctx, logger, s, hash, md.Attempt, md.Attempt)

			return nil, spends, errors.NewTxExistsError("SpendAndCreate create-first: tx %s was completed by a roll-forward of this attempt", hash.String(), finErr)
		}

		releaseSpendLease(ctx, logger, s, hash, md.Attempt, md.Attempt)

		logger.Errorf("SpendAndCreate create-first: FinalizeTransaction failed for %s, tx left creating for recovery: %v", hash.String(), finErr)
		prometheusCreateFirstFinalizeFailed.Inc()

		return md, spends, errors.NewProcessingError("SpendAndCreate create-first: finalize failed for %s, tx left creating for recovery", hash.String(), finErr)
	}

	md.Creating = false

	return md, spends, nil
}

// spendOutcomeUnknown reports whether a failed spend may still commit after it returned: it gave
// no per-input result, its context ended, the store stopped waiting for its batch, the error of
// one input is not a verdict of the store on that input, or no input carries an error and the
// failure itself is not a verdict. Such a spend is never reversed by its
// caller and its lease is never released, so the lease stays live for as long as the store may
// still apply the write.
func spendOutcomeUnknown(spends []*Spend, err error) bool {
	if spends == nil || errors.IsContextError(err) || errors.Is(err, errors.ErrServiceUnavailable) {
		return true
	}

	verdicts := 0

	for _, sp := range spends {
		if sp == nil || sp.Err == nil {
			continue
		}

		if !isSpendVerdict(sp.Err) && !isInvalidSpend(sp.Err) {
			return true
		}

		verdicts++
	}

	// A failure that names no input is known only when the error itself is a verdict.
	return verdicts == 0 && !isSpendVerdict(err)
}

// isSpendVerdict reports whether the error of one input is the store's answer about that input:
// the write was evaluated and refused, so it cannot still land. Any other error, a storage fault
// or a timeout among them, may belong to a write that is still in doubt.
func isSpendVerdict(err error) bool {
	return errors.Is(err, errors.ErrSpent) ||
		errors.Is(err, errors.ErrTxNotFound) ||
		errors.Is(err, errors.ErrNotFound) ||
		errors.Is(err, errors.ErrTxConflicting) ||
		errors.Is(err, errors.ErrTxLocked) ||
		errors.Is(err, errors.ErrTxCreating) ||
		errors.Is(err, errors.ErrFrozen) ||
		errors.Is(err, errors.ErrUtxoHashMismatch) ||
		errors.Is(err, errors.ErrUtxoInvalidSize) ||
		errors.Is(err, errors.ErrTxCoinbaseImmature)
}

// isInvalidSpend reports whether the error of one input is the store's refusal of an invalid
// spend: a UTXO error that wraps no cause. It is a verdict on that input. It is not asked of the
// error of the whole spend, which is a UTXO error whatever made the spend fail, and a UTXO error
// that wraps a cause is judged by that cause.
func isInvalidSpend(err error) bool {
	var e *errors.Error

	return errors.As(err, &e) && e.Code() == errors.ERR_UTXO_ERROR && e.WrappedErr() == nil
}

// spendLeaseReleaser is the store subset releaseSpendLease needs.
type spendLeaseReleaser interface {
	ReleaseSpendLease(ctx context.Context, hash *chainhash.Hash, attempt, lease []byte) error
}

// releaseSpendLease releases a spend lease under a detached context bounded by the rollback bound,
// so that a caller whose own context has ended still releases it. A release that fails is logged:
// the lease goes stale on its own, and a rollback of the record waits for that.
func releaseSpendLease(ctx context.Context, logger ulogger.Logger, s spendLeaseReleaser, hash *chainhash.Hash, attempt, lease []byte) {
	relCtx, cancel := rollbackContext(ctx, s)
	defer cancel()

	if err := s.ReleaseSpendLease(relCtx, hash, attempt, lease); err != nil {
		logger.Errorf("SpendAndCreate create-first: releasing the spend lease of %s failed, it expires on its own: %v", hash.String(), err)
	}
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

// CreatingRollForwarder is the store subset needed to roll a creating record forward: take a
// spend lease, re-run the spend-only phase and clear the tentative flag.
type CreatingRollForwarder interface {
	creatingFinisher
	SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, []*Spend, error)
}

// RollForwardCreating completes a create-first transition abandoned in the tentative
// "creating" state: it re-runs the input spends (idempotent for the same spender) and
// finalizes the record. It is FAIL-CLOSED — it returns an error unless every input was
// actually spent and the finalize succeeded, so a caller can never mistake a partial
// roll-forward for a completed one. It returns the spends, and completed reports whether this call
// cleared the creating state. Exactly one finalize of an attempt does, and only that caller hands
// the tx to block assembly: with a nil error and completed false, another actor completed the
// record and owns its hand-off. It does NOT unlock the record: the caller unlocks only after block
// assembly holds the tx, the order loadUnminedTransactions enforces, so a child cannot
// be mined ahead of a parent that is in no template. Shared by the create-first
// SpendAndCreate on ErrTxExists, the setMined pre-flight, and the pruner sweeper so the
// "is this really spent?" rule has a single definition.
//
// attempt is the token of the attempt being recovered, read from the record by the caller. The
// lease and the finalize touch only a record that carries it.
//
// Before it spends, the roll-forward takes a spend lease on the record. The store grants it with
// one write that is refused when the record is rolling back, owned by another attempt or absent,
// so no input is spent for a record whose rollback has begun. A rollback that begins after the
// lease is granted waits for the lease before it reverses a spend, so every spend this call makes
// lands before that reversal and is undone by it. The roll-forward itself reverses nothing.
//
// The whole roll-forward, the lease, the spend and the finalize, runs under a context bounded by
// the store's rollback bound. That is the time the lease names, so a spend never runs past its
// lease.
//
// The outcomes:
//   - The lease is refused because the record is rolling back or owned by another attempt (a
//     retryable error) or absent (ErrTxNotFound): the error is returned and nothing is spent.
//   - The lease is refused because the record is already final, or the finalize finds it final:
//     another actor completed it. The inputs of a final record are spent, so the call returns
//     their spends, completed false and no error.
//   - A spend error, or a spend that did not cover every input: the lease is released, the error
//     is returned and the spends stay in place. The record is still creating and remains the
//     recovery journal, or it is rolling back and its owner reverses the spends. A spend whose
//     outcome is unknown keeps its lease, which goes stale on its own.
//   - FinalizeTransaction fails with ErrTxNotFound: the record is gone, rolling back or owned by
//     another attempt. The lease is released and the error is returned. The owner of the
//     rollback, which was waiting for the lease, reverses every input of the transaction.
//   - FinalizeTransaction fails otherwise: the lease is released, the error is returned and the
//     spends stay in place, because the record is still creating.
//
// opts thread the caller's IgnoreFlags into the re-spend. A roll-forward is COMPLETING a
// spend the node already committed to, so recovery callers that are in block context may
// pass at least as permissive a set as the original spend (e.g. WithIgnoreLocked). A caller
// that is not in block context must not bypass the lock: a locked parent is the handoff
// barrier, and the roll-forward then fails with ErrTxLocked, which is a retry. The re-spend
// does not set RequireParentOutputs, because the record it finds exists by design and a
// parent is pruned only once every output is spent.
func RollForwardCreating(ctx context.Context, logger ulogger.Logger, s CreatingRollForwarder, tx *bt.Tx, attempt []byte, blockHeight uint32, opts ...CreateOption) (spends []*Spend, completed bool, err error) {
	return rollForwardCreating(ctx, logger, s, tx, attempt, func(rfCtx context.Context) ([]*Spend, error) {
		_, spends, err := s.SpendAndCreate(rfCtx, tx, blockHeight, append([]CreateOption{WithSpendOnly()}, opts...)...)

		return spends, err
	})
}

// creatingFinisher is the store subset the roll-forward rule needs around the re-spend.
type creatingFinisher interface {
	FinalizeTransaction(ctx context.Context, tx *bt.Tx, attempt, lease []byte) error
	GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error
	TakeSpendLease(ctx context.Context, hash *chainhash.Hash, attempt []byte) ([]byte, bool, error)
	ReleaseSpendLease(ctx context.Context, hash *chainhash.Hash, attempt, lease []byte) error
}

// rollForwardCreating is the single definition of the roll-forward rule; spend is the re-spend of
// the caller's store. completed is true when this call spent and finalized the record, and false,
// with a nil error, when the record was already final and the call did nothing.
func rollForwardCreating(ctx context.Context, logger ulogger.Logger, s creatingFinisher, tx *bt.Tx, attempt []byte, spend func(context.Context) ([]*Spend, error)) (spends []*Spend, completed bool, err error) {
	hash := tx.TxIDChainHash()

	bCtx, cancel := boundedContext(ctx, s)
	defer cancel()

	lease, took, err := s.TakeSpendLease(bCtx, hash, attempt)
	if err != nil {
		return nil, false, err
	}

	if !took {
		return leaseRefused(ctx, s, tx, attempt)
	}

	spends, err = spend(bCtx)
	if err != nil {
		if !spendOutcomeUnknown(spends, err) {
			releaseSpendLease(ctx, logger, s, hash, attempt, lease)
		}

		return spends, false, err
	}

	if !AllInputsSpent(tx, spends) {
		releaseSpendLease(ctx, logger, s, hash, attempt, lease)

		return spends, false, errors.NewProcessingError("RollForwardCreating: re-spend of %s did not cover all inputs (%d spends for %d inputs)",
			hash.String(), len(spends), len(tx.Inputs))
	}

	if err = finalizeWithRetry(bCtx, logger, s, tx, attempt, lease); err != nil {
		releaseSpendLease(ctx, logger, s, hash, attempt, lease)

		if IsAlreadyFinal(err) {
			// The record is complete, and another actor's finalize of this attempt cleared the
			// creating state. That actor owns the hand-off, so this call did not complete it.
			return spends, false, nil
		}

		return spends, false, err
	}

	return spends, true, nil
}

// leaseRefused reads the record after its spend lease was refused and returns the roll-forward's
// result: an error for a record that is rolling back, owned by another attempt, absent or still
// creating, and the spends of the transaction for a record that is final.
func leaseRefused(ctx context.Context, s creatingFinisher, tx *bt.Tx, attempt []byte) ([]*Spend, bool, error) {
	hash := tx.TxIDChainHash()

	current := &meta.Data{}
	if err := s.GetMeta(ctx, hash, current); err != nil {
		return nil, false, err
	}

	switch {
	case current.RollingBack:
		return nil, false, errors.NewProcessingError("RollForwardCreating: tx %s is rolling back; retry after it completes", hash.String())
	case current.Creating && !bytes.Equal(current.Attempt, attempt):
		return nil, false, errors.NewProcessingError("RollForwardCreating: tx %s is creating under another attempt; retry", hash.String())
	case current.Creating:
		return nil, false, errors.NewProcessingError("RollForwardCreating: the spend lease of %s was refused although the record is creating; retry", hash.String())
	}

	// The inputs of a final record are spent. Their spends are returned for the caller's hand-off;
	// a transaction they cannot be built from returns none, which is not a failure of a record
	// that is already complete.
	spends, _ := GetSpends(tx)

	return spends, false, nil
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

// transactionFinalizer is the store subset finalizeWithRetry needs.
type transactionFinalizer interface {
	FinalizeTransaction(ctx context.Context, tx *bt.Tx, attempt, lease []byte) error
}

// finalizeWithRetry finalizes tx with up to 3 attempts and the same exponential backoff
// as unspendWithRetry. The backoff aborts early when ctx is cancelled. A repeat is safe: the
// store records the lease of the caller whose write clears the creating state, so a try that
// finds the record final returns nil when an earlier try of this caller did it, and an error for
// which IsAlreadyFinal is true when another actor did.
func finalizeWithRetry(ctx context.Context, logger ulogger.Logger, s transactionFinalizer, tx *bt.Tx, attempt, lease []byte) error {
	var finErr error

	for retries := uint(0); retries < 3; retries++ {
		tryErr := s.FinalizeTransaction(ctx, tx, attempt, lease)
		if tryErr == nil || IsAlreadyFinal(tryErr) || errors.Is(tryErr, errors.ErrTxNotFound) {
			// A record that is final, gone, rolling back or under another attempt is an answer:
			// a repeat would get the same one.
			return tryErr
		}

		finErr = tryErr

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
