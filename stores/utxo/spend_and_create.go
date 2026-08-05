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
// behaviour implement these three methods and delegate.
type SequentialStore interface {
	Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error)
	Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...IgnoreFlags) ([]*Spend, error)
	Unspend(ctx context.Context, spends []*Spend, flagAsLocked ...bool) error
	// FinalizeTransaction clears the tentative creating state; used by the create-first path.
	FinalizeTransaction(ctx context.Context, tx *bt.Tx) error
}

// SequentialSpendAndCreate implements the Store.SpendAndCreate contract as the
// original two-call sequence: spend the inputs, create the outputs, and roll the
// spends back (with retries) when the create fails with anything other than
// ErrTxExists. Concrete stores delegate to it until they implement SpendAndCreate
// atomically (Postgres transactions, Aerospike-specific logic).
//
// Returned spends: nil when the create failed and the rollback succeeded (the
// spends are no longer in effect); the live spends when the create failed with
// ErrTxExists or the rollback itself failed.
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

		if len(spends) > 0 {
			if rollbackErr := unspendWithRetry(ctx, logger, s, spends); rollbackErr != nil {
				return nil, spends, errors.NewProcessingError("SpendAndCreate: error reversing utxo spends: %v", rollbackErr, err)
			}
		}

		return nil, nil, err
	}

	return md, spends, nil
}

// sequentialCreateFirst persists a tx create-first: the record is created in the
// tentative "creating" state, the inputs are spent, then the flag is cleared. Because
// the record exists (spend-gated) before its inputs are spent, a crash in the
// spend→finalize window leaves an inspectable, recoverable record rather than a
// dangling spender reference. This addresses the spend-before-create crash window (one
// mechanism related to #1214); it does not by itself satisfy #1214's delete-path audit.
//
// Recovery is roll-forward, not rollback:
//   - Create returns ErrTxExists (a prior attempt already created the record): return it
//     unchanged; the caller treats it as a duplicate and the pruner sweeper rolls a
//     still-creating record forward (re-spend + finalize).
//   - Spend fails after the tentative create: the creating record is left in place as the
//     recovery WAL (NOT finalized, NOT deleted); the error + spends are returned so the
//     caller's conflicting handling can act and the pruner sweeper can roll it forward.
//     Terminal double-spends are NOT resolved inline here: doing so on the free propagation
//     path is attacker-paceable, and — because the delete side does not yet remove parent
//     conflictingChildren refs — would manufacture the dangling-ref shape #1214 documents.
//   - FinalizeTransaction fails after successful spends: return an error. The inputs are
//     durably spent but the tentative flag could not be cleared, so the outputs are still
//     unspendable; reporting success would make the validator announce the tx to block
//     assembly (and unlock it) while its children fail with an unhandled ErrTxCreating. The
//     record stays creating and is recovered by the ErrTxExists roll-forward on re-encounter,
//     the pruner sweep, or setMined.
func sequentialCreateFirst(ctx context.Context, logger ulogger.Logger, s SequentialStore,
	tx *bt.Tx, blockHeight uint32, ignoreFlags IgnoreFlags, opts ...CreateOption) (*meta.Data, []*Spend, error) {
	// Force WithCreating(true) last so it cannot be overridden by a caller option (belt and
	// suspenders alongside the caller-supplied WithCreating rejection in SequentialSpendAndCreate).
	createOpts := append(append([]CreateOption{}, opts...), WithCreating(true))

	md, err := s.Create(ctx, tx, blockHeight, createOpts...)
	if err != nil {
		// ErrTxExists or any create error: nothing has been spent yet, nothing to roll back.
		return nil, nil, err
	}

	spends, err := s.Spend(ctx, tx, blockHeight, ignoreFlags)
	if err != nil {
		// Leave the creating record in place as the recovery WAL; hand back the spends+error so
		// the caller's conflicting handling and the pruner sweeper can resolve it.
		return md, spends, err
	}

	if finErr := s.FinalizeTransaction(ctx, tx); finErr != nil {
		logger.Errorf("SpendAndCreate create-first: FinalizeTransaction failed for %s, tx left creating for recovery: %v", tx.TxIDChainHash().String(), finErr)
		prometheusCreateFirstFinalizeFailed.Inc()

		return md, spends, errors.NewProcessingError("SpendAndCreate create-first: finalize failed for %s, tx left creating for recovery", tx.TxIDChainHash().String(), finErr)
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
// re-run the spend-only phase, clear the tentative flag, and clear the lock.
type CreatingRollForwarder interface {
	SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, []*Spend, error)
	FinalizeTransaction(ctx context.Context, tx *bt.Tx) error
	SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error
}

// RollForwardCreating completes a create-first transition abandoned in the tentative
// "creating" state: it re-runs the input spends (idempotent for the same spender) and
// finalizes the record. It is FAIL-CLOSED — it returns an error unless every input was
// actually spent and the finalize succeeded, so a caller can never mistake a partial
// roll-forward for a completed one. Shared by the ErrTxExists validator fast-path, the
// setMined pre-flight, and the pruner sweeper so the "is this really spent?" rule has a
// single definition.
//
// opts thread the caller's IgnoreFlags into the re-spend. A roll-forward is COMPLETING a
// spend the node already committed to, so recovery callers should pass at least as
// permissive a set as the original spend (e.g. WithIgnoreLocked) — otherwise a condition
// the original spend was told to tolerate (a locked parent) turns a fail-closed roll-forward
// into a hard stop, and the record never converges.
func RollForwardCreating(ctx context.Context, s CreatingRollForwarder, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) error {
	_, spends, err := s.SpendAndCreate(ctx, tx, blockHeight, append([]CreateOption{WithSpendOnly()}, opts...)...)
	if err != nil {
		return err
	}

	if !AllInputsSpent(tx, spends) {
		return errors.NewProcessingError("RollForwardCreating: re-spend of %s did not cover all inputs (%d spends for %d inputs)",
			tx.TxIDChainHash().String(), len(spends), len(tx.Inputs))
	}

	if err := s.FinalizeTransaction(ctx, tx); err != nil {
		return err
	}

	// The tentative create carried WithLocked(true) on the block-assembly path, and
	// FinalizeTransaction does not clear the lock — so a rolled-forward record would be
	// creating=false but locked=true, i.e. still unspendable, the opposite of "rolled
	// forward". Clear the lock so the outputs are actually spendable. Idempotent: clearing
	// an already-unlocked record is a no-op.
	if err := s.SetLocked(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, false); err != nil {
		return err
	}

	return nil
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
