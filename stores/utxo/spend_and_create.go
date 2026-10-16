package utxo

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
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
// A create that fails with ErrTxExists, or with a context error, keeps the
// spends. ErrTxExists means the record is there. A context error means the
// outcome is not known: the SQL and Aerospike creates go through a batcher, and
// a caller whose context ends while its item waits returns at once, while the
// batch can still commit the record. Unspending then would leave a record whose
// inputs are unspent, which a repeat never heals, since a repeat drops a
// transaction whose record exists. Keeping the spends leaves either a written
// transaction or a parent naming a spender with no record yet, and a repeat of
// the same SpendAndCreate heals the second: the spend is accepted as the same
// spend and the create runs.
//
// Returned spends: nil when the create failed and the rollback succeeded (the
// spends are no longer in effect); the live spends when the create failed with
// ErrTxExists or a context error, or the rollback itself failed.
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
	if err != nil {
		if errors.Is(err, errors.ErrTxExists) {
			// the tx already exists; leave the spends in place for the caller to decide
			return nil, spends, err
		}

		if errors.IsContextError(err) {
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
