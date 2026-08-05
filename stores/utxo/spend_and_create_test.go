package utxo

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

type fakeSequentialStore struct {
	spends      []*Spend
	spendErr    error
	createMeta  *meta.Data
	createErr   error
	finalizeErr error
	// unspendErrs is popped once per Unspend call; nil result when exhausted.
	unspendErrs []error

	spendCalls     int
	createCalls    int
	unspendCalls   int
	finalizeCalls  int
	unspentWith    []*Spend
	spendFlags     []IgnoreFlags
	lastCreateOpts CreateOptions
	callSeq        []string
}

func (f *fakeSequentialStore) Spend(_ context.Context, _ *bt.Tx, _ uint32, ignoreFlags ...IgnoreFlags) ([]*Spend, error) {
	f.spendCalls++
	f.spendFlags = ignoreFlags
	f.callSeq = append(f.callSeq, "spend")

	return f.spends, f.spendErr
}

func (f *fakeSequentialStore) Create(_ context.Context, _ *bt.Tx, _ uint32, opts ...CreateOption) (*meta.Data, error) {
	f.createCalls++
	f.callSeq = append(f.callSeq, "create")

	o := CreateOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	f.lastCreateOpts = o

	return f.createMeta, f.createErr
}

func (f *fakeSequentialStore) Unspend(_ context.Context, spends []*Spend, _ ...bool) error {
	f.unspendCalls++
	f.unspentWith = spends
	f.callSeq = append(f.callSeq, "unspend")

	if len(f.unspendErrs) > 0 {
		err := f.unspendErrs[0]
		f.unspendErrs = f.unspendErrs[1:]

		return err
	}

	return nil
}

func (f *fakeSequentialStore) FinalizeTransaction(_ context.Context, _ *bt.Tx) error {
	f.finalizeCalls++
	f.callSeq = append(f.callSeq, "finalize")

	return f.finalizeErr
}

func shortenUnspendBackoff(t *testing.T) {
	t.Helper()

	orig := unspendRetryBackoffBase
	unspendRetryBackoffBase = time.Millisecond

	t.Cleanup(func() { unspendRetryBackoffBase = orig })
}

func TestSequentialSpendAndCreate(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tx := bt.NewTx()

	t.Run("happy path spends then creates", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:     []*Spend{{}},
			createMeta: &meta.Data{},
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.NoError(t, err)
		require.Same(t, f.createMeta, md)
		require.Len(t, spends, 1)
		require.Equal(t, 1, f.spendCalls)
		require.Equal(t, 1, f.createCalls)
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("ignore flags are passed to the spend phase", func(t *testing.T) {
		f := &fakeSequentialStore{createMeta: &meta.Data{}}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false,
			WithIgnoreLocked(true), WithSkipUTXOHashCheck(true))
		require.NoError(t, err)
		require.Len(t, f.spendFlags, 1)
		require.True(t, f.spendFlags[0].IgnoreLocked)
		require.True(t, f.spendFlags[0].SkipUTXOHashCheck)
		require.False(t, f.spendFlags[0].IgnoreConflicting)
	})

	t.Run("spend failure returns per-input spends and skips create", func(t *testing.T) {
		spendErr := errors.NewUtxoError("boom")
		f := &fakeSequentialStore{
			spends:   []*Spend{{Err: errors.ErrSpent}},
			spendErr: spendErr,
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.Nil(t, md)
		require.Len(t, spends, 1)
		require.Equal(t, 0, f.createCalls)
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("create failure rolls back the spends", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:    []*Spend{{}, {}},
			createErr: errors.NewProcessingError("create blew up"),
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Nil(t, md)
		require.Nil(t, spends, "rolled-back spends must not be returned as live")
		require.Equal(t, 1, f.unspendCalls)
		require.Same(t, f.spends[0], f.unspentWith[0])
		require.Len(t, f.unspentWith, 2)
	})

	t.Run("create-only create failure never touches Unspend", func(t *testing.T) {
		f := &fakeSequentialStore{
			createErr: errors.NewProcessingError("create blew up"),
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, WithCreateOnly())
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Nil(t, spends)
		require.Equal(t, 0, f.spendCalls)
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("rollback backoff aborts when the context is cancelled", func(t *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()

		f := &fakeSequentialStore{
			spends:      []*Spend{{}},
			createErr:   errors.NewProcessingError("create blew up"),
			unspendErrs: []error{errors.NewStorageError("t1")},
		}

		_, _, err := SequentialSpendAndCreate(cancelledCtx, logger, f, tx, 100, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "context cancelled")
		require.Equal(t, 1, f.unspendCalls)
	})

	t.Run("create ErrTxExists keeps the spends in place", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:    []*Spend{{}},
			createErr: errors.NewTxExistsError("already there"),
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.Nil(t, md)
		require.Len(t, spends, 1)
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("rollback retries transient unspend failures", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeSequentialStore{
			spends:      []*Spend{{}},
			createErr:   errors.NewProcessingError("create blew up"),
			unspendErrs: []error{errors.NewStorageError("t1"), errors.NewStorageError("t2")},
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Equal(t, 3, f.unspendCalls)
	})

	t.Run("rollback failure after all retries is reported with the create error", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeSequentialStore{
			spends:    []*Spend{{}},
			createErr: errors.NewProcessingError("create blew up"),
			unspendErrs: []error{
				errors.NewStorageError("u1"),
				errors.NewStorageError("u2"),
				errors.NewStorageError("u3"),
			},
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.Error(t, err)
		require.Equal(t, 3, f.unspendCalls)
		require.Contains(t, err.Error(), "create blew up")
		require.Contains(t, err.Error(), "u3")
	})

	t.Run("create only skips the spend phase", func(t *testing.T) {
		f := &fakeSequentialStore{createMeta: &meta.Data{}}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, WithCreateOnly())
		require.NoError(t, err)
		require.Same(t, f.createMeta, md)
		require.Nil(t, spends)
		require.Equal(t, 0, f.spendCalls)
		require.Equal(t, 1, f.createCalls)
	})

	t.Run("spend only skips the create phase", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}}}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, WithSpendOnly())
		require.NoError(t, err)
		require.Nil(t, md)
		require.Len(t, spends, 1)
		require.Equal(t, 1, f.spendCalls)
		require.Equal(t, 0, f.createCalls)
	})

	t.Run("create only and spend only together are rejected", func(t *testing.T) {
		f := &fakeSequentialStore{}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, WithCreateOnly(), WithSpendOnly())
		require.ErrorIs(t, err, errors.ErrInvalidArgument)
		require.Equal(t, 0, f.spendCalls)
		require.Equal(t, 0, f.createCalls)
	})
}

// fakeRollForwarder scripts the CreatingRollForwarder surface (SpendAndCreate +
// FinalizeTransaction) so RollForwardCreating can be tested in isolation.
type fakeRollForwarder struct {
	spends         []*Spend
	spendErr       error
	finalizeErr    error
	setLockedErr   error
	finalizeCalls  int
	setLockedCalls int
	setLockedValue bool
	lastOpts       CreateOptions
}

func (f *fakeRollForwarder) SpendAndCreate(_ context.Context, _ *bt.Tx, _ uint32, opts ...CreateOption) (*meta.Data, []*Spend, error) {
	o := CreateOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	f.lastOpts = o

	return nil, f.spends, f.spendErr
}

func (f *fakeRollForwarder) FinalizeTransaction(_ context.Context, _ *bt.Tx) error {
	f.finalizeCalls++
	return f.finalizeErr
}

func (f *fakeRollForwarder) SetLocked(_ context.Context, _ []chainhash.Hash, value bool) error {
	f.setLockedCalls++
	f.setLockedValue = value

	return f.setLockedErr
}

// twoInputTx returns a tx with two (empty) inputs so AllInputsSpent has a length to match.
func twoInputTx() *bt.Tx {
	tx := bt.NewTx()
	tx.Inputs = append(tx.Inputs, &bt.Input{}, &bt.Input{})

	return tx
}

// TestRollForwardCreating pins the fail-closed contract every roll-forward site
// (validator ErrTxExists fast-path, setMined pre-flight, pruner sweep) relies on:
// finalize ONLY when the re-spend covered every input with no per-input error.
func TestRollForwardCreating(t *testing.T) {
	ctx := context.Background()
	tx := twoInputTx()

	t.Run("finalizes and unlocks only after every input is re-spent", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}}

		err := RollForwardCreating(ctx, f, tx, 100)
		require.NoError(t, err)
		require.True(t, f.lastOpts.SpendOnly, "roll-forward must re-run the spend-only phase")
		require.Equal(t, 1, f.finalizeCalls, "a complete re-spend must finalize")
		require.Equal(t, 1, f.setLockedCalls, "a rolled-forward record must be unlocked")
		require.False(t, f.setLockedValue, "unlock means SetLocked(false)")
	})

	t.Run("threads caller ignore-flags into the re-spend", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}}

		err := RollForwardCreating(ctx, f, tx, 100, WithIgnoreLocked(true))
		require.NoError(t, err)
		require.True(t, f.lastOpts.SpendOnly, "must still be spend-only")
		require.True(t, f.lastOpts.IgnoreFlags.IgnoreLocked, "the caller's IgnoreLocked must reach the spend, so a locked parent does not defeat recovery")
	})

	t.Run("fails closed when the re-spend covers fewer inputs than the tx has", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}}} // 1 spend for a 2-input tx

		err := RollForwardCreating(ctx, f, tx, 100)
		require.Error(t, err)
		require.Equal(t, 0, f.finalizeCalls, "an incomplete re-spend must NOT finalize")
		require.Equal(t, 0, f.setLockedCalls, "an incomplete re-spend must NOT unlock")
	})

	t.Run("fails closed on a per-input spend error", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {Err: errors.ErrSpent}}}

		err := RollForwardCreating(ctx, f, tx, 100)
		require.Error(t, err)
		require.Equal(t, 0, f.finalizeCalls)
		require.Equal(t, 0, f.setLockedCalls)
	})

	t.Run("returns the spend error without finalizing", func(t *testing.T) {
		f := &fakeRollForwarder{spendErr: errors.NewStorageError("spend boom")}

		err := RollForwardCreating(ctx, f, tx, 100)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, 0, f.finalizeCalls)
	})

	t.Run("propagates a finalize failure and does not unlock", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, finalizeErr: errors.NewStorageError("finalize boom")}

		err := RollForwardCreating(ctx, f, tx, 100)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, 1, f.finalizeCalls)
		require.Equal(t, 0, f.setLockedCalls, "must not unlock if finalize failed")
	})

	t.Run("propagates an unlock failure", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, setLockedErr: errors.NewStorageError("unlock boom")}

		err := RollForwardCreating(ctx, f, tx, 100)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, 1, f.finalizeCalls)
		require.Equal(t, 1, f.setLockedCalls)
	})
}

// TestAllInputsSpent covers the shared completeness predicate directly.
func TestAllInputsSpent(t *testing.T) {
	tx := twoInputTx()

	require.True(t, AllInputsSpent(tx, []*Spend{{}, {}}), "two clean spends for two inputs is complete")
	require.False(t, AllInputsSpent(tx, []*Spend{{}}), "fewer spends than inputs is incomplete")
	require.False(t, AllInputsSpent(tx, []*Spend{{}, {}, {}}), "more spends than inputs is not a match")
	require.False(t, AllInputsSpent(tx, []*Spend{{}, {Err: errors.ErrSpent}}), "a per-input error is incomplete")
	require.False(t, AllInputsSpent(tx, []*Spend{{}, nil}), "a nil spend is incomplete")
}

// TestSequentialSpendAndCreate_CreateFirst covers the create-first ordering folded
// into the combined path (createFirst=true, neither CreateOnly nor SpendOnly).
func TestSequentialSpendAndCreate_CreateFirst(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tx := bt.NewTx()

	t.Run("orders create-tentative then spend then finalize", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}}, createMeta: &meta.Data{Creating: true}}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.NoError(t, err)
		require.Equal(t, []string{"create", "spend", "finalize"}, f.callSeq)
		require.True(t, f.lastCreateOpts.Creating, "the tentative create must carry WithCreating")
		require.Len(t, spends, 1)
		require.False(t, md.Creating, "returned meta must reflect the finalized state")
	})

	t.Run("spend failure leaves the creating record (no finalize, no rollback)", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrSpent}},
			spendErr:   errors.NewUtxoError("double spend"),
			createMeta: &meta.Data{Creating: true},
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.Equal(t, []string{"create", "spend"}, f.callSeq, "must not finalize or unspend a failed create-first tx")
		require.Equal(t, 0, f.finalizeCalls)
		require.Equal(t, 0, f.unspendCalls)
		require.NotNil(t, md, "the creating record is returned for the caller/sweeper to resolve")
		require.Len(t, spends, 1)
	})

	t.Run("create ErrTxExists returns without spending", func(t *testing.T) {
		f := &fakeSequentialStore{createErr: errors.NewTxExistsError("already there")}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.Equal(t, []string{"create"}, f.callSeq, "on ErrTxExists the sweeper handles recovery; no spend/finalize here")
	})

	t.Run("finalize failure returns an error (not announced as validated)", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:      []*Spend{{}},
			createMeta:  &meta.Data{Creating: true},
			finalizeErr: errors.NewStorageError("finalize boom"),
		}

		md, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.Error(t, err, "the inputs are spent but the tentative flag is not cleared, so the outputs are unspendable; reporting success would announce an unspendable tx")
		require.Equal(t, []string{"create", "spend", "finalize"}, f.callSeq)
		require.NotNil(t, md)
		require.True(t, md.Creating, "the record is left creating for recovery (re-encounter roll-forward / sweep / setMined)")
	})

	t.Run("caller-supplied WithCreating(true) is rejected", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}}, createMeta: &meta.Data{}}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true, WithCreating(true))
		require.ErrorIs(t, err, errors.ErrInvalidArgument, "WithCreating is managed by the create-first path and must not be passed by callers")
		require.Empty(t, f.callSeq, "the guard must reject before any store call")
	})

	t.Run("caller-supplied WithCreating(false) cannot defeat the forced tentative create", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}}, createMeta: &meta.Data{Creating: true}}

		// WithCreating(false) is not caught by the guard (it is the zero value), so the
		// forced WithCreating(true) appended last must win — otherwise the record would be
		// created spendable before its inputs are spent.
		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true, WithCreating(false))
		require.NoError(t, err)
		require.True(t, f.lastCreateOpts.Creating, "the forced tentative create must win over a caller's WithCreating(false)")
	})

	t.Run("CreateOnly bypasses create-first ordering", func(t *testing.T) {
		f := &fakeSequentialStore{createMeta: &meta.Data{}}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true, WithCreateOnly())
		require.NoError(t, err)
		require.Equal(t, []string{"create"}, f.callSeq, "single-phase CreateOnly must not spend or finalize")
		require.False(t, f.lastCreateOpts.Creating)
	})

	t.Run("SpendOnly bypasses create-first ordering", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}}}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true, WithSpendOnly())
		require.NoError(t, err)
		require.Equal(t, []string{"spend"}, f.callSeq, "single-phase SpendOnly must not create or finalize")
	})
}
