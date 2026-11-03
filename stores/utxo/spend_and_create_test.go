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
	spends     []*Spend
	spendErr   error
	createMeta *meta.Data
	createErr  error
	// unspendErrs is popped once per Unspend call; nil result when exhausted.
	unspendErrs []error

	spendCalls   int
	createCalls  int
	unspendCalls int
	unspentWith  []*Spend
	spendFlags   []IgnoreFlags
}

func (f *fakeSequentialStore) Spend(_ context.Context, _ *bt.Tx, _ uint32, ignoreFlags ...IgnoreFlags) ([]*Spend, error) {
	f.spendCalls++
	f.spendFlags = ignoreFlags

	return f.spends, f.spendErr
}

func (f *fakeSequentialStore) Create(_ context.Context, _ *bt.Tx, _ uint32, _ ...CreateOption) (*meta.Data, error) {
	f.createCalls++

	return f.createMeta, f.createErr
}

func (f *fakeSequentialStore) Unspend(_ context.Context, spends []*Spend, _ ...bool) error {
	f.unspendCalls++
	f.unspentWith = spends

	if len(f.unspendErrs) > 0 {
		err := f.unspendErrs[0]
		f.unspendErrs = f.unspendErrs[1:]

		return err
	}

	return nil
}

// boundedSequentialStore names its own rollback bound.
type boundedSequentialStore struct {
	fakeSequentialStore
	bound time.Duration
}

func (b *boundedSequentialStore) SpendRollbackTimeout() time.Duration { return b.bound }

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

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
		require.NoError(t, err)
		require.Same(t, f.createMeta, md)
		require.Len(t, spends, 1)
		require.Equal(t, 1, f.spendCalls)
		require.Equal(t, 1, f.createCalls)
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("ignore flags are passed to the spend phase", func(t *testing.T) {
		f := &fakeSequentialStore{createMeta: &meta.Data{}}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100,
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

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
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

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
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

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, WithCreateOnly())
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Nil(t, spends)
		require.Equal(t, 0, f.spendCalls)
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("rollback outlives the caller's cancel", func(t *testing.T) {
		shortenUnspendBackoff(t)

		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()

		f := &fakeSequentialStore{
			spends:      []*Spend{{}},
			createErr:   errors.NewProcessingError("create blew up"),
			unspendErrs: []error{errors.NewStorageError("t1")},
		}

		_, spends, err := SequentialSpendAndCreate(cancelledCtx, logger, f, tx, 100)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotContains(t, err.Error(), "reversing", "the rollback must complete under its own context")
		require.Nil(t, spends, "a completed rollback leaves no spends in effect")
		require.Equal(t, 2, f.unspendCalls)
	})

	t.Run("a create whose outcome is unknown keeps the spends", func(t *testing.T) {
		for _, createErr := range []error{
			context.DeadlineExceeded,
			errors.NewContextCanceledError("create did not finish", context.Canceled),
			MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("store batch did not complete within 1s")),
			errors.NewProcessingError("wrapped", MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("store batch did not complete within 1s"))),
		} {
			f := &fakeSequentialStore{
				spends:    []*Spend{{}},
				createErr: createErr,
			}

			md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
			require.True(t, IsCreateOutcomeUnknown(err), "%v", err)
			require.Nil(t, md)
			require.Len(t, spends, 1, "the create may still commit, so the spends stay in effect")
			require.Equal(t, 0, f.unspendCalls)
		}
	})

	t.Run("an unmarked service-unavailable create failure rolls back", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:    []*Spend{{}},
			createErr: errors.NewServiceUnavailableError("store is down"),
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
		require.False(t, IsCreateOutcomeUnknown(err))
		require.Nil(t, spends, "a create that definitely failed is rolled back")
		require.Equal(t, 1, f.unspendCalls)
	})

	t.Run("rollback backoff stops at the rollback bound", func(t *testing.T) {
		f := &boundedSequentialStore{
			fakeSequentialStore: fakeSequentialStore{
				spends:      []*Spend{{}},
				createErr:   errors.NewProcessingError("create blew up"),
				unspendErrs: []error{errors.NewStorageError("t1"), errors.NewStorageError("t2"), errors.NewStorageError("t3")},
			},
			bound: time.Millisecond,
		}

		start := time.Now()
		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
		require.Less(t, time.Since(start), unspendRetryBackoffBase/2, "a backoff past the rollback bound must be cut short")
		require.Error(t, err)
		require.Contains(t, err.Error(), "context cancelled during retry backoff")
		require.Len(t, spends, 1, "a failed rollback returns the spends still in effect")
		require.Equal(t, 1, f.unspendCalls)
	})

	t.Run("create ErrTxExists keeps the spends in place", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:    []*Spend{{}},
			createErr: errors.NewTxExistsError("already there"),
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
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

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
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

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
		require.Error(t, err)
		require.Equal(t, 3, f.unspendCalls)
		require.Contains(t, err.Error(), "create blew up")
		require.Contains(t, err.Error(), "u3")
	})

	t.Run("create only skips the spend phase", func(t *testing.T) {
		f := &fakeSequentialStore{createMeta: &meta.Data{}}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, WithCreateOnly())
		require.NoError(t, err)
		require.Same(t, f.createMeta, md)
		require.Nil(t, spends)
		require.Equal(t, 0, f.spendCalls)
		require.Equal(t, 1, f.createCalls)
	})

	t.Run("spend only skips the create phase", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}}}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, WithSpendOnly())
		require.NoError(t, err)
		require.Nil(t, md)
		require.Len(t, spends, 1)
		require.Equal(t, 1, f.spendCalls)
		require.Equal(t, 0, f.createCalls)
	})

	t.Run("create only and spend only together are rejected", func(t *testing.T) {
		f := &fakeSequentialStore{}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, WithCreateOnly(), WithSpendOnly())
		require.ErrorIs(t, err, errors.ErrInvalidArgument)
		require.Equal(t, 0, f.spendCalls)
		require.Equal(t, 0, f.createCalls)
	})
}

// recheckStore answers the first Spend with spends and each recheck with the
// next of recheckErrs, then with recheckErr, and records the recheck's flags. A
// recheck under a context that is already done fails with the context's error.
type recheckStore struct {
	fakeSequentialStore
	recheckErrs  []error
	recheckErr   error
	recheckFlags []IgnoreFlags
}

func (r *recheckStore) Spend(ctx context.Context, tx *bt.Tx, h uint32, ignoreFlags ...IgnoreFlags) ([]*Spend, error) {
	if r.spendCalls > 0 {
		r.spendCalls++
		r.recheckFlags = ignoreFlags

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if len(r.recheckErrs) > 0 {
			err := r.recheckErrs[0]
			r.recheckErrs = r.recheckErrs[1:]

			return nil, err
		}

		return nil, r.recheckErr
	}

	return r.fakeSequentialStore.Spend(ctx, tx, h, ignoreFlags...)
}

// unknownCreateStore can read and remove a record, and its create reports an
// unknown outcome.
type unknownCreateStore struct {
	deletingRecheckStore
	record    meta.Data
	readErr   error
	readCalls int
}

func (u *unknownCreateStore) GetMeta(_ context.Context, _ *chainhash.Hash, data *meta.Data) error {
	u.readCalls++
	*data = u.record

	return u.readErr
}

// deletingRecheckStore can remove the record it created.
type deletingRecheckStore struct {
	recheckStore
	deleteErr   error
	deleteCalls int
}

func (d *deletingRecheckStore) DeleteComplete(context.Context, *chainhash.Hash) error {
	d.deleteCalls++

	return d.deleteErr
}

// TestSequentialSpendAndCreate_Recheck covers the recheck after a create over a
// spend the store had already answered for, against fakes, for the outcomes the
// store tests do not reach: a store that cannot delete, and a delete that fails.
func TestSequentialSpendAndCreate_Recheck(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tx := bt.NewTx()
	pruned := errors.NewUtxoSpendingTxPrunedError("pruned")

	t.Run("a fresh spend is not rechecked", func(t *testing.T) {
		f := &recheckStore{fakeSequentialStore: fakeSequentialStore{spends: []*Spend{{}}, createMeta: &meta.Data{}}}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
		require.NoError(t, err)
		require.Equal(t, 1, f.spendCalls)
	})

	t.Run("the recheck requires parent outputs", func(t *testing.T) {
		f := &recheckStore{fakeSequentialStore: fakeSequentialStore{spends: []*Spend{{Blessed: true}}, createMeta: &meta.Data{}}}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, WithIgnoreLocked(true))
		require.NoError(t, err)
		require.Equal(t, 2, f.spendCalls)
		require.Len(t, f.recheckFlags, 1)
		require.True(t, f.recheckFlags[0].RequireParentOutputs)
		require.True(t, f.recheckFlags[0].IgnoreLocked, "the caller's flags are kept")
	})

	t.Run("a recheck that cannot answer is retried, then reported with the record and spends kept", func(t *testing.T) {
		f := &recheckStore{fakeSequentialStore: fakeSequentialStore{spends: []*Spend{{Idempotent: true}}, createMeta: &meta.Data{}}, recheckErr: errors.NewStorageError("timeout")}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
		require.Nil(t, md)
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.True(t, IsCreateOutcomeUnknown(err))
		require.True(t, errors.IsRetryableError(err), "the block path retries it")
		require.Len(t, spends, 1, "the spends are kept")
		require.Equal(t, 0, f.unspendCalls)
		require.Equal(t, 1+deleteCreatedAttempts, f.spendCalls)
	})

	t.Run("a recheck that fails once and then answers is not reported", func(t *testing.T) {
		f := &recheckStore{
			fakeSequentialStore: fakeSequentialStore{spends: []*Spend{{Idempotent: true}}, createMeta: &meta.Data{}},
			recheckErrs:         []error{errors.NewStorageError("timeout")},
		}

		md, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
		require.NoError(t, err)
		require.NotNil(t, md)
		require.Equal(t, 3, f.spendCalls)
	})

	t.Run("the recheck runs past the caller's deadline", func(t *testing.T) {
		done, cancel := context.WithCancel(ctx)
		cancel()

		f := &deletingRecheckStore{
			recheckStore: recheckStore{fakeSequentialStore: fakeSequentialStore{spends: []*Spend{{Idempotent: true}}, createMeta: &meta.Data{}}, recheckErr: pruned},
		}

		_, _, err := SequentialSpendAndCreate(done, logger, f, tx, 100)
		require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned, "the recheck answered under a detached context")
		require.Equal(t, 1, f.deleteCalls)
	})

	t.Run("after an unknown create, only a marker over an unmined record removes it", func(t *testing.T) {
		unknown := MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("batch wait ran out"))

		for _, tc := range []struct {
			name       string
			spend      Spend
			recheck    error
			record     meta.Data
			readErr    error
			wantDelete bool
		}{
			{name: "marker, unmined", spend: Spend{Idempotent: true}, recheck: pruned, wantDelete: true},
			{name: "marker, mined", spend: Spend{Idempotent: true}, recheck: pruned, record: meta.Data{BlockIDs: []uint32{7}}},
			{name: "marker, record unreadable", spend: Spend{Idempotent: true}, recheck: pruned, readErr: errors.NewStorageError("down")},
			{name: "missing parent", spend: Spend{Blessed: true}, recheck: errors.NewTxNotFoundError("parent gone")},
			{name: "not a replay", spend: Spend{Idempotent: true}},
			{name: "recheck cannot answer", spend: Spend{Idempotent: true}, recheck: errors.NewStorageError("down")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f := &unknownCreateStore{
					deletingRecheckStore: deletingRecheckStore{recheckStore: recheckStore{
						fakeSequentialStore: fakeSequentialStore{spends: []*Spend{&tc.spend}, createErr: unknown},
						recheckErr:          tc.recheck,
					}},
					record:  tc.record,
					readErr: tc.readErr,
				}

				_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
				require.True(t, IsCreateOutcomeUnknown(err))
				require.Len(t, spends, 1, "the spends are kept")
				require.Equal(t, 0, f.unspendCalls)

				if tc.wantDelete {
					require.Equal(t, 1, f.deleteCalls)
					require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)
				} else {
					require.Equal(t, 0, f.deleteCalls)
					require.Same(t, unknown, err, "the create's own error is returned")
				}
			})
		}
	})

	t.Run("after an unknown create, a fresh spend is not rechecked", func(t *testing.T) {
		f := &recheckStore{fakeSequentialStore: fakeSequentialStore{spends: []*Spend{{}}, createErr: MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("x"))}}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
		require.True(t, IsCreateOutcomeUnknown(err))
		require.Equal(t, 1, f.spendCalls)
	})

	t.Run("a store that cannot delete reports a storage error", func(t *testing.T) {
		f := &recheckStore{fakeSequentialStore: fakeSequentialStore{spends: []*Spend{{Idempotent: true}}, createMeta: &meta.Data{}}, recheckErr: pruned}

		md, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
		require.Nil(t, md)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)
	})

	t.Run("a failed delete reports a storage error", func(t *testing.T) {
		f := &deletingRecheckStore{
			recheckStore: recheckStore{fakeSequentialStore: fakeSequentialStore{spends: []*Spend{{Idempotent: true}}, createMeta: &meta.Data{}}, recheckErr: pruned},
			deleteErr:    errors.NewStorageError("down"),
		}

		md, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
		require.Nil(t, md)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, deleteCreatedAttempts, f.deleteCalls)
	})

	t.Run("a delete that finds nothing returns the replay answer", func(t *testing.T) {
		f := &deletingRecheckStore{
			recheckStore: recheckStore{fakeSequentialStore: fakeSequentialStore{spends: []*Spend{{Idempotent: true}}, createMeta: &meta.Data{}}, recheckErr: pruned},
			deleteErr:    errors.NewTxNotFoundError("gone"),
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100)
		require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)
		require.Equal(t, 1, f.deleteCalls)
	})
}
