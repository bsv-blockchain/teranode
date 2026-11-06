package utxo

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
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
	// finalizeErrs is popped once per FinalizeTransaction call; finalizeErr applies when exhausted.
	finalizeErrs []error
	deleteErr    error
	// beginDeclined makes BeginCreateFirstRollback report that the record was no longer creating.
	beginDeclined bool
	beginErr      error
	// existing is what GetMeta returns after a Create that reported ErrTxExists.
	existing   meta.Data
	getMetaErr error
	// unspendErrs is popped once per Unspend call; nil result when exhausted.
	unspendErrs []error

	spendCalls     int
	createCalls    int
	unspendCalls   int
	finalizeCalls  int
	deleteCalls    int
	deletedHashes  []*chainhash.Hash
	unspentWith    []*Spend
	unspentBatches [][]*Spend
	spendFlags     []IgnoreFlags
	beginCalls     int
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
	f.unspentBatches = append(f.unspentBatches, spends)
	f.callSeq = append(f.callSeq, "unspend")

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

func (f *fakeSequentialStore) FinalizeTransaction(_ context.Context, _ *bt.Tx) error {
	f.finalizeCalls++
	f.callSeq = append(f.callSeq, "finalize")

	if len(f.finalizeErrs) > 0 {
		err := f.finalizeErrs[0]
		f.finalizeErrs = f.finalizeErrs[1:]

		return err
	}

	return f.finalizeErr
}

func (f *fakeSequentialStore) DeleteComplete(_ context.Context, hash *chainhash.Hash) error {
	f.deleteCalls++
	f.deletedHashes = append(f.deletedHashes, hash)
	f.callSeq = append(f.callSeq, "delete")

	return f.deleteErr
}

func (f *fakeSequentialStore) DeleteRollingBack(ctx context.Context, hash *chainhash.Hash) error {
	return f.DeleteComplete(ctx, hash)
}

func (f *fakeSequentialStore) BeginCreateFirstRollback(_ context.Context, _ *chainhash.Hash) (bool, error) {
	f.beginCalls++
	f.callSeq = append(f.callSeq, "begin")

	if f.beginErr != nil {
		return false, f.beginErr
	}

	return !f.beginDeclined, nil
}

func (f *fakeSequentialStore) GetMeta(_ context.Context, _ *chainhash.Hash, data *meta.Data) error {
	f.callSeq = append(f.callSeq, "getmeta")

	if f.getMetaErr != nil {
		return f.getMetaErr
	}

	*data = f.existing

	return nil
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

	t.Run("rollback outlives the caller's cancel", func(t *testing.T) {
		shortenUnspendBackoff(t)

		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()

		f := &fakeSequentialStore{
			spends:      []*Spend{{}},
			createErr:   errors.NewProcessingError("create blew up"),
			unspendErrs: []error{errors.NewStorageError("t1")},
		}

		_, spends, err := SequentialSpendAndCreate(cancelledCtx, logger, f, tx, 100, false)
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

			md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
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

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
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
		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
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
	spends        []*Spend
	spendErr      error
	finalizeErr   error
	finalizeCalls int
	lastOpts      CreateOptions
	unspendCalls  int
	unspentWith   []*Spend
	spendCalls    int
	existing      meta.Data
	getMetaErr    error
}

func (f *fakeRollForwarder) GetMeta(_ context.Context, _ *chainhash.Hash, data *meta.Data) error {
	if f.getMetaErr != nil {
		return f.getMetaErr
	}

	*data = f.existing

	return nil
}

func (f *fakeRollForwarder) Unspend(_ context.Context, spends []*Spend, _ ...bool) error {
	f.unspendCalls++
	f.unspentWith = spends

	return nil
}

func (f *fakeRollForwarder) SpendAndCreate(_ context.Context, _ *bt.Tx, _ uint32, opts ...CreateOption) (*meta.Data, []*Spend, error) {
	o := CreateOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	f.lastOpts = o
	f.spendCalls++

	return nil, f.spends, f.spendErr
}

func (f *fakeRollForwarder) FinalizeTransaction(_ context.Context, _ *bt.Tx) error {
	f.finalizeCalls++
	return f.finalizeErr
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

	t.Run("finalizes only after every input is re-spent and returns the spends", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, 100)
		require.NoError(t, err)
		require.True(t, f.lastOpts.SpendOnly, "roll-forward must re-run the spend-only phase")
		require.Equal(t, 1, f.finalizeCalls, "a complete re-spend must finalize")
		require.Equal(t, f.spends, spends, "the spends must reach the caller so it can hand the tx to block assembly")
	})

	t.Run("threads caller ignore-flags into the re-spend", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}}

		_, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, 100, WithIgnoreLocked(true))
		require.NoError(t, err)
		require.True(t, f.lastOpts.SpendOnly, "must still be spend-only")
		require.True(t, f.lastOpts.IgnoreFlags.IgnoreLocked, "the caller's IgnoreLocked must reach the spend, so a locked parent does not defeat recovery")
	})

	t.Run("fails closed when the re-spend covers fewer inputs than the tx has", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}}} // 1 spend for a 2-input tx

		_, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, 100)
		require.Error(t, err)
		require.Equal(t, 0, f.finalizeCalls, "an incomplete re-spend must NOT finalize")
	})

	t.Run("fails closed on a per-input spend error", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {Err: errors.ErrSpent}}}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, 100)
		require.Error(t, err)
		require.Equal(t, 0, f.finalizeCalls)
		require.Equal(t, f.spends, spends, "the per-input errors must reach the caller")
	})

	t.Run("returns the spend error without finalizing", func(t *testing.T) {
		f := &fakeRollForwarder{spendErr: errors.NewStorageError("spend boom")}

		_, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, 100)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, 0, f.finalizeCalls)
	})

	t.Run("propagates a finalize failure, still returns the spends and leaves them in place", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, finalizeErr: errors.NewStorageError("finalize boom")}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, 100)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, 1, f.finalizeCalls)
		require.Equal(t, f.spends, spends)
		require.Equal(t, 0, f.unspendCalls, "the record is still creating, so it stays the recovery journal")
	})

	t.Run("a finalize that finds the record gone or rolling back unspends its own spends", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, finalizeErr: errors.NewTxNotFoundError("rolling back")}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, 100)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.Equal(t, 1, f.unspendCalls)
		require.Equal(t, f.spends, f.unspentWith, "the spends this call made are its own to remove")
		require.Equal(t, f.spends, spends)
	})

	t.Run("a record that is rolling back is refused before any spend", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, existing: meta.Data{Creating: true, RollingBack: true}}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "rolling back")
		require.Nil(t, spends)
		require.Equal(t, 0, f.spendCalls, "a rolling back record must not have its parents spent")
		require.Equal(t, 0, f.finalizeCalls)
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("an absent record is refused before any spend", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, getMetaErr: errors.NewTxNotFoundError("gone")}

		_, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, 100)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.Equal(t, 0, f.spendCalls, "an absent record must not have its parents spent")
		require.Equal(t, 0, f.finalizeCalls)
	})

	t.Run("a failed spend never unspends", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {Err: errors.ErrSpent}}}

		_, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, 100)
		require.Error(t, err)
		require.Equal(t, 0, f.unspendCalls)
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
// scriptedTwoInputTx returns a tx with two inputs, each carrying the previous locking script
// that the spend hash needs.
func scriptedTwoInputTx(t *testing.T) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	for vout := uint32(0); vout < 2; vout++ {
		in := &bt.Input{PreviousTxOutIndex: vout, PreviousTxSatoshis: 1000, PreviousTxScript: bscript.NewFromBytes([]byte{0x51}), UnlockingScript: bscript.NewFromBytes([]byte{})}
		require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{byte(vout + 1)}))
		tx.Inputs = append(tx.Inputs, in)
	}

	return tx
}

func TestSequentialSpendAndCreate_CreateFirst(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tx := scriptedTwoInputTx(t)

	t.Run("orders create-tentative then spend then finalize", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}}, createMeta: &meta.Data{Creating: true}}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.NoError(t, err)
		require.Equal(t, []string{"create", "spend", "finalize"}, f.callSeq)
		require.True(t, f.lastCreateOpts.Creating, "the tentative create must carry WithCreating")
		require.Len(t, spends, 1)
		require.False(t, md.Creating, "returned meta must reflect the finalized state")
	})

	t.Run("spend failure marks the record rolling back, reverses the partial spends, then deletes it", func(t *testing.T) {
		shortenUnspendBackoff(t)

		free := &Spend{}
		spent := &Spend{Err: errors.ErrSpent}
		f := &fakeSequentialStore{
			spends:     []*Spend{spent, free},
			spendErr:   errors.NewUtxoError("double spend"),
			createMeta: &meta.Data{Creating: true},
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.Nil(t, md)
		require.Equal(t, []string{"create", "spend", "begin", "unspend", "delete"}, f.callSeq)
		require.Equal(t, 0, f.finalizeCalls)
		require.Len(t, f.unspentWith, 2, "every input of the tx is reversed, not only the spends that succeeded")
		require.Len(t, f.deletedHashes, 1)
		require.Equal(t, tx.TxIDChainHash().String(), f.deletedHashes[0].String())
		require.Len(t, spends, 2, "the caller still classifies the per-input errors")
		require.ErrorIs(t, spends[0].Err, errors.ErrSpent)
	})

	t.Run("spend failure with nothing succeeded still reverses every input and deletes the tentative record", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrSpent}},
			spendErr:   errors.NewUtxoError("double spend"),
			createMeta: &meta.Data{Creating: true},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.Equal(t, []string{"create", "spend", "begin", "unspend", "delete"}, f.callSeq)
		require.Len(t, f.unspentWith, 2, "an earlier attempt may have spent inputs this pass did not")
		require.Len(t, spends, 1)
	})

	t.Run("a spend that may still be in flight leaves the creating record without rollback", func(t *testing.T) {
		for name, tc := range map[string]struct {
			spends   []*Spend
			spendErr error
		}{
			"deadline":            {[]*Spend{{}, {}}, context.DeadlineExceeded},
			"cancelled":           {[]*Spend{{}, {}}, errors.NewContextCanceledError("spend did not finish", context.Canceled)},
			"service unavailable": {[]*Spend{{}, {}}, errors.NewServiceUnavailableError("spend batch did not complete")},
			"no per-input result": {nil, errors.NewStorageError("batch outcome unknown")},
		} {
			t.Run(name, func(t *testing.T) {
				f := &fakeSequentialStore{
					spends:     tc.spends,
					spendErr:   tc.spendErr,
					createMeta: &meta.Data{Creating: true},
				}

				md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
				require.Error(t, err)
				require.Nil(t, md)
				require.Equal(t, []string{"create", "spend"}, f.callSeq, "the spend may still commit, so nothing is marked, reversed or deleted")
				require.Equal(t, 0, f.beginCalls)
				require.Equal(t, 0, f.unspendCalls)
				require.Equal(t, 0, f.deleteCalls)
				require.Equal(t, tc.spends, spends)
			})
		}
	})

	t.Run("the fresh spend requires parent outputs", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}}, createMeta: &meta.Data{Creating: true}}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true, WithIgnoreLocked(true))
		require.NoError(t, err)
		require.Len(t, f.spendFlags, 1)
		require.True(t, f.spendFlags[0].RequireParentOutputs, "the record the spend would find is its own tentative one")
		require.True(t, f.spendFlags[0].IgnoreLocked, "the caller's other flags are kept")
	})

	t.Run("failed reversal leaves the record rolling back, wraps both errors and does not delete", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrSpent}, {}},
			spendErr:   errors.NewUtxoError("double spend"),
			createMeta: &meta.Data{Creating: true},
			unspendErrs: []error{
				errors.NewStorageError("u1"),
				errors.NewStorageError("u2"),
				errors.NewStorageError("u3"),
			},
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.Error(t, err)
		require.Nil(t, md)
		require.Equal(t, 3, f.unspendCalls)
		require.Equal(t, 1, f.beginCalls, "the marker is set before anything is undone")
		require.Equal(t, 0, f.deleteCalls, "a record whose spends are still in effect must not be deleted")
		require.Contains(t, err.Error(), "double spend")
		require.Contains(t, err.Error(), "u3")
		require.Len(t, spends, 2)
	})

	t.Run("a failed marker leaves the record creating and reverses nothing", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrSpent}, {}},
			spendErr:   errors.NewUtxoError("double spend"),
			createMeta: &meta.Data{Creating: true},
			beginErr:   errors.NewStorageError("begin boom"),
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "begin boom")
		require.Contains(t, err.Error(), "double spend")
		require.Equal(t, []string{"create", "spend", "begin"}, f.callSeq)
		require.Len(t, spends, 2)
	})

	t.Run("failed delete after the reversal is reported and leaves the record rolling back", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrSpent}, {}},
			spendErr:   errors.NewUtxoError("double spend"),
			createMeta: &meta.Data{Creating: true},
			deleteErr:  errors.NewStorageError("delete boom"),
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "delete boom")
		require.Contains(t, err.Error(), "double spend")
		require.Equal(t, []string{"create", "spend", "begin", "unspend", "delete"}, f.callSeq)
		require.Len(t, spends, 2)
	})

	t.Run("record finalized by a concurrent roll-forward is not deleted or unspent", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:        []*Spend{{Err: errors.ErrSpent}, {}},
			spendErr:      errors.NewStorageError("transient"),
			createMeta:    &meta.Data{Creating: true},
			beginDeclined: true,
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.Error(t, err)
		require.Nil(t, md)
		require.Equal(t, []string{"create", "spend", "begin"}, f.callSeq)
		require.Equal(t, 0, f.unspendCalls, "the finalizer relies on the spends that succeeded")
		require.Empty(t, f.deletedHashes)
		require.Len(t, spends, 2)
	})

	t.Run("record still creating has every input unspent and is then deleted", func(t *testing.T) {
		free := &Spend{}
		f := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrSpent}, free},
			spendErr:   errors.NewStorageError("transient"),
			createMeta: &meta.Data{Creating: true},
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.Error(t, err)
		require.Equal(t, []string{"create", "spend", "begin", "unspend", "delete"}, f.callSeq)
		require.Len(t, f.unspentWith, 2)
		require.Len(t, f.deletedHashes, 1)
		require.Equal(t, tx.TxIDChainHash().String(), f.deletedHashes[0].String())
	})

	t.Run("create ErrTxExists on a final record is an ordinary duplicate", func(t *testing.T) {
		f := &fakeSequentialStore{createErr: errors.NewTxExistsError("already there")}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.False(t, IsRolledForward(err))
		require.Nil(t, spends)
		require.Equal(t, []string{"create", "getmeta"}, f.callSeq, "a final record is neither spent nor finalized again")
	})

	t.Run("create ErrTxExists on a rolling-back record is a transient error and touches nothing", func(t *testing.T) {
		f := &fakeSequentialStore{
			createErr: errors.NewTxExistsError("already there"),
			existing:  meta.Data{Creating: true, RollingBack: true},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxExists)
		require.Contains(t, err.Error(), "rolling back")
		require.False(t, IsRolledForward(err))
		require.Nil(t, spends)
		require.Equal(t, []string{"create", "getmeta"}, f.callSeq, "no spend, finalize or delete")
	})

	t.Run("create ErrTxExists whose state cannot be read is an error", func(t *testing.T) {
		f := &fakeSequentialStore{createErr: errors.NewTxExistsError("already there"), getMetaErr: errors.NewStorageError("read boom")}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Equal(t, []string{"create", "getmeta"}, f.callSeq)
	})

	t.Run("create ErrTxExists on a creating record rolls it forward with the caller's flags", func(t *testing.T) {
		two := twoInputTx()
		f := &fakeSequentialStore{
			createErr: errors.NewTxExistsError("already there"),
			existing:  meta.Data{Creating: true},
			spends:    []*Spend{{}, {}},
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, two, 100, true, WithIgnoreLocked(true))
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.True(t, IsRolledForward(err))
		require.True(t, IsRolledForward(errors.NewProcessingError("wrapped by the validator", err)), "the marker survives a wrapper")
		require.Nil(t, md)
		require.Equal(t, f.spends, spends, "the spends reach the caller for the block assembly hand-off")
		require.Equal(t, []string{"create", "getmeta", "spend", "finalize"}, f.callSeq)
		require.Len(t, f.spendFlags, 1)
		require.True(t, f.spendFlags[0].IgnoreLocked, "the caller's flags reach the re-spend")
		require.False(t, f.spendFlags[0].RequireParentOutputs, "a roll-forward keeps the already-blessed fallback")
	})

	t.Run("create ErrTxExists roll-forward with a failed spend returns the failure and leaves the record", func(t *testing.T) {
		two := twoInputTx()
		f := &fakeSequentialStore{
			createErr: errors.NewTxExistsError("already there"),
			existing:  meta.Data{Creating: true},
			spends:    []*Spend{{}, {Err: errors.ErrSpent}},
			spendErr:  errors.NewUtxoError("double spend"),
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, two, 100, true)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.False(t, IsRolledForward(err))
		require.Len(t, spends, 2)
		require.Equal(t, []string{"create", "getmeta", "spend"}, f.callSeq, "no finalize, no rollback")
	})

	t.Run("create ErrTxExists roll-forward whose finalize finds the record gone unspends its own spends", func(t *testing.T) {
		shortenUnspendBackoff(t)

		two := twoInputTx()
		f := &fakeSequentialStore{
			createErr:   errors.NewTxExistsError("already there"),
			existing:    meta.Data{Creating: true},
			spends:      []*Spend{{}, {}},
			finalizeErr: errors.NewTxNotFoundError("rolling back"),
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, two, 100, true)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.False(t, IsRolledForward(err))
		require.Equal(t, f.spends, spends)
		require.Equal(t, []string{"create", "getmeta", "spend", "finalize", "unspend"}, f.callSeq)
		require.Equal(t, f.spends, f.unspentWith)
		require.Equal(t, 0, f.deleteCalls, "the rollback owner deletes, not the roll-forward")
	})

	t.Run("finalize failure after every retry returns an error and leaves the record creating", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeSequentialStore{
			spends:      []*Spend{{}},
			createMeta:  &meta.Data{Creating: true},
			finalizeErr: errors.NewStorageError("finalize boom"),
		}

		md, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.Error(t, err, "the inputs are spent but the tentative flag is not cleared, so the outputs are unspendable; reporting success would announce an unspendable tx")
		require.Equal(t, []string{"create", "spend", "finalize", "finalize", "finalize"}, f.callSeq)
		require.Equal(t, 3, f.finalizeCalls)
		require.NotNil(t, md)
		require.True(t, md.Creating, "the record is left creating for recovery (re-encounter roll-forward / sweep / setMined)")
		require.Equal(t, 0, f.unspendCalls)
		require.Equal(t, 0, f.deleteCalls)
	})

	t.Run("finalize that fails twice then succeeds clears the creating flag", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeSequentialStore{
			spends:       []*Spend{{}},
			createMeta:   &meta.Data{Creating: true},
			finalizeErrs: []error{errors.NewStorageError("f1"), errors.NewStorageError("f2")},
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.NoError(t, err)
		require.Equal(t, 3, f.finalizeCalls)
		require.False(t, md.Creating)
		require.Len(t, spends, 1)
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

// TestRollBackCreating pins the rollback order: mark, reverse every input of the tx,
// then delete, and the cases that must do less.
func TestRollBackCreating(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tx := scriptedTwoInputTx(t)
	noInputs := bt.NewTx()

	t.Run("marks, unspends every input of the tx, then deletes", func(t *testing.T) {
		f := &fakeSequentialStore{}

		err := RollBackCreating(ctx, logger, f, tx)
		require.NoError(t, err)
		require.Equal(t, []string{"begin", "unspend", "delete"}, f.callSeq)
		require.Len(t, f.unspentWith, 2)

		for i, sp := range f.unspentWith {
			require.Equal(t, tx.Inputs[i].PreviousTxIDChainHash().String(), sp.TxID.String())
			require.Equal(t, tx.Inputs[i].PreviousTxOutIndex, sp.Vout)
			require.Equal(t, tx.TxIDChainHash().String(), sp.SpendingData.TxID.String())
			require.Equal(t, i, sp.SpendingData.Vin)
		}

		require.Len(t, f.deletedHashes, 1)
		require.Equal(t, tx.TxIDChainHash().String(), f.deletedHashes[0].String())
	})

	t.Run("a marker that reports false does nothing more and is not an error", func(t *testing.T) {
		f := &fakeSequentialStore{beginDeclined: true}

		err := RollBackCreating(ctx, logger, f, tx)
		require.NoError(t, err)
		require.Equal(t, []string{"begin"}, f.callSeq)
	})

	t.Run("a failed marker returns the error and does nothing more", func(t *testing.T) {
		f := &fakeSequentialStore{beginErr: errors.NewStorageError("begin boom")}

		err := RollBackCreating(ctx, logger, f, tx)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, []string{"begin"}, f.callSeq)
	})

	t.Run("a failed unspend skips the delete and returns the error", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeSequentialStore{unspendErrs: []error{errors.NewStorageError("u1"), errors.NewStorageError("u2"), errors.NewStorageError("u3")}}

		err := RollBackCreating(ctx, logger, f, tx)
		require.Error(t, err)
		require.Equal(t, 3, f.unspendCalls)
		require.Equal(t, 0, f.deleteCalls)
	})

	t.Run("a parent that is already gone does not fail the rollback", func(t *testing.T) {
		f := &fakeSequentialStore{unspendErrs: []error{errors.NewTxNotFoundError("gone")}}

		err := RollBackCreating(ctx, logger, f, tx)
		require.NoError(t, err)
		require.Equal(t, 3, f.unspendCalls, "one batch call, then one call per input")
		require.Equal(t, []string{"begin", "unspend", "unspend", "unspend", "delete"}, f.callSeq)
	})

	t.Run("an input whose UTXO hash does not match is skipped and the other input is unspent", func(t *testing.T) {
		f := &fakeSequentialStore{unspendErrs: []error{
			errors.NewUtxoHashMismatchError("mismatch"),
			nil,
			errors.NewUtxoHashMismatchError("mismatch"),
		}}

		err := RollBackCreating(ctx, logger, f, tx)
		require.NoError(t, err)
		require.Equal(t, []string{"begin", "unspend", "unspend", "unspend", "delete"}, f.callSeq)
		require.Len(t, f.unspentBatches, 3)
		require.Len(t, f.unspentBatches[0], 2, "the first call carries every input")
		require.Len(t, f.unspentBatches[1], 1)
		require.Equal(t, 0, f.unspentBatches[1][0].SpendingData.Vin, "input 0 is unspent on its own")
		require.Len(t, f.unspentBatches[2], 1)
		require.Equal(t, 1, f.unspentBatches[2][0].SpendingData.Vin, "input 1 is the one skipped")
		require.Equal(t, 1, f.deleteCalls)
	})

	t.Run("a tx without inputs still deletes", func(t *testing.T) {
		f := &fakeSequentialStore{}

		err := RollBackCreating(ctx, logger, f, noInputs)
		require.NoError(t, err)
		require.Equal(t, []string{"begin", "delete"}, f.callSeq)
	})

	t.Run("it runs under its own context after the caller is cancelled", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		f := &fakeSequentialStore{}

		err := RollBackCreating(cancelled, logger, f, tx)
		require.NoError(t, err)
		require.Equal(t, []string{"begin", "unspend", "delete"}, f.callSeq)
	})
}

func TestIsRolledForward(t *testing.T) {
	require.False(t, IsRolledForward(nil))
	require.False(t, IsRolledForward(errors.NewTxExistsError("plain")))
	require.True(t, IsRolledForward(MarkRolledForward(errors.NewTxExistsError("marked"))))
	require.True(t, IsRolledForward(errors.NewProcessingError("wrapped", MarkRolledForward(errors.NewTxExistsError("marked")))))
}
