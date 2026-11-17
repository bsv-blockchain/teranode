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
	existing meta.Data
	// existingSeq is popped once per GetMeta call; existing applies when exhausted.
	existingSeq []meta.Data
	getMetaErr  error
	// unspendErrs is popped once per Unspend call; nil result when exhausted.
	unspendErrs []error
	// createErrs is popped once per Create call; createErr applies when exhausted.
	createErrs []error
	// leaseDeclined makes TakeSpendLease report that the record refuses a spender.
	leaseDeclined bool
	leaseErr      error
	unleaseErr    error
	// leasesLive is the number of SpendLeasesLive calls that report a live lease before one
	// reports none; a negative value reports a live lease on every call.
	leasesLive int
	leasesErr  error
	// clearResult and clearErr are what ClearCreateFirstHold answers.
	clearResult bool
	clearErr    error
	// takeDeclined makes TakeRollbackHold report that the hold is not the caller's to take.
	takeDeclined bool
	takeErr      error
	takeCalls    int
	takeTokens   [][]byte
	// endErr is what EndCreateFirstRollback answers.
	endErr error

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
	leaseCalls     int
	unleaseCalls   int
	leasesCalls    int
	releaseCalls   int
	releaseErr     error
	clearCalls     int
	clearAdopt     []bool
	finalizeTokens [][]byte
	beginTokens    [][]byte
	endTokens      [][]byte
	leaseTokens    [][]byte
	leasesTokens   [][]byte
	// unleased holds, per ReleaseSpendLease call, the attempt and the lease it named.
	unleased       [][2][]byte
	spendDeadlines []time.Time
	lastCreateOpts CreateOptions
	callSeq        []string
}

func (f *fakeSequentialStore) Spend(ctx context.Context, _ *bt.Tx, _ uint32, ignoreFlags ...IgnoreFlags) ([]*Spend, error) {
	deadline, _ := ctx.Deadline()
	f.spendDeadlines = append(f.spendDeadlines, deadline)
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

	if len(f.createErrs) > 0 {
		err := f.createErrs[0]
		f.createErrs = f.createErrs[1:]

		return f.createMeta, err
	}

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

type laneSequentialStore struct {
	fakeSequentialStore
	LanePositions
}

func TestSequentialSpendAndCreateRejectsInvalidPositionBeforeSpending(t *testing.T) {
	f := &laneSequentialStore{}
	f.SetLanePositionSource(fixedLanePosition(1))
	_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, f, bt.NewTx(), 100, false,
		WithLanePosition(1<<56))
	require.Error(t, err)
	require.Zero(t, f.spendCalls, "invalid named metadata must fail before the first spend")
	require.Zero(t, f.createCalls)
	require.Zero(t, f.unspendCalls, "there must be nothing to roll back")
}

func (b *boundedSequentialStore) SpendRollbackTimeout() time.Duration { return b.bound }

func (f *fakeSequentialStore) FinalizeTransaction(_ context.Context, _ *bt.Tx, attempt []byte) error {
	f.finalizeCalls++
	f.finalizeTokens = append(f.finalizeTokens, attempt)
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

func (f *fakeSequentialStore) EndCreateFirstRollback(ctx context.Context, hash *chainhash.Hash, attempt []byte) error {
	f.endTokens = append(f.endTokens, attempt)

	if f.endErr != nil {
		f.callSeq = append(f.callSeq, "delete")

		return f.endErr
	}

	return f.DeleteComplete(ctx, hash)
}

// fakeLease is the lease the fakes hand out from TakeSpendLease.
var fakeLease = []byte{0xAA}

func (f *fakeSequentialStore) TakeSpendLease(_ context.Context, _ *chainhash.Hash, attempt []byte) ([]byte, bool, error) {
	f.leaseCalls++
	f.leaseTokens = append(f.leaseTokens, attempt)
	f.callSeq = append(f.callSeq, "lease")

	if f.leaseErr != nil {
		return nil, false, f.leaseErr
	}

	if f.leaseDeclined {
		return nil, false, nil
	}

	return fakeLease, true, nil
}

func (f *fakeSequentialStore) ReleaseSpendLease(_ context.Context, _ *chainhash.Hash, attempt, lease []byte) error {
	f.unleaseCalls++
	f.unleased = append(f.unleased, [2][]byte{attempt, lease})
	f.callSeq = append(f.callSeq, "unlease")

	return f.unleaseErr
}

func (f *fakeSequentialStore) SpendLeasesLive(_ context.Context, _ *chainhash.Hash, attempt []byte) (bool, error) {
	f.leasesCalls++
	f.leasesTokens = append(f.leasesTokens, attempt)
	f.callSeq = append(f.callSeq, "leases")

	if f.leasesErr != nil {
		return false, f.leasesErr
	}

	if f.leasesLive < 0 {
		return true, nil
	}

	if f.leasesLive > 0 {
		f.leasesLive--

		return true, nil
	}

	return false, nil
}

func (f *fakeSequentialStore) TakeRollbackHold(_ context.Context, _ *chainhash.Hash, attempt []byte) (bool, error) {
	f.takeCalls++
	f.takeTokens = append(f.takeTokens, attempt)
	f.callSeq = append(f.callSeq, "take")

	if f.takeErr != nil {
		return false, f.takeErr
	}

	return !f.takeDeclined, nil
}

func (f *fakeSequentialStore) ReleaseRollbackHold(_ context.Context, _ *chainhash.Hash, _ []byte) error {
	f.releaseCalls++
	f.callSeq = append(f.callSeq, "release")

	return f.releaseErr
}

func (f *fakeSequentialStore) ClearCreateFirstHold(_ context.Context, _ *chainhash.Hash, adopt bool) (bool, error) {
	f.clearCalls++
	f.clearAdopt = append(f.clearAdopt, adopt)
	f.callSeq = append(f.callSeq, "clear")

	return f.clearResult, f.clearErr
}

func (f *fakeSequentialStore) BeginCreateFirstRollback(_ context.Context, _ *chainhash.Hash, attempt []byte) (bool, error) {
	f.beginCalls++
	f.beginTokens = append(f.beginTokens, attempt)
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

	if len(f.existingSeq) > 0 {
		*data = f.existingSeq[0]
		f.existingSeq = f.existingSeq[1:]

		return nil
	}

	*data = f.existing

	return nil
}

func shortenSpendLeaseWait(t *testing.T) {
	t.Helper()

	orig := spendLeaseWait
	spendLeaseWait = 30 * time.Millisecond

	t.Cleanup(func() { spendLeaseWait = orig })
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
		require.Equal(t, []string{"spend", "create", "getmeta"}, f.callSeq, "the record is read once to tell a duplicate from a hold")
		require.Equal(t, 0, f.clearCalls, "a duplicate is not a hold")
	})

	t.Run("create ErrTxExists whose record cannot be read keeps the spends in place", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:     []*Spend{{}},
			createErr:  errors.NewTxExistsError("already there"),
			getMetaErr: errors.NewStorageError("read boom"),
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.Len(t, spends, 1)
		require.Equal(t, 0, f.unspendCalls)
		require.Equal(t, 0, f.clearCalls)
		require.Equal(t, 1, f.createCalls)
	})

	t.Run("create over a hold that is cleared creates again and keeps the spends", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:      []*Spend{{}},
			createErrs:  []error{errors.NewTxExistsError("held")},
			createMeta:  &meta.Data{},
			existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
			clearResult: true,
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.NoError(t, err)
		require.Same(t, f.createMeta, md)
		require.Len(t, spends, 1)
		require.Equal(t, []string{"spend", "create", "getmeta", "clear", "create"}, f.callSeq)
		require.Equal(t, []bool{false}, f.clearAdopt, "outside block context only an expired hold goes")
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("create over a hold that stands reverses the spends and is a retryable error", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:    []*Spend{{}, {}},
			createErr: errors.NewTxExistsError("held"),
			existing:  meta.Data{Creating: true, RollingBack: true, Hold: true},
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxExists, "a hold is not a duplicate: the caller must not treat the tx as stored")
		require.Nil(t, md)
		require.Nil(t, spends, "the spends were reversed, so none is in effect")
		require.Equal(t, []string{"spend", "create", "getmeta", "clear", "unspend"}, f.callSeq)
		require.Equal(t, f.spends, f.unspentWith)
		require.Equal(t, 1, f.createCalls, "a hold that stands is not created over")
	})

	t.Run("create over a hold that cannot be cleared reverses the spends and is a retryable error", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:    []*Spend{{}},
			createErr: errors.NewTxExistsError("held"),
			existing:  meta.Data{Creating: true, RollingBack: true, Hold: true},
			clearErr:  errors.NewStorageError("clear boom"),
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxExists)
		require.Contains(t, err.Error(), "clear boom")
		require.Nil(t, spends)
		require.Equal(t, []string{"spend", "create", "getmeta", "clear", "unspend"}, f.callSeq)
		require.Equal(t, 1, f.createCalls)
	})

	t.Run("create over a hold adopts it in block context only", func(t *testing.T) {
		for name, tc := range map[string]struct {
			opts  []CreateOption
			adopt bool
		}{
			"no block context": {nil, false},
			"ignore locked":    {[]CreateOption{WithIgnoreLocked(true)}, true},
			"mined block info": {[]CreateOption{WithMinedBlockInfo(MinedBlockInfo{BlockID: 1, BlockHeight: 99})}, true},
		} {
			f := &fakeSequentialStore{
				spends:      []*Spend{{}},
				createErrs:  []error{errors.NewTxExistsError("held")},
				createMeta:  &meta.Data{},
				existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
				clearResult: true,
			}

			_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, tc.opts...)
			require.NoError(t, err, name)
			require.Equal(t, []bool{tc.adopt}, f.clearAdopt, name)
		}
	})

	t.Run("create over a record whose rollback is still open reverses the spends and is a retryable error", func(t *testing.T) {
		for name, existing := range map[string]meta.Data{
			"rolling back":              {Creating: true, RollingBack: true},
			"rolling back and reversed": {Creating: true, RollingBack: true, Reversed: true},
		} {
			f := &fakeSequentialStore{
				spends:    []*Spend{{}, {}},
				createErr: errors.NewTxExistsError("rolling back"),
				existing:  existing,
			}

			md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
			require.ErrorIs(t, err, errors.ErrProcessing, name)
			require.NotErrorIs(t, err, errors.ErrTxExists, "%s: a record that is being rolled back is not a stored transaction", name)
			require.Contains(t, err.Error(), "is being rolled back", name)
			require.Nil(t, md, name)
			require.Nil(t, spends, "%s: the rollback reverses the inputs only once, so this call reverses its own spends", name)
			require.Equal(t, []string{"spend", "create", "getmeta", "unspend"}, f.callSeq, name)
			require.Equal(t, f.spends, f.unspentWith, name)
			require.Equal(t, 0, f.clearCalls, "%s: a record that is not a hold is not cleared", name)
			require.Equal(t, 0, f.takeCalls, "%s: the spend-first path does not finish another attempt's rollback", name)
		}
	})

	t.Run("create ErrTxExists on a record that is creating and not rolling back keeps the spends in place", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:    []*Spend{{}},
			createErr: errors.NewTxExistsError("tentative"),
			existing:  meta.Data{Creating: true},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.Len(t, spends, 1)
		require.Equal(t, []string{"spend", "create", "getmeta"}, f.callSeq)
	})

	t.Run("a create-only call over a hold that is cleared creates again", func(t *testing.T) {
		for name, tc := range map[string]struct {
			opts  []CreateOption
			adopt bool
		}{
			"no block context": {[]CreateOption{WithCreateOnly()}, false},
			"mined block info": {[]CreateOption{WithCreateOnly(), WithMinedBlockInfo(MinedBlockInfo{BlockID: 1, BlockHeight: 99})}, true},
		} {
			f := &fakeSequentialStore{
				createErrs:  []error{errors.NewTxExistsError("held")},
				createMeta:  &meta.Data{},
				existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
				clearResult: true,
			}

			md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, tc.opts...)
			require.NoError(t, err, name)
			require.Same(t, f.createMeta, md, name)
			require.Nil(t, spends, name)
			require.Equal(t, []string{"create", "getmeta", "clear", "create"}, f.callSeq, name)
			require.Equal(t, []bool{tc.adopt}, f.clearAdopt, name)
		}
	})

	t.Run("a create-only call over a hold that stands returns ErrTxExists unchanged", func(t *testing.T) {
		for name, clearErr := range map[string]error{
			"not cleared": nil,
			"clear error": errors.NewStorageError("clear boom"),
		} {
			existsErr := errors.NewTxExistsError("held")
			f := &fakeSequentialStore{
				createErr: existsErr,
				existing:  meta.Data{Creating: true, RollingBack: true, Hold: true},
				clearErr:  clearErr,
			}

			_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, WithCreateOnly())
			require.Same(t, existsErr, err, "%s: the caller of a single-phase create resolves the hold itself", name)
			require.Nil(t, spends, name)
			require.Equal(t, []string{"create", "getmeta", "clear"}, f.callSeq, name)
			require.Equal(t, 0, f.unspendCalls, name)
		}
	})

	t.Run("a create-only call over a record whose rollback is still open returns ErrTxExists unchanged", func(t *testing.T) {
		existsErr := errors.NewTxExistsError("rolling back")
		f := &fakeSequentialStore{
			createErr: existsErr,
			existing:  meta.Data{Creating: true, RollingBack: true},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, WithCreateOnly())
		require.Same(t, existsErr, err)
		require.Nil(t, spends)
		require.Equal(t, []string{"create", "getmeta"}, f.callSeq, "no clear, no unspend: a create-only call made no spend")
	})

	t.Run("a create-only duplicate, or one whose record cannot be read, returns ErrTxExists unchanged", func(t *testing.T) {
		for name, f := range map[string]*fakeSequentialStore{
			"duplicate":  {createErr: errors.NewTxExistsError("there")},
			"read error": {createErr: errors.NewTxExistsError("there"), getMetaErr: errors.NewStorageError("read boom"), existing: meta.Data{RollingBack: true, Hold: true}},
		} {
			_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, WithCreateOnly())
			require.ErrorIs(t, err, errors.ErrTxExists, name)
			require.Equal(t, []string{"create", "getmeta"}, f.callSeq, name)
		}
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

// fakeRollForwarder scripts the CreatingRollForwarder surface (TakeSpendLease, SpendAndCreate,
// FinalizeTransaction, ReleaseSpendLease, GetMeta) so RollForwardCreating can be tested in
// isolation. The surface has no Unspend: a roll-forward cannot reverse a spend.
type fakeRollForwarder struct {
	spends        []*Spend
	spendErr      error
	finalizeErr   error
	finalizeCalls int
	lastOpts      CreateOptions
	spendCalls    int
	existing      meta.Data
	getMetaErr    error
	getMetaCalls  int
	leaseDeclined bool
	leaseErr      error
	leaseCalls    int
	unleaseCalls  int
	leaseTokens   [][]byte
	// unleased holds, per ReleaseSpendLease call, the attempt and the lease it named.
	unleased      [][2][]byte
	finalizeToken []byte
	callSeq       []string

	leaseDeadline    time.Time
	spendDeadline    time.Time
	finalizeDeadline time.Time
}

func (f *fakeRollForwarder) TakeSpendLease(ctx context.Context, _ *chainhash.Hash, attempt []byte) ([]byte, bool, error) {
	f.leaseDeadline, _ = ctx.Deadline()
	f.leaseCalls++
	f.leaseTokens = append(f.leaseTokens, attempt)
	f.callSeq = append(f.callSeq, "lease")

	if f.leaseErr != nil {
		return nil, false, f.leaseErr
	}

	if f.leaseDeclined {
		return nil, false, nil
	}

	return fakeLease, true, nil
}

func (f *fakeRollForwarder) ReleaseSpendLease(_ context.Context, _ *chainhash.Hash, attempt, lease []byte) error {
	f.unleaseCalls++
	f.unleased = append(f.unleased, [2][]byte{attempt, lease})
	f.callSeq = append(f.callSeq, "unlease")

	return nil
}

func (f *fakeRollForwarder) GetMeta(_ context.Context, _ *chainhash.Hash, data *meta.Data) error {
	f.getMetaCalls++
	f.callSeq = append(f.callSeq, "getmeta")

	if f.getMetaErr != nil {
		return f.getMetaErr
	}

	*data = f.existing

	return nil
}

func (f *fakeRollForwarder) SpendAndCreate(ctx context.Context, _ *bt.Tx, _ uint32, opts ...CreateOption) (*meta.Data, []*Spend, error) {
	f.spendDeadline, _ = ctx.Deadline()

	o := CreateOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	f.lastOpts = o
	f.spendCalls++
	f.callSeq = append(f.callSeq, "spend")

	return nil, f.spends, f.spendErr
}

func (f *fakeRollForwarder) FinalizeTransaction(ctx context.Context, _ *bt.Tx, attempt []byte) error {
	f.finalizeDeadline, _ = ctx.Deadline()
	f.finalizeCalls++
	f.finalizeToken = attempt
	f.callSeq = append(f.callSeq, "finalize")

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

	t.Run("takes the lease, re-spends every input, finalizes and returns the spends", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.NoError(t, err)
		require.True(t, f.lastOpts.SpendOnly, "roll-forward must re-run the spend-only phase")
		require.Equal(t, []string{"lease", "spend", "finalize"}, f.callSeq, "the lease is taken before any input is spent, and the finalize ends it")
		require.Equal(t, [][]byte{{1}}, f.leaseTokens, "the lease names the attempt being recovered")
		require.Equal(t, 0, f.unleaseCalls, "a finalized record carries no lease to release")
		require.Equal(t, f.spends, spends, "the spends must reach the caller so it can hand the tx to block assembly")
	})

	t.Run("threads caller ignore-flags into the re-spend", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}}

		_, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100, WithIgnoreLocked(true))
		require.NoError(t, err)
		require.True(t, f.lastOpts.SpendOnly, "must still be spend-only")
		require.True(t, f.lastOpts.IgnoreFlags.IgnoreLocked, "the caller's IgnoreLocked must reach the spend, so a locked parent does not defeat recovery")
	})

	t.Run("fails closed and releases the lease when the re-spend covers fewer inputs than the tx has", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}}} // 1 spend for a 2-input tx

		_, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.Error(t, err)
		require.Equal(t, []string{"lease", "spend", "unlease"}, f.callSeq, "an incomplete re-spend must NOT finalize, and its lease is released")
		require.Equal(t, [][2][]byte{{{1}, fakeLease}}, f.unleased, "the release names the attempt and the lease that was taken")
	})

	t.Run("fails closed and releases the lease on a per-input spend error", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {Err: errors.ErrSpent}}}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
		require.Error(t, err)
		require.Equal(t, []string{"lease", "spend", "unlease"}, f.callSeq)
		require.Equal(t, f.spends, spends, "the per-input errors must reach the caller")
	})

	t.Run("a spend error with a known outcome releases the lease without finalizing", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {Err: errors.ErrSpent}}, spendErr: errors.NewUtxoError("double spend")}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.Equal(t, f.spends, spends)
		require.Equal(t, []string{"lease", "spend", "unlease"}, f.callSeq, "the spend returned, so a rollback need not wait for this spender")
		require.Equal(t, [][2][]byte{{{1}, fakeLease}}, f.unleased)
	})

	t.Run("a spend whose outcome is unknown keeps its lease", func(t *testing.T) {
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
				f := &fakeRollForwarder{spends: tc.spends, spendErr: tc.spendErr}

				spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
				require.Error(t, err)
				require.Equal(t, tc.spends, spends)
				require.Equal(t, []string{"lease", "spend"}, f.callSeq, "the spend may still land, so the lease must stay live for a rollback to wait on")
				require.Equal(t, 0, f.unleaseCalls)
				require.Equal(t, 0, f.finalizeCalls)
			})
		}
	})

	t.Run("a finalize failure releases the lease, returns the spends and leaves them in place", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, finalizeErr: errors.NewStorageError("finalize boom")}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, f.spends, spends)
		require.Equal(t, []string{"lease", "spend", "finalize", "unlease"}, f.callSeq, "the record is still creating, so it stays the recovery journal and nothing is reversed")
		require.Equal(t, [][2][]byte{{{1}, fakeLease}}, f.unleased)
	})

	t.Run("a finalize that finds the record gone or rolling back releases the lease and reverses nothing", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, finalizeErr: errors.NewTxNotFoundError("rolling back")}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.Equal(t, f.spends, spends)
		require.Equal(t, []string{"lease", "spend", "finalize", "unlease"}, f.callSeq,
			"the owner of the rollback waits for this lease and then reverses every input; the roll-forward only releases it")
		require.Equal(t, [][2][]byte{{{1}, fakeLease}}, f.unleased)
		require.Equal(t, 0, f.getMetaCalls, "no read and no take of a hold follow a refused finalize")
	})

	t.Run("the finalize carries the token the caller names", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, existing: meta.Data{Creating: true, Attempt: []byte{4, 5}}}

		_, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{4, 5}, 100)
		require.NoError(t, err)
		require.Equal(t, []byte{4, 5}, f.finalizeToken)
		require.Equal(t, [][]byte{{4, 5}}, f.leaseTokens)
	})

	t.Run("a lease refused for a record creating under another attempt spends nothing", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, leaseDeclined: true, existing: meta.Data{Creating: true, Attempt: []byte{2}}}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Contains(t, err.Error(), "another attempt")
		require.Nil(t, spends)
		require.Equal(t, []string{"lease", "getmeta"}, f.callSeq, "a stale roll-forward must not spend the parents of a later attempt")
		require.Equal(t, 0, f.unleaseCalls, "nothing was taken, so nothing is released")
	})

	t.Run("a lease refused for a record that is rolling back spends nothing", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, leaseDeclined: true, existing: meta.Data{Creating: true, RollingBack: true}}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Contains(t, err.Error(), "rolling back")
		require.Nil(t, spends)
		require.Equal(t, []string{"lease", "getmeta"}, f.callSeq, "a rolling back record must not have its parents spent")
		require.Equal(t, 0, f.unleaseCalls)
	})

	t.Run("a lease refused for an absent record spends nothing", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, leaseDeclined: true, getMetaErr: errors.NewTxNotFoundError("gone")}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.Nil(t, spends)
		require.Equal(t, []string{"lease", "getmeta"}, f.callSeq, "an absent record must not have its parents spent")
	})

	t.Run("a lease refused although the record is creating under this attempt is a retryable error", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, leaseDeclined: true, existing: meta.Data{Creating: true, Attempt: []byte{1}}}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Nil(t, spends)
		require.Equal(t, []string{"lease", "getmeta"}, f.callSeq, "without a lease nothing is spent, whatever the read says")
	})

	t.Run("a lease refused for a final record returns the spends of the tx without spending or finalizing", func(t *testing.T) {
		scripted := scriptedTwoInputTx(t)
		f := &fakeRollForwarder{leaseDeclined: true}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, scripted, []byte{1}, 100)
		require.NoError(t, err, "another actor completed the record")
		require.Equal(t, []string{"lease", "getmeta"}, f.callSeq, "a final record is neither spent nor finalized again")
		require.Len(t, spends, 2)

		for i, sp := range spends {
			require.Equal(t, scripted.Inputs[i].PreviousTxIDChainHash().String(), sp.TxID.String())
			require.Equal(t, scripted.Inputs[i].PreviousTxOutIndex, sp.Vout)
			require.Equal(t, scripted.TxIDChainHash().String(), sp.SpendingData.TxID.String())
			require.NoError(t, sp.Err)
		}
	})

	t.Run("a lease that cannot be taken returns the error and spends nothing", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, leaseErr: errors.NewStorageError("lease boom")}

		spends, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Nil(t, spends)
		require.Equal(t, []string{"lease"}, f.callSeq, "a lease that may or may not be registered is never spent under")
	})

	t.Run("the lease, the spend and the finalize run under a context bounded by the rollback bound", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}}

		before := time.Now()
		_, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
		require.NoError(t, err)

		for name, deadline := range map[string]time.Time{"lease": f.leaseDeadline, "spend": f.spendDeadline, "finalize": f.finalizeDeadline} {
			require.False(t, deadline.IsZero(), "the %s must have a deadline", name)
			require.WithinDuration(t, before.Add(defaultSequentialRollbackTimeout), deadline, 5*time.Second, "the %s deadline is the rollback bound", name)
		}

		require.Equal(t, f.leaseDeadline, f.spendDeadline, "the spend ends when its lease does")
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
		require.Equal(t, 0, f.leaseCalls, "the create writes the creator's lease, so none is taken")
		require.Equal(t, 0, f.unleaseCalls, "the finalize ends the lease")
	})

	t.Run("the fresh spend ends at the rollback bound after the create", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}}, createMeta: &meta.Data{Creating: true}}

		before := time.Now()
		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.NoError(t, err)
		require.Len(t, f.spendDeadlines, 1)
		require.False(t, f.spendDeadlines[0].IsZero(), "the spend must not outlive the lease the create wrote")
		require.WithinDuration(t, before.Add(defaultSequentialRollbackTimeout), f.spendDeadlines[0], 5*time.Second)
		require.False(t, f.spendDeadlines[0].After(time.Now().Add(defaultSequentialRollbackTimeout)), "the deadline is read before the create, never after it")
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
		require.Equal(t, []string{"create", "spend", "unlease", "begin", "leases", "unspend", "release", "delete"}, f.callSeq,
			"the creator releases its own lease before it claims the rollback, and reverses only once no lease is live")
		require.Equal(t, 0, f.finalizeCalls)
		require.Len(t, f.unspentWith, 2, "every input of the tx is reversed, not only the spends that succeeded")
		require.Len(t, f.deletedHashes, 1)
		require.Equal(t, tx.TxIDChainHash().String(), f.deletedHashes[0].String())
		require.Len(t, spends, 2, "the caller still classifies the per-input errors")
		require.ErrorIs(t, spends[0].Err, errors.ErrSpent)
	})

	t.Run("the fresh path finalizes and rolls back the attempt its create returned", func(t *testing.T) {
		shortenUnspendBackoff(t)

		fin := &fakeSequentialStore{spends: []*Spend{{}, {}}, createMeta: &meta.Data{Creating: true, Attempt: []byte{8, 8}}}

		_, _, err := SequentialSpendAndCreate(ctx, logger, fin, tx, 100, true)
		require.NoError(t, err)
		require.Equal(t, [][]byte{{8, 8}}, fin.finalizeTokens)

		rb := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrSpent}, {}},
			spendErr:   errors.NewUtxoError("double spend"),
			createMeta: &meta.Data{Creating: true, Attempt: []byte{9, 9}},
		}

		_, _, err = SequentialSpendAndCreate(ctx, logger, rb, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.Equal(t, [][2][]byte{{{9, 9}, {9, 9}}}, rb.unleased, "the creator's lease is its attempt token")
		require.Equal(t, [][]byte{{9, 9}}, rb.beginTokens)
		require.Equal(t, [][]byte{{9, 9}}, rb.leasesTokens)
		require.Equal(t, [][]byte{{9, 9}}, rb.endTokens)
	})

	t.Run("spend failure with nothing succeeded still reverses every input and deletes the tentative record", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrSpent}},
			spendErr:   errors.NewUtxoError("double spend"),
			createMeta: &meta.Data{Creating: true},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.Equal(t, []string{"create", "spend", "unlease", "begin", "leases", "unspend", "release", "delete"}, f.callSeq)
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
				require.Equal(t, []string{"create", "spend"}, f.callSeq, "the spend may still commit, so nothing is marked, reversed or deleted, and the lease stays live")
				require.Equal(t, 0, f.unleaseCalls)
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
		require.Equal(t, []string{"create", "spend", "unlease", "begin"}, f.callSeq)
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
		require.Equal(t, []string{"create", "spend", "unlease", "begin", "leases", "unspend", "release", "delete"}, f.callSeq)
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
		require.Equal(t, []string{"create", "spend", "unlease", "begin"}, f.callSeq)
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
		require.Equal(t, []string{"create", "spend", "unlease", "begin", "leases", "unspend", "release", "delete"}, f.callSeq)
		require.Len(t, f.unspentWith, 2)
		require.Len(t, f.deletedHashes, 1)
		require.Equal(t, tx.TxIDChainHash().String(), f.deletedHashes[0].String())
	})

	t.Run("a rollback that finds a spender's lease live waits for it and then reverses", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrSpent}, {}},
			spendErr:   errors.NewUtxoError("double spend"),
			createMeta: &meta.Data{Creating: true, Attempt: []byte{9}},
			leasesLive: 2,
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.NotContains(t, err.Error(), "reversing", "the rollback completed")
		require.Equal(t, []string{"create", "spend", "unlease", "begin", "leases", "leases", "leases", "unspend", "release", "delete"}, f.callSeq,
			"a concurrent roll-forward's spend lands before the reversal, so the reversal undoes it")
	})

	t.Run("a rollback whose spender never releases its lease reverses nothing and is reported", func(t *testing.T) {
		shortenSpendLeaseWait(t)

		f := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrSpent}, {}},
			spendErr:   errors.NewUtxoError("double spend"),
			createMeta: &meta.Data{Creating: true, Attempt: []byte{9}},
			leasesLive: -1,
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Contains(t, err.Error(), "double spend")
		require.Contains(t, err.Error(), "still live")
		require.Len(t, spends, 2)
		require.Equal(t, 0, f.unspendCalls, "a spend may still land, so a reversal now could be undone by it")
		require.Equal(t, 0, f.deleteCalls, "the record stays rolling back as the journal for the sweep")
		require.Equal(t, 1, f.releaseCalls, "the hold is released so the sweep can take the record")
	})

	t.Run("create ErrTxExists on a final record is an ordinary duplicate", func(t *testing.T) {
		f := &fakeSequentialStore{createErr: errors.NewTxExistsError("already there")}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.False(t, IsRolledForward(err))
		require.Nil(t, spends)
		require.Equal(t, []string{"create", "getmeta"}, f.callSeq, "a final record is neither spent nor finalized again")
	})

	t.Run("create ErrTxExists on a hold is a transient error and touches nothing", func(t *testing.T) {
		f := &fakeSequentialStore{
			createErr: errors.NewTxExistsError("already there"),
			existing:  meta.Data{Creating: true, RollingBack: true, Hold: true},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxExists)
		require.Contains(t, err.Error(), "rolling back")
		require.False(t, IsRolledForward(err))
		require.Nil(t, spends)
		require.Equal(t, []string{"create", "getmeta", "clear"}, f.callSeq, "no spend, finalize or delete; the store is only asked to clear an expired hold")
		require.Equal(t, []bool{false}, f.clearAdopt, "a create outside block context never adopts a hold")
		require.Equal(t, 1, f.createCalls, "a hold that is not cleared is not retried")
		require.Equal(t, 0, f.takeCalls, "a hold has no open rollback to finish")
	})

	t.Run("create ErrTxExists on an open rollback finishes it and returns a transient error when the hold stands", func(t *testing.T) {
		f := &fakeSequentialStore{
			createErr: errors.NewTxExistsError("already there"),
			existing:  meta.Data{Creating: true, RollingBack: true, Attempt: []byte{4}},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxExists)
		require.Contains(t, err.Error(), "rolling back")
		require.Nil(t, spends)
		require.Equal(t, []string{"create", "getmeta", "take", "leases", "unspend", "release", "delete", "clear"}, f.callSeq,
			"the hold is taken, the leases are read, every input is unspent, the hold is released, the rollback is ended, then the hold is cleared if it may go")
		require.Equal(t, [][]byte{{4}}, f.takeTokens, "the finish names the attempt the record carries")
		require.Equal(t, [][]byte{{4}}, f.leasesTokens)
		require.Equal(t, [][]byte{{4}}, f.endTokens)
		require.Len(t, f.unspentWith, 2, "every input of the tx is unspent")
		require.Equal(t, []bool{false}, f.clearAdopt)
		require.Equal(t, 1, f.createCalls)
		require.Equal(t, 0, f.spendCalls)
	})

	t.Run("create ErrTxExists on an open rollback finishes it and creates again once the hold is cleared", func(t *testing.T) {
		f := &fakeSequentialStore{
			createErrs:  []error{errors.NewTxExistsError("already there")},
			createMeta:  &meta.Data{Creating: true, Attempt: []byte{5}},
			existing:    meta.Data{Creating: true, RollingBack: true, Attempt: []byte{4}},
			clearResult: true,
			spends:      []*Spend{{}},
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true, WithIgnoreLocked(true))
		require.NoError(t, err)
		require.NotNil(t, md)
		require.Equal(t, f.spends, spends)
		require.Equal(t, []string{"create", "getmeta", "take", "leases", "unspend", "release", "delete", "clear", "create", "spend", "finalize"}, f.callSeq)
		require.Equal(t, []bool{true}, f.clearAdopt, "block context adopts the hold the finish left")
		require.Equal(t, [][]byte{{5}}, f.finalizeTokens, "the finalize carries the token of the new attempt")
	})

	t.Run("create ErrTxExists on a reversed rollback only ends it", func(t *testing.T) {
		f := &fakeSequentialStore{
			createErr: errors.NewTxExistsError("already there"),
			existing:  meta.Data{Creating: true, RollingBack: true, Reversed: true, Attempt: []byte{4}},
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Equal(t, []string{"create", "getmeta", "delete", "clear"}, f.callSeq, "the reversal is recorded, so there is no take, no lease read and no unspend")
		require.Equal(t, [][]byte{{4}}, f.endTokens)
		require.Equal(t, 0, f.takeCalls)
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("an open rollback that cannot be finished still returns the transient error", func(t *testing.T) {
		for name, tc := range map[string]struct {
			store   *fakeSequentialStore
			wantSeq []string
		}{
			"take refused": {
				&fakeSequentialStore{takeDeclined: true},
				[]string{"create", "getmeta", "take", "clear"},
			},
			"take error": {
				&fakeSequentialStore{takeErr: errors.NewStorageError("take boom")},
				[]string{"create", "getmeta", "take", "clear"},
			},
			"lease live": {
				&fakeSequentialStore{leasesLive: -1},
				[]string{"create", "getmeta", "take", "leases", "release", "clear"},
			},
			"lease read error": {
				&fakeSequentialStore{leasesErr: errors.NewStorageError("leases boom")},
				[]string{"create", "getmeta", "take", "leases", "release", "clear"},
			},
			"unspend error": {
				&fakeSequentialStore{unspendErrs: []error{errors.NewStorageError("u1")}},
				[]string{"create", "getmeta", "take", "leases", "unspend", "release", "clear"},
			},
			"release error": {
				&fakeSequentialStore{releaseErr: errors.NewStorageError("release boom")},
				[]string{"create", "getmeta", "take", "leases", "unspend", "release", "clear"},
			},
			"end error": {
				&fakeSequentialStore{endErr: errors.NewStorageError("end boom")},
				[]string{"create", "getmeta", "take", "leases", "unspend", "release", "delete", "clear"},
			},
		} {
			f := tc.store
			f.createErr = errors.NewTxExistsError("already there")
			f.existing = meta.Data{Creating: true, RollingBack: true, Attempt: []byte{4}}

			_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
			require.ErrorIs(t, err, errors.ErrProcessing, name)
			require.Contains(t, err.Error(), "rolling back; retry after it completes", "%s: the failed finish is logged, not returned", name)
			require.Nil(t, spends, name)
			require.Equal(t, tc.wantSeq, f.callSeq, name)
			require.Equal(t, 1, f.createCalls, name)
			require.Equal(t, 0, f.spendCalls, name)
		}
	})

	t.Run("create over a hold that has expired creates again and goes on", func(t *testing.T) {
		f := &fakeSequentialStore{
			createErrs:  []error{errors.NewTxExistsError("held")},
			createMeta:  &meta.Data{Creating: true, Attempt: []byte{2}},
			existing:    meta.Data{Creating: true, RollingBack: true, Hold: true, Attempt: []byte{1}},
			clearResult: true,
			spends:      []*Spend{{}},
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.NoError(t, err)
		require.NotNil(t, md)
		require.Equal(t, f.spends, spends)
		require.Equal(t, 2, f.createCalls, "the create runs again once the hold is cleared")
		require.Equal(t, []string{"create", "getmeta", "clear", "create", "spend", "finalize"}, f.callSeq)
		require.Equal(t, [][]byte{{2}}, f.finalizeTokens, "the finalize carries the token of the new attempt, not the hold's")
	})

	t.Run("create in block context asks the store to adopt the hold", func(t *testing.T) {
		for name, opt := range map[string]CreateOption{
			"ignore locked":    WithIgnoreLocked(true),
			"mined block info": WithMinedBlockInfo(MinedBlockInfo{BlockID: 1, BlockHeight: 99}),
		} {
			f := &fakeSequentialStore{
				createErrs: []error{errors.NewTxExistsError("held")},
				createMeta: &meta.Data{Creating: true, Attempt: []byte{3}},
				existing:   meta.Data{Creating: true, RollingBack: true, Hold: true, Attempt: []byte{1}},

				clearResult: true,
				spends:      []*Spend{{}},
			}

			_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true, opt)
			require.NoError(t, err, name)
			require.Equal(t, []bool{true}, f.clearAdopt, "%s: block context adopts the hold instead of waiting for it to expire", name)
		}
	})

	t.Run("create in block context over a busy hold is refused as transient", func(t *testing.T) {
		f := &fakeSequentialStore{
			createErr: errors.NewTxExistsError("held"),
			existing:  meta.Data{Creating: true, RollingBack: true, Hold: true},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true, WithIgnoreLocked(true))
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Contains(t, err.Error(), "rolling back")
		require.Nil(t, spends)
		require.Equal(t, []bool{true}, f.clearAdopt)
		require.Equal(t, 1, f.createCalls)
		require.Equal(t, 0, f.spendCalls)
	})

	t.Run("a hold that cannot be cleared is a transient error", func(t *testing.T) {
		f := &fakeSequentialStore{
			createErr: errors.NewTxExistsError("held"),
			existing:  meta.Data{Creating: true, RollingBack: true, Hold: true},
			clearErr:  errors.NewStorageError("clear boom"),
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Equal(t, 0, f.spendCalls)
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
		require.Equal(t, []string{"create", "getmeta", "lease", "spend", "finalize"}, f.callSeq, "the lease is taken before the re-spend")
		require.Equal(t, 0, f.unleaseCalls, "the finalize ends the lease")
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
		require.Equal(t, []string{"create", "getmeta", "lease", "spend", "unlease"}, f.callSeq, "no finalize, no rollback; the lease is released")
		require.Equal(t, 0, f.unspendCalls, "a roll-forward never reverses a spend")
	})

	t.Run("create ErrTxExists roll-forward whose finalize finds the record gone releases its lease and reverses nothing", func(t *testing.T) {
		two := twoInputTx()
		f := &fakeSequentialStore{
			createErr:   errors.NewTxExistsError("already there"),
			existing:    meta.Data{Creating: true, Attempt: []byte{7}},
			spends:      []*Spend{{}, {}},
			finalizeErr: errors.NewTxNotFoundError("rolling back"),
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, two, 100, true)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.False(t, IsRolledForward(err))
		require.Equal(t, f.spends, spends)
		require.Equal(t, []string{"create", "getmeta", "lease", "spend", "finalize", "unlease"}, f.callSeq,
			"the owner of the rollback waits for the lease and reverses every input; the roll-forward takes no hold and reads nothing")
		require.Equal(t, [][]byte{{7}}, f.leaseTokens, "the lease names the attempt the roll-forward recovered")
		require.Equal(t, [][]byte{{7}}, f.finalizeTokens)
		require.Equal(t, [][2][]byte{{{7}, fakeLease}}, f.unleased)
		require.Equal(t, 0, f.unspendCalls)
		require.Equal(t, 0, f.deleteCalls, "the rollback owner deletes, not the roll-forward")
	})

	t.Run("create ErrTxExists roll-forward whose finalize fails otherwise releases its lease and leaves the spends", func(t *testing.T) {
		two := twoInputTx()
		f := &fakeSequentialStore{
			createErr:   errors.NewTxExistsError("already there"),
			existing:    meta.Data{Creating: true},
			spends:      []*Spend{{}, {}},
			finalizeErr: errors.NewStorageError("finalize boom"),
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, two, 100, true)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, f.spends, spends)
		require.Equal(t, []string{"create", "getmeta", "lease", "spend", "finalize", "unlease"}, f.callSeq)
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("create ErrTxExists roll-forward whose spend outcome is unknown keeps its lease", func(t *testing.T) {
		two := twoInputTx()

		for name, tc := range map[string]struct {
			spends   []*Spend
			spendErr error
		}{
			"deadline":            {[]*Spend{{}, {}}, context.DeadlineExceeded},
			"service unavailable": {[]*Spend{{}, {}}, errors.NewServiceUnavailableError("spend batch did not complete")},
			"no per-input result": {nil, errors.NewStorageError("batch outcome unknown")},
		} {
			f := &fakeSequentialStore{
				createErr: errors.NewTxExistsError("already there"),
				existing:  meta.Data{Creating: true},
				spends:    tc.spends,
				spendErr:  tc.spendErr,
			}

			_, _, err := SequentialSpendAndCreate(ctx, logger, f, two, 100, true)
			require.Error(t, err, name)
			require.Equal(t, []string{"create", "getmeta", "lease", "spend"}, f.callSeq, "%s: the spend may still land, so its lease stays live", name)
		}
	})

	t.Run("create ErrTxExists roll-forward whose lease is refused for a rolling back record spends nothing", func(t *testing.T) {
		two := twoInputTx()
		f := &fakeSequentialStore{
			createErr:     errors.NewTxExistsError("already there"),
			existingSeq:   []meta.Data{{Creating: true, Attempt: []byte{7}}},
			existing:      meta.Data{Creating: true, RollingBack: true, Attempt: []byte{7}},
			spends:        []*Spend{{}, {}},
			leaseDeclined: true,
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, two, 100, true)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxExists)
		require.Contains(t, err.Error(), "rolling back")
		require.Nil(t, spends)
		require.Equal(t, []string{"create", "getmeta", "lease", "getmeta"}, f.callSeq,
			"the marker landed after the first read: the store refuses the lease, so no input is spent for a record that is being undone")
	})

	t.Run("create ErrTxExists roll-forward whose lease is refused for another attempt spends nothing", func(t *testing.T) {
		two := twoInputTx()
		f := &fakeSequentialStore{
			createErr:     errors.NewTxExistsError("already there"),
			existingSeq:   []meta.Data{{Creating: true, Attempt: []byte{7}}},
			existing:      meta.Data{Creating: true, Attempt: []byte{8}},
			spends:        []*Spend{{}, {}},
			leaseDeclined: true,
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, two, 100, true)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Contains(t, err.Error(), "another attempt")
		require.Nil(t, spends)
		require.Equal(t, 0, f.spendCalls)
		require.Equal(t, 0, f.finalizeCalls)
	})

	t.Run("create ErrTxExists roll-forward whose lease is refused for an absent record spends nothing", func(t *testing.T) {
		two := twoInputTx()
		f := &absentAfterFirstRead{fakeSequentialStore: fakeSequentialStore{
			createErr:     errors.NewTxExistsError("already there"),
			existing:      meta.Data{Creating: true, Attempt: []byte{7}},
			spends:        []*Spend{{}, {}},
			leaseDeclined: true,
		}}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, two, 100, true)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.Nil(t, spends)
		require.Equal(t, 0, f.spendCalls)
		require.Equal(t, 0, f.finalizeCalls)
	})

	t.Run("create ErrTxExists roll-forward whose lease is refused because the record is now final is an ordinary duplicate", func(t *testing.T) {
		createErr := errors.NewTxExistsError("already there")
		f := &fakeSequentialStore{
			createErr:     createErr,
			existingSeq:   []meta.Data{{Creating: true, Attempt: []byte{7}}},
			existing:      meta.Data{},
			spends:        []*Spend{{}, {}},
			leaseDeclined: true,
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.Same(t, createErr, err, "the create's own ErrTxExists is returned")
		require.False(t, IsRolledForward(err), "this call completed nothing, so it must not hand the tx to block assembly")
		require.Nil(t, md)
		require.Nil(t, spends)
		require.Equal(t, []string{"create", "getmeta", "lease", "getmeta"}, f.callSeq, "a final record is neither spent nor finalized again")
	})

	t.Run("a lease that cannot be taken fails the roll-forward before any spend", func(t *testing.T) {
		two := twoInputTx()
		f := &fakeSequentialStore{
			createErr: errors.NewTxExistsError("already there"),
			existing:  meta.Data{Creating: true},
			spends:    []*Spend{{}, {}},
			leaseErr:  errors.NewStorageError("lease boom"),
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, two, 100, true)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Nil(t, spends)
		require.Equal(t, []string{"create", "getmeta", "lease"}, f.callSeq)
	})

	t.Run("a create that names its attempt for rollback is rolled back by the caller", func(t *testing.T) {
		createErr := MarkRollBackAttempt(errors.NewProcessingError("adoption failed"), []byte{9})
		f := &fakeSequentialStore{createErr: createErr}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Equal(t, []byte{9}, RollBackAttempt(err), "the original error is returned")
		require.Nil(t, spends)
		require.Equal(t, 0, f.spendCalls, "the failed create is not spent")
		require.Equal(t, []string{"create", "unlease", "begin", "leases", "unspend", "release", "delete"}, f.callSeq,
			"the lease the create wrote is released, then the published master goes through the rollback protocol, which reverses every input")
		require.Equal(t, [][2][]byte{{{9}, {9}}}, f.unleased, "the creator's lease is its attempt token")
		require.Equal(t, [][]byte{{9}}, f.beginTokens)
		require.Equal(t, [][]byte{{9}}, f.endTokens)
		require.Len(t, f.unspentWith, 2, "every input of the tx is reversed, so a concurrent roll-forward's spends do not stay")
	})

	t.Run("a create that names its attempt for rollback reports a rollback that fails", func(t *testing.T) {
		f := &fakeSequentialStore{
			createErr: MarkRollBackAttempt(errors.NewProcessingError("adoption failed"), []byte{9}),
			beginErr:  errors.NewStorageError("begin boom"),
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Contains(t, err.Error(), "begin boom")
		require.Contains(t, err.Error(), "left for the sweep")
		require.Equal(t, 0, f.spendCalls)
		require.Equal(t, 0, f.deleteCalls)
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
		require.Equal(t, []string{"create", "spend", "finalize", "finalize", "finalize", "unlease"}, f.callSeq, "the spends are complete, so the lease is released")
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
		require.Equal(t, 0, f.unleaseCalls)
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

// absentAfterFirstRead reports the scripted record on the first GetMeta and ErrTxNotFound after.
type absentAfterFirstRead struct {
	fakeSequentialStore
	reads int
}

func (a *absentAfterFirstRead) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	a.reads++
	if a.reads > 1 {
		return errors.NewTxNotFoundError("gone")
	}

	return a.fakeSequentialStore.GetMeta(ctx, hash, data)
}

// TestRollBackCreating pins the rollback order: mark, wait for the spend leases, reverse every
// input of the tx, then delete, and the cases that must do less.
func TestRollBackCreating(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tx := scriptedTwoInputTx(t)
	noInputs := bt.NewTx()

	t.Run("marks, checks the leases, unspends every input of the tx, then deletes", func(t *testing.T) {
		f := &fakeSequentialStore{}

		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"begin", "leases", "unspend", "release", "delete"}, f.callSeq)
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

	t.Run("claims and ends the rollback of the attempt it names", func(t *testing.T) {
		f := &fakeSequentialStore{}

		require.NoError(t, RollBackCreating(ctx, logger, f, tx, []byte{6, 6}))
		require.Equal(t, [][]byte{{6, 6}}, f.beginTokens, "the claim names the attempt being rolled back")
		require.Equal(t, [][]byte{{6, 6}}, f.leasesTokens, "the leases read are those of the same attempt")
		require.Equal(t, [][]byte{{6, 6}}, f.endTokens, "the end names the same attempt")
	})

	t.Run("a marker that reports false does nothing more and is not an error", func(t *testing.T) {
		f := &fakeSequentialStore{beginDeclined: true}

		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"begin"}, f.callSeq)
	})

	t.Run("a failed marker returns the error and does nothing more", func(t *testing.T) {
		f := &fakeSequentialStore{beginErr: errors.NewStorageError("begin boom")}

		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, []string{"begin"}, f.callSeq)
	})

	t.Run("a failed unspend releases the hold, skips the delete and returns the error", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeSequentialStore{unspendErrs: []error{errors.NewStorageError("u1"), errors.NewStorageError("u2"), errors.NewStorageError("u3")}}

		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.Error(t, err)
		require.Equal(t, 3, f.unspendCalls)
		require.Equal(t, 0, f.deleteCalls)
		require.Equal(t, 1, f.releaseCalls, "the owner releases the hold so the sweep can take the record")
	})

	t.Run("it waits while a spend lease is live and unspends only after none is", func(t *testing.T) {
		f := &fakeSequentialStore{leasesLive: 3}

		require.NoError(t, RollBackCreating(ctx, logger, f, tx, []byte{6}))
		require.Equal(t, []string{"begin", "leases", "leases", "leases", "leases", "unspend", "release", "delete"}, f.callSeq,
			"a spend that is in flight lands before the reversal")
		require.Equal(t, [][]byte{{6}, {6}, {6}, {6}}, f.leasesTokens)
	})

	t.Run("a lease that stays live ends the wait: nothing is unspent, the hold is released and the rollback is not ended", func(t *testing.T) {
		shortenSpendLeaseWait(t)

		f := &fakeSequentialStore{leasesLive: -1}

		start := time.Now()
		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Contains(t, err.Error(), "still live")
		require.Less(t, time.Since(start), 5*time.Second, "the wait is spendLeaseWait, not the rollback bound")
		require.Greater(t, f.leasesCalls, 1, "the leases are read again while the wait lasts")
		require.Equal(t, 0, f.unspendCalls, "a spend may still land, so nothing is reversed")
		require.Equal(t, 1, f.releaseCalls, "the hold is released so the sweep can take the record once the lease is stale")
		require.Equal(t, 0, f.deleteCalls, "the record stays rolling back")
		require.Equal(t, "begin", f.callSeq[0])
		require.Equal(t, "release", f.callSeq[len(f.callSeq)-1])
	})

	t.Run("a failed read of the leases is returned: nothing is unspent, the hold is released and the rollback is not ended", func(t *testing.T) {
		f := &fakeSequentialStore{leasesErr: errors.NewStorageError("leases boom")}

		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, []string{"begin", "leases", "release"}, f.callSeq)
	})

	t.Run("a marker that reports false reads no lease", func(t *testing.T) {
		f := &fakeSequentialStore{beginDeclined: true, leasesLive: -1}

		require.NoError(t, RollBackCreating(ctx, logger, f, tx, nil))
		require.Equal(t, 0, f.leasesCalls, "another caller owns the reversal and the wait")
	})

	t.Run("the owner holds the hold for the whole unspend and releases it before it ends the rollback", func(t *testing.T) {
		f := &fakeSequentialStore{}

		require.NoError(t, RollBackCreating(ctx, logger, f, tx, []byte{6, 6}))
		require.Equal(t, []string{"begin", "leases", "unspend", "release", "delete"}, f.callSeq)
		require.Equal(t, 1, f.releaseCalls)
	})

	t.Run("a failed release returns the error and leaves the record rolling back", func(t *testing.T) {
		f := &fakeSequentialStore{releaseErr: errors.NewStorageError("release boom")}

		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, []string{"begin", "leases", "unspend", "release"}, f.callSeq, "the rollback is not ended while the hold could not be released")
		require.Equal(t, 0, f.deleteCalls)
	})

	t.Run("a parent that is already gone does not fail the rollback", func(t *testing.T) {
		f := &fakeSequentialStore{unspendErrs: []error{errors.NewTxNotFoundError("gone")}}

		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.NoError(t, err)
		require.Equal(t, 3, f.unspendCalls, "one batch call, then one call per input")
		require.Equal(t, []string{"begin", "leases", "unspend", "unspend", "unspend", "release", "delete"}, f.callSeq)
	})

	t.Run("an input whose UTXO hash does not match is skipped and the other input is unspent", func(t *testing.T) {
		f := &fakeSequentialStore{unspendErrs: []error{
			errors.NewUtxoHashMismatchError("mismatch"),
			nil,
			errors.NewUtxoHashMismatchError("mismatch"),
		}}

		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"begin", "leases", "unspend", "unspend", "unspend", "release", "delete"}, f.callSeq)
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

		err := RollBackCreating(ctx, logger, f, noInputs, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"begin", "leases", "release", "delete"}, f.callSeq)
	})

	t.Run("it runs under its own context after the caller is cancelled", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		f := &fakeSequentialStore{}

		err := RollBackCreating(cancelled, logger, f, tx, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"begin", "leases", "unspend", "release", "delete"}, f.callSeq)
	})
}

func TestFinishRollBackCreating(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tx := scriptedTwoInputTx(t)
	attempt := []byte{9}

	t.Run("takes the hold, reads the leases, unspends every input, releases and ends", func(t *testing.T) {
		f := &fakeSequentialStore{}

		finished, err := FinishRollBackCreating(ctx, logger, f, tx, attempt, false)
		require.NoError(t, err)
		require.True(t, finished)
		require.Equal(t, []string{"take", "leases", "unspend", "release", "delete"}, f.callSeq)
		require.Equal(t, [][]byte{attempt}, f.takeTokens)
		require.Equal(t, [][]byte{attempt}, f.leasesTokens)
		require.Equal(t, [][]byte{attempt}, f.endTokens)
		require.Len(t, f.unspentWith, 2, "every input of the tx, not a subset")
	})

	t.Run("a reversed record is only ended", func(t *testing.T) {
		f := &fakeSequentialStore{takeDeclined: true, leasesLive: -1}

		finished, err := FinishRollBackCreating(ctx, logger, f, tx, attempt, true)
		require.NoError(t, err)
		require.True(t, finished)
		require.Equal(t, []string{"delete"}, f.callSeq, "no take, no lease read, no unspend, no release")
		require.Equal(t, [][]byte{attempt}, f.endTokens)
	})

	t.Run("a reversed record whose end fails reports the error", func(t *testing.T) {
		f := &fakeSequentialStore{endErr: errors.NewStorageError("end boom")}

		finished, err := FinishRollBackCreating(ctx, logger, f, tx, attempt, true)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.False(t, finished)
		require.Equal(t, 0, f.takeCalls)
	})

	t.Run("a refused take does nothing more and is not an error", func(t *testing.T) {
		f := &fakeSequentialStore{takeDeclined: true}

		finished, err := FinishRollBackCreating(ctx, logger, f, tx, attempt, false)
		require.NoError(t, err)
		require.False(t, finished)
		require.Equal(t, []string{"take"}, f.callSeq)
	})

	t.Run("a failed take returns the error and does nothing more", func(t *testing.T) {
		f := &fakeSequentialStore{takeErr: errors.NewStorageError("take boom")}

		finished, err := FinishRollBackCreating(ctx, logger, f, tx, attempt, false)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.False(t, finished)
		require.Equal(t, []string{"take"}, f.callSeq)
	})

	t.Run("a live lease releases the hold, reverses nothing and is not an error", func(t *testing.T) {
		f := &fakeSequentialStore{leasesLive: -1}

		finished, err := FinishRollBackCreating(ctx, logger, f, tx, attempt, false)
		require.NoError(t, err)
		require.False(t, finished)
		require.Equal(t, []string{"take", "leases", "release"}, f.callSeq, "one read of the leases, no wait: the caller tries again later")
	})

	t.Run("a lease read error releases the hold and returns the error", func(t *testing.T) {
		f := &fakeSequentialStore{leasesErr: errors.NewStorageError("leases boom")}

		finished, err := FinishRollBackCreating(ctx, logger, f, tx, attempt, false)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Contains(t, err.Error(), "leases boom")
		require.False(t, finished)
		require.Equal(t, []string{"take", "leases", "release"}, f.callSeq)
	})

	t.Run("a failed unspend releases the hold, does not end the rollback and returns the error", func(t *testing.T) {
		f := &fakeSequentialStore{unspendErrs: []error{errors.NewStorageError("u1")}}

		finished, err := FinishRollBackCreating(ctx, logger, f, tx, attempt, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "u1")
		require.False(t, finished)
		require.Equal(t, []string{"take", "leases", "unspend", "release"}, f.callSeq)
		require.Empty(t, f.endTokens)
	})

	t.Run("a parent that is already gone does not fail the finish", func(t *testing.T) {
		f := &fakeSequentialStore{unspendErrs: []error{errors.NewTxNotFoundError("parent gone")}}

		finished, err := FinishRollBackCreating(ctx, logger, f, tx, attempt, false)
		require.NoError(t, err)
		require.True(t, finished)
		require.Equal(t, "delete", f.callSeq[len(f.callSeq)-1])
	})

	t.Run("a failed release returns the error and does not end the rollback", func(t *testing.T) {
		f := &fakeSequentialStore{releaseErr: errors.NewStorageError("release boom")}

		finished, err := FinishRollBackCreating(ctx, logger, f, tx, attempt, false)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Contains(t, err.Error(), "release boom")
		require.False(t, finished)
		require.Equal(t, []string{"take", "leases", "unspend", "release"}, f.callSeq)
		require.Empty(t, f.endTokens, "the end would record the reversal over a busy marker")
	})

	t.Run("a failed end returns the error", func(t *testing.T) {
		f := &fakeSequentialStore{endErr: errors.NewStorageError("end boom")}

		finished, err := FinishRollBackCreating(ctx, logger, f, tx, attempt, false)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Contains(t, err.Error(), "end boom")
		require.False(t, finished)
		require.Equal(t, []string{"take", "leases", "unspend", "release", "delete"}, f.callSeq)
	})

	t.Run("a tx without inputs skips the unspend and still ends", func(t *testing.T) {
		f := &fakeSequentialStore{}

		finished, err := FinishRollBackCreating(ctx, logger, f, bt.NewTx(), attempt, false)
		require.NoError(t, err)
		require.True(t, finished)
		require.Equal(t, []string{"take", "leases", "release", "delete"}, f.callSeq)
	})
}

func TestIsRolledForward(t *testing.T) {
	require.False(t, IsRolledForward(nil))
	require.False(t, IsRolledForward(errors.NewTxExistsError("plain")))
	require.True(t, IsRolledForward(MarkRolledForward(errors.NewTxExistsError("marked"))))
	require.True(t, IsRolledForward(errors.NewProcessingError("wrapped", MarkRolledForward(errors.NewTxExistsError("marked")))))
}

func TestRollBackAttempt(t *testing.T) {
	require.Nil(t, RollBackAttempt(nil))
	require.Nil(t, RollBackAttempt(errors.NewProcessingError("plain")))
	require.Nil(t, RollBackAttempt(MarkRollBackAttempt(errors.NewProcessingError("no token"), nil)), "an empty token names no attempt")
	require.Equal(t, []byte{1, 2}, RollBackAttempt(MarkRollBackAttempt(errors.NewProcessingError("marked"), []byte{1, 2})))
	require.Equal(t, []byte{1, 2}, RollBackAttempt(errors.NewStorageError("wrapped", MarkRollBackAttempt(errors.NewProcessingError("marked"), []byte{1, 2}))))
}
