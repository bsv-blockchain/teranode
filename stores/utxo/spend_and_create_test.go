package utxo

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
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
	// beginErrs is popped once per BeginCreateFirstRollback call; beginErr applies when exhausted.
	beginErrs []error
	// spendWait is what SpendWaitTimeout answers; zero leaves the shared default in force.
	spendWait time.Duration
	// spendsSeq and spendErrs are popped once per Spend call; spends and spendErr apply when exhausted.
	spendsSeq [][]*Spend
	spendErrs []error
	// takeSeq is popped once per TakeRollbackHold call; takeDeclined applies when exhausted.
	takeSeq []bool
	// clearSeq is popped once per ClearCreateFirstHold call; clearResult applies when exhausted.
	clearSeq []bool
	// existing is what GetMeta returns after a Create that reported ErrTxExists.
	existing meta.Data
	// existingSeq is popped once per GetMeta call; existing applies when exhausted.
	existingSeq []meta.Data
	getMetaErr  error
	// getMetaErrs is popped once per GetMeta call, and a nil entry lets the call answer from
	// existingSeq or existing; getMetaErr applies when exhausted.
	getMetaErrs []error
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

	spendCalls    int
	createCalls   int
	unspendCalls  int
	finalizeCalls int
	deleteCalls   int
	deletedHashes []*chainhash.Hash
	unspentWith   []*Spend
	// unspendGuards holds the reversal guard of each Unspend that ran under one that names a record;
	// unguardedUnspends counts the others.
	unspendGuards     []ReversalGuard
	unguardedUnspends int
	unspentBatches    [][]*Spend
	spendFlags        []IgnoreFlags
	spendLeaseBound   []bool
	beginCalls        int
	leaseCalls        int
	unleaseCalls      int
	leasesCalls       int
	releaseCalls      int
	releaseErr        error
	clearCalls        int
	clearAdopt        []bool
	finalizeTokens    [][]byte
	finalizeLeases    [][]byte
	beginTokens       [][]byte
	endTokens         [][]byte
	leaseTokens       [][]byte
	leasesTokens      [][]byte
	// unleased holds, per ReleaseSpendLease call, the attempt and the lease it named.
	unleased       [][2][]byte
	spendDeadlines []time.Time
	lastCreateOpts CreateOptions
	callSeq        []string
}

func (f *fakeSequentialStore) Spend(ctx context.Context, _ *bt.Tx, _ uint32, ignoreFlags ...IgnoreFlags) ([]*Spend, error) {
	deadline, _ := ctx.Deadline()
	f.spendDeadlines = append(f.spendDeadlines, deadline)
	f.spendLeaseBound = append(f.spendLeaseBound, IsLeaseBound(ctx))
	f.spendCalls++
	f.spendFlags = ignoreFlags
	f.callSeq = append(f.callSeq, "spend")

	spends, err := f.spends, f.spendErr

	if len(f.spendsSeq) > 0 {
		spends = f.spendsSeq[0]
		f.spendsSeq = f.spendsSeq[1:]
	}

	if len(f.spendErrs) > 0 {
		err = f.spendErrs[0]
		f.spendErrs = f.spendErrs[1:]
	}

	return spends, err
}

// SpendWaitTimeout names how long a rollback waits for the spenders of its attempt.
func (f *fakeSequentialStore) SpendWaitTimeout() time.Duration { return f.spendWait }

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

func (f *fakeSequentialStore) Unspend(ctx context.Context, spends []*Spend, _ ...bool) error {
	guard, guarded := ReversalGuardFrom(ctx)
	if guarded && guard.Record != nil {
		f.unspendGuards = append(f.unspendGuards, guard)
	} else {
		f.unguardedUnspends++
	}

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

// deadCtxStore ends the caller's context when its second spend returns, and refuses every read
// and take on a context that has ended, as the real store does.
type deadCtxStore struct {
	fakeSequentialStore
	cancel context.CancelFunc
}

func (d *deadCtxStore) Spend(ctx context.Context, tx *bt.Tx, h uint32, flags ...IgnoreFlags) ([]*Spend, error) {
	spends, err := d.fakeSequentialStore.Spend(ctx, tx, h, flags...)
	if d.spendCalls == 2 {
		d.cancel()
	}

	return spends, err
}

func (d *deadCtxStore) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return d.fakeSequentialStore.GetMeta(ctx, hash, data)
}

func (d *deadCtxStore) TakeRollbackHold(ctx context.Context, hash *chainhash.Hash, attempt []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	return d.fakeSequentialStore.TakeRollbackHold(ctx, hash, attempt)
}

// A reversal that tolerates missing parents wraps the store in a reverser with a one-method set.
// It must still report the store's own bound, or every reversal under it runs at the default.
func TestTolerantReverserKeepsTheBoundOfItsStore(t *testing.T) {
	store := &boundedSequentialStore{bound: 7 * time.Second}

	require.Equal(t, 7*time.Second, rollbackBound(tolerantReverser{store}))

	ctx, cancel := rollbackContext(context.Background(), tolerantReverser{store})
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.Less(t, time.Until(deadline), 8*time.Second, "a reversal is bounded by the configured timeout, not by the default of two minutes")
}

func (f *fakeSequentialStore) FinalizeTransaction(_ context.Context, _ *bt.Tx, attempt, lease []byte) error {
	f.finalizeCalls++
	f.finalizeTokens = append(f.finalizeTokens, attempt)
	f.finalizeLeases = append(f.finalizeLeases, lease)
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

	if len(f.takeSeq) > 0 {
		took := f.takeSeq[0]
		f.takeSeq = f.takeSeq[1:]

		return took, nil
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

	if len(f.clearSeq) > 0 {
		cleared := f.clearSeq[0]
		f.clearSeq = f.clearSeq[1:]

		return cleared, f.clearErr
	}

	return f.clearResult, f.clearErr
}

func (f *fakeSequentialStore) BeginCreateFirstRollback(_ context.Context, _ *chainhash.Hash, attempt []byte) (bool, error) {
	f.beginCalls++
	f.beginTokens = append(f.beginTokens, attempt)
	f.callSeq = append(f.callSeq, "begin")

	if len(f.beginErrs) > 0 {
		err := f.beginErrs[0]
		f.beginErrs = f.beginErrs[1:]

		if err != nil {
			return false, err
		}

		return !f.beginDeclined, nil
	}

	if f.beginErr != nil {
		return false, f.beginErr
	}

	return !f.beginDeclined, nil
}

func (f *fakeSequentialStore) GetMeta(_ context.Context, _ *chainhash.Hash, data *meta.Data) error {
	f.callSeq = append(f.callSeq, "getmeta")

	if len(f.getMetaErrs) > 0 {
		err := f.getMetaErrs[0]
		f.getMetaErrs = f.getMetaErrs[1:]

		if err != nil {
			return err
		}
	} else if f.getMetaErr != nil {
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

// leaseLive is the error a store returns from BeginCreateFirstRollback while a spender of the
// attempt holds a live lease.
func leaseLive() error {
	return MarkSpendLeaseLive(errors.NewProcessingError("a spender of the attempt holds a live lease"))
}

func shortenUnspendBackoff(t *testing.T) {
	t.Helper()

	orig := unspendRetryBackoffBase
	unspendRetryBackoffBase = time.Millisecond

	t.Cleanup(func() { unspendRetryBackoffBase = orig })
}

// shortenClaimBackoff makes the wait between two tries of the claim in reverseCarried short.
func shortenClaimBackoff(t *testing.T, d time.Duration) {
	t.Helper()

	orig := claimRetryBackoffBase
	claimRetryBackoffBase = d

	t.Cleanup(func() { claimRetryBackoffBase = orig })
}

// errorLog is a test logger that keeps the error lines it is given.
type errorLog struct {
	ulogger.TestLogger
	mu     sync.Mutex
	errors []string
}

func (l *errorLog) Errorf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.errors = append(l.errors, fmt.Sprintf(format, args...))
}

func (l *errorLog) lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.errors...)
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

	t.Run("create ErrTxExists whose record cannot be read keeps the spends and is not reported as a stored transaction", func(t *testing.T) {
		for name, getErr := range map[string]error{
			"storage error": errors.NewStorageError("read boom"),
			"context error": context.DeadlineExceeded,
		} {
			f := &fakeSequentialStore{
				spends:     []*Spend{{}},
				createErr:  errors.NewTxExistsError("already there"),
				getMetaErr: getErr,
				existing:   meta.Data{Creating: true, RollingBack: true, Hold: true},
			}

			md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
			require.ErrorIs(t, err, errors.ErrStorageError, name)
			require.True(t, errors.IsRetryableError(err), "%s: quick validation retries a retryable error and fails the block on any other", name)
			require.NotErrorIs(t, err, errors.ErrTxExists, "%s: the record may be a hold that carries no transaction", name)
			require.Nil(t, md, name)
			require.Len(t, spends, 1, "%s: the spends stay in effect, as for any outcome the call cannot settle", name)
			require.Equal(t, 0, f.unspendCalls, name)
			require.Equal(t, 0, f.clearCalls, name)
			require.Equal(t, 1, f.createCalls, name)
		}
	})

	t.Run("create ErrTxExists whose read runs after the caller's context ended still tells a duplicate from a hold", func(t *testing.T) {
		ended, cancel := context.WithCancel(ctx)
		cancel()

		final := &deadCtxStore{
			fakeSequentialStore: fakeSequentialStore{
				spends:    []*Spend{{}},
				createErr: errors.NewTxExistsError("already there"),
				existing:  meta.Data{},
			},
		}

		_, spends, err := SequentialSpendAndCreate(ended, logger, final, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrTxExists, "a stored transaction is a duplicate whether or not the caller is still waiting")
		require.Len(t, spends, 1)
		require.Equal(t, 0, final.unspendCalls)

		held := &deadCtxStore{
			fakeSequentialStore: fakeSequentialStore{
				spends:    []*Spend{{}},
				createErr: errors.NewTxExistsError("held"),
				existing:  meta.Data{Creating: true, RollingBack: true, Hold: true},
			},
		}

		_, spends, err = SequentialSpendAndCreate(ended, logger, held, tx, 100, false)
		require.Error(t, err)
		require.NotErrorIs(t, err, errors.ErrTxExists, "a hold carries no transaction")
		require.Len(t, spends, 1, "the spends the call could not reverse stay in effect")

		heldCreateOnly := &deadCtxStore{
			fakeSequentialStore: fakeSequentialStore{
				createErr: errors.NewTxExistsError("held"),
				existing:  meta.Data{Creating: true, RollingBack: true, Hold: true},
			},
		}

		_, _, err = SequentialSpendAndCreate(ended, logger, heldCreateOnly, tx, 100, false, WithCreateOnly())
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxExists, "a hold carries no transaction")
	})

	t.Run("create over a hold that is cleared spends again and then creates", func(t *testing.T) {
		first, second := []*Spend{{Vout: 1}}, []*Spend{{Vout: 2}}
		f := &fakeSequentialStore{
			spendsSeq:   [][]*Spend{first, second},
			createErrs:  []error{errors.NewTxExistsError("held")},
			createMeta:  &meta.Data{},
			existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
			clearResult: true,
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.NoError(t, err)
		require.Same(t, f.createMeta, md)
		require.Equal(t, second, spends, "the spends in effect are those made after the hold was gone")
		require.Equal(t, []string{"spend", "create", "getmeta", "getmeta", "clear", "spend", "create"}, f.callSeq,
			"the rollback of the hold's attempt may have reversed the first spends, so the record is never created over them")
		require.Equal(t, []bool{false}, f.clearAdopt, "outside block context only an expired hold goes")
		require.Equal(t, 0, f.unspendCalls)
		require.Equal(t, 0, f.takeCalls)
	})

	t.Run("a second spend that fails after the hold was cleared is returned, and the first spends are reversed", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeSequentialStore{
			spends:      []*Spend{{}},
			spendErrs:   []error{nil, errors.NewUtxoError("taken meanwhile")},
			createErr:   errors.NewTxExistsError("held"),
			existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
			clearResult: true,
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.Nil(t, md)
		require.Len(t, spends, 1)
		require.Equal(t, []string{"spend", "create", "getmeta", "getmeta", "clear", "spend", "getmeta", "take", "unspend", "release"}, f.callSeq,
			"a hold is at the txid again, so the spends of the first round are reversed under it")
	})

	t.Run("create over a hold that stands reverses the spends under the hold and is a retryable error", func(t *testing.T) {
		for name, clearErr := range map[string]error{
			"not cleared": nil,
			"clear error": errors.NewStorageError("clear boom"),
		} {
			f := &fakeSequentialStore{
				spends:    []*Spend{{}, {}},
				createErr: errors.NewTxExistsError("held"),
				existing:  meta.Data{Creating: true, RollingBack: true, Hold: true, Attempt: []byte{7}},
				clearErr:  clearErr,
			}

			md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
			require.ErrorIs(t, err, errors.ErrProcessing, name)
			require.NotErrorIs(t, err, errors.ErrTxExists, "%s: a hold is not a duplicate: the caller must not treat the tx as stored", name)
			require.Nil(t, md, name)
			require.Nil(t, spends, "%s: the spends were reversed, so none is in effect", name)
			require.Equal(t, []string{"spend", "create", "getmeta", "getmeta", "clear", "take", "unspend", "release"}, f.callSeq,
				"%s: the reversal runs only while the call holds the hold, which no create can adopt", name)
			require.Equal(t, [][]byte{{7}}, f.takeTokens, "%s: the hold taken is that of the attempt the record carries", name)
			require.Equal(t, f.spends, f.unspentWith, name)
			require.Equal(t, 1, f.createCalls, "%s: a hold that stands is not created over", name)
		}
	})

	t.Run("create over a record whose rollback is still open reverses the spends under the hold and is a retryable error", func(t *testing.T) {
		for name, existing := range map[string]meta.Data{
			"rolling back":              {Creating: true, RollingBack: true, Attempt: []byte{8}},
			"rolling back and reversed": {Creating: true, RollingBack: true, Reversed: true, Attempt: []byte{8}},
		} {
			f := &fakeSequentialStore{
				spends:    []*Spend{{}, {}},
				createErr: errors.NewTxExistsError("rolling back"),
				existing:  existing,
			}

			md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
			require.ErrorIs(t, err, errors.ErrProcessing, name)
			require.NotErrorIs(t, err, errors.ErrTxExists, "%s: a record that is being rolled back is not a stored transaction", name)
			require.Nil(t, md, name)
			require.Nil(t, spends, "%s: the rollback reverses the inputs only once, so this call reverses its own spends", name)
			require.Equal(t, []string{"spend", "create", "getmeta", "getmeta", "take", "unspend", "release"}, f.callSeq, name)
			require.Equal(t, [][]byte{{8}}, f.takeTokens, name)
			require.Equal(t, f.spends, f.unspentWith, name)
			require.Equal(t, 0, f.clearCalls, "%s: a record that is not a hold is not cleared", name)
		}
	})

	t.Run("a take that fails keeps the spends and is a retryable error", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:    []*Spend{{}},
			createErr: errors.NewTxExistsError("held"),
			existing:  meta.Data{Creating: true, RollingBack: true, Hold: true},
			takeErr:   errors.NewStorageError("take boom"),
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxExists)
		require.Contains(t, err.Error(), "take boom")
		require.Len(t, spends, 1, "without the hold a reversal could clear the spends of a create that adopted it")
		require.Equal(t, []string{"spend", "create", "getmeta", "getmeta", "clear", "take"}, f.callSeq)
		require.Equal(t, 0, f.unspendCalls)
		require.Equal(t, 0, f.releaseCalls)
	})

	t.Run("a reversal that fails under the hold keeps the spends and still releases the hold", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeSequentialStore{
			spends:    []*Spend{{}},
			createErr: errors.NewTxExistsError("held"),
			existing:  meta.Data{Creating: true, RollingBack: true, Hold: true},
			unspendErrs: []error{
				errors.NewStorageError("u1"),
				errors.NewStorageError("u2"),
				errors.NewStorageError("u3"),
			},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxExists)
		require.Contains(t, err.Error(), "u3")
		require.Len(t, spends, 1, "the spends are still in effect")
		require.Equal(t, []string{"spend", "create", "getmeta", "getmeta", "clear", "take", "unspend", "unspend", "unspend", "release"}, f.callSeq)
	})

	t.Run("a take that is refused waits at the record, spends nothing more, and reverses once the hold is free", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:    []*Spend{{}},
			createErr: errors.NewTxExistsError("held"),
			existing:  meta.Data{Creating: true, RollingBack: true, Attempt: []byte{5}},
			takeSeq:   []bool{false, false, true},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Nil(t, spends)
		require.Equal(t, []string{
			"spend", "create", "getmeta",
			"getmeta", "take",
			"getmeta", "take",
			"getmeta", "take", "unspend", "release",
		}, f.callSeq, "while the owner of the rollback is busy nothing is reversed, and no round spends again")
	})

	t.Run("a take that is refused and then finds the record gone creates over the spends it has just made again", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:     []*Spend{{}},
			createErrs: []error{errors.NewTxExistsError("held")},
			createMeta: &meta.Data{},
			existing:   meta.Data{Creating: true, RollingBack: true, Hold: true},
			takeSeq:    []bool{false},
			// The record is there for the two reads before the take, and gone for the one after it.
			getMetaErrs: []error{nil, nil, errors.NewTxNotFoundError("the hold expired")},
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.NoError(t, err)
		require.Same(t, f.createMeta, md)
		require.Len(t, spends, 1)
		require.Equal(t, []string{"spend", "create", "getmeta", "getmeta", "clear", "take", "getmeta", "spend", "create"}, f.callSeq)
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("a wait for the hold ends with the caller's context and keeps the spends", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		f := &fakeSequentialStore{
			spends:       []*Spend{{}},
			createErr:    errors.NewTxExistsError("held"),
			existing:     meta.Data{Creating: true, RollingBack: true},
			takeDeclined: true,
		}

		_, spends, err := SequentialSpendAndCreate(cancelled, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxExists)
		require.Len(t, spends, 1)
		require.Equal(t, []string{"spend", "create", "getmeta", "getmeta", "take"}, f.callSeq)
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("once the rollback bound has passed the call stops and keeps the spends it could not reverse safely", func(t *testing.T) {
		for name, existing := range map[string]meta.Data{
			"hold":          {Creating: true, RollingBack: true, Hold: true},
			"open rollback": {Creating: true, RollingBack: true},
		} {
			f := &boundedSequentialStore{
				fakeSequentialStore: fakeSequentialStore{
					spends:       []*Spend{{}},
					createErr:    errors.NewTxExistsError("held"),
					existing:     existing,
					takeDeclined: true,
					clearResult:  true,
				},
				bound: time.Nanosecond,
			}

			_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
			require.ErrorIs(t, err, errors.ErrProcessing, name)
			require.NotErrorIs(t, err, errors.ErrTxExists, name)
			require.Len(t, spends, 1, name)
			require.Equal(t, []string{"spend", "create", "getmeta"}, f.callSeq, "%s: no clear, no take, no reversal after the bound", name)
		}
	})

	t.Run("a refused take is retried until twice the rollback bound and then the spends are kept", func(t *testing.T) {
		f := &boundedSequentialStore{
			fakeSequentialStore: fakeSequentialStore{
				spends:       []*Spend{{}},
				createErr:    errors.NewTxExistsError("rolling back"),
				existing:     meta.Data{Creating: true, RollingBack: true},
				takeDeclined: true,
			},
			bound: 60 * time.Millisecond,
		}

		start := time.Now()
		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Len(t, spends, 1)
		require.Less(t, time.Since(start), 5*time.Second)
		require.Greater(t, f.takeCalls, 1, "the take is tried again while the bound lasts")
		require.Equal(t, 1, f.spendCalls, "the call waits at the record and does not spend again")
		require.Equal(t, 0, f.unspendCalls)
		require.Equal(t, "take", f.callSeq[len(f.callSeq)-1])
	})

	t.Run("a spend that fails in a later round reverses the spends of the earlier round", func(t *testing.T) {
		shortenUnspendBackoff(t)

		two := scriptedTwoInputTx(t)
		earlier := []*Spend{{Vout: 0}, {Vout: 1}}
		spendErr := errors.NewStorageError("the spend of the second round failed")

		// No record is left at the txid: the call claims it with the tentative create of the
		// transaction, which the store writes CREATE_ONLY, and rolls that attempt back. The rollback
		// reverses every input under its marker and leaves a hold, so no read-then-write is left
		// for another attempt to slip between.
		gone := &fakeSequentialStore{
			spendsSeq:   [][]*Spend{earlier, {{Vout: 0}}},
			spendErrs:   []error{nil, spendErr},
			createErrs:  []error{errors.NewTxExistsError("held")},
			createMeta:  &meta.Data{Creating: true, Attempt: []byte{3}},
			existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
			clearResult: true,
			getMetaErrs: []error{nil, nil, errors.NewTxNotFoundError("no record")},
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, gone, two, 100, false)
		require.ErrorIs(t, err, spendErr)
		require.Nil(t, md)
		require.Len(t, spends, 1, "the spends of the failed round are returned for the caller to classify")
		require.Equal(t, []string{
			"spend", "create", "getmeta", "getmeta", "clear",
			"spend", "getmeta",
			"create", "unlease", "begin", "unspend", "release", "delete",
		}, gone.callSeq, "the claim is the create, and the reversal runs under the marker of its attempt")
		require.Equal(t, [][]byte{{3}}, gone.beginTokens)
		require.True(t, gone.lastCreateOpts.Creating, "the claim is the tentative create")

		// The record of a rollback is back at the txid: they are reversed under its hold.
		back := &fakeSequentialStore{
			spendsSeq:   [][]*Spend{earlier, {{Vout: 0}}},
			spendErrs:   []error{nil, spendErr},
			createErr:   errors.NewTxExistsError("held"),
			existingSeq: []meta.Data{{Creating: true, RollingBack: true, Hold: true}, {Creating: true, RollingBack: true, Hold: true}},
			existing:    meta.Data{Creating: true, RollingBack: true, Attempt: []byte{6}},
			clearResult: true,
		}

		_, _, err = SequentialSpendAndCreate(ctx, logger, back, two, 100, false)
		require.ErrorIs(t, err, spendErr)
		require.Equal(t, []string{"spend", "create", "getmeta", "getmeta", "clear", "spend", "getmeta", "take", "unspend", "release"}, back.callSeq)
		require.Equal(t, [][]byte{{6}}, back.takeTokens)
		require.Equal(t, earlier, back.unspentWith)

		// A record of a later attempt owns the spends of its txid: they stay.
		replaced := &fakeSequentialStore{
			spendsSeq:   [][]*Spend{earlier, {{Vout: 0}}},
			spendErrs:   []error{nil, spendErr},
			createErr:   errors.NewTxExistsError("held"),
			existingSeq: []meta.Data{{Creating: true, RollingBack: true, Hold: true}, {Creating: true, RollingBack: true, Hold: true}},
			existing:    meta.Data{},
			clearResult: true,
		}

		_, _, err = SequentialSpendAndCreate(ctx, logger, replaced, two, 100, false)
		require.ErrorIs(t, err, spendErr)
		require.Equal(t, 0, replaced.unspendCalls)
		require.Equal(t, 0, replaced.takeCalls)

		// The claim is refused because another attempt wrote the record between the read and the
		// create: the call looks again, finds the record of that attempt and leaves the spends.
		raced := &fakeSequentialStore{
			spendsSeq:   [][]*Spend{earlier, {{Vout: 0}}},
			spendErrs:   []error{nil, spendErr},
			createErr:   errors.NewTxExistsError("another attempt"),
			existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
			clearResult: true,
			getMetaErrs: []error{nil, nil, errors.NewTxNotFoundError("no record"), nil},
			existingSeq: []meta.Data{{Creating: true, RollingBack: true, Hold: true}, {Creating: true, RollingBack: true, Hold: true}, {}, {}},
		}

		_, _, err = SequentialSpendAndCreate(ctx, logger, raced, two, 100, false)
		require.ErrorIs(t, err, spendErr)
		require.Equal(t, 0, raced.unspendCalls, "a create that the store refused wrote nothing, and the record that is there now owns the spends")
		require.Equal(t, 0, raced.beginCalls)
	})

	t.Run("a claim that fails with nothing written is tried again until it reverses the spends of the earlier round", func(t *testing.T) {
		shortenUnspendBackoff(t)
		shortenClaimBackoff(t, time.Millisecond)

		two := scriptedTwoInputTx(t)
		earlier := []*Spend{{Vout: 0}, {Vout: 1}}
		spendErr := errors.NewStorageError("the spend of the second round failed")

		// The hold of the first round is cleared and the second round's spend fails. No record is at
		// the txid, and the claim of it fails twice before the store takes it.
		f := &fakeSequentialStore{
			spendsSeq:   [][]*Spend{earlier, {{Vout: 0}}},
			spendErrs:   []error{nil, spendErr},
			createErrs:  []error{errors.NewTxExistsError("held"), errors.NewStorageError("the store is busy"), MarkCreateOutcomeUnknown(errors.NewStorageError("the answer was lost"))},
			createMeta:  &meta.Data{Creating: true, Attempt: []byte{3}},
			existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
			clearResult: true,
			getMetaErrs: []error{nil, nil},
			getMetaErr:  errors.NewTxNotFoundError("no record"),
		}

		log := &errorLog{}

		_, _, err := SequentialSpendAndCreate(ctx, log, f, two, 100, false)
		require.ErrorIs(t, err, spendErr)
		require.Equal(t, []string{
			"spend", "create", "getmeta", "getmeta", "clear",
			"spend",
			"getmeta", "create",
			"getmeta", "create",
			"getmeta", "create", "unlease", "begin", "unspend", "release", "delete",
		}, f.callSeq, "the call looks at the txid again before each try, and the third claim reverses the spends")
		require.Len(t, f.unspentWith, len(two.Inputs), "every input of the transaction is reversed under the claim")
		require.Empty(t, log.lines(), "the spends were reversed, so nothing is reported as left in place")
	})

	t.Run("a claim that keeps failing is given up after twice the rollback bound and reported", func(t *testing.T) {
		shortenUnspendBackoff(t)
		shortenClaimBackoff(t, 5*time.Millisecond)

		two := scriptedTwoInputTx(t)
		spendErr := errors.NewStorageError("the spend of the second round failed")

		f := &boundedSequentialStore{
			fakeSequentialStore: fakeSequentialStore{
				spendsSeq:   [][]*Spend{{{Vout: 0}, {Vout: 1}}, {{Vout: 0}}},
				spendErrs:   []error{nil, spendErr},
				createErrs:  []error{errors.NewTxExistsError("held")},
				createErr:   errors.NewStorageError("the store refuses every write"),
				existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
				clearResult: true,
				getMetaErrs: []error{nil, nil},
				getMetaErr:  errors.NewTxNotFoundError("no record"),
			},
			bound: 50 * time.Millisecond,
		}

		log := &errorLog{}
		start := time.Now()

		_, _, err := SequentialSpendAndCreate(ctx, log, f, two, 100, false)
		require.ErrorIs(t, err, spendErr)
		require.Less(t, time.Since(start), 2*time.Second, "the retries end with the bound")
		require.Greater(t, f.createCalls, 3, "the claim is tried more than twice before the call gives up")
		require.Zero(t, f.beginCalls, "no claim was written, so no rollback ran")
		require.Zero(t, f.unspendCalls)

		lines := log.lines()
		require.Len(t, lines, 1, "the spends that stay are reported once")
		require.Contains(t, lines[0], "left in place")
		require.Contains(t, lines[0], "twice the rollback bound", "the call gives up at the bound of its wait, not at the end of its context")
		require.Contains(t, lines[0], two.TxIDChainHash().String())
	})

	t.Run("a claim that wrote its record and could not end the rollback is not tried again", func(t *testing.T) {
		shortenUnspendBackoff(t)
		shortenClaimBackoff(t, time.Millisecond)

		two := scriptedTwoInputTx(t)
		spendErr := errors.NewStorageError("the spend of the second round failed")

		f := &fakeSequentialStore{
			spendsSeq:   [][]*Spend{{{Vout: 0}, {Vout: 1}}, {{Vout: 0}}},
			spendErrs:   []error{nil, spendErr},
			createErrs:  []error{errors.NewTxExistsError("held")},
			createMeta:  &meta.Data{Creating: true, Attempt: []byte{3}},
			beginErr:    errors.NewStorageError("the marker write failed"),
			existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
			clearResult: true,
			getMetaErrs: []error{nil, nil},
			getMetaErr:  errors.NewTxNotFoundError("no record"),
		}

		log := &errorLog{}

		_, _, err := SequentialSpendAndCreate(ctx, log, f, two, 100, false)
		require.ErrorIs(t, err, spendErr)
		require.Equal(t, 2, f.createCalls, "the first create of the call and one claim: the record of the claim is at the txid, so it is not claimed again")
		require.Equal(t, 1, f.beginCalls)

		lines := log.lines()
		require.NotEmpty(t, lines)
		require.Contains(t, lines[len(lines)-1], "left for the sweep", "the record is the durable trace of the spends, and the sweep ends its rollback")
	})

	t.Run("a claim that another record keeps refusing is given up after three looks", func(t *testing.T) {
		shortenUnspendBackoff(t)
		shortenClaimBackoff(t, time.Millisecond)

		two := scriptedTwoInputTx(t)
		spendErr := errors.NewStorageError("the spend of the second round failed")

		// Each look finds no record, and each claim is refused by one: a record that comes and goes
		// between the read and the create. The call does not go round for ever.
		f := &fakeSequentialStore{
			spendsSeq:   [][]*Spend{{{Vout: 0}, {Vout: 1}}, {{Vout: 0}}},
			spendErrs:   []error{nil, spendErr},
			createErr:   errors.NewTxExistsError("another attempt"),
			existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
			clearResult: true,
			getMetaErrs: []error{nil, nil},
			getMetaErr:  errors.NewTxNotFoundError("no record"),
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, two, 100, false)
		require.ErrorIs(t, err, spendErr)
		require.Equal(t, 1+reverseCarriedRounds, f.createCalls, "the create of the call, and one claim per look")
		require.Zero(t, f.beginCalls, "a create that the store refused wrote nothing")
		require.Zero(t, f.unspendCalls)
	})

	t.Run("a spend that fails because the caller's context ended still reverses the spends of the earlier round", func(t *testing.T) {
		shortenUnspendBackoff(t)

		two := scriptedTwoInputTx(t)
		callerCtx, cancel := context.WithCancel(ctx)

		f := &deadCtxStore{
			fakeSequentialStore: fakeSequentialStore{
				spendsSeq:   [][]*Spend{{{Vout: 0}, {Vout: 1}}, {{Vout: 0}}},
				spendErrs:   []error{nil, context.Canceled},
				createErrs:  []error{errors.NewTxExistsError("held")},
				createMeta:  &meta.Data{Creating: true, Attempt: []byte{4}},
				existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
				clearResult: true,
				getMetaErrs: []error{nil, nil, errors.NewTxNotFoundError("no record")},
			},
			cancel: cancel,
		}

		_, _, err := SequentialSpendAndCreate(callerCtx, logger, f, two, 100, false)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, f.unspendCalls, "the cleanup runs on a context of its own: the caller's ended with the spend")
		require.Equal(t, [][]byte{{4}}, f.beginTokens)
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

	t.Run("a spend-first call whose create finds a record that is creating rolls it forward and says so", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:    []*Spend{{}, {}},
			createErr: errors.NewTxExistsError("tentative"),
			existing:  meta.Data{Creating: true, Attempt: []byte{0x01}},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, twoInputTx(), 100, false)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.True(t, IsRolledForward(err), "the validator accepts a creating record only when the store rolled it forward")
		require.Len(t, spends, 2)
		require.Equal(t, []string{"spend", "create", "getmeta", "lease", "spend", "finalize"}, f.callSeq)
		require.Equal(t, [][]byte{{0x01}}, f.finalizeTokens, "the finalize carries the attempt read from the record")
		require.Equal(t, [][]byte{fakeLease}, f.finalizeLeases)
	})

	t.Run("a spend-first call over a creating record whose lease is refused because it is final is a plain duplicate", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:        []*Spend{{}, {}},
			createErr:     errors.NewTxExistsError("tentative"),
			leaseDeclined: true,
			existingSeq:   []meta.Data{{Creating: true, Attempt: []byte{0x01}}, {}},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, twoInputTx(), 100, false)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.False(t, IsRolledForward(err), "another actor completed it and owns the hand-off")
		require.Len(t, spends, 2)
		require.Equal(t, 0, f.finalizeCalls)
	})

	t.Run("a spend-first call over a creating record that cannot be rolled forward keeps the spends and does not report a stored transaction", func(t *testing.T) {
		for name, f := range map[string]*fakeSequentialStore{
			"lease refused, owned by another attempt": {
				leaseDeclined: true,
				existingSeq:   []meta.Data{{Creating: true, Attempt: []byte{0x01}}, {Creating: true, Attempt: []byte{0x02}}},
			},
			"lease refused, rolling back": {
				leaseDeclined: true,
				existingSeq:   []meta.Data{{Creating: true, Attempt: []byte{0x01}}, {Creating: true, RollingBack: true, Attempt: []byte{0x01}}},
			},
			"finalize fails": {
				existing:    meta.Data{Creating: true, Attempt: []byte{0x01}},
				finalizeErr: errors.NewStorageError("finalize boom"),
			},
			"the re-spend fails": {
				existing:  meta.Data{Creating: true, Attempt: []byte{0x01}},
				spendErrs: []error{nil, errors.NewStorageError("respend boom")},
			},
		} {
			f.spends = []*Spend{{}, {}}
			f.createErr = errors.NewTxExistsError("tentative")

			_, spends, err := SequentialSpendAndCreate(ctx, logger, f, twoInputTx(), 100, false)
			require.Error(t, err, name)
			require.NotErrorIs(t, err, errors.ErrTxExists, name)
			require.False(t, IsRolledForward(err), name)
			require.Len(t, spends, 2, name)
			require.Equal(t, 0, f.unspendCalls, name)
		}
	})

	t.Run("a create-only call over a creating record that is not rolling back is a duplicate and rolls nothing forward", func(t *testing.T) {
		f := &fakeSequentialStore{
			createErr: errors.NewTxExistsError("tentative"),
			existing:  meta.Data{Creating: true, Attempt: []byte{0x01}},
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, WithCreateOnly())
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.False(t, IsRolledForward(err))
		require.Equal(t, []string{"create", "getmeta"}, f.callSeq)
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

	t.Run("a create-only call over a hold that stands returns a transient error, not ErrTxExists", func(t *testing.T) {
		for name, clearErr := range map[string]error{
			"not cleared": nil,
			"clear error": errors.NewStorageError("clear boom"),
		} {
			f := &fakeSequentialStore{
				createErr: errors.NewTxExistsError("held"),
				existing:  meta.Data{Creating: true, RollingBack: true, Hold: true},
				clearErr:  clearErr,
			}

			_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, WithCreateOnly())
			require.ErrorIs(t, err, errors.ErrProcessing, name)
			require.NotErrorIs(t, err, errors.ErrTxExists, "%s: a placeholder is not a stored transaction, and a caller that reads ErrTxExists as done would spend under it", name)
			require.Nil(t, spends, name)
			require.Equal(t, []string{"create", "getmeta", "clear"}, f.callSeq, name)
			require.Equal(t, 0, f.unspendCalls, name)
		}
	})

	t.Run("a create-only call over a record whose rollback is still open returns a transient error, not ErrTxExists", func(t *testing.T) {
		f := &fakeSequentialStore{
			createErr: errors.NewTxExistsError("rolling back"),
			existing:  meta.Data{Creating: true, RollingBack: true},
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, WithCreateOnly())
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.NotErrorIs(t, err, errors.ErrTxExists)
		require.Nil(t, spends)
		require.Equal(t, []string{"create", "getmeta"}, f.callSeq, "no clear, no unspend: a create-only call made no spend")
	})

	t.Run("a create-only duplicate returns ErrTxExists unchanged", func(t *testing.T) {
		f := &fakeSequentialStore{createErr: errors.NewTxExistsError("there")}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, WithCreateOnly())
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.Equal(t, []string{"create", "getmeta"}, f.callSeq)
	})

	t.Run("a create-only call whose record cannot be read does not report a stored transaction", func(t *testing.T) {
		for name, getErr := range map[string]error{
			"storage error": errors.NewStorageError("read boom"),
			"context error": context.Canceled,
		} {
			f := &fakeSequentialStore{
				createErr:  errors.NewTxExistsError("there"),
				getMetaErr: getErr,
				existing:   meta.Data{Creating: true, RollingBack: true, Hold: true},
			}

			_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, false, WithCreateOnly())
			require.ErrorIs(t, err, errors.ErrStorageError, name)
			require.True(t, errors.IsRetryableError(err), "%s: quick validation retries a retryable error and fails the block on any other", name)
			require.NotErrorIs(t, err, errors.ErrTxExists, "%s: the record may be a hold, and a caller that reads ErrTxExists as done would spend under it", name)
			require.Nil(t, spends, name)
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

// laneSpenderStore is a laneSequentialStore whose spend can record the spender's lane position.
// countedLanePosition is a lane position source that counts how often it is asked.
type countedLanePosition struct {
	position uint64
	calls    int
}

func (c *countedLanePosition) Next(context.Context) (uint64, error) {
	c.calls++
	return c.position, nil
}

type laneSpenderStore struct {
	laneSequentialStore
	laneSpendCalls int
	laneSpendPos   uint64
}

func (l *laneSpenderStore) SpendWithLanePosition(_ context.Context, _ *bt.Tx, _ uint32, position uint64, _ IgnoreFlags) ([]*Spend, error) {
	l.laneSpendCalls++
	l.laneSpendPos = position

	return l.spends, l.spendErr
}

// A store that can record the spender's position gets it only when the record has one: with lane
// positions off there is no source and every spend goes through the plain Spend, as before.
func TestSequentialSpendAndCreateRecordsTheSpenderPositionOnlyWithOne(t *testing.T) {
	tx := bt.NewTx()

	t.Run("off", func(t *testing.T) {
		f := &laneSpenderStore{}
		f.createMeta = &meta.Data{}

		_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, f, tx, 100, false)
		require.NoError(t, err)
		require.Equal(t, 1, f.spendCalls)
		require.Zero(t, f.laneSpendCalls)
	})

	t.Run("shadow", func(t *testing.T) {
		f := &laneSpenderStore{}
		f.createMeta = &meta.Data{}
		f.SetLanePositionSource(fixedLanePosition(lane.Pack(3, 9)))

		_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, f, tx, 100, false)
		require.NoError(t, err)
		require.Zero(t, f.spendCalls)
		require.Equal(t, 1, f.laneSpendCalls)
		require.Equal(t, lane.Pack(3, 9), f.laneSpendPos)
	})

	t.Run("a named position is the one the spend records", func(t *testing.T) {
		f := &laneSpenderStore{}
		f.createMeta = &meta.Data{}
		f.SetLanePositionSource(fixedLanePosition(lane.Pack(3, 9)))

		_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, f, tx, 100, false, WithLanePosition(lane.Pack(5, 7)))
		require.NoError(t, err)
		require.Zero(t, f.spendCalls)
		require.Equal(t, 1, f.laneSpendCalls)
		require.Equal(t, lane.Pack(5, 7), f.laneSpendPos, "the caller's position, not one from the source")
	})

	t.Run("spend only with a named position records it and takes nothing", func(t *testing.T) {
		f := &laneSpenderStore{}
		source := &countedLanePosition{position: lane.Pack(3, 9)}
		f.SetLanePositionSource(source)

		_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, f, tx, 100, false, WithSpendOnly(), WithLanePosition(lane.Pack(6, 4)))
		require.NoError(t, err)
		require.Zero(t, f.spendCalls)
		require.Equal(t, 1, f.laneSpendCalls)
		require.Equal(t, lane.Pack(6, 4), f.laneSpendPos, "the record's position, which the caller already wrote")
		require.Zero(t, source.calls, "a spend-only call takes no position")
		require.Zero(t, f.createCalls)
	})

	t.Run("spend only with an invalid named position spends nothing", func(t *testing.T) {
		f := &laneSpenderStore{}

		_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, f, tx, 100, false, WithSpendOnly(), WithLanePosition(1<<56))
		require.ErrorIs(t, err, errors.ErrInvalidArgument)
		require.Zero(t, f.spendCalls)
		require.Zero(t, f.laneSpendCalls)
	})

	t.Run("spend only takes no position", func(t *testing.T) {
		f := &laneSpenderStore{}
		f.SetLanePositionSource(fixedLanePosition(lane.Pack(3, 9)))

		_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, f, tx, 100, false, WithSpendOnly())
		require.NoError(t, err)
		require.Equal(t, 1, f.spendCalls)
		require.Zero(t, f.laneSpendCalls)
	})
}

// spendVerdicts are the per-input errors that are the store's answer about one input: the write
// was evaluated and refused, so it cannot still land.
var spendVerdicts = map[string]error{
	"spent":             errors.ErrSpent,
	"tx not found":      errors.NewTxNotFoundError("parent missing"),
	"not found":         errors.NewNotFoundError("output missing"),
	"conflicting":       errors.ErrTxConflicting,
	"locked":            errors.ErrTxLocked,
	"creating":          errors.ErrTxCreating,
	"frozen":            errors.ErrFrozen,
	"hash mismatch":     errors.ErrUtxoHashMismatch,
	"invalid size":      errors.NewUtxoInvalidSize("output has a bad size"),
	"coinbase immature": errors.ErrTxCoinbaseImmature,
	"pruned replay":     errors.ErrUtxoSpendingTxPruned,
}

// spendFaults are per-input errors that are not a verdict: the write of that input may be in doubt.
var spendFaults = map[string]error{
	"storage":    errors.NewStorageError("write timed out"),
	"processing": errors.NewProcessingError("batch item failed"),
	"plain":      context.DeadlineExceeded,
}

// fakeRollForwarder scripts the CreatingRollForwarder surface (TakeSpendLease, SpendAndCreate,
// FinalizeTransaction, ReleaseSpendLease, GetMeta) so RollForwardCreating can be tested in
// isolation. The surface has no Unspend: a roll-forward cannot reverse a spend.
type fakeRollForwarder struct {
	spends        []*Spend
	spendErr      error
	finalizeErr   error
	finalizeErrs  []error
	finalizeLease []byte
	finalizeCalls int
	// finalizeCtxErr and finalizeLeaseBound describe the context of the finalize. afterSpend runs
	// when the spend has returned.
	finalizeCtxErr     error
	finalizeLeaseBound bool
	afterSpend         func()
	spendLeaseBound    bool
	lastOpts           CreateOptions
	spendCalls         int
	existing           meta.Data
	getMetaErr         error
	getMetaCalls       int
	leaseDeclined      bool
	leaseErr           error
	leaseCalls         int
	unleaseCalls       int
	leaseTokens        [][]byte
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
	f.spendLeaseBound = IsLeaseBound(ctx)

	if f.afterSpend != nil {
		defer f.afterSpend()
	}

	o := CreateOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	f.lastOpts = o
	f.spendCalls++
	f.callSeq = append(f.callSeq, "spend")

	return nil, f.spends, f.spendErr
}

func (f *fakeRollForwarder) FinalizeTransaction(ctx context.Context, _ *bt.Tx, attempt, lease []byte) error {
	f.finalizeDeadline, _ = ctx.Deadline()
	f.finalizeCtxErr = ctx.Err()
	f.finalizeLeaseBound = IsLeaseBound(ctx)
	f.finalizeCalls++
	f.finalizeToken = attempt
	f.finalizeLease = lease
	f.callSeq = append(f.callSeq, "finalize")

	if len(f.finalizeErrs) > 0 {
		err := f.finalizeErrs[0]
		f.finalizeErrs = f.finalizeErrs[1:]

		return err
	}

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

		spends, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.NoError(t, err)
		require.True(t, f.lastOpts.SpendOnly, "roll-forward must re-run the spend-only phase")
		require.Equal(t, []string{"lease", "spend", "finalize"}, f.callSeq, "the lease is taken before any input is spent, and the finalize ends it")
		require.Equal(t, [][]byte{{1}}, f.leaseTokens, "the lease names the attempt being recovered")
		require.Equal(t, 0, f.unleaseCalls, "a finalized record carries no lease to release")
		require.Equal(t, f.spends, spends, "the spends must reach the caller so it can hand the tx to block assembly")
	})

	t.Run("threads caller ignore-flags into the re-spend", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}}

		_, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100, WithIgnoreLocked(true))
		require.NoError(t, err)
		require.True(t, f.lastOpts.SpendOnly, "must still be spend-only")
		require.True(t, f.lastOpts.IgnoreFlags.IgnoreLocked, "the caller's IgnoreLocked must reach the spend, so a locked parent does not defeat recovery")
	})

	t.Run("fails closed and releases the lease when the re-spend covers fewer inputs than the tx has", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}}} // 1 spend for a 2-input tx

		_, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.Error(t, err)
		require.Equal(t, []string{"lease", "spend", "getmeta", "unlease"}, f.callSeq, "an incomplete re-spend must NOT finalize, and its lease is released")
		require.Equal(t, [][2][]byte{{{1}, fakeLease}}, f.unleased, "the release names the attempt and the lease that was taken")
	})

	t.Run("fails closed and releases the lease on a per-input spend error", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {Err: errors.ErrSpent}}}

		spends, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
		require.Error(t, err)
		require.Equal(t, []string{"lease", "spend", "getmeta", "unlease"}, f.callSeq)
		require.Equal(t, f.spends, spends, "the per-input errors must reach the caller")
	})

	t.Run("a spend error with a known outcome releases the lease without finalizing", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {Err: errors.ErrSpent}}, spendErr: errors.NewUtxoError("double spend")}

		spends, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.Equal(t, f.spends, spends)
		require.Equal(t, []string{"lease", "spend", "getmeta", "unlease"}, f.callSeq, "the spend returned, so a rollback need not wait for this spender")
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

				spends, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
				require.Error(t, err)
				require.Equal(t, tc.spends, spends)
				require.Equal(t, []string{"lease", "spend"}, f.callSeq, "the spend may still land, so the lease must stay live for a rollback to wait on")
				require.Equal(t, 0, f.unleaseCalls)
				require.Equal(t, 0, f.finalizeCalls)
			})
		}
	})

	t.Run("a per-input error that is not a verdict of the store keeps the lease", func(t *testing.T) {
		for name, fault := range spendFaults {
			f := &fakeRollForwarder{spends: []*Spend{{}, {Err: fault}}, spendErr: errors.NewUtxoError("spend failed")}

			_, completed, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
			require.Error(t, err, name)
			require.False(t, completed, name)
			require.Equal(t, []string{"lease", "spend"}, f.callSeq, "%s: the write of that input may be in doubt, so the lease must stay live", name)
		}
	})

	t.Run("a per-input error that is a verdict of the store releases the lease", func(t *testing.T) {
		for name, verdict := range spendVerdicts {
			f := &fakeRollForwarder{spends: []*Spend{{}, {Err: verdict}}, spendErr: errors.NewUtxoError("spend failed")}

			_, completed, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
			require.Error(t, err, name)
			require.False(t, completed, name)
			require.Equal(t, []string{"lease", "spend", "getmeta", "unlease"}, f.callSeq, "%s: the store refused the input, so no write of it can still land", name)
		}
	})

	t.Run("a record whose finalize has begun is finalized although its re-spend is refused", func(t *testing.T) {
		attempt := []byte{1}

		// The spends that reach the caller are built from the transaction, so its inputs are real.
		spent := bt.NewTx()
		require.NoError(t, spent.From(strings.Repeat("aa", 32), 0, "76a914000000000000000000000000000000000000000088ac", 1000))
		require.NoError(t, spent.From(strings.Repeat("bb", 32), 1, "76a914000000000000000000000000000000000000000088ac", 2000))

		for name, tc := range map[string]struct {
			spends   []*Spend
			spendErr error
		}{
			"a parent is locked":      {[]*Spend{{}, {Err: errors.ErrTxLocked}}, errors.NewUtxoError("spend failed")},
			"a parent is conflicting": {[]*Spend{{}, {Err: errors.ErrTxConflicting}}, errors.NewUtxoError("spend failed")},
			"a parent is gone":        {[]*Spend{{}, {Err: errors.ErrTxNotFound}}, errors.NewUtxoError("spend failed")},
			"an input is not covered": {[]*Spend{{}}, nil},
		} {
			f := &fakeRollForwarder{spends: tc.spends, spendErr: tc.spendErr, existing: meta.Data{Creating: true, Finalizing: true, Attempt: attempt}}

			spends, completed, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, spent, attempt, 100)
			require.NoError(t, err, name)
			require.True(t, completed, name)
			require.Equal(t, []string{"lease", "spend", "getmeta", "finalize"}, f.callSeq, "%s: the inputs were spent before the finalize began, so only the finalize is left", name)
			require.Equal(t, fakeLease, f.finalizeLease, name)
			require.Len(t, spends, len(spent.Inputs), "%s: the spends of the transaction reach the caller for its hand-off", name)

			for _, sp := range spends {
				require.NoError(t, sp.Err, name)
			}
		}
	})

	t.Run("a refused re-spend is not set aside unless the record of the attempt carries the mark", func(t *testing.T) {
		attempt := []byte{1}

		for name, existing := range map[string]meta.Data{
			"not finalizing":                 {Creating: true, Attempt: attempt},
			"finalizing under another token": {Creating: true, Finalizing: true, Attempt: []byte{2}},
			"rolling back":                   {Creating: true, Finalizing: true, RollingBack: true, Attempt: attempt},
			"not creating":                   {Finalizing: true, Attempt: attempt},
		} {
			f := &fakeRollForwarder{spends: []*Spend{{}, {Err: errors.ErrTxLocked}}, spendErr: errors.NewUtxoError("spend failed"), existing: existing}

			_, completed, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, attempt, 100)
			require.Error(t, err, name)
			require.False(t, completed, name)
			require.Equal(t, []string{"lease", "spend", "getmeta", "unlease"}, f.callSeq, name)
		}

		unreadable := &fakeRollForwarder{spends: []*Spend{{}, {Err: errors.ErrTxLocked}}, spendErr: errors.NewUtxoError("spend failed"), getMetaErr: errors.NewStorageError("read failed")}

		_, completed, err := RollForwardCreating(ctx, ulogger.TestLogger{}, unreadable, tx, attempt, 100)
		require.ErrorIs(t, err, errors.ErrUtxoError, "a record that cannot be read is not known to carry the mark")
		require.False(t, completed)
		require.Zero(t, unreadable.finalizeCalls)
	})

	t.Run("a re-spend whose outcome is unknown is not set aside for a finalizing record", func(t *testing.T) {
		attempt := []byte{1}
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, spendErr: context.DeadlineExceeded, existing: meta.Data{Creating: true, Finalizing: true, Attempt: attempt}}

		_, completed, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, attempt, 100)
		require.Error(t, err)
		require.False(t, completed)
		require.Equal(t, []string{"lease", "spend"}, f.callSeq, "the lease stays, and the next roll-forward completes the record")
	})

	t.Run("completed is true only for the call whose finalize cleared the creating state", func(t *testing.T) {
		won := &fakeRollForwarder{spends: []*Spend{{}, {}}}

		spends, completed, err := RollForwardCreating(ctx, ulogger.TestLogger{}, won, tx, []byte{1}, 100)
		require.NoError(t, err)
		require.True(t, completed)
		require.Equal(t, won.spends, spends)

		lost := &fakeRollForwarder{spends: []*Spend{{}, {}}, finalizeErr: MarkAlreadyFinal(errors.NewTxExistsError("already final"))}

		spends, completed, err = RollForwardCreating(ctx, ulogger.TestLogger{}, lost, tx, []byte{1}, 100)
		require.NoError(t, err, "the record is complete, so this is not a failure")
		require.False(t, completed, "another actor's finalize completed the record and owns its hand-off")
		require.Equal(t, lost.spends, spends)
		require.Equal(t, []string{"lease", "spend", "finalize", "unlease"}, lost.callSeq, "a finalize removes only the lease of its caller, so the loser releases its own")
	})

	t.Run("the finalize names the lease of the roll-forward, and a try that failed is repeated under it", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, finalizeErrs: []error{errors.NewStorageError("answer lost")}}

		spends, completed, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.NoError(t, err)
		require.True(t, completed, "the store tells the repeat whether its own write cleared the state, so a lost answer does not lose the hand-off")
		require.Equal(t, f.spends, spends)
		require.Equal(t, []string{"lease", "spend", "finalize", "finalize"}, f.callSeq)
		require.NotEmpty(t, f.finalizeLease)
		require.Equal(t, fakeLease, f.finalizeLease, "the store records this lease as the caller of the write")
		require.NotEqual(t, []byte{1}, f.finalizeLease, "the lease of a roll-forward is not the attempt token")
	})

	t.Run("the spend and the finalize are bound by the lease, and the finalize outlives the caller's context", func(t *testing.T) {
		callerCtx, cancel := context.WithCancel(ctx)

		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, afterSpend: cancel}

		_, completed, err := RollForwardCreating(callerCtx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.NoError(t, err)
		require.True(t, completed)
		require.True(t, f.spendLeaseBound, "the store must not start the write of this spend after the time its lease names")
		require.True(t, f.finalizeLeaseBound)
		require.NoError(t, f.finalizeCtxErr, "the spends are done, so the finalize runs to its end: a finalize that stopped with its caller leaves a write that may land with nobody to read its answer")
		require.False(t, f.finalizeDeadline.IsZero(), "a finalize is bounded by the rollback bound")
	})

	t.Run("a finalize that fails on every try releases the lease, returns the spends and leaves them in place", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, finalizeErr: errors.NewStorageError("finalize boom")}

		spends, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, f.spends, spends)
		require.Equal(t, []string{"lease", "spend", "finalize", "finalize", "finalize", "unlease"}, f.callSeq, "the record is still creating, so it stays the recovery journal and nothing is reversed")
		require.Equal(t, [][2][]byte{{{1}, fakeLease}}, f.unleased)
	})

	t.Run("a finalize that finds the record gone or rolling back releases the lease and reverses nothing", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, finalizeErr: errors.NewTxNotFoundError("rolling back")}

		spends, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.Equal(t, f.spends, spends)
		require.Equal(t, []string{"lease", "spend", "finalize", "unlease"}, f.callSeq,
			"the owner of the rollback waits for this lease and then reverses every input; the roll-forward only releases it")
		require.Equal(t, [][2][]byte{{{1}, fakeLease}}, f.unleased)
		require.Equal(t, 0, f.getMetaCalls, "no read and no take of a hold follow a refused finalize")
	})

	t.Run("the finalize carries the token the caller names", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, existing: meta.Data{Creating: true, Attempt: []byte{4, 5}}}

		_, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{4, 5}, 100)
		require.NoError(t, err)
		require.Equal(t, []byte{4, 5}, f.finalizeToken)
		require.Equal(t, [][]byte{{4, 5}}, f.leaseTokens)
	})

	t.Run("a lease refused for a record creating under another attempt spends nothing", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, leaseDeclined: true, existing: meta.Data{Creating: true, Attempt: []byte{2}}}

		spends, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Contains(t, err.Error(), "another attempt")
		require.Nil(t, spends)
		require.Equal(t, []string{"lease", "getmeta"}, f.callSeq, "a stale roll-forward must not spend the parents of a later attempt")
		require.Equal(t, 0, f.unleaseCalls, "nothing was taken, so nothing is released")
	})

	t.Run("a lease refused for a record that is rolling back spends nothing", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, leaseDeclined: true, existing: meta.Data{Creating: true, RollingBack: true}}

		spends, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Contains(t, err.Error(), "rolling back")
		require.Nil(t, spends)
		require.Equal(t, []string{"lease", "getmeta"}, f.callSeq, "a rolling back record must not have its parents spent")
		require.Equal(t, 0, f.unleaseCalls)
	})

	t.Run("a lease refused for an absent record spends nothing", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, leaseDeclined: true, getMetaErr: errors.NewTxNotFoundError("gone")}

		spends, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.Nil(t, spends)
		require.Equal(t, []string{"lease", "getmeta"}, f.callSeq, "an absent record must not have its parents spent")
	})

	t.Run("a lease refused although the record is creating under this attempt is a retryable error", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}, leaseDeclined: true, existing: meta.Data{Creating: true, Attempt: []byte{1}}}

		spends, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, []byte{1}, 100)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Nil(t, spends)
		require.Equal(t, []string{"lease", "getmeta"}, f.callSeq, "without a lease nothing is spent, whatever the read says")
	})

	t.Run("a lease refused for a final record returns the spends of the tx without spending or finalizing", func(t *testing.T) {
		scripted := scriptedTwoInputTx(t)
		f := &fakeRollForwarder{leaseDeclined: true}

		spends, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, scripted, []byte{1}, 100)
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

		spends, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Nil(t, spends)
		require.Equal(t, []string{"lease"}, f.callSeq, "a lease that may or may not be registered is never spent under")
	})

	t.Run("the lease, the spend and the finalize run under a context bounded by the rollback bound", func(t *testing.T) {
		f := &fakeRollForwarder{spends: []*Spend{{}, {}}}

		before := time.Now()
		_, _, err := RollForwardCreating(ctx, ulogger.TestLogger{}, f, tx, nil, 100)
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
		require.Equal(t, []string{"create", "spend", "unlease", "begin", "unspend", "release", "delete"}, f.callSeq,
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
		require.Equal(t, []string{"create", "spend", "unlease", "begin", "unspend", "release", "delete"}, f.callSeq)
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
		require.Equal(t, []string{"create", "spend", "unlease", "begin", "unspend", "release", "delete"}, f.callSeq)
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
		require.Equal(t, []string{"create", "spend", "unlease", "begin", "unspend", "release", "delete"}, f.callSeq)
		require.Len(t, f.unspentWith, 2)
		require.Len(t, f.deletedHashes, 1)
		require.Equal(t, tx.TxIDChainHash().String(), f.deletedHashes[0].String())
	})

	t.Run("a rollback whose claim is refused for a live lease tries again and then reverses", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrSpent}, {}},
			spendErr:   errors.NewUtxoError("double spend"),
			createMeta: &meta.Data{Creating: true, Attempt: []byte{9}},
			beginErrs:  []error{leaseLive(), leaseLive()},
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.NotContains(t, err.Error(), "reversing", "the rollback completed")
		require.Equal(t, []string{"create", "spend", "unlease", "begin", "begin", "begin", "unspend", "release", "delete"}, f.callSeq,
			"a concurrent roll-forward's spend lands before the claim, so the reversal undoes it")
	})

	t.Run("a rollback whose spender never releases its lease reverses nothing and is reported", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrSpent}, {}},
			spendErr:   errors.NewUtxoError("double spend"),
			createMeta: &meta.Data{Creating: true, Attempt: []byte{9}},
			beginErr:   leaseLive(),
			spendWait:  30 * time.Millisecond,
		}

		_, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Contains(t, err.Error(), "double spend")
		require.Contains(t, err.Error(), "holds a live lease", "the refusal of the claim is reported")
		require.Len(t, spends, 2)
		require.Equal(t, 0, f.unspendCalls, "a spend may still land, so a reversal now could be undone by it")
		require.Equal(t, 0, f.deleteCalls, "the rollback never began, so it is not ended")
		require.Equal(t, 0, f.releaseCalls, "no claim was made, so there is no hold to release")
		require.Greater(t, f.beginCalls, 1, "the claim is tried again while the wait lasts")
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
		require.Equal(t, []string{"create", "getmeta", "lease", "spend", "getmeta", "unlease"}, f.callSeq, "no finalize, no rollback; the lease is released")
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
		shortenUnspendBackoff(t)

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
		require.Equal(t, []string{"create", "getmeta", "lease", "spend", "finalize", "finalize", "finalize", "unlease"}, f.callSeq)
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
		require.Equal(t, []string{"create", "unlease", "begin", "unspend", "release", "delete"}, f.callSeq,
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

	t.Run("a per-input error that is not a verdict of the store keeps the creator's lease and rolls nothing back", func(t *testing.T) {
		for name, fault := range spendFaults {
			f := &fakeSequentialStore{
				spends:     []*Spend{{}, {Err: fault}},
				spendErr:   errors.NewUtxoError("spend failed"),
				createMeta: &meta.Data{Creating: true, Attempt: []byte{9}},
			}

			md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
			require.ErrorIs(t, err, errors.ErrUtxoError, name)
			require.Nil(t, md, name)
			require.Len(t, spends, 2, name)
			require.Equal(t, []string{"create", "spend"}, f.callSeq, "%s: the write of that input may be in doubt, so the lease stays and no rollback begins", name)
		}
	})

	t.Run("a per-input error that is a verdict of the store releases the creator's lease and rolls back", func(t *testing.T) {
		for name, verdict := range spendVerdicts {
			if name == "locked" || name == "creating" {
				continue // a parent that is committing: see the next test
			}

			f := &fakeSequentialStore{
				spends:     []*Spend{{}, {Err: verdict}},
				spendErr:   errors.NewUtxoError("spend failed"),
				createMeta: &meta.Data{Creating: true, Attempt: []byte{9}},
			}

			_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
			require.ErrorIs(t, err, errors.ErrUtxoError, name)
			require.Equal(t, []string{"create", "spend", "unlease", "begin", "unspend", "release", "delete"}, f.callSeq, name)
		}
	})

	t.Run("the spend of a create-first call is bound by its lease", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}}, createMeta: &meta.Data{Creating: true, Attempt: []byte{9}}}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.NoError(t, err)
		require.Equal(t, []bool{true}, f.spendLeaseBound)

		plain := &fakeSequentialStore{spends: []*Spend{{}}, createMeta: &meta.Data{}}

		_, _, err = SequentialSpendAndCreate(ctx, logger, plain, tx, 100, false)
		require.NoError(t, err)
		require.Equal(t, []bool{false}, plain.spendLeaseBound, "a spend that no lease bounds is not held to a deadline")
	})

	t.Run("a parent that is still committing keeps the record creating and leaves no hold", func(t *testing.T) {
		for name, transient := range map[string]error{
			"locked":   errors.ErrTxLocked,
			"creating": errors.ErrTxCreating,
		} {
			f := &fakeSequentialStore{
				spends:     []*Spend{{}, {Err: transient}},
				spendErr:   errors.NewTxLockedError("parent is committing", errors.NewUtxoError("spend failed", transient)),
				createMeta: &meta.Data{Creating: true, Attempt: []byte{9}},
			}

			md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
			require.Nil(t, md, name)
			require.Len(t, spends, 2, name)
			require.ErrorIs(t, err, errors.ErrTxLocked, "%s: the retry loop of the validator keys on this error", name)
			require.Equal(t, []string{"create", "spend", "unlease"}, f.callSeq,
				"%s: no marker, no reversal and no hold: a retry finds the record creating, and spends and finalizes it", name)
		}

		// One input that can never be spent decides it: the rollback runs.
		mixed := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.ErrTxLocked}, {Err: errors.ErrSpent}},
			spendErr:   errors.NewUtxoError("spend failed"),
			createMeta: &meta.Data{Creating: true, Attempt: []byte{9}},
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, mixed, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.Equal(t, []string{"create", "spend", "unlease", "begin", "unspend", "release", "delete"}, mixed.callSeq)
	})

	t.Run("a finalize that finds the record already final on its first try is another actor's: an ordinary duplicate", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:      []*Spend{{}},
			createMeta:  &meta.Data{Creating: true, Attempt: []byte{9}},
			finalizeErr: MarkAlreadyFinal(errors.NewTxExistsError("already final")),
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, f, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.False(t, IsRolledForward(err), "the actor that cleared the creating state owns the hand-off, so this call must not hand the tx on")
		require.Nil(t, md)
		require.Len(t, spends, 1, "the spends are in effect")
		require.Equal(t, []string{"create", "spend", "finalize", "unlease"}, f.callSeq, "no retry and no rollback; the creator releases its lease, which the other actor's finalize left on the record")
	})

	t.Run("a finalize try that failed is repeated, and the store says whose write cleared the state", func(t *testing.T) {
		shortenUnspendBackoff(t)

		// The store answers nil to the repeat when the write of the first try was this caller's.
		own := &fakeSequentialStore{
			spends:       []*Spend{{}},
			createMeta:   &meta.Data{Creating: true, Attempt: []byte{9}},
			finalizeErrs: []error{errors.NewStorageError("answer lost")},
		}

		md, spends, err := SequentialSpendAndCreate(ctx, logger, own, tx, 100, true)
		require.NoError(t, err)
		require.NotNil(t, md)
		require.False(t, md.Creating)
		require.Len(t, spends, 1)
		require.Equal(t, []string{"create", "spend", "finalize", "finalize"}, own.callSeq)
		require.Equal(t, [][]byte{{9}, {9}}, own.finalizeLeases, "the lease of the call that creates a record is its attempt token, on every try")

		// It answers already final when a roll-forward cleared the state during the backoff. The
		// creator then owns neither the hand-off nor an unwind, whatever the number of the try.
		other := &fakeSequentialStore{
			spends:       []*Spend{{}},
			createMeta:   &meta.Data{Creating: true, Attempt: []byte{9}},
			finalizeErrs: []error{errors.NewStorageError("not applied"), MarkAlreadyFinal(errors.NewTxExistsError("already final"))},
		}

		md, spends, err = SequentialSpendAndCreate(ctx, logger, other, tx, 100, true)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.False(t, IsRolledForward(err), "the roll-forward delivered the tx, so this call must not hand it on")
		require.Nil(t, md, "a call that returns no record has nothing to unwind")
		require.Len(t, spends, 1)
		require.Equal(t, []string{"create", "spend", "finalize", "finalize", "unlease"}, other.callSeq, "the loser releases its own lease")
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
		require.Equal(t, []string{"begin", "unspend", "release", "delete"}, f.callSeq)
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

	t.Run("a claim refused for a live spend lease is tried again, and the unspend runs only after it is granted", func(t *testing.T) {
		f := &fakeSequentialStore{beginErrs: []error{leaseLive(), leaseLive(), leaseLive()}}

		require.NoError(t, RollBackCreating(ctx, logger, f, tx, []byte{6}))
		require.Equal(t, []string{"begin", "begin", "begin", "begin", "unspend", "release", "delete"}, f.callSeq,
			"a spend that is in flight lands before the claim, so the reversal covers it")
		require.Equal(t, [][]byte{{6}, {6}, {6}, {6}}, f.beginTokens)
		require.Equal(t, 0, f.leasesCalls, "the store decides inside the claim, so no separate read of the leases is made")
	})

	t.Run("a claim that stays refused ends the wait: nothing is written, unspent, released or ended", func(t *testing.T) {
		f := &fakeSequentialStore{beginErr: leaseLive(), spendWait: 30 * time.Millisecond}

		start := time.Now()
		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.True(t, IsSpendLeaseLive(err), "the store's refusal is returned, got: %v", err)
		require.Less(t, time.Since(start), 5*time.Second, "the wait is the spend wait, not the rollback bound")
		require.Greater(t, f.beginCalls, 1, "the claim is tried again while the wait lasts")
		require.Equal(t, 0, f.unspendCalls, "a spend may still land, so nothing is reversed")
		require.Equal(t, 0, f.releaseCalls, "no claim was made, so there is no hold to release")
		require.Equal(t, 0, f.deleteCalls, "the record stays creating")

		for _, call := range f.callSeq {
			require.Equal(t, "begin", call)
		}
	})

	t.Run("the wait for a live lease defaults to the spend wait of the settings when the store names none", func(t *testing.T) {
		require.Equal(t, defaultSpendLeaseWait, spendLeaseWait(&fakeSequentialStore{}))
		require.Equal(t, defaultSpendLeaseWait, spendLeaseWait(struct{}{}))
		require.Equal(t, 7*time.Second, spendLeaseWait(&fakeSequentialStore{spendWait: 7 * time.Second}))
	})

	t.Run("a claim refused for a live lease stops waiting when the caller's rollback context ends", func(t *testing.T) {
		f := &boundedSequentialStore{fakeSequentialStore: fakeSequentialStore{beginErr: leaseLive(), spendWait: time.Hour}, bound: 40 * time.Millisecond}

		start := time.Now()
		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.True(t, IsSpendLeaseLive(err))
		require.Less(t, time.Since(start), 5*time.Second, "the rollback bound ends the wait")
		require.Equal(t, 0, f.unspendCalls)
	})

	t.Run("a claim that fails for another reason is returned at once", func(t *testing.T) {
		f := &fakeSequentialStore{beginErr: errors.NewStorageError("begin boom")}

		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.False(t, IsSpendLeaseLive(err))
		require.Equal(t, []string{"begin"}, f.callSeq)
	})

	t.Run("a marker that reports false reads no lease", func(t *testing.T) {
		f := &fakeSequentialStore{beginDeclined: true, leasesLive: -1}

		require.NoError(t, RollBackCreating(ctx, logger, f, tx, nil))
		require.Equal(t, []string{"begin"}, f.callSeq, "another caller owns the reversal")
	})

	t.Run("the owner holds the hold for the whole unspend and releases it before it ends the rollback", func(t *testing.T) {
		f := &fakeSequentialStore{}

		require.NoError(t, RollBackCreating(ctx, logger, f, tx, []byte{6, 6}))
		require.Equal(t, []string{"begin", "unspend", "release", "delete"}, f.callSeq)
		require.Equal(t, 1, f.releaseCalls)
	})

	t.Run("a failed release returns the error and leaves the record rolling back", func(t *testing.T) {
		f := &fakeSequentialStore{releaseErr: errors.NewStorageError("release boom")}

		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, []string{"begin", "unspend", "release"}, f.callSeq, "the rollback is not ended while the hold could not be released")
		require.Equal(t, 0, f.deleteCalls)
	})

	t.Run("a parent that is already gone does not fail the rollback", func(t *testing.T) {
		f := &fakeSequentialStore{unspendErrs: []error{errors.NewTxNotFoundError("gone")}}

		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.NoError(t, err)
		require.Equal(t, 3, f.unspendCalls, "one batch call, then one call per input")
		require.Equal(t, []string{"begin", "unspend", "unspend", "unspend", "release", "delete"}, f.callSeq)
	})

	t.Run("an input whose UTXO hash does not match is skipped and the other input is unspent", func(t *testing.T) {
		f := &fakeSequentialStore{unspendErrs: []error{
			errors.NewUtxoHashMismatchError("mismatch"),
			nil,
			errors.NewUtxoHashMismatchError("mismatch"),
		}}

		err := RollBackCreating(ctx, logger, f, tx, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"begin", "unspend", "unspend", "unspend", "release", "delete"}, f.callSeq)
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
		require.Equal(t, []string{"begin", "release", "delete"}, f.callSeq)
	})

	t.Run("it runs under its own context after the caller is cancelled", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		f := &fakeSequentialStore{}

		err := RollBackCreating(cancelled, logger, f, tx, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"begin", "unspend", "release", "delete"}, f.callSeq)
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

func TestIsAlreadyFinal(t *testing.T) {
	require.Nil(t, MarkAlreadyFinal(nil))
	require.False(t, IsAlreadyFinal(nil))
	require.False(t, IsAlreadyFinal(errors.NewTxExistsError("plain duplicate")))
	require.False(t, IsAlreadyFinal(context.Canceled))

	marked := MarkAlreadyFinal(errors.NewTxExistsError("already final"))
	require.True(t, IsAlreadyFinal(marked))
	require.ErrorIs(t, marked, errors.ErrTxExists)
	require.True(t, IsAlreadyFinal(errors.NewProcessingError("wrapped", marked)), "callers wrap the error before they test it")
	require.False(t, IsSpendLeaseLive(marked), "the markers are independent")
	require.False(t, IsRolledForward(marked))
}

func TestIsSpendLeaseLive(t *testing.T) {
	require.Nil(t, MarkSpendLeaseLive(nil))
	require.False(t, IsSpendLeaseLive(nil))
	require.False(t, IsSpendLeaseLive(errors.NewProcessingError("plain")))

	marked := MarkSpendLeaseLive(errors.NewProcessingError("lease live"))
	require.True(t, IsSpendLeaseLive(marked))
	require.True(t, IsSpendLeaseLive(errors.NewStorageError("wrapped", marked)))
	require.False(t, IsAlreadyFinal(marked))
}

func TestSpendOutcomeUnknown(t *testing.T) {
	failed := errors.NewUtxoError("spend failed")

	require.True(t, spendOutcomeUnknown(nil, failed), "no per-input result")
	require.True(t, spendOutcomeUnknown([]*Spend{{}}, context.DeadlineExceeded))
	require.True(t, spendOutcomeUnknown([]*Spend{{}}, errors.NewServiceUnavailableError("gave up waiting")))
	require.True(t, spendOutcomeUnknown([]*Spend{{}, nil}, failed), "a failure that names no input and is not a verdict itself")
	require.True(t, spendOutcomeUnknown([]*Spend{{}}, errors.NewStorageError("fault")), "a storage fault that names no input")
	require.False(t, spendOutcomeUnknown([]*Spend{{}, nil}, errors.NewUtxoError("spent", errors.ErrSpent)), "a failure that names no input and is a verdict itself")
	require.False(t, spendOutcomeUnknown([]*Spend{{}, nil}, errors.NewUtxoSpendingTxPrunedError("replay")), "the refusal of a replay of a pruned transaction is a verdict itself")

	for name, verdict := range spendVerdicts {
		require.False(t, spendOutcomeUnknown([]*Spend{{}, {Err: verdict}}, failed), name)
		require.False(t, spendOutcomeUnknown([]*Spend{{Err: errors.NewUtxoError("wrapped", verdict)}}, failed), "%s wrapped", name)
	}

	for name, fault := range spendFaults {
		require.True(t, spendOutcomeUnknown([]*Spend{{Err: errors.ErrSpent}, {Err: fault}}, failed), "%s: one input in doubt is enough", name)
		require.True(t, spendOutcomeUnknown([]*Spend{{Err: errors.NewUtxoError("wrapped", fault)}}, failed), "%s wrapped in a UTXO error", name)
	}

	// INVALID_SPEND reaches the caller as a UTXO error with no cause. On one input it is a verdict.
	require.False(t, spendOutcomeUnknown([]*Spend{{Err: errors.NewUtxoError("invalid spend for vout 1")}}, failed))
}

// TestCreateFirstReversalsCarryTheReversalGuard pins that every create-first reversal runs its
// Unspend under the guard that names the tentative record and the height of the call, so a store
// can keep the spends that predate the record, and that no other reversal does.
func TestCreateFirstReversalsCarryTheReversalGuard(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tx := scriptedTwoInputTx(t)
	hash := tx.TxIDChainHash()

	const callHeight = uint32(100)

	requireGuarded := func(t *testing.T, f *fakeSequentialStore, wantCallHeight uint32) {
		t.Helper()

		require.NotEmpty(t, f.unspendGuards, "the reversal ran")
		require.Zero(t, f.unguardedUnspends, "no reversal of the rollback ran without the guard")

		for _, guard := range f.unspendGuards {
			require.NotNil(t, guard.Record)
			require.Equal(t, *hash, *guard.Record, "the guard names the record being rolled back")
			require.Equal(t, wantCallHeight, guard.CallHeight)
		}
	}

	t.Run("RollBackCreating names the record, with the call height when the caller sets it", func(t *testing.T) {
		f := &fakeSequentialStore{}

		require.NoError(t, RollBackCreating(WithReversalCallHeight(ctx, callHeight), logger, f, tx, []byte{1}))
		requireGuarded(t, f, callHeight)

		bare := &fakeSequentialStore{}

		require.NoError(t, RollBackCreating(ctx, logger, bare, tx, []byte{1}))
		requireGuarded(t, bare, 0)
	})

	t.Run("FinishRollBackCreating names the record", func(t *testing.T) {
		f := &fakeSequentialStore{}

		finished, err := FinishRollBackCreating(WithReversalCallHeight(ctx, callHeight), logger, f, tx, []byte{9}, false)
		require.NoError(t, err)
		require.True(t, finished)
		requireGuarded(t, f, callHeight)
	})

	t.Run("a create-first spend that fails rolls back under the guard", func(t *testing.T) {
		f := &fakeSequentialStore{
			spends:     []*Spend{{Err: errors.NewTxNotFoundError("gone")}},
			spendErr:   errors.NewUtxoSpendingTxPrunedError("replay"),
			createMeta: &meta.Data{Creating: true, Attempt: []byte{2}},
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, callHeight, true)
		require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)
		requireGuarded(t, f, callHeight)
	})

	t.Run("a spend-first call settles the record of a rollback under the guard", func(t *testing.T) {
		shortenUnspendBackoff(t)

		earlier := []*Spend{{Vout: 0}, {Vout: 1}}
		spendErr := errors.NewStorageError("the spend of the second round failed")

		back := &fakeSequentialStore{
			spendsSeq:   [][]*Spend{earlier, {{Vout: 0}}},
			spendErrs:   []error{nil, spendErr},
			createErr:   errors.NewTxExistsError("held"),
			existingSeq: []meta.Data{{Creating: true, RollingBack: true, Hold: true}, {Creating: true, RollingBack: true, Hold: true}},
			existing:    meta.Data{Creating: true, RollingBack: true, Attempt: []byte{6}},
			clearResult: true,
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, back, tx, callHeight, false)
		require.ErrorIs(t, err, spendErr)
		require.Equal(t, earlier, back.unspentWith)
		requireGuarded(t, back, callHeight)
	})

	t.Run("a spend-first call that claims the txid to reverse an earlier round rolls back under the guard", func(t *testing.T) {
		shortenUnspendBackoff(t)

		earlier := []*Spend{{Vout: 0}, {Vout: 1}}
		spendErr := errors.NewStorageError("the spend of the second round failed")

		gone := &fakeSequentialStore{
			spendsSeq:   [][]*Spend{earlier, {{Vout: 0}}},
			spendErrs:   []error{nil, spendErr},
			createErrs:  []error{errors.NewTxExistsError("held")},
			createMeta:  &meta.Data{Creating: true, Attempt: []byte{3}},
			existing:    meta.Data{Creating: true, RollingBack: true, Hold: true},
			clearResult: true,
			getMetaErrs: []error{nil, nil, errors.NewTxNotFoundError("no record")},
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, gone, tx, callHeight, false)
		require.ErrorIs(t, err, spendErr)
		requireGuarded(t, gone, callHeight)
	})

	t.Run("the reversal of a spend-first create that failed has no record to guard", func(t *testing.T) {
		shortenUnspendBackoff(t)

		f := &fakeSequentialStore{
			spends:    []*Spend{{Vout: 0}},
			createErr: errors.NewStorageError("create failed"),
		}

		_, _, err := SequentialSpendAndCreate(ctx, logger, f, tx, callHeight, false)
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.Equal(t, 1, f.unguardedUnspends, "main's reversal clears what it is told to, as before")
		require.Empty(t, f.unspendGuards)
	})
}

func TestIsSpendVerdictKnowsThePrunedReplayRefusal(t *testing.T) {
	refusal := errors.NewUtxoSpendingTxPrunedError("[SPEND] invalid spend: spending transaction was pruned")

	require.True(t, isSpendVerdict(refusal), "the store evaluated the spend and refused it before any write could land")
	require.True(t, isSpendVerdict(errors.NewUtxoError("wrapped", refusal)))
	require.False(t, spendFailureTransient([]*Spend{{Err: refusal}}), "the refusal does not end by itself, so the record is rolled back")
	require.True(t, RefusedSpend(errors.NewUtxoError("spend failed", refusal), []*Spend{{Err: refusal}}), "the lane position is counted as refused")
	require.True(t, LaneSpendWroteNothing([]*Spend{{Err: refusal}}), "the refusal is made before any write, and an idempotent match writes no lane slot")
	require.False(t, LaneSpendWroteNothing([]*Spend{{}, {Err: refusal}}), "an input beside the refusal that was spent may have written its lane slot, so the position stays burned")
}

// settlerStore adds the spend-only call of a roll-forward to the sequential fake, which answers it
// with its scripted spend.
type settlerStore struct {
	*fakeSequentialStore
}

func (s settlerStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, []*Spend, error) {
	o := CreateOptions{}
	for _, opt := range opts {
		opt(&o)
	}

	spends, err := s.Spend(ctx, tx, blockHeight, o.IgnoreFlags)

	return nil, spends, err
}

// TestSettleTentativeInTheWay pins the rule by which a block ends the create-first attempt whose
// record is in the way of one of its transactions: the rule of the pruner sweep, run at once. The
// record is rolled forward, and it is rolled back only when the transaction can never be completed
// and no spender of the attempt holds a live lease.
func TestSettleTentativeInTheWay(t *testing.T) {
	ctx := context.Background()
	attempt := []byte{7}

	// A rollback reverses the spends it builds from the transaction, so its inputs are real.
	tx := bt.NewTx()
	require.NoError(t, tx.From(strings.Repeat("aa", 32), 0, "76a914000000000000000000000000000000000000000088ac", 1000))
	require.NoError(t, tx.From(strings.Repeat("bb", 32), 1, "76a914000000000000000000000000000000000000000088ac", 2000))

	settle := func(f *fakeSequentialStore, existing *meta.Data, opts ...CreateOption) (bool, error) {
		return SettleTentativeInTheWay(ctx, ulogger.TestLogger{}, settlerStore{f}, tx, existing, 100, opts...)
	}

	t.Run("a record that is not a tentative record of a create-first attempt is left alone", func(t *testing.T) {
		for name, existing := range map[string]*meta.Data{
			"no record":       nil,
			"a hold":          {Creating: true, RollingBack: true, Hold: true, Attempt: attempt},
			"no token":        {Creating: true},
			"a stored record": {Attempt: attempt},
		} {
			f := &fakeSequentialStore{}

			settled, err := settle(f, existing)
			require.NoError(t, err, name)
			require.False(t, settled, name)
			require.Empty(t, f.callSeq, "%s: nothing is read, spent or written", name)
		}
	})

	t.Run("a creating record whose inputs can be spent is rolled forward", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}, {}}}

		settled, err := settle(f, &meta.Data{Creating: true, Attempt: attempt}, WithIgnoreLocked(true))
		require.NoError(t, err)
		require.True(t, settled, "the record is final")
		require.Equal(t, []string{"lease", "spend", "finalize"}, f.callSeq)
		require.Equal(t, [][]byte{attempt}, f.leaseTokens)
		require.True(t, f.spendFlags[0].IgnoreLocked, "the re-spend runs under the flags of the block's own spend")
		require.Zero(t, f.beginCalls, "nothing is rolled back")
	})

	t.Run("a creating record that can never be completed is rolled back when no lease is live", func(t *testing.T) {
		for name, verdict := range map[string]error{
			"spent by another tx": errors.ErrSpent,
			"conflicting parent":  errors.ErrTxConflicting,
			"frozen output":       errors.ErrFrozen,
			"hash mismatch":       errors.ErrUtxoHashMismatch,
			"missing parent":      errors.ErrTxNotFound,
			"pruned replay":       errors.ErrUtxoSpendingTxPruned,
		} {
			f := &fakeSequentialStore{spends: []*Spend{{}, {Err: verdict}}, spendErr: errors.NewUtxoError("spend failed", verdict)}

			settled, err := settle(f, &meta.Data{Creating: true, Attempt: attempt})
			require.NoError(t, err, name)
			require.True(t, settled, "%s: the rollback ended and left a hold", name)
			require.Equal(t, []string{"lease", "spend", "getmeta", "unlease", "leases", "begin", "unspend", "release", "delete"}, f.callSeq, name)
			require.Equal(t, [][]byte{attempt}, f.beginTokens, "%s: the rollback names the attempt that was read", name)
			require.Len(t, f.unspentWith, len(tx.Inputs), "%s: every input of the transaction is reversed", name)
		}
	})

	t.Run("a live lease keeps a record that can never be completed from being rolled back", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}, {Err: errors.ErrSpent}}, spendErr: errors.NewUtxoError("spend failed", errors.ErrSpent), leasesLive: -1}

		settled, err := settle(f, &meta.Data{Creating: true, Attempt: attempt})
		require.NoError(t, err)
		require.False(t, settled)
		require.Equal(t, []string{"lease", "spend", "getmeta", "unlease", "leases"}, f.callSeq, "a spender may still be at work, so no rollback is claimed and the call does not wait for the lease")
		require.Zero(t, f.beginCalls)
	})

	t.Run("a roll-forward that failed for a reason that ends by itself leaves the record", func(t *testing.T) {
		for name, tc := range map[string]struct {
			spends   []*Spend
			spendErr error
		}{
			"locked parent":   {[]*Spend{{}, {Err: errors.ErrTxLocked}}, errors.NewUtxoError("spend failed", errors.ErrTxLocked)},
			"creating parent": {[]*Spend{{}, {Err: errors.ErrTxCreating}}, errors.NewUtxoError("spend failed", errors.ErrTxCreating)},
			"unknown outcome": {[]*Spend{{}, {}}, context.DeadlineExceeded},
		} {
			f := &fakeSequentialStore{spends: tc.spends, spendErr: tc.spendErr}

			settled, err := settle(f, &meta.Data{Creating: true, Attempt: attempt})
			require.NoError(t, err, name)
			require.False(t, settled, name)
			require.Zero(t, f.leasesCalls, name)
			require.Zero(t, f.beginCalls, "%s: nothing is rolled back", name)
		}
	})

	t.Run("a rollback that the store refuses is not a settled record", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}, {Err: errors.ErrSpent}}, spendErr: errors.NewUtxoError("spend failed", errors.ErrSpent), beginDeclined: true}

		settled, err := settle(f, &meta.Data{Creating: true, Attempt: attempt})
		require.NoError(t, err)
		require.False(t, settled, "the record is final, finalizing or another caller's, and nothing was reversed")
		require.Zero(t, f.unspendCalls)
	})

	t.Run("a failed read of the leases is an error and nothing is rolled back", func(t *testing.T) {
		f := &fakeSequentialStore{spends: []*Spend{{}, {Err: errors.ErrSpent}}, spendErr: errors.NewUtxoError("spend failed", errors.ErrSpent), leasesErr: errors.NewStorageError("read failed")}

		settled, err := settle(f, &meta.Data{Creating: true, Attempt: attempt})
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.False(t, settled)
		require.Zero(t, f.beginCalls)
	})

	t.Run("a rollback that its owner left open is finished", func(t *testing.T) {
		f := &fakeSequentialStore{}

		settled, err := settle(f, &meta.Data{Creating: true, RollingBack: true, Attempt: attempt})
		require.NoError(t, err)
		require.True(t, settled, "the rollback ended and left a hold")
		require.Equal(t, []string{"take", "leases", "unspend", "release", "delete"}, f.callSeq)
		require.Zero(t, f.leaseCalls, "a rolling back record is never rolled forward")

		reversed := &fakeSequentialStore{}

		settled, err = settle(reversed, &meta.Data{Creating: true, RollingBack: true, Reversed: true, Attempt: attempt})
		require.NoError(t, err)
		require.True(t, settled)
		require.Equal(t, []string{"delete"}, reversed.callSeq, "a reversed record only needs its rollback ended")

		busy := &fakeSequentialStore{takeDeclined: true}

		settled, err = settle(busy, &meta.Data{Creating: true, RollingBack: true, Attempt: attempt})
		require.NoError(t, err)
		require.False(t, settled, "a holder is at work on it")
		require.Equal(t, []string{"take"}, busy.callSeq)
	})
}
