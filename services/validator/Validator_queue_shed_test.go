package validator

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// unlockSpy wraps a real utxo store and counts SetLocked(..., false) calls so a
// test can assert whether a transaction was unlocked (its two-phase commit ran).
//
// It also counts Unspend calls and can be told to fail Delete, or to LIE about it —
// report success while leaving the record readable, which is exactly TxMetaCache's
// documented cache-only behaviour. Those two knobs are what exercise the shed
// unwind's failure arm and its verify-after-delete guard against an otherwise real
// sqlitememory store rather than a fully synthetic double.
type unlockSpy struct {
	utxostore.Store
	unlockCalls  int
	unspendCalls int

	// deleteErr, when set, is returned by Delete instead of delegating.
	deleteErr error

	// deleteIsCacheOnly models a decorator whose Delete reports success without
	// removing the underlying record.
	deleteIsCacheOnly bool

	// deleteErrAfterRemoving models the master-first cascade failing after the
	// master is gone: the record really is deleted, and DeleteComplete still
	// reports the failure of a later step. It is the state the residue arm of
	// unwindShed exists for.
	deleteErrAfterRemoving bool

	// deletedHash records the hash Delete was called with, so the verify knobs below
	// can be scoped to the unwind's own read-back and cannot perturb the unrelated
	// GetMeta calls earlier in validation.
	deletedHash *chainhash.Hash

	// verifyErr and verifyFailures make the first verifyFailures read-backs of
	// deletedHash fail with verifyErr; later ones delegate. verifyReadCalls counts the
	// read-backs actually attempted, which is how the bounded retry and the number of
	// verify ROUNDS are observed.
	verifyErr       error
	verifyFailures  int
	verifyReadCalls int

	// reappearAfterGoneReads models a concurrent submission of the same txid
	// recreating the record: once this many read-backs of deletedHash have reported
	// it gone, every later one reports it readable again. Deterministic, where
	// racing a real create would not be. goneReads is its running count.
	reappearAfterGoneReads int
	goneReads              int

	// verifyFailFromRead makes read-backs of deletedHash from this 1-based index
	// onward fail with verifyErr. It is the mirror of verifyFailures, which fails the
	// FIRST n: it is what lets the second verify round fail while the first succeeds.
	verifyFailFromRead int

	// unspendPartialErr, when set, makes Unspend delegate only the FIRST spend and
	// then return this error — a partial failure on a multi-input transaction.
	unspendPartialErr error

	// setLockedBlocks makes the two-phase-commit unlock park until the context it was
	// handed expires, modelling a wedged store on the post-acceptance bookkeeping path.
	// It returns ctx.Err() rather than an error of its own, because that is the error
	// the caller wraps and the one the assertions key on.
	setLockedBlocks bool
}

func (s *unlockSpy) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	if !value {
		s.unlockCalls++

		if s.setLockedBlocks {
			<-ctx.Done()

			return ctx.Err()
		}
	}

	return s.Store.SetLocked(ctx, txHashes, value)
}

func (s *unlockSpy) Delete(ctx context.Context, hash *chainhash.Hash) error {
	s.deletedHash = hash

	if s.deleteErr != nil {
		return s.deleteErr
	}

	if s.deleteIsCacheOnly {
		return nil
	}

	return s.Store.Delete(ctx, hash)
}

// DeleteComplete mirrors Delete's behaviour knobs. The shed unwind calls this
// (not Delete), so without this override the deleteErr / deleteIsCacheOnly /
// deletedHash knobs would go dead — the call would promote straight to the
// embedded real store and the failure-path and verify-after-delete tests would
// silently stop testing what they claim.
func (s *unlockSpy) DeleteComplete(ctx context.Context, hash *chainhash.Hash) error {
	s.deletedHash = hash

	if s.deleteErr != nil {
		if s.deleteErrAfterRemoving {
			// The master really goes first, and the cascade then fails on a later
			// step — the half-completed shape, not a delete that did nothing.
			if err := s.Store.DeleteComplete(ctx, hash); err != nil {
				return err
			}
		}

		return s.deleteErr
	}

	if s.deleteIsCacheOnly {
		return nil
	}

	return s.Store.DeleteComplete(ctx, hash)
}

func (s *unlockSpy) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	if s.deletedHash == nil || !hash.IsEqual(s.deletedHash) {
		return s.Store.GetMeta(ctx, hash, data)
	}

	// Scoped to the unwind's own read-backs: deletedHash is only set once Delete has
	// run, so the ordinary GetMeta calls earlier in validation cannot be perturbed.
	s.verifyReadCalls++

	if s.verifyErr != nil && s.verifyReadCalls <= s.verifyFailures {
		return s.verifyErr
	}

	if s.verifyErr != nil && s.verifyFailFromRead > 0 && s.verifyReadCalls >= s.verifyFailFromRead {
		return s.verifyErr
	}

	err := s.Store.GetMeta(ctx, hash, data)

	if s.reappearAfterGoneReads > 0 && errors.Is(err, errors.ErrTxNotFound) {
		if s.goneReads >= s.reappearAfterGoneReads {
			// Readable again: another submission owns this txid now.
			return nil
		}

		s.goneReads++
	}

	return err
}

func (s *unlockSpy) Unspend(ctx context.Context, spends []*utxostore.Spend, flagAsLocked ...bool) error {
	s.unspendCalls++

	if s.unspendPartialErr != nil && len(spends) > 1 {
		// Restore the first input, then fail: the operator-visible state is a spend set
		// that is half undone, which is what the recovery log has to describe.
		if err := s.Store.Unspend(ctx, spends[:1], flagAsLocked...); err != nil {
			return err
		}

		return s.unspendPartialErr
	}

	return s.Store.Unspend(ctx, spends, flagAsLocked...)
}

// capturingLogger records Errorf and Warnf lines so a test can assert that a failure
// arm actually told an operator which outpoints to reconcile. A counter proves an arm
// was taken; only the log proves it is actionable.
type capturingLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	lines []string
}

func (l *capturingLogger) Errorf(format string, args ...interface{}) {
	l.record(format, args...)
	l.TestLogger.Errorf(format, args...)
}

func (l *capturingLogger) Warnf(format string, args ...interface{}) {
	l.record(format, args...)
	l.TestLogger.Warnf(format, args...)
}

func (l *capturingLogger) Infof(format string, args ...interface{}) {
	l.record(format, args...)
	l.TestLogger.Infof(format, args...)
}

func (l *capturingLogger) record(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) joined() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return strings.Join(l.lines, "\n")
}

// outpointOf renders a tx:vout the way unwindOutpoints does, for log assertions.
func outpointOf(tx *bt.Tx, vout uint32) string {
	return fmt.Sprintf("%s:%d", tx.TxIDChainHash().String(), vout)
}

// shrinkHandoffFloor scales the per-attempt hand-off deadline down for tests, so a
// short retry window still leaves room to retry. handoffFloor is
// blockassembly_queueFullWaitTimeout plus handoffRoundTripSlack, and at the shipped
// defaults that is 600ms — larger than the sub-second windows these tests configure,
// which would leave zero room for retries.
//
// Returns the resulting floor so a test can reason about it explicitly rather than
// re-deriving it.
//
// The slack is set on this validator's own settings rather than on package state, so
// concurrently running tests cannot observe each other's shrink.
func shrinkHandoffFloor(t *testing.T, v *Validator, queueWait, slack time.Duration) time.Duration {
	t.Helper()

	v.settings.Validator.HandoffRoundTripSlack = slack
	v.settings.BlockAssembly.QueueFullWaitTimeout = queueWait

	return v.handoffFloor()
}

// shrinkTwoPhaseCommitTimeout scales the 2PC unlock bound down for tests, so a wedged
// SetLocked surfaces in milliseconds rather than after the shipped 2s.
//
// Unlike the handoff slack this bound is package state, not a setting — it is
// post-acceptance bookkeeping and appears in no operator-facing arithmetic — so the
// helper has to restore it.
func shrinkTwoPhaseCommitTimeout(t *testing.T, d time.Duration) {
	t.Helper()

	original := twoPhaseCommitTimeout
	twoPhaseCommitTimeout = d

	t.Cleanup(func() { twoPhaseCommitTimeout = original })
}

// parentOutpointSpent reports whether output 0 of parentTx reads as spent — the
// observable that distinguishes a shed unwind's Unspend from a no-op.
func parentOutpointSpent(t *testing.T, store utxostore.Store, parentTx *bt.Tx) bool {
	t.Helper()

	return outpointSpent(t, store, parentTx, 0)
}

// outpointSpent reports whether a specific output of tx reads as spent — needed on the
// multi-input shape, where a partial unwind leaves the two outputs in different states.
func outpointSpent(t *testing.T, store utxostore.Store, tx *bt.Tx, vout uint32) bool {
	t.Helper()

	utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[vout], vout)
	require.NoError(t, err)

	resp, err := store.GetSpend(context.Background(), &utxostore.Spend{
		TxID:     tx.TxIDChainHash(),
		Vout:     vout,
		UTXOHash: utxoHash,
	})
	require.NoError(t, err)

	return resp.Status == int(utxostore.Status_SPENT)
}

// requireTxAbsent asserts the transaction has no record left in the store.
func requireTxAbsent(t *testing.T, store utxostore.Store, hash *chainhash.Hash) {
	t.Helper()

	require.ErrorIs(t, store.GetMeta(context.Background(), hash, &meta.Data{}), errors.ErrTxNotFound,
		"a shed transaction must leave no record behind")
}

// recoveryBAStore is a controllable blockassembly.Store. It records how many
// times Store was called, sheds (ErrThresholdExceeded) the first shedFirst
// calls to model a queue that is full then drains, and otherwise returns a
// configurable error.
// blockUntilCtxDone models a well-behaved gRPC client against a WEDGED server: it
// blocks until the context it was handed expires, which is what the hand-off deadline
// exists to bound. blockAfter delays that behaviour until after N prompt sheds, so a
// full retry window followed by one stalled final send can be exercised. shedAfter
// stands in for the remote's own bounded queue wait answering late — the split-
// deployment skew shape — by sleeping before returning the shed.
type recoveryBAStore struct {
	err       error
	calls     int
	shedFirst int

	blockUntilCtxDone bool
	blockAfter        int
	shedAfter         time.Duration

	// abandonErr stands in for the error the REAL batched block-assembly client
	// returns when the caller's deadline ends the wait: a ServiceError wrapping
	// context.DeadlineExceeded, returned promptly rather than at the deadline,
	// because in batch mode the wait is abandoned while the item may still be
	// dispatched. It exists so the fake can return exactly what the shipped client
	// returns, instead of the bare ctx.Err() a ctx-honouring fake produces.
	abandonErr error

	// cancelCaller, when set, is invoked at the moment of the hand-off — the point a
	// saturated node makes a client give up — and the context this Store was handed is
	// then checked. That makes "the hand-off runs on a context the caller cannot
	// cancel" directly observable: with the detached context it reports the shed, and
	// without it it reports the cancellation instead, which is what used to skip the
	// unwind entirely.
	cancelCaller func()
}

var _ blockassembly.Store = (*recoveryBAStore)(nil)

func (f *recoveryBAStore) Store(ctx context.Context, _ *chainhash.Hash, _, _ uint64, _ subtree.TxInpoints) (bool, error) {
	f.calls++

	if f.cancelCaller != nil {
		f.cancelCaller()

		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		return false, errors.NewThresholdExceededError("block assembly queue full")
	}

	if f.blockUntilCtxDone && f.calls > f.blockAfter {
		<-ctx.Done()

		return false, ctx.Err()
	}

	if f.shedAfter > 0 {
		timer := time.NewTimer(f.shedAfter)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timer.C:
		}

		return false, errors.NewThresholdExceededError("block assembly queue full")
	}

	if f.calls <= f.shedFirst {
		return false, errors.NewThresholdExceededError("block assembly queue full")
	}

	if f.abandonErr != nil {
		return false, f.abandonErr
	}

	if f.err != nil {
		return false, f.err
	}

	return true, nil
}

func (f *recoveryBAStore) RemoveTx(_ context.Context, _ *chainhash.Hash) error { return nil }

// recoverySetup builds a validator backed by a real sqlitememory store (wrapped
// in unlockSpy) and a controllable block-assembly store, plus a confirmed parent
// tx and a child spending it. The child is not yet stored, so a caller decides
// whether to pre-create it (to exercise the ErrTxExists gate) or to validate it
// fresh. The parent is returned so a caller can assert on its outpoint's spent
// state, which is how a shed unwind's Unspend is observed.
func recoverySetup(t *testing.T, dbName string) (*Validator, *unlockSpy, *recoveryBAStore, *sql.Store, *bt.Tx, *bt.Tx) {
	t.Helper()

	v, spy, baStore, realStore, childTx, parentTx, _ := recoverySetupWithLogger(t, dbName, 1)

	return v, spy, baStore, realStore, childTx, parentTx
}

// recoverySetupMultiInput is recoverySetup with a parent carrying TWO spendable
// outputs and a child spending both, so a partial Unspend failure is expressible.
func recoverySetupMultiInput(t *testing.T, dbName string) (*Validator, *unlockSpy, *recoveryBAStore, *sql.Store, *bt.Tx, *bt.Tx, *capturingLogger) {
	t.Helper()

	return recoverySetupWithLogger(t, dbName, 2)
}

// recoverySetupWithLogger is the full builder: inputs controls how many of the
// parent's outputs the child spends, and the returned capturingLogger makes the
// unwind's recovery log lines assertable.
func recoverySetupWithLogger(t *testing.T, dbName string, inputs int) (*Validator, *unlockSpy, *recoveryBAStore, *sql.Store, *bt.Tx, *bt.Tx, *capturingLogger) {
	t.Helper()

	ctx := context.Background()
	logger := &capturingLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	u, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)

	realStore, err := sql.New(ctx, logger, tSettings, u)
	require.NoError(t, err)

	realStore.RawDB().SetMaxOpenConns(1)
	require.NoError(t, realStore.SetBlockHeight(100))
	require.NoError(t, realStore.SetMedianBlockTime(1_700_000_000))

	spy := &unlockSpy{Store: realStore}

	vi, err := New(ctx, logger, tSettings, spy, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	v, ok := vi.(*Validator)
	require.True(t, ok)

	baStore := &recoveryBAStore{}
	v.blockAssembler = baStore

	priv, pub := bec.PrivateKeyFromBytes([]byte("QUEUE_SHED_RECOVERY_TESTKEY_1234"))

	parentTx := transactions.Create(t,
		transactions.WithCoinbaseData(1, "/queue shed recovery test/"),
		transactions.WithP2PKHOutputs(inputs, 100_000, pub),
	)

	_, err = realStore.Create(ctx, parentTx, 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 1, BlockHeight: 1, SubtreeIdx: 0}))
	require.NoError(t, err)

	childOpts := []transactions.TxOption{transactions.WithPrivateKey(priv)}
	for vout := 0; vout < inputs; vout++ {
		childOpts = append(childOpts, transactions.WithInput(parentTx, uint32(vout), priv)) // nolint:gosec
	}

	childOpts = append(childOpts, transactions.WithP2PKHOutputs(1, 90_000, pub))

	childTx := transactions.Create(t, childOpts...)

	return v, spy, baStore, realStore, childTx, parentTx, logger
}

func metaLocked(t *testing.T, store utxostore.Store, hash *chainhash.Hash) *meta.Data {
	t.Helper()

	md := &meta.Data{}
	require.NoError(t, store.GetMeta(context.Background(), hash, md))

	return md
}

// Test 11 — a shed unwinds its own work. With WaitForBlockAssembly unset, a
// block-assembly send that fails with ThresholdExceeded surfaces the shed AND
// undoes this call's store work: the record is deleted and the parent's output is
// unspent, so nothing is left in the store for a resubmit to trip over or for the
// unmined reload to lift into a template. The 2PC unlock must NOT run — we delete
// the record, we do not unlock it.
func TestValidate_ShedUnwindsItsWork(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_unwind")

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	initPrometheusMetrics()

	unwindsBefore := testutil.ToFloat64(prometheusValidatorShedUnwindTotal)
	droppedBefore := testutil.ToFloat64(prometheusValidatorShedDroppedTotal)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.Error(t, err, "a shed on the send path must surface as an error")
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "the caller still sees the shed")

	require.GreaterOrEqual(t, baStore.calls, 1, "the send was attempted")
	require.Equal(t, 0, spy.unlockCalls, "the unwind deletes the record; it must not unlock it")

	requireTxAbsent(t, realStore, childTx.TxIDChainHash())
	require.False(t, parentOutpointSpent(t, realStore, parentTx), "the unwind unspends the parent's output")
	require.Equal(t, 1, spy.unspendCalls, "the unwind unspends exactly once")

	require.Equal(t, unwindsBefore+1, testutil.ToFloat64(prometheusValidatorShedUnwindTotal))
	require.Equal(t, droppedBefore, testutil.ToFloat64(prometheusValidatorShedDroppedTotal),
		"the drop counter is for the SILENT ingest drop; a synchronous caller got its retryable status")
}

// Test 11h — the shed unwind must survive a caller that has already given up.
//
// The unwind ran on the context returned by DecoupleTracingSpan, whose documented fast
// path for tracing disabled returns the caller's context UNCHANGED — and tracing is
// disabled by default. So in the default configuration a cancelled caller took the
// unwind down with it, in two ways: sendToBlockAssembler returned context.Canceled
// rather than ErrThresholdExceeded, so the shed branch (and therefore the unwind) was
// never entered at all; or the cancellation landed during the unwind and Delete failed.
// Both left the transaction spent, created and Locked — the state the unwind exists to
// eliminate. The two conditions are correlated: a shed happens when block assembly is
// saturated, which is exactly when clients time out.
//
// The synchronous path is the shape under test: no retry loop, no ctx.Done() guard.
// Default settings, so tracing is disabled — the configuration the fix has to hold in.
func TestValidate_SyncShedUnwindsWithCancelledCaller(t *testing.T) {
	v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_sync_cancelled")

	require.False(t, v.settings.TracingEnabled, "precondition: this must exercise the tracing-disabled default")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The client gives up exactly at the hand-off, which is when it really happens: a
	// shed only occurs while block assembly is saturated, and that is precisely when
	// clients time out. Cancelling before Validate instead would abort in the
	// input-height lookup and never reach the code under test.
	baStore.cancelCaller = cancel

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))

	// The classification survives, which is what gets the unwind entered at all. On the
	// unfixed code the hand-off saw the caller's cancelled context and returned
	// context.Canceled, so control took the else branch and unwindShed was never called.
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "a cancelled caller must still see the shed, not context.Canceled")
	require.NotErrorIs(t, err, context.Canceled)

	// And the unwind itself ran to completion on a context the caller cannot cancel.
	requireTxAbsent(t, realStore, childTx.TxIDChainHash())
	require.False(t, parentOutpointSpent(t, realStore, parentTx), "the unwind unspent the parent's output despite the cancelled caller")
	require.Equal(t, 1, spy.unspendCalls)
	require.Equal(t, 0, spy.unlockCalls, "a shed never unlocks; it deletes")
}

// Test 11a — the resubmit arm. After a shed has unwound, a resubmit is an ORDINARY
// FIRST SUBMISSION, not an already-exists success: once the queue drains it spends,
// creates, hands off exactly once and unlocks, ending unlocked and unmined. This is
// the coverage the earlier resubmit-recovery test provided and that its removal
// lost; it is what makes the "a shed is a clean rejection" design observable
// end-to-end through the real gRPC production path.
func TestValidateTransactionGRPC_ResubmitAfterShedIsFirstSubmission(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_resubmit")

	producer := kafka.NewKafkaAsyncProducerMock()
	v.txmetaKafkaProducerClient = producer

	initPrometheusMetrics()

	srv := &Server{
		logger:    &ulogger.TestLogger{},
		validator: v,
		settings:  test.CreateBaseTestSettings(t),
	}

	skipPolicy := true
	req := &validator_api.ValidateTransactionRequest{
		TransactionData:  childTx.ExtendedBytes(),
		BlockHeight:      100,
		SkipPolicyChecks: &skipPolicy,
	}

	// First submission: the queue is full, so it is shed and unwound.
	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	resp, err := srv.ValidateTransaction(ctx, req)
	require.Error(t, err)
	require.NotNil(t, resp)
	require.False(t, resp.Valid)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "a shed surfaces as ResourceExhausted, telling the client to retry")

	requireTxAbsent(t, realStore, childTx.TxIDChainHash())
	require.False(t, parentOutpointSpent(t, realStore, parentTx), "the shed left the parent's output spendable")
	require.Equal(t, 0, spy.unlockCalls)

	// The queue drains; the resubmit is an ordinary first submission.
	baStore.err = nil
	callsAfterShed := baStore.calls

	resp, err = srv.ValidateTransaction(ctx, req)
	require.NoError(t, err, "the resubmit succeeds for real, not by way of an already-exists shortcut")
	require.NotNil(t, resp)
	require.True(t, resp.Valid)

	require.Equal(t, callsAfterShed+1, baStore.calls, "exactly one block-assembly handoff on the resubmit")
	require.Equal(t, 1, spy.unlockCalls, "the 2PC unlock runs exactly once, on the successful submission")

	md := metaLocked(t, realStore, childTx.TxIDChainHash())
	require.False(t, md.Locked, "the accepted tx ends unlocked")
	require.Empty(t, md.BlockIDs, "the accepted tx is unmined until it is actually mined")
	require.True(t, parentOutpointSpent(t, realStore, parentTx), "the accepted tx spends the parent's output")
}

// Test 11b — the unwind is best-effort and never changes the answer. When Delete
// fails, the caller still receives the shed, the transaction is left Locked (the
// unmined-reload backstop is intact), the inputs are NOT unspent — the ordering
// invariant: never free the inputs of a record that still exists — and the failure
// metric moves.
func TestValidate_ShedUnwindFailureLeavesTxLockedAndStillSheds(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_unwind_fail")

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")
	spy.deleteErr = errors.NewStorageError("utxo store unavailable")

	initPrometheusMetrics()

	failuresBefore := testutil.ToFloat64(prometheusValidatorShedUnwindFailures)
	abortedBefore := testutil.ToFloat64(prometheusValidatorShedUnwindAborted)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "an unwind failure must not change the error the caller sees")

	require.Equal(t, failuresBefore+1, testutil.ToFloat64(prometheusValidatorShedUnwindFailures))
	require.Equal(t, abortedBefore, testutil.ToFloat64(prometheusValidatorShedUnwindAborted),
		"a delete that reported failure is not the same condition as a store that did not honour a successful delete")

	require.Equal(t, 0, spy.unspendCalls, "a failed Delete must not be followed by an Unspend")
	require.True(t, parentOutpointSpent(t, realStore, parentTx), "the parent's output stays spent while the record survives")
	require.True(t, metaLocked(t, realStore, childTx.TxIDChainHash()).Locked, "the tx is left locked for the unmined-reload backstop")
	require.Equal(t, 0, spy.unlockCalls)
}

// Test 11b2 — the residue arm: the cascade removed the master record and then failed
// on a later step. The record is provably gone, so the ordering invariant no longer
// forbids the unspend — nothing survives that a competing double-spend could collide
// with, and nothing survives that can be lifted into a mining template. Under the old
// master-last order this same failure left the master alive and the inputs burnt; the
// point of the change is that the inputs come back.
//
// The residue itself (orphan pagination children, an external blob) is a store-level
// shape and is pinned at that level; what this test pins is the decision the validator
// takes on it, and that the log line says what was left behind.
func TestValidate_ShedUnwindUnspendsWhenDeleteFailedButRecordIsGone(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx, logger := recoverySetupWithLogger(t, "queue_shed_unwind_residue", 1)

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")
	spy.deleteErr = errors.NewStorageError("pagination child delete failed after the master record was removed")
	spy.deleteErrAfterRemoving = true

	initPrometheusMetrics()

	residueBefore := testutil.ToFloat64(prometheusValidatorShedUnwindResidue)
	failuresBefore := testutil.ToFloat64(prometheusValidatorShedUnwindFailures)
	abortedBefore := testutil.ToFloat64(prometheusValidatorShedUnwindAborted)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "the caller still sees the shed")

	require.Equal(t, 1, spy.unspendCalls, "a delete that is provably gone must be followed by the unspend; this is the burnt-inputs fix")
	require.False(t, parentOutpointSpent(t, realStore, parentTx), "the inputs are returned rather than burnt")
	requireTxAbsent(t, realStore, childTx.TxIDChainHash())
	require.Equal(t, 0, spy.unlockCalls, "a shed never unlocks; it deletes")

	require.Equal(t, residueBefore+1, testutil.ToFloat64(prometheusValidatorShedUnwindResidue),
		"the residue outcome is counted distinctly from a delete that left the record intact")
	require.Equal(t, failuresBefore+1, testutil.ToFloat64(prometheusValidatorShedUnwindFailures),
		"the delete still errored, so the 'something failed at all' counter moves too")
	require.Equal(t, abortedBefore, testutil.ToFloat64(prometheusValidatorShedUnwindAborted),
		"a delete that reported failure is not a store that did not honour a successful delete")

	logged := logger.joined()
	require.Contains(t, logged, outpointOf(parentTx, 0), "the residue arm names the outpoints it unspent")
	require.Contains(t, logged, "orphan pagination children", "an arm is only actionable if it says what is left behind")
}

// Test 11c — the verify-after-delete guard. A store whose Delete returns nil while
// leaving the record readable (exactly TxMetaCache's documented cache-only
// behaviour) must NOT get its inputs unspent: that is the one intermediate state
// the ordering was chosen to avoid, because it frees the inputs of a record that
// still exists and can still be lifted into a mining template. The unwind aborts,
// the abort is counted distinctly from a failure, and the caller still sees the
// shed.
func TestValidate_ShedUnwindAbortsWhenRecordSurvivesDelete(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_unwind_abort")

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")
	spy.deleteIsCacheOnly = true

	initPrometheusMetrics()

	abortedBefore := testutil.ToFloat64(prometheusValidatorShedUnwindAborted)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "the caller still sees the shed")

	require.Equal(t, abortedBefore+1, testutil.ToFloat64(prometheusValidatorShedUnwindAborted),
		"a store that reports a delete it did not perform must be visible as an abort, not a failure")

	require.Equal(t, 0, spy.unspendCalls, "Unspend must never run when the record survived the delete")
	require.True(t, parentOutpointSpent(t, realStore, parentTx), "the inputs stay spent, so no competing spend can take them")
	require.True(t, metaLocked(t, realStore, childTx.TxIDChainHash()).Locked, "the tx is left exactly as the shed found it")
	require.Equal(t, 0, spy.unlockCalls)
}

// Test 11d — the OTHER arm of the verify-after-delete guard: Delete succeeded but the
// read-back kept failing, so deletion could not be CONFIRMED.
//
// It fails closed, and the choice is deliberate. A failed read does not establish that
// the record survived — but it does not establish that it is gone either, and the two
// candidate outcomes are asymmetric: unspending on an unconfirmed delete risks freeing
// the inputs of a surviving record, which a competing double-spend can then take while
// the survivor is still liftable into a mining template (consensus-visible), whereas
// leaving them spent makes specific outpoints temporarily unspendable (node-local,
// operator-recoverable from the log line asserted here).
//
// The read is retried first, so a transient store error does not reach this arm at all;
// see the companion test below.
func TestValidate_ShedUnwindFailsClosedWhenVerifyReadFails(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx, logger := recoverySetupWithLogger(t, "queue_shed_unwind_unverified", 1)

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	// Fail every read-back, so the bounded retry is exhausted.
	spy.verifyErr = errors.NewStorageError("utxo store read unavailable")
	spy.verifyFailures = shedUnwindVerifyAttempts + 5

	initPrometheusMetrics()

	unverifiedBefore := testutil.ToFloat64(prometheusValidatorShedUnwindUnverified)
	abortedBefore := testutil.ToFloat64(prometheusValidatorShedUnwindAborted)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "the caller still sees the shed")

	require.Equal(t, shedUnwindVerifyAttempts, spy.verifyReadCalls,
		"the verify read is retried a bounded number of times, no more and no fewer")

	require.Equal(t, unverifiedBefore+1, testutil.ToFloat64(prometheusValidatorShedUnwindUnverified),
		"an unconfirmable deletion is counted distinctly so an operator can reconcile it")
	require.Equal(t, abortedBefore, testutil.ToFloat64(prometheusValidatorShedUnwindAborted),
		"a failed read is not the same condition as a store that did not honour Delete")

	require.Equal(t, 0, spy.unspendCalls, "fail closed: no Unspend on an unconfirmed delete")
	require.True(t, parentOutpointSpent(t, realStore, parentTx), "the inputs stay spent")
	require.Equal(t, 0, spy.unlockCalls)

	require.Contains(t, logger.joined(), outpointOf(parentTx, 0),
		"the inconclusive arm must name the outpoints an operator has to reconcile")
}

// Test 11e — the bounded retry earns its keep: a TRANSIENT verify-read failure does
// NOT reach the fail-closed arm. Without the retry, any single blip on the read-back
// left a transaction's inputs spent for no reason.
func TestValidate_ShedUnwindRetriesTransientVerifyRead(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_unwind_verify_retry")

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	// One failure, then the real store answers (ErrTxNotFound, since Delete ran).
	spy.verifyErr = errors.NewStorageError("transient utxo store read failure")
	spy.verifyFailures = 1

	initPrometheusMetrics()

	unverifiedBefore := testutil.ToFloat64(prometheusValidatorShedUnwindUnverified)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.ErrorIs(t, err, errors.ErrThresholdExceeded)

	require.Equal(t, 3, spy.verifyReadCalls,
		"round one: one failed read then the retry that answered; round two: the pre-unspend re-check")
	require.Equal(t, unverifiedBefore, testutil.ToFloat64(prometheusValidatorShedUnwindUnverified),
		"a transient read failure absorbed by the retry must not be reported as unverified")

	require.Equal(t, 1, spy.unspendCalls, "the unwind completed")
	requireTxAbsent(t, realStore, childTx.TxIDChainHash())
	require.False(t, parentOutpointSpent(t, realStore, parentTx), "the inputs were freed as intended")
}

// Test 11f — the Delete-failure arm names the outpoints too. The method's godoc claims
// failure "is logged with the txid and the outpoints"; before this, only the
// Unspend-failure arm actually did.
func TestValidate_ShedUnwindDeleteFailureLogsOutpoints(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx, logger := recoverySetupWithLogger(t, "queue_shed_unwind_delete_log", 1)

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")
	spy.deleteErr = errors.NewStorageError("utxo store unavailable")

	initPrometheusMetrics()

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.ErrorIs(t, err, errors.ErrThresholdExceeded)

	require.Equal(t, 0, spy.unspendCalls)
	require.True(t, parentOutpointSpent(t, realStore, parentTx))

	require.Contains(t, logger.joined(), outpointOf(parentTx, 0),
		"a failed Delete must log the outpoints it is deliberately leaving spent")
}

// Test 11g — a PARTIAL Unspend failure on a multi-input transaction. The store restored
// one input and then failed, so the spend set is half undone. The recovery log must
// name EVERY outpoint in the set, not just the residue: an operator reconciling this
// has to check both, and cannot know from the log which half succeeded.
func TestValidate_ShedUnwindPartialUnspendLogsAllOutpoints(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx, logger := recoverySetupMultiInput(t, "queue_shed_unwind_partial")

	require.Len(t, childTx.Inputs, 2, "precondition: the child spends two of the parent's outputs")

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")
	spy.unspendPartialErr = errors.NewStorageError("utxo store unavailable mid-unspend")

	initPrometheusMetrics()

	failuresBefore := testutil.ToFloat64(prometheusValidatorShedUnwindFailures)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "a partial unwind must not change the error the caller sees")

	require.Equal(t, failuresBefore+1, testutil.ToFloat64(prometheusValidatorShedUnwindFailures))
	require.Equal(t, 1, spy.unspendCalls)
	require.Equal(t, 0, spy.unlockCalls)

	// The partial state is real and observable: one output restored, one still spent.
	require.False(t, outpointSpent(t, realStore, parentTx, 0), "the first input was restored before the failure")
	require.True(t, outpointSpent(t, realStore, parentTx, 1), "the second input is still spent")

	logged := logger.joined()
	require.Contains(t, logged, outpointOf(parentTx, 0), "the recovery log names the restored outpoint")
	require.Contains(t, logged, outpointOf(parentTx, 1), "the recovery log names the still-spent outpoint")
}

// TestUnwindOutpoints covers the renderer four log lines now depend on, including the
// defensive skips: a nil spend or a nil TxID must not panic on a recovery path.
func TestUnwindOutpoints(t *testing.T) {
	hashA := (&chainhash.Hash{1}).String()
	hashB := (&chainhash.Hash{2}).String()

	spendA := &utxostore.Spend{TxID: &chainhash.Hash{1}, Vout: 0}
	spendB := &utxostore.Spend{TxID: &chainhash.Hash{2}, Vout: 7}

	tests := []struct {
		name   string
		spends []*utxostore.Spend
		want   string
	}{
		{"empty", nil, ""},
		{"single", []*utxostore.Spend{spendA}, hashA + ":0"},
		{"order preserved", []*utxostore.Spend{spendA, spendB}, hashA + ":0," + hashB + ":7"},
		{"nil spend skipped", []*utxostore.Spend{nil, spendB}, hashB + ":7"},
		{"nil txid skipped", []*utxostore.Spend{{TxID: nil, Vout: 3}, spendB}, hashB + ":7"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, unwindOutpoints(tc.spends))
		})
	}
}

// Test 12 — how a resubmit of an EXISTING transaction is handled. An ordinary
// duplicate (existing, not locked), locked-but-mined and locked-but-conflicting are
// all legitimate idempotent duplicates: they return success and never re-drive the
// handoff or mutate lock state. The fourth case — locked, unmined and NOT
// conflicting — is the residual stranded state; after the shed unwind it can only
// come from an interrupted submission or an in-flight conflict resolution, which
// are indistinguishable in the store and both recovered by the unmined reload, so
// it too returns success but is now counted and logged rather than passing
// unnoticed.
func TestValidate_ExistingTxHandling(t *testing.T) {
	ctx := context.Background()

	t.Run("ordinary duplicate is not resent", func(t *testing.T) {
		v, spy, baStore, realStore, childTx, _ := recoverySetup(t, "queue_shed_dup")

		// Create the child already unlocked, as a normally-accepted tx would be
		// after its own two-phase commit.
		_, err := realStore.Create(ctx, childTx, 100)
		require.NoError(t, err)

		_, err = v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
		require.NoError(t, err, "an idempotent duplicate returns success")
		require.Equal(t, 0, baStore.calls, "an unlocked duplicate must not be resent to block assembly")
		require.Equal(t, 0, spy.unlockCalls)
	})

	t.Run("locked but mined is not resent", func(t *testing.T) {
		v, _, baStore, realStore, childTx, _ := recoverySetup(t, "queue_shed_mined")

		_, err := realStore.Create(ctx, childTx, 100,
			utxostore.WithLocked(true),
			utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 2, BlockHeight: 2, SubtreeIdx: 0}))
		require.NoError(t, err)

		require.NotEmpty(t, metaLocked(t, realStore, childTx.TxIDChainHash()).BlockIDs, "precondition: tx is mined")

		_, err = v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
		require.NoError(t, err)
		require.Equal(t, 0, baStore.calls, "a mined transaction must not be resent")
	})

	t.Run("locked but conflicting is not resent", func(t *testing.T) {
		v, _, baStore, realStore, childTx, _ := recoverySetup(t, "queue_shed_conflicting")

		_, err := realStore.Create(ctx, childTx, 100,
			utxostore.WithLocked(true),
			utxostore.WithConflicting(true))
		require.NoError(t, err)

		require.True(t, metaLocked(t, realStore, childTx.TxIDChainHash()).Conflicting, "precondition: tx is conflicting")

		_, err = v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
		require.NoError(t, err)
		require.Equal(t, 0, baStore.calls, "a conflicting transaction must not be resent")
	})

	t.Run("locked, unmined and not conflicting is still success, but observed", func(t *testing.T) {
		v, spy, baStore, realStore, childTx, _ := recoverySetup(t, "queue_shed_locked_unmined")

		_, err := realStore.Create(ctx, childTx, 100, utxostore.WithLocked(true))
		require.NoError(t, err)

		md := metaLocked(t, realStore, childTx.TxIDChainHash())
		require.True(t, md.Locked, "precondition: tx is locked")
		require.Empty(t, md.BlockIDs, "precondition: tx is unmined")
		require.False(t, md.Conflicting, "precondition: tx is not conflicting")

		initPrometheusMetrics()

		observedBefore := testutil.ToFloat64(prometheusValidatorExistingTxLockedUnmined)

		_, err = v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
		require.NoError(t, err, "the contract is unchanged: the resubmit still returns success")
		require.Equal(t, 0, baStore.calls, "the resubmit must not re-drive the handoff")
		require.Equal(t, 0, spy.unlockCalls, "the resubmit must not mutate lock state")

		require.Equal(t, observedBefore+1, testutil.ToFloat64(prometheusValidatorExistingTxLockedUnmined),
			"the stranded state must be observable to an operator")
	})
}

// Test 13 — Kafka ingest path: the handoff is retried in place until it
// succeeds. WaitForBlockAssembly makes a queue-full shed retry rather than
// surface; once the queue drains, the send succeeds and the tx falls through to
// the txmeta publish and the 2PC unlock. Exactly one successful handoff, one
// txmeta message, one unlock — and the tx ends unlocked and unmined.
func TestValidate_KafkaIngestRetriesUntilHandoffSucceeds(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, _ := recoverySetup(t, "queue_shed_kafka_retry")

	producer := kafka.NewKafkaAsyncProducerMock()
	v.txmetaKafkaProducerClient = producer

	// The first send sheds (queue full); the retry then succeeds.
	baStore.shedFirst = 1

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true), WithWaitForBlockAssembly(true))
	require.NoError(t, err, "the Kafka ingest path retries the shed until the handoff succeeds")

	require.Equal(t, 2, baStore.calls, "one shed then one successful handoff")
	require.Equal(t, 1, spy.unlockCalls, "the 2PC unlock runs after a successful handoff")

	md := metaLocked(t, realStore, childTx.TxIDChainHash())
	require.False(t, md.Locked, "the tx ends unlocked")
	require.Empty(t, md.BlockIDs, "the tx remains unmined until it is actually mined")

	require.Len(t, producer.PublishChannel(), 1, "exactly one txmeta message is published on the successful handoff")
}

// Test 13a — the retry is BOUNDED. With the queue permanently full, the in-place
// retry gives up after BlockAssemblyShedRetryTimeout instead of parking the ingest
// goroutine (and the Kafka record batch it holds) for the whole stall, then falls
// through to the unwind. The transaction is dropped cleanly: no record, no held
// parent lock, no store residue — which is what makes the drop safe to describe as
// clean rather than lossless.
func TestValidate_KafkaIngestRetryTimesOutThenUnwinds(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_kafka_timeout")

	// The queue stays full forever.
	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	// A short bound keeps the test fast while still exercising the real deadline
	// arithmetic (several 5ms backoff iterations). The per-attempt floor is scaled down
	// with it, because the window is reduced by that floor and the shipped 600ms floor
	// would leave a 50ms window no room to retry at all.
	shrinkHandoffFloor(t, v, 5*time.Millisecond, 5*time.Millisecond)
	v.settings.Validator.BlockAssemblyShedRetryTimeout = 50 * time.Millisecond

	initPrometheusMetrics()

	droppedBefore := testutil.ToFloat64(prometheusValidatorShedDroppedTotal)

	start := time.Now()

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true), WithWaitForBlockAssembly(true))
	elapsed := time.Since(start)

	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "on timeout the shed is returned, not retried forever")
	require.Greater(t, baStore.calls, 1, "the handoff was retried before giving up")
	require.Less(t, elapsed, 5*time.Second, "the retry must be bounded by the configured window, not by the stall")

	require.Equal(t, droppedBefore+1, testutil.ToFloat64(prometheusValidatorShedDroppedTotal),
		"a transaction dropped on the ingest path is counted; its submitter was already told it was accepted")

	requireTxAbsent(t, realStore, childTx.TxIDChainHash())
	require.False(t, parentOutpointSpent(t, realStore, parentTx), "the timed-out shed unwinds like any other")
	require.Equal(t, 0, spy.unlockCalls)
}

// Test 14 — Kafka ingest path: a context cancel mid-retry aborts promptly and
// leaves the tx durably Locked and unmined (the precondition for the unmined
// reload on the next block-assembly start) — deliberately NOT unwound, because the
// redelivered Kafka record or the unmined reload needs the record to still be
// there. No 2PC unlock runs.
func TestValidate_KafkaIngestCtxCancelLeavesTxLocked(t *testing.T) {
	v, spy, baStore, realStore, childTx, _ := recoverySetup(t, "queue_shed_kafka_cancel")

	// The queue stays full forever; the retry must abort on ctx cancel.
	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel once validation has reached the retry loop.
	time.AfterFunc(100*time.Millisecond, cancel)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true), WithWaitForBlockAssembly(true))
	require.Error(t, err, "a cancelled retry surfaces as an error")

	require.GreaterOrEqual(t, baStore.calls, 1, "the send was attempted at least once")
	require.Equal(t, 0, spy.unlockCalls, "no 2PC unlock runs when the retry is cancelled")

	md := metaLocked(t, realStore, childTx.TxIDChainHash())
	require.True(t, md.Locked, "the tx is left locked for the unmined-reload backstop")
	require.Empty(t, md.BlockIDs, "the tx is left unmined")
}

// Test 14a — a STALLED handoff is bounded, and a stall costs exactly one attempt.
//
// Detaching the post-decision context (test 11h) removed the only thing that unblocked
// a stalled unary handoff, so the handoff carries a deadline of its own. Without it this
// test hangs until the Go test timeout, which is the regression it guards.
//
// The call-count assertion is the load-bearing one: a deadline error is not
// ErrThresholdExceeded, so the retry loop exits immediately and a stall can consume at
// most ONE handoff floor. That is what makes the total ingest stall a sum of two terms
// (retry window + one floor) rather than a product.
//
// Two shapes, because the two block-assembly client modes fail differently and the
// validator's disposition has to be identical for both. The first subtest models a
// ctx-honouring client blocking to the deadline (unary mode); the second returns the
// exact error the batched client now returns when it abandons a wait, which is the
// shipped default and is NOT a ctx.Err() of its own.
func TestValidate_StalledBlockAssemblyHandoffIsBounded(t *testing.T) {
	// requireAmbiguousHandoffDisposition asserts the four properties that define the
	// validator's answer to an ambiguous hand-off failure, whichever mode produced it.
	requireAmbiguousHandoffDisposition := func(t *testing.T, spy *unlockSpy, realStore *sql.Store, childTx, parentTx *bt.Tx, deadlinesBefore, droppedBefore float64) {
		t.Helper()

		// NOT unwound: the deadline can fire after block assembly enqueued the
		// transaction, so deleting the record could remove one already in a subtree or
		// template. Left Locked for the unmined reload.
		require.Equal(t, 0, spy.unspendCalls, "no unwind on an ambiguous handoff failure")
		require.True(t, metaLocked(t, realStore, childTx.TxIDChainHash()).Locked, "the tx is left locked for the unmined reload")
		require.True(t, parentOutpointSpent(t, realStore, parentTx), "its inputs stay spent while the record survives")
		require.Equal(t, 0, spy.unlockCalls)

		require.Equal(t, droppedBefore, testutil.ToFloat64(prometheusValidatorShedDroppedTotal),
			"a stall must not be counted as a clean shed drop")
		require.Equal(t, deadlinesBefore+1, testutil.ToFloat64(prometheusValidatorHandoffDeadlineTotal),
			"the handoff deadline is attributable to its own cause")
	}

	t.Run("a client that blocks to the deadline", func(t *testing.T) {
		ctx := context.Background()
		v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_handoff_stall")

		// A well-behaved gRPC client against a wedged server: blocks until its context
		// expires. No sleeps, no store I/O — the smallest possible blocking path, so
		// scheduling noise has minimal surface.
		baStore.blockUntilCtxDone = true

		floor := shrinkHandoffFloor(t, v, 20*time.Millisecond, 20*time.Millisecond)
		require.Equal(t, 40*time.Millisecond, floor)

		initPrometheusMetrics()

		droppedBefore := testutil.ToFloat64(prometheusValidatorShedDroppedTotal)
		deadlinesBefore := testutil.ToFloat64(prometheusValidatorHandoffDeadlineTotal)

		start := time.Now()

		_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true), WithWaitForBlockAssembly(true))
		elapsed := time.Since(start)

		// Hang detector, deliberately generous: the quantitative bound is carried by the
		// call count below and by TestHandoffFloorAndRetryWindow, not by wall clock.
		require.Less(t, elapsed, 5*time.Second, "a stalled handoff must be bounded by its own deadline")

		require.Error(t, err)
		require.NotErrorIs(t, err, errors.ErrThresholdExceeded, "a stall is not a shed")

		require.Equal(t, 1, baStore.calls,
			"a deadline error exits the retry loop, so a stall costs exactly one handoff floor and never compounds")

		requireAmbiguousHandoffDisposition(t, spy, realStore, childTx, parentTx, deadlinesBefore, droppedBefore)
	})

	// The fake above blocks on ctx.Done() and returns ctx.Err(), which is the unary
	// client's shape and has one property the batched client does not: the error IS a
	// context error, and it arrives exactly at the deadline. On its own it therefore
	// pins the bound against a client shape that is not the shipped default.
	//
	// In batch mode the client abandons the wait instead: it returns a ServiceError
	// WRAPPING context.DeadlineExceeded, promptly, and while the item may still be
	// dispatched. This subtest feeds the validator exactly that error and asserts the
	// SAME disposition, so the classification arm is pinned against both shapes.
	t.Run("the error shape the batched client returns", func(t *testing.T) {
		ctx := context.Background()
		v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_handoff_abandon")

		baStore.abandonErr = errors.NewServiceError("block assembly batch handoff abandoned before dispatch completed", context.DeadlineExceeded)

		initPrometheusMetrics()

		droppedBefore := testutil.ToFloat64(prometheusValidatorShedDroppedTotal)
		deadlinesBefore := testutil.ToFloat64(prometheusValidatorHandoffDeadlineTotal)

		_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true), WithWaitForBlockAssembly(true))

		require.Error(t, err)
		require.NotErrorIs(t, err, errors.ErrThresholdExceeded, "an abandoned batch handoff is not a shed")
		require.ErrorIs(t, err, context.DeadlineExceeded,
			"the deadline classification must survive the client's own wrapping, or the arm that leaves the tx locked never fires")

		require.Equal(t, 1, baStore.calls,
			"the abandonment is not ErrThresholdExceeded, so the retry loop is never entered")

		requireAmbiguousHandoffDisposition(t, spy, realStore, childTx, parentTx, deadlinesBefore, droppedBefore)
	})
}

// TestHandoffDeadlineClassificationSurvivesWrapping pins an otherwise invisible
// dependency: the hand-off-deadline arm in Validate reads a context error out of an
// error chain that has been wrapped two or three times before it gets there.
//
// It survives partly by substring match rather than by unwrapping — the errors package
// falls back to comparing message text when the target is not one of its own error
// types — so the classification depends on the chain's rendered text, not just on its
// structure. Nothing else in the suite would notice if a change to that rendering, or
// one more layer of wrapping, quietly broke it: the arm would simply stop firing, the
// counter would stop moving, and a transaction that should be left locked for the
// unmined reload would take a different path with no test failing.
func TestHandoffDeadlineClassificationSurvivesWrapping(t *testing.T) {
	t.Run("batch mode: the client abandons the wait", func(t *testing.T) {
		// Layer 1, what the batched block-assembly client returns.
		clientErr := errors.NewServiceError("block assembly batch handoff abandoned before dispatch completed", context.DeadlineExceeded)
		require.ErrorIs(t, clientErr, context.DeadlineExceeded)

		// Layer 2, sendToBlockAssembler's wrapping of a non-shed failure. This is the
		// value the classification arm actually reads.
		sendErr := errors.NewServiceError("error calling blockAssembler Store()", clientErr)
		require.ErrorIs(t, sendErr, context.DeadlineExceeded, "the arm that leaves the tx locked reads THIS error")

		// Layer 3, what the caller finally sees.
		require.ErrorIs(t, errors.NewProcessingError("[Validate][%s] error sending tx to block assembler", "txid", sendErr), context.DeadlineExceeded)
	})

	t.Run("unary mode: gRPC renders the deadline as a status message", func(t *testing.T) {
		// The unary path never carries context.DeadlineExceeded as a wrapped value at
		// all: gRPC returns a status error whose text is "context deadline exceeded",
		// and the substring fallback is the ONLY reason the arm fires. This is the case
		// that would break silently.
		unary := errors.UnwrapGRPC(status.Error(codes.DeadlineExceeded, "context deadline exceeded"))
		require.ErrorIs(t, unary, context.DeadlineExceeded)

		sendErr := errors.NewServiceError("error calling blockAssembler Store()", unary)
		require.ErrorIs(t, sendErr, context.DeadlineExceeded)

		require.ErrorIs(t, errors.NewProcessingError("[Validate][%s] error sending tx to block assembler", "txid", sendErr), context.DeadlineExceeded)
	})

	t.Run("a shed stays distinguishable through the same wrapping", func(t *testing.T) {
		// The other half of the classification: if a shed matched the deadline arm, a
		// transaction that should be unwound would be left locked instead.
		shed := errors.NewProcessingError("[Validate][%s] error sending tx to block assembler", "txid",
			errors.NewServiceError("error calling blockAssembler Store()", errors.NewThresholdExceededError("block assembly queue full")))

		require.ErrorIs(t, shed, errors.ErrThresholdExceeded)
		require.NotErrorIs(t, shed, context.DeadlineExceeded)
	})
}

// TestValidate_TwoPhaseCommitIsBounded pins the deadline on the 2PC unlock.
//
// The unlock runs on the post-decision context, which is detached from the caller by
// design. Nothing outside the store could therefore interrupt it: a wedged store parked
// the ingest goroutine — and, on the Kafka path, the record batch it holds —
// indefinitely, on work that is post-acceptance bookkeeping. Without the bound this
// test hangs to the Go test timeout, which is the regression it guards.
//
// The return shape is asserted exactly, because it is not the obvious one: Validate
// returns a NON-NIL txMetaData together with a NON-NIL error. The transaction really
// was accepted, spent and created; only the unlock failed, and the caller is
// deliberately told so. Recovery is the unlock that happens on the next block the
// transaction is mined into, not a success return here.
func TestValidate_TwoPhaseCommitIsBounded(t *testing.T) {
	const bound = 50 * time.Millisecond

	ctx := context.Background()
	v, spy, _, realStore, childTx, _ := recoverySetup(t, "queue_shed_2pc_bounded")

	shrinkTwoPhaseCommitTimeout(t, bound)

	// A wedged store on the unlock: it parks until the context it was handed expires,
	// which with the bound in place is the 2PC deadline and nothing else. The
	// block-assembly hand-off succeeds, so this is the only thing that can fail.
	spy.setLockedBlocks = true

	start := time.Now()

	txMetaData, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	elapsed := time.Since(start)

	// Hang detector, deliberately generous: the bound itself is the 50ms above, and what
	// matters here is that Validate returns at all rather than parking forever.
	require.Less(t, elapsed, 5*time.Second, "a wedged 2PC unlock must be bounded by its own deadline")

	require.Error(t, err, "the caller is told the commit failed even though the transaction was accepted")
	require.ErrorIs(t, err, context.DeadlineExceeded, "and told why: the unlock hit the 2PC bound")
	require.NotNil(t, txMetaData, "the transaction was accepted and created, so its metadata is still returned")

	require.Equal(t, 1, spy.unlockCalls, "the unlock was attempted exactly once")

	// The record is still Locked: that is precisely what the unlock failed to clear, and
	// what the next-block unlock has to recover.
	require.True(t, metaLocked(t, realStore, childTx.TxIDChainHash()).Locked,
		"a failed 2PC leaves the transaction locked for the next block it is mined into")
}

// Test 14b — the total ingest stall stays within the CONFIGURED retry window.
//
// The budget used to be created after the first handoff, so a stalled first attempt
// pinned the consumer goroutine and its fetched record batch for a whole handoff
// timeout before the window even started. It is now an absolute instant taken before
// the first attempt, reduced by the per-attempt floor, so the window plus one final
// in-flight attempt together stay inside it.
//
// Both directions are asserted: the ceiling, and that the retry actually ran. A test
// asserting only the ceiling would pass on a build with the retry accidentally
// disabled.
func TestValidate_KafkaIngestStallBoundedByConfiguredRetryWindow(t *testing.T) {
	const window = 300 * time.Millisecond

	t.Run("window exhausted by prompt sheds, then a stalled final send", func(t *testing.T) {
		ctx := context.Background()
		v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_window_sheds")

		shrinkHandoffFloor(t, v, 20*time.Millisecond, 20*time.Millisecond)
		v.settings.Validator.BlockAssemblyShedRetryTimeout = window

		// Sheds promptly, forever: the window is what ends the retry.
		baStore.err = errors.NewThresholdExceededError("block assembly queue full")

		start := time.Now()

		_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true), WithWaitForBlockAssembly(true))
		elapsed := time.Since(start)

		require.Less(t, elapsed, 5*time.Second, "hang detector; the bound itself is proved by the call count and the arithmetic test")
		require.ErrorIs(t, err, errors.ErrThresholdExceeded, "the window ran out on a shed, so a shed is what surfaces")
		require.Greater(t, baStore.calls, 1, "the retry ran; the bound must not be met by disabling it")

		requireTxAbsent(t, realStore, childTx.TxIDChainHash())
		require.False(t, parentOutpointSpent(t, realStore, parentTx), "a window-exhausted shed unwinds")
		require.Equal(t, 0, spy.unlockCalls)
	})

	t.Run("stall inside the window leaves the tx locked", func(t *testing.T) {
		ctx := context.Background()
		v, spy, baStore, realStore, childTx, _ := recoverySetup(t, "queue_shed_window_stall")

		shrinkHandoffFloor(t, v, 20*time.Millisecond, 20*time.Millisecond)
		v.settings.Validator.BlockAssemblyShedRetryTimeout = window

		// Two prompt sheds, then a stall well inside the window.
		baStore.err = errors.NewThresholdExceededError("block assembly queue full")
		baStore.blockUntilCtxDone = true
		baStore.blockAfter = 2

		start := time.Now()

		_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true), WithWaitForBlockAssembly(true))
		elapsed := time.Since(start)

		require.Less(t, elapsed, 5*time.Second)
		require.Error(t, err)
		require.NotErrorIs(t, err, errors.ErrThresholdExceeded, "the last word was the stall, not the shed")

		require.Equal(t, 3, baStore.calls, "two sheds then the stalled attempt that ended the loop")
		require.Equal(t, 0, spy.unspendCalls, "an ambiguous stall is not unwound")
		require.True(t, metaLocked(t, realStore, childTx.TxIDChainHash()).Locked)
	})
}

// Test 14c — a legitimately LONG configured queue wait still surfaces as a shed.
//
// Block assembly waits up to blockassembly_queueFullWaitTimeout before shedding and
// treats caller cancellation as explicitly NOT a shed, so a per-attempt deadline
// shorter than that wait converts every shed into a local context deadline: the
// resource-exhausted classification is lost, the unwind never runs, and the transaction
// is stranded locked. That is why the deadline is DERIVED from the setting rather than
// picked. Here the configured queue wait (200ms) deliberately exceeds the retry window
// (50ms), which is the incoherent configuration: the shed must still win.
func TestValidate_LongQueueFullWaitStillClassifiedAsShed(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_long_queue_wait")

	floor := shrinkHandoffFloor(t, v, 200*time.Millisecond, 20*time.Millisecond)
	require.Equal(t, 220*time.Millisecond, floor)

	v.settings.Validator.BlockAssemblyShedRetryTimeout = 50 * time.Millisecond
	require.Less(t, v.shedRetryTimeout(), floor, "precondition: the window cannot accommodate one attempt")

	// The remote's own bounded wait runs almost to completion before answering: longer
	// than the retry window, but inside the floor.
	baStore.shedAfter = 150 * time.Millisecond

	initPrometheusMetrics()

	droppedBefore := testutil.ToFloat64(prometheusValidatorShedDroppedTotal)
	deadlinesBefore := testutil.ToFloat64(prometheusValidatorHandoffDeadlineTotal)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true), WithWaitForBlockAssembly(true))

	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "a late shed must arrive AS a shed, not as our own deadline")
	require.NotErrorIs(t, err, context.DeadlineExceeded)

	require.Equal(t, 1, baStore.calls, "with the window below the floor there is exactly one bounded attempt")

	// The classification reached the code that depends on it.
	requireTxAbsent(t, realStore, childTx.TxIDChainHash())
	require.False(t, parentOutpointSpent(t, realStore, parentTx), "the unwind ran, which is what the classification buys")
	require.Equal(t, 1, spy.unspendCalls)

	require.Equal(t, droppedBefore+1, testutil.ToFloat64(prometheusValidatorShedDroppedTotal))
	require.Equal(t, deadlinesBefore, testutil.ToFloat64(prometheusValidatorHandoffDeadlineTotal),
		"nothing was misclassified as a handoff deadline")
}

// Test 14d — the split-deployment skew is OBSERVABLE.
//
// handoffFloor is sized from this process's copy of blockassembly_queueFullWaitTimeout.
// If that copy under-reports what the block-assembly process actually enforces, the
// deadline fires before the shed arrives and the shed is neither classified nor unwound.
// The self-reporting RPC field that would remove the guesswork is deferred, so this
// test pins what ships instead: the condition is reproduced, the degradation is exactly
// pre-PR behaviour, and it is counted and named so an operator can act on it.
func TestValidate_UnderReportedRemoteQueueWaitIsObservable(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx, logger := recoverySetupWithLogger(t, "queue_shed_skew", 1)

	// Local copy says 20ms (floor 40ms); the remote really waits 200ms.
	floor := shrinkHandoffFloor(t, v, 20*time.Millisecond, 20*time.Millisecond)
	require.Equal(t, 40*time.Millisecond, floor)

	baStore.shedAfter = 200 * time.Millisecond

	initPrometheusMetrics()

	droppedBefore := testutil.ToFloat64(prometheusValidatorShedDroppedTotal)
	deadlinesBefore := testutil.ToFloat64(prometheusValidatorHandoffDeadlineTotal)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true), WithWaitForBlockAssembly(true))

	// The misclassification is reproduced, not assumed: this is a test OF the residual.
	require.Error(t, err)
	require.NotErrorIs(t, err, errors.ErrThresholdExceeded, "under skew the shed is lost to our own deadline")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// The degradation is exactly pre-PR behaviour: left locked, recovered by the
	// unmined reload. No unwind, and nothing worse than before.
	require.Equal(t, 0, spy.unspendCalls)
	require.True(t, metaLocked(t, realStore, childTx.TxIDChainHash()).Locked)
	require.True(t, parentOutpointSpent(t, realStore, parentTx))
	require.Equal(t, 0, spy.unlockCalls)

	require.Equal(t, deadlinesBefore+1, testutil.ToFloat64(prometheusValidatorHandoffDeadlineTotal),
		"the skew is attributable to its own counter")
	require.Equal(t, droppedBefore, testutil.ToFloat64(prometheusValidatorShedDroppedTotal),
		"and is NOT double-counted as a clean shed drop")

	logged := logger.joined()
	require.Contains(t, logged, "blockassembly_queueFullWaitTimeout", "the log must name the setting to align")
	require.Contains(t, logged, "validator_blockAssemblyShedRetryTimeout", "and the one that could be raised instead")
}

// TestHandoffFloorAndRetryWindow pins the deadline arithmetic with no timing at all, so
// it cannot flake and it fails instantly if the derivation is wrong.
//
// It asserts the two COMPUTED values against per-row constants. The
// window+floor==shedRetryTimeout identity is a tautology of the expression and is not
// what this proves: this proves the rule is computed as designed, while the bound being
// honoured at runtime is what the call-count assertions in tests 14a-14c carry.
//
// The slack is set on every row through validator_handoffRoundTripSlack's field rather
// than being left implicit, so the rows that drive it to a non-default value prove the
// setting actually reaches the arithmetic, and the rows that leave it non-positive
// prove the accessor's fallback does — the loader can never produce those, so nothing
// else covers them.
func TestHandoffFloorAndRetryWindow(t *testing.T) {
	const defaultSlack = 500 * time.Millisecond

	tests := []struct {
		name       string
		queueWait  time.Duration
		shedRetry  time.Duration
		slack      time.Duration
		wantFloor  time.Duration
		wantWindow time.Duration
	}{
		{
			name:       "shipped defaults",
			queueWait:  100 * time.Millisecond,
			shedRetry:  2 * time.Second,
			slack:      defaultSlack,
			wantFloor:  600 * time.Millisecond,
			wantWindow: 1400 * time.Millisecond,
		},
		{
			name:       "no queue wait configured: the floor is the slack alone",
			queueWait:  0,
			shedRetry:  2 * time.Second,
			slack:      defaultSlack,
			wantFloor:  defaultSlack,
			wantWindow: 1500 * time.Millisecond,
		},
		{
			name:       "negative queue wait is clamped to zero",
			queueWait:  -5 * time.Second,
			shedRetry:  2 * time.Second,
			slack:      defaultSlack,
			wantFloor:  defaultSlack,
			wantWindow: 1500 * time.Millisecond,
		},
		{
			name:       "queue wait far above the window: floor wins, no room to retry",
			queueWait:  10 * time.Second,
			shedRetry:  2 * time.Second,
			slack:      defaultSlack,
			wantFloor:  10500 * time.Millisecond,
			wantWindow: -8500 * time.Millisecond,
		},
		{
			name:       "unset shedRetryTimeout falls back to the documented default",
			queueWait:  100 * time.Millisecond,
			shedRetry:  0,
			slack:      defaultSlack,
			wantFloor:  600 * time.Millisecond,
			wantWindow: 1400 * time.Millisecond,
		},
		{
			// The remedy the handoff-deadline warning names: a raised slack widens the
			// per-attempt deadline, and pays for it out of the retry window.
			name:       "a raised slack widens the floor and narrows the window",
			queueWait:  100 * time.Millisecond,
			shedRetry:  2 * time.Second,
			slack:      1500 * time.Millisecond,
			wantFloor:  1600 * time.Millisecond,
			wantWindow: 400 * time.Millisecond,
		},
		{
			name:       "a lowered slack restores room to retry under a large queue wait",
			queueWait:  time.Second,
			shedRetry:  2 * time.Second,
			slack:      50 * time.Millisecond,
			wantFloor:  1050 * time.Millisecond,
			wantWindow: 950 * time.Millisecond,
		},
		{
			name:       "unset slack falls back to the documented default",
			queueWait:  100 * time.Millisecond,
			shedRetry:  2 * time.Second,
			slack:      0,
			wantFloor:  600 * time.Millisecond,
			wantWindow: 1400 * time.Millisecond,
		},
		{
			name:       "negative slack falls back to the documented default, it does not subtract",
			queueWait:  100 * time.Millisecond,
			shedRetry:  2 * time.Second,
			slack:      -250 * time.Millisecond,
			wantFloor:  600 * time.Millisecond,
			wantWindow: 1400 * time.Millisecond,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockAssembly.QueueFullWaitTimeout = tc.queueWait
			tSettings.Validator.BlockAssemblyShedRetryTimeout = tc.shedRetry
			tSettings.Validator.HandoffRoundTripSlack = tc.slack

			v := &Validator{settings: tSettings}

			require.Equal(t, tc.wantFloor, v.handoffFloor())
			require.Equal(t, tc.wantWindow, v.shedRetryTimeout()-v.handoffFloor())

			// A window at or below zero is the degenerate case New warns about: one
			// bounded attempt, no retries.
			require.Equal(t, tc.wantWindow <= 0, v.handoffFloor() >= v.shedRetryTimeout(),
				"the startup-warning condition and a non-positive window are the same predicate")
		})
	}
}

// TestEffectiveBatcherFlushWait pins the effective flush wait the F5 startup guard
// compares against handoffFloor. It must follow the block-assembly client's batcher
// construction: no bound when batching is off or in drain mode; the ticker interval
// supersedes the lazy first-item timeout when set; otherwise the lazy timeout.
func TestEffectiveBatcherFlushWait(t *testing.T) {
	tests := []struct {
		name        string
		batchSize   int
		drainMode   bool
		sendTimeout int
		tickerMs    int
		wantWait    time.Duration
		wantKey     string
		wantBounded bool
	}{
		{
			name:        "lazy first-item timeout governs by default",
			batchSize:   1024,
			sendTimeout: 5,
			wantWait:    5 * time.Millisecond,
			wantKey:     "blockassembly_sendBatchTimeout",
			wantBounded: true,
		},
		{
			name:        "ticker interval supersedes the lazy timeout when set",
			batchSize:   1024,
			sendTimeout: 5,
			tickerMs:    250,
			wantWait:    250 * time.Millisecond,
			wantKey:     "blockassembly_sendBatchTickerIntervalMillis",
			wantBounded: true,
		},
		{
			name:        "batching off: nothing is bounded",
			batchSize:   0,
			sendTimeout: 5000,
			wantBounded: false,
		},
		{
			name:        "drain mode flushes immediately: nothing is bounded",
			batchSize:   1024,
			drainMode:   true,
			sendTimeout: 5000,
			tickerMs:    5000,
			wantBounded: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockAssembly.SendBatchSize = tc.batchSize
			tSettings.BatcherDrainMode = tc.drainMode
			tSettings.BlockAssembly.SendBatchTimeout = tc.sendTimeout
			tSettings.BlockAssembly.SendBatchTickerIntervalMillis = tc.tickerMs

			v := &Validator{settings: tSettings}

			wait, key, bounded := v.effectiveBatcherFlushWait()
			require.Equal(t, tc.wantBounded, bounded)

			if tc.wantBounded {
				require.Equal(t, tc.wantWait, wait)
				require.Equal(t, tc.wantKey, key)
			}
		})
	}
}

// TestValidatorSettings_UnwindTimeoutFallback pins the defensive arm of the two
// duration accessors promoted to settings in this change.
//
// The loader can never produce a non-positive value for either key — it substitutes the
// default before the struct is built — so nothing but this test exercises the fallback.
// It matters because tests, and any other code that builds a Settings struct directly,
// bypass the loader entirely: without the fallback a zero-valued struct would give the
// unwind an already-expired context and the handoff a deadline with no margin at all,
// which is the exact failure mode (a shed misclassified as a local context deadline)
// these bounds exist to avoid.
//
// This is the executable counterpart of the "### Values" section in both longdescs. If
// the fallback behaviour is ever changed, this test and that documentation both have to
// move.
func TestValidatorSettings_UnwindTimeoutFallback(t *testing.T) {
	nonPositive := []struct {
		name  string
		value time.Duration
	}{
		{name: "unset", value: 0},
		{name: "negative", value: -3 * time.Second},
	}

	t.Run("shedUnwindTimeout", func(t *testing.T) {
		for _, tc := range nonPositive {
			t.Run(tc.name, func(t *testing.T) {
				tSettings := test.CreateBaseTestSettings(t)
				tSettings.Validator.ShedUnwindTimeout = tc.value

				v := &Validator{settings: tSettings}

				require.Equal(t, defaultShedUnwindTimeout, v.shedUnwindTimeout())
				require.Equal(t, 2*time.Second, v.shedUnwindTimeout(), "the default must stay the documented 2s")
			})
		}

		t.Run("a positive value is used as configured", func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Validator.ShedUnwindTimeout = 7 * time.Second

			v := &Validator{settings: tSettings}

			require.Equal(t, 7*time.Second, v.shedUnwindTimeout())
		})
	})

	t.Run("handoffRoundTripSlack", func(t *testing.T) {
		for _, tc := range nonPositive {
			t.Run(tc.name, func(t *testing.T) {
				tSettings := test.CreateBaseTestSettings(t)
				tSettings.Validator.HandoffRoundTripSlack = tc.value

				v := &Validator{settings: tSettings}

				require.Equal(t, defaultHandoffRoundTripSlack, v.handoffRoundTripSlack())
				require.Equal(t, 500*time.Millisecond, v.handoffRoundTripSlack(), "the default must stay the documented 500ms")
			})
		}

		t.Run("a positive value is used as configured", func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Validator.HandoffRoundTripSlack = 250 * time.Millisecond

			v := &Validator{settings: tSettings}

			require.Equal(t, 250*time.Millisecond, v.handoffRoundTripSlack())
		})
	})
}

// Test 15 — the validator gRPC ValidateTransaction surfaces a shed as
// codes.ResourceExhausted.
func TestValidateTransactionGRPC_SurfacesResourceExhausted(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	server := NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil, nil)
	server.validator = &MockValidator{
		ValidateFunc: func(_ context.Context, _ *bt.Tx) (*meta.Data, error) {
			return nil, errors.NewThresholdExceededError("block assembly queue full")
		},
	}

	resp, err := server.ValidateTransaction(context.Background(), &validator_api.ValidateTransactionRequest{
		TransactionData: sampleTx,
		BlockHeight:     100,
	})

	require.Error(t, err)
	require.NotNil(t, resp)
	require.False(t, resp.Valid)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

// Test 15 (fresh main path) — a first-submission synchronous shed. A brand-new
// transaction validates, spends and is created (Locked), and its block-assembly
// send fails with the queue full. Driven through the real
// Server.ValidateTransaction with WaitForBlockAssembly unset, this must surface
// as codes.ResourceExhausted (not Internal) and must leave NO trace in the store:
// the record is deleted and the parent's output unspent, so the 503 the client
// receives is a truthful rejection it can simply retry.
func TestValidateTransactionGRPC_FreshMainPathShedSurfacesResourceExhausted(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_grpc_fresh")

	// Fresh child — NOT pre-created. The send fails after spend/create.
	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	initPrometheusMetrics()

	srv := &Server{
		logger:    &ulogger.TestLogger{},
		validator: v,
		settings:  test.CreateBaseTestSettings(t),
	}

	skipPolicy := true

	resp, err := srv.ValidateTransaction(ctx, &validator_api.ValidateTransactionRequest{
		TransactionData:  childTx.ExtendedBytes(),
		BlockHeight:      100,
		SkipPolicyChecks: &skipPolicy,
	})

	require.Error(t, err)
	require.NotNil(t, resp)
	require.False(t, resp.Valid)
	require.Equal(t, codes.ResourceExhausted, status.Code(err),
		"a fresh first-submission shed must surface as ResourceExhausted, not Internal")
	require.GreaterOrEqual(t, baStore.calls, 1, "the send was attempted")

	// The shed left no trace: the record is gone and the parent's output is
	// spendable again. The 2PC unlock never ran — the unwind deletes the record
	// rather than unlocking it, so there is nothing left to unlock.
	requireTxAbsent(t, realStore, childTx.TxIDChainHash())
	require.False(t, parentOutpointSpent(t, realStore, parentTx), "a shed tx's inputs are unspent again")
	require.Equal(t, 0, spy.unlockCalls, "the unwind deletes rather than unlocks; no 2PC unlock runs on a main-path shed")
}

// Test 16 — the validator HTTP /tx surface maps a shed to 503, not 500.
func TestValidateHTTP_ShedReturns503(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	server := &Server{
		logger: &ulogger.TestLogger{},
		validator: &MockValidator{
			ValidateFunc: func(_ context.Context, _ *bt.Tx) (*meta.Data, error) {
				return nil, errors.NewThresholdExceededError("block assembly queue full")
			},
		},
		settings:   test.CreateBaseTestSettings(t),
		httpServer: echo.New(),
	}

	e := echo.New()

	testTx, err := bt.NewTxFromBytes(sampleTx)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader(testTx.ExtendedBytes()))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, server.handleSingleTx(ctx)(c))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, "an overload shed must be 503, not 500")
}

// Test 17 — the status-mapping helper returns 503 for ErrThresholdExceeded and
// keeps 500 for everything else.
func TestHTTPStatusForTxError_ThresholdExceeded(t *testing.T) {
	require.Equal(t, http.StatusServiceUnavailable, httpStatusForTxError(errors.ErrThresholdExceeded))
	require.Equal(t, http.StatusServiceUnavailable, httpStatusForTxError(errors.NewThresholdExceededError("wrapped")))
	require.Equal(t, http.StatusInternalServerError, httpStatusForTxError(errors.NewProcessingError("other")))
}

// A block-context shed is accepted, not unwound. The unwind's own preconditions
// assumed block-context callers never reach it because they set
// AddTXToBlockAssembly=false; several of them do not, so a shed on a transaction
// that arrived as part of a block used to DeleteComplete that transaction and
// unspend its inputs, and then fail the block. The InBlock arm makes the
// documented invariant a branch: the block is the authority and the template
// entry is only an optimisation.
func TestValidate_InBlockShedIsAcceptedWithoutUnwind(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_inblock_accepted")

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	initPrometheusMetrics()

	unwindsBefore := testutil.ToFloat64(prometheusValidatorShedUnwindTotal)
	inBlockBefore := testutil.ToFloat64(prometheusValidatorShedInBlockDroppedTotal)

	// AddTXToBlockAssembly is left at its true default, which is exactly what the
	// unreached block-context call sites do.
	txMeta, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true), WithInBlock(true))
	require.NoError(t, err, "a block-context shed must not fail the transaction, and so must not fail the block")
	require.NotNil(t, txMeta)

	require.GreaterOrEqual(t, baStore.calls, 1, "the hand-off was still attempted")

	// Fully accepted: record present, unlocked by the two-phase commit, inputs spent.
	require.NotNil(t, metaLocked(t, realStore, childTx.TxIDChainHash()), "the record survives")
	require.False(t, txMeta.Locked, "the two-phase commit ran, so the transaction is spendable")
	require.Equal(t, 1, spy.unlockCalls, "the 2PC unlock ran exactly once")
	require.True(t, parentOutpointSpent(t, realStore, parentTx), "the parent's output stays spent by this transaction")

	require.Equal(t, 0, spy.unspendCalls, "nothing is unspent on the block-context arm")
	require.Equal(t, unwindsBefore, testutil.ToFloat64(prometheusValidatorShedUnwindTotal),
		"the unwind must not even be attempted for a block-context transaction")
	require.Equal(t, inBlockBefore+1, testutil.ToFloat64(prometheusValidatorShedInBlockDroppedTotal),
		"the missing template entry is the accepted residual, so it has to be observable")
}

// The ordering item, made concrete. A same-block descendant can spend a Locked
// parent's outputs, because the block-context spend paths pass IgnoreLocked. If a
// shed then deleted the parent, the descendant's spends of those outputs went with
// its UTXO map: child accepted without parent.
func TestValidate_InBlockShedDoesNotDeleteAParentADescendantSpent(t *testing.T) {
	ctx := context.Background()
	v, _, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_inblock_ordering")

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	initPrometheusMetrics()

	// The parent of the pair under test is childTx: it is validated here (so a shed
	// can act on it) and then spent by a grandchild below.
	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true), WithInBlock(true))
	require.NoError(t, err)

	priv, pub := bec.PrivateKeyFromBytes([]byte("QUEUE_SHED_RECOVERY_TESTKEY_1234"))

	grandchildTx := transactions.Create(t,
		transactions.WithPrivateKey(priv),
		transactions.WithInput(childTx, 0, priv),
		transactions.WithP2PKHOutputs(1, 80_000, pub),
	)

	// The descendant's spend is driven at the store, with the same option the
	// block-context paths thread through (Validate's WithIgnoreLocked becomes
	// utxo.WithIgnoreLocked on this call). Driving it through Validate instead is not
	// possible against a shed parent: consensus rejects an in-block transaction whose
	// input is unconfirmed (bad-txns-unconfirmed-input-in-block) and a shed parent is
	// by definition unmined, so the option set the item is about cannot be assembled
	// above the store. The store call is where the harm lands anyway — a deleted
	// parent takes its UTXO map, and with it the descendant's recorded spends.
	_, _, err = realStore.SpendAndCreate(ctx, grandchildTx, 100, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err, "the descendant must be able to spend its shed parent's outputs")

	require.NotNil(t, metaLocked(t, realStore, childTx.TxIDChainHash()),
		"the parent a descendant spent must still exist")
	require.True(t, outpointSpent(t, realStore, childTx, 0),
		"the descendant's spend of the parent's output must survive")
	require.True(t, parentOutpointSpent(t, realStore, parentTx))
}

// The unwind aborts when the record is back immediately before the unspend. The
// delete writes no tombstone and nothing serialises the unwind against a
// concurrent submission of the same txid, so a resubmission can recreate the
// record and take ownership of spends that are byte-identical to this call's own
// (SpendingData is {txid, vin}). Clearing them would free the inputs of a live,
// possibly-mined transaction. One verify taken before the cascade's child passes,
// the blob deletes and the bounded verify retries is not a basis for that.
func TestValidate_ShedUnwindAbortsWhenRecordReappearedBeforeUnspend(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx, logger := recoverySetupWithLogger(t, "queue_shed_unwind_reappeared", 1)

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	// The first read-back reports gone (the delete really ran); the pre-unspend
	// re-check finds it readable again.
	spy.reappearAfterGoneReads = 1

	initPrometheusMetrics()

	reappearedBefore := testutil.ToFloat64(prometheusValidatorShedUnwindReappeared)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "the caller still sees the shed")

	require.Equal(t, 0, spy.unspendCalls, "fail closed: the inputs belong to whoever owns the record now")
	require.True(t, parentOutpointSpent(t, realStore, parentTx), "the parent's output stays spent")

	require.Equal(t, reappearedBefore+1, testutil.ToFloat64(prometheusValidatorShedUnwindReappeared))

	require.Contains(t, logger.joined(), childTx.TxID(), "the abort names the transaction")
	require.Contains(t, logger.joined(), outpointOf(parentTx, 0), "and the outpoints left spent")
}

// The negative: with no reappearance the re-check must not become an
// unconditional abort. Two verify rounds run and the unspend still happens.
func TestValidate_ShedUnwindStillUnspendsWhenRecordStaysGone(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx := recoverySetup(t, "queue_shed_unwind_stays_gone")

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	initPrometheusMetrics()

	reappearedBefore := testutil.ToFloat64(prometheusValidatorShedUnwindReappeared)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.ErrorIs(t, err, errors.ErrThresholdExceeded)

	require.Equal(t, 2, spy.verifyReadCalls,
		"two verify rounds: the post-delete check and the pre-unspend re-check")
	require.Equal(t, 1, spy.unspendCalls, "the unwind completed")
	require.False(t, parentOutpointSpent(t, realStore, parentTx), "the inputs were freed as intended")
	requireTxAbsent(t, realStore, childTx.TxIDChainHash())

	require.Equal(t, reappearedBefore, testutil.ToFloat64(prometheusValidatorShedUnwindReappeared))
}

// newForStartupCheck builds a validator purely to exercise New's startup checks,
// returning the logger that captured them. mutate shapes the settings the checks
// read.
func newForStartupCheck(t *testing.T, mutate func(*settings.Settings)) *capturingLogger {
	t.Helper()

	logger := &capturingLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	mutate(tSettings)

	nullStore, err := nullstore.NewNullStore()
	require.NoError(t, err)

	_, err = New(context.Background(), logger, tSettings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	return logger
}

// The hand-off floor models the batcher's first-item flush wait but cannot model
// the wait for a dispatch slot: that is (producers queued ahead / limit) x
// per-send latency and neither factor is configured, so any number a guard
// computed would be fabricated. Startup must therefore disclose the omission
// rather than report on a set of timing keys that silently excludes the one that
// matters — a hand-off that overruns the floor lands in the arm that leaves the
// transaction locked.
func TestNew_ReportsSendBatchMaxConcurrentOutsideTheHandoffFloor(t *testing.T) {
	t.Run("discloses when batching with a concurrency limit", func(t *testing.T) {
		logged := newForStartupCheck(t, func(s *settings.Settings) {
			s.BlockAssembly.SendBatchSize = 100
			s.BatcherDrainMode = false
			s.BlockAssembly.SendBatchMaxConcurrent = 3
		}).joined()

		require.Contains(t, logged, "blockassembly_sendBatchMaxConcurrent",
			"the line must name the key whose wait is unmodelled")
		require.Contains(t, logged, "teranode_validator_handoff_deadline_total",
			"and the counter that shows the consequence")
	})

	t.Run("silent with no concurrency limit", func(t *testing.T) {
		logged := newForStartupCheck(t, func(s *settings.Settings) {
			s.BlockAssembly.SendBatchSize = 100
			s.BatcherDrainMode = false
			s.BlockAssembly.SendBatchMaxConcurrent = 0
		}).joined()

		require.NotContains(t, logged, "blockassembly_sendBatchMaxConcurrent")
	})

	t.Run("silent when nothing is batched", func(t *testing.T) {
		// Nothing is batched, so nothing blocks for a dispatch slot.
		logged := newForStartupCheck(t, func(s *settings.Settings) {
			s.BlockAssembly.SendBatchSize = 0
			s.BlockAssembly.SendBatchMaxConcurrent = 3
		}).joined()

		require.NotContains(t, logged, "blockassembly_sendBatchMaxConcurrent")
	})
}

// A shed's in-place retry holds the parent Locked for the whole window while a
// child spending it gets only the TX_LOCKED backoff budget, and nothing in the
// settings relates the two. At the shipped defaults the window is ~20x the
// budget, so children are lost silently — BSV has no mempool to hold them.
// Startup is the only place that arithmetic can be named before it costs
// transactions.
func TestNew_WarnsWhenShedRetryWindowOutrunsChildLockBudget(t *testing.T) {
	t.Run("shipped defaults with a positive cap", func(t *testing.T) {
		logged := newForStartupCheck(t, func(s *settings.Settings) {
			s.BlockAssembly.MaxQueueItems = 1
		}).joined()

		require.Contains(t, logged, "validator_blockAssemblyShedRetryTimeout")
		require.Contains(t, logged, "validator_txlocked_maxRetries")
		require.Contains(t, logged, "teranode_validator_parent_commit_exhausted")
	})

	t.Run("silent with no queue cap", func(t *testing.T) {
		// No cap, no shed, so the two budgets never meet.
		logged := newForStartupCheck(t, func(s *settings.Settings) {
			s.BlockAssembly.MaxQueueItems = 0
		}).joined()

		require.NotContains(t, logged, "child TX_LOCKED budget")
	})

	t.Run("silent when the two budgets are coupled", func(t *testing.T) {
		// A 100ms window (700ms retry timeout minus the 600ms shipped floor) against
		// a 5-retry child budget of 310ms.
		logged := newForStartupCheck(t, func(s *settings.Settings) {
			s.BlockAssembly.MaxQueueItems = 1
			s.Validator.BlockAssemblyShedRetryTimeout = 700 * time.Millisecond
			s.Validator.TxLockedMaxRetries = 5
		}).joined()

		require.NotContains(t, logged, "child TX_LOCKED budget")
	})
}

// childLockBudget must mirror the retry loop's own arithmetic, including its
// clamps, or the startup warning above compares against a number the loop does
// not use.
func TestChildLockBudget(t *testing.T) {
	require.Equal(t, time.Duration(0), childLockBudget(0), "no retries, no budget")
	require.Equal(t, 10*time.Millisecond, childLockBudget(1))
	require.Equal(t, 70*time.Millisecond, childLockBudget(3), "10+20+40 at the shipped default")
	require.Equal(t, 310*time.Millisecond, childLockBudget(5))
	require.Equal(t, time.Duration(0), childLockBudget(-1), "clamped like the loop clamps")
	require.Equal(t, childLockBudget(txLockedMaxSafeRetries), childLockBudget(txLockedMaxSafeRetries+7),
		"the loop's safe-retry clamp applies here too")
}

// The pre-unspend re-check has two distinct abort causes and must not conflate
// them. verifyRecordDeleted reports "not gone" both for a record that is
// conclusively readable and for a read that kept failing, and only the first is a
// competing submission. Counting an unreliable store as a reappearance would make
// shed_unwind_reappeared_total useless as the signal that per-txid serialisation is
// actually needed, and the log would tell an operator a transaction came back when
// nothing established that.
func TestValidate_ShedUnwindSecondVerifyReadFailureIsNotAReappearance(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx, parentTx, logger := recoverySetupWithLogger(t, "queue_shed_unwind_second_verify_fails", 1)

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	// Read 1 (the post-delete verify) answers "gone"; every read from the second on
	// — the pre-unspend round and its bounded retries — fails.
	spy.verifyErr = errors.NewStorageError("utxo store read unavailable")
	spy.verifyFailFromRead = 2

	initPrometheusMetrics()

	reappearedBefore := testutil.ToFloat64(prometheusValidatorShedUnwindReappeared)
	unverifiedBefore := testutil.ToFloat64(prometheusValidatorShedUnwindUnverified)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "the caller still sees the shed")

	// Still fails closed: an inconclusive read is no basis for freeing the inputs.
	require.Equal(t, 0, spy.unspendCalls, "an unconfirmable re-check must not unspend")
	require.True(t, parentOutpointSpent(t, realStore, parentTx), "the inputs stay spent")

	require.Equal(t, reappearedBefore, testutil.ToFloat64(prometheusValidatorShedUnwindReappeared),
		"a failing read establishes nothing, so it is NOT a reappearance")
	require.Equal(t, unverifiedBefore+1, testutil.ToFloat64(prometheusValidatorShedUnwindUnverified),
		"it is the unverified condition, the same one the post-delete round reports")

	logged := logger.joined()
	require.NotContains(t, logged, "present again",
		"the log must not claim the record came back when the read never said so")
	require.Contains(t, logged, outpointOf(parentTx, 0),
		"and it must still name the outpoints an operator has to reconcile")

	// The bounded retry ran on the second round too: read 1 plus shedUnwindVerifyAttempts.
	require.Equal(t, 1+shedUnwindVerifyAttempts, spy.verifyReadCalls)
}
