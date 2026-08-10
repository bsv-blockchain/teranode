package validator

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
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
}

func (s *unlockSpy) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	if !value {
		s.unlockCalls++
	}

	return s.Store.SetLocked(ctx, txHashes, value)
}

func (s *unlockSpy) Delete(ctx context.Context, hash *chainhash.Hash) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}

	if s.deleteIsCacheOnly {
		return nil
	}

	return s.Store.Delete(ctx, hash)
}

func (s *unlockSpy) Unspend(ctx context.Context, spends []*utxostore.Spend, flagAsLocked ...bool) error {
	s.unspendCalls++

	return s.Store.Unspend(ctx, spends, flagAsLocked...)
}

// parentOutpointSpent reports whether output 0 of parentTx reads as spent — the
// observable that distinguishes a shed unwind's Unspend from a no-op.
func parentOutpointSpent(t *testing.T, store utxostore.Store, parentTx *bt.Tx) bool {
	t.Helper()

	utxoHash, err := util.UTXOHashFromOutput(parentTx.TxIDChainHash(), parentTx.Outputs[0], 0)
	require.NoError(t, err)

	resp, err := store.GetSpend(context.Background(), &utxostore.Spend{
		TxID:     parentTx.TxIDChainHash(),
		Vout:     0,
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
type recoveryBAStore struct {
	err       error
	calls     int
	shedFirst int
}

var _ blockassembly.Store = (*recoveryBAStore)(nil)

func (f *recoveryBAStore) Store(_ context.Context, _ *chainhash.Hash, _, _ uint64, _ subtree.TxInpoints) (bool, error) {
	f.calls++

	if f.calls <= f.shedFirst {
		return false, errors.NewThresholdExceededError("block assembly queue full")
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

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
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
		transactions.WithP2PKHOutputs(1, 100_000, pub),
	)

	_, err = realStore.Create(ctx, parentTx, 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 1, BlockHeight: 1, SubtreeIdx: 0}))
	require.NoError(t, err)

	childTx := transactions.Create(t,
		transactions.WithPrivateKey(priv),
		transactions.WithInput(parentTx, 0, priv),
		transactions.WithP2PKHOutputs(1, 90_000, pub),
	)

	return v, spy, baStore, realStore, childTx, parentTx
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

	unwindsBefore := testutil.ToFloat64(prometheusValidatorShedUnwindTotal)

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.Error(t, err, "a shed on the send path must surface as an error")
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "the caller still sees the shed")

	require.GreaterOrEqual(t, baStore.calls, 1, "the send was attempted")
	require.Equal(t, 0, spy.unlockCalls, "the unwind deletes the record; it must not unlock it")

	requireTxAbsent(t, realStore, childTx.TxIDChainHash())
	require.False(t, parentOutpointSpent(t, realStore, parentTx), "the unwind unspends the parent's output")
	require.Equal(t, 1, spy.unspendCalls, "the unwind unspends exactly once")

	require.Equal(t, unwindsBefore+1, testutil.ToFloat64(prometheusValidatorShedUnwindTotal))
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

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "an unwind failure must not change the error the caller sees")

	require.Equal(t, failuresBefore+1, testutil.ToFloat64(prometheusValidatorShedUnwindFailures))

	require.Equal(t, 0, spy.unspendCalls, "a failed Delete must not be followed by an Unspend")
	require.True(t, parentOutpointSpent(t, realStore, parentTx), "the parent's output stays spent while the record survives")
	require.True(t, metaLocked(t, realStore, childTx.TxIDChainHash()).Locked, "the tx is left locked for the unmined-reload backstop")
	require.Equal(t, 0, spy.unlockCalls)
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
	// arithmetic (several 5ms backoff iterations).
	v.settings.Validator.BlockAssemblyShedRetryTimeout = 50 * time.Millisecond

	start := time.Now()

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true), WithWaitForBlockAssembly(true))
	elapsed := time.Since(start)

	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "on timeout the shed is returned, not retried forever")
	require.Greater(t, baStore.calls, 1, "the handoff was retried before giving up")
	require.Less(t, elapsed, 5*time.Second, "the retry must be bounded by the configured window, not by the stall")

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
