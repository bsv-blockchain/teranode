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
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// unlockSpy wraps a real utxo store and counts SetLocked(..., false) calls so a
// test can assert whether a transaction was unlocked (its two-phase commit ran).
type unlockSpy struct {
	utxostore.Store
	unlockCalls int
}

func (s *unlockSpy) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	if !value {
		s.unlockCalls++
	}

	return s.Store.SetLocked(ctx, txHashes, value)
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
// whether to pre-create it (to exercise the ErrTxExists recovery gate) or to
// validate it fresh.
func recoverySetup(t *testing.T, dbName string) (*Validator, *unlockSpy, *recoveryBAStore, *sql.Store, *bt.Tx) {
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

	return v, spy, baStore, realStore, childTx
}

func metaLocked(t *testing.T, store utxostore.Store, hash *chainhash.Hash) *meta.Data {
	t.Helper()

	md := &meta.Data{}
	require.NoError(t, store.GetMeta(context.Background(), hash, md))

	return md
}

// Test 11 — the synchronous shed path leaves the transaction locked. With
// WaitForBlockAssembly unset, a block-assembly send that fails with
// ThresholdExceeded surfaces the shed and must NOT unlock the transaction: its
// Locked flag is the only interlock keeping descendants out of the template
// ahead of it, and the unmined reload on the next block-assembly start recovers
// it.
func TestValidate_ShedLeavesTxLocked(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx := recoverySetup(t, "queue_shed_locked")

	baStore.err = errors.NewThresholdExceededError("block assembly queue full")

	_, err := v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.Error(t, err, "a shed on the send path must surface as an error")

	require.GreaterOrEqual(t, baStore.calls, 1, "the send was attempted")
	require.Equal(t, 0, spy.unlockCalls, "SetLocked(false) must not be called when the send failed")
	require.True(t, metaLocked(t, realStore, childTx.TxIDChainHash()).Locked, "the transaction stays locked")
}

// Test 12 — a resubmit of an existing transaction is never re-sent. Whether it
// is an ordinary duplicate (existing, not locked), locked-but-mined, or
// locked-but-conflicting, an idempotent resubmit returns success and never
// triggers a block-assembly send: the resubmit-recovery branch was removed, so
// a resubmit cannot re-drive the handoff or mutate lock state.
func TestValidate_ExistingTxNotResent(t *testing.T) {
	ctx := context.Background()

	t.Run("ordinary duplicate is not resent", func(t *testing.T) {
		v, spy, baStore, realStore, childTx := recoverySetup(t, "queue_shed_dup")

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
		v, _, baStore, realStore, childTx := recoverySetup(t, "queue_shed_mined")

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
		v, _, baStore, realStore, childTx := recoverySetup(t, "queue_shed_conflicting")

		_, err := realStore.Create(ctx, childTx, 100,
			utxostore.WithLocked(true),
			utxostore.WithConflicting(true))
		require.NoError(t, err)

		require.True(t, metaLocked(t, realStore, childTx.TxIDChainHash()).Conflicting, "precondition: tx is conflicting")

		_, err = v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
		require.NoError(t, err)
		require.Equal(t, 0, baStore.calls, "a conflicting transaction must not be resent")
	})
}

// Test 13 — Kafka ingest path: the handoff is retried in place until it
// succeeds. WaitForBlockAssembly makes a queue-full shed retry rather than
// surface; once the queue drains, the send succeeds and the tx falls through to
// the txmeta publish and the 2PC unlock. Exactly one successful handoff, one
// txmeta message, one unlock — and the tx ends unlocked and unmined.
func TestValidate_KafkaIngestRetriesUntilHandoffSucceeds(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx := recoverySetup(t, "queue_shed_kafka_retry")

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

// Test 14 — Kafka ingest path: a context cancel mid-retry aborts promptly and
// leaves the tx durably Locked and unmined (the precondition for the unmined
// reload on the next block-assembly start). No 2PC unlock runs.
func TestValidate_KafkaIngestCtxCancelLeavesTxLocked(t *testing.T) {
	v, spy, baStore, realStore, childTx := recoverySetup(t, "queue_shed_kafka_cancel")

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
// as codes.ResourceExhausted (not Internal) and must leave the tx Locked and
// unmined so the unmined reload on the next block-assembly start recovers it.
func TestValidateTransactionGRPC_FreshMainPathShedSurfacesResourceExhausted(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx := recoverySetup(t, "queue_shed_grpc_fresh")

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

	// The shed transaction stays Locked and unmined, and its 2PC unlock never
	// ran — so the unmined reload on the next block-assembly start recovers it.
	md := metaLocked(t, realStore, childTx.TxIDChainHash())
	require.True(t, md.Locked, "a shed tx stays locked")
	require.Empty(t, md.BlockIDs, "a shed tx stays unmined")
	require.Equal(t, 0, spy.unlockCalls, "no 2PC unlock runs on a main-path shed")
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
