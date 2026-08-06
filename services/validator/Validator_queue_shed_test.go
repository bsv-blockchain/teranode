package validator

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

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
// times Store was called and whether the transaction was still locked at the
// moment of the send (used to assert send-then-unlock ordering), and returns a
// configurable error.
type recoveryBAStore struct {
	store        utxostore.Store
	err          error
	calls        int
	lockedAtSend bool
}

var _ blockassembly.Store = (*recoveryBAStore)(nil)

func (f *recoveryBAStore) Store(ctx context.Context, hash *chainhash.Hash, _, _ uint64, _ subtree.TxInpoints) (bool, error) {
	f.calls++

	md := &meta.Data{}
	if getErr := f.store.GetMeta(ctx, hash, md); getErr == nil {
		f.lockedAtSend = md.Locked
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

	baStore := &recoveryBAStore{store: spy}
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

// Test 11 — the shed path leaves the transaction locked. A block-assembly send
// that fails with ThresholdExceeded must NOT unlock the transaction (the
// withdrawn §6.5): its Locked flag is the only interlock keeping descendants out
// of the template ahead of it.
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

// Test 12 — gate tightness. An ordinary duplicate (existing, not locked) does
// not re-drive the handoff; nor does a locked-but-mined or locked-but-conflicting
// transaction. None of them triggers a block-assembly send.
func TestValidate_RecoveryGateTightness(t *testing.T) {
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

// Test 13 — recovery works. A resubmit of an existing, locked, unmined,
// non-conflicting transaction re-drives the handoff (a send) and then unlocks
// it (the two-phase commit), in that order: at send time the tx is still locked,
// and afterwards it is unlocked.
func TestValidate_RecoveryReDrivesHandoff(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx := recoverySetup(t, "queue_shed_recover")

	_, err := realStore.Create(ctx, childTx, 100, utxostore.WithLocked(true))
	require.NoError(t, err)

	_, err = v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.NoError(t, err)

	require.Equal(t, 1, baStore.calls, "recovery re-drives exactly one send")
	require.True(t, baStore.lockedAtSend, "ordering: the tx is still locked at the moment of the send")
	require.Equal(t, 1, spy.unlockCalls, "recovery unlocks after the send")
	require.False(t, metaLocked(t, realStore, childTx.TxIDChainHash()).Locked, "the transaction ends unlocked")
}

// Test 14 — recovery fails safely. If the queue is still full on resubmit, the
// send errors and the transaction is left locked (no unlock).
func TestValidate_RecoveryFailsSafely(t *testing.T) {
	ctx := context.Background()
	v, spy, baStore, realStore, childTx := recoverySetup(t, "queue_shed_recover_fail")

	_, err := realStore.Create(ctx, childTx, 100, utxostore.WithLocked(true))
	require.NoError(t, err)

	baStore.err = errors.NewThresholdExceededError("block assembly queue still full")

	_, err = v.Validate(ctx, childTx, 100, WithSkipPolicyChecks(true))
	require.Error(t, err, "a still-full queue surfaces as an error")

	require.Equal(t, 1, baStore.calls, "recovery attempted the send once")
	require.Equal(t, 0, spy.unlockCalls, "a failed recovery must not unlock the transaction")
	require.True(t, metaLocked(t, realStore, childTx.TxIDChainHash()).Locked, "the transaction stays locked for a later retry")
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

// Test 15 (production path) — the same ResourceExhausted surface, but proven
// through the real chain: Server.ValidateTransaction -> real Validator ->
// sendToBlockAssembler -> blockassembly.Store returns ErrThresholdExceeded ->
// WrapGRPC. This exercises the actual wrapping shape rather than a MockValidator
// shortcut, and confirms the classification is not masked as a service fault.
func TestValidateTransactionGRPC_ProductionPathSurfacesResourceExhausted(t *testing.T) {
	ctx := context.Background()
	v, _, baStore, realStore, childTx := recoverySetup(t, "queue_shed_grpc_prod")

	// Strand the child (existing, locked, unmined) so a resubmit re-drives the
	// handoff, and keep the queue full so that send fails with the shed error.
	_, err := realStore.Create(ctx, childTx, 100, utxostore.WithLocked(true))
	require.NoError(t, err)

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
		"a shed must surface as ResourceExhausted through the real sendToBlockAssembler/WrapGRPC path, not be masked as a service fault")
	require.Equal(t, 1, baStore.calls, "the recovery send was attempted")
}

// Test 15 (fresh main path) — a first-submission shed. A brand-new transaction
// validates, spends and is created (Locked), and its block-assembly send fails
// with the queue full. Driven through the real Server.ValidateTransaction, this
// must surface as codes.ResourceExhausted (not Internal) — the common case plan
// section 6.6 targets — and must leave the tx Locked and unmined (section 6.5)
// so a resubmit can recover it (section 6.5.1).
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

	// Section 6.5: the shed transaction stays Locked and unmined, and its 2PC
	// unlock never ran — so section 6.5.1 recovery still applies on resubmit.
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
