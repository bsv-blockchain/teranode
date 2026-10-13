package subtreevalidation

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// unlockFailingStore is a real sqlitememory UTXO store whose SetLocked always
// fails, which is what a store slower than validator_twoPhaseCommitTimeout
// looks like to the validator.
type unlockFailingStore struct {
	*sql.Store
	failsLeft atomic.Int64
}

func (s *unlockFailingStore) SetLocked(ctx context.Context, hashes []chainhash.Hash, value bool) error {
	if s.failsLeft.Add(-1) >= 0 {
		return errors.NewProcessingError("context deadline exceeded")
	}

	return s.Store.SetLocked(ctx, hashes, value)
}

// metaDroppingValidator behaves like the gRPC client: when the validator returns an
// error, the response metadata does not reach the caller.
type metaDroppingValidator struct {
	validator.Interface
}

func (m *metaDroppingValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts *validator.Options) (*meta.Data, error) {
	txMeta, err := m.Interface.ValidateWithOptions(ctx, tx, blockHeight, opts)
	if err != nil {
		return nil, errors.UnwrapGRPC(errors.WrapGRPC(err))
	}

	return txMeta, nil
}

const alwaysFail = int64(1) << 40

func newUnlockFailureServer(t *testing.T, remote bool, unlockFailures int64) (*Server, *unlockFailingStore, []*bt.Tx, *blockassembly.Mock) {
	t.Helper()

	InitPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.Disabled = false

	utxoURL, err := url.Parse("sqlitememory:///bless_unlock_failure_" + t.Name())
	require.NoError(t, err)

	baseStore, err := sql.New(ctx, logger, tSettings, utxoURL)
	require.NoError(t, err)

	store := &unlockFailingStore{Store: baseStore}
	store.failsLeft.Store(unlockFailures)

	txs := transactions.CreateTestTransactionChainWithCount(t, 4)

	_, err = store.Create(ctx, txs[0], 1, utxostore.WithLocked(false))
	require.NoError(t, err)

	require.NoError(t, store.SetBlockHeight(2))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	localClient, err := blockchain.NewLocalClient(logger, tSettings, &blockchainstore.MockStore{}, memory.New(), store)
	require.NoError(t, err)

	blockAssembly := blockassembly.NewMock()
	blockAssembly.On("Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(true, nil).Times(2)

	realValidator, err := validator.New(ctx, logger, tSettings, store,
		kafka.NewKafkaAsyncProducerMock(), kafka.NewKafkaAsyncProducerMock(), nil, blockAssembly, localClient)
	require.NoError(t, err)

	var validatorClient validator.Interface = realValidator
	if remote {
		validatorClient = &metaDroppingValidator{Interface: realValidator}
	}

	server := &Server{
		logger:          logger,
		settings:        tSettings,
		utxoStore:       store,
		validatorClient: validatorClient,
	}

	return server, store, txs, blockAssembly
}

// TestBlessMissingTransaction_UnlockFailureDoesNotRejectTx pins the requirement
// that a transaction the validator accepted, spent and created is blessed even
// when the post-acceptance two-phase-commit unlock fails. The record stays Locked
// in the store and heals when the tx is mined, so subtree validation must not
// count it as a bless failure and reject the subtree. The remote case covers the
// gRPC client, where the response metadata is lost together with the error.
func TestBlessMissingTransaction_UnlockFailureDoesNotRejectTx(t *testing.T) {
	for _, remote := range []bool{false, true} {
		name := "in-process validator"
		if remote {
			name = "remote validator without metadata"
		}

		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			server, store, txs, _ := newUnlockFailureServer(t, remote, alwaysFail)
			tx := txs[1]

			opts := validator.ProcessOptions(validator.WithAddTXToBlockAssembly(true))

			txMeta, err := server.blessMissingTransaction(ctx, chainhash.Hash{}, chainhash.Hash{}, tx, 2, map[uint32]bool{}, opts)
			require.NoError(t, err, "an accepted tx whose unlock timed out must not fail the bless")
			require.NotNil(t, txMeta, "bless must hand back the accepted tx's metadata")
			require.True(t, txMeta.Locked, "the accepted tx is still locked")

			stored := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), stored))
			require.True(t, stored.Locked, "the unlock failed, so the record stays Locked until it is mined")

		})
	}
}

// TestBlessMissingTransaction_UnlockFailureStillRejectsRealFailures guards against
// the unlock tolerance swallowing genuine validation errors.
func TestBlessMissingTransaction_UnlockFailureStillRejectsRealFailures(t *testing.T) {
	server, _, txs, _ := newUnlockFailureServer(t, true, alwaysFail)
	tx := txs[1]

	server.validatorClient = &failingValidator{Interface: server.validatorClient, err: errors.NewTxInvalidError("bad tx")}

	_, err := server.blessMissingTransaction(context.Background(), chainhash.Hash{}, chainhash.Hash{}, tx, 2, map[uint32]bool{}, validator.ProcessOptions())
	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrTxUnlockFailed))
}

type failingValidator struct {
	validator.Interface
	err error
}

func (f *failingValidator) ValidateWithOptions(context.Context, *bt.Tx, uint32, *validator.Options) (*meta.Data, error) {
	return nil, f.err
}

// TestBlessMissingTransactions_ChildOfUnlockFailedParentIsBlessed covers a subtree where the
// parent was accepted but its unlock failed. The child sits in the next level and must not be
// answered TX_LOCKED: subtree validation unlocks the level's parents before it moves on.
func TestBlessMissingTransactions_ChildOfUnlockFailedParentIsBlessed(t *testing.T) {
	ctx := context.Background()

	server, store, txs, _ := newUnlockFailureServer(t, true, 1)
	server.settings.SubtreeValidation.SpendBatcherSize = 10

	parent, child := txs[1], txs[2]

	missing := []missingTx{{tx: parent, idx: 0}, {tx: child, idx: 1}}
	txMetaSlice := make([]metaSliceItem, len(missing))

	err := server.blessMissingTransactions(ctx, chainhash.Hash{}, missing, "", "", txMetaSlice, 2, map[uint32]bool{})
	require.NoError(t, err, "a child of an accepted parent must not fail because the parent's unlock timed out")

	for i, tx := range []*bt.Tx{parent, child} {
		require.True(t, txMetaSlice[i].isSet, "tx %d must have its metadata recorded", i)

		stored := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), stored))
		require.False(t, stored.Locked, "tx %d must be spendable once the subtree is blessed", i)
	}
}

// TestBlessMissingTransactions_LevelUnlockFailureLeavesParentLocked pins the fallback: when the
// level unlock also fails, the parent stays locked and the child fails exactly as it did before.
func TestBlessMissingTransactions_LevelUnlockFailureLeavesParentLocked(t *testing.T) {
	ctx := context.Background()

	server, store, txs, _ := newUnlockFailureServer(t, true, alwaysFail)
	server.settings.SubtreeValidation.SpendBatcherSize = 10

	parent, child := txs[1], txs[2]

	missing := []missingTx{{tx: parent, idx: 0}, {tx: child, idx: 1}}
	txMetaSlice := make([]metaSliceItem, len(missing))

	err := server.blessMissingTransactions(ctx, chainhash.Hash{}, missing, "", "", txMetaSlice, 2, map[uint32]bool{})
	require.Error(t, err)
	require.True(t, txMetaSlice[0].isSet, "the parent was accepted")
	require.False(t, txMetaSlice[1].isSet, "the child could not spend a locked parent")

	stored := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, parent.TxIDChainHash(), stored))
	require.True(t, stored.Locked)
}

// unreadableStore writes normally and fails GetMeta, which is how a store that errors on reading
// back the record of an already-accepted transaction looks. With only set, the failures are limited
// to that one hash so a test can fail one read while the rest of the path still works.
type unreadableStore struct {
	*sql.Store
	only *chainhash.Hash
}

func (s *unreadableStore) GetMeta(ctx context.Context, hash *chainhash.Hash, md *meta.Data) error {
	if s.only == nil || *s.only == *hash {
		return errors.NewProcessingError("simulated GetMeta failure")
	}

	return s.Store.GetMeta(ctx, hash, md)
}

// TestBlessMissing_UnlockFailureSurvivesFailedMetaReadBack pins that a transaction the
// validator accepted still reaches the level's unlock retry when the accepted-meta
// read-back fails. The lock is what blocks the child; proof metadata has nothing to do
// with it, so losing the read must not discard the retry. Returning unlockFailed=false
// here — as the error arm did before — dropped the one signal the retry runs on, and
// left a child in the next level answered TX_LOCKED by a parent that was merely
// un-unlocked, not rejected.
func TestBlessMissing_UnlockFailureSurvivesFailedMetaReadBack(t *testing.T) {
	ctx := context.Background()

	server, store, txs, _ := newUnlockFailureServer(t, true, alwaysFail)
	server.validatorClient = &failingValidator{
		Interface: server.validatorClient,
		err:       errors.NewTxUnlockFailedError("[Validate] error marking tx as spendable", errors.NewProcessingError("context deadline exceeded")),
	}
	server.utxoStore = &unreadableStore{Store: store.Store}

	txMeta, unlockFailed, err := server.blessMissing(ctx, chainhash.Hash{}, chainhash.Hash{}, txs[1], 2, map[uint32]bool{}, validator.ProcessOptions())

	require.Error(t, err, "the bless still fails: the accepted metadata could not be read")
	require.Nil(t, txMeta, "no metadata is invented for a read that failed")
	require.True(t, unlockFailed, "an accepted transaction must still be queued for the unlock retry")
}

// TestUnlockAcceptedTransactions_CountsRetryOutcome pins the two counters that make the
// retry's outcome visible. bless_unlock_failed counts these transactions when they are
// identified; without a recovered/failed split on the retry itself, a node whose retry
// never succeeds looks identical to one where it always does.
func TestUnlockAcceptedTransactions_CountsRetryOutcome(t *testing.T) {
	ctx := context.Background()

	t.Run("counts the hashes the retry unlocked", func(t *testing.T) {
		// 0 failures left: the store unlocks on the first call.
		server, store, txs, _ := newUnlockFailureServer(t, true, 0)

		// the retry unlocks records that exist and are locked, as after an accepted bless
		for _, tx := range []*bt.Tx{txs[1], txs[2]} {
			_, err := store.Create(ctx, tx, 1, utxostore.WithLocked(true))
			require.NoError(t, err)
		}

		before := testutil.ToFloat64(prometheusSubtreeValidationBlessUnlockRecovered)

		server.unlockAcceptedTransactions(ctx, chainhash.Hash{}, []chainhash.Hash{*txs[1].TxIDChainHash(), *txs[2].TxIDChainHash()})

		for _, tx := range []*bt.Tx{txs[1], txs[2]} {
			stored := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), stored))
			require.False(t, stored.Locked, "%s must be spendable after the retry", tx.TxID())
		}

		require.Equal(t, before+2, testutil.ToFloat64(prometheusSubtreeValidationBlessUnlockRecovered))
	})

	t.Run("counts one failed retry call, not one per hash", func(t *testing.T) {
		server, _, txs, _ := newUnlockFailureServer(t, true, alwaysFail)
		before := testutil.ToFloat64(prometheusSubtreeValidationBlessUnlockRetryFailedBatches)

		server.unlockAcceptedTransactions(ctx, chainhash.Hash{}, []chainhash.Hash{*txs[1].TxIDChainHash(), *txs[2].TxIDChainHash()})

		require.Equal(t, before+1, testutil.ToFloat64(prometheusSubtreeValidationBlessUnlockRetryFailedBatches), "two hashes in one failed call count once")
	})

	t.Run("an empty level counts nothing", func(t *testing.T) {
		server, _, _, _ := newUnlockFailureServer(t, true, alwaysFail)
		before := testutil.ToFloat64(prometheusSubtreeValidationBlessUnlockRetryFailedBatches)

		server.unlockAcceptedTransactions(ctx, chainhash.Hash{}, nil)

		require.Equal(t, before, testutil.ToFloat64(prometheusSubtreeValidationBlessUnlockRetryFailedBatches))
	})
}

// TestProcessTransactionsInLevels_UnlockFailedParentGetsUnlocked pins the block-arrival path.
// processTransactionsInLevels is Phase 3, the blessing of transactions that arrive inside a
// block. It validates with WithIgnoreLocked(true), so children never meet a locked parent as
// TX_LOCKED; the accepted parent is unlocked once, when the function returns, so it does not
// stay Locked until it is mined.
func TestProcessTransactionsInLevels_UnlockFailedParentGetsUnlocked(t *testing.T) {
	ctx := context.Background()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	tx, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	require.NoError(t, err)

	store := server.utxoStore.(*utxostore.MockUtxostore)
	store.On("SetLocked", mock.Anything, []chainhash.Hash{*tx.TxIDChainHash()}, false).Return(nil).Maybe()
	// blessMissing reads the accepted record back; the fixture's GetMeta stub predates the
	// 3-argument signature, so give this test one the call actually matches.
	store.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// the validator accepts the transaction and only its unlock fails
	server.validatorClient = &failingValidator{
		Interface: server.validatorClient,
		err:       errors.NewTxUnlockFailedError("[Validate] error marking tx as spendable", errors.NewProcessingError("context deadline exceeded")),
	}

	require.NoError(t, server.processTransactionsInLevels(ctx, []*bt.Tx{tx}, chainhash.Hash{}, chainhash.Hash{}, 100, 0, 0, map[uint32]bool{}, true))

	store.AssertCalled(t, "SetLocked", mock.Anything, []chainhash.Hash{*tx.TxIDChainHash()}, false)
}

// scriptedValidator answers each transaction with a canned outcome, so a test can put an accepted
// transaction and a truly invalid one in the same level.
type scriptedValidator struct {
	validator.Interface

	unlockFailedFor chainhash.Hash
	invalidFor      chainhash.Hash
}

func (v *scriptedValidator) ValidateWithOptions(_ context.Context, tx *bt.Tx, _ uint32, _ *validator.Options) (*meta.Data, error) {
	h := *tx.TxIDChainHash()

	switch h {
	case v.unlockFailedFor:
		return nil, errors.NewTxUnlockFailedError("[Validate] error marking tx as spendable", errors.NewProcessingError("context deadline exceeded"))
	case v.invalidFor:
		return nil, errors.NewTxInvalidError("bad transaction")
	default:
		return nil, errors.NewProcessingError("unexpected transaction %s", h.String())
	}
}

// TestProcessTransactionsInLevels_InvalidSiblingDoesNotSkipUnlock pins the unlock on the error
// return. An invalid transaction makes the errgroup fail and cancels its own context. The
// transactions the same level had already ACCEPTED are spent, created and delivered, so skipping
// their unlock would leave them Locked until mined - a sibling being invalid has nothing to do
// with them. The unlock runs on the caller's context, which is still live.
func TestProcessTransactionsInLevels_InvalidSiblingDoesNotSkipUnlock(t *testing.T) {
	ctx := context.Background()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	accepted, err := createTestTransaction("tx1")
	require.NoError(t, err)

	sibling, err := createTestTransaction("tx2")
	require.NoError(t, err)
	require.NotEqual(t, *accepted.TxIDChainHash(), *sibling.TxIDChainHash(), "the two fixtures must be distinct transactions")

	store := server.utxoStore.(*utxostore.MockUtxostore)
	store.On("SetLocked", mock.Anything, mock.Anything, false).Return(nil).Maybe()
	store.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	server.validatorClient = &scriptedValidator{
		Interface:       server.validatorClient,
		unlockFailedFor: *accepted.TxIDChainHash(),
		invalidFor:      *sibling.TxIDChainHash(),
	}

	err = server.processTransactionsInLevels(ctx, []*bt.Tx{accepted, sibling}, chainhash.Hash{}, chainhash.Hash{}, 100, 0, 0, map[uint32]bool{}, true)
	require.Error(t, err, "the invalid sibling still fails the level")

	store.AssertCalled(t, "SetLocked", mock.Anything, []chainhash.Hash{*accepted.TxIDChainHash()}, false)
}

// TestBlessMissingTransactions_InvalidSiblingDoesNotSkipUnlock is the same ordering pin for the
// subtree a peer announces, which has its own level loop. The invalid transaction cancels the
// errgroup's context; the unlock runs on the caller's, which is still live.
func TestBlessMissingTransactions_InvalidSiblingDoesNotSkipUnlock(t *testing.T) {
	ctx := context.Background()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	accepted, err := createTestTransaction("tx1")
	require.NoError(t, err)

	sibling, err := createTestTransaction("tx2")
	require.NoError(t, err)

	store := server.utxoStore.(*utxostore.MockUtxostore)
	store.On("SetLocked", mock.Anything, mock.Anything, false).Return(nil).Maybe()
	store.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	server.validatorClient = &scriptedValidator{
		Interface:       server.validatorClient,
		unlockFailedFor: *accepted.TxIDChainHash(),
		invalidFor:      *sibling.TxIDChainHash(),
	}

	missing := []missingTx{{tx: accepted, idx: 0}, {tx: sibling, idx: 1}}
	txMetaSlice := make([]metaSliceItem, len(missing))

	err = server.blessMissingTransactions(ctx, chainhash.Hash{}, missing, "", "", txMetaSlice, 100, map[uint32]bool{})
	require.Error(t, err, "the invalid sibling still fails the subtree")

	store.AssertCalled(t, "SetLocked", mock.Anything, []chainhash.Hash{*accepted.TxIDChainHash()}, false)
}

// TestBlessMissingTransactions_QueuesAcceptedParentDespiteBlessError pins the ordering inside the
// level closure: a parent the validator ACCEPTED has to reach the level's unlock retry even when a
// check that runs after acceptance rejects the bless. Queuing sat in the success arm until the only
// place it could fire for these transactions was skipped, so the record stayed Locked and the
// child in the next level was answered TX_LOCKED.
func TestBlessMissingTransactions_QueuesAcceptedParentDespiteBlessError(t *testing.T) {
	ctx := context.Background()

	// the validator's own unlock fails, and the accepted record cannot be read back either
	server, store, txs, _ := newUnlockFailureServer(t, true, alwaysFail)
	server.settings.SubtreeValidation.SpendBatcherSize = 10

	parent := txs[1]
	server.utxoStore = &unreadableStore{Store: store.Store, only: parent.TxIDChainHash()}

	missing := []missingTx{{tx: parent, idx: 0}}
	txMetaSlice := make([]metaSliceItem, 1)

	err := server.blessMissingTransactions(ctx, chainhash.Hash{}, missing, "", "", txMetaSlice, 2, map[uint32]bool{})
	require.Error(t, err, "the bless still fails: the accepted metadata could not be read")
	require.False(t, txMetaSlice[0].isSet, "no metadata is recorded for a read that failed")

	stored := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, parent.TxIDChainHash(), stored))
	require.False(t, stored.Locked, "the accepted parent must still be spendable for the next level")
}
