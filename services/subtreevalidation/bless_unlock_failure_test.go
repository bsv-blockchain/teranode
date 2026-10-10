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
