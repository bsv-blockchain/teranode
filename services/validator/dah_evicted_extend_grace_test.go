package validator

// Tests for the extend/heights-step DAH-evicted-parent grace.
//
// The spend step has long tolerated a missing parent when the child itself is
// already mined and unflagged (see TestValidate_TxNotFoundShortcut). The
// extend/heights step used to hard-fail on the same condition, so a re-validated
// mined child whose parent had been DAH-evicted (or lost with a store partition)
// could never pass extension even though its stored metadata proves prior full
// validation. These tests pin the parity: the same dahEvictedParentGrace gate
// must short-circuit the extend step, and must NOT fire for unmined, conflicting
// or locked children.

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func makeExtendGraceTxAndParent(t *testing.T) (*bt.Tx, *bt.Tx) {
	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))
	coinbaseTx := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/Test miner/"),
		transactions.WithP2PKHOutputs(1, 50e8, publicKey),
	)
	tx := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 1000),
		transactions.WithChangeOutput(),
	)

	return tx, coinbaseTx
}

func setupExtendGraceValidator(t *testing.T, getMetaResult *meta.Data, getMetaErr error, onCurrentChain bool, chainErr error) (*Validator, *utxo.MockUtxostore) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	blockchainMock := &blockchain.Mock{}
	blockchainMock.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(onCurrentChain, chainErr).Maybe()

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, blockchainMock)
	require.NoError(t, err)

	v := validator.(*Validator)

	// The per-parent lookup during extension / input-height resolution fails:
	// the parent record is gone from the store.
	mockStore.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("parent gone"))
	mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000})

	if getMetaErr != nil {
		mockStore.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).Return(getMetaErr)
	} else {
		mockStore.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				data := args.Get(2).(*meta.Data)
				*data = *getMetaResult
			}).
			Return(nil)
	}

	return v, mockStore
}

func TestValidate_ExtendStepDAHEvictedParentGrace(t *testing.T) {
	t.Run("grace allowed when child mined and not flagged", func(t *testing.T) {
		tx, _ := makeExtendGraceTxAndParent(t)
		existingMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}, Conflicting: false, Locked: false}
		v, mockStore := setupExtendGraceValidator(t, existingMeta, nil, true, nil)

		txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.NoError(t, err)
		require.NotNil(t, txMetaData)
		require.Equal(t, []uint32{1, 2}, txMetaData.BlockIDs)
		// The grace must short-circuit BEFORE any spend is attempted: no Spend
		// expectation is registered, so a Spend call would fail the test.
		mockStore.AssertExpectations(t)
	})

	t.Run("no grace when child not mined", func(t *testing.T) {
		tx, _ := makeExtendGraceTxAndParent(t)
		existingMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{}}
		v, mockStore := setupExtendGraceValidator(t, existingMeta, nil, true, nil)

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		mockStore.AssertExpectations(t)
	})

	t.Run("no grace when child conflicting", func(t *testing.T) {
		tx, _ := makeExtendGraceTxAndParent(t)
		existingMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1}, Conflicting: true}
		v, mockStore := setupExtendGraceValidator(t, existingMeta, nil, true, nil)

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		mockStore.AssertExpectations(t)
	})

	t.Run("no grace when child locked", func(t *testing.T) {
		tx, _ := makeExtendGraceTxAndParent(t)
		existingMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1}, Locked: true}
		v, mockStore := setupExtendGraceValidator(t, existingMeta, nil, true, nil)

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		mockStore.AssertExpectations(t)
	})

	t.Run("no grace when child absent from store", func(t *testing.T) {
		tx, _ := makeExtendGraceTxAndParent(t)
		v, mockStore := setupExtendGraceValidator(t, nil, errors.NewTxNotFoundError("child not found"), true, nil)

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		mockStore.AssertExpectations(t)
	})

	t.Run("no grace when mined block lost a reorg (not on current chain)", func(t *testing.T) {
		tx, _ := makeExtendGraceTxAndParent(t)
		// BlockIDs alone are not proof of main-chain inclusion: a block that lost
		// a reorg keeps its stale ID on the tx meta.
		staleMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}}
		v, mockStore := setupExtendGraceValidator(t, staleMeta, nil, false, nil)

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		mockStore.AssertExpectations(t)
	})

	t.Run("chain-check fault aborts instead of reclassifying as missing parent", func(t *testing.T) {
		tx, _ := makeExtendGraceTxAndParent(t)
		minedMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}}
		v, mockStore := setupExtendGraceValidator(t, minedMeta, nil, false, errors.NewServiceError("blockchain unavailable"))

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		// A blockchain-service fault is not a verdict about the transaction: it
		// must surface as the fault itself, not as "invalid: missing parent"
		// (which would fail the enclosing block and feed the BLOCK_INCOMPLETE
		// cap on a transient infrastructure blip).
		require.False(t, errors.Is(err, errors.ErrTxMissingParent), "fault must not be reported as missing parent, got: %v", err)
		require.True(t, errors.Is(err, errors.ErrServiceError), "the blockchain fault must surface, got: %v", err)
		mockStore.AssertExpectations(t)
	})

	t.Run("store fault on meta read aborts instead of reclassifying", func(t *testing.T) {
		tx, _ := makeExtendGraceTxAndParent(t)
		v, mockStore := setupExtendGraceValidator(t, nil, errors.NewStorageError("aerospike timeout"), true, nil)

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrTxMissingParent), "fault must not be reported as missing parent, got: %v", err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "the store fault must surface, got: %v", err)
		mockStore.AssertExpectations(t)
	})

	t.Run("no grace without a blockchain client (fail closed)", func(t *testing.T) {
		tx, _ := makeExtendGraceTxAndParent(t)
		minedMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}}
		v, mockStore := setupExtendGraceValidator(t, minedMeta, nil, true, nil)
		v.blockchainClient = nil

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		mockStore.AssertExpectations(t)
	})
}

// TestExtendGraceChainCheckMemoised pins the per-block collapse of the
// current-chain check. The grace fires while re-validating a block whose
// transactions all carry the SAME BlockIDs, so an un-memoised check makes one
// blockchain round-trip per transaction — a load spike on the blockchain
// service at exactly the moment the node is degraded, and (since the
// BLOCK_INCOMPLETE cap now stays engaged when the FSM cannot be read) a
// self-reinforcing one.
func TestExtendGraceChainCheckMemoised(t *testing.T) {
	tx, _ := makeExtendGraceTxAndParent(t)
	existingMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{7, 9}, Conflicting: false, Locked: false}

	v, _ := setupExtendGraceValidator(t, existingMeta, nil, true, nil)

	blockchainMock := v.blockchainClient.(*blockchain.Mock)

	for i := 0; i < 5; i++ {
		txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})
		require.NoError(t, err)
		require.NotNil(t, txMetaData)
	}

	chainChecks := 0

	for _, call := range blockchainMock.Calls {
		if call.Method == "CheckBlockIsInCurrentChain" {
			chainChecks++
		}
	}

	require.Equal(t, 1, chainChecks, "the current-chain verdict must be computed once per block-ID set, not once per transaction")

	// Order must not defeat the memo: the same set in a different order is the
	// same question.
	require.Equal(t, blockIDsKey([]uint32{9, 7}), blockIDsKey([]uint32{7, 9}))
	require.NotEqual(t, blockIDsKey([]uint32{7, 9}), blockIDsKey([]uint32{7, 10}))
}

// TestExtendGraceChainCheckErrorsNotMemoised verifies a failed current-chain
// check is retried rather than cached: a transient blockchain fault must not
// pin a verdict for the whole memo window.
func TestExtendGraceChainCheckErrorsNotMemoised(t *testing.T) {
	tx, _ := makeExtendGraceTxAndParent(t)
	existingMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{11}, Conflicting: false, Locked: false}

	v, _ := setupExtendGraceValidator(t, existingMeta, nil, false, errors.NewServiceError("blockchain unavailable"))

	blockchainMock := v.blockchainClient.(*blockchain.Mock)

	for i := 0; i < 3; i++ {
		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})
		require.Error(t, err)
	}

	chainChecks := 0

	for _, call := range blockchainMock.Calls {
		if call.Method == "CheckBlockIsInCurrentChain" {
			chainChecks++
		}
	}

	require.Equal(t, 3, chainChecks, "a failed chain check must not be memoised")
}
