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
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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

func makeExtendGraceTx(t *testing.T) *bt.Tx {
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

	return tx
}

type extendGraceSetup struct {
	childMeta      *meta.Data
	childMetaErr   error
	onCurrentChain bool
	chainErr       error
	parentResult   []utxo.ParentOutput
	parentErr      error
}

func setupExtendGraceValidator(t *testing.T, tx *bt.Tx, cfg extendGraceSetup) (*Validator, *utxo.MockUtxostore, *blockchain.Mock) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	blockchainMock := &blockchain.Mock{}

	if cfg.childMeta != nil {
		blockchainMock.On("CheckBlockIsInCurrentChain", mock.Anything, cfg.childMeta.BlockIDs).Return(cfg.onCurrentChain, cfg.chainErr).Maybe()
	}

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, blockchainMock)
	require.NoError(t, err)

	v := validator.(*Validator)

	parentResult := cfg.parentResult
	if parentResult == nil && cfg.parentErr == nil {
		parentResult = []utxo.ParentOutput{{Status: utxo.ParentOutputTxNotFound}}
	}

	if parentResult != nil {
		mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentResult, nil)
	} else {
		mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(nil, cfg.parentErr)
	}

	mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000})

	// The grace must read the CHILD's record, never a parent's.
	childHash := mock.MatchedBy(func(h *chainhash.Hash) bool { return h.IsEqual(tx.TxIDChainHash()) })

	if cfg.childMetaErr != nil {
		mockStore.On("GetMeta", mock.Anything, childHash, mock.Anything).Return(cfg.childMetaErr)
	} else {
		mockStore.On("GetMeta", mock.Anything, childHash, mock.Anything).
			Run(func(args mock.Arguments) {
				data := args.Get(2).(*meta.Data)
				*data = *cfg.childMeta
			}).
			Return(nil)
	}

	return v, mockStore, blockchainMock
}

func TestValidate_ExtendStepDAHEvictedParentGrace(t *testing.T) {
	t.Run("grace allowed when child mined on current chain and not flagged", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		minedMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}}
		v, mockStore, chain := setupExtendGraceValidator(t, tx, extendGraceSetup{childMeta: minedMeta, onCurrentChain: true})

		txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.NoError(t, err)
		require.NotNil(t, txMetaData)
		require.Equal(t, []uint32{1, 2}, txMetaData.BlockIDs)
		// The verdict must come from the chain oracle, asked about the child's own BlockIDs.
		chain.AssertCalled(t, "CheckBlockIsInCurrentChain", mock.Anything, []uint32{1, 2})
		// The grace must short-circuit BEFORE any spend is attempted: no Spend
		// expectation is registered, so a Spend call would fail the test.
		mockStore.AssertExpectations(t)
	})

	for _, tc := range []struct {
		name string
		meta *meta.Data
	}{
		{"child not mined", &meta.Data{BlockIDs: []uint32{}}},
		{"child conflicting", &meta.Data{BlockIDs: []uint32{1}, Conflicting: true}},
		{"child locked", &meta.Data{BlockIDs: []uint32{1}, Locked: true}},
		{"child still creating", &meta.Data{BlockIDs: []uint32{1}, Creating: true}},
	} {
		t.Run("no grace when "+tc.name, func(t *testing.T) {
			tx := makeExtendGraceTx(t)
			tc.meta.Tx = tx
			v, mockStore, chain := setupExtendGraceValidator(t, tx, extendGraceSetup{childMeta: tc.meta, onCurrentChain: true})

			_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
			// The flags are checked before the blockchain round-trip.
			chain.AssertNotCalled(t, "CheckBlockIsInCurrentChain", mock.Anything, mock.Anything)
			mockStore.AssertExpectations(t)
		})
	}

	t.Run("no extend-step grace for a block-sourced tx", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		minedMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}}
		v, mockStore, chain := setupExtendGraceValidator(t, tx, extendGraceSetup{childMeta: minedMeta, onCurrentChain: true})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{InBlock: true})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
		chain.AssertNotCalled(t, "CheckBlockIsInCurrentChain", mock.Anything, mock.Anything)
	})

	t.Run("no grace when child absent from store", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		v, mockStore, chain := setupExtendGraceValidator(t, tx, extendGraceSetup{childMetaErr: errors.NewTxNotFoundError("child not found"), onCurrentChain: true})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		chain.AssertNotCalled(t, "CheckBlockIsInCurrentChain", mock.Anything, mock.Anything)
		mockStore.AssertExpectations(t)
	})

	t.Run("no grace when mined block lost a reorg (not on current chain)", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		// BlockIDs alone are not proof of main-chain inclusion: a block that lost
		// a reorg keeps its stale ID on the tx meta.
		staleMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}}
		v, mockStore, chain := setupExtendGraceValidator(t, tx, extendGraceSetup{childMeta: staleMeta, onCurrentChain: false})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		chain.AssertCalled(t, "CheckBlockIsInCurrentChain", mock.Anything, []uint32{1, 2})
		mockStore.AssertExpectations(t)
	})

	t.Run("finality is enforced before the grace can bless", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		// A height-based nLockTime above the validation height with a non-final
		// sequence: the record was final at the height it was mined at, not at 100.
		tx.LockTime = 500
		for _, input := range tx.Inputs {
			input.SequenceNumber = 0
		}

		minedMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}}
		v, mockStore, chain := setupExtendGraceValidator(t, tx, extendGraceSetup{childMeta: minedMeta, onCurrentChain: true})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNonFinal), "non-final error must surface, got: %v", err)
		mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
		chain.AssertNotCalled(t, "CheckBlockIsInCurrentChain", mock.Anything, mock.Anything)
	})

	t.Run("chain-check fault aborts as a node fault instead of missing parent", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		minedMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}}
		v, mockStore, _ := setupExtendGraceValidator(t, tx, extendGraceSetup{childMeta: minedMeta, chainErr: errors.NewServiceError("blockchain unavailable")})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrTxMissingParent), "fault must not be reported as missing parent, got: %v", err)
		// SERVICE_UNAVAILABLE is on the RPC layer's node-fault list; SERVICE_ERROR is not,
		// and would be reported to the submitter as a verdict on the transaction.
		require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "the fault must surface as a node-fault class, got: %v", err)
		mockStore.AssertExpectations(t)
	})

	t.Run("store fault on meta read aborts instead of reclassifying", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		v, mockStore, _ := setupExtendGraceValidator(t, tx, extendGraceSetup{childMetaErr: errors.NewStorageError("aerospike timeout")})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrTxMissingParent), "fault must not be reported as missing parent, got: %v", err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "the store fault must surface, got: %v", err)
		mockStore.AssertExpectations(t)
	})

	t.Run("no grace without a blockchain client (fail closed, no store read)", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		minedMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}}
		v, mockStore, _ := setupExtendGraceValidator(t, tx, extendGraceSetup{childMeta: minedMeta, onCurrentChain: true})
		v.blockchainClient = nil

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("a parent read fault that is not a missing parent never reaches the grace", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		minedMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}}
		v, mockStore, chain := setupExtendGraceValidator(t, tx, extendGraceSetup{childMeta: minedMeta, onCurrentChain: true, parentErr: errors.NewStorageError("bulk parent read failed")})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrTxMissingParent), "a read fault is not a missing parent, got: %v", err)
		mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
		chain.AssertNotCalled(t, "CheckBlockIsInCurrentChain", mock.Anything, mock.Anything)
	})

	t.Run("no grace for a mined child whose parent is present", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		minedMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}}
		v, mockStore, chain := setupExtendGraceValidator(t, tx, extendGraceSetup{childMeta: minedMeta, onCurrentChain: true, parentResult: []utxo.ParentOutput{{Status: utxo.ParentOutputNoSuchIndex}}})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrTxMissingParent), "a per-output verdict is not a missing parent, got: %v", err)
		mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
		chain.AssertNotCalled(t, "CheckBlockIsInCurrentChain", mock.Anything, mock.Anything)
	})
}

// A current-chain verdict must be refreshed for every grace decision: a reorg
// can orphan the block between two validations of the same transaction.
func TestExtendGraceChainCheckRecheckedAfterReorg(t *testing.T) {
	tx := makeExtendGraceTx(t)
	existingMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{7, 9}}

	v, _, _ := setupExtendGraceValidator(t, tx, extendGraceSetup{childMeta: existingMeta, onCurrentChain: true})
	chain := &blockchain.Mock{}
	chain.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7, 9}).Return(true, nil).Once()
	chain.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7, 9}).Return(false, nil).Once()
	v.blockchainClient = chain

	mined, err := v.validateInternal(context.Background(), tx, 100, &Options{})
	require.NoError(t, err)
	require.NotNil(t, mined)

	_, err = v.validateInternal(context.Background(), tx, 100, &Options{})
	require.ErrorIs(t, err, errors.ErrTxMissingParent)
	chain.AssertExpectations(t)
}

// TestExtendGraceChainCheckErrorsRetried verifies a failed current-chain
// check is retried: a transient blockchain fault must not pin a verdict.
func TestExtendGraceChainCheckErrorsRetried(t *testing.T) {
	tx := makeExtendGraceTx(t)
	existingMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{11}}

	v, _, chain := setupExtendGraceValidator(t, tx, extendGraceSetup{childMeta: existingMeta, chainErr: errors.NewServiceError("blockchain unavailable")})

	for i := 0; i < 3; i++ {
		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "the chain fault must surface on every attempt, got: %v", err)
	}

	chain.AssertNumberOfCalls(t, "CheckBlockIsInCurrentChain", 3)
}
