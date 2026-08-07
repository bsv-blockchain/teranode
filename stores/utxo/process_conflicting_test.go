package utxo

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestProcessConflicting_Success(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	// Create test transaction hash
	conflictingTxHash := createTestHash("conflicting-tx-1")
	conflictingTxHashes := []chainhash.Hash{conflictingTxHash}

	// Create test transaction
	testTx := createTestTransaction()

	// Mock Get call for winning transaction
	mockStore.On("Get", mock.Anything, &conflictingTxHash, mock.Anything).Return(&meta.Data{
		Tx:          testTx,
		Conflicting: true,
	}, nil)

	// Stub the counter-conflicting walk: the parent slot records the losing tx
	// as a live spender, so the walk result is {conflicting tx, losing tx}
	losingTxHash := createTestHash("losing-tx-1")
	stubCounterConflictingWalk(mockStore, testTx, losingTxHash)

	// Mock SetConflicting call for marking losing txs as conflicting
	affectedSpends := []*Spend{
		{TxID: &losingTxHash, Vout: 0},
	}
	mockStore.On("SetConflicting", mock.Anything, hashSetMatcher(conflictingTxHash, losingTxHash), true).
		Return(affectedSpends, []chainhash.Hash{}, nil)

	// Mock Unspend call
	mockStore.On("Unspend", mock.Anything, affectedSpends, mock.Anything).Return(nil)

	// Mock Spend call for winning transaction
	mockStore.On("Spend", mock.Anything, testTx, mock.Anything, mock.Anything).Return([]*Spend{}, nil)

	// Mock SetConflicting call for marking winning txs as not conflicting
	mockStore.On("SetConflicting", mock.Anything, conflictingTxHashes, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil)

	// Mock SetLocked call
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{losingTxHash}, false).Return(nil)

	// Execute test
	result, _, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]struct{}{})

	// Assertions
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.Exists(losingTxHash))
	mockStore.AssertExpectations(t)
}

func TestProcessConflicting_FrozenTxError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	// Use coinbase placeholder hash (frozen tx)
	conflictingTxHashes := []chainhash.Hash{subtree.CoinbasePlaceholderHashValue}

	// Execute test
	result, _, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]struct{}{})

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tx is frozen")
	mockStore.AssertExpectations(t)
}

func TestProcessConflicting_TxNotConflictingError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	conflictingTxHash := createTestHash("not-conflicting-tx")
	conflictingTxHashes := []chainhash.Hash{conflictingTxHash}

	// Mock Get call returning non-conflicting transaction
	mockStore.On("Get", mock.Anything, &conflictingTxHash, mock.Anything).Return(&meta.Data{
		Tx:          createTestTransaction(),
		Conflicting: false, // Not conflicting
	}, nil)

	// Execute test
	result, _, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]struct{}{})

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tx is not conflicting")
	mockStore.AssertExpectations(t)
}

func TestProcessConflicting_GetTxError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	conflictingTxHash := createTestHash("error-tx")
	conflictingTxHashes := []chainhash.Hash{conflictingTxHash}

	// Mock Get call returning error
	mockStore.On("Get", mock.Anything, &conflictingTxHash, mock.Anything).Return(nil, errors.NewProcessingError("database error"))

	// Execute test
	result, _, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]struct{}{})

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error getting tx")
	mockStore.AssertExpectations(t)
}

func TestProcessConflicting_GetCounterConflictingError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	conflictingTxHash := createTestHash("conflicting-tx")
	conflictingTxHashes := []chainhash.Hash{conflictingTxHash}

	// Mock Get call succeeding
	mockStore.On("Get", mock.Anything, &conflictingTxHash, mock.Anything).Return(&meta.Data{
		Tx:          createTestTransaction(),
		Conflicting: true,
	}, nil)

	// The walk fails reading the parent record: the error must surface as the
	// counter-conflicting failure
	prevTxHash := createTestHash("prev-tx")
	mockStore.On("Get", mock.Anything, &prevTxHash, []fields.FieldName{fields.Utxos, fields.DeletedChildren}).
		Return(nil, errors.NewProcessingError("counter conflicting error"))

	// Execute test
	result, _, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]struct{}{})

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error getting counter conflicting txs")
	mockStore.AssertExpectations(t)
}

func TestProcessConflicting_UnspendError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	conflictingTxHash := createTestHash("conflicting-tx")
	conflictingTxHashes := []chainhash.Hash{conflictingTxHash}
	losingTxHash := createTestHash("losing-tx")

	// Mock successful setup calls
	mockStore.On("Get", mock.Anything, &conflictingTxHash, mock.Anything).Return(&meta.Data{
		Tx:          createTestTransaction(),
		Conflicting: true,
	}, nil)

	stubCounterConflictingWalk(mockStore, createTestTransaction(), losingTxHash)

	affectedSpends := []*Spend{{TxID: &losingTxHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, hashSetMatcher(conflictingTxHash, losingTxHash), true).
		Return(affectedSpends, []chainhash.Hash{}, nil)

	// Mock Unspend call returning error
	mockStore.On("Unspend", mock.Anything, mock.Anything, mock.Anything).
		Return(errors.NewProcessingError("unspend failed"))

	// step 2 failed → rollback only undoes step 1 (clear conflicting flag).
	mockStore.On("SetConflicting", mock.Anything, hashSetMatcher(conflictingTxHash, losingTxHash), false).
		Return([]*Spend{}, []chainhash.Hash{}, nil)

	// Execute test
	result, _, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]struct{}{})

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error unspending affected parent spends")
	mockStore.AssertExpectations(t)
}

func TestProcessConflicting_SpendError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	conflictingTxHash := createTestHash("conflicting-tx")
	conflictingTxHashes := []chainhash.Hash{conflictingTxHash}
	losingTxHash := createTestHash("losing-tx")
	testTx := createTestTransaction()
	// Use a distinct fixture for the losing tx so the mock can differentiate the
	// step-3 Spend(winning) failure from the rollback Spend(losing) call.
	losingTx := createTestTransactionWithInputs(losingTxHash, 1)

	// Mock successful setup calls
	mockStore.On("Get", mock.Anything, &conflictingTxHash, mock.Anything).Return(&meta.Data{
		Tx:          testTx,
		Conflicting: true,
	}, nil).Times(3)

	stubCounterConflictingWalk(mockStore, testTx, losingTxHash)

	affectedSpends := []*Spend{{TxID: &losingTxHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, hashSetMatcher(conflictingTxHash, losingTxHash), true).
		Return(affectedSpends, []chainhash.Hash{}, nil)

	mockStore.On("Unspend", mock.Anything, affectedSpends, mock.Anything).Return(nil)

	// Mock Spend call returning error with spends that have errors
	spendWithError := &Spend{
		TxID: &losingTxHash,
		Vout: 0,
		Err:  errors.NewProcessingError("spend error"),
	}
	mockStore.On("Spend", mock.Anything, testTx, mock.Anything, mock.Anything).
		Return([]*Spend{spendWithError}, errors.NewTxInvalidError("spend failed")).Once()
	// rollback re-spends the winning tx too (it is part of the marked cascade)
	mockStore.On("Spend", mock.Anything, testTx, mock.Anything, mock.Anything).
		Return([]*Spend{}, nil).Once()

	// step-3 failure rollback path: re-fetch losing tx body, re-spend it, clear conflicting,
	// unlock parents. (No partial successful step-3 spends — the only spend has Err != nil.)
	mockStore.On("Get", mock.Anything, &losingTxHash, mock.Anything).Return(&meta.Data{
		Tx: losingTx,
	}, nil).Once()
	mockStore.On("Spend", mock.Anything, losingTx, mock.Anything, mock.Anything).
		Return([]*Spend{}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, hashSetMatcher(conflictingTxHash, losingTxHash), false).
		Return([]*Spend{}, []chainhash.Hash{}, nil)
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{losingTxHash}, false).Return(nil)

	// Execute test
	result, _, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]struct{}{})

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "spend failed")
	mockStore.AssertExpectations(t)
}

func TestMarkConflictingRecursively_Success(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")
	childHash := createTestHash("child-tx")

	// Mock SetConflicting returning affected spends and child transactions
	affectedSpends := []*Spend{{TxID: &txHash, Vout: 0}}
	childTxs := []chainhash.Hash{childHash}

	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{txHash}, true).
		Return(affectedSpends, childTxs, nil)

	// the cascade probes each discovered child before recursing (ghost filter);
	// the child is a live record, so the probe finds it
	mockStore.On("Get", mock.Anything, &childHash, []fields.FieldName{fields.Conflicting}).
		Return(&meta.Data{}, nil)

	// Mock recursive call for child transactions
	childAffectedSpends := []*Spend{{TxID: &childHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, childTxs, true).
		Return(childAffectedSpends, []chainhash.Hash{}, nil)

	// Execute test
	result, markedHashes, err := MarkConflictingRecursively(ctx, mockStore, []chainhash.Hash{txHash})

	// Assertions
	require.NoError(t, err)
	assert.Len(t, result, 2) // Should contain both parent and child spends
	assert.Equal(t, []chainhash.Hash{txHash, childHash}, markedHashes,
		"marked set must be returned in BFS order: input first, then cascaded child")
	mockStore.AssertExpectations(t)
}

func TestMarkConflictingRecursively_SetConflictingError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")

	// Mock SetConflicting returning error
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{txHash}, true).
		Return([]*Spend{}, []chainhash.Hash{}, errors.NewProcessingError("set conflicting error"))

	// Execute test
	result, markedHashes, err := MarkConflictingRecursively(ctx, mockStore, []chainhash.Hash{txHash})

	// Assertions
	assert.Nil(t, result)
	assert.Nil(t, markedHashes)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "set conflicting error")
	mockStore.AssertExpectations(t)
}

func TestMarkConflictingRecursively_NoChildren(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")

	// Mock SetConflicting returning no child transactions
	affectedSpends := []*Spend{{TxID: &txHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{txHash}, true).
		Return(affectedSpends, []chainhash.Hash{}, nil)

	// Execute test
	result, markedHashes, err := MarkConflictingRecursively(ctx, mockStore, []chainhash.Hash{txHash})

	// Assertions
	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, txHash, *result[0].TxID)
	assert.Equal(t, []chainhash.Hash{txHash}, markedHashes)
	mockStore.AssertExpectations(t)
}

func TestGetAndLockChildren_Success(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("parent-tx")
	childHash := createTestHash("child-tx")
	grandChildHash := createTestHash("grandchild-tx")

	// Mock SetLocked call
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{parentHash}, true).Return(nil)

	// Mock Get call for parent transaction
	spendingData := &spend.SpendingData{TxID: &childHash}
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).Return(&meta.Data{
		SpendingDatas: []*spend.SpendingData{spendingData},
	}, nil)

	// Mock recursive call for child
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{childHash}, true).Return(nil)
	mockStore.On("Get", mock.Anything, &childHash, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{{TxID: &grandChildHash}},
		}, nil)

	// Mock recursive call for grandchild (no children)
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{grandChildHash}, true).Return(nil)
	mockStore.On("Get", mock.Anything, &grandChildHash, mock.Anything).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{}}, nil)

	// Execute test
	result, err := GetAndLockChildren(ctx, mockStore, parentHash)

	// Assertions
	require.NoError(t, err)
	assert.Contains(t, result, childHash)
	assert.Contains(t, result, grandChildHash)
	mockStore.AssertExpectations(t)
}

func TestGetAndLockChildren_FrozenTx(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	// Use coinbase placeholder hash (frozen tx)
	frozenHash := subtree.CoinbasePlaceholderHashValue

	// Execute test
	result, err := GetAndLockChildren(ctx, mockStore, frozenHash)

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tx is frozen")
	mockStore.AssertExpectations(t)
}

func TestGetAndLockChildren_SetLockedError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")

	// Mock SetLocked call returning error
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{txHash}, true).
		Return(errors.NewProcessingError("lock error"))

	// Execute test
	result, err := GetAndLockChildren(ctx, mockStore, txHash)

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lock error")
	mockStore.AssertExpectations(t)
}

func TestGetAndLockChildren_GetError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")

	// Mock successful SetLocked call
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{txHash}, true).Return(nil)

	// Mock Get call returning error
	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Return(nil, errors.NewProcessingError("get error"))

	// Execute test
	result, err := GetAndLockChildren(ctx, mockStore, txHash)

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get error")
	mockStore.AssertExpectations(t)
}

func TestGetAndLockChildren_NilSpendingData(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")

	// Mock SetLocked call
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{txHash}, true).Return(nil)

	// Mock Get call returning nil spending data
	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{nil}, // nil spending data
		}, nil)

	// Execute test
	result, err := GetAndLockChildren(ctx, mockStore, txHash)

	// Assertions
	require.NoError(t, err)
	assert.Empty(t, result) // Should be empty since spending data is nil
	mockStore.AssertExpectations(t)
}

func TestGetAndLockChildren_CycleDetection(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("parent-tx")
	childHash := createTestHash("child-tx")

	// Mock SetLocked calls for both nodes
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{parentHash}, true).Return(nil)
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{childHash}, true).Return(nil)

	// Parent points to child
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).Return(&meta.Data{
		SpendingDatas: []*spend.SpendingData{{TxID: &childHash}},
	}, nil)

	// Child points back to parent (cycle) — should be skipped by visited set
	mockStore.On("Get", mock.Anything, &childHash, mock.Anything).Return(&meta.Data{
		SpendingDatas: []*spend.SpendingData{{TxID: &parentHash}},
	}, nil)

	// Execute test — should complete without infinite loop
	result, err := GetAndLockChildren(ctx, mockStore, parentHash)

	// Assertions
	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Contains(t, result, childHash)
	mockStore.AssertExpectations(t)
}

func TestGetAndLockChildren_DiamondGraph(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	root := createTestHash("root")
	left := createTestHash("left")
	right := createTestHash("right")
	bottom := createTestHash("bottom")

	// root -> left, right
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{root}, true).Return(nil)
	mockStore.On("Get", mock.Anything, &root, mock.Anything).Return(&meta.Data{
		SpendingDatas: []*spend.SpendingData{{TxID: &left}, {TxID: &right}},
	}, nil)

	// left -> bottom
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{left}, true).Return(nil)
	mockStore.On("Get", mock.Anything, &left, mock.Anything).Return(&meta.Data{
		SpendingDatas: []*spend.SpendingData{{TxID: &bottom}},
	}, nil)

	// right -> bottom (convergent path — bottom should only be visited once)
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{right}, true).Return(nil)
	mockStore.On("Get", mock.Anything, &right, mock.Anything).Return(&meta.Data{
		SpendingDatas: []*spend.SpendingData{{TxID: &bottom}},
	}, nil)

	// bottom has no children
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{bottom}, true).Return(nil)
	mockStore.On("Get", mock.Anything, &bottom, mock.Anything).Return(&meta.Data{
		SpendingDatas: []*spend.SpendingData{},
	}, nil)

	// Execute test
	result, err := GetAndLockChildren(ctx, mockStore, root)

	// Assertions
	require.NoError(t, err)
	assert.Len(t, result, 3)
	assert.Contains(t, result, left)
	assert.Contains(t, result, right)
	assert.Contains(t, result, bottom)
	mockStore.AssertExpectations(t)
}

func TestGetConflictingChildren_Success(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("parent-tx")
	childHash := createTestHash("child-tx")
	grandChildHash := createTestHash("grandchild-tx")

	// Mock Get call for parent transaction
	spendingData := &spend.SpendingData{TxID: &childHash}
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything, mock.Anything).
		Return(&meta.Data{
			SpendingDatas:       []*spend.SpendingData{spendingData},
			ConflictingChildren: []chainhash.Hash{grandChildHash},
		}, nil)

	// Mock Get calls for child and grandchild (iterative BFS fetches each directly)
	mockStore.On("Get", mock.Anything, &childHash, mock.Anything, mock.Anything).
		Return(&meta.Data{}, nil)
	mockStore.On("Get", mock.Anything, &grandChildHash, mock.Anything, mock.Anything).
		Return(&meta.Data{}, nil)

	// Execute test
	result, err := GetConflictingChildren(ctx, mockStore, parentHash)

	// Assertions
	require.NoError(t, err)
	assert.Contains(t, result, childHash)
	assert.Contains(t, result, grandChildHash)
	mockStore.AssertExpectations(t)
}

func TestGetConflictingChildren_CoinbasePlaceholder(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	// Use coinbase placeholder hash
	coinbaseHash := subtree.CoinbasePlaceholderHashValue

	// Execute test
	result, err := GetConflictingChildren(ctx, mockStore, coinbaseHash)

	// Assertions
	require.NoError(t, err)
	assert.Empty(t, result) // Should return empty slice for coinbase placeholder
	mockStore.AssertExpectations(t)
}

func TestGetConflictingChildren_GetError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")

	// Mock Get call returning error
	mockStore.On("Get", mock.Anything, &txHash, mock.Anything, mock.Anything).
		Return(nil, errors.NewProcessingError("get error"))

	// Execute test
	result, err := GetConflictingChildren(ctx, mockStore, txHash)

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get error")
	mockStore.AssertExpectations(t)
}

func TestGetConflictingChildren_ChildGetError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("parent-tx")
	childHash := createTestHash("child-tx")

	// Mock Get call for parent transaction
	spendingData := &spend.SpendingData{TxID: &childHash}
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{spendingData},
		}, nil)

	// Mock Get call for child returning error
	mockStore.On("Get", mock.Anything, &childHash, mock.Anything, mock.Anything).
		Return(nil, errors.NewProcessingError("child get error"))

	// Execute test
	result, err := GetConflictingChildren(ctx, mockStore, parentHash)

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "child get error")
	mockStore.AssertExpectations(t)
}

func TestGetConflictingChildren_CycleDetection(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	parentHash := createTestHash("parent-tx")
	childHash := createTestHash("child-tx")

	// Parent points to child
	spendingData := &spend.SpendingData{TxID: &childHash}
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{spendingData},
		}, nil)

	// Child points back to parent (cycle)
	mockStore.On("Get", mock.Anything, &childHash, mock.Anything, mock.Anything).
		Return(&meta.Data{
			ConflictingChildren: []chainhash.Hash{parentHash},
		}, nil)

	// Execute test - should complete without infinite loop
	result, err := GetConflictingChildren(ctx, mockStore, parentHash)

	// Assertions
	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Contains(t, result, childHash)
	mockStore.AssertExpectations(t)
}

func TestGetCounterConflictingTxHashes_Success(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")
	parentTxHash := createTestHash("parent-tx")
	conflictingTxHash := createTestHash("conflicting-tx")
	childTxHash := createTestHash("child-tx")

	// Create test transaction with inputs
	testTx := createTestTransactionWithInputs(parentTxHash, 0)

	// Mock Get call for main transaction
	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Return(&meta.Data{Tx: testTx}, nil)

	// Mock Get call for parent transaction
	spendingData := &spend.SpendingData{TxID: &conflictingTxHash}
	mockStore.On("Get", mock.Anything, &parentTxHash, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{spendingData},
		}, nil)

	// Mock GetConflictingChildren call
	mockStore.On("GetConflictingChildren", mock.Anything, conflictingTxHash).
		Return([]chainhash.Hash{childTxHash}, nil)

	// Execute test
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash)

	// Assertions
	require.NoError(t, err)
	assert.Contains(t, result, txHash)            // Should include original tx
	assert.Contains(t, result, conflictingTxHash) // Should include conflicting tx
	assert.Contains(t, result, childTxHash)       // Should include child
	mockStore.AssertExpectations(t)
}

func TestGetCounterConflictingTxHashes_GetTxError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")

	// Mock Get call returning error
	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Return(nil, errors.NewProcessingError("get tx error"))

	// Execute test
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash)

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get tx error")
	mockStore.AssertExpectations(t)
}

func TestGetCounterConflictingTxHashes_GetParentError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")
	parentTxHash := createTestHash("parent-tx")

	// Create test transaction with inputs
	testTx := createTestTransactionWithInputs(parentTxHash, 0)

	// Mock Get call for main transaction
	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Return(&meta.Data{Tx: testTx}, nil)

	// Mock Get call for parent transaction returning error
	mockStore.On("Get", mock.Anything, &parentTxHash, mock.Anything).
		Return(nil, errors.NewProcessingError("get parent error"))

	// Execute test
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash)

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get parent error")
	mockStore.AssertExpectations(t)
}

func TestGetCounterConflictingTxHashes_OutOfRangeError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")
	parentTxHash := createTestHash("parent-tx")

	// Create test transaction with input that's out of range
	testTx := createTestTransactionWithInputs(parentTxHash, 5) // Index 5, but parent only has 1 output

	// Mock Get call for main transaction
	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Return(&meta.Data{Tx: testTx}, nil)

	// Mock Get call for parent transaction with only 1 spending data (index 0)
	spendingData := &spend.SpendingData{TxID: &txHash}
	mockStore.On("Get", mock.Anything, &parentTxHash, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{spendingData}, // Only index 0
		}, nil)

	// Execute test
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash)

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "input 5 of")
	assert.Contains(t, err.Error(), "is out of range")
	mockStore.AssertExpectations(t)
}

func TestGetCounterConflictingTxHashes_FrozenChildError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")
	parentTxHash := createTestHash("parent-tx")
	conflictingTxHash := createTestHash("conflicting-tx")

	// Create test transaction with inputs
	testTx := createTestTransactionWithInputs(parentTxHash, 0)

	// Mock Get call for main transaction
	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Return(&meta.Data{Tx: testTx}, nil)

	// Mock Get call for parent transaction
	spendingData := &spend.SpendingData{TxID: &conflictingTxHash}
	mockStore.On("Get", mock.Anything, &parentTxHash, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{spendingData},
		}, nil)

	// Mock GetConflictingChildren call returning frozen child
	mockStore.On("GetConflictingChildren", mock.Anything, conflictingTxHash).
		Return([]chainhash.Hash{subtree.FrozenBytesTxHash}, nil)

	// Execute test
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash)

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tx has frozen child")
	mockStore.AssertExpectations(t)
}

func TestGetCounterConflictingTxHashes_GetConflictingChildrenError(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")
	parentTxHash := createTestHash("parent-tx")
	conflictingTxHash := createTestHash("conflicting-tx")

	// Create test transaction with inputs
	testTx := createTestTransactionWithInputs(parentTxHash, 0)

	// Mock Get call for main transaction
	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Return(&meta.Data{Tx: testTx}, nil)

	// Mock Get call for parent transaction
	spendingData := &spend.SpendingData{TxID: &conflictingTxHash}
	mockStore.On("Get", mock.Anything, &parentTxHash, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{spendingData},
		}, nil)

	// Mock GetConflictingChildren call returning error
	mockStore.On("GetConflictingChildren", mock.Anything, conflictingTxHash).
		Return([]chainhash.Hash{}, errors.NewProcessingError("get conflicting children error"))

	// Execute test
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash)

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get conflicting children error")
	mockStore.AssertExpectations(t)
}

func TestGetCounterConflictingTxHashes_NilSpendingData(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")
	parentTxHash := createTestHash("parent-tx")

	// Create test transaction with inputs
	testTx := createTestTransactionWithInputs(parentTxHash, 0)

	// Mock Get call for main transaction
	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Return(&meta.Data{Tx: testTx}, nil)

	// Mock Get call for parent transaction with nil spending data
	mockStore.On("Get", mock.Anything, &parentTxHash, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{nil}, // nil spending data
		}, nil)

	// Execute test
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash)

	// Assertions
	require.NoError(t, err)
	assert.Contains(t, result, txHash) // Should only contain the original tx
	assert.Len(t, result, 1)
	mockStore.AssertExpectations(t)
}

// Helper functions for creating test data

func createTestHash(seed string) chainhash.Hash {
	hash := chainhash.HashH([]byte(seed))
	return hash
}

func createTestTransaction() *bt.Tx {
	tx := bt.NewTx()
	// Add a test input
	input := &bt.Input{
		PreviousTxOutIndex: 0,
	}
	prevTxHash := createTestHash("prev-tx")
	_ = input.PreviousTxIDAdd(&prevTxHash)
	tx.Inputs = append(tx.Inputs, input)

	// Add a test output
	output := &bt.Output{
		Satoshis: 1000,
	}
	tx.Outputs = append(tx.Outputs, output)

	return tx
}

func createTestTransactionWithInputs(parentTxHash chainhash.Hash, inputIndex uint32) *bt.Tx {
	tx := bt.NewTx()

	// Add input with specific parent and index
	input := &bt.Input{
		PreviousTxOutIndex: inputIndex,
	}
	_ = input.PreviousTxIDAdd(&parentTxHash)
	tx.Inputs = append(tx.Inputs, input)

	// Add a test output
	output := &bt.Output{
		Satoshis: 1000,
	}
	tx.Outputs = append(tx.Outputs, output)

	return tx
}

// ghostSpendsToleratedCounterValue reads the current value of the
// teranode_utxo_counter_conflicting_ghost_spends counter from the default
// prometheus registry, returning 0 when the metric is not registered yet.
func ghostSpendsToleratedCounterValue(t *testing.T) float64 {
	t.Helper()

	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() == "teranode_utxo_counter_conflicting_ghost_spends" {
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}

	return 0
}

// A parent output records a spender whose own record no longer exists (a
// "ghost", e.g. a never-mined conflicting loser purged by the reaper while the
// parent still references it). The walk must probe the spender itself and,
// once confirmed absent, exclude it from the counter-conflicting set instead
// of failing the whole walk with TX_NOT_FOUND.
func TestGetCounterConflictingTxHashes_ToleratesConfirmedGhostSpender(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	winningTx := createTestTransaction()
	winningTxHash := createTestHash("winning-tx")
	parentTxHash := *winningTx.Inputs[0].PreviousTxIDChainHash()
	ghostTxHash := createTestHash("ghost-spender-tx")

	parentTx := createTestParentTransaction()

	mockStore.On("Get", mock.Anything, &winningTxHash, []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: winningTx}, nil)
	mockStore.On("Get", mock.Anything, &parentTxHash, []fields.FieldName{fields.Utxos, fields.DeletedChildren}).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&ghostTxHash, 0)}}, nil)
	mockStore.On("GetConflictingChildren", mock.Anything, ghostTxHash).
		Return(([]chainhash.Hash)(nil), errors.NewTxNotFoundError("%v not found", ghostTxHash))
	// the probe confirms the spender record itself is gone
	mockStore.On("Get", mock.Anything, &ghostTxHash, []fields.FieldName{fields.Conflicting}).
		Return(nil, errors.NewTxNotFoundError("%v not found", ghostTxHash))
	// the ghost-slot build re-reads the parent with its tx body for the utxo hash
	mockStore.On("Get", mock.Anything, &parentTxHash, []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: parentTx}, nil)

	counterBefore := ghostSpendsToleratedCounterValue(t)

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, winningTxHash)

	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{winningTxHash}, result)
	require.Equal(t, counterBefore+1, ghostSpendsToleratedCounterValue(t))
	mockStore.AssertExpectations(t)
}

// An absent spender record is not necessarily a ghost: DAH housekeeping reaps
// mined, fully-spent records too, and marks each reaped child in its surviving
// parents' deletedChildren map. A spender confirmed absent by the probe but
// listed there held a settled, mined spend — tolerating it would clear the
// slot and bless a block that double-spends a settled output. The walk must
// fail closed instead.
func TestGetCounterConflictingTxHashes_FailsClosedOnReapedMinedSpender(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	winningTx := createTestTransaction()
	winningTxHash := createTestHash("winning-tx")
	parentTxHash := *winningTx.Inputs[0].PreviousTxIDChainHash()
	reapedTxHash := createTestHash("reaped-mined-spender-tx")

	mockStore.On("Get", mock.Anything, &winningTxHash, []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: winningTx}, nil)
	mockStore.On("Get", mock.Anything, &parentTxHash, []fields.FieldName{fields.Utxos, fields.DeletedChildren}).
		Return(&meta.Data{
			SpendingDatas:   []*spend.SpendingData{spend.NewSpendingData(&reapedTxHash, 0)},
			DeletedChildren: map[chainhash.Hash]bool{reapedTxHash: true},
		}, nil)
	mockStore.On("GetConflictingChildren", mock.Anything, reapedTxHash).
		Return(([]chainhash.Hash)(nil), errors.NewTxNotFoundError("%v not found", reapedTxHash))
	// the probe confirms the record is gone — but the parent says it was reaped,
	// not never-created
	mockStore.On("Get", mock.Anything, &reapedTxHash, []fields.FieldName{fields.Conflicting}).
		Return(nil, errors.NewTxNotFoundError("%v not found", reapedTxHash))

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, winningTxHash)

	require.Error(t, err)
	require.Nil(t, result)
	mockStore.AssertExpectations(t)
}

// A ghost can also sit one level deeper: a live conflicting loser whose output
// slot records a spender that has no record (spends applied, record never
// created). The BFS must skip such a confirmed-absent descendant instead of
// failing the whole walk with NOT_FOUND — which wedged block validation just
// like the depth-1 case.
func TestGetConflictingChildren_ToleratesGhostDescendant(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	rootHash := createTestHash("live-loser-root")
	ghostChildHash := createTestHash("ghost-descendant")

	bfsFields := []fields.FieldName{fields.Utxos, fields.ConflictingChildren, fields.DeletedChildren}

	mockStore.On("Get", mock.Anything, &rootHash, bfsFields).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&ghostChildHash, 0)}}, nil)
	mockStore.On("Get", mock.Anything, &ghostChildHash, bfsFields).
		Return(nil, errors.NewTxNotFoundError("%v not found", ghostChildHash))

	children, err := GetConflictingChildren(ctx, mockStore, rootHash)

	require.NoError(t, err)
	require.Empty(t, children, "the ghost descendant must not appear in the result")
	mockStore.AssertExpectations(t)
}

// An absent descendant listed in its parent's deletedChildren was reaped after
// being mined — the subtree holds settled history and must not be treated as
// demotable. The BFS fails closed, mirroring the counter walk's reaped gate.
func TestGetConflictingChildren_FailsClosedOnReapedDescendant(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	rootHash := createTestHash("live-loser-root")
	reapedChildHash := createTestHash("reaped-descendant")

	bfsFields := []fields.FieldName{fields.Utxos, fields.ConflictingChildren, fields.DeletedChildren}

	mockStore.On("Get", mock.Anything, &rootHash, bfsFields).
		Return(&meta.Data{
			SpendingDatas:   []*spend.SpendingData{spend.NewSpendingData(&reapedChildHash, 0)},
			DeletedChildren: map[chainhash.Hash]bool{reapedChildHash: true},
		}, nil)
	mockStore.On("Get", mock.Anything, &reapedChildHash, bfsFields).
		Return(nil, errors.NewTxNotFoundError("%v not found", reapedChildHash))

	children, err := GetConflictingChildren(ctx, mockStore, rootHash)

	require.Error(t, err)
	require.Nil(t, children)
	mockStore.AssertExpectations(t)
}

// A frozen output records the record-less frozen/coinbase-placeholder sentinel
// as its spender. The BFS must surface that sentinel in the result set (so the
// caller's frozen-child check fires) WITHOUT recursing into it — recursing
// would Get a NOT_FOUND record that the ghost tolerance then silently swallows,
// defeating frozen-tx rejection during conflict resolution. The absence of any
// Get stub for the sentinel proves it is never dereferenced: a recursion would
// panic the mock as an unexpected call.
func TestGetConflictingChildren_SurfacesFrozenSentinelWithoutRecursing(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	rootHash := createTestHash("frozen-loser-root")
	frozenSentinel := subtree.CoinbasePlaceholderHashValue

	bfsFields := []fields.FieldName{fields.Utxos, fields.ConflictingChildren, fields.DeletedChildren}

	// the live loser's frozen output points at the sentinel
	mockStore.On("Get", mock.Anything, &rootHash, bfsFields).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&frozenSentinel, 0)}}, nil)
	// deliberately NO stub for Get(frozenSentinel, ...): it must not be recursed

	children, err := GetConflictingChildren(ctx, mockStore, rootHash)

	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{frozenSentinel}, children,
		"the frozen sentinel must be returned so the caller's frozen check can fire")
	mockStore.AssertExpectations(t)
}

// The cascade discovers children from the SpendingDatas of the records it just
// marked — which can include a ghost (spends applied, record never created).
// Feeding the ghost back into SetConflicting would fail NOT_FOUND and wedge
// the cascade; it must be probed and skipped instead.
func TestMarkConflictingRecursively_SkipsGhostChild(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	loserHash := createTestHash("live-loser")
	ghostChildHash := createTestHash("ghost-spending-child")

	loserSpends := []*Spend{{TxID: &loserHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{loserHash}, true).
		Return(loserSpends, []chainhash.Hash{ghostChildHash}, nil)
	// the probe of the discovered child finds no record — a ghost
	mockStore.On("Get", mock.Anything, &ghostChildHash, []fields.FieldName{fields.Conflicting}).
		Return(nil, errors.NewTxNotFoundError("%v not found", ghostChildHash))

	spends, markedHashes, err := MarkConflictingRecursively(ctx, mockStore, []chainhash.Hash{loserHash})

	require.NoError(t, err)
	require.Equal(t, loserSpends, spends)
	require.Equal(t, []chainhash.Hash{loserHash}, markedHashes,
		"the ghost child was never marked and must not appear in the marked set")
	mockStore.AssertExpectations(t)
}

// A NOT_FOUND out of the BFS does not prove the spender is absent — the walk
// may have failed on a missing descendant while the spender itself is alive
// (and possibly mined on our chain). The probe must detect the live spender
// and fail closed by propagating the original error.
func TestGetCounterConflictingTxHashes_FailsClosedWhenSpenderAliveAndDescendantMissing(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	winningTx := createTestTransaction()
	winningTxHash := createTestHash("winning-tx")
	parentTxHash := *winningTx.Inputs[0].PreviousTxIDChainHash()
	spenderTxHash := createTestHash("live-spender-tx")

	mockStore.On("Get", mock.Anything, &winningTxHash, []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: winningTx}, nil)
	mockStore.On("Get", mock.Anything, &parentTxHash, []fields.FieldName{fields.Utxos, fields.DeletedChildren}).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&spenderTxHash, 0)}}, nil)
	// the BFS fails on a missing descendant of the (live) spender
	mockStore.On("GetConflictingChildren", mock.Anything, spenderTxHash).
		Return(([]chainhash.Hash)(nil), errors.NewTxNotFoundError("descendant not found"))
	// the probe finds the spender record alive
	mockStore.On("Get", mock.Anything, &spenderTxHash, []fields.FieldName{fields.Conflicting}).
		Return(&meta.Data{}, nil)

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, winningTxHash)

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
	require.Nil(t, result)
	mockStore.AssertExpectations(t)
}

// When the probe itself fails with anything other than NOT_FOUND, the ghost is
// unconfirmed and the walk must fail closed by propagating the original error.
func TestGetCounterConflictingTxHashes_FailsClosedWhenProbeErrors(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	winningTx := createTestTransaction()
	winningTxHash := createTestHash("winning-tx")
	parentTxHash := *winningTx.Inputs[0].PreviousTxIDChainHash()
	spenderTxHash := createTestHash("maybe-ghost-tx")

	mockStore.On("Get", mock.Anything, &winningTxHash, []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: winningTx}, nil)
	mockStore.On("Get", mock.Anything, &parentTxHash, []fields.FieldName{fields.Utxos, fields.DeletedChildren}).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&spenderTxHash, 0)}}, nil)
	mockStore.On("GetConflictingChildren", mock.Anything, spenderTxHash).
		Return(([]chainhash.Hash)(nil), errors.NewTxNotFoundError("%v not found", spenderTxHash))
	mockStore.On("Get", mock.Anything, &spenderTxHash, []fields.FieldName{fields.Conflicting}).
		Return(nil, errors.NewStorageError("backend unavailable"))

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, winningTxHash)

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
	require.Nil(t, result)
	mockStore.AssertExpectations(t)
}

// A missing parent record erases the whole spend graph for that input; the
// walk must keep failing closed rather than dereference a missing slot.
func TestGetCounterConflictingTxHashes_FailsClosedOnMissingParent(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	winningTx := createTestTransaction()
	winningTxHash := createTestHash("winning-tx")
	parentTxHash := *winningTx.Inputs[0].PreviousTxIDChainHash()

	mockStore.On("Get", mock.Anything, &winningTxHash, []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: winningTx}, nil)
	mockStore.On("Get", mock.Anything, &parentTxHash, []fields.FieldName{fields.Utxos, fields.DeletedChildren}).
		Return(nil, errors.NewTxNotFoundError("%v not found", parentTxHash))

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, winningTxHash)

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
	require.Nil(t, result)
	mockStore.AssertExpectations(t)
}

// Resolution over a confirmed ghost must not only tolerate the ghost in the
// walk: the parent slot still records the ghost's spend, which no loser-derived
// unspend can clear (ownership check). ProcessConflicting must clear that slot
// explicitly — passing the recorded ghost spending data as the expected value —
// so the winner's spend in step 3 can succeed and the dangling ref is healed.
func TestProcessConflicting_HealsConfirmedGhostSlot(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	winningTx := createTestTransaction()
	winningTxHash := createTestHash("winning-tx")
	parentTxHash := *winningTx.Inputs[0].PreviousTxIDChainHash()
	ghostTxHash := createTestHash("ghost-spender-tx")

	parentTx := createTestParentTransaction()
	ghostSpendingData := spend.NewSpendingData(&ghostTxHash, 0)

	expectedUtxoHash, err := util.UTXOHash(&parentTxHash, 0, parentTx.Outputs[0].LockingScript, parentTx.Outputs[0].Satoshis)
	require.NoError(t, err)

	mockStore.On("Get", mock.Anything, &winningTxHash, []fields.FieldName{fields.Tx, fields.BlockIDs, fields.Conflicting}).
		Return(&meta.Data{Tx: winningTx, Conflicting: true}, nil)
	mockStore.On("Get", mock.Anything, &winningTxHash, []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: winningTx}, nil)
	mockStore.On("Get", mock.Anything, &parentTxHash, []fields.FieldName{fields.Utxos, fields.DeletedChildren}).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{ghostSpendingData}}, nil)
	mockStore.On("GetConflictingChildren", mock.Anything, ghostTxHash).
		Return(([]chainhash.Hash)(nil), errors.NewTxNotFoundError("%v not found", ghostTxHash))
	mockStore.On("Get", mock.Anything, &ghostTxHash, []fields.FieldName{fields.Conflicting}).
		Return(nil, errors.NewTxNotFoundError("%v not found", ghostTxHash))
	mockStore.On("Get", mock.Anything, &parentTxHash, []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: parentTx}, nil)

	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{winningTxHash}, true).
		Return([]*Spend{}, []chainhash.Hash{}, nil)

	// step 2 must include the ghost slot, with the recorded ghost spend as the
	// expected spending data so the store's ownership check lets it clear
	mockStore.On("Unspend", mock.Anything, mock.MatchedBy(func(spends []*Spend) bool {
		if len(spends) != 1 {
			return false
		}

		sp := spends[0]

		return sp.TxID.Equal(parentTxHash) && sp.Vout == 0 &&
			sp.UTXOHash.Equal(*expectedUtxoHash) &&
			sp.SpendingData != nil && sp.SpendingData.TxID.Equal(ghostTxHash)
	}), mock.Anything).Return(nil)

	mockStore.On("Spend", mock.Anything, winningTx, mock.Anything, mock.Anything).
		Return([]*Spend{}, nil)
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{winningTxHash}, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil)
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{parentTxHash}, false).
		Return(nil)

	losingMap, _, err := ProcessConflicting(ctx, mockStore, 1, []chainhash.Hash{winningTxHash}, map[chainhash.Hash]struct{}{})

	require.NoError(t, err)
	require.NotNil(t, losingMap)
	mockStore.AssertExpectations(t)
}

// createTestParentTransaction builds a parent tx with a spendable output so
// the ghost-slot tests can compute a real utxo hash for output 0.
func createTestParentTransaction() *bt.Tx {
	tx := bt.NewTx()

	input := &bt.Input{
		PreviousTxOutIndex: 0,
	}
	prevTxHash := createTestHash("grandparent-tx")
	_ = input.PreviousTxIDAdd(&prevTxHash)
	tx.Inputs = append(tx.Inputs, input)

	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      1000,
		LockingScript: &bscript.Script{bscript.OpTRUE},
	})

	return tx
}

// hashSetMatcher matches a []chainhash.Hash argument against an expected set,
// ignoring order — the counter-conflicting walk builds its result from a map.
func hashSetMatcher(want ...chainhash.Hash) interface{} {
	return mock.MatchedBy(func(hs []chainhash.Hash) bool {
		if len(hs) != len(want) {
			return false
		}

		seen := make(map[chainhash.Hash]struct{}, len(hs))
		for _, h := range hs {
			seen[h] = struct{}{}
		}

		for _, w := range want {
			if _, ok := seen[w]; !ok {
				return false
			}
		}

		return true
	})
}

// stubCounterConflictingWalk wires the store calls the ghost-aware counter-
// conflicting walk makes for a root tx built by the createTestTransaction
// helpers: the parent slot records spenderTxHash as a live spender with no
// conflicting children of its own. The walk result is {root, spender}.
func stubCounterConflictingWalk(mockStore *MockUtxostore, rootTx *bt.Tx, spenderTxHash chainhash.Hash) {
	parentTxHash := *rootTx.Inputs[0].PreviousTxIDChainHash()

	mockStore.On("Get", mock.Anything, &parentTxHash, []fields.FieldName{fields.Utxos, fields.DeletedChildren}).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&spenderTxHash, 0)}}, nil).Once()
	mockStore.On("GetConflictingChildren", mock.Anything, spenderTxHash).
		Return([]chainhash.Hash{}, nil).Once()
}

// TestGetCounterConflictingTxHashes_DedupesSpenderWalks pins the per-input walk
// dedupe of issue 1391: a tx whose inputs are all counter-spent by the same
// transaction must walk that spender's descendant cone exactly once, not once
// per input.
func TestGetCounterConflictingTxHashes_DedupesSpenderWalks(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}
	mockStore.Test(t)

	txHash := createTestHash("dedupe-tx")
	parentTxHash := createTestHash("dedupe-parent")
	spenderHash := createTestHash("dedupe-spender")
	childHash := createTestHash("dedupe-spender-child")

	// tx spends three outputs of the same parent, all counter-spent by the same tx
	testTx := bt.NewTx()

	for vout := uint32(0); vout < 3; vout++ {
		input := &bt.Input{PreviousTxOutIndex: vout}
		_ = input.PreviousTxIDAdd(&parentTxHash)
		testTx.Inputs = append(testTx.Inputs, input)
	}

	testTx.Outputs = append(testTx.Outputs, &bt.Output{Satoshis: 1000})

	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Return(&meta.Data{Tx: testTx}, nil)
	mockStore.On("Get", mock.Anything, &parentTxHash, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{
				{TxID: &spenderHash},
				{TxID: &spenderHash},
				{TxID: &spenderHash},
			},
		}, nil)

	// exactly ONE walk of the unique counter-spender, not one per input
	mockStore.On("GetConflictingChildren", mock.Anything, spenderHash).
		Return([]chainhash.Hash{childHash}, nil).Once()

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash)
	require.NoError(t, err)

	assert.Contains(t, result, txHash)
	assert.Contains(t, result, spenderHash)
	assert.Contains(t, result, childHash)
	assert.Len(t, result, 3)

	mockStore.AssertExpectations(t)
	mockStore.AssertNumberOfCalls(t, "GetConflictingChildren", 1)
}

// TestGetConflictingChildren_ReapedMarkOrIsOrderIndependent pins the fix for the
// first-writer-wins reapedByParent verdict. A reaped descendant reachable from
// two in-cone parents must fail closed even when only the parent that did NOT
// enqueue it carries the deletedChildren mark; the pre-fix walk recorded the
// first parent's verdict and silently tolerated the descendant as a ghost,
// which lets ProcessConflicting clear a settled, mined spend.
func TestGetConflictingChildren_ReapedMarkOrIsOrderIndependent(t *testing.T) {
	rootHash := createTestHash("reaped-root")
	parentAHash := createTestHash("reaped-parent-a")
	parentBHash := createTestHash("reaped-parent-b")
	childHash := createTestHash("reaped-child")

	// markOn selects which of the two parents carries the deletedChildren mark.
	// Both orderings must fail closed; only "b" is red before the fix.
	newStore := func(markOn string) *MockUtxostore {
		mockStore := &MockUtxostore{}
		mockStore.Test(t)

		mockStore.On("Get", mock.Anything, &rootHash, mock.Anything).
			Return(&meta.Data{
				SpendingDatas: []*spend.SpendingData{{TxID: &parentAHash}, {TxID: &parentBHash}},
			}, nil)

		deleted := func(marked bool) map[chainhash.Hash]bool {
			if !marked {
				return nil
			}

			return map[chainhash.Hash]bool{childHash: true}
		}

		mockStore.On("Get", mock.Anything, &parentAHash, mock.Anything).
			Return(&meta.Data{
				SpendingDatas:   []*spend.SpendingData{{TxID: &childHash}},
				DeletedChildren: deleted(markOn == "a"),
			}, nil)
		mockStore.On("Get", mock.Anything, &parentBHash, mock.Anything).
			Return(&meta.Data{
				SpendingDatas:   []*spend.SpendingData{{TxID: &childHash}},
				DeletedChildren: deleted(markOn == "b"),
			}, nil)

		// the reaped descendant's own record is gone
		mockStore.On("Get", mock.Anything, &childHash, mock.Anything).
			Return(nil, errors.NewTxNotFoundError("%v not found", childHash))

		return mockStore
	}

	for _, markOn := range []string{"a", "b"} {
		t.Run("mark on parent "+markOn, func(t *testing.T) {
			result, err := GetConflictingChildren(context.Background(), newStore(markOn), rootHash)
			require.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "was reaped after being mined")
		})
	}
}

// TestGetConflictingChildren_ConflictingChildrenTraversal covers the
// ConflictingChildren edge of the walk. The scaling and frozen tests link their
// cones entirely through SpendingDatas, so without this the branch production
// uses for explicitly-marked conflicting descendants is never exercised.
func TestGetConflictingChildren_ConflictingChildrenTraversal(t *testing.T) {
	rootHash := createTestHash("cc-root")
	midHash := createTestHash("cc-mid")
	leafHash := createTestHash("cc-leaf")

	mockStore := &MockUtxostore{}
	mockStore.Test(t)

	mockStore.On("Get", mock.Anything, &rootHash, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{midHash}}, nil)
	mockStore.On("Get", mock.Anything, &midHash, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{leafHash}}, nil)
	mockStore.On("Get", mock.Anything, &leafHash, mock.Anything).
		Return(&meta.Data{}, nil)

	result, err := GetConflictingChildren(context.Background(), mockStore, rootHash)
	require.NoError(t, err)

	assert.ElementsMatch(t, []chainhash.Hash{midHash, leafHash}, result)
}

// fanOutProbeStore serves a fixed descendant graph and records the peak number
// of concurrent Get calls, so the per-level errgroup ceiling can be asserted
// rather than assumed. Only Get is exercised by the walk; the embedded Store is
// nil and must stay unused.
type fanOutProbeStore struct {
	Store
	children map[chainhash.Hash][]chainhash.Hash
	inFlight atomic.Int32
	peak     atomic.Int32
}

func (f *fanOutProbeStore) Get(_ context.Context, hash *chainhash.Hash, _ ...fields.FieldName) (*meta.Data, error) {
	cur := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)

	for {
		peak := f.peak.Load()
		if cur <= peak || f.peak.CompareAndSwap(peak, cur) {
			break
		}
	}

	// hold the slot long enough that a level wider than the ceiling actually
	// overlaps, otherwise the peak measurement is vacuous
	time.Sleep(2 * time.Millisecond)

	kids := f.children[*hash]
	spendingDatas := make([]*spend.SpendingData, 0, len(kids))

	for i := range kids {
		spendingDatas = append(spendingDatas, &spend.SpendingData{TxID: &kids[i]})
	}

	return &meta.Data{SpendingDatas: spendingDatas}, nil
}

// TestGetConflictingChildren_FanOutCapped pins the per-level ceiling. Deleting
// the SafeSetLimit call leaves every other test in these packages green, because
// their cones are linear (BFS level width 1) — this is the only case with a
// level wide enough for the cap to matter.
func TestGetConflictingChildren_FanOutCapped(t *testing.T) {
	const levelWidth = ConflictingWalkFanOut * 3

	rootHash := createTestHash("fanout-root")

	kids := make([]chainhash.Hash, levelWidth)
	for i := range kids {
		kids[i] = createTestHash(fmt.Sprintf("fanout-child-%d", i))
	}

	store := &fanOutProbeStore{children: map[chainhash.Hash][]chainhash.Hash{rootHash: kids}}

	result, err := GetConflictingChildren(context.Background(), store, rootHash)
	require.NoError(t, err)
	require.Len(t, result, levelWidth)

	peak := store.peak.Load()
	require.LessOrEqualf(t, peak, int32(ConflictingWalkFanOut),
		"per-level fan-out must stay at or below %d, peaked at %d", ConflictingWalkFanOut, peak)
	require.Greaterf(t, peak, int32(1),
		"the probe never observed concurrent reads (peak %d), so the ceiling assertion above proves nothing", peak)
}

// TestGetCounterConflictingTxHashes_MemoDoesNotCacheErrors pins that the
// per-spender walk memo caches successes only. A spender briefly unreadable
// while the first input is processed and readable by the second used to resolve
// on the second walk; caching the error replayed a stale NOT_FOUND, the
// un-memoised probe below then found the record present, and the whole call
// failed closed on an error that no longer reflected the store.
func TestGetCounterConflictingTxHashes_MemoDoesNotCacheErrors(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}
	mockStore.Test(t)

	txHash := createTestHash("memo-tx")
	parentTxHash := createTestHash("memo-parent")
	spenderHash := createTestHash("memo-spender")
	childHash := createTestHash("memo-spender-child")

	// one tx spending two outputs of the same parent, both counter-spent by the
	// same transaction — so both inputs route through the same memo entry
	testTx := bt.NewTx()

	for vout := uint32(0); vout < 2; vout++ {
		input := &bt.Input{PreviousTxOutIndex: vout}
		_ = input.PreviousTxIDAdd(&parentTxHash)
		testTx.Inputs = append(testTx.Inputs, input)
	}

	testTx.Outputs = append(testTx.Outputs, &bt.Output{Satoshis: 1000})

	parentTx := bt.NewTx()
	for range 2 {
		parentTx.Outputs = append(parentTx.Outputs, &bt.Output{
			Satoshis:      1000,
			LockingScript: bscript.NewFromBytes([]byte{bscript.OpTRUE}),
		})
	}

	mockStore.On("Get", mock.Anything, &txHash, []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: testTx}, nil)
	mockStore.On("Get", mock.Anything, &parentTxHash, []fields.FieldName{fields.Utxos, fields.DeletedChildren}).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{{TxID: &spenderHash}, {TxID: &spenderHash}},
		}, nil)

	// the walk fails once, then succeeds — the record showed up in between
	mockStore.On("GetConflictingChildren", mock.Anything, spenderHash).
		Return(([]chainhash.Hash)(nil), errors.NewTxNotFoundError("%v not found", spenderHash)).Once()
	mockStore.On("GetConflictingChildren", mock.Anything, spenderHash).
		Return([]chainhash.Hash{childHash}, nil).Once()

	// the ghost probe agrees with the walk the first time, then sees the record
	mockStore.On("Get", mock.Anything, &spenderHash, []fields.FieldName{fields.Conflicting}).
		Return(nil, errors.NewTxNotFoundError("%v not found", spenderHash)).Once()
	mockStore.On("Get", mock.Anything, &spenderHash, []fields.FieldName{fields.Conflicting}).
		Return(&meta.Data{}, nil)

	// input 0 is tolerated as a ghost, which builds a slot-clearing spend
	mockStore.On("Get", mock.Anything, &parentTxHash, []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: parentTx}, nil)

	result, ghostSpends, err := getCounterConflictingTxHashesAndGhostSpends(ctx, mockStore, txHash)
	require.NoError(t, err)

	// input 1's fresh walk found the spender, so it is in the counter set
	assert.Contains(t, result, txHash)
	assert.Contains(t, result, spenderHash)
	assert.Contains(t, result, childHash)

	// input 0 still emitted exactly one ghost slot clear
	assert.Len(t, ghostSpends, 1)
	assert.Equal(t, uint32(0), ghostSpends[0].Vout)
}

// TestGetConflictingChildren_ReapedMarkSameLevelParent pins the harder half of
// the order-independence property: the parent carrying the deletedChildren mark
// sits on the SAME BFS level as the reaped descendant, so the mark is not known
// when the level's goroutines classify the NOT_FOUND.
//
// Cone: root spends to A and to X, and X also spends an output of A. Both land
// on level 1, A lists X in deletedChildren, and X's record was reaped after
// being mined. Classifying absence inside the goroutine tolerated X as a ghost
// whenever A happened to be iterated after X was dropped — the verdict flipped
// on SpendingDatas ordering.
func TestGetConflictingChildren_ReapedMarkSameLevelParent(t *testing.T) {
	rootHash := createTestHash("samelevel-root")
	parentHash := createTestHash("samelevel-parent")
	reapedHash := createTestHash("samelevel-reaped")

	// both orderings of the root's spending data must reach the same verdict
	orderings := map[string][]*spend.SpendingData{
		"parent enqueued first": {{TxID: &parentHash}, {TxID: &reapedHash}},
		"reaped enqueued first": {{TxID: &reapedHash}, {TxID: &parentHash}},
	}

	for name, rootSpends := range orderings {
		t.Run(name, func(t *testing.T) {
			mockStore := &MockUtxostore{}
			mockStore.Test(t)

			mockStore.On("Get", mock.Anything, &rootHash, mock.Anything).
				Return(&meta.Data{SpendingDatas: rootSpends}, nil)

			// the marking parent is on the same level as the reaped descendant
			mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
				Return(&meta.Data{
					SpendingDatas:   []*spend.SpendingData{{TxID: &reapedHash}},
					DeletedChildren: map[chainhash.Hash]bool{reapedHash: true},
				}, nil)

			mockStore.On("Get", mock.Anything, &reapedHash, mock.Anything).
				Return(nil, errors.NewTxNotFoundError("%v not found", reapedHash))

			result, err := GetConflictingChildren(context.Background(), mockStore, rootHash)
			require.Error(t, err, "a reaped, marked descendant must fail closed regardless of level ordering")
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "was reaped after being mined")
		})
	}
}

// TestConflictingWalks_NilSpendingDataTxID pins the nil-TxID guard on both
// descendant walks. SpendingData.Clone handles a nil TxID and the counter walk
// checks it, so the codebase treats it as reachable; dereferencing it inside a
// BFS goroutine is an unrecovered panic that takes the process down rather than
// failing the block. The entry is skipped, matching the counter walk.
func TestConflictingWalks_NilSpendingDataTxID(t *testing.T) {
	rootHash := createTestHash("niltxid-root")
	childHash := createTestHash("niltxid-child")

	newStore := func() *MockUtxostore {
		mockStore := &MockUtxostore{}
		mockStore.Test(t)

		mockStore.On("Get", mock.Anything, &rootHash, mock.Anything).
			Return(&meta.Data{
				SpendingDatas: []*spend.SpendingData{
					{TxID: nil, Vin: 0}, // frozen/unset slot with no spender recorded
					{TxID: &childHash, Vin: 1},
				},
			}, nil)
		mockStore.On("Get", mock.Anything, &childHash, mock.Anything).
			Return(&meta.Data{}, nil)

		return mockStore
	}

	t.Run("GetConflictingChildren", func(t *testing.T) {
		require.NotPanics(t, func() {
			result, err := GetConflictingChildren(context.Background(), newStore(), rootHash)
			require.NoError(t, err)
			assert.Equal(t, []chainhash.Hash{childHash}, result)
		})
	})

	t.Run("GetAndLockChildren", func(t *testing.T) {
		store := newStore()
		store.On("SetLocked", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		require.NotPanics(t, func() {
			result, err := GetAndLockChildren(context.Background(), store, rootHash)
			require.NoError(t, err)
			assert.Contains(t, result, childHash)
		})
	})
}
