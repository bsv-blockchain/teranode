package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
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

	// Mock GetCounterConflicting call
	losingTxHash := createTestHash("losing-tx-1")
	mockStore.On("GetCounterConflicting", mock.Anything, conflictingTxHash).
		Return([]chainhash.Hash{losingTxHash}, nil)

	// Mock SetConflicting call for marking losing txs as conflicting
	affectedSpends := []*Spend{
		{TxID: &losingTxHash, Vout: 0},
	}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{losingTxHash}, true).
		Return(affectedSpends, []chainhash.Hash{}, nil)

	// Mock Unspend call
	mockStore.On("Unspend", mock.Anything, affectedSpends, mock.Anything).Return(nil)

	// Mock Spend call for winning transaction
	mockStore.On("SpendAndCreate", mock.Anything, testTx, mock.Anything, mock.Anything).Return(nil, []*Spend{}, nil)

	// Mock SetConflicting call for marking winning txs as not conflicting
	mockStore.On("SetConflicting", mock.Anything, conflictingTxHashes, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil)

	// Mock SetLocked call
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{losingTxHash}, false).Return(nil)

	// Execute test
	result, _, err := ProcessConflicting(ctx, mockStore, 1, chainhash.Hash{}, conflictingTxHashes, map[chainhash.Hash]struct{}{})

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
	result, _, err := ProcessConflicting(ctx, mockStore, 1, chainhash.Hash{}, conflictingTxHashes, map[chainhash.Hash]struct{}{})

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
	result, _, err := ProcessConflicting(ctx, mockStore, 1, chainhash.Hash{}, conflictingTxHashes, map[chainhash.Hash]struct{}{})

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
	result, _, err := ProcessConflicting(ctx, mockStore, 1, chainhash.Hash{}, conflictingTxHashes, map[chainhash.Hash]struct{}{})

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

	// Mock GetCounterConflicting call returning error
	mockStore.On("GetCounterConflicting", mock.Anything, conflictingTxHash).
		Return([]chainhash.Hash{}, errors.NewProcessingError("counter conflicting error"))

	// Execute test
	result, _, err := ProcessConflicting(ctx, mockStore, 1, chainhash.Hash{}, conflictingTxHashes, map[chainhash.Hash]struct{}{})

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

	mockStore.On("GetCounterConflicting", mock.Anything, conflictingTxHash).
		Return([]chainhash.Hash{losingTxHash}, nil)

	affectedSpends := []*Spend{{TxID: &losingTxHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{losingTxHash}, true).
		Return(affectedSpends, []chainhash.Hash{}, nil)

	// Mock Unspend call returning error
	mockStore.On("Unspend", mock.Anything, mock.Anything, mock.Anything).
		Return(errors.NewProcessingError("unspend failed"))

	// step 2 failed → rollback only undoes step 1 (clear conflicting flag).
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{losingTxHash}, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil)

	// Execute test
	result, _, err := ProcessConflicting(ctx, mockStore, 1, chainhash.Hash{}, conflictingTxHashes, map[chainhash.Hash]struct{}{})

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
	}, nil).Once()

	mockStore.On("GetCounterConflicting", mock.Anything, conflictingTxHash).
		Return([]chainhash.Hash{losingTxHash}, nil)

	affectedSpends := []*Spend{{TxID: &losingTxHash, Vout: 0}}
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{losingTxHash}, true).
		Return(affectedSpends, []chainhash.Hash{}, nil)

	mockStore.On("Unspend", mock.Anything, affectedSpends, mock.Anything).Return(nil)

	// Mock Spend call returning error with spends that have errors
	spendWithError := &Spend{
		TxID: &losingTxHash,
		Vout: 0,
		Err:  errors.NewProcessingError("spend error"),
	}
	mockStore.On("SpendAndCreate", mock.Anything, testTx, mock.Anything, mock.Anything).
		Return(nil, []*Spend{spendWithError}, errors.NewTxInvalidError("spend failed"))

	// step-3 failure rollback path: re-fetch losing tx body, re-spend it, clear conflicting,
	// unlock parents. (No partial successful step-3 spends — the only spend has Err != nil.)
	mockStore.On("Get", mock.Anything, &losingTxHash, mock.Anything).Return(&meta.Data{
		Tx: losingTx,
	}, nil).Once()
	mockStore.On("SpendAndCreate", mock.Anything, losingTx, mock.Anything, mock.Anything).
		Return(nil, []*Spend{}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{losingTxHash}, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil)
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{losingTxHash}, false).Return(nil)

	// Execute test
	result, _, err := ProcessConflicting(ctx, mockStore, 1, chainhash.Hash{}, conflictingTxHashes, map[chainhash.Hash]struct{}{})

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
	result, err := GetConflictingChildren(ctx, mockStore, parentHash, 0)

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
	result, err := GetConflictingChildren(ctx, mockStore, coinbaseHash, 0)

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
	result, err := GetConflictingChildren(ctx, mockStore, txHash, 0)

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
	result, err := GetConflictingChildren(ctx, mockStore, parentHash, 0)

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
	result, err := GetConflictingChildren(ctx, mockStore, parentHash, 0)

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

	// Mock the descendant walk of the conflicting tx: it has one spending child,
	// which itself has no descendants
	childSpendingData := &spend.SpendingData{TxID: &childTxHash}
	mockStore.On("Get", mock.Anything, &conflictingTxHash, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{childSpendingData},
		}, nil)
	mockStore.On("Get", mock.Anything, &childTxHash, mock.Anything).
		Return(&meta.Data{}, nil)

	// Execute test
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0)

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
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0)

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
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0)

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
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0)

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

	// Mock the descendant walk of the conflicting tx: one of its outputs is
	// frozen, i.e. its spending data carries the frozen sentinel tx hash
	frozenSpendingData := &spend.SpendingData{TxID: &subtree.FrozenBytesTxHash}
	mockStore.On("Get", mock.Anything, &conflictingTxHash, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{frozenSpendingData},
		}, nil)
	// the sentinel is not a real record: Aerospike Get returns (nil, nil) for it
	mockStore.On("Get", mock.Anything, &subtree.FrozenBytesTxHash, mock.Anything).
		Return(nil, nil).Maybe()

	// Execute test
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0)

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

	// Mock the descendant walk of the conflicting tx failing on its root read
	mockStore.On("Get", mock.Anything, &conflictingTxHash, mock.Anything).
		Return(nil, errors.NewProcessingError("get conflicting children error"))

	// Execute test
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0)

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
	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0)

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

func TestGetConflictingChildren_BudgetExceeded(t *testing.T) {
	// linear chain: root -> childA -> childB (3 visited nodes including root)
	rootHash := createTestHash("budget-root")
	childAHash := createTestHash("budget-child-a")
	childBHash := createTestHash("budget-child-b")

	newMockChain := func() *MockUtxostore {
		mockStore := &MockUtxostore{}
		mockStore.On("Get", mock.Anything, &rootHash, mock.Anything).
			Return(&meta.Data{
				SpendingDatas: []*spend.SpendingData{{TxID: &childAHash}},
			}, nil)
		mockStore.On("Get", mock.Anything, &childAHash, mock.Anything).
			Return(&meta.Data{
				SpendingDatas: []*spend.SpendingData{{TxID: &childBHash}},
			}, nil).Maybe()
		mockStore.On("Get", mock.Anything, &childBHash, mock.Anything).
			Return(&meta.Data{}, nil).Maybe()
		return mockStore
	}

	t.Run("cone equal to budget passes", func(t *testing.T) {
		result, err := GetConflictingChildren(context.Background(), newMockChain(), rootHash, 3)
		require.NoError(t, err)
		assert.Len(t, result, 2) // root excluded from result
	})

	t.Run("cone exceeding budget fails closed with distinct error", func(t *testing.T) {
		result, err := GetConflictingChildren(context.Background(), newMockChain(), rootHash, 2)
		require.Error(t, err)
		assert.Nil(t, result)

		require.ErrorIs(t, err, errors.ErrUtxoWalkLimitExceeded)

		// must never be classified as an invalid tx (permanent block poison) or a
		// missing tx (block-incomplete retry loop)
		assert.NotErrorIs(t, err, errors.ErrTxInvalid)
		assert.NotErrorIs(t, err, errors.ErrTxNotFound)

		// remains detectable through the processTxMetaUsingStore context-canceled wrap
		wrapped := errors.NewContextCanceledError("wrapped", err)
		assert.ErrorIs(t, wrapped, errors.ErrUtxoWalkLimitExceeded)
	})
}

func TestGetConflictingChildren_FrozenSentinelNotWalked(t *testing.T) {
	// root has a frozen output (sentinel spending data) and a real child; the
	// sentinel must be returned in the result set for the callers' frozen checks
	// but must not itself be walked (it is not a real record)
	rootHash := createTestHash("frozen-root")
	childHash := createTestHash("frozen-sibling-child")

	mockStore := &MockUtxostore{}
	mockStore.On("Get", mock.Anything, &rootHash, mock.Anything).
		Return(&meta.Data{
			SpendingDatas: []*spend.SpendingData{
				{TxID: &subtree.FrozenBytesTxHash},
				{TxID: &childHash},
			},
		}, nil)
	mockStore.On("Get", mock.Anything, &childHash, mock.Anything).
		Return(&meta.Data{}, nil)
	// tolerated pre-fix behavior stub; the assertion below requires it unused
	mockStore.On("Get", mock.Anything, &subtree.FrozenBytesTxHash, mock.Anything).
		Return(nil, nil).Maybe()

	result, err := GetConflictingChildren(context.Background(), mockStore, rootHash, 0)
	require.NoError(t, err)

	assert.Contains(t, result, subtree.FrozenBytesTxHash)
	assert.Contains(t, result, childHash)
	mockStore.AssertNotCalled(t, "Get", mock.Anything, &subtree.FrozenBytesTxHash, mock.Anything)
}

func TestGetCounterConflictingTxHashes_DedupesSpenderWalks(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("dedupe-tx")
	parentTxHash := createTestHash("dedupe-parent")
	spenderHash := createTestHash("dedupe-spender")

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
	mockStore.On("Get", mock.Anything, &spenderHash, mock.Anything).
		Return(&meta.Data{}, nil)

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0)
	require.NoError(t, err)
	assert.Contains(t, result, txHash)
	assert.Contains(t, result, spenderHash)
	assert.Len(t, result, 2)

	// one Get for the tx, one for the unique parent, and exactly ONE walk of the
	// unique counter-spender — not one walk per input
	mockStore.AssertNumberOfCalls(t, "Get", 3)
}
