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
	mockStore.On("Spend", mock.Anything, testTx, mock.Anything, mock.Anything).Return([]*Spend{}, nil)

	// Mock SetConflicting call for marking winning txs as not conflicting
	mockStore.On("SetConflicting", mock.Anything, conflictingTxHashes, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil)

	// Mock SetLocked call
	mockStore.On("SetLocked", mock.Anything, []chainhash.Hash{losingTxHash}, false).Return(nil)

	// Execute test
	result, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]bool{})

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
	result, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]bool{})

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
	result, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]bool{})

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
	result, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]bool{})

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
	result, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]bool{})

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

	// Execute test
	result, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]bool{})

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

	// Mock successful setup calls
	mockStore.On("Get", mock.Anything, &conflictingTxHash, mock.Anything).Return(&meta.Data{
		Tx:          testTx,
		Conflicting: true,
	}, nil)

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
	mockStore.On("Spend", mock.Anything, testTx, mock.Anything, mock.Anything).
		Return([]*Spend{spendWithError}, errors.NewTxInvalidError("spend failed"))

	// Execute test
	result, err := ProcessConflicting(ctx, mockStore, 1, conflictingTxHashes, map[chainhash.Hash]bool{})

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
	result, err := markConflictingRecursively(ctx, mockStore, []chainhash.Hash{txHash})

	// Assertions
	require.NoError(t, err)
	assert.Len(t, result, 2) // Should contain both parent and child spends
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
	result, err := markConflictingRecursively(ctx, mockStore, []chainhash.Hash{txHash})

	// Assertions
	assert.Nil(t, result)
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
	result, err := markConflictingRecursively(ctx, mockStore, []chainhash.Hash{txHash})

	// Assertions
	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, txHash, *result[0].TxID)
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

	// Mock GetConflictingChildren recursive call
	mockStore.On("GetConflictingChildren", mock.Anything, childHash).Return([]chainhash.Hash{}, nil)
	mockStore.On("GetConflictingChildren", mock.Anything, grandChildHash).Return([]chainhash.Hash{}, nil)

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

func TestGetConflictingChildren_RecursiveError(t *testing.T) {
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

	// Mock GetConflictingChildren recursive call returning error
	mockStore.On("GetConflictingChildren", mock.Anything, childHash).
		Return([]chainhash.Hash{}, errors.NewProcessingError("recursive error"))

	// Execute test
	result, err := GetConflictingChildren(ctx, mockStore, parentHash)

	// Assertions
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "recursive error")
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
