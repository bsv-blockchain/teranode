package logger

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockStore implements utxo.Store interface for testing
type MockStore struct {
	mock.Mock
}

func (m *MockStore) SetBlockHeight(blockHeight uint32) error {
	args := m.Called(blockHeight)
	return args.Error(0)
}

func (m *MockStore) GetBlockHeight() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

func (m *MockStore) SetMedianBlockTime(medianTime uint32) error {
	args := m.Called(medianTime)
	return args.Error(0)
}

func (m *MockStore) GetMedianBlockTime() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

func (m *MockStore) GetBlockState() utxo.BlockState {
	return utxo.BlockState{
		Height:     m.GetBlockHeight(),
		MedianTime: m.GetMedianBlockTime(),
	}
}

func (m *MockStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

func (m *MockStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	args := m.Called(ctx, tx, blockHeight, opts)
	return args.Get(0).(*meta.Data), args.Error(1)
}

func (m *MockStore) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	args := m.Called(ctx, hash, data)
	if result := args.Get(0); result != nil {
		*data = *result.(*meta.Data)
	}
	return args.Error(1)
}

func (m *MockStore) Get(ctx context.Context, hash *chainhash.Hash, fieldsArg ...fields.FieldName) (*meta.Data, error) {
	args := m.Called(ctx, hash, fieldsArg)
	return args.Get(0).(*meta.Data), args.Error(1)
}

func (m *MockStore) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	args := m.Called(ctx, tx, ignoreFlags)
	return args.Get(0).([]*utxo.Spend), args.Error(1)
}

func (m *MockStore) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	args := m.Called(ctx, spends, flagAsLocked)
	return args.Error(0)
}

func (m *MockStore) Delete(ctx context.Context, hash *chainhash.Hash) error {
	args := m.Called(ctx, hash)
	return args.Error(0)
}

func (m *MockStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	args := m.Called(ctx, hashes, minedBlockInfo)
	return args.Get(0).(map[chainhash.Hash][]uint32), args.Error(1)
}

func (m *MockStore) GetUnminedTxIterator(bool) (utxo.UnminedTxIterator, error) {
	args := m.Called()
	return args.Get(0).(utxo.UnminedTxIterator), args.Error(1)
}

func (m *MockStore) GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (utxo.UnminedTxIterator, error) {
	args := m.Called(cutoffBlockHeight)
	return args.Get(0).(utxo.UnminedTxIterator), args.Error(1)
}

func (m *MockStore) GetSpend(ctx context.Context, spendArg *utxo.Spend) (*utxo.SpendResponse, error) {
	args := m.Called(ctx, spendArg)
	return args.Get(0).(*utxo.SpendResponse), args.Error(1)
}

func (m *MockStore) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, fieldsArg ...fields.FieldName) error {
	args := m.Called(ctx, unresolvedMetaDataSlice, fieldsArg)
	return args.Error(0)
}

func (m *MockStore) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	args := m.Called(ctx, tx)
	return args.Error(0)
}

func (m *MockStore) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	args := m.Called(ctx, spends, tSettings)
	return args.Error(0)
}

func (m *MockStore) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	args := m.Called(ctx, spends, tSettings)
	return args.Error(0)
}

func (m *MockStore) ReAssignUTXO(ctx context.Context, utxoSpend *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	args := m.Called(ctx, utxoSpend, newUtxo, tSettings)
	return args.Error(0)
}

func (m *MockStore) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	args := m.Called(ctx, txHash)
	return args.Get(0).([]chainhash.Hash), args.Error(1)
}

func (m *MockStore) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	args := m.Called(ctx, txHash)
	return args.Get(0).([]chainhash.Hash), args.Error(1)
}

func (m *MockStore) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	args := m.Called(ctx, txHashes, setValue)
	return args.Get(0).([]*utxo.Spend), args.Get(1).([]chainhash.Hash), args.Error(2)
}

func (m *MockStore) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	args := m.Called(ctx, txHashes, setValue)
	return args.Error(0)
}

func (m *MockStore) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	args := m.Called(ctx, txHashes, onLongestChain)
	return args.Error(0)
}

func (m *MockStore) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	args := m.Called(ctx, cutoffBlockHeight)
	return args.Get(0).([]chainhash.Hash), args.Error(1)
}

func (m *MockStore) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	args := m.Called(ctx, txIDs, preserveUntilHeight)
	return args.Error(0)
}

func (m *MockStore) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	args := m.Called(ctx, currentHeight)
	return args.Error(0)
}

// MockIterator implements utxo.UnminedTxIterator for testing
type MockIterator struct {
	mock.Mock
}

func (m *MockIterator) Next(ctx context.Context) ([]*utxo.UnminedTransaction, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*utxo.UnminedTransaction), args.Error(1)
}

func (m *MockIterator) Err() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockIterator) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Helper functions for creating test data
func createTestHash(nonce byte) *chainhash.Hash {
	hash := &chainhash.Hash{}
	hash[0] = nonce
	return hash
}

func createTestTx(nonce byte) *bt.Tx {
	hash := createTestHash(nonce)
	tx := &bt.Tx{
		Version:  1,
		LockTime: 0,
	}

	// Add test inputs
	input := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xffffffff,
	}
	// Set the previous transaction ID using the proper method
	_ = input.PreviousTxIDAdd(hash)
	tx.Inputs = []*bt.Input{input}

	// Add test outputs
	output := &bt.Output{
		Satoshis:      100000000, // 1 BTC
		LockingScript: &bscript.Script{},
	}
	tx.Outputs = []*bt.Output{output}

	return tx
}

func createTestMetaData() *meta.Data {
	return &meta.Data{
		Tx:           createTestTx(1),
		BlockHeights: []uint32{100},
		BlockIDs:     []uint32{1},
		Fee:          1000,
	}
}

func createTestSpend(nonce byte) *utxo.Spend {
	return &utxo.Spend{
		TxID: createTestHash(nonce),
		Vout: 0,
		SpendingData: &spend.SpendingData{
			TxID: createTestHash(nonce),
			Vin:  0,
		},
	}
}

func createTestSpendResponse() *utxo.SpendResponse {
	return &utxo.SpendResponse{
		SpendingData: &spend.SpendingData{
			TxID: createTestHash(1),
			Vin:  0,
		},
	}
}

func TestNew(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}

	store := New(ctx, logger, mockStore)

	require.NotNil(t, store)
	assert.IsType(t, &Store{}, store)

	// Verify the returned store implements utxo.Store interface
	var _ utxo.Store = store
}

func TestCaller(t *testing.T) {
	// Test the caller function by calling it directly
	result := caller()

	// Should return a non-empty string with caller information
	assert.NotEmpty(t, result)
	assert.Contains(t, result, "called from")

	// Should contain multiple callers due to depth = 5
	assert.Contains(t, result, ",")

	// Should contain testing framework functions in the call stack
	assert.Contains(t, result, "testing.tRunner")
}

func TestSetBlockHeight(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	blockHeight := uint32(12345)
	expectedErr := errors.NewError("test error")

	mockStore.On("SetBlockHeight", blockHeight).Return(expectedErr)

	err := store.SetBlockHeight(blockHeight)

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestGetBlockHeight(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	expectedHeight := uint32(54321)

	mockStore.On("GetBlockHeight").Return(expectedHeight)

	height := store.GetBlockHeight()

	assert.Equal(t, expectedHeight, height)
	mockStore.AssertExpectations(t)
}

func TestSetMedianBlockTime(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	medianTime := uint32(1609459200) // 2021-01-01 00:00:00 UTC
	expectedErr := errors.NewError("median time error")

	mockStore.On("SetMedianBlockTime", medianTime).Return(expectedErr)

	err := store.SetMedianBlockTime(medianTime)

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestGetMedianBlockTime(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	expectedTime := uint32(1609459200)

	mockStore.On("GetMedianBlockTime").Return(expectedTime)

	result := store.GetMedianBlockTime()

	assert.Equal(t, expectedTime, result)
	mockStore.AssertExpectations(t)
}

func TestHealth(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	checkLiveness := true
	expectedStatus := 200
	expectedMessage := "healthy"
	expectedErr := errors.NewError("health check error")

	mockStore.On("Health", ctx, checkLiveness).Return(expectedStatus, expectedMessage, expectedErr)

	status, message, err := store.Health(ctx, checkLiveness)

	assert.Equal(t, expectedStatus, status)
	assert.Equal(t, expectedMessage, message)
	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestCreate(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	tx := createTestTx(1)
	blockHeight := uint32(100)
	expectedData := createTestMetaData()
	expectedErr := errors.NewError("create error")

	mockStore.On("Create", ctx, tx, blockHeight, mock.Anything).Return(expectedData, expectedErr)

	data, err := store.Create(ctx, tx, blockHeight)

	assert.Equal(t, expectedData, data)
	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestCreate_SkipLogging(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	tx := createTestTx(1)
	// Create a transaction with nil output to trigger skipLogging
	tx.Outputs[0] = nil
	blockHeight := uint32(100)
	expectedData := createTestMetaData()

	mockStore.On("Create", ctx, tx, blockHeight, mock.Anything).Return(expectedData, nil)

	data, err := store.Create(ctx, tx, blockHeight)

	assert.Equal(t, expectedData, data)
	assert.NoError(t, err)
	mockStore.AssertExpectations(t)
}

func TestGetMeta(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	hash := createTestHash(1)
	expectedData := createTestMetaData()
	expectedErr := errors.NewError("get meta error")

	mockStore.On("GetMeta", ctx, hash, mock.Anything).Return(expectedData, expectedErr)

	data := &meta.Data{}
	err := store.GetMeta(ctx, hash, data)

	assert.Equal(t, expectedData, data)
	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestGet(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	hash := createTestHash(1)
	fieldsToGet := []fields.FieldName{fields.Tx, fields.BlockHeights}
	expectedData := createTestMetaData()
	expectedErr := errors.NewError("get error")

	mockStore.On("Get", ctx, hash, fieldsToGet).Return(expectedData, expectedErr)

	data, err := store.Get(ctx, hash, fieldsToGet...)

	assert.Equal(t, expectedData, data)
	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestSpend(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	tx := createTestTx(1)
	ignoreFlags := []utxo.IgnoreFlags{{IgnoreConflicting: true, IgnoreLocked: false}}
	expectedSpends := []*utxo.Spend{createTestSpend(1), createTestSpend(2)}
	expectedErr := errors.NewError("spend error")

	mockStore.On("GetBlockHeight").Return(uint32(100))
	mockStore.On("Spend", ctx, tx, ignoreFlags).Return(expectedSpends, expectedErr)

	spends, err := store.Spend(ctx, tx, store.GetBlockHeight()+1, ignoreFlags...)

	assert.Equal(t, expectedSpends, spends)
	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestUnspend(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	spends := []*utxo.Spend{createTestSpend(1), createTestSpend(2)}
	expectedErr := errors.NewError("unspend error")

	// Note: The implementation always passes false as the third argument (ignoring variadic parameter)
	// The mock receives it as []bool{false} due to variadic expansion
	mockStore.On("Unspend", ctx, spends, []bool{false}).Return(expectedErr)

	err := store.Unspend(ctx, spends, true) // flagAsLocked is ignored in implementation

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestDelete(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	hash := createTestHash(1)
	expectedErr := errors.NewError("delete error")

	mockStore.On("Delete", ctx, hash).Return(expectedErr)

	err := store.Delete(ctx, hash)

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestSetMinedMulti(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	hashes := []*chainhash.Hash{createTestHash(1), createTestHash(2)}
	minedBlockInfo := utxo.MinedBlockInfo{BlockID: 123, BlockHeight: 100}
	expectedMap := map[chainhash.Hash][]uint32{
		*createTestHash(1): {0, 1},
		*createTestHash(2): {2},
	}
	expectedErr := errors.NewError("set mined multi error")

	mockStore.On("SetMinedMulti", ctx, hashes, minedBlockInfo).Return(expectedMap, expectedErr)

	resultMap, err := store.SetMinedMulti(ctx, hashes, minedBlockInfo)

	assert.Equal(t, expectedMap, resultMap)
	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestGetUnminedTxIterator(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	mockIterator := &MockIterator{}
	expectedErr := errors.NewError("iterator error")

	mockStore.On("GetUnminedTxIterator").Return(mockIterator, expectedErr)

	iterator, err := store.GetUnminedTxIterator(false)

	assert.Equal(t, mockIterator, iterator)
	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestGetSpend(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	spendArg := createTestSpend(1)
	expectedResponse := createTestSpendResponse()
	expectedErr := errors.NewError("get spend error")

	mockStore.On("GetSpend", ctx, spendArg).Return(expectedResponse, expectedErr)

	response, err := store.GetSpend(ctx, spendArg)

	assert.Equal(t, expectedResponse, response)
	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestBatchDecorate(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	unresolvedData := []*utxo.UnresolvedMetaData{{Hash: *createTestHash(1), Idx: 0}}
	fieldsToDecorate := []fields.FieldName{fields.Tx}
	expectedErr := errors.NewError("batch decorate error")

	mockStore.On("BatchDecorate", ctx, unresolvedData, fieldsToDecorate).Return(expectedErr)

	err := store.BatchDecorate(ctx, unresolvedData, fieldsToDecorate...)

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestPreviousOutputsDecorate(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	tx := createTestTx(1)
	expectedErr := errors.NewError("previous outputs decorate error")

	mockStore.On("PreviousOutputsDecorate", ctx, tx).Return(expectedErr)

	err := store.PreviousOutputsDecorate(ctx, tx)

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestFreezeUTXOs(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	spends := []*utxo.Spend{createTestSpend(1)}
	tSettings := &settings.Settings{}
	expectedErr := errors.NewError("freeze utxos error")

	mockStore.On("FreezeUTXOs", ctx, spends, tSettings).Return(expectedErr)

	err := store.FreezeUTXOs(ctx, spends, tSettings)

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestUnFreezeUTXOs(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	spends := []*utxo.Spend{createTestSpend(1)}
	tSettings := &settings.Settings{}
	expectedErr := errors.NewError("unfreeze utxos error")

	mockStore.On("UnFreezeUTXOs", ctx, spends, tSettings).Return(expectedErr)

	err := store.UnFreezeUTXOs(ctx, spends, tSettings)

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestReAssignUTXO(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	oldUtxo := createTestSpend(1)
	newUtxo := createTestSpend(2)
	tSettings := &settings.Settings{}
	expectedErr := errors.NewError("reassign utxo error")

	mockStore.On("ReAssignUTXO", ctx, oldUtxo, newUtxo, tSettings).Return(expectedErr)

	err := store.ReAssignUTXO(ctx, oldUtxo, newUtxo, tSettings)

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestGetCounterConflicting(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	txHash := *createTestHash(1)
	expectedHashes := []chainhash.Hash{*createTestHash(2), *createTestHash(3)}
	expectedErr := errors.NewError("get counter conflicting error")

	mockStore.On("GetCounterConflicting", ctx, txHash).Return(expectedHashes, expectedErr)

	hashes, err := store.GetCounterConflicting(ctx, txHash)

	assert.Equal(t, expectedHashes, hashes)
	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestGetConflictingChildren(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	txHash := *createTestHash(1)
	expectedHashes := []chainhash.Hash{*createTestHash(2), *createTestHash(3)}
	expectedErr := errors.NewError("get conflicting children error")

	mockStore.On("GetConflictingChildren", ctx, txHash).Return(expectedHashes, expectedErr)

	hashes, err := store.GetConflictingChildren(ctx, txHash)

	assert.Equal(t, expectedHashes, hashes)
	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestSetConflicting(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	txHashes := []chainhash.Hash{*createTestHash(1), *createTestHash(2)}
	setValue := true
	expectedSpends := []*utxo.Spend{createTestSpend(1)}
	expectedHashes := []chainhash.Hash{*createTestHash(3)}
	expectedErr := errors.NewError("set conflicting error")

	mockStore.On("SetConflicting", ctx, txHashes, setValue).Return(expectedSpends, expectedHashes, expectedErr)

	spends, hashes, err := store.SetConflicting(ctx, txHashes, setValue)

	assert.Equal(t, expectedSpends, spends)
	assert.Equal(t, expectedHashes, hashes)
	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestSetLocked(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	txHashes := []chainhash.Hash{*createTestHash(1), *createTestHash(2)}
	setValue := false
	expectedErr := errors.NewError("set locked error")

	mockStore.On("SetLocked", ctx, txHashes, setValue).Return(expectedErr)

	err := store.SetLocked(ctx, txHashes, setValue)

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestQueryOldUnminedTransactions(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	cutoffBlockHeight := uint32(50)
	expectedHashes := []chainhash.Hash{*createTestHash(1), *createTestHash(2)}
	expectedErr := errors.NewError("query old unmined error")

	mockStore.On("QueryOldUnminedTransactions", ctx, cutoffBlockHeight).Return(expectedHashes, expectedErr)

	hashes, err := store.QueryOldUnminedTransactions(ctx, cutoffBlockHeight)

	assert.Equal(t, expectedHashes, hashes)
	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestPreserveTransactions(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	txIDs := []chainhash.Hash{*createTestHash(1), *createTestHash(2)}
	preserveUntilHeight := uint32(200)
	expectedErr := errors.NewError("preserve transactions error")

	mockStore.On("PreserveTransactions", ctx, txIDs, preserveUntilHeight).Return(expectedErr)

	err := store.PreserveTransactions(ctx, txIDs, preserveUntilHeight)

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestProcessExpiredPreservations(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	currentHeight := uint32(150)
	expectedErr := errors.NewError("process expired preservations error")

	mockStore.On("ProcessExpiredPreservations", ctx, currentHeight).Return(expectedErr)

	err := store.ProcessExpiredPreservations(ctx, currentHeight)

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

// Integration tests
func TestLoggerIntegration(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()

	// Test a sequence of operations that would commonly occur together
	blockHeight := uint32(100)
	hash := createTestHash(1)
	tx := createTestTx(1)
	metaData := createTestMetaData()

	// Setup expectations
	mockStore.On("SetBlockHeight", blockHeight).Return(nil)
	mockStore.On("GetBlockHeight").Return(blockHeight)
	mockStore.On("Create", ctx, tx, blockHeight, mock.Anything).Return(metaData, nil)
	mockStore.On("GetMeta", ctx, hash, mock.Anything).Return(metaData, nil)

	// Execute operations
	err := store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	retrievedHeight := store.GetBlockHeight()
	assert.Equal(t, blockHeight, retrievedHeight)

	createdData, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)
	assert.Equal(t, metaData, createdData)

	retrievedData := &meta.Data{}
	err = store.GetMeta(ctx, hash, retrievedData)
	require.NoError(t, err)
	assert.Equal(t, metaData, retrievedData)

	mockStore.AssertExpectations(t)
}

func TestLoggerErrorHandling(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	hash := createTestHash(1)
	expectedErr := errors.NewError("underlying store error")

	// Test that errors are properly propagated
	mockStore.On("Delete", ctx, hash).Return(expectedErr)

	err := store.Delete(ctx, hash)

	assert.Equal(t, expectedErr, err)
	mockStore.AssertExpectations(t)
}

func TestLoggerWithComplexData(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()

	// Create a transaction with multiple inputs and outputs
	tx := &bt.Tx{
		Version:  1,
		LockTime: 0,
	}

	// Add multiple inputs
	for i := 0; i < 3; i++ {
		hash := createTestHash(byte(i))
		input := &bt.Input{
			PreviousTxOutIndex: uint32(i),
			SequenceNumber:     0xffffffff,
		}
		_ = input.PreviousTxIDAdd(hash)
		tx.Inputs = append(tx.Inputs, input)
	}

	// Add multiple outputs
	for i := 0; i < 2; i++ {
		output := &bt.Output{
			Satoshis:      uint64(100000000 + i*50000000), // Different amounts
			LockingScript: &bscript.Script{},
		}
		tx.Outputs = append(tx.Outputs, output)
	}

	blockHeight := uint32(200)
	expectedData := createTestMetaData()

	mockStore.On("Create", ctx, tx, blockHeight, mock.Anything).Return(expectedData, nil)

	data, err := store.Create(ctx, tx, blockHeight)

	require.NoError(t, err)
	assert.Equal(t, expectedData, data)
	mockStore.AssertExpectations(t)
}

func TestCallerFunctionEdgeCases(t *testing.T) {
	// Test caller function indirectly by calling a method that uses it
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	expectedHeight := uint32(999)
	mockStore.On("GetBlockHeight").Return(expectedHeight)

	// This should invoke caller() internally and log the stack trace
	height := store.GetBlockHeight()

	assert.Equal(t, expectedHeight, height)
	mockStore.AssertExpectations(t)
}

func TestStoreTypeAssertion(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}

	store := New(context.Background(), logger, mockStore)

	// Ensure the returned type can be cast back to *Store
	loggerStore, ok := store.(*Store)
	require.True(t, ok)
	require.NotNil(t, loggerStore)

	// Verify internal fields are set correctly
	assert.Equal(t, logger, loggerStore.logger)
	assert.Equal(t, mockStore, loggerStore.store)
}

func TestConcurrentAccess(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()
	hash := createTestHash(1)
	metaData := createTestMetaData()

	// Setup expectations for concurrent calls
	mockStore.On("GetMeta", ctx, hash, mock.Anything).Return(metaData, nil).Times(10)

	// Test concurrent access to ensure thread safety of logging
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()

			data := &meta.Data{}
			err := store.GetMeta(ctx, hash, data)
			assert.NoError(t, err)
			assert.Equal(t, metaData, data)
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for concurrent operations to complete")
		}
	}

	mockStore.AssertExpectations(t)
}

// Test edge cases and error conditions
func TestCreateWithNilOutputs(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	ctx := context.Background()

	// Create a transaction with nil output to test skipLogging path
	tx := &bt.Tx{
		Version:  1,
		LockTime: 0,
		Inputs:   []*bt.Input{},
		Outputs:  []*bt.Output{nil}, // This will trigger skipLogging = true
	}

	blockHeight := uint32(100)
	expectedData := createTestMetaData()

	mockStore.On("Create", ctx, tx, blockHeight, mock.Anything).Return(expectedData, nil)

	data, err := store.Create(ctx, tx, blockHeight)

	require.NoError(t, err)
	assert.Equal(t, expectedData, data)
	mockStore.AssertExpectations(t)
}

func TestMethodsWithNoLogging(t *testing.T) {
	// Test methods that don't add logging (just pass through)
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	blockHeight := uint32(123)
	mockStore.On("SetBlockHeight", blockHeight).Return(nil)

	err := store.SetBlockHeight(blockHeight)

	assert.NoError(t, err)
	mockStore.AssertExpectations(t)
}

func TestGetUnminedTxIteratorPassthrough(t *testing.T) {
	// Test that GetUnminedTxIterator properly passes through without logging
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore).(*Store)

	mockIterator := &MockIterator{}
	mockStore.On("GetUnminedTxIterator").Return(mockIterator, nil)

	iterator, err := store.GetUnminedTxIterator(false)

	assert.Equal(t, mockIterator, iterator)
	assert.NoError(t, err)
	mockStore.AssertExpectations(t)
}

// Test to ensure all interface methods are implemented
func TestInterfaceCompliance(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockStore := &MockStore{}
	store := New(context.Background(), logger, mockStore)

	// This test will fail at compile time if Store doesn't implement utxo.Store
	var _ utxo.Store = store

	// Additionally verify the type assertion works
	loggerStore, ok := store.(*Store)
	require.True(t, ok)
	require.NotNil(t, loggerStore)
}
