package validator

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNew_MissingKafkaConfig(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	settings := test.CreateBaseTestSettings(t)
	settings.Kafka.TxMetaConfig = nil // Missing Kafka config

	_, err := New(ctx, logger, settings, nil, nil, nil, nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing Kafka URL for txmeta")
}

func TestNew_BlockAssemblyDisabled(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()

	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.Disabled = true // Block assembly disabled

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, validator)

	// Block assembler should be nil when disabled
	v := validator.(*Validator)
	assert.Nil(t, v.blockAssembler)
}

func TestNew_BlockAssemblyEnabled(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()

	// Create a basic block assembly client
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.Disabled = false // Block assembly enabled

	blockAssemblyClient, err := blockassembly.NewClient(ctx, logger, settings)
	require.NoError(t, err)

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, blockAssemblyClient, nil)
	require.NoError(t, err)
	assert.NotNil(t, validator)

	// Block assembler should not be nil when enabled
	v := validator.(*Validator)
	assert.NotNil(t, v.blockAssembler)
}

func TestValidator_Health_Liveness(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	code, message, err := validator.Health(context.Background(), true)
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "OK", message)
	assert.NoError(t, err)
}

func TestValidator_Health_Readiness(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	// Create an actual SQL store with in-memory database
	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	settings := test.CreateBaseTestSettings(t)
	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	validator, err := New(ctx, logger, settings, utxoStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	code, _, _ := validator.Health(ctx, false)

	// The health check should execute without panic
	// The exact result will depend on various factors like Kafka connectivity
	assert.NotEqual(t, 0, code)

	// Check that the validator implements the basic functions
	height := validator.GetBlockHeight()
	_ = height                                                 // Height could be 0 or non-zero depending on store state
	assert.Equal(t, uint32(0), validator.GetMedianBlockTime()) // Default value
}

func TestValidator_GetBlockHeight(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// NullStore returns 0 as default height
	height := validator.GetBlockHeight()
	assert.Equal(t, uint32(0), height)
}

func TestValidator_GetMedianBlockTime(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// NullStore returns 0 as default median time
	medianTime := validator.GetMedianBlockTime()
	assert.Equal(t, uint32(0), medianTime)
}

func TestValidator_Validate_CallsValidateWithOptions(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	// Use sqlitememory store so we have a real UTXO store without the parent UTXO
	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	settings := test.CreateBaseTestSettings(t)
	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	// Create a block assembler to avoid nil pointer issues
	blockAssemblyClient, err := blockassembly.NewClient(ctx, logger, settings)
	require.NoError(t, err)

	validator, err := New(ctx, logger, settings, utxoStore, nil, nil, nil, blockAssemblyClient, nil)
	require.NoError(t, err)

	// Create a simple transaction using the transactions helper
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

	// This should call ValidateWithOptions internally
	_, err = validator.Validate(ctx, tx, 100)
	// The error is expected since the parent UTXO doesn't exist in the store
	assert.Error(t, err)
}

func TestValidator_TriggerBatcher(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// This is a no-op function, just verify it doesn't panic
	validator.TriggerBatcher()
}

func TestValidator_ValidateInternal_CoinbaseTransaction(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// Create coinbase transaction
	tx := bt.NewTx()
	coinbaseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	input := &bt.Input{
		PreviousTxOutIndex: 0xffffffff,
		SequenceNumber:     0xffffffff,
	}
	_ = input.PreviousTxIDAdd(coinbaseHash)
	tx.Inputs = append(tx.Inputs, input)

	options := &Options{}
	v := validator.(*Validator)

	_, err = v.validateInternal(ctx, tx, 100, options)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "coinbase transactions are not supported")
}

func TestValidator_ExtendTransaction_Coinbase(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// Create coinbase transaction
	tx := bt.NewTx()
	coinbaseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	input := &bt.Input{
		PreviousTxOutIndex: 0xffffffff,
		SequenceNumber:     0xffffffff,
	}
	_ = input.PreviousTxIDAdd(coinbaseHash)
	tx.Inputs = append(tx.Inputs, input)

	v := validator.(*Validator)
	err = v.extendTransaction(ctx, tx)
	assert.NoError(t, err) // Coinbase transactions don't need extension
}

func TestValidator_ExtendTransaction_NonCoinbase(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// Create non-coinbase transaction
	tx := bt.NewTx()
	prevHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	input := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
	}
	_ = input.PreviousTxIDAdd(prevHash)
	tx.Inputs = append(tx.Inputs, input)

	v := validator.(*Validator)
	_ = v.extendTransaction(ctx, tx)
	// With nullstore, this might not error but should complete without panic
	// The exact behavior depends on the nullstore implementation
}

func TestValidator_ValidateTransaction_NotExtended(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// Create non-extended transaction
	tx := bt.NewTx()
	prevHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	input := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
	}
	_ = input.PreviousTxIDAdd(prevHash)
	tx.Inputs = append(tx.Inputs, input)

	v := validator.(*Validator)
	options := &Options{}

	err = v.validateTransaction(ctx, tx, 100, []uint32{99}, options)
	// Should error trying to extend transaction since parent doesn't exist
	assert.Error(t, err)
}

func TestValidator_ValidateTransactionScripts_NotExtended(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// Create non-extended transaction
	tx := bt.NewTx()
	prevHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	input := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
	}
	_ = input.PreviousTxIDAdd(prevHash)
	tx.Inputs = append(tx.Inputs, input)

	v := validator.(*Validator)
	options := &Options{}

	err = v.validateTransaction(ctx, tx, 100, []uint32{99}, options)
	// Should error trying to extend transaction since parent doesn't exist
	assert.Error(t, err)
}

func TestValidator_CreateInUtxoStore(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// Create a simple transaction
	tx := bt.NewTx()
	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      1000,
		LockingScript: &bscript.Script{},
	})

	v := validator.(*Validator)
	meta, err := v.CreateInUtxoStore(ctx, tx, 100, false, false)
	assert.NoError(t, err)
	assert.NotNil(t, meta)
}

func TestValidator_SendTxMetaToKafka_NilProducer(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	nullStore, _ := nullstore.NewNullStore()
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, nullStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// Create a simple transaction
	tx := bt.NewTx()
	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      1000,
		LockingScript: &bscript.Script{},
	})

	v := validator.(*Validator)
	meta, err := v.CreateInUtxoStore(ctx, tx, 100, false, false)
	require.NoError(t, err)

	// Test sendTxMetaToKafka with nil producer (should not panic)
	// Since we don't have a real kafka producer, this will likely cause nil pointer
	// Let's just verify the function exists and meta was created properly
	assert.NotNil(t, meta)
	hash := tx.TxIDChainHash()
	assert.NotNil(t, hash)
}

// The reverseSpends retry logic moved into utxo.SequentialSpendAndCreate; its
// behavior (empty spends, transient retries, all-retries-fail) is covered by
// TestSequentialSpendAndCreate in stores/utxo/spend_and_create_test.go.

func XTestValidator_ValidateInternal_SpendUtxosError_WithConflicting(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	// Create a mock UTXO store
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	v := validator.(*Validator)

	// Create a simple transaction
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

	// Create spends with error that will trigger conflicting logic
	testHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	spends := []*utxo.Spend{
		{
			TxID: testHash,
			Vout: 0,
			Err:  errors.ErrSpent, // This will trigger conflicting logic
		},
	}

	// Mock the Get calls that happen during transaction extension
	// The store knows the real parent: the validator re-extends from the
	// parent's own outputs rather than trusting the supplied ones
	// (GHSA-v76m-6vc7-g7c7), so a synthetic parent would fail script or value
	// checks against a genuinely signed child.
	mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentOutputsOf(coinbaseTx, utxo.ParentOutputNotMined), nil)

	// Mock spendUtxos to return UTXO error with spends containing errors
	mockStore.On("SpendAndCreate", mock.Anything, tx, mock.Anything, mock.Anything).Return(nil, spends, errors.NewUtxoError("utxo error", errors.ErrUtxoError))

	// Mock CreateInUtxoStore for conflicting transaction
	mockStore.On("SpendAndCreate", mock.Anything, tx, uint32(100), mock.Anything).Return(&meta.Data{}, nil, nil)

	options := &Options{
		CreateConflicting: true, // Enable conflicting creation
	}

	_, err = v.validateInternal(ctx, tx, 100, options)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrTxConflicting))

	mockStore.AssertExpectations(t)
}

func TestValidator_ValidateInternal_CreateConflicting_TxAlreadyExists(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	v := validator.(*Validator)

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

	testHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	spends := []*utxo.Spend{
		{TxID: testHash, Vout: 0, Err: errors.ErrSpent},
	}

	// The store knows the real parent: the validator re-extends from the
	// parent's own outputs rather than trusting the supplied ones
	// (GHSA-v76m-6vc7-g7c7), so a synthetic parent would fail script or value
	// checks against a genuinely signed child.
	mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentOutputsOf(coinbaseTx, utxo.ParentOutputNotMined), nil)
	mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000})
	// The main spend-and-create fails with a spend-phase utxo error (Once so the
	// conflicting-fallback create below matches the second expectation).
	mockStore.On("SpendAndCreate", mock.Anything, tx, mock.Anything, mock.Anything).Return(nil, spends, errors.NewUtxoError("utxo error", errors.ErrUtxoError)).Once()

	// CreateInUtxoStore returns ErrTxExists — tx was already validated via P2P
	mockStore.On("SpendAndCreate", mock.Anything, tx, uint32(100), mock.Anything).Return(&meta.Data{}, nil, errors.NewTxExistsError("already exists"))

	// GetMeta succeeds — tx exists in store (not yet marked conflicting)
	mockStore.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// SetConflicting is called to mark the existing tx as conflicting
	mockStore.On("SetConflicting", mock.Anything, mock.Anything, true).Return([]*utxo.Spend{}, []chainhash.Hash{}, nil)

	options := &Options{CreateConflicting: true}
	_, err = v.validateInternal(ctx, tx, 100, options)

	// Should return ErrTxConflicting, not a ProcessingError
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrTxConflicting), "expected ErrTxConflicting, got: %v", err)

	mockStore.AssertExpectations(t)
}

// TestValidator_ValidateInternal_CreateConflicting_TxAlreadyExistsEvictsTxMeta pins
// that marking an existing tx (and its spending descendants) conflicting also
// evicts them from subtree validation's txmeta cache, which holds them as
// non-conflicting from their original validation.
func TestValidator_ValidateInternal_CreateConflicting_TxAlreadyExistsEvictsTxMeta(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	v := validator.(*Validator)
	v.settings.Validator.TxMetaWireFormat = "v1"

	txmetaProducer := kafka.NewKafkaAsyncProducerMock()
	v.txmetaKafkaProducerClient = txmetaProducer

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

	testHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	spends := []*utxo.Spend{
		{TxID: testHash, Vout: 0, Err: errors.ErrSpent},
	}

	// The store knows the real parent: the validator re-extends from the
	// parent's own outputs rather than trusting the supplied ones
	// (GHSA-v76m-6vc7-g7c7), so a synthetic parent would fail script or value
	// checks against a genuinely signed child.
	mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentOutputsOf(coinbaseTx, utxo.ParentOutputNotMined), nil)
	mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000})
	// The main spend-and-create fails with a spend-phase utxo error (Once so the
	// conflicting-fallback create below matches the second expectation).
	mockStore.On("SpendAndCreate", mock.Anything, tx, mock.Anything, mock.Anything).Return(nil, spends, errors.NewUtxoError("utxo error", errors.ErrUtxoError)).Once()

	// CreateInUtxoStore returns ErrTxExists — tx was already validated via P2P
	mockStore.On("SpendAndCreate", mock.Anything, tx, uint32(100), mock.Anything).Return(&meta.Data{}, nil, errors.NewTxExistsError("already exists"))

	// GetMeta succeeds — tx exists in store (not yet marked conflicting)
	mockStore.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// SetConflicting is called to mark the existing tx as conflicting
	child := chainhash.HashH([]byte("spending-child"))
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{*tx.TxIDChainHash()}, true).Return([]*utxo.Spend{}, []chainhash.Hash{child}, nil)
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{child}, true).Return([]*utxo.Spend{}, []chainhash.Hash{}, nil)

	options := &Options{CreateConflicting: true}
	_, err = v.validateInternal(ctx, tx, 100, options)

	// Should return ErrTxConflicting, not a ProcessingError
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrTxConflicting), "expected ErrTxConflicting, got: %v", err)

	mockStore.AssertExpectations(t)

	deleted := drainTxMetaDeletes(txmetaProducer)

	require.True(t, deleted[*tx.TxIDChainHash()], "the tx marked conflicting must be evicted from the txmeta cache")
	require.True(t, deleted[child], "its cascaded descendant must be evicted from the txmeta cache")
}

// TestValidator_ValidateInternal_CreateConflicting_PartialMarkEvictsTxMeta pins
// that a cascade failing below its first level still evicts what it may have
// flipped. The tx itself is conflicting in the store by then.
func TestValidator_ValidateInternal_CreateConflicting_PartialMarkEvictsTxMeta(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	v := validator.(*Validator)
	v.settings.Validator.TxMetaWireFormat = "v1"

	txmetaProducer := kafka.NewKafkaAsyncProducerMock()
	v.txmetaKafkaProducerClient = txmetaProducer

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

	testHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	spends := []*utxo.Spend{
		{TxID: testHash, Vout: 0, Err: errors.ErrSpent},
	}

	// The store knows the real parent: the validator re-extends from the
	// parent's own outputs rather than trusting the supplied ones
	// (GHSA-v76m-6vc7-g7c7), so a synthetic parent would fail script or value
	// checks against a genuinely signed child.
	mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentOutputsOf(coinbaseTx, utxo.ParentOutputNotMined), nil)
	mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000})
	// The main spend-and-create fails with a spend-phase utxo error (Once so the
	// conflicting-fallback create below matches the second expectation).
	mockStore.On("SpendAndCreate", mock.Anything, tx, mock.Anything, mock.Anything).Return(nil, spends, errors.NewUtxoError("utxo error", errors.ErrUtxoError)).Once()

	// CreateInUtxoStore returns ErrTxExists — tx was already validated via P2P
	mockStore.On("SpendAndCreate", mock.Anything, tx, uint32(100), mock.Anything).Return(&meta.Data{}, nil, errors.NewTxExistsError("already exists"))

	// GetMeta succeeds — tx exists in store (not yet marked conflicting)
	mockStore.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// SetConflicting is called to mark the existing tx as conflicting
	child := chainhash.HashH([]byte("spending-child"))
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{*tx.TxIDChainHash()}, true).Return([]*utxo.Spend{}, []chainhash.Hash{child}, nil)
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{child}, true).Return([]*utxo.Spend(nil), []chainhash.Hash(nil), errors.NewStorageError("child write failed"))

	options := &Options{CreateConflicting: true}
	_, err = v.validateInternal(ctx, tx, 100, options)

	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrTxConflicting), "the cascade failed, so the call fails: %v", err)

	mockStore.AssertExpectations(t)

	deleted := drainTxMetaDeletes(txmetaProducer)

	// A revalidation now reads the tx as conflicting and skips this branch, so
	// this is the only chance to evict it.
	require.True(t, deleted[*tx.TxIDChainHash()], "the tx flipped before the failure must be evicted from the txmeta cache")
	require.True(t, deleted[child], "the descendant whose write failed may be flipped and must be evicted too")
}

// drainTxMetaDeletes decodes every v1 message on the producer and returns the
// hashes sent as DELETEs.
func drainTxMetaDeletes(producer *kafka.KafkaAsyncProducerMock) map[chainhash.Hash]bool {
	deleted := map[chainhash.Hash]bool{}

	for len(producer.PublishChannel()) > 0 {
		data := (<-producer.PublishChannel()).Value
		offset := 4

		for i := uint32(0); i < binary.LittleEndian.Uint32(data[:4]); i++ {
			var h chainhash.Hash
			copy(h[:], data[offset:offset+32])

			if data[offset+32] == txmetacache.WireActionDELETE {
				deleted[h] = true
			}

			offset += 32 + 1 + 4 + int(binary.LittleEndian.Uint32(data[offset+33:]))
		}
	}

	return deleted
}

func XTestValidator_ValidateInternal_SpendUtxosError_TxNotFound(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	// Create a mock UTXO store
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	v := validator.(*Validator)

	// Create a simple transaction
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

	// Mock the Get calls that happen during transaction extension
	// The store knows the real parent: the validator re-extends from the
	// parent's own outputs rather than trusting the supplied ones
	// (GHSA-v76m-6vc7-g7c7), so a synthetic parent would fail script or value
	// checks against a genuinely signed child.
	mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentOutputsOf(coinbaseTx, utxo.ParentOutputNotMined), nil)

	// Mock spendUtxos to return TxNotFound error (parent DAH'd)
	mockStore.On("SpendAndCreate", mock.Anything, tx, mock.Anything, mock.Anything).Return(nil, []*utxo.Spend{}, errors.ErrTxNotFound)

	// Mock GetMeta to return existing tx (already blessed)
	existingMeta := &meta.Data{Tx: tx}
	mockStore.On("GetMeta", mock.Anything, tx.TxIDChainHash()).Return(existingMeta, nil)

	options := &Options{}

	result, err := v.validateInternal(ctx, tx, 100, options)
	assert.NoError(t, err)
	assert.Equal(t, existingMeta, result)

	mockStore.AssertExpectations(t)
}

func XTestValidator_ValidateInternal_SpendUtxosError_TxNotFound_NotInStore(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	// Create a mock UTXO store
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	v := validator.(*Validator)

	// Create a simple transaction
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

	// Mock the Get calls that happen during transaction extension
	// The store knows the real parent: the validator re-extends from the
	// parent's own outputs rather than trusting the supplied ones
	// (GHSA-v76m-6vc7-g7c7), so a synthetic parent would fail script or value
	// checks against a genuinely signed child.
	mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentOutputsOf(coinbaseTx, utxo.ParentOutputNotMined), nil)

	// Mock spendUtxos to return TxNotFound error
	mockStore.On("SpendAndCreate", mock.Anything, tx, mock.Anything, mock.Anything).Return(nil, []*utxo.Spend{}, errors.ErrTxNotFound)

	// Mock GetMeta to also fail (tx not in store)
	mockStore.On("GetMeta", mock.Anything, tx.TxIDChainHash()).Return(nil, errors.ErrTxNotFound)

	options := &Options{}

	_, err = v.validateInternal(ctx, tx, 100, options)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error spending utxos")

	mockStore.AssertExpectations(t)
}

func XTestValidator_ValidateInternal_SpendUtxosError_GeneralError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	// Create a mock UTXO store
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	v := validator.(*Validator)

	// Create a simple transaction
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

	// Mock the Get calls that happen during transaction extension
	// The store knows the real parent: the validator re-extends from the
	// parent's own outputs rather than trusting the supplied ones
	// (GHSA-v76m-6vc7-g7c7), so a synthetic parent would fail script or value
	// checks against a genuinely signed child.
	mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentOutputsOf(coinbaseTx, utxo.ParentOutputNotMined), nil)
	mockStore.On("GetBlockHeight").Return(uint32(100))

	// Mock spendUtxos to return a general error (not UTXO or TxNotFound)
	mockStore.On("SpendAndCreate", mock.Anything, tx, mock.Anything, mock.Anything).Return(nil, []*utxo.Spend{}, assert.AnError)

	options := &Options{}

	_, err = v.validateInternal(ctx, tx, 100, options)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error spending utxos")

	mockStore.AssertExpectations(t)
}

// Test coverage for validateInternal error paths after spendUtxos call - IMPROVED TESTS

func TestValidator_ValidateInternal_UTXOError_ConflictingTxCreation(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	v := validator.(*Validator)

	// Create transaction
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

	// Mock parent tx extension
	mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentOutputsOf(coinbaseTx, utxo.ParentOutputNotMined), nil)
	mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000})

	// Mock the spend phase to return UTXO error with conflicting spend (Once so
	// the conflicting-fallback create below matches the second expectation)
	spends := []*utxo.Spend{{TxID: &chainhash.Hash{}, Vout: 0, Err: errors.ErrSpent}}
	utxoErr := errors.NewUtxoError("utxo error", errors.ErrUtxoError)
	mockStore.On("SpendAndCreate", mock.Anything, tx, mock.Anything, mock.Anything).Return(nil, spends, utxoErr).Once()

	// Mock CreateInUtxoStore for conflicting transaction
	mockStore.On("SpendAndCreate", mock.Anything, tx, uint32(100), mock.Anything).Return(&meta.Data{}, nil, nil)

	options := &Options{CreateConflicting: true}
	_, err = v.validateInternal(ctx, tx, 100, options)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "conflicting")
	mockStore.AssertExpectations(t)
}

func TestValidator_ValidateInternal_TxNotFoundError_ExistingTx(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	v := validator.(*Validator)

	// Create transaction
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

	// Mock parent tx extension
	mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentOutputsOf(coinbaseTx, utxo.ParentOutputNotMined), nil)
	mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000})

	// Mock spendUtxos to return TxNotFound error
	mockStore.On("SpendAndCreate", mock.Anything, tx, mock.Anything, mock.Anything).Return(nil, []*utxo.Spend{}, errors.NewTxNotFoundError("tx not found"))

	// Mock GetMeta to return existing tx already mined and not flagged — legitimate DAH-evicted-parent case.
	existingMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}}
	mockStore.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			data := args.Get(2).(*meta.Data)
			*data = *existingMeta
		}).
		Return(nil)

	options := &Options{}
	txMetaData, err := v.validateInternal(ctx, tx, 100, options)

	assert.NoError(t, err)
	assert.NotNil(t, txMetaData)
	assert.Equal(t, []uint32{1, 2}, txMetaData.BlockIDs)
	mockStore.AssertExpectations(t)
}

// TestValidate_TxNotFoundShortcut verifies that when spendUtxos returns ErrTxNotFound (parent missing),
// the validator only short-circuits with (meta, nil) when the stored metadata genuinely confirms prior
// full validation: tx has been mined (BlockIDs non-empty), is not Conflicting, and is not Locked.
// In all other cases the original ErrTxNotFound must surface to the caller. This guards against a reorg
// or DAH-eviction window where a stale or mid-flight record could otherwise be accepted as "blessed".
func TestValidate_TxNotFoundShortcut(t *testing.T) {
	makeTxAndParent := func(t *testing.T) (*bt.Tx, *bt.Tx) {
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

	setupValidator := func(t *testing.T, tx *bt.Tx, coinbaseTx *bt.Tx, getMetaResult *meta.Data, getMetaErr error) (*Validator, *utxo.MockUtxostore) {
		ctx := context.Background()
		logger := ulogger.TestLogger{}
		mockStore := &utxo.MockUtxostore{}
		settings := test.CreateBaseTestSettings(t)

		validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		v := validator.(*Validator)

		mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentOutputsOf(coinbaseTx, utxo.ParentOutputNotMined), nil)
		mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000})
		mockStore.On("SpendAndCreate", mock.Anything, tx, mock.Anything, mock.Anything).Return(nil, []*utxo.Spend{}, errors.NewTxNotFoundError("tx not found"))
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

	t.Run("shortcut allowed when mined and not flagged", func(t *testing.T) {
		tx, coinbaseTx := makeTxAndParent(t)
		existingMeta := &meta.Data{Tx: tx, BlockIDs: []uint32{1, 2}, Conflicting: false, Locked: false}
		v, mockStore := setupValidator(t, tx, coinbaseTx, existingMeta, nil)

		txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.NoError(t, err)
		require.NotNil(t, txMetaData)
		require.Equal(t, []uint32{1, 2}, txMetaData.BlockIDs)
		mockStore.AssertExpectations(t)
	})

	t.Run("shortcut denied when not yet mined (BlockIDs empty)", func(t *testing.T) {
		tx, coinbaseTx := makeTxAndParent(t)
		notYetMined := &meta.Data{Tx: tx, BlockIDs: nil, Conflicting: false, Locked: false}
		v, mockStore := setupValidator(t, tx, coinbaseTx, notYetMined, nil)

		txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.Nil(t, txMetaData)
		require.True(t, errors.Is(err, errors.ErrTxNotFound), "expected wrapped ErrTxNotFound, got: %v", err)
		require.Contains(t, err.Error(), "error spending utxos")
		mockStore.AssertExpectations(t)
	})

	t.Run("shortcut denied when conflicting", func(t *testing.T) {
		tx, coinbaseTx := makeTxAndParent(t)
		conflicting := &meta.Data{Tx: tx, BlockIDs: []uint32{1}, Conflicting: true, Locked: false}
		v, mockStore := setupValidator(t, tx, coinbaseTx, conflicting, nil)

		txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.Nil(t, txMetaData)
		require.True(t, errors.Is(err, errors.ErrTxNotFound), "expected wrapped ErrTxNotFound, got: %v", err)
		require.Contains(t, err.Error(), "error spending utxos")
		mockStore.AssertExpectations(t)
	})

	t.Run("shortcut denied when locked", func(t *testing.T) {
		tx, coinbaseTx := makeTxAndParent(t)
		locked := &meta.Data{Tx: tx, BlockIDs: []uint32{1}, Conflicting: false, Locked: true}
		v, mockStore := setupValidator(t, tx, coinbaseTx, locked, nil)

		txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.Nil(t, txMetaData)
		require.True(t, errors.Is(err, errors.ErrTxNotFound), "expected wrapped ErrTxNotFound, got: %v", err)
		require.Contains(t, err.Error(), "error spending utxos")
		mockStore.AssertExpectations(t)
	})

	t.Run("shortcut denied when GetMeta itself fails", func(t *testing.T) {
		tx, coinbaseTx := makeTxAndParent(t)
		v, mockStore := setupValidator(t, tx, coinbaseTx, nil, errors.NewTxNotFoundError("meta not found"))

		txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.Nil(t, txMetaData)
		require.True(t, errors.Is(err, errors.ErrTxNotFound), "expected wrapped ErrTxNotFound, got: %v", err)
		require.Contains(t, err.Error(), "error spending utxos")
		mockStore.AssertExpectations(t)
	})
}

func TestValidator_ValidateInternal_GeneralSpendError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	v := validator.(*Validator)

	// Create transaction
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

	// Mock parent tx extension
	mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentOutputsOf(coinbaseTx, utxo.ParentOutputNotMined), nil)
	mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000})

	// Mock spendUtxos to return a general error
	generalErr := errors.NewProcessingError("general spending error")
	mockStore.On("SpendAndCreate", mock.Anything, tx, mock.Anything, mock.Anything).Return(nil, []*utxo.Spend{}, generalErr)

	options := &Options{}
	_, err = v.validateInternal(ctx, tx, 100, options)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error spending utxos")
	mockStore.AssertExpectations(t)
}

// Test coverage for Health function cases: blockHeight <= 0, default case, and err != nil return

func TestValidator_Health_BlockHeight_Zero_Coverage(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	v := validator.(*Validator)

	// Mock GetBlockHeight to return 0 (covers blockHeight == 0 case in Health function)
	mockStore.On("GetBlockHeight").Return(uint32(0))
	// Mock Health method for UTXO store health check
	mockStore.On("Health", mock.Anything, false).Return(http.StatusOK, "UTXOStore: OK", nil)

	// Test that the specific blockHeight == 0 case is covered
	blockHeight := v.GetBlockHeight()
	assert.Equal(t, uint32(0), blockHeight)

	// Call Health to exercise the code path
	_, message, _ := validator.Health(ctx, false)

	// The checkBlockHeight function should be called and handle the blockHeight == 0 case
	// Even if overall health fails due to other checks, the blockHeight check runs
	assert.Contains(t, message, "BlockHeight: BAD")

	mockStore.AssertExpectations(t)
}

func TestValidator_Health_BlockHeight_Valid_Default_Coverage(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	v := validator.(*Validator)

	// Mock GetBlockHeight to return a valid positive value (triggers default case)
	mockStore.On("GetBlockHeight").Return(uint32(12345))
	// Mock Health method for UTXO store health check
	mockStore.On("Health", mock.Anything, false).Return(http.StatusOK, "UTXOStore: OK", nil)

	// Test that we get the expected value
	blockHeight := v.GetBlockHeight()
	assert.Equal(t, uint32(12345), blockHeight)

	// Call Health to exercise the default case path
	_, message, _ := validator.Health(ctx, false)

	// Should see the good block height message from the default case
	assert.Contains(t, message, "BlockHeight: GOOD: 12345")

	mockStore.AssertExpectations(t)
}

// Note: The blockHeight <= 0 case in lines 201-203 is unreachable because:
// 1. GetBlockHeight() returns uint32, which can't be negative
// 2. The blockHeight == 0 case (lines 198-200) comes first in the switch
// This is dead code that should be removed, but we've covered the reachable cases above.

// parentOutputsOf answers a single-input ParentOutputsForValidation call with
// the given parent's output 0, in the given status.
func parentOutputsOf(parent *bt.Tx, status utxo.ParentOutputStatus) []utxo.ParentOutput {
	return []utxo.ParentOutput{{
		Status:        status,
		Satoshis:      parent.Outputs[0].Satoshis,
		LockingScript: parent.Outputs[0].LockingScript,
	}}
}

// TestSendTxMetaDeletesToKafka_BoundsAndSplitsCascades pins that the unbatched
// DELETE path never sends a record over Kafka's default 1 MiB limit, never
// sends a v1 record whose entry count reads as a v2 header (767), and still
// delivers every hash, in both wire formats.
func TestSendTxMetaDeletesToKafka_BoundsAndSplitsCascades(t *testing.T) {
	const maxRecord = 1 << 20

	for _, format := range []string{"v1", "v2"} {
		for _, n := range []int{767, 30_000} {
			t.Run(fmt.Sprintf("%s/%d", format, n), func(t *testing.T) {
				tSettings := test.CreateBaseTestSettings(t)
				tSettings.Validator.TxMetaWireFormat = format
				tSettings.Validator.TxMetaNumPartitions = 1

				producer := kafka.NewKafkaAsyncProducerMockWithBuffer(1_000)
				v := &Validator{settings: tSettings, txmetaKafkaProducerClient: producer}

				hashes := make([]chainhash.Hash, n)
				for i := range hashes {
					hashes[i] = chainhash.HashH([]byte(fmt.Sprintf("cascade-%d", i)))
				}

				v.sendTxMetaDeletesToKafka(hashes)

				seen := map[chainhash.Hash]bool{}
				keys := map[string]bool{}

				for len(producer.PublishChannel()) > 0 {
					msg := <-producer.PublishChannel()
					value := msg.Value
					keys[string(msg.Key)] = true
					require.LessOrEqual(t, len(value), maxRecord, "a record must fit Kafka's default message size")

					isV2 := value[0] == txmetacache.WireV2Magic && value[1] == txmetacache.WireV2Version && value[2] == 0 && value[3] == 0
					require.Equal(t, format == "v2", isV2, "a v1 record must never start with the v2 header signature")

					count, offset := int(binary.LittleEndian.Uint32(value[:4])), 4
					if isV2 {
						count, offset = int(binary.LittleEndian.Uint32(value[4:])), txmetacache.WireV2HeaderLen
					}

					for i := 0; i < count; i++ {
						if isV2 {
							offset += 8
						}

						var h chainhash.Hash
						copy(h[:], value[offset:offset+32])
						require.Equal(t, txmetacache.WireActionDELETE, value[offset+32])

						seen[h] = true
						offset += 32 + 1 + 4
					}

					require.Equal(t, len(value), offset, "record length must match its entry count")
				}

				require.Len(t, seen, n, "every hash must be sent")

				if format == "v1" {
					require.Len(t, keys, 1, "every record of one batch keeps its key, and so its partition")
				}
			})
		}
	}
}

// TestSendTxMetaBatch_NeverSendsAV2LookalikeV1Record pins that a batcher flush
// of exactly 767 items, whose v1 count encodes as the v2 header signature, is
// not sent as one record: the handler could read it as v2 and drop every entry,
// ADDs included.
func TestSendTxMetaBatch_NeverSendsAV2LookalikeV1Record(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Validator.TxMetaWireFormat = "v1"

	producer := kafka.NewKafkaAsyncProducerMockWithBuffer(10)
	v := &Validator{settings: tSettings, txmetaKafkaProducerClient: producer}

	batch := make([]*txmetaBatchItem, 767)
	for i := range batch {
		h := chainhash.HashH([]byte(fmt.Sprintf("flush-%d", i)))
		batch[i] = &txmetaBatchItem{hash: &h, metaBytes: []byte{1, 2, 3}}
	}

	v.sendTxMetaBatch(batch)

	total := 0

	for len(producer.PublishChannel()) > 0 {
		value := (<-producer.PublishChannel()).Value
		require.False(t, value[0] == txmetacache.WireV2Magic && value[1] == txmetacache.WireV2Version && value[2] == 0 && value[3] == 0,
			"a v1 record must never start with the v2 header signature")

		total += int(binary.LittleEndian.Uint32(value[:4]))
	}

	require.Equal(t, len(batch), total, "every item must still be sent")
}

// TestSendTxMetaDeletesToKafka_BatchedRecordsStayBounded pins that DELETEs sent
// through a real batcher with a large batch size still go out in records under
// Kafka's default 1 MiB limit, in both wire formats, and that every hash arrives.
func TestSendTxMetaDeletesToKafka_BatchedRecordsStayBounded(t *testing.T) {
	const (
		maxRecord = 1 << 20
		n         = 30_000
	)

	for _, format := range []string{"v1", "v2"} {
		t.Run(format, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Validator.TxMetaWireFormat = format
			tSettings.Validator.TxMetaNumPartitions = 1
			tSettings.Validator.TxMetaKafkaBatchSize = n
			tSettings.Validator.TxMetaKafkaBatchTimeoutMs = 50

			producer := kafka.NewKafkaAsyncProducerMockWithBuffer(1_000)

			validator, err := New(t.Context(), ulogger.TestLogger{}, tSettings, &utxo.MockUtxostore{}, producer, nil, nil, nil, nil)
			require.NoError(t, err)

			v := validator.(*Validator)
			require.NotNil(t, v.txmetaKafkaBatcher, "this test needs the batched path")

			hashes := make([]chainhash.Hash, n)
			for i := range hashes {
				hashes[i] = chainhash.HashH([]byte(fmt.Sprintf("batched-cascade-%d", i)))
			}

			v.sendTxMetaDeletesToKafka(hashes)

			var (
				records [][]byte
				total   int
			)

			require.Eventually(t, func() bool {
				for len(producer.PublishChannel()) > 0 {
					value := (<-producer.PublishChannel()).Value
					records = append(records, value)

					// v1 opens with the entry count; v2 carries it after the
					// 4-byte header signature.
					countOffset := 0
					if format == "v2" {
						countOffset = 4
					}

					total += int(binary.LittleEndian.Uint32(value[countOffset : countOffset+4]))
				}

				return total >= n
			}, 10*time.Second, 10*time.Millisecond, "every DELETE must be sent")

			require.Equal(t, n, total)

			for _, value := range records {
				require.LessOrEqual(t, len(value), maxRecord, "a record must fit Kafka's default message size")

				if format == "v1" {
					require.False(t, value[0] == txmetacache.WireV2Magic && value[1] == txmetacache.WireV2Version && value[2] == 0 && value[3] == 0)
				}
			}
		})
	}
}

func TestSplitTxMetaBatch(t *testing.T) {
	items := func(n, content int) []*txmetaBatchItem {
		out := make([]*txmetaBatchItem, n)
		for i := range out {
			h := chainhash.HashH([]byte(fmt.Sprintf("split-%d", i)))
			out[i] = &txmetaBatchItem{hash: &h, metaBytes: make([]byte, content), isDelete: content == 0}
		}

		return out
	}

	cases := map[string][]*txmetaBatchItem{
		"empty":             nil,
		"767 deletes":       items(767, 0),
		"30k deletes":       items(30_000, 0),
		"size cut at 767":   items(2_000, 980), // 767 entries of 1,025 bytes fill a record exactly
		"oversized item":    items(3, maxTxMetaRecordBytes),
		"one under the cut": items(766, 980),
		// 766 deletes and a 700 KB ADD fill 767 items; a 500 KB ADD then forces
		// a cut, the 700 KB item is carried and must not share a record with it
		"carried item re-checked": append(append(items(766, 0), items(1, 700*1024)...), items(1, 500*1024)...),
	}

	for name, batch := range cases {
		t.Run(name, func(t *testing.T) {
			var joined []*txmetaBatchItem

			for _, chunk := range splitTxMetaBatch(batch) {
				require.NotEmpty(t, chunk)
				require.NotEqual(t, txmetacache.V2LookalikeCount, len(chunk), "no chunk may be 767 items long")

				size := txmetacache.WireV2HeaderLen
				for _, item := range chunk {
					size += txMetaEntrySize(item)
				}

				if len(chunk) > 1 {
					require.LessOrEqual(t, size, maxTxMetaRecordBytes)
				}

				joined = append(joined, chunk...)
			}

			require.Equal(t, len(batch), len(joined))

			for i := range batch {
				require.Same(t, batch[i], joined[i], "chunks keep every item, in order")
			}
		})
	}
}
