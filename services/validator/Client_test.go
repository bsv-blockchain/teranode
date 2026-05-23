package validator

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MockValidatorAPIClient is a mock implementation of validator_api.ValidatorAPIClient interface
type MockValidatorAPIClient struct {
	validateTxFunc         func(ctx context.Context, in *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error)
	validateBatchFunc      func(ctx context.Context, in *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error)
	healthGRPCFunc         func(ctx context.Context, in *validator_api.EmptyMessage) (*validator_api.HealthResponse, error)
	getBlockHeightFunc     func(ctx context.Context, in *validator_api.EmptyMessage) (*validator_api.GetBlockHeightResponse, error)
	getMedianBlockTimeFunc func(ctx context.Context, in *validator_api.EmptyMessage) (*validator_api.GetMedianBlockTimeResponse, error)
}

func (m *MockValidatorAPIClient) ValidateTransaction(ctx context.Context, in *validator_api.ValidateTransactionRequest, opts ...grpc.CallOption) (*validator_api.ValidateTransactionResponse, error) {
	if m.validateTxFunc != nil {
		return m.validateTxFunc(ctx, in)
	}

	return nil, errors.NewProcessingError("not implemented")
}

func (m *MockValidatorAPIClient) ValidateTransactionBatch(ctx context.Context, in *validator_api.ValidateTransactionBatchRequest, opts ...grpc.CallOption) (*validator_api.ValidateTransactionBatchResponse, error) {
	if m.validateBatchFunc != nil {
		return m.validateBatchFunc(ctx, in)
	}

	return nil, errors.NewProcessingError("not implemented")
}

func (m *MockValidatorAPIClient) HealthGRPC(ctx context.Context, in *validator_api.EmptyMessage, opts ...grpc.CallOption) (*validator_api.HealthResponse, error) {
	if m.healthGRPCFunc != nil {
		return m.healthGRPCFunc(ctx, in)
	}

	return nil, errors.NewProcessingError("not implemented")
}

func (m *MockValidatorAPIClient) GetBlockHeight(ctx context.Context, in *validator_api.EmptyMessage, opts ...grpc.CallOption) (*validator_api.GetBlockHeightResponse, error) {
	if m.getBlockHeightFunc != nil {
		return m.getBlockHeightFunc(ctx, in)
	}

	return nil, errors.NewProcessingError("not implemented")
}

func (m *MockValidatorAPIClient) GetMedianBlockTime(ctx context.Context, in *validator_api.EmptyMessage, opts ...grpc.CallOption) (*validator_api.GetMedianBlockTimeResponse, error) {
	if m.getMedianBlockTimeFunc != nil {
		return m.getMedianBlockTimeFunc(ctx, in)
	}

	return nil, errors.NewProcessingError("not implemented")
}

func setupTestClient(t *testing.T, mockClient *MockValidatorAPIClient) (*Client, *httptest.Server) {
	// Create an HTTP test server for HTTP fallback testing
	mockHTTPServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if tx path
		if r.URL.Path == "/tx" {
			w.WriteHeader(http.StatusOK)
			return
		}
		// Check if txs batch path
		if r.URL.Path == "/txs" {
			w.WriteHeader(http.StatusOK)
			return
		}
		// Default response for unexpected paths
		w.WriteHeader(http.StatusNotFound)
	}))

	// Parse the URL
	validatorHTTPAddr, err := url.Parse(mockHTTPServer.URL)
	require.NoError(t, err)

	// Create running atomic
	running := atomic.Bool{}
	running.Store(true)

	// Create client instance
	client := &Client{
		client:            mockClient,
		logger:            &testLogger{t: t},
		running:           &running,
		validatorHTTPAddr: validatorHTTPAddr,
		batchSize:         0, // No batching by default
	}

	return client, mockHTTPServer
}

// Simple test logger implementing ulogger.Logger
type testLogger struct {
	t *testing.T
}

func (l *testLogger) Debug(args ...interface{}) {
	l.t.Log(args...)
}

func (l *testLogger) Debugf(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func (l *testLogger) Info(args ...interface{}) {
	l.t.Log(args...)
}

func (l *testLogger) Infof(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func (l *testLogger) Warn(args ...interface{}) {
	l.t.Log(args...)
}

func (l *testLogger) Warnf(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func (l *testLogger) Error(args ...interface{}) {
	l.t.Log(args...)
}

func (l *testLogger) Errorf(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func (l *testLogger) Fatal(args ...interface{}) {
	l.t.Log(args...)
}

func (l *testLogger) Fatalf(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func (l *testLogger) LogLevel() int {
	return 0 // Debug level
}

func (l *testLogger) SetLogLevel(level string) {
	// No-op for test logger
}

func (l *testLogger) New(service string, options ...ulogger.Option) ulogger.Logger {
	return l
}

func (l *testLogger) Duplicate(options ...ulogger.Option) ulogger.Logger {
	return l
}

func (l *testLogger) WithTraceContext(_ context.Context) ulogger.Logger {
	return l
}

func createTestTransaction(t *testing.T) *bt.Tx {
	rawTx := "0100000001abad53d72f342dd3f338e5e3346b492110d261a66ead1e2478051a30cda38890000000006a4730440220570b3cb3585f04b1bbad82532911441b6189e037d42628cd1391b12912682cb802206d20166f8207be1372c990a2bd8ad7c58908b66f19b706ef2b549c89adebe1de4121020d0fb6753e03f956a9ad3aaf68e0c4abe0f6825883e9768e3f77b73782fc14d5ffffffff010065cd1d000000001976a9144d196d35d2d908914a0395268ac105794510410088ac00000000"
	tx, err := bt.NewTxFromString(rawTx)
	require.NoError(t, err)

	return tx
}

func TestValidateWithOptions_Success(t *testing.T) {
	// Create mock client that returns a successful validation response
	mockClient := &MockValidatorAPIClient{
		validateTxFunc: func(ctx context.Context, in *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
			// Create valid metadata with minimum required length (25 bytes)
			// 8 bytes for Fee
			// 8 bytes for SizeInBytes
			// 1 byte for flags
			// 8 bytes for number of ParentTxHashes (0 in this case)
			metaBytes := make([]byte, 25)

			// Fee (8 bytes)
			binary.LittleEndian.PutUint64(metaBytes[0:8], 1000)

			// SizeInBytes (8 bytes)
			binary.LittleEndian.PutUint64(metaBytes[8:16], 250)

			// Flags (1 byte) - no flags set
			metaBytes[16] = 0

			// ParentTxHashesLen (8 bytes) - 0 parent tx hashes
			binary.LittleEndian.PutUint64(metaBytes[17:25], 0)

			return &validator_api.ValidateTransactionResponse{
				Metadata: metaBytes,
			}, nil
		},
	}

	// Setup test client
	client, server := setupTestClient(t, mockClient)
	defer server.Close()

	// Create test transaction and options
	tx := createTestTransaction(t)
	opts := NewDefaultOptions()

	// Call the method under test
	result, err := client.ValidateWithOptions(context.Background(), tx, 100, opts)

	// Assert results
	assert.NoError(t, err)
	assert.NotNil(t, result) // We should get metadata
}

func TestValidateWithOptions_Error(t *testing.T) {
	// Create mock client that returns an error
	mockClient := &MockValidatorAPIClient{
		validateTxFunc: func(ctx context.Context, in *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
			return nil, status.Error(codes.InvalidArgument, "invalid transaction")
		},
	}

	// Setup test client
	client, server := setupTestClient(t, mockClient)
	defer server.Close()

	// Create test transaction and options
	tx := createTestTransaction(t)
	opts := NewDefaultOptions()

	// Call the method under test
	result, err := client.ValidateWithOptions(context.Background(), tx, 100, opts)

	// Assert results
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestValidateWithOptions_HTTPFallback(t *testing.T) {
	// Create mock client that returns a ResourceExhausted error to trigger HTTP fallback
	mockClient := &MockValidatorAPIClient{
		validateTxFunc: func(ctx context.Context, in *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "message too large")
		},
	}

	// Setup test client
	client, server := setupTestClient(t, mockClient)
	defer server.Close()

	// Create test transaction and options
	tx := createTestTransaction(t)
	opts := NewDefaultOptions()

	// Call the method under test
	result, err := client.ValidateWithOptions(context.Background(), tx, 100, opts)

	// Assert results
	assert.NoError(t, err) // HTTP fallback should succeed
	assert.Nil(t, result)  // HTTP fallback doesn't return metadata
}

func TestHandleValidationError_NotResourceExhausted(t *testing.T) {
	client, server := setupTestClient(t, &MockValidatorAPIClient{})
	defer server.Close()

	// Create a non-ResourceExhausted error
	originalErr := status.Error(codes.Internal, "internal server error")
	tx := createTestTransaction(t)
	opts := NewDefaultOptions()

	// Call the method under test
	resultErr := client.handleValidationError(context.Background(), tx, 100, opts, originalErr)

	// The method should just unwrap and return the original error
	assert.Error(t, resultErr)
	assert.Contains(t, resultErr.Error(), "internal server error")
}

func TestHandleValidationError_HTTPFallbackSuccess(t *testing.T) {
	client, server := setupTestClient(t, &MockValidatorAPIClient{})
	defer server.Close()

	// Create a ResourceExhausted error
	originalErr := status.Error(codes.ResourceExhausted, "message too large")
	tx := createTestTransaction(t)
	opts := NewDefaultOptions()

	// Call the method under test
	resultErr := client.handleValidationError(context.Background(), tx, 100, opts, originalErr)

	// The HTTP fallback should succeed and return nil error
	assert.NoError(t, resultErr)
}

func TestHandleValidationError_HTTPServerFail(t *testing.T) {
	// Create client with an invalid HTTP URL to force HTTP failure
	mockClient := &MockValidatorAPIClient{}
	client, server := setupTestClient(t, mockClient)
	server.Close() // Close server to force HTTP failure

	// Create a ResourceExhausted error
	originalErr := status.Error(codes.ResourceExhausted, "message too large")
	tx := createTestTransaction(t)
	opts := NewDefaultOptions()

	// Call the method under test
	resultErr := client.handleValidationError(context.Background(), tx, 100, opts, originalErr)

	// The method should return an error due to HTTP failure
	assert.Error(t, resultErr)
}

func TestBatchValidation(t *testing.T) {
	// Create mock client that returns batch responses
	mockClient := &MockValidatorAPIClient{
		validateBatchFunc: func(ctx context.Context, in *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error) {
			// Example batch response with one success and one error
			terrors := make([]*errors.TError, len(in.Transactions))
			metadata := make([][]byte, len(in.Transactions))

			// First tx successful, second fails
			terrors[0] = nil // Success
			metadata[0] = []byte{1, 2, 3, 4}

			if len(terrors) > 1 {
				// Create a proper TError
				errObj := errors.NewProcessingError("validation failed for tx 2")
				terrors[1] = errors.Wrap(errObj)
				metadata[1] = nil
			}

			return &validator_api.ValidateTransactionBatchResponse{
				Errors:   terrors,
				Metadata: metadata,
			}, nil
		},
	}

	// Setup test client with batching enabled
	client, server := setupTestClient(t, mockClient)
	client.batchSize = 2 // Enable batch mode
	defer server.Close()

	// Create test transaction to get valid transaction data
	tx := createTestTransaction(t)
	txBytes := tx.Bytes()

	// Create test batch with valid transaction data
	batch := []*batchItem{
		{
			req: &validator_api.ValidateTransactionRequest{
				TransactionData:      txBytes,
				BlockHeight:          100,
				SkipUtxoCreation:     boolPtr(false),
				AddTxToBlockAssembly: boolPtr(true),
				SkipPolicyChecks:     boolPtr(false),
				CreateConflicting:    boolPtr(false),
			},
			done: make(chan validateBatchResponse),
		},
		{
			req: &validator_api.ValidateTransactionRequest{
				TransactionData:      txBytes,
				BlockHeight:          100,
				SkipUtxoCreation:     boolPtr(false),
				AddTxToBlockAssembly: boolPtr(true),
				SkipPolicyChecks:     boolPtr(false),
				CreateConflicting:    boolPtr(false),
			},
			done: make(chan validateBatchResponse),
		},
	}

	// Call the method under test in a goroutine
	go client.sendBatchToValidator(context.Background(), batch)

	// Get the results from the channels
	response1 := <-batch[0].done
	response2 := <-batch[1].done

	// Verify results
	assert.NoError(t, response1.err)
	assert.NotNil(t, response1.metaData)

	assert.Error(t, response2.err)
	assert.Nil(t, response2.metaData)
}

func TestBatchValidation_ResourceExhausted(t *testing.T) {
	// Create mock client that returns ResourceExhausted error
	mockClient := &MockValidatorAPIClient{
		validateBatchFunc: func(ctx context.Context, in *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "message too large")
		},
	}

	// Setup test client with batching enabled
	client, server := setupTestClient(t, mockClient)
	client.batchSize = 2 // Enable batch mode
	defer server.Close()

	// Create test transaction to get valid transaction data
	tx := createTestTransaction(t)
	txBytes := tx.Bytes()

	// Create test batch with valid transaction data
	batch := []*batchItem{
		{
			req: &validator_api.ValidateTransactionRequest{
				TransactionData:      txBytes,
				BlockHeight:          100,
				SkipUtxoCreation:     boolPtr(false),
				AddTxToBlockAssembly: boolPtr(true),
				SkipPolicyChecks:     boolPtr(false),
				CreateConflicting:    boolPtr(false),
			},
			done: make(chan validateBatchResponse),
		},
	}

	// Call the method under test in a goroutine
	go client.sendBatchToValidator(context.Background(), batch)

	// Get the results from the channel
	response := <-batch[0].done

	// Verify results - HTTP fallback should work
	assert.NoError(t, response.err)
	assert.Nil(t, response.metaData)
}

// Helper for creating bool pointers
func boolPtr(b bool) *bool {
	return &b
}
