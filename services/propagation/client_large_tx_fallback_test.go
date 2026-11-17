package propagation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MockPropagationAPIClient implements the PropagationAPIClient interface for testing
type MockPropagationAPIClient struct {
	propagation_api.PropagationAPIClient
	// Configuration options
	MaxAllowedSize int
	// Tracking for assertions
	ProcessedTransactionCount     int
	ProcessedTransactionBatchSize int
	LastProcessedTx               []byte
}

// ProcessTransaction implements the gRPC method, returning ResourceExhausted for large transactions
func (m *MockPropagationAPIClient) ProcessTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest, opts ...grpc.CallOption) (*propagation_api.EmptyMessage, error) {
	m.ProcessedTransactionCount++
	m.LastProcessedTx = req.Tx

	// If transaction is too large, return ResourceExhausted error
	if len(req.Tx) > m.MaxAllowedSize {
		return nil, status.Errorf(codes.ResourceExhausted, "transaction size %d bytes exceeds maximum allowed size %d bytes", len(req.Tx), m.MaxAllowedSize)
	}

	// Normal transaction - return success
	return &propagation_api.EmptyMessage{}, nil
}

// ProcessTransactionBatch implements the gRPC method for batch transactions
func (m *MockPropagationAPIClient) ProcessTransactionBatch(ctx context.Context, req *propagation_api.ProcessTransactionBatchRequest, opts ...grpc.CallOption) (*propagation_api.ProcessTransactionBatchResponse, error) {
	m.ProcessedTransactionBatchSize = len(req.Items)

	// Calculate total size
	totalSize := 0
	for _, item := range req.Items {
		totalSize += len(item.Tx)
	}

	// If batch is too large, return ResourceExhausted error
	if totalSize > m.MaxAllowedSize {
		return nil, status.Errorf(codes.ResourceExhausted, "batch size %d bytes exceeds maximum allowed size %d bytes", totalSize, m.MaxAllowedSize)
	}

	// Normal batch - return success with no errors
	response := &propagation_api.ProcessTransactionBatchResponse{
		Errors: make([]*errors.TError, len(req.Items)),
	}

	for i := range response.Errors {
		response.Errors[i] = &errors.TError{}
	}

	return response, nil
}

// TestClientLargeTransactionFallback tests the client-side HTTP fallback mechanism for large transactions.
// This test verifies that the propagation client automatically switches from gRPC to HTTP transport
// when transactions exceed the gRPC message size limit. It validates client fallback logic, HTTP request
// handling, and ensures seamless transaction delivery regardless of size constraints.
func TestClientLargeTransactionFallback(t *testing.T) {
	// Setup mock validator HTTP server
	validatorHTTPCalls := 0
	validatorTxCalls := 0
	validatorTxsBatchCalls := 0

	mockHTTPServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		validatorHTTPCalls++

		switch r.URL.Path {
		case "/tx":
			validatorTxCalls++
		case "/txs":
			validatorTxsBatchCalls++
		default:
			t.Errorf("Unexpected HTTP endpoint called: %s", r.URL.Path)
		}

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("OK"))
		require.NoError(t, err)
	}))
	defer mockHTTPServer.Close()

	// Create mock gRPC client
	mockGRPCClient := &MockPropagationAPIClient{
		MaxAllowedSize: 500 * 1024, // 500KB max for testing
	}

	// Parse the URL
	httpAddr, err := url.Parse(mockHTTPServer.URL)
	if err != nil {
		t.Fatal(err)
	}

	// Create client with mock dependencies
	logger := ulogger.TestLogger{}
	testSettings := &settings.Settings{
		Validator: settings.ValidatorSettings{
			HTTPAddress: httpAddr,
		},
	}

	client := &Client{
		client:              mockGRPCClient,
		conn:                nil, // Not needed for test
		batchSize:           0,   // Disable batching for single tx tests
		logger:              logger,
		settings:            testSettings,
		propagationHTTPAddr: httpAddr,
	}

	t.Run("Small transaction uses gRPC", func(t *testing.T) {
		// Reset counters
		mockGRPCClient.ProcessedTransactionCount = 0
		validatorHTTPCalls = 0

		// Create a small transaction
		smallTx := bt.NewTx()
		err := smallTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		require.NoError(t, err)

		// Process the transaction
		err = client.ProcessTransaction(context.Background(), smallTx)
		require.NoError(t, err)

		// Verify gRPC was used and HTTP fallback was not
		assert.Equal(t, 1, mockGRPCClient.ProcessedTransactionCount, "gRPC client should be used")
		assert.Equal(t, 0, validatorHTTPCalls, "HTTP fallback should not be used")
	})

	t.Run("Large transaction falls back to HTTP", func(t *testing.T) {
		// Reset counters
		mockGRPCClient.ProcessedTransactionCount = 0
		validatorHTTPCalls = 0
		validatorTxCalls = 0

		// Create a large transaction
		largeTx := bt.NewTx()
		// Add a simple P2PKH output first
		err := largeTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		require.NoError(t, err)

		// Create data larger than the mock client's max size
		largeData := make([]byte, 600*1024) // 600KB (exceeds 500KB limit)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		// Add an OP_RETURN output with large data
		err = largeTx.AddOpReturnOutput(largeData)
		require.NoError(t, err)

		// Process the transaction - should trigger fallback
		err = client.ProcessTransaction(context.Background(), largeTx)
		require.NoError(t, err)

		// Verify gRPC was attempted first, then HTTP fallback was used
		assert.Equal(t, 1, mockGRPCClient.ProcessedTransactionCount, "gRPC client should be attempted")
		assert.Equal(t, 1, validatorHTTPCalls, "HTTP fallback should be used")
		assert.Equal(t, 1, validatorTxCalls, "Should call /tx endpoint")
	})

	// Setup client for batch tests
	batchClient := &Client{
		client:              mockGRPCClient,
		conn:                nil, // Not needed for test
		batchSize:           10,  // Set batch size, but we'll call ProcessTransactionBatch directly
		logger:              logger,
		settings:            testSettings,
		propagationHTTPAddr: httpAddr,
	}

	t.Run("Small transaction batch uses gRPC", func(t *testing.T) {
		// Reset counters
		mockGRPCClient.ProcessedTransactionBatchSize = 0
		validatorHTTPCalls = 0

		// Create a batch of small transactions sharing one completion group.
		batch := make([]*batchItem, 3)
		group := completion.NewGroup(int32(len(batch)))

		for i := 0; i < 3; i++ {
			smallTx := bt.NewTx()
			err := smallTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
			require.NoError(t, err)

			batch[i] = &batchItem{
				tx:    smallTx,
				group: group,
			}
		}

		// Process the batch
		err := batchClient.ProcessTransactionBatch(context.Background(), batch)
		require.NoError(t, err)

		// Every item must have been completed exactly once.
		require.NoError(t, group.Wait(context.Background(), 0))

		// Verify gRPC was used and HTTP fallback was not
		assert.Equal(t, 3, mockGRPCClient.ProcessedTransactionBatchSize, "gRPC client should be used for batch")
		assert.Equal(t, 0, validatorHTTPCalls, "HTTP fallback should not be used")
	})

	t.Run("Large transaction batch falls back to HTTP", func(t *testing.T) {
		// Reset counters
		mockGRPCClient.ProcessedTransactionBatchSize = 0
		validatorHTTPCalls = 0
		validatorTxsBatchCalls = 0

		// Create a batch with one large transaction sharing one completion group.
		batch := make([]*batchItem, 2)
		group := completion.NewGroup(int32(len(batch)))

		// First transaction is small
		smallTx := bt.NewTx()
		err := smallTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		require.NoError(t, err)

		batch[0] = &batchItem{
			tx:    smallTx,
			group: group,
		}

		// Second transaction is large
		largeTx := bt.NewTx()
		err = largeTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		require.NoError(t, err)

		// Create data larger than the mock client's max size
		largeData := make([]byte, 600*1024) // 600KB (exceeds 500KB limit)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		err = largeTx.AddOpReturnOutput(largeData)
		require.NoError(t, err)

		batch[1] = &batchItem{
			tx:    largeTx,
			group: group,
		}

		// Process the batch - should trigger fallback
		err = batchClient.ProcessTransactionBatch(context.Background(), batch)
		require.NoError(t, err)

		// Every item must have been completed exactly once.
		require.NoError(t, group.Wait(context.Background(), 0))

		// Verify gRPC was attempted first, then HTTP fallback was used
		assert.Equal(t, 2, mockGRPCClient.ProcessedTransactionBatchSize, "gRPC client should be attempted for both transactions")
		assert.Equal(t, 1, validatorHTTPCalls, "HTTP fallback should be used")
		assert.Equal(t, 1, validatorTxsBatchCalls, "Should call /txs endpoint for batch")
	})
}
