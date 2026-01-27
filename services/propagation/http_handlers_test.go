package propagation

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func init() {
	initPrometheusMetrics()
}

// MockValidatorForTxTest is a mock validator implementation for transaction tests
type MockValidatorForTxTest struct {
	validator.MockValidatorClient
	validateCalled atomic.Bool
	validateErr    error
}

// NewMockValidatorForTxTest returns a new instance of MockValidatorForTxTest
func NewMockValidatorForTxTest(validateErr error) *MockValidatorForTxTest {
	return &MockValidatorForTxTest{
		validateErr: validateErr,
	}
}

// Validate implements a mock validator.Validate method
func (m *MockValidatorForTxTest) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...validator.Option) (*meta.Data, error) {
	m.validateCalled.Store(true)

	if m.validateErr != nil {
		return nil, m.validateErr
	}

	return &meta.Data{
		Tx:          tx,
		SizeInBytes: uint64(len(tx.Bytes())),
		Fee:         1000, // Mock fee value
	}, nil
}

// WasValidateCalled returns true if the Validate method was called
func (m *MockValidatorForTxTest) WasValidateCalled() bool {
	return m.validateCalled.Load()
}

// MockTxStore is a simple mock transaction store for testing
type MockTxStore struct {
	storeCalled atomic.Bool
	storeErr    error
	txIDs       [][]byte
}

// Store implements a mock Store method
func (s *MockTxStore) Store(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	s.storeCalled.Store(true)
	s.txIDs = append(s.txIDs, key)

	return s.storeErr
}

// Get implements a mock Get method for blob.Store interface
func (s *MockTxStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	return s.Fetch(ctx, key, fileType, opts...)
}

// Exists implements a mock Exists method matching blob.Store interface
func (s *MockTxStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	for _, id := range s.txIDs {
		if bytes.Equal(id, key) {
			return true, nil
		}
	}

	return false, nil
}

// Fetch implements a mock Fetch method matching blob.Store interface
func (s *MockTxStore) Fetch(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	return []byte{}, nil
}

// GetIoReader implements a mock method for blob.Store interface
func (s *MockTxStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader([]byte{})), nil
}

// Health implements a mock method for blob.Store interface
func (s *MockTxStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

// Set implements a mock method for blob.Store interface
func (s *MockTxStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	return s.Store(ctx, key, fileType, value)
}

// SetFromReader implements a mock method for blob.Store interface
func (s *MockTxStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	// Just return success for the mock
	return nil
}

// SetDAH implements a mock method for blob.Store interface
func (s *MockTxStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error {
	// Just return success for the mock
	return nil
}

// Delete implements a mock Delete method - not part of blob.Store interface
func (s *MockTxStore) Delete(ctx context.Context, key []byte) error {
	return nil
}

// Del implements the blob.Store Del method
func (s *MockTxStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	return s.Delete(ctx, key)
}

// Size implements a mock Size method
func (s *MockTxStore) Size() uint64 {
	return uint64(len(s.txIDs))
}

// Close implements a mock Close method
func (s *MockTxStore) Close(ctx context.Context) error {
	return nil
}

// SetCurrentBlockHeight implements a mock SetCurrentBlockHeight method
func (s *MockTxStore) SetCurrentBlockHeight(_ uint32) {
	// noop
}

// WasStoreCalled returns true if the Store method was called
func (s *MockTxStore) WasStoreCalled() bool {
	return s.storeCalled.Load()
}

// createRobustTestTx creates a transaction suitable for testing
func createRobustTestTx(t *testing.T) *bt.Tx {
	// Use a known valid transaction in hex format that includes inputs and outputs
	rawTx := "0100000001b3807042c92f449bbf79b33ca59d7dfec7f4cc71096704a2e67eead3c4055ef0000000006a473044022039a36013301597daef41fbe593a02cc513d0b55527ec2df1050e2e8ff49c85c202201035fe810e283bcf394485c6a9dfd117ad9f684cdd83d56453cfaa8393240b3b0121026ccfb8061f235cc110697c0bfb3afb99d82c886672f6b9b5393b25a434c0cbf3ffffffff0280f0fa020000000017a914eec2121e48620159c93b62b50677a5c58badaa9987400d03000000000017a914e4133909e8909428c5b131fdab33261ea07649118700000000"

	// Create a new transaction from raw hex
	tx, err := bt.NewTxFromString(rawTx)
	require.NoError(t, err)

	// Ensure transaction has inputs and outputs
	require.NotEmpty(t, tx.Inputs, "Transaction must have inputs")
	require.NotEmpty(t, tx.Outputs, "Transaction must have outputs")

	return tx
}

// MockPropagationServer extends PropagationServer to override specific methods for testing
type MockPropagationServer struct {
	PropagationServer
}

// setupPropagationServer creates a mock propagation server for testing
func setupPropagationServer(t *testing.T, mockValidator validator.Interface, storeErr error) (*MockPropagationServer, *MockTxStore) {
	t.Helper()

	// Initialize mocks
	/*
		mockBlockchainClient := &CustomMockBlockchainClient{
			healthStatus: http.StatusOK,
			healthMsg:    "OK",
		}
	*/
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusOK, "OK", nil)

	// Initialize with a simple mock block (removing model references)
	// mockBlockchainClient.Block = nil

	// Create our mock store
	mockStore := &MockTxStore{
		storeErr: storeErr,
	}

	// Create stats object
	stats := &gocore.Stat{}

	// Create a logger
	logger := ulogger.New("test-logger")

	// Create and return the mock propagation server with dependencies
	ps := &MockPropagationServer{
		PropagationServer: PropagationServer{
			logger:           logger,
			validator:        mockValidator,
			blockchainClient: mockBlockchainClient,
			txStore:          mockStore,
			stats:            stats,
		},
	}

	return ps, mockStore
}

// TestHandleSingleTx tests the HTTP handler for processing a single transaction
func TestHandleSingleTx(t *testing.T) {
	// Create a valid test transaction designed to work with the actual handlers
	tx := createRobustTestTx(t)

	// Create raw transaction bytes that the handler expects
	// Use ExtendedBytes to include previous outputs for extended transactions
	txBytes := tx.ExtendedBytes()

	// Test cases
	testCases := []struct {
		name                string
		requestBody         []byte
		expectedStatusCode  int
		expectedResponse    string
		mockValidationError error
		storeError          error
	}{
		{
			name:               "Valid transaction",
			requestBody:        txBytes,
			expectedStatusCode: http.StatusOK,
			expectedResponse:   "OK",
		},
		{
			name:               "Empty request body",
			requestBody:        []byte{},
			expectedStatusCode: http.StatusInternalServerError,
			expectedResponse:   "Failed to process transaction",
		},
		{
			name:                "Transaction validation error",
			requestBody:         txBytes,
			expectedStatusCode:  http.StatusInternalServerError,
			expectedResponse:    "Failed to process transaction",
			mockValidationError: errors.NewTxInvalidError("test validation error"),
		},
		{
			name:               "Store error",
			requestBody:        txBytes,
			expectedStatusCode: http.StatusInternalServerError,
			expectedResponse:   "Failed to process transaction",
			storeError:         errors.NewStorageError("test store error"),
		},
		{
			name:               "Invalid transaction bytes",
			requestBody:        []byte("invalid-tx-data"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedResponse:   "Failed to process transaction",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create mock validator
			mockValidator := NewMockValidatorForTxTest(tc.mockValidationError)

			// Set up server with mocked dependencies
			ps, storeTracker := setupPropagationServer(t, mockValidator, tc.storeError)

			// Get the ACTUAL handler function - this is what you wanted us to test
			handler := ps.handleSingleTx(context.Background())

			// Setup Echo server
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader(tc.requestBody))
			rec := httptest.NewRecorder()

			req.Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)
			c := e.NewContext(req, rec)

			// Execute the actual handler from the Server
			err := handler(c)
			require.NoError(t, err)

			// Verify response
			assert.Equal(t, tc.expectedStatusCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.expectedResponse)

			// Verify validation and storage behaviors for valid cases only
			if tc.name == "Valid transaction" {
				assert.True(t, mockValidator.WasValidateCalled(), "Validate should be called for valid transactions")
				assert.True(t, storeTracker.WasStoreCalled(), "Store should be called for valid transactions")
			}

			// Even with store errors, the store method should be called
			if tc.name == "Store error" {
				assert.True(t, storeTracker.WasStoreCalled(), "Store should be called even when it returns an error")
			}
		})
	}
}

// TestHandleMultipleTx tests the HTTP handler for processing multiple transactions
func TestHandleMultipleTx(t *testing.T) {
	// Create test transactions that will properly serialize/deserialize
	tx1 := createRobustTestTx(t)
	tx2 := createRobustTestTx(t)

	// Create a buffer with multiple transactions
	var buf bytes.Buffer

	buf.Write(tx1.ExtendedBytes())
	buf.Write(tx2.ExtendedBytes())

	// Test cases
	testCases := []struct {
		name                string
		setupRequestBody    func() *bytes.Buffer
		expectedStatusCode  int
		expectedResponse    string
		mockValidationError error
		storeError          error
	}{
		{
			name: "Multiple valid transactions",
			setupRequestBody: func() *bytes.Buffer {
				return &buf
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   "OK",
		},
		{
			name: "Transaction validation error",
			setupRequestBody: func() *bytes.Buffer {
				var buf bytes.Buffer
				buf.Write(tx1.ExtendedBytes())
				return &buf
			},
			expectedStatusCode:  http.StatusInternalServerError,
			expectedResponse:    "Failed to process transaction",
			mockValidationError: errors.NewTxInvalidError("test validation error"),
		},
		{
			name: "Store error",
			setupRequestBody: func() *bytes.Buffer {
				var buf bytes.Buffer
				buf.Write(tx1.ExtendedBytes())
				return &buf
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedResponse:   "Failed to process transaction",
			storeError:         errors.NewStorageError("test store error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create mock validator
			mockValidator := NewMockValidatorForTxTest(tc.mockValidationError)

			// Set up server with mocked dependencies
			ps, storeTracker := setupPropagationServer(t, mockValidator, tc.storeError)

			// Get the ACTUAL handler function from handleMultipleTx
			handler := ps.handleMultipleTx(context.Background())

			// Setup Echo server with the prepared request body
			e := echo.New()
			reqBody := tc.setupRequestBody()
			req := httptest.NewRequest(http.MethodPost, "/txs", reqBody)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			// Execute the actual handler from the Server
			err := handler(c)

			// Verify results
			require.NoError(t, err)
			assert.Equal(t, tc.expectedStatusCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.expectedResponse)

			// Verify validation and storage behaviors for non-error cases
			if tc.name == "Multiple valid transactions" {
				assert.True(t, mockValidator.WasValidateCalled(), "Validate should be called")
				assert.True(t, storeTracker.WasStoreCalled(), "Store should be called")
			}

			// Even with store errors, the store method should be called
			if tc.name == "Store error" {
				assert.True(t, storeTracker.WasStoreCalled(), "Store should be called even when it returns an error")
			}
		})
	}
}

// TestHTTPIntegration tests the HTTP endpoints via actual HTTP requests
func TestHTTPIntegration(t *testing.T) {
	// Create a test transaction
	tx := createRobustTestTx(t)

	// Create an Echo test server
	e := echo.New()

	// Create minimal mocks
	mockValidator := NewMockValidatorForTxTest(nil)
	mockStore := &MockTxStore{}

	// Create a minimal PropagationServer with just the dependencies needed for the test
	ps := &MockPropagationServer{
		PropagationServer: PropagationServer{
			logger:    ulogger.New("test-logger"),
			validator: mockValidator,
			txStore:   mockStore,
			// blockchainClient: &CustomMockBlockchainClient{},
			blockchainClient: &blockchain.Mock{},
		},
	}

	// Register handlers
	e.POST("/tx", ps.handleSingleTx(context.Background()))
	e.POST("/txs", ps.handleMultipleTx(context.Background()))

	// Start the test server
	server := httptest.NewServer(e)
	defer server.Close()

	// Test single tx endpoint
	t.Run("Single transaction endpoint", func(t *testing.T) {
		// Using ExtendedBytes to include previous outputs for extended transactions
		resp, err := http.Post(server.URL+"/tx", echo.MIMEOctetStream, bytes.NewReader(tx.ExtendedBytes()))
		require.NoError(t, err)
		defer resp.Body.Close()

		// Verify response
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "OK", string(body))

		// Verify validator was called
		assert.True(t, mockValidator.WasValidateCalled(), "Validate should be called")
		assert.True(t, mockStore.WasStoreCalled(), "Store should be called")
	})

	// Test multiple tx endpoint
	t.Run("Multiple transactions endpoint", func(t *testing.T) {
		// Create a buffer with multiple transactions
		tx1 := createRobustTestTx(t)
		tx2 := createRobustTestTx(t)

		var buf bytes.Buffer

		buf.Write(tx1.ExtendedBytes())
		buf.Write(tx2.ExtendedBytes())

		resp, err := http.Post(server.URL+"/txs", echo.MIMEOctetStream, &buf)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Verify response
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "OK", string(body))

		// Verify validator was called
		assert.True(t, mockValidator.WasValidateCalled(), "Validate should be called")
		assert.True(t, mockStore.WasStoreCalled(), "Store should be called")
	})
}
