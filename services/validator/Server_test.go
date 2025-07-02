package validator

import (
	"bytes"
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/pkg/go-subtree"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

var (
	sampleTx, _ = hex.DecodeString("01000000016ec78dc364a3a911b38b32e58e5c228030f172da7d550e699939f6f3771f7f63010000006b483045022100dc6378291c4f8e06a54b0b60701cff7719c985db20537cd16d0350abe2f749660220694d67764dd8501e32190e6209a48103e3c7b202bb8c7682fc4c86e890c58409412103c3e59e22c5a32e54183a43993e8f584f709785f440fbf6f960551cb32041c5a9ffffffff03d0070000000000001976a9149e10b4a781c5be9f67367c3994fb3419aafd358e88ac2a240c00000000001976a914c52f8797b57f0b0cfc5856a5dd4a6f491a41822c88ac00000000000000000a006a075354554b2e434f00000000")
)

func TestHTTPServer_Endpoints(t *testing.T) {
	// Create test context
	ctx := context.Background()

	// Setup the validator server with mocked dependencies
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings()
	tSettings.Policy.MinMiningTxFee = 0
	tSettings.BlockAssembly.Disabled = true

	// Create empty mock implementations
	utxoStore := &utxo.MockUtxostore{}
	blockchainClient := &blockchain.Mock{}

	// Create server instance
	server := NewServer(logger, tSettings, utxoStore, blockchainClient, nil, nil, nil, nil)

	// Create a mock validator to replace the real one
	txid, _ := chainhash.NewHashFromStr("63f7f771376f9f9369e650d7a72d1f0328c2e5582eb3381b913a4a36dc78ec6e")
	mockValidator := &TestMockValidator{
		validateTxFunc: func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
			// Check if this is an invalid transaction by seeing if it has inputs and outputs
			if len(tx.Inputs) == 0 || len(tx.Outputs) == 0 {
				return nil, echo.NewHTTPError(http.StatusBadRequest, "invalid transaction: no inputs or outputs")
			}

			return &meta.Data{
				Fee:         32279815860,
				SizeInBytes: 245,
				TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{*txid}, Idxs: [][]uint32{{0}}},
			}, nil
		},
	}

	// Replace the real validator with our mock
	server.validator = mockValidator

	// Setup Echo server for testing without starting actual HTTP server
	e := echo.New()
	e.HideBanner = true
	e.POST("/tx", server.handleSingleTx(ctx))
	e.POST("/txs", server.handleMultipleTx(ctx))

	// Create valid transaction for testing
	validTx, err := bt.NewTxFromBytes(sampleTx)
	require.NoError(t, err)

	// Create an invalid transaction (missing inputs/outputs)
	invalidTx := bt.NewTx()

	t.Run("Valid transaction", func(t *testing.T) {
		// Create a request with a valid transaction
		req := httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader(validTx.ExtendedBytes()))
		rec := httptest.NewRecorder()

		req.Header.Set(echo.HeaderContentType, echo.MIMETextPlain)

		// Execute the request
		c := e.NewContext(req, rec)
		err := server.handleSingleTx(ctx)(c)

		// Assert response
		require.NoError(t, err, "Expected no error")
		require.Equal(t, http.StatusOK, rec.Code, "Expected status code %d, got %d", http.StatusOK, rec.Code)
		require.Equal(t, "OK", rec.Body.String(), "Expected body 'OK', got %q", rec.Body.String())
	})

	t.Run("Invalid transaction", func(t *testing.T) {
		// Create a request with an invalid transaction
		req := httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader(invalidTx.ExtendedBytes()))
		rec := httptest.NewRecorder()

		req.Header.Set(echo.HeaderContentType, echo.MIMETextPlain)

		// Execute the request
		c := e.NewContext(req, rec)

		// Call the handler directly to see the error
		err := server.handleSingleTx(ctx)(c)

		// For invalid transactions, we should either get an explicit error or a non-200 status code
		if err != nil {
			// If the handler returns an error directly, it's expected behavior
			t.Logf("Handler returned error as expected: %v", err)

			// Check if it's an HTTP error with the expected status code
			httpErr, ok := err.(*echo.HTTPError)
			if ok {
				require.Equal(t, http.StatusBadRequest, httpErr.Code, "Expected status code %d, got %d", http.StatusBadRequest, httpErr.Code)
			}
		} else {
			// If no error was returned, the response code should not be 200 OK
			require.NotEqual(t, http.StatusOK, rec.Code, "Expected status code not to be %d", http.StatusOK)
		}
	})

	t.Run("/txs endpoint - multiple transactions", func(t *testing.T) {
		// Create request with multiple transactions
		var buf bytes.Buffer

		buf.Write(validTx.ExtendedBytes())
		buf.Write(validTx.ExtendedBytes()) // Add another copy

		req := httptest.NewRequest(http.MethodPost, "/txs", &buf)
		rec := httptest.NewRecorder()

		// Process the request
		c := e.NewContext(req, rec)
		err := server.handleMultipleTx(ctx)(c)

		// Verify response
		require.NoError(t, err, "Expected no error")
		require.Equal(t, http.StatusOK, rec.Code, "Expected status code %d, got %d", http.StatusOK, rec.Code)
		require.Equal(t, "OK", rec.Body.String(), "Expected body 'OK', got %q", rec.Body.String())
	})

	// Test with a larger transaction to ensure the HTTP endpoint handles it properly
	t.Run("Large transaction handling", func(t *testing.T) {
		// Create a larger transaction by adding several outputs
		largeTx := validTx.Clone()

		// Add multiple outputs to make it larger than default gRPC message size
		for i := 0; i < 100; i++ {
			err := largeTx.AddP2PKHOutputFromAddress("1CgPCp3E9399ZBukMNmgT4GXwrMZ94Zpyj", 1000)
			require.NoError(t, err)
		}

		req := httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader(largeTx.ExtendedBytes()))
		rec := httptest.NewRecorder()

		req.Header.Set(echo.HeaderContentType, echo.MIMETextPlain)

		c := e.NewContext(req, rec)
		err := server.handleSingleTx(ctx)(c)

		// Should process successfully despite size
		require.NoError(t, err, "Expected no error")
		require.Equal(t, http.StatusOK, rec.Code, "Expected status code %d, got %d", http.StatusOK, rec.Code)
		require.Equal(t, "OK", rec.Body.String(), "Expected body 'OK', got %q", rec.Body.String())
	})
}

func TestValidatorHTTP_Endpoints(t *testing.T) {
	// Create test context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup test server with mocked dependencies
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings()
	tSettings.Policy.MinMiningTxFee = 0
	tSettings.BlockAssembly.Disabled = true

	// Create empty mock implementations
	utxoStore := &utxo.MockUtxostore{}
	blockchainClient := &blockchain.Mock{}

	// Create server instance
	server := NewServer(logger, tSettings, utxoStore, blockchainClient, nil, nil, nil, nil)

	// Create a mock validator to replace the real one
	txid, _ := chainhash.NewHashFromStr("63f7f771376f9f9369e650d7a72d1f0328c2e5582eb3381b913a4a36dc78ec6e")
	mockValidator := &TestMockValidator{
		validateTxFunc: func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
			return &meta.Data{
				Fee:         32279815860,
				SizeInBytes: 245,
				TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{*txid}, Idxs: [][]uint32{{0}}},
			}, nil
		},
	}

	// Replace the real validator with our mock
	server.validator = mockValidator

	// Create Echo server for testing
	e := echo.New()
	e.HideBanner = true

	// Register HTTP endpoint handlers
	e.POST("/tx", server.handleSingleTx(ctx))
	e.POST("/txs", server.handleMultipleTx(ctx))

	// Create test transaction
	testTx, err := bt.NewTxFromBytes(sampleTx)
	require.NoError(t, err)

	t.Run("/tx endpoint - single transaction", func(t *testing.T) {
		// Create request with single transaction
		req := httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader(testTx.ExtendedBytes()))
		rec := httptest.NewRecorder()

		req.Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)

		// Process the request
		c := e.NewContext(req, rec)
		err := server.handleSingleTx(ctx)(c)

		// Verify response
		require.NoError(t, err, "Expected no error")
		require.Equal(t, http.StatusOK, rec.Code, "Expected status code %d, got %d", http.StatusOK, rec.Code)
		require.Equal(t, "OK", rec.Body.String(), "Expected body 'OK', got %q", rec.Body.String())
	})

	t.Run("/txs endpoint - multiple transactions", func(t *testing.T) {
		// Create request with multiple transactions
		var buf bytes.Buffer

		buf.Write(testTx.ExtendedBytes())
		buf.Write(testTx.ExtendedBytes()) // Add another copy

		req := httptest.NewRequest(http.MethodPost, "/txs", &buf)
		rec := httptest.NewRecorder()

		req.Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)

		// Process the request
		c := e.NewContext(req, rec)
		err = server.handleMultipleTx(ctx)(c)

		// Verify response
		require.NoError(t, err, "Expected no error")
		require.Equal(t, http.StatusOK, rec.Code, "Expected status code %d, got %d", http.StatusOK, rec.Code)
		require.Equal(t, "OK", rec.Body.String(), "Expected body 'OK', got %q", rec.Body.String())
	})

	t.Run("Large transaction handling", func(t *testing.T) {
		// Create a large transaction
		largeTx := testTx.Clone()

		// Add multiple outputs to make it large
		for i := 0; i < 100; i++ {
			err := largeTx.AddP2PKHOutputFromAddress("1CgPCp3E9399ZBukMNmgT4GXwrMZ94Zpyj", 1000)
			require.NoError(t, err)
		}

		// Test large transaction via /tx endpoint
		req := httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader(largeTx.ExtendedBytes()))
		rec := httptest.NewRecorder()

		req.Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)

		c := e.NewContext(req, rec)
		err := server.handleSingleTx(ctx)(c)

		// Verify response - should handle large transactions
		require.NoError(t, err, "Expected no error")
		require.Equal(t, http.StatusOK, rec.Code, "Expected status code %d, got %d", http.StatusOK, rec.Code)
		require.Equal(t, "OK", rec.Body.String(), "Expected body 'OK', got %q", rec.Body.String())
	})
}

func TestHTTPServerIntegration(t *testing.T) {
	// Create test context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup test server with mocked dependencies
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings()
	tSettings.Validator.HTTPListenAddress = "localhost:0" // Use any available port
	tSettings.Validator.HTTPRateLimit = 1000

	utxoStore := &utxo.MockUtxostore{}
	blockchainClient := &blockchain.Mock{}

	// Create server instance
	server := NewServer(logger, tSettings, utxoStore, blockchainClient, nil, nil, nil, nil)

	// Create a mock validator to replace the real one
	txid, _ := chainhash.NewHashFromStr("63f7f771376f9f9369e650d7a72d1f0328c2e5582eb3381b913a4a36dc78ec6e")
	mockValidator := &TestMockValidator{
		validateTxFunc: func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
			return &meta.Data{
				Fee:         32279815860,
				SizeInBytes: 245,
				TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{*txid}, Idxs: [][]uint32{{0}}},
			}, nil
		},
	}

	// Replace the real validator with our mock
	server.validator = mockValidator

	// Start HTTP server in a separate goroutine
	readyCh := make(chan struct{})
	go func() {
		err := server.startHTTPServer(ctx, "localhost:0")
		if err != nil {
			if err == http.ErrServerClosed {
				server.logger.Infof("http server shutdown")
			} else {
				server.logger.Errorf("failed to start http server: %v", err)
			}
		}

		close(readyCh)
	}()

	// Give server time to start
	<-readyCh
	time.Sleep(100 * time.Millisecond)

	// Test server shutdown (just cancel context since we're not calling the full Start method)
	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestHTTPServerHandlers(t *testing.T) {
	// Create test context
	ctx := context.Background()

	// Create a mock validator for testing
	txid, _ := chainhash.NewHashFromStr("63f7f771376f9f9369e650d7a72d1f0328c2e5582eb3381b913a4a36dc78ec6e")
	mockValidator := &TestMockValidator{
		validateTxFunc: func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
			return &meta.Data{
				Fee:         32279815860,
				SizeInBytes: 245,
				TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{*txid}, Idxs: [][]uint32{{0}}},
			}, nil
		},
	}

	// Create a server instance with the mock validator
	server := &Server{
		logger:     &ulogger.TestLogger{},
		validator:  mockValidator,
		settings:   test.CreateBaseTestSettings(),
		httpServer: echo.New(),
	}

	// Create Echo server for testing
	e := echo.New()
	e.HideBanner = true

	// Register handlers
	e.POST("/tx", server.handleSingleTx(ctx))
	e.POST("/txs", server.handleMultipleTx(ctx))

	// Create test transaction
	testTx, err := bt.NewTxFromBytes(sampleTx)
	if err != nil {
		t.Fatalf("Failed to create test transaction: %v", err)
	}

	t.Run("Single transaction endpoint", func(t *testing.T) {
		// Create request with valid transaction
		req := httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader(testTx.ExtendedBytes()))
		rec := httptest.NewRecorder()

		// Process request
		c := e.NewContext(req, rec)
		if err := server.handleSingleTx(ctx)(c); err != nil {
			t.Fatalf("Handler returned error: %v", err)
		}

		// Verify response
		require.Equal(t, http.StatusOK, rec.Code, "Expected status OK, got %d", rec.Code)
		require.Equal(t, "OK", rec.Body.String(), "Expected body 'OK', got %q", rec.Body.String())
	})

	t.Run("Multiple transactions endpoint", func(t *testing.T) {
		// Create request with multiple transactions
		var buf bytes.Buffer

		buf.Write(testTx.ExtendedBytes())
		buf.Write(testTx.ExtendedBytes()) // Add another copy

		req := httptest.NewRequest(http.MethodPost, "/txs", &buf)
		rec := httptest.NewRecorder()

		// Process request
		c := e.NewContext(req, rec)
		if err := server.handleMultipleTx(ctx)(c); err != nil {
			t.Fatalf("Handler returned error: %v", err)
		}

		// Verify response
		require.Equal(t, http.StatusOK, rec.Code, "Expected status OK, got %d", rec.Code)
		require.Equal(t, "OK", rec.Body.String(), "Expected body 'OK', got %q", rec.Body.String())
	})
}

type TestMockValidator struct {
	validateTxFunc func(ctx context.Context, tx *bt.Tx) (*meta.Data, error)
}

func (m *TestMockValidator) Init(ctx context.Context) error {
	return nil
}

func (m *TestMockValidator) Start(ctx context.Context) error {
	return nil
}

func (m *TestMockValidator) Stop() error {
	return nil
}

func (m *TestMockValidator) Health(ctx context.Context, _ bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

func (m *TestMockValidator) ValidateTx(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
	if m.validateTxFunc != nil {
		return m.validateTxFunc(ctx, tx)
	}

	return &meta.Data{}, nil
}

func (m *TestMockValidator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*meta.Data, error) {
	if m.validateTxFunc != nil {
		return m.validateTxFunc(ctx, tx)
	}

	return &meta.Data{}, nil
}

func (m *TestMockValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (*meta.Data, error) {
	if m.validateTxFunc != nil {
		return m.validateTxFunc(ctx, tx)
	}

	return &meta.Data{}, nil
}

func (m *TestMockValidator) GetBlockHeight() uint32 {
	return 101
}

func (m *TestMockValidator) GetMedianBlockTime() uint32 {
	return uint32(time.Now().Unix()) // nolint:gosec
}

func (m *TestMockValidator) TriggerBatcher() {
	// No-op implementation for testing
}
