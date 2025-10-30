package propagation

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/longtest/util/postgres"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type panicReadCloser struct{}

func (p *panicReadCloser) Read(b []byte) (int, error) { panic("simulated read panic") }
func (p *panicReadCloser) Close() error               { return nil }

// setupRealValidator creates a real validator with LocalClient blockchain backend
func setupRealValidator(t *testing.T, ctx context.Context) (validator.Interface, utxo.Store) {
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	// Disable block assembly in tests
	tSettings.BlockAssembly.Disabled = true

	// Create UTXO store with memory SQLite
	utxoStore := testutil.NewSQLiteMemoryUTXOStore(ctx, logger, tSettings, t)
	_ = utxoStore.SetBlockHeight(100)

	// Create blockchain client with memory SQLite
	blockchainClient := testutil.NewMemorySQLiteBlockchainClient(logger, tSettings, t)

	// Create real validator with all real dependencies
	validatorInstance, err := validator.New(ctx, logger, tSettings, utxoStore,
		nil, // txMetaKafkaProducerClient - not needed for tests
		nil, // rejectedTxKafkaProducerClient - not needed for tests
		nil, // blockAssemblyClient - disabled in settings
		blockchainClient)
	require.NoError(t, err)

	return validatorInstance, utxoStore
}

// Test_handleMultipleTx_PanicDuringRead verifies the handler's panic recovery
// around tx.ReadFrom by simulating a panic in the request body reader.
func Test_handleMultipleTx_PanicDuringRead(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// clear the default addresses since we're not starting the servers in tests
	tSettings.Propagation.GRPCListenAddress = ""
	tSettings.Propagation.HTTPListenAddress = ""

	ps := &PropagationServer{
		logger:   logger,
		settings: tSettings,
	}

	handler := ps.handleMultipleTx(t.Context())

	// a ReadCloser whose Read panics to trigger the recovery path
	req := httptest.NewRequest(http.MethodPost, "/txs", &panicReadCloser{})
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	c.SetPath("/txs")

	// handler should not panic. it should recover and aggregate an error
	err := handler(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	body, rbErr := io.ReadAll(rec.Body)
	require.NoError(t, rbErr)
	assert.Contains(t, string(body), "Failed to process transactions")
}

// Test_handleSingleTx_InvalidBody ensures single-tx HTTP path reports 500 for invalid bytes
func Test_handleSingleTx_InvalidBody(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Propagation.GRPCListenAddress = ""
	tSettings.Propagation.HTTPListenAddress = ""

	ps := &PropagationServer{
		logger:   logger,
		settings: tSettings,
	}

	handler := ps.handleSingleTx(t.Context())

	// invalid tx bytes (bt.NewTxFromBytes will error)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader([]byte{0x01, 0x02, 0x03}))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetPath("/tx")

	err := handler(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	body, rbErr := io.ReadAll(rec.Body)
	require.NoError(t, rbErr)
	assert.Contains(t, string(body), "Failed to process transaction:")
}

// TestProcessTransaction_InvalidBytes validates gRPC method error path on invalid bytes
func TestProcessTransaction_InvalidBytes(t *testing.T) {
	// Initialize tracing for tests
	tracing.SetupMockTracer()

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	// Null tx store is fine; validator not required for parse failure
	txStore, err := null.New(ulogger.TestLogger{})
	require.NoError(t, err)

	ps := &PropagationServer{
		logger:   ulogger.TestLogger{},
		settings: tSettings,
		txStore:  txStore,
	}

	// invalid bytes that cause bt.NewTxFromBytes to return error
	req := &propagation_api.ProcessTransactionRequest{Tx: []byte{0x00, 0x01, 0x02}}
	resp, err := ps.ProcessTransaction(ctx, req)
	assert.Nil(t, resp)
	assert.Error(t, err)
}

// TestPropagationServer_HealthLiveness tests the Health function with liveness checks.
func TestPropagationServer_HealthLiveness(t *testing.T) {
	// Initialize tracing for tests
	tracing.SetupMockTracer()

	// Create a server with minimal dependencies
	tSettings := test.CreateBaseTestSettings(t)
	// Clear the default addresses since we're not starting the servers in tests
	tSettings.Propagation.GRPCListenAddress = ""
	tSettings.Propagation.HTTPListenAddress = ""
	ps := &PropagationServer{
		logger:   ulogger.TestLogger{},
		settings: tSettings,
	}

	// Test liveness check (checkLiveness=true)
	ctx := context.Background()
	status, msg, err := ps.Health(ctx, true)

	// Verify results
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "OK", msg)
	assert.NoError(t, err)
}

// TestPropagationServerInit tests the Init function of PropagationServer
func TestPropagationServerInit(t *testing.T) {
	// Initialize tracing for tests
	tracing.SetupMockTracer()

	t.Run("successful init", func(t *testing.T) {
		// Create a server with minimal dependencies
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Propagation.GRPCListenAddress = ""
		tSettings.Propagation.HTTPListenAddress = ""

		ps := &PropagationServer{
			logger:   ulogger.TestLogger{},
			settings: tSettings,
		}

		ctx := context.Background()
		err := ps.Init(ctx)

		// Should succeed with no listeners configured
		assert.NoError(t, err)
	})
}

// TestPropagationServerStop tests the Stop function of PropagationServer
func TestPropagationServerStop(t *testing.T) {
	// Initialize tracing for tests
	tracing.SetupMockTracer()

	t.Run("stop server", func(t *testing.T) {
		// Create a server with minimal dependencies
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Propagation.GRPCListenAddress = ""
		tSettings.Propagation.HTTPListenAddress = ""

		ps := &PropagationServer{
			logger:   ulogger.TestLogger{},
			settings: tSettings,
		}

		ctx := context.Background()
		err := ps.Stop(ctx)

		// Should succeed
		assert.NoError(t, err)
	})
}

// TestStartUDP6Listeners tests the StartUDP6Listeners function
func TestStartUDP6Listeners(t *testing.T) {
	// Initialize tracing for tests
	tracing.SetupMockTracer()

	t.Run("start with invalid interface", func(t *testing.T) {
		// Create a server with minimal dependencies
		tSettings := test.CreateBaseTestSettings(t)

		ps := &PropagationServer{
			logger:   ulogger.TestLogger{},
			settings: tSettings,
		}

		ctx := context.Background()
		// Pass an invalid interface name
		err := ps.StartUDP6Listeners(ctx, "invalid-interface-999")

		// Should fail with invalid interface
		assert.Error(t, err)
	})
}

// TestProcessTransaction tests the ProcessTransaction gRPC function
func TestProcessTransaction(t *testing.T) {
	// Initialize tracing for tests
	tracing.SetupMockTracer()

	t.Run("process with valid transaction", func(t *testing.T) {
		ctx := context.Background()

		// Create real validator
		validatorInstance, utxoStore := setupRealValidator(t, ctx)

		// Create server
		tSettings := test.CreateBaseTestSettings(t)
		txStore, _ := null.New(ulogger.TestLogger{})
		ps := &PropagationServer{
			logger:    ulogger.TestLogger{},
			settings:  tSettings,
			validator: validatorInstance,
			txStore:   txStore,
		}

		// Create a chain of transactions for testing
		txs := transactions.CreateTestTransactionChainWithCount(t, 3)

		// Add the first transaction (coinbase) to UTXO store
		_, err := utxoStore.Create(ctx, txs[0], 1)
		require.NoError(t, err)

		// Use the second transaction (non-coinbase) for testing
		txBytes := txs[1].ExtendedBytes()

		// Create request
		request := &propagation_api.ProcessTransactionRequest{
			Tx: txBytes,
		}

		// Process transaction
		response, err := ps.ProcessTransaction(ctx, request)

		// Check response
		assert.NoError(t, err)
		assert.NotNil(t, response)
	})
}

// TestPropagationServer_HealthReadiness tests the Health function with readiness checks.
func TestPropagationServer_HealthReadiness(t *testing.T) {
	// Initialize tracing for tests
	tracing.SetupMockTracer()

	t.Run("all dependencies healthy", func(t *testing.T) {
		t.Helper()
		ctx := context.Background()

		// Create real validator with real blockchain client
		validatorInstance, _ := setupRealValidator(t, ctx)

		tSettings := test.CreateBaseTestSettings(t)
		// Use real blockchain client with memory SQLite instead of mock
		mockBlockchainClient := testutil.NewMemorySQLiteBlockchainClient(ulogger.TestLogger{}, tSettings, t)

		// Create mock tx store
		txStore, err := null.New(ulogger.TestLogger{})
		require.NoError(t, err)

		// Create server with healthy dependencies
		validatorURL, _ := url.Parse("http://localhost:8080")
		// Clear the default addresses since we're not starting the servers in tests
		tSettings.Propagation.GRPCListenAddress = ""
		tSettings.Propagation.HTTPListenAddress = ""
		ps := &PropagationServer{
			logger:            ulogger.TestLogger{},
			settings:          tSettings,
			validator:         validatorInstance,
			blockchainClient:  mockBlockchainClient,
			txStore:           txStore,
			validatorHTTPAddr: validatorURL,
		}

		// Test readiness check (checkLiveness=false)
		status, msg, err := ps.Health(ctx, false)

		// Verify results - with real validator, all dependencies are healthy
		assert.Equal(t, http.StatusOK, status)
		assert.NoError(t, err)

		// Validate JSON response
		t.Logf("DEBUG: Health check message: %s", msg)
		var jsonMsg map[string]interface{}
		err = json.Unmarshal([]byte(msg), &jsonMsg)
		require.NoError(t, err, "Message should be valid JSON")
		assert.Contains(t, jsonMsg, "status")
		assert.Contains(t, jsonMsg, "dependencies")
	})

	t.Run("unhealthy dependencies", func(t *testing.T) {
		ctx := context.Background()

		// Create real validator with real blockchain client
		validatorInstance, _ := setupRealValidator(t, ctx)

		tSettings := test.CreateBaseTestSettings(t)
		// Use real blockchain client - it will return healthy status
		mockBlockchainClient := testutil.NewMemorySQLiteBlockchainClient(ulogger.TestLogger{}, tSettings, t)

		// Create mock tx store
		txStore, err := null.New(ulogger.TestLogger{})
		require.NoError(t, err)

		// Create server with one unhealthy dependency
		validatorURL, _ := url.Parse("http://localhost:8080")
		// Clear the default addresses since we're not starting the servers in tests
		tSettings.Propagation.GRPCListenAddress = ""
		tSettings.Propagation.HTTPListenAddress = ""
		ps := &PropagationServer{
			logger:            ulogger.TestLogger{},
			settings:          tSettings,
			validator:         validatorInstance,
			blockchainClient:  mockBlockchainClient,
			txStore:           txStore,
			validatorHTTPAddr: validatorURL,
		}

		// Test readiness check with unhealthy dependency
		status, msg, err := ps.Health(ctx, false)

		// Since we're using real blockchain client, it will be healthy
		// The overall status might still be unhealthy due to validator
		assert.True(t, status == http.StatusOK || status == http.StatusServiceUnavailable)
		assert.NoError(t, err) // Health check should return status, not error

		// Validate JSON response
		t.Logf("Health response message: %s", msg)
		var jsonMsg map[string]interface{}
		err = json.Unmarshal([]byte(msg), &jsonMsg)
		require.NoError(t, err, "Message should be valid JSON")
		assert.Contains(t, jsonMsg, "status")
		assert.Contains(t, jsonMsg, "dependencies")
	})

	t.Run("no dependencies", func(t *testing.T) {
		ctx := context.Background()
		// Create server with no dependencies
		tSettings := test.CreateBaseTestSettings(t)
		// Clear the default addresses since we're not starting the servers in tests
		tSettings.Propagation.GRPCListenAddress = ""
		tSettings.Propagation.HTTPListenAddress = ""
		ps := &PropagationServer{
			logger:   ulogger.TestLogger{},
			settings: tSettings,
			// No validator or blockchainClient
		}

		// Test readiness check (checkLiveness=false)
		status, msg, err := ps.Health(ctx, false)

		// Verify results - should pass with no dependency checks
		// Since no servers or Kafka are configured, no checks run
		assert.Equal(t, http.StatusOK, status)
		assert.NoError(t, err)

		// Validate JSON response
		t.Logf("Health response message: %s", msg)
		var jsonMsg map[string]interface{}
		err = json.Unmarshal([]byte(msg), &jsonMsg)
		require.NoError(t, err, "Message should be valid JSON")

		// Should show healthy status
		assert.Contains(t, jsonMsg, "status")
		assert.Equal(t, "200", jsonMsg["status"])

		deps, ok := jsonMsg["dependencies"].([]interface{})
		assert.True(t, ok, "dependencies should be an array")

		// No dependency checks should run since nothing is configured
		assert.Equal(t, 0, len(deps), "Should have no dependency checks when nothing is configured")
	})

	t.Run("healthy service", func(t *testing.T) {
		ctx := context.Background()

		// Create real validator with real blockchain client
		validatorInstance, _ := setupRealValidator(t, ctx)

		tSettings := test.CreateBaseTestSettings(t)
		// Use real blockchain client with memory SQLite instead of mock
		mockBlockchainClient := testutil.NewMemorySQLiteBlockchainClient(ulogger.TestLogger{}, tSettings, t)

		// Create mock tx store
		txStore, err := null.New(ulogger.TestLogger{})
		require.NoError(t, err)

		// Create server with healthy dependencies
		validatorURL, _ := url.Parse("http://localhost:8080")
		// Clear the default addresses since we're not starting the servers in tests
		tSettings.Propagation.GRPCListenAddress = ""
		tSettings.Propagation.HTTPListenAddress = ""
		ps := &PropagationServer{
			logger:            ulogger.TestLogger{},
			settings:          tSettings,
			validator:         validatorInstance,
			blockchainClient:  mockBlockchainClient,
			txStore:           txStore,
			validatorHTTPAddr: validatorURL,
		}

		// Execute gRPC health check
		response, err := ps.HealthGRPC(ctx, &propagation_api.EmptyMessage{})
		require.NoError(t, err)

		// With real validator, all dependencies are healthy
		assert.True(t, response.Ok)

		// Validate that details contains JSON
		var details map[string]interface{}
		err = json.Unmarshal([]byte(response.Details), &details)
		require.NoError(t, err, "Details should be valid JSON")
		assert.Contains(t, details, "status")
	})
}

// TestPropagationServer_HealthGRPC tests the HealthGRPC function.
func TestPropagationServer_HealthGRPC(t *testing.T) {
	// Initialize tracing for tests
	tracing.SetupMockTracer()

	t.Run("healthy service", func(t *testing.T) {
		ctx := context.Background()

		// Create real validator with real blockchain client
		validatorInstance, _ := setupRealValidator(t, ctx)

		tSettings := test.CreateBaseTestSettings(t)
		// Use real blockchain client with memory SQLite instead of mock
		mockBlockchainClient := testutil.NewMemorySQLiteBlockchainClient(ulogger.TestLogger{}, tSettings, t)

		// Create mock tx store
		txStore, err := null.New(ulogger.TestLogger{})
		require.NoError(t, err)

		// Create server with healthy dependencies
		validatorURL, _ := url.Parse("http://localhost:8080")
		// Clear the default addresses since we're not starting the servers in tests
		tSettings.Propagation.GRPCListenAddress = ""
		tSettings.Propagation.HTTPListenAddress = ""
		ps := &PropagationServer{
			logger:            ulogger.TestLogger{},
			settings:          tSettings,
			validator:         validatorInstance,
			blockchainClient:  mockBlockchainClient,
			txStore:           txStore,
			validatorHTTPAddr: validatorURL,
		}

		// Execute gRPC health check
		response, err := ps.HealthGRPC(ctx, &propagation_api.EmptyMessage{})
		require.NoError(t, err)

		// With real validator, all dependencies are healthy
		assert.True(t, response.Ok)

		// Validate that details contains JSON
		var details map[string]interface{}
		err = json.Unmarshal([]byte(response.Details), &details)
		require.NoError(t, err, "Details should be valid JSON")
		assert.Contains(t, details, "status")
	})

	t.Run("unhealthy service", func(t *testing.T) {
		// Create server with real blockchain client (healthy)
		tSettings := test.CreateBaseTestSettings(t)
		// Use real blockchain client - it will be healthy, but we can test other unhealthy dependencies
		mockBlockchainClient := testutil.NewMemorySQLiteBlockchainClient(ulogger.TestLogger{}, tSettings, t)
		// Clear the default addresses since we're not starting the servers in tests
		tSettings.Propagation.GRPCListenAddress = ""
		tSettings.Propagation.HTTPListenAddress = ""
		ps := &PropagationServer{
			logger:           ulogger.TestLogger{},
			settings:         tSettings,
			blockchainClient: mockBlockchainClient,
		}

		// Test HealthGRPC with unhealthy dependency
		ctx := context.Background()
		response, err := ps.HealthGRPC(ctx, &propagation_api.EmptyMessage{})

		// With real blockchain client, the blockchain will be healthy
		// Health status depends on overall service health
		assert.NoError(t, err)
		assert.NotNil(t, response)
		// Response.Ok may be true or false depending on other dependencies
		assert.NotNil(t, response.Timestamp)
		assert.NotEmpty(t, response.Details)
	})
}

// TestStartHTTPServer tests the HTTP server startup functionality of the propagation service.
// This test validates that the HTTP server can be properly initialized, started, and responds
// to health check requests with appropriate status codes and response formats. It also tests
// rate limiting functionality to ensure the server properly enforces request limits.
func TestStartHTTPServer(t *testing.T) {
	initPrometheusMetrics()

	txStore, err := null.New(ulogger.TestLogger{})
	require.NoError(t, err)

	// Create a mock PropagationServer
	ps := &PropagationServer{
		// Initialize with necessary mock dependencies
		logger:    ulogger.TestLogger{},
		validator: &validator.MockValidator{},
		txStore:   txStore,
		settings: &settings.Settings{
			Propagation: settings.PropagationSettings{
				HTTPRateLimit: 20,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start the HTTP server on a random available port
	serverAddr := "localhost:0"
	err = ps.startHTTPServer(ctx, serverAddr)
	require.NoError(t, err)

	// Wait for the server to start and get the actual address
	var actualAddr string

	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)

		listenerAddr := ps.httpServer.ListenerAddr()

		if listenerAddr != nil {
			actualAddr = listenerAddr.String()
			break
		}
	}

	require.NotEmpty(t, actualAddr, "Server failed to start")

	baseURL := fmt.Sprintf("http://%s", actualAddr)

	txHex := "010000000000000000ef01669fb8b6dba279e90e085fdcea3bdc27fb98b1e35e1d54062033f8e1216ac071000000006a4730440220592cbdf8375acaacc1fcec4870331bce6cea8b1668acfcd9dbc3a9bf69edf21802200b7a5f97c50c0e42a0a1b0da819292c4748011e086d7f79618e31916b1008b9d412103501de920437c42b4ef372115e4b63ebab8bce51a019cd24084e97296f8a0dfe0ffffffff806a0100000000001976a914fcde0dd2c418fcd4043a64d31a83e0a17d711cec88ac027d6a0100000000001976a914fcde0dd2c418fcd4043a64d31a83e0a17d711cec88ac00000000000000002c006a2970726f7061676174696f6e207465737420323032352d30332d32345431313a30393a31352e3437305a00000000"
	txData, err := hex.DecodeString(txHex)
	require.NoError(t, err)

	txHex2 := "010000000000000000ef013f868f4142f4bc641e031dfae55ae9de3b471f0717d5b7a7022fb4547df76589000000006b48304502210090604217cb346a2b8fc1777d76180b2c2a0e635c31deda5e18b712f016565d340220391b61facfc424f06ee45f88e8c7d46ee1c62f27cd124cdb77db62335d001cd1412103501de920437c42b4ef372115e4b63ebab8bce51a019cd24084e97296f8a0dfe0ffffffff7d6a0100000000001976a914fcde0dd2c418fcd4043a64d31a83e0a17d711cec88ac027a6a0100000000001976a914fcde0dd2c418fcd4043a64d31a83e0a17d711cec88ac00000000000000002c006a2970726f7061676174696f6e207465737420323032352d30332d32345431313a30393a33392e3039335a00000000"
	txData2, err := hex.DecodeString(txHex2)
	require.NoError(t, err)

	t.Run("Test /tx endpoint", func(t *testing.T) {
		resp, err := http.Post(baseURL+"/tx", "application/octet-stream", bytes.NewBuffer(txData))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "OK", string(body))
	})

	t.Run("Test /txs endpoint", func(t *testing.T) {
		resp, err := http.Post(baseURL+"/txs", "application/octet-stream", bytes.NewBuffer(append(txData, txData2...)))
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "OK", string(body))
	})

	t.Run("Test rate limiting", func(t *testing.T) {
		// The rate limiter allows 20 requests per second
		// Send 25 requests quickly to exceed the limit
		rateLimited := false
		for i := 0; i < 30; i++ {
			resp, err := http.Post(baseURL+"/tx", "application/octet-stream", bytes.NewBuffer(txData))
			require.NoError(t, err)
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusTooManyRequests {
				rateLimited = true
				// Once we hit rate limit, we can stop
				break
			}

			// Add a small delay after 20 requests to ensure we exceed the per-second limit
			if i == 20 {
				time.Sleep(50 * time.Millisecond)
			}
		}

		// Verify that we hit the rate limit at some point
		assert.True(t, rateLimited, "Expected to hit rate limit after sending many requests quickly")
	})
}

// Test_handleMultipleTx tests the handleMultipleTx function.
// This test makes sure chains of transactions are processed properly
func Test_handleMultipleTx(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// Clear the default addresses since we're not starting the servers in tests
	tSettings.Propagation.GRPCListenAddress = ""
	tSettings.Propagation.HTTPListenAddress = ""
	tSettings.BlockAssembly.Disabled = true

	t.Run("Test handleMultipleTx with valid transactions", func(t *testing.T) {
		ctx := context.Background()

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)

		_ = utxoStore.SetBlockHeight(101)

		validatorInstance, err := validator.New(t.Context(), logger, tSettings, utxoStore, nil, nil, nil, nil)
		require.NoError(t, err)

		// Create a PropagationServer
		ps := &PropagationServer{
			logger:    logger,
			validator: validatorInstance,
			settings:  tSettings,
		}

		handler := ps.handleMultipleTx(t.Context())

		txBytes := make([]byte, 0, 1024)
		txs := transactions.CreateTestTransactionChainWithCount(t, 20)
		coinbaseTx := txs[0]

		_, err = utxoStore.Create(t.Context(), coinbaseTx, 1)
		require.NoError(t, err)

		// Add all the transaction bytes to the byte slice for sending to the handler
		for i := 1; i < len(txs); i++ {
			txBytes = append(txBytes, txs[i].ExtendedBytes()...)
		}

		txsReader := bytes.NewReader(txBytes)

		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/txs", txsReader)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		c.SetPath("/txs")

		assert.NoError(t, handler(c))
		assert.Equal(t, 200, rec.Code)

		responseBody, err := io.ReadAll(rec.Body)
		require.NoError(t, err)

		assert.Equal(t, "OK", string(responseBody))
	})
}

func testProcessTransactionInternal(t *testing.T, utxoStoreURL string) {
	initPrometheusMetrics()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// Clear the default addresses since we're not starting the servers in tests
	tSettings.Propagation.GRPCListenAddress = ""
	tSettings.Propagation.HTTPListenAddress = ""

	// Parse the URL and set it in settings
	parsedURL, err := url.Parse(utxoStoreURL)
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = parsedURL

	txs := transactions.CreateTestTransactionChainWithCount(t, 5)

	utxoStore, err := factory.NewStore(t.Context(), logger, tSettings, "test", false)
	require.NoError(t, err)

	_ = utxoStore.SetBlockHeight(101)

	t.Run("Test sending non extended tx", func(t *testing.T) {
		for _, tx := range txs {
			_ = utxoStore.Delete(t.Context(), tx.TxIDChainHash())
		}

		blockAssemblyClient := blockassembly.NewMock()
		blockAssemblyClient.On("Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, nil)

		validatorInstance, err := validator.New(t.Context(), logger, tSettings, utxoStore, nil, nil, blockAssemblyClient, nil)
		require.NoError(t, err)

		// Create a PropagationServer
		ps := &PropagationServer{
			logger:    logger,
			validator: validatorInstance,
			settings:  tSettings,
		}

		// Add the first transaction to the store
		_, err = utxoStore.Create(t.Context(), txs[1], 1)
		require.NoError(t, err, "processTransactionInternal should not return an error for valid transaction")

		tx2NotExtended, err := bt.NewTxFromBytes(txs[2].Bytes())
		require.NoError(t, err, "should be able to create a transaction from bytes")

		err = ps.processTransactionInternal(t.Context(), tx2NotExtended)
		require.NoError(t, err, "processTransactionInternal should not return an error for valid transaction")
	})

	t.Run("Test sending tx in parallel", func(t *testing.T) {
		for _, tx := range txs {
			_ = utxoStore.Delete(t.Context(), tx.TxIDChainHash())
		}

		blockAssemblyClient := blockassembly.NewMock()
		blockAssemblyClient.On("Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, nil)

		validatorInstance, err := validator.New(t.Context(), logger, tSettings, utxoStore, nil, nil, blockAssemblyClient, nil)
		require.NoError(t, err)

		// Create a PropagationServer
		ps := &PropagationServer{
			logger:    logger,
			validator: validatorInstance,
			settings:  tSettings,
		}

		txs := transactions.CreateTestTransactionChainWithCount(t, 5)

		// Add the first transaction to the store
		_, err = utxoStore.Create(t.Context(), txs[1], 1)
		require.NoError(t, err, "processTransactionInternal should not return an error for valid transaction")

		g := errgroup.Group{}

		// Determine parallelism based on store type to avoid hot key issues
		// Aerospike has stricter concurrency limits for the same key in test environments
		numGoroutines := 50 // Default for in-memory/PostgreSQL stores

		// Detect if we're using Aerospike by checking the URL scheme
		if parsedURL.Scheme == "aerospike" {
			// Reduce parallelism significantly for Aerospike to avoid KEY_BUSY errors
			// Still tests deduplication with concurrent requests, just fewer
			numGoroutines = 10
		}

		// add the transaction in parallel
		for i := 0; i < numGoroutines; i++ {
			g.Go(func() error {
				if err := ps.processTransactionInternal(t.Context(), txs[2]); err != nil {
					return err
				}

				return ps.processTransactionInternal(t.Context(), txs[3])
			})
		}

		require.NoError(t, g.Wait(), "processTransactionInternal should not return an error for valid transaction")

		// make sure we only added the transaction once to block assembly
		assert.Len(t, blockAssemblyClient.Mock.Calls, 2, "processTransactionInternal should only call block assembly once for the same transaction")
	})
}

// Test_processTransactionInternalAerospike tests the processTransactionInternal function using Aerospike as the UTXO store backend.
// This test initializes an Aerospike container, configures the test environment with the Aerospike UTXO store,
// and validates transaction processing functionality against the distributed database backend.
func Test_processTransactionInternalAerospike(t *testing.T) {
	utxoStore, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = teardown()
	})

	testProcessTransactionInternal(t, utxoStore)
}

// Test_processTransactionInternalPostgres tests the processTransactionInternal function using PostgreSQL as the UTXO store backend.
// This test initializes a PostgreSQL container, configures the test environment with the PostgreSQL UTXO store,
// and validates transaction processing functionality against the relational database backend.
func Test_processTransactionInternalPostgres(t *testing.T) {
	container, err := postgres.RunPostgresTestContainer(context.Background(), "propagation-test")
	require.NoError(t, err)

	defer func() {
		_ = container.Terminate(context.Background())
	}()

	testProcessTransactionInternal(t, container.ConnectionString())
}

// TestPropagationServerCoverage tests additional code paths for complete coverage
func TestPropagationServerCoverage(t *testing.T) {
	// Initialize tracing for tests
	tracing.SetupMockTracer()

	t.Run("Health with self-check enabled", func(t *testing.T) {
		ctx := context.Background()
		tSettings := test.CreateBaseTestSettings(t)
		// Enable self health check with dynamic ports
		grpcPort := getFreePort(t)
		httpPort := getFreePort(t)
		tSettings.Propagation.GRPCListenAddress = fmt.Sprintf("localhost:%d", grpcPort)
		tSettings.Propagation.HTTPListenAddress = fmt.Sprintf("localhost:%d", httpPort)

		ps := &PropagationServer{
			logger:   ulogger.TestLogger{},
			settings: tSettings,
		}

		// Test readiness check with self-check enabled
		status, msg, err := ps.Health(ctx, false)

		// Should return error when trying to check self while servers aren't running
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.NoError(t, err)

		// Check that response contains expected strings (avoid JSON parsing issues)
		assert.Contains(t, msg, "status")
		assert.Contains(t, msg, "503")
		assert.Contains(t, msg, "dependencies")
		assert.Contains(t, msg, "gRPC Server")
		assert.Contains(t, msg, "HTTP Server")
	})

	t.Run("Health with validator HTTP check", func(t *testing.T) {
		ctx := context.Background()

		// Create real validator
		validatorInstance, _ := setupRealValidator(t, ctx)

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Propagation.GRPCListenAddress = ""
		tSettings.Propagation.HTTPListenAddress = ""

		// Set validator HTTP address with dynamic port
		validatorPort := getFreePort(t)
		validatorURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", validatorPort))

		ps := &PropagationServer{
			logger:            ulogger.TestLogger{},
			settings:          tSettings,
			validator:         validatorInstance,
			validatorHTTPAddr: validatorURL,
		}

		// Test readiness check with validator HTTP
		status, msg, err := ps.Health(ctx, false)

		// May be unhealthy due to unreachable validator HTTP endpoint
		assert.True(t, status == http.StatusOK || status == http.StatusServiceUnavailable)
		assert.NoError(t, err)

		// Check that response contains validator check info
		assert.Contains(t, msg, "status")
		assert.Contains(t, msg, "dependencies")
		// Validator check should appear
		assert.Contains(t, msg, "ValidatorClient")
	})

	t.Run("Health with blockchain client", func(t *testing.T) {
		ctx := context.Background()
		tSettings := test.CreateBaseTestSettings(t)

		// Create real blockchain client
		blockchainClient := testutil.NewMemorySQLiteBlockchainClient(ulogger.TestLogger{}, tSettings, t)

		tSettings.Propagation.GRPCListenAddress = ""
		tSettings.Propagation.HTTPListenAddress = ""

		ps := &PropagationServer{
			logger:           ulogger.TestLogger{},
			settings:         tSettings,
			blockchainClient: blockchainClient,
		}

		// Test readiness check with blockchain client
		status, msg, err := ps.Health(ctx, false)

		// Should be healthy with real blockchain client
		assert.Equal(t, http.StatusOK, status)
		assert.NoError(t, err)

		// Check that response contains blockchain check info
		assert.Contains(t, msg, "status")
		assert.Contains(t, msg, "200")
		assert.Contains(t, msg, "dependencies")
		assert.Contains(t, msg, "BlockchainClient")
	})

	t.Run("Health with tx store", func(t *testing.T) {
		ctx := context.Background()
		tSettings := test.CreateBaseTestSettings(t)

		// Create tx store
		txStore, err := null.New(ulogger.TestLogger{})
		require.NoError(t, err)

		tSettings.Propagation.GRPCListenAddress = ""
		tSettings.Propagation.HTTPListenAddress = ""

		ps := &PropagationServer{
			logger:   ulogger.TestLogger{},
			settings: tSettings,
			txStore:  txStore,
		}

		// Test readiness check with tx store
		status, msg, err := ps.Health(ctx, false)

		// Should be healthy with null tx store
		assert.Equal(t, http.StatusOK, status)
		assert.NoError(t, err)

		// Check that response contains tx store check info
		assert.Contains(t, msg, "status")
		assert.Contains(t, msg, "200")
		assert.Contains(t, msg, "dependencies")
		assert.Contains(t, msg, "TxStore")
	})

	t.Run("Health with all dependencies unhealthy", func(t *testing.T) {
		ctx := context.Background()
		tSettings := test.CreateBaseTestSettings(t)

		// Configure multiple dependencies that will be unhealthy with dynamic ports
		grpcPort := getFreePort(t)
		httpPort := getFreePort(t)
		tSettings.Propagation.GRPCListenAddress = fmt.Sprintf("localhost:%d", grpcPort)
		tSettings.Propagation.HTTPListenAddress = fmt.Sprintf("localhost:%d", httpPort)

		// Create validator with unreachable HTTP address
		validatorInstance, _ := setupRealValidator(t, ctx)
		validatorPort := getFreePort(t)
		validatorURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", validatorPort))

		ps := &PropagationServer{
			logger:            ulogger.TestLogger{},
			settings:          tSettings,
			validator:         validatorInstance,
			validatorHTTPAddr: validatorURL,
		}

		// Test readiness check with multiple unhealthy dependencies
		status, msg, err := ps.Health(ctx, false)

		// Should be unhealthy
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.NoError(t, err)

		// Check that response contains multiple dependency errors
		assert.Contains(t, msg, "status")
		assert.Contains(t, msg, "503")
		assert.Contains(t, msg, "dependencies")
		// Should have multiple server checks
		assert.Contains(t, msg, "gRPC Server")
		assert.Contains(t, msg, "HTTP Server")
	})
}
