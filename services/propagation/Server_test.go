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

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/services/propagation/propagation_api"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/null"
	"github.com/bitcoin-sv/teranode/stores/utxo/sql"
	"github.com/bitcoin-sv/teranode/test/utils/transactions"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

/*
// CustomMockBlockchainClient extends the standard blockchain.MockBlockchain
// with additional control over health check responses
type CustomMockBlockchainClient struct {
	blockchain.MockBlockchain
	healthStatus int
	healthMsg    string
	healthErr    error
	fsmState     string
	fsmErr       error
}

// Health overrides the standard implementation to allow customizing the response
func (m *CustomMockBlockchainClient) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return m.healthStatus, m.healthMsg, m.healthErr
}

// GetFSMCurrentState overrides the standard implementation to allow customizing the FSM state and error
func (m *CustomMockBlockchainClient) GetFSMCurrentState(ctx context.Context) (*blockchain_api.FSMStateType, error) {
	// Convert the string state to FSMStateType
	var state blockchain_api.FSMStateType

	switch m.fsmState {
	case "IDLE":
		state = blockchain_api.FSMStateType_IDLE
	case "RUNNING":
		state = blockchain_api.FSMStateType_RUNNING
	case "CATCHINGBLOCKS":
		state = blockchain_api.FSMStateType_CATCHINGBLOCKS
	case "LEGACYSYNCING":
		state = blockchain_api.FSMStateType_LEGACYSYNCING
	default:
		state = blockchain_api.FSMStateType_IDLE
	}

	return &state, m.fsmErr
}

// GetFSMCurrentStateForE2ETestMode returns the FSM state for E2E testing
func (m *CustomMockBlockchainClient) GetFSMCurrentStateForE2ETestMode() blockchain_api.FSMStateType {
	// Convert the string state to FSMStateType
	switch m.fsmState {
	case "IDLE":
		return blockchain_api.FSMStateType_IDLE
	case "RUNNING":
		return blockchain_api.FSMStateType_RUNNING
	case "CATCHINGBLOCKS":
		return blockchain_api.FSMStateType_CATCHINGBLOCKS
	case "LEGACYSYNCING":
		return blockchain_api.FSMStateType_LEGACYSYNCING
	default:
		return blockchain_api.FSMStateType_IDLE
	}
}
*/
// TestPropagationServer_HealthLiveness tests the Health function with liveness checks.
func TestPropagationServer_HealthLiveness(t *testing.T) {
	// Initialize tracing for tests
	tracing.SetupMockTracer()

	// Create a server with minimal dependencies
	ps := &PropagationServer{
		logger:   ulogger.TestLogger{},
		settings: test.CreateBaseTestSettings(),
	}

	// Test liveness check (checkLiveness=true)
	ctx := context.Background()
	status, msg, err := ps.Health(ctx, true)

	// Verify results
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "OK", msg)
	assert.NoError(t, err)
}

// TestPropagationServer_HealthReadiness tests the Health function with readiness checks.
func TestPropagationServer_HealthReadiness(t *testing.T) {
	// Initialize tracing for tests
	tracing.SetupMockTracer()

	t.Run("all dependencies healthy", func(t *testing.T) {
		// Create mock dependencies - all healthy
		mockValidator := &validator.MockValidatorClient{
			// MockValidatorClient.Health returns 0 by default but we can set the BlockHeight
			// to indicate it's initialized and healthy
			BlockHeight: 123,
		}

		/*
			mockBlockchainClient := &CustomMockBlockchainClient{
				healthStatus: http.StatusOK,
				healthMsg:    "OK",
				healthErr:    nil,
				fsmState:     "RUNNING",
				fsmErr:       nil,
			}*/

		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusOK, "OK", nil)

		// Configure FSM state to be RUNNING
		fsmState := blockchain_api.FSMStateType_RUNNING
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
		mockBlockchainClient.On("GetFSMCurrentStateForE2ETestMode").Return(blockchain_api.FSMStateType_RUNNING)

		// Create mock tx store
		txStore, err := null.New(ulogger.TestLogger{})
		require.NoError(t, err)

		// Create server with healthy dependencies
		validatorURL, _ := url.Parse("http://localhost:8080")
		ps := &PropagationServer{
			logger:            ulogger.TestLogger{},
			settings:          test.CreateBaseTestSettings(),
			validator:         mockValidator,
			blockchainClient:  mockBlockchainClient,
			txStore:           txStore,
			validatorHTTPAddr: validatorURL,
		}

		// Test readiness check (checkLiveness=false)
		ctx := context.Background()
		status, msg, err := ps.Health(ctx, false)

		// Verify results - should be unhealthy due to validator returning 0 status
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.NoError(t, err)

		// Validate JSON response
		var jsonMsg map[string]interface{}
		err = json.Unmarshal([]byte(msg), &jsonMsg)
		require.NoError(t, err, "Message should be valid JSON")
		assert.Contains(t, jsonMsg, "status")
		assert.Contains(t, jsonMsg, "dependencies")
	})

	t.Run("unhealthy dependencies", func(t *testing.T) {
		// Create mock dependencies with one unhealthy
		mockValidator := &validator.MockValidatorClient{
			// Return unhealthy status when called with Health
		}

		/*
			mockBlockchainClient := &CustomMockBlockchainClient{
				healthStatus: http.StatusServiceUnavailable,
				healthMsg:    "Service unavailable",
				healthErr:    errors.NewServiceUnavailableError("blockchain unavailable"),
				fsmState:     "",
				fsmErr:       errors.NewServiceUnavailableError("fsm unavailable"),
			}*/
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("Health", mock.Anything, false).Return(
			http.StatusServiceUnavailable,
			"Service unavailable",
			errors.NewServiceUnavailableError("blockchain unavailable"),
		)

		// Configure FSM state to return error
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(
			(*blockchain_api.FSMStateType)(nil),
			errors.NewServiceUnavailableError("fsm unavailable"),
		)

		// Create mock tx store
		txStore, err := null.New(ulogger.TestLogger{})
		require.NoError(t, err)

		// Create server with one unhealthy dependency
		validatorURL, _ := url.Parse("http://localhost:8080")
		ps := &PropagationServer{
			logger:            ulogger.TestLogger{},
			settings:          test.CreateBaseTestSettings(),
			validator:         mockValidator,
			blockchainClient:  mockBlockchainClient,
			txStore:           txStore,
			validatorHTTPAddr: validatorURL,
		}

		// Test readiness check with unhealthy dependency
		ctx := context.Background()
		status, msg, err := ps.Health(ctx, false)

		// Verify results
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.NoError(t, err) // Health check should return status, not error

		// Validate JSON response
		var jsonMsg map[string]interface{}
		err = json.Unmarshal([]byte(msg), &jsonMsg)
		require.NoError(t, err, "Message should be valid JSON")
		assert.Contains(t, jsonMsg, "status")
		assert.Contains(t, jsonMsg, "dependencies")
	})

	t.Run("no dependencies", func(t *testing.T) {
		// Create server with no dependencies
		ps := &PropagationServer{
			logger:   ulogger.TestLogger{},
			settings: test.CreateBaseTestSettings(),
			// No validator or blockchainClient
		}

		// Test readiness check (checkLiveness=false)
		ctx := context.Background()
		status, msg, err := ps.Health(ctx, false)

		// Verify results - should pass with only Kafka check
		// Since we only have the Kafka check which passes when not configured
		assert.Equal(t, http.StatusOK, status)
		assert.NoError(t, err)

		// Validate JSON response
		var jsonMsg map[string]interface{}
		err = json.Unmarshal([]byte(msg), &jsonMsg)
		require.NoError(t, err, "Message should be valid JSON")

		// Should show healthy status
		assert.Contains(t, jsonMsg, "status")
		deps, ok := jsonMsg["dependencies"].([]interface{})
		assert.True(t, ok, "dependencies should be an array")

		// Only one dependency check (Kafka) should run
		assert.Equal(t, 1, len(deps), "Should only have one dependency check")

		// First dependency should be Kafka
		kafka, ok := deps[0].(map[string]interface{})
		assert.True(t, ok, "Kafka dependency should be a map")
		assert.Equal(t, "Kafka", kafka["resource"])
		assert.Equal(t, "200", kafka["status"])
	})

	t.Run("healthy service", func(t *testing.T) {
		// Create mock dependencies - all healthy
		mockValidator := &validator.MockValidatorClient{}

		/*
			mockBlockchainClient := &CustomMockBlockchainClient{
				healthStatus: http.StatusOK,
				healthMsg:    "OK",
				healthErr:    nil,
				fsmState:     "RUNNING",
				fsmErr:       nil,
			}*/
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusOK, "OK", nil)

		// Configure FSM state to be RUNNING
		fsmState := blockchain_api.FSMStateType_RUNNING
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
		mockBlockchainClient.On("GetFSMCurrentStateForE2ETestMode").Return(blockchain_api.FSMStateType_RUNNING)

		// Create mock tx store
		txStore, err := null.New(ulogger.TestLogger{})
		require.NoError(t, err)

		// Create server with healthy dependencies
		validatorURL, _ := url.Parse("http://localhost:8080")
		ps := &PropagationServer{
			logger:            ulogger.TestLogger{},
			settings:          test.CreateBaseTestSettings(),
			validator:         mockValidator,
			blockchainClient:  mockBlockchainClient,
			txStore:           txStore,
			validatorHTTPAddr: validatorURL,
		}

		// Execute gRPC health check
		ctx := context.Background()
		response, err := ps.HealthGRPC(ctx, &propagation_api.EmptyMessage{})
		require.NoError(t, err)

		// Since validator returns 0 status, the overall health should be false
		assert.False(t, response.Ok)

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
		// Create mock dependencies - all healthy
		mockValidator := &validator.MockValidatorClient{}

		/* mockBlockchainClient := &CustomMockBlockchainClient{
			healthStatus: http.StatusOK,
			healthMsg:    "OK",
			healthErr:    nil,
			fsmState:     "RUNNING",
			fsmErr:       nil,
		} */
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusOK, "OK", nil)

		// Configure FSM state to be RUNNING
		fsmState := blockchain_api.FSMStateType_RUNNING
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
		mockBlockchainClient.On("GetFSMCurrentStateForE2ETestMode").Return(blockchain_api.FSMStateType_RUNNING)

		// Create mock tx store
		txStore, err := null.New(ulogger.TestLogger{})
		require.NoError(t, err)

		// Create server with healthy dependencies
		validatorURL, _ := url.Parse("http://localhost:8080")
		ps := &PropagationServer{
			logger:            ulogger.TestLogger{},
			settings:          test.CreateBaseTestSettings(),
			validator:         mockValidator,
			blockchainClient:  mockBlockchainClient,
			txStore:           txStore,
			validatorHTTPAddr: validatorURL,
		}

		// Execute gRPC health check
		ctx := context.Background()
		response, err := ps.HealthGRPC(ctx, &propagation_api.EmptyMessage{})
		require.NoError(t, err)

		// Since validator returns 0 status, the overall health should be false
		assert.False(t, response.Ok)

		// Validate that details contains JSON
		var details map[string]interface{}
		err = json.Unmarshal([]byte(response.Details), &details)
		require.NoError(t, err, "Details should be valid JSON")
		assert.Contains(t, details, "status")
	})

	t.Run("unhealthy service", func(t *testing.T) {
		// Configure the blockchain mock to return unhealthy status
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("Health", mock.Anything, false).Return(
			http.StatusServiceUnavailable,
			"Service unavailable",
			errors.NewServiceUnavailableError("blockchain unavailable"),
		)

		// Add missing expectation for GetFSMCurrentState
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(
			(*blockchain_api.FSMStateType)(nil),
			errors.NewServiceUnavailableError("fsm unavailable"),
		)

		// Create server with unhealthy blockchain client
		ps := &PropagationServer{
			logger:           ulogger.TestLogger{},
			settings:         test.CreateBaseTestSettings(),
			blockchainClient: mockBlockchainClient,
		}

		// Test HealthGRPC with unhealthy dependency
		ctx := context.Background()
		response, err := ps.HealthGRPC(ctx, &propagation_api.EmptyMessage{})

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, response)
		assert.False(t, response.Ok)
		assert.NotNil(t, response.Timestamp)
		assert.NotEqual(t, "OK", response.Details)
	})
}

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
		for i := 0; i < 25; i++ {
			resp, err := http.Post(baseURL+"/tx", "application/octet-stream", bytes.NewBuffer(txData))
			require.NoError(t, err)
			defer resp.Body.Close()

			if i > 20 {
				assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode, "unexpected status code for %d: %d", i, resp.StatusCode)
			}
		}
	})
}

// Test_handleMultipleTx tests the handleMultipleTx function.
// This test makes sure chains of transactions are processed properly
func Test_handleMultipleTx(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()
	tSettings.BlockAssembly.Disabled = true

	t.Run("Test handleMultipleTx with valid transactions", func(t *testing.T) {
		ctx := context.Background()

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)

		_ = utxoStore.SetBlockHeight(101)

		validatorInstance, err := validator.New(t.Context(), logger, tSettings, utxoStore, nil, nil, nil)
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
