package propagation

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/services/propagation/propagation_api"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/stores/blob/null"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
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
	tracing.SetGlobalMockTracer()

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
	tracing.SetGlobalMockTracer()

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
	tracing.SetGlobalMockTracer()

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
