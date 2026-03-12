package httpimpl

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockBlockchainClient is already defined in block_handler_test.go, so we don't need to redefine it here

// setupFSMHandlerTest creates a test environment for FSM handler tests
func setupFSMHandlerTest(_ *testing.T, requestBody string) (*FSMHandler, *blockchain.Mock, echo.Context, *httptest.ResponseRecorder) {
	// Create a new Echo instance
	e := echo.New()

	// Create a new HTTP request with the provided request body
	var req *http.Request
	if requestBody != "" {
		req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(requestBody))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	} else {
		req = httptest.NewRequest(http.MethodGet, "/", nil)
	}

	// Create a response recorder
	rec := httptest.NewRecorder()

	// Create a new Echo context
	c := e.NewContext(req, rec)

	// Create a mock blockchain client
	mockClient := &blockchain.Mock{}

	// Create a test logger
	logger := ulogger.TestLogger{}

	// Create a new FSM handler with the mock client
	handler := NewFSMHandler(mockClient, logger)

	return handler, mockClient, c, rec
}

// TestNewFSMHandler tests the NewFSMHandler function
func TestNewFSMHandler(t *testing.T) {
	// Create a mock blockchain client
	mockClient := &blockchain.Mock{}

	// Create a test logger
	logger := ulogger.TestLogger{}

	// Call the function to be tested
	handler := NewFSMHandler(mockClient, logger)

	// Assert that the handler was created correctly
	assert.NotNil(t, handler)
	assert.Equal(t, mockClient, handler.blockchainClient)
	assert.Equal(t, logger, handler.logger)
}

// TestGetFSMState tests the GetFSMState method
func TestGetFSMState(t *testing.T) {
	t.Run("Success case", func(t *testing.T) {
		// Setup test
		handler, mockClient, c, rec := setupFSMHandlerTest(t, "")

		// Create a mock FSM state
		state := blockchain_api.FSMStateType_IDLE

		// Mock the blockchain client response
		mockClient.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

		// Call the function to be tested
		err := handler.GetFSMState(c)

		// Assert results
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Verify response
		var response map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Equal(t, state.String(), response["state"])
		assert.Equal(t, float64(state.Number()), response["state_value"])

		// Verify that the mock was called with the correct arguments
		mockClient.AssertCalled(t, "GetFSMCurrentState", mock.Anything)
	})

	t.Run("Error from GetFSMCurrentState", func(t *testing.T) {
		// Setup test
		handler, mockClient, c, _ := setupFSMHandlerTest(t, "")

		// Mock the blockchain client response with error
		mockClient.On("GetFSMCurrentState", mock.Anything).Return(nil, errors.ErrServiceUnavailable)

		// Call the function to be tested
		err := handler.GetFSMState(c)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusInternalServerError, httpErr.Code)
		assert.Contains(t, httpErr.Message, "Failed to get FSM state")

		// Verify that the mock was called with the correct arguments
		mockClient.AssertCalled(t, "GetFSMCurrentState", mock.Anything)
	})
}

// TestGetFSMEvents tests the GetFSMEvents method
func TestGetFSMEvents(t *testing.T) {
	// Setup test
	handler, mockClient, c, rec := setupFSMHandlerTest(t, "")

	state := blockchain_api.FSMStateType_IDLE
	mockClient.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

	// Call the function to be tested
	err := handler.GetFSMEvents(c)

	// Assert results
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify response
	var response map[string]interface{}
	err = json.Unmarshal(rec.Body.Bytes(), &response)
	require.NoError(t, err)

	events, ok := response["events"].([]interface{})
	require.True(t, ok)

	// Check that we have at least some events
	assert.NotEmpty(t, events)

	// Verify that the events include at least the basic ones
	// Convert events to a map for easier checking
	eventMap := make(map[string]bool)
	for _, event := range events {
		eventMap[event.(string)] = true
	}

	// Check for some known FSM events
	// Note: These may need to be updated based on the actual events in the blockchain_api.FSMEventType enum
	assert.True(t, len(eventMap) > 0, "Should have at least one event")

	// Verify that the mock was called with the correct arguments
	mockClient.AssertCalled(t, "GetFSMCurrentState", mock.Anything)
}

func TestExtractInvalidArgumentMessage(t *testing.T) {
	t.Run("Nil error", func(t *testing.T) {
		assert.Equal(t, "", extractInvalidArgumentMessage(nil))
	})

	t.Run("Extracts from INVALID_ARGUMENT marker", func(t *testing.T) {
		err := errors.NewInvalidArgumentError("rpc error: code = INVALID_ARGUMENT: bad event")
		assert.Equal(t, "bad event", extractInvalidArgumentMessage(err))
	})

	t.Run("Extracts from InvalidArgument marker", func(t *testing.T) {
		err := errors.NewInvalidArgumentError("rpc error: code = InvalidArgument: bad event")
		assert.Equal(t, "bad event", extractInvalidArgumentMessage(err))
	})

	t.Run("Extracts from desc field", func(t *testing.T) {
		err := errors.NewInvalidArgumentError("rpc error: code = InvalidArgument desc = bad event")
		assert.Equal(t, "bad event", extractInvalidArgumentMessage(err))
	})
}

// TestGetFSMStates tests the GetFSMStates method
func TestGetFSMStates(t *testing.T) {
	// Setup test
	handler, _, c, rec := setupFSMHandlerTest(t, "")

	// Call the function to be tested
	err := handler.GetFSMStates(c)

	// Assert results
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify response
	var states []map[string]interface{}
	err = json.Unmarshal(rec.Body.Bytes(), &states)
	require.NoError(t, err)

	// Check that we have at least some states
	assert.NotEmpty(t, states)

	// Verify that each state has the expected fields
	for _, state := range states {
		assert.Contains(t, state, "name")
		assert.Contains(t, state, "value")
	}

	// Convert states to a map for easier checking
	stateMap := make(map[string]bool)
	for _, state := range states {
		stateMap[state["name"].(string)] = true
	}

	// Check for some known FSM states
	// Note: These may need to be updated based on the actual states in the blockchain_api.FSMStateType enum
	assert.True(t, len(stateMap) > 0, "Should have at least one state")
}

// TestSendFSMEvent tests the SendFSMEvent method
func TestSendFSMEvent(t *testing.T) {
	t.Run("Success case", func(t *testing.T) {
		// Setup test with a valid event request
		const requestBody = `{"event": "RUN"}`

		handler, mockClient, c, rec := setupFSMHandlerTest(t, requestBody)

		// Create a mock FSM state for after the event is sent
		state := blockchain_api.FSMStateType_RUNNING

		// Mock the blockchain client responses
		mockClient.On("SendFSMEvent", mock.Anything, blockchain_api.FSMEventType_RUN).Return(nil)
		mockClient.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

		// Call the function to be tested
		err := handler.SendFSMEvent(c)

		// Assert results
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Verify response
		var response map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Equal(t, state.String(), response["state"])
		assert.Equal(t, float64(state.Number()), response["state_value"])

		// Verify that the mocks were called with the correct arguments
		mockClient.AssertCalled(t, "SendFSMEvent", mock.Anything, blockchain_api.FSMEventType_RUN)
		mockClient.AssertCalled(t, "GetFSMCurrentState", mock.Anything)
	})

	t.Run("Invalid request body", func(t *testing.T) {
		// Setup test with an invalid JSON request
		requestBody := `{"event": "RUN"` // Missing closing brace
		handler, _, c, _ := setupFSMHandlerTest(t, requestBody)

		// Call the function to be tested
		err := handler.SendFSMEvent(c)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, httpErr.Code)
		assert.Contains(t, httpErr.Message, "Invalid request body")
	})

	t.Run("Missing event parameter", func(t *testing.T) {
		// Setup test with a request missing the event parameter
		requestBody := `{}`
		handler, _, c, _ := setupFSMHandlerTest(t, requestBody)

		// Call the function to be tested
		err := handler.SendFSMEvent(c)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, httpErr.Code)
		assert.Contains(t, httpErr.Message, "Event is required")
	})

	t.Run("Invalid event type", func(t *testing.T) {
		// Setup test with an invalid event type
		requestBody := `{"event": "INVALID_EVENT"}`
		handler, _, c, _ := setupFSMHandlerTest(t, requestBody)

		// Call the function to be tested
		err := handler.SendFSMEvent(c)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, httpErr.Code)
		assert.Contains(t, httpErr.Message, "Invalid event type")
	})

	t.Run("Error from SendFSMEvent", func(t *testing.T) {
		// Setup test with a valid event request
		requestBody := `{"event": "RUN"}`
		handler, mockClient, c, rec := setupFSMHandlerTest(t, requestBody)

		// Mock the blockchain client response with error
		mockClient.On("SendFSMEvent", mock.Anything, blockchain_api.FSMEventType_RUN).Return(errors.ErrServiceUnavailable)

		// Call the function to be tested
		err := handler.SendFSMEvent(c)

		// Assert results
		require.NoError(t, err)
		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var response errorResponse
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Equal(t, int32(http.StatusInternalServerError), response.Status)
		assert.Equal(t, int32(errors.ERR_SERVICE_ERROR), response.Code)
		assert.Contains(t, strings.ToLower(response.Err), "failed to send fsm event")

		// Verify that the mock was called with the correct arguments
		mockClient.AssertCalled(t, "SendFSMEvent", mock.Anything, blockchain_api.FSMEventType_RUN)
	})

	t.Run("Error from GetFSMCurrentState after SendFSMEvent", func(t *testing.T) {
		// Setup test with a valid event request
		requestBody := `{"event": "RUN"}`
		handler, mockClient, c, _ := setupFSMHandlerTest(t, requestBody)

		// Mock the blockchain client responses
		mockClient.On("SendFSMEvent", mock.Anything, blockchain_api.FSMEventType_RUN).Return(nil)
		mockClient.On("GetFSMCurrentState", mock.Anything).Return(nil, errors.ErrServiceUnavailable)

		// Call the function to be tested
		err := handler.SendFSMEvent(c)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusInternalServerError, httpErr.Code)
		assert.Contains(t, httpErr.Message, "Failed to get FSM state")

		// Verify that the mocks were called with the correct arguments
		mockClient.AssertCalled(t, "SendFSMEvent", mock.Anything, blockchain_api.FSMEventType_RUN)
		mockClient.AssertCalled(t, "GetFSMCurrentState", mock.Anything)
	})
}
