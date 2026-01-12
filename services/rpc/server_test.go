package rpc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestCheckAuth tests the RPCServer checkAuth method
func TestCheckAuth(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	// Set up test credentials
	adminUser := "admin"
	adminPass := "adminpass"
	limitUser := "limited"
	limitPass := "limitpass"

	// Create auth strings
	adminLogin := adminUser + ":" + adminPass
	adminAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(adminLogin))
	adminAuthSha := sha256.Sum256([]byte(adminAuth))

	limitLogin := limitUser + ":" + limitPass
	limitAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(limitLogin))
	limitAuthSha := sha256.Sum256([]byte(limitAuth))

	s := &RPCServer{
		logger:       logger,
		authsha:      adminAuthSha,
		limitauthsha: limitAuthSha,
		settings: &settings.Settings{
			RPC: settings.RPCSettings{
				RPCUser:      adminUser,
				RPCPass:      adminPass,
				RPCLimitUser: limitUser,
				RPCLimitPass: limitPass,
			},
		},
	}

	t.Run("no auth header - require false", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/", nil)

		authenticated, isAdmin, err := s.checkAuth(req, false)

		require.NoError(t, err)
		assert.False(t, authenticated)
		assert.False(t, isAdmin)
	})

	t.Run("no auth header - require true", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/", nil)

		authenticated, isAdmin, err := s.checkAuth(req, true)

		require.Error(t, err)
		assert.False(t, authenticated)
		assert.False(t, isAdmin)
		assert.Contains(t, err.Error(), "auth failure")
	})

	t.Run("valid admin credentials", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/", nil)
		req.Header.Set("Authorization", adminAuth)

		authenticated, isAdmin, err := s.checkAuth(req, true)

		require.NoError(t, err)
		assert.True(t, authenticated)
		assert.True(t, isAdmin)
	})

	t.Run("valid limited credentials", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/", nil)
		req.Header.Set("Authorization", limitAuth)

		authenticated, isAdmin, err := s.checkAuth(req, true)

		require.NoError(t, err)
		assert.True(t, authenticated)
		assert.False(t, isAdmin) // Limited user, not admin
	})

	t.Run("invalid credentials", func(t *testing.T) {
		invalidAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("invalid:credentials"))
		req := httptest.NewRequest("POST", "/", nil)
		req.Header.Set("Authorization", invalidAuth)

		authenticated, isAdmin, err := s.checkAuth(req, true)

		require.Error(t, err)
		assert.False(t, authenticated)
		assert.False(t, isAdmin)
		assert.Contains(t, err.Error(), "auth failure")
	})

	t.Run("malformed auth header", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/", nil)
		req.Header.Set("Authorization", "InvalidFormat")

		authenticated, isAdmin, err := s.checkAuth(req, true)

		require.Error(t, err)
		assert.False(t, authenticated)
		assert.False(t, isAdmin)
	})
}

// TestConnectionLimiting tests the RPCServer connection limiting functionality
func TestConnectionLimiting(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger:        logger,
		rpcMaxClients: 2, // Set low limit for testing
		numClients:    0,
	}

	t.Run("under limit - connection allowed", func(t *testing.T) {
		w := httptest.NewRecorder()

		limited := s.limitConnections(w, "127.0.0.1:1234")

		assert.False(t, limited)
		assert.Equal(t, http.StatusOK, w.Code) // Should not have written error response
	})

	t.Run("at limit - connection rejected", func(t *testing.T) {
		// Set current clients to max - 1, so next one hits limit
		s.numClients = int32(s.rpcMaxClients)

		w := httptest.NewRecorder()

		limited := s.limitConnections(w, "127.0.0.1:1234")

		assert.True(t, limited)
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Contains(t, w.Body.String(), "503 Too busy")
	})

	t.Run("increment clients", func(t *testing.T) {
		s.numClients = 0

		s.incrementClients()

		assert.Equal(t, int32(1), s.numClients)
	})

	t.Run("decrement clients", func(t *testing.T) {
		s.numClients = 5

		s.decrementClients()

		assert.Equal(t, int32(4), s.numClients)
	})

	t.Run("decrement from zero", func(t *testing.T) {
		s.numClients = 0

		s.decrementClients()

		assert.Equal(t, int32(-1), s.numClients) // Atomic operation allows negative
	})
}

// TestHTTPStatusLine tests the httpStatusLine method
func TestHTTPStatusLine(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger:      logger,
		statusLines: make(map[int]string),
	}

	t.Run("HTTP/1.1 request - 200 OK", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		req.Proto = "HTTP/1.1"
		req.ProtoMajor = 1
		req.ProtoMinor = 1

		line := s.httpStatusLine(req, http.StatusOK)

		expected := "HTTP/1.1 200 OK\r\n"
		assert.Equal(t, expected, line)

		// Should be cached now
		assert.Contains(t, s.statusLines, http.StatusOK)
	})

	t.Run("HTTP/1.0 request - 404 Not Found", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		req.Proto = "HTTP/1.0"
		req.ProtoMajor = 1
		req.ProtoMinor = 0

		line := s.httpStatusLine(req, http.StatusNotFound)

		expected := "HTTP/1.0 404 Not Found\r\n"
		assert.Equal(t, expected, line)
	})

	t.Run("unknown status code", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		req.Proto = "HTTP/1.1"
		req.ProtoMajor = 1
		req.ProtoMinor = 1

		line := s.httpStatusLine(req, 999)

		expected := "HTTP/1.1 999 status code 999\r\n"
		assert.Equal(t, expected, line)
	})

	t.Run("cached status line", func(t *testing.T) {
		// First call to cache the line
		req1 := httptest.NewRequest("GET", "/", nil)
		req1.Proto = "HTTP/1.1"
		req1.ProtoMajor = 1
		req1.ProtoMinor = 1

		line1 := s.httpStatusLine(req1, http.StatusOK)

		// Second call should use cached version
		req2 := httptest.NewRequest("GET", "/", nil)
		req2.Proto = "HTTP/1.1"
		req2.ProtoMajor = 1
		req2.ProtoMinor = 1

		line2 := s.httpStatusLine(req2, http.StatusOK)

		assert.Equal(t, line1, line2)
		assert.Equal(t, "HTTP/1.1 200 OK\r\n", line2)
	})
}

// TestWriteHTTPResponseHeaders tests the writeHTTPResponseHeaders method
func TestWriteHTTPResponseHeaders(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger:      logger,
		statusLines: make(map[int]string),
	}

	t.Run("successful response headers", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/", nil)
		headers := make(http.Header)
		headers.Set("Content-Type", "application/json")
		headers.Set("Connection", "close")

		var buf strings.Builder

		err := s.writeHTTPResponseHeaders(req, headers, http.StatusOK, &buf)

		require.NoError(t, err)

		response := buf.String()
		assert.Contains(t, response, "HTTP/1.1 200 OK")
		assert.Contains(t, response, "Content-Type: application/json")
		assert.Contains(t, response, "Connection: close")
		assert.Contains(t, response, "\r\n\r\n") // End of headers
	})

	t.Run("error response headers", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/", nil)
		headers := make(http.Header)
		headers.Set("Content-Type", "application/json")

		var buf strings.Builder

		err := s.writeHTTPResponseHeaders(req, headers, http.StatusBadRequest, &buf)

		require.NoError(t, err)

		response := buf.String()
		assert.Contains(t, response, "HTTP/1.1 400 Bad Request")
		assert.Contains(t, response, "Content-Type: application/json")
	})
}

// TestCreateMarshalledReply tests the createMarshalledReply method
func TestCreateMarshalledReply(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger: logger,
	}

	t.Run("successful response", func(t *testing.T) {
		result := map[string]interface{}{
			"version": "1.0.0",
			"blocks":  100,
		}

		reply, err := s.createMarshalledReply(1, result, nil)

		require.NoError(t, err)
		assert.Contains(t, string(reply), `"id":1`)
		assert.Contains(t, string(reply), `"result"`)
		assert.Contains(t, string(reply), `"version":"1.0.0"`)
		assert.Contains(t, string(reply), `"blocks":100`)
	})

	t.Run("error response", func(t *testing.T) {
		rpcErr := &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParams.Code,
			Message: "Invalid parameters",
		}

		reply, err := s.createMarshalledReply(2, nil, rpcErr)

		require.NoError(t, err)
		assert.Contains(t, string(reply), `"id":2`)
		assert.Contains(t, string(reply), `"error"`)
		assert.Contains(t, string(reply), `"Invalid parameters"`)
	})

	t.Run("generic error conversion", func(t *testing.T) {
		genericErr := assert.AnError // A regular error, not RPCError

		reply, err := s.createMarshalledReply(3, nil, genericErr)

		require.NoError(t, err)
		assert.Contains(t, string(reply), `"id":3`)
		assert.Contains(t, string(reply), `"error"`)
		assert.Contains(t, string(reply), `"code":-32603`) // Internal error code
	})
}

// TestHandleUnimplemented tests the handleUnimplemented function
func TestHandleUnimplemented(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger: logger,
	}

	result, err := handleUnimplemented(nil, s, nil, nil)

	require.Error(t, err)
	assert.Nil(t, result)

	rpcErr, ok := err.(*bsvjson.RPCError)
	require.True(t, ok)
	assert.Equal(t, bsvjson.ErrRPCUnimplemented, rpcErr.Code)
	assert.Equal(t, "Command unimplemented", rpcErr.Message)
}

// TestHandleAskWallet tests the handleAskWallet function
func TestHandleAskWallet(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger: logger,
	}

	result, err := handleAskWallet(nil, s, nil, nil)

	require.Error(t, err)
	assert.Nil(t, result)

	rpcErr, ok := err.(*bsvjson.RPCError)
	require.True(t, ok)
	assert.Equal(t, bsvjson.ErrRPCNoWallet, rpcErr.Code)
	assert.Contains(t, rpcErr.Message, "wallet commands")
}

// TestHandleStop tests the handleStop function
func TestHandleStop(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger:                 logger,
		requestProcessShutdown: make(chan struct{}, 1),
	}

	t.Run("successful stop request", func(t *testing.T) {
		result, err := handleStop(context.Background(), s, nil, nil)

		require.NoError(t, err)
		assert.Equal(t, "bsvd stopping.", result)

		// Check that shutdown signal was sent
		select {
		case <-s.requestProcessShutdown:
			// Expected - shutdown signal received
		default:
			t.Error("Expected shutdown signal to be sent")
		}
	})

	t.Run("shutdown channel full - no blocking", func(t *testing.T) {
		// Pre-fill the channel
		s.requestProcessShutdown <- struct{}{}

		// This should not block even though channel is full
		result, err := handleStop(context.Background(), s, nil, nil)

		require.NoError(t, err)
		assert.Equal(t, "bsvd stopping.", result)
	})
}

// TestHandleVersion tests the handleVersion function
func TestHandleVersion(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger: logger,
	}

	result, err := handleVersion(context.Background(), s, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, result)

	versionMap, ok := result.(map[string]bsvjson.VersionResult)
	require.True(t, ok)

	// Check that btcdjsonrpcapi version is present
	btcdVersion, exists := versionMap["btcdjsonrpcapi"]
	require.True(t, exists)

	assert.Equal(t, jsonrpcSemverString, btcdVersion.VersionString)
	assert.Equal(t, uint32(jsonrpcSemverMajor), btcdVersion.Major)
	assert.Equal(t, uint32(jsonrpcSemverMinor), btcdVersion.Minor)
	assert.Equal(t, uint32(jsonrpcSemverPatch), btcdVersion.Patch)
}

// TestParseCmd tests the parseCmd method
func TestParseCmd(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger: logger,
	}

	t.Run("valid getinfo command", func(t *testing.T) {
		request := &bsvjson.Request{
			ID:      1,
			Method:  "getinfo",
			Params:  nil,
			Jsonrpc: "1.0",
		}

		parsed := s.parseCmd(request)

		require.NotNil(t, parsed)
		assert.Equal(t, 1, parsed.id)
		assert.Equal(t, "getinfo", parsed.method)
		assert.Nil(t, parsed.err)
		assert.NotNil(t, parsed.cmd)
	})

	t.Run("invalid method", func(t *testing.T) {
		request := &bsvjson.Request{
			ID:      2,
			Method:  "invalidmethod",
			Params:  nil,
			Jsonrpc: "1.0",
		}

		parsed := s.parseCmd(request)

		require.NotNil(t, parsed)
		assert.Equal(t, 2, parsed.id)
		assert.Equal(t, "invalidmethod", parsed.method)
		assert.NotNil(t, parsed.err)
		assert.Equal(t, bsvjson.ErrRPCMethodNotFound, parsed.err)
	})

	t.Run("invalid params", func(t *testing.T) {
		// Create raw JSON params that will cause parsing error
		invalidParam1, _ := json.Marshal("invalid_hash_format")
		invalidParam2, _ := json.Marshal("invalid_verbose")
		request := &bsvjson.Request{
			ID:      3,
			Method:  "getblock",
			Params:  []json.RawMessage{invalidParam1, invalidParam2},
			Jsonrpc: "1.0",
		}

		parsed := s.parseCmd(request)

		require.NotNil(t, parsed)
		assert.Equal(t, 3, parsed.id)
		assert.Equal(t, "getblock", parsed.method)
		assert.NotNil(t, parsed.err)
		assert.Equal(t, bsvjson.ErrRPCInvalidParams.Code, parsed.err.Code)
	})
}

// TestRPCServerShutdown tests the server shutdown functionality
func TestRPCServerShutdown(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger:   logger,
		quit:     make(chan int),
		shutdown: 0,
		started:  0,
	}

	t.Run("normal shutdown", func(t *testing.T) {
		// Reset shutdown state
		atomic.StoreInt32(&s.shutdown, 0)

		err := s.Stop(context.Background())

		require.NoError(t, err)

		// Verify shutdown flag is set
		assert.Equal(t, int32(1), atomic.LoadInt32(&s.shutdown))
	})

	t.Run("double shutdown", func(t *testing.T) {
		// Create a fresh server instance for this test
		s2 := &RPCServer{
			logger:   logger,
			quit:     make(chan int),
			shutdown: 0,
			started:  0,
		}

		// First shutdown
		err1 := s2.Stop(context.Background())
		require.NoError(t, err1)

		// Second shutdown should not block or error
		err2 := s2.Stop(context.Background())
		require.NoError(t, err2)

		// Shutdown counter should be > 1
		assert.Greater(t, atomic.LoadInt32(&s2.shutdown), int32(1))
	})
}

// TestRequestedProcessShutdown tests the RequestedProcessShutdown method
func TestRequestedProcessShutdown(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger:                 logger,
		requestProcessShutdown: make(chan struct{}),
	}

	shutdownChan := s.RequestedProcessShutdown()

	// Verify we get a read-only channel (can't compare directly due to type conversion)
	assert.NotNil(t, shutdownChan)

	// Send shutdown signal
	close(s.requestProcessShutdown)

	// Verify we can read from the channel
	select {
	case <-shutdownChan:
		// Expected - channel was closed
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected to receive shutdown signal")
	}
}

// TestServerInternalRPCError tests the internalRPCError method
func TestServerInternalRPCError(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger: logger,
	}

	t.Run("error with context", func(t *testing.T) {
		errStr := "database connection failed"
		context := "GetBlock operation"

		rpcErr := s.internalRPCError(errStr, context)

		require.NotNil(t, rpcErr)
		assert.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
		assert.Equal(t, errStr, rpcErr.Message)
	})

	t.Run("error without context", func(t *testing.T) {
		errStr := "general error"

		rpcErr := s.internalRPCError(errStr, "")

		require.NotNil(t, rpcErr)
		assert.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
		assert.Equal(t, errStr, rpcErr.Message)
	})
}

// TestServerRPCDecodeHexError tests the rpcDecodeHexError function
func TestServerRPCDecodeHexError(t *testing.T) {
	invalidHex := "xyz123"

	rpcErr := rpcDecodeHexError(invalidHex)

	require.NotNil(t, rpcErr)
	assert.Equal(t, bsvjson.ErrRPCDecodeHexString, rpcErr.Code)
	assert.Contains(t, rpcErr.Message, invalidHex)
	assert.Contains(t, rpcErr.Message, "hexadecimal string")
}

// TestStandardCmdResult tests the standardCmdResult method
func TestStandardCmdResult(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	s := &RPCServer{
		logger: logger,
		settings: &settings.Settings{
			RPC: settings.RPCSettings{
				RPCTimeout: 5 * time.Second,
			},
		},
		requestProcessShutdown: make(chan struct{}, 1),
	}

	// Initialize the server to populate rpcHandlers
	err := s.Init(context.Background())
	require.NoError(t, err)

	closeChan := make(chan struct{})

	t.Run("valid RPC handler command", func(t *testing.T) {
		parsedCmd := &parsedRPCCmd{
			method: "stop",
			cmd:    &bsvjson.StopCmd{},
		}

		result, err := s.standardCmdResult(context.Background(), parsedCmd, closeChan)

		// Should execute successfully with stop command
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, "", result) // Stop returns a string
	})

	t.Run("ask wallet command", func(t *testing.T) {
		parsedCmd := &parsedRPCCmd{
			method: "dumpprivkey",
			cmd:    &bsvjson.DumpPrivKeyCmd{},
		}

		result, err := s.standardCmdResult(context.Background(), parsedCmd, closeChan)

		require.Error(t, err)
		assert.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCNoWallet, rpcErr.Code)
	})

	t.Run("unimplemented command", func(t *testing.T) {
		parsedCmd := &parsedRPCCmd{
			method: "estimatepriority",
			cmd:    &bsvjson.EstimatePriorityCmd{},
		}

		result, err := s.standardCmdResult(context.Background(), parsedCmd, closeChan)

		require.Error(t, err)
		assert.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCUnimplemented, rpcErr.Code)
	})

	t.Run("unknown command", func(t *testing.T) {
		parsedCmd := &parsedRPCCmd{
			method: "unknowncommand",
			cmd:    nil,
		}

		result, err := s.standardCmdResult(context.Background(), parsedCmd, closeChan)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, bsvjson.ErrRPCMethodNotFound, err)
	})

	t.Run("context cancellation", func(t *testing.T) {
		parsedCmd := &parsedRPCCmd{
			method: "estimatepriority", // Use simple unimplemented command
			cmd:    &bsvjson.EstimatePriorityCmd{},
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		result, err := s.standardCmdResult(ctx, parsedCmd, closeChan)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("connection closed by client", func(t *testing.T) {
		parsedCmd := &parsedRPCCmd{
			method: "estimatepriority", // Use simple unimplemented command
			cmd:    &bsvjson.EstimatePriorityCmd{},
		}

		// Create a new closed channel to simulate client disconnection
		closedChan := make(chan struct{})
		close(closedChan)

		result, err := s.standardCmdResult(context.Background(), parsedCmd, closedChan)

		require.Error(t, err)
		assert.Nil(t, result)

		rpcErr, ok := err.(*bsvjson.RPCError)
		require.True(t, ok)
		assert.Equal(t, bsvjson.ErrRPCMisc, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "Connection closed by client")
	})
}

// TestJsonAuthFail tests the jsonAuthFail function
func TestJsonAuthFail(t *testing.T) {
	t.Run("sends 401 with WWW-Authenticate header", func(t *testing.T) {
		w := httptest.NewRecorder()

		jsonAuthFail(w)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
		assert.Equal(t, `Basic realm="bsvd RPC"`, w.Header().Get("WWW-Authenticate"))
		assert.Contains(t, w.Body.String(), "401 Unauthorized.")
	})
}

// TestHealth tests the Health method
func TestHealth(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("liveness check - always returns OK", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
		}

		status, message, err := s.Health(context.Background(), true)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "OK", message)
	})

	t.Run("readiness check - no clients", func(t *testing.T) {
		s := &RPCServer{
			logger: logger,
		}

		status, message, err := s.Health(context.Background(), false)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, message, `"status":"200"`)
		assert.Contains(t, message, `"dependencies":[]`)
	})

	t.Run("readiness check - with mock blockchain client", func(t *testing.T) {
		// Create a mock blockchain client
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("Health", mock.Anything, mock.Anything).Return(
			http.StatusOK, "Blockchain healthy", nil,
		)
		runningState := blockchain_api.FSMStateType_RUNNING
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
		}

		status, message, err := s.Health(context.Background(), false)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, message, `"status":"200"`)
		assert.Contains(t, message, `BlockchainClient`)
		assert.Contains(t, message, `FSM`)
	})

	t.Run("readiness check - with mock block assembly client", func(t *testing.T) {
		// Create a mock block assembly client
		mockBlockAssemblyClient := &mockBlockAssemblyClient{
			healthFunc: func(ctx context.Context, checkLiveness bool) (int, string, error) {
				return http.StatusOK, "Block assembly healthy", nil
			},
		}

		s := &RPCServer{
			logger:              logger,
			blockAssemblyClient: mockBlockAssemblyClient,
		}

		status, message, err := s.Health(context.Background(), false)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, message, `"status":"200"`)
		assert.Contains(t, message, `BlockAssemblyClient`)
	})

	t.Run("readiness check - unhealthy dependency", func(t *testing.T) {
		// Create a mock blockchain client that reports unhealthy
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("Health", mock.Anything, mock.Anything).Return(
			http.StatusServiceUnavailable, "Database connection failed", assert.AnError,
		)
		runningState := blockchain_api.FSMStateType_RUNNING
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil)

		s := &RPCServer{
			logger:           logger,
			blockchainClient: mockBlockchainClient,
		}

		status, message, err := s.Health(context.Background(), false)

		require.NoError(t, err)
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Contains(t, message, `"status":"503"`)
		assert.Contains(t, message, `BlockchainClient`)
	})
}

// TestJsonRPCRead tests the jsonRPCRead method
func TestJsonRPCRead(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	// Create a basic server for testing
	s := &RPCServer{
		logger:                 logger,
		shutdown:               0,
		rpcQuirks:              false,
		requestProcessShutdown: make(chan struct{}, 1),
		settings: &settings.Settings{
			RPC: settings.RPCSettings{
				RPCTimeout: 5 * time.Second,
			},
		},
	}
	err := s.Init(context.Background())
	require.NoError(t, err)

	t.Run("server shutdown - returns immediately", func(t *testing.T) {
		// Set shutdown flag
		atomic.StoreInt32(&s.shutdown, 1)

		req := httptest.NewRequest("POST", "/", strings.NewReader(`{"method":"getinfo","id":1}`))
		w := httptest.NewRecorder()

		s.jsonRPCRead(w, req, true)

		// Should return immediately without processing
		assert.Equal(t, 200, w.Code) // Default recorder status
		assert.Empty(t, w.Body.String())

		// Reset shutdown for other tests
		atomic.StoreInt32(&s.shutdown, 0)
	})

	t.Run("successful body read but hijacking not supported", func(t *testing.T) {
		// This test verifies the path where body read succeeds but hijacking fails
		// (which is the case with httptest.ResponseRecorder)
		req := httptest.NewRequest("POST", "/", strings.NewReader(`{"method":"version","id":1}`))
		w := httptest.NewRecorder() // This doesn't implement http.Hijacker

		s.jsonRPCRead(w, req, true)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "webserver doesn't support hijacking")
	})

	// Note: The jsonRPCRead method is primarily designed to work with real HTTP connections
	// that can be hijacked for long polling support. The httptest.ResponseRecorder used in
	// unit tests doesn't support hijacking, so we can only test the early error paths.
	// The main JSON-RPC parsing, command execution, and response writing logic is tested
	// through other methods like parseCmd, standardCmdResult, and createMarshalledReply.
}

// TestNewServer tests the NewServer constructor
func TestNewServer(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("missing asset HTTP address", func(t *testing.T) {
		settings := &settings.Settings{
			Asset: settings.AssetSettings{
				HTTPAddress: "", // Empty address
			},
		}

		server, err := NewServer(logger, settings, nil, nil, nil, nil, nil, nil, nil, nil)

		require.Error(t, err)
		assert.Nil(t, server)
		assert.Contains(t, err.Error(), "missing setting: asset_httpAddress")
	})

	t.Run("invalid asset URL", func(t *testing.T) {
		settings := &settings.Settings{
			Asset: settings.AssetSettings{
				HTTPAddress: "not a valid url",
			},
			Propagation: settings.PropagationSettings{
				GRPCAddresses: []string{"localhost:9090"},
				HTTPAddresses: []string{"http://localhost:9091"},
			},
		}

		server, err := NewServer(logger, settings, nil, nil, nil, nil, nil, nil, nil, nil)

		require.Error(t, err)
		assert.Nil(t, server)
		assert.Contains(t, err.Error(), "Invalid URL")
	})

	t.Run("missing RPC listener URL", func(t *testing.T) {
		settings := &settings.Settings{
			Asset: settings.AssetSettings{
				HTTPAddress: "http://localhost:8090",
			},
			Propagation: settings.PropagationSettings{
				GRPCAddresses: []string{"localhost:9090"},
				HTTPAddresses: []string{"http://localhost:9091"},
			},
			RPC: settings.RPCSettings{
				RPCListenerURL: nil, // Missing listener URL
			},
		}

		server, err := NewServer(logger, settings, nil, nil, nil, nil, nil, nil, nil, nil)

		require.Error(t, err)
		assert.Nil(t, server)
		assert.Contains(t, err.Error(), "rpc_listener_url not set in config")
	})

	// Note: Testing successful server creation is complex because the real NewServer
	// calls util.GetListener which creates network listeners and os.Exit(1) on failure.
	// These aspects are better tested through integration tests.
}

// TestStart tests the Start method
func TestStart(t *testing.T) {
	logger := mocklogger.NewTestLogger()

	t.Run("already started - returns immediately", func(t *testing.T) {
		s := &RPCServer{
			logger:   logger,
			started:  1, // Already started
			settings: &settings.Settings{},
		}

		readyCh := make(chan struct{})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := s.Start(ctx, readyCh)

		require.NoError(t, err)

		// Should close ready channel even if already started
		select {
		case <-readyCh:
			// Good, channel was closed
		case <-time.After(100 * time.Millisecond):
			t.Fatal("ready channel was not closed")
		}
	})

	t.Run("start with listeners", func(t *testing.T) {
		// Create a test listener
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		s := &RPCServer{
			logger:    logger,
			started:   0,
			shutdown:  0,
			listeners: []net.Listener{listener},
			settings: &settings.Settings{
				RPC: settings.RPCSettings{
					RPCMaxClients: 10,
				},
			},
			statusLines:            make(map[int]string),
			requestProcessShutdown: make(chan struct{}),
		}

		// Initialize the server
		err = s.Init(context.Background())
		require.NoError(t, err)

		readyCh := make(chan struct{})

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		// Start server in background
		go func() {
			_ = s.Start(ctx, readyCh)
		}()

		// Wait for ready signal
		select {
		case <-readyCh:
			// Server is ready - this is what we want to test
		case <-time.After(1 * time.Second):
			t.Fatal("server did not become ready in time")
		}

		// The server should now be started
		assert.Equal(t, int32(1), atomic.LoadInt32(&s.started))
	})

	t.Run("multiple starts only start once", func(t *testing.T) {
		s := &RPCServer{
			logger:   logger,
			started:  0,
			settings: &settings.Settings{},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Start multiple times concurrently
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				readyCh := make(chan struct{})
				_ = s.Start(ctx, readyCh)
			}()
		}

		wg.Wait()

		// All 3 calls should have incremented the started counter
		assert.Equal(t, int32(3), atomic.LoadInt32(&s.started))
	})
}
