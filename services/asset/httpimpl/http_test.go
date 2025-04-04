package httpimpl

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/services/asset/repository"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockRepository is a mock implementation of repository.Interface
type MockRepository struct {
	mock.Mock
}

// Health mocks the Health method
func (m *MockRepository) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

// BlockchainClient mocks the BlockchainClient method
func (m *MockRepository) BlockchainClient() blockchain.ClientI {
	args := m.Called()
	return args.Get(0).(blockchain.ClientI)
}

// TestNew tests the New function
func TestNew(t *testing.T) {
	t.Run("Without dashboard", func(t *testing.T) {
		// Create a test logger
		logger := ulogger.TestLogger{}

		// Create test settings
		testSettings := &settings.Settings{
			Asset: settings.AssetSettings{
				APIPrefix:         "/api/v1",
				EchoDebug:         true,
				SignHTTPResponses: false,
			},
			Dashboard: settings.DashboardSettings{
				Enabled: false,
			},
			SecurityLevelHTTP: 0, // HTTP mode
		}

		// Create a mock repository
		mockRepo := new(MockRepository)
		mockBlockchainClient := &blockchain.Mock{}
		mockRepo.On("BlockchainClient").Return(mockBlockchainClient)
		mockRepo.On("Health", mock.Anything, false).Return(http.StatusOK, "OK", nil)

		// Create a repository wrapper with our mock
		repoWrapper := &repository.Repository{}

		// Call the function to be tested
		httpServer, err := New(logger, testSettings, repoWrapper)

		// Assert that the HTTP server was created successfully
		assert.NoError(t, err)
		assert.NotNil(t, httpServer)
		assert.Equal(t, logger, httpServer.logger)
		assert.Equal(t, testSettings, httpServer.settings)
		assert.NotNil(t, httpServer.e)
		assert.False(t, httpServer.startTime.IsZero())
		assert.Nil(t, httpServer.privKey)
	})

	t.Run("With dashboard enabled", func(t *testing.T) {
		// Create a test logger
		logger := ulogger.TestLogger{}

		// Create test settings with dashboard enabled and RPC auth configured
		testSettings := &settings.Settings{
			Asset: settings.AssetSettings{
				APIPrefix:         "/api/v1",
				EchoDebug:         true,
				SignHTTPResponses: false,
			},
			Dashboard: settings.DashboardSettings{
				Enabled: true,
			},
			RPC: settings.RPCSettings{
				RPCUser: "bitcoin",
				RPCPass: "bitcoin",
			},
			SecurityLevelHTTP: 0, // HTTP mode
		}

		// Create a mock repository
		mockRepo := new(MockRepository)
		mockBlockchainClient := &blockchain.Mock{}
		mockRepo.On("BlockchainClient").Return(mockBlockchainClient)
		mockRepo.On("Health", mock.Anything, false).Return(http.StatusOK, "OK", nil)

		// Create a repository wrapper with our mock
		repoWrapper := &repository.Repository{}

		// Call the function to be tested
		httpServer, err := New(logger, testSettings, repoWrapper)

		// Assert that the HTTP server was created successfully
		assert.NoError(t, err)
		assert.NotNil(t, httpServer)

		// Test dashboard authentication endpoints
		e := httpServer.e

		// First test OPTIONS request to check CORS headers
		optionsReq := httptest.NewRequest(http.MethodOptions, "/api/auth/check", nil)
		optionsReq.Header.Set("Origin", "http://localhost:8090")
		optionsReq.Header.Set("Access-Control-Request-Method", "GET")
		optionsReq.Header.Set("Access-Control-Request-Headers", "Authorization")

		optionsRec := httptest.NewRecorder()

		e.ServeHTTP(optionsRec, optionsReq)

		// Test CORS headers
		corsHeaders := optionsRec.Header()
		assert.Equal(t, "http://localhost:8090", corsHeaders.Get("Access-Control-Allow-Origin"))
		assert.Equal(t, "true", corsHeaders.Get("Access-Control-Allow-Credentials"))
		assert.Contains(t, corsHeaders.Get("Access-Control-Allow-Methods"), "GET")
		assert.Contains(t, corsHeaders.Get("Access-Control-Allow-Headers"), "Authorization")

		// Now test authentication endpoint without credentials
		req := httptest.NewRequest(http.MethodGet, "/api/auth/check", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code) // Should return 401 when not authenticated
	})
}

// TestNewWithSigningEnabled tests the New function with response signing enabled
func TestNewWithSigningEnabled(t *testing.T) {
	// Create a test logger
	logger := ulogger.TestLogger{}

	// Generate a test Ed25519 key pair
	_, privKey, _ := ed25519.GenerateKey(nil)
	privKeyHex := hex.EncodeToString(privKey)

	// Create test settings with signing enabled
	testSettings := &settings.Settings{
		Asset: settings.AssetSettings{
			APIPrefix:         "/api/v1",
			EchoDebug:         true,
			SignHTTPResponses: true,
		},
		P2P: settings.P2PSettings{
			PrivateKey: privKeyHex,
		},
		Dashboard: settings.DashboardSettings{
			Enabled: false,
		},
		SecurityLevelHTTP: 0, // HTTP mode
	}

	// Create a mock repository
	mockRepo := new(MockRepository)
	mockBlockchainClient := &blockchain.Mock{}
	mockRepo.On("BlockchainClient").Return(mockBlockchainClient)
	mockRepo.On("Health", mock.Anything, false).Return(http.StatusOK, "OK", nil)

	// Create a repository wrapper with our mock
	repoWrapper := &repository.Repository{}

	// Call the function to be tested
	httpServer, err := New(logger, testSettings, repoWrapper)

	// Assert that the HTTP server was created successfully
	assert.NoError(t, err)
	assert.NotNil(t, httpServer)
	assert.NotNil(t, httpServer.privKey)
}

// TestAdaptStdHandler tests the AdaptStdHandler function
func TestAdaptStdHandler(t *testing.T) {
	// Create a test handler
	handlerCalled := false
	testHandler := func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true

		w.WriteHeader(http.StatusOK)
		n, err := w.Write([]byte("Test response"))
		assert.NoError(t, err)
		assert.Equal(t, 13, n)
	}

	// Adapt the handler
	adaptedHandler := AdaptStdHandler(testHandler)

	// Create a test Echo context
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Call the adapted handler
	err := adaptedHandler(c)

	// Assert that the handler was called and returned the expected response
	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "Test response", rec.Body.String())
}

// TestSign tests the Sign function
func TestSign(t *testing.T) {
	// Generate a test Ed25519 key pair
	pubKey, privKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	// Convert to libp2p key
	privLibp2pKey, err := crypto.UnmarshalEd25519PrivateKey(privKey)
	require.NoError(t, err)

	// Create a test HTTP server with the private key
	httpServer := &HTTP{
		logger:    ulogger.TestLogger{},
		privKey:   privLibp2pKey,
		startTime: time.Now(),
	}

	// Create a test Echo response
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Test data to sign
	testData := []byte("test data to sign")

	// Call the Sign function
	err = httpServer.Sign(c.Response(), testData)
	assert.NoError(t, err)

	// Get the signature from the response header
	signature := rec.Header().Get("X-Signature")
	assert.NotEmpty(t, signature)

	// Decode the signature
	signatureBytes, err := hex.DecodeString(signature)
	require.NoError(t, err)

	// Verify the signature
	assert.True(t, ed25519.Verify(pubKey, testData, signatureBytes))
}

// TestCustomLoggerMiddleware tests the customLoggerMiddleware function
func TestCustomLoggerMiddleware(t *testing.T) {
	// Create a test logger
	logger := ulogger.TestLogger{}

	// Create the middleware
	middleware := customLoggerMiddleware(logger)

	// Create a test Echo instance
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Create a test handler
	testHandler := func(c echo.Context) error {
		return c.String(http.StatusOK, "Test response")
	}

	// Call the middleware with the test handler
	handler := middleware(testHandler)
	err := handler(c)

	// Assert that the handler was called and returned the expected response
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "Test response", rec.Body.String())
}

// TestHTTPInit tests the Init method
func TestHTTPInit(t *testing.T) {
	// Create a test HTTP server
	httpServer := &HTTP{
		logger:    ulogger.TestLogger{},
		startTime: time.Now(),
	}

	// Call the Init method
	err := httpServer.Init(context.Background())

	// Assert that the method returned no error
	assert.NoError(t, err)
}

// TestHTTPStop tests the Stop method
func TestHTTPStop(t *testing.T) {
	// Create a test HTTP server
	httpServer := &HTTP{
		logger:    ulogger.TestLogger{},
		e:         echo.New(),
		startTime: time.Now(),
	}

	// Call the Stop method
	err := httpServer.Stop(context.Background())

	// Assert that the method returned no error
	assert.NoError(t, err)
}

// TestStartHTTPS tests the Start method with HTTPS configuration
func TestStartHTTPS(t *testing.T) {
	// Create a test logger
	logger := ulogger.TestLogger{}

	// Create test settings with HTTPS enabled
	testSettings := &settings.Settings{
		SecurityLevelHTTP: 1, // HTTPS mode
		ServerCertFile:    "test-cert.pem",
		ServerKeyFile:     "test-key.pem",
	}

	// Create a test HTTP server
	httpServer := &HTTP{
		logger:   logger,
		settings: testSettings,
		e:        echo.New(),
	}

	// Create a context that can be canceled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Call cancel immediately to ensure the server doesn't actually try to start
	cancel()

	// Call the Start method with a test address
	err := httpServer.Start(ctx, "localhost:0")

	// We expect an error since we're not actually starting the server
	// The exact error may vary depending on the implementation
	assert.Error(t, err)
}

// TestStartHTTPSMissingCertFile tests the Start method with HTTPS configuration but missing cert file
func TestStartHTTPSMissingCertFile(t *testing.T) {
	// Create a test logger
	logger := ulogger.TestLogger{}

	// Create test settings with HTTPS enabled but missing cert file
	testSettings := &settings.Settings{
		SecurityLevelHTTP: 1,  // HTTPS mode
		ServerCertFile:    "", // Missing cert file
		ServerKeyFile:     "test-key.pem",
	}

	// Create a test HTTP server
	httpServer := &HTTP{
		logger:   logger,
		settings: testSettings,
		e:        echo.New(),
	}

	// Call the Start method with a test address
	err := httpServer.Start(context.Background(), "localhost:0")

	// Assert that the method returns an error about missing cert file
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server_certFile is required")
}

// TestStartHTTPSMissingKeyFile tests the Start method with HTTPS configuration but missing key file
func TestStartHTTPSMissingKeyFile(t *testing.T) {
	// Create a test logger
	logger := ulogger.TestLogger{}

	// Create test settings with HTTPS enabled but missing key file
	testSettings := &settings.Settings{
		SecurityLevelHTTP: 1, // HTTPS mode
		ServerCertFile:    "test-cert.pem",
		ServerKeyFile:     "", // Missing key file
	}

	// Create a test HTTP server
	httpServer := &HTTP{
		logger:   logger,
		settings: testSettings,
		e:        echo.New(),
	}

	// Call the Start method with a test address
	err := httpServer.Start(context.Background(), "localhost:0")

	// Assert that the method returns an error about missing key file
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server_keyFile is required")
}

// TestAddHTTPHandler tests the AddHTTPHandler method
func TestAddHTTPHandler(t *testing.T) {
	// Create a test Echo instance
	e := echo.New()

	// Create a test HTTP server
	httpServer := &HTTP{
		logger:    ulogger.TestLogger{},
		e:         e,
		startTime: time.Now(),
	}

	// Create a test handler
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		n, err := w.Write([]byte("Test response"))
		assert.NoError(t, err)
		assert.Equal(t, 13, n)
	})

	// Call the AddHTTPHandler method
	err := httpServer.AddHTTPHandler("/test", testHandler)

	// Assert that the method returned no error
	assert.NoError(t, err)

	// Test that the handler was added correctly
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "Test response", rec.Body.String())
}
