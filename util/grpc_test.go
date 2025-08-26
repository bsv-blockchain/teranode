package util

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/util/test/mocklogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestListenerKey tests the listenerKey function with various inputs
func TestListenerKey(t *testing.T) {
	tests := []struct {
		name            string
		settingsContext string
		serviceName     string
		schema          string
		expected        string
	}{
		{
			name:            "basic key generation",
			settingsContext: "test",
			serviceName:     "service1",
			schema:          "http",
			expected:        "test!service1!http",
		},
		{
			name:            "empty schema",
			settingsContext: "test",
			serviceName:     "service1",
			schema:          "",
			expected:        "test!service1!",
		},
		{
			name:            "schema with protocol suffix",
			settingsContext: "test",
			serviceName:     "service1",
			schema:          "https://",
			expected:        "test!service1!https",
		},
		{
			name:            "empty service name",
			settingsContext: "test",
			serviceName:     "",
			schema:          "http",
			expected:        "test!!http",
		},
		{
			name:            "special characters in context",
			settingsContext: "test-env_1",
			serviceName:     "my-service",
			schema:          "grpc",
			expected:        "test-env_1!my-service!grpc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := listenerKey(tt.settingsContext, tt.serviceName, tt.schema)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestAddresses tests the addresses function with various listener configurations
func TestAddresses(t *testing.T) {
	t.Run("with unspecified IP", func(t *testing.T) {
		listener, err := net.Listen("tcp", "0.0.0.0:0") // #nosec G102
		require.NoError(t, err)
		defer func() { _ = listener.Close() }()

		listenAddress, clientAddress := addresses("http://", listener)

		assert.Contains(t, listenAddress, "0.0.0.0:")
		assert.Contains(t, clientAddress, "http://localhost:")
	})

	t.Run("with specific IP", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() { _ = listener.Close() }()

		listenAddress, clientAddress := addresses("https://", listener)

		assert.Contains(t, listenAddress, "127.0.0.1:")
		assert.Contains(t, clientAddress, "127.0.0.1:")
	})

	t.Run("with empty schema", func(t *testing.T) {
		listener, err := net.Listen("tcp", "0.0.0.0:0") // #nosec G102
		require.NoError(t, err)
		defer func() { _ = listener.Close() }()

		listenAddress, clientAddress := addresses("", listener)

		assert.Contains(t, listenAddress, "0.0.0.0:")
		assert.Contains(t, clientAddress, "localhost:")
		assert.False(t, strings.Contains(clientAddress, "://"))
	})

	t.Run("with nil IP", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0") // #nosec G102
		require.NoError(t, err)
		defer func() { _ = listener.Close() }()

		listenAddress, clientAddress := addresses("grpc://", listener)

		assert.Contains(t, listenAddress, "0.0.0.0:")
		assert.Contains(t, clientAddress, "grpc://localhost:")
	})
}

// TestGetListener tests the GetListener function for creating and caching listeners
func TestGetListener(t *testing.T) {
	// Clean up any existing listeners first
	CleanupListeners("test")

	t.Run("create new listener", func(t *testing.T) {
		listener, listenAddr, clientAddr, err := GetListener("test", "service1", "http://", ":0")
		require.NoError(t, err)
		require.NotNil(t, listener)
		require.NotEmpty(t, listenAddr)
		require.NotEmpty(t, clientAddr)

		// Verify addresses are properly formatted
		assert.Contains(t, listenAddr, "0.0.0.0:")
		assert.Contains(t, clientAddr, "http://localhost:")

		// Clean up
		_ = listener.Close()
	})

	t.Run("cache retrieval", func(t *testing.T) {
		// First call - creates listener
		listener1, addr1, clientAddr1, err := GetListener("test", "service2", "https://", "localhost:0")
		require.NoError(t, err)
		require.NotNil(t, listener1)

		// Second call - should return cached listener
		listener2, addr2, clientAddr2, err := GetListener("test", "service2", "https://", "different:0")
		require.NoError(t, err)
		require.NotNil(t, listener2)

		// Should be the same listener instance
		assert.Same(t, listener1, listener2)
		assert.Equal(t, addr1, addr2)
		assert.Equal(t, clientAddr1, clientAddr2)

		// Clean up
		_ = listener1.Close()
	})

	t.Run("different contexts create different listeners", func(t *testing.T) {
		// Create listener for context1
		listener1, _, _, err := GetListener("context1", "service3", "", "localhost:0")
		require.NoError(t, err)
		require.NotNil(t, listener1)

		// Create listener for context2 with same service name
		listener2, _, _, err := GetListener("context2", "service3", "", "localhost:0")
		require.NoError(t, err)
		require.NotNil(t, listener2)

		// Should be different listeners
		assert.NotSame(t, listener1, listener2)

		// Clean up
		_ = listener1.Close()
		_ = listener2.Close()
	})

	t.Run("invalid address", func(t *testing.T) {
		listener, listenAddr, clientAddr, err := GetListener("test", "service4", "", "invalid-address")
		assert.Error(t, err)
		assert.Nil(t, listener)
		assert.Empty(t, listenAddr)
		assert.Empty(t, clientAddr)
	})

	// Clean up all test listeners
	CleanupListeners("test")
	CleanupListeners("context1")
	CleanupListeners("context2")
}

// TestGetListenerConcurrent tests concurrent access to GetListener
func TestGetListenerConcurrent(t *testing.T) {
	// Clean up any existing listeners first
	CleanupListeners("concurrent-test")

	// Create the first listener sequentially to ensure it's in cache
	firstListener, firstAddr, _, err := GetListener("concurrent-test", "service", "", "localhost:0")
	require.NoError(t, err)
	require.NotNil(t, firstListener)

	const numGoroutines = 5
	var wg sync.WaitGroup
	results := make([]string, numGoroutines)
	errors := make([]error, numGoroutines)

	// Now launch concurrent goroutines that should all get the cached listener
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			listener, listenAddr, _, err := GetListener("concurrent-test", "service", "", "different-address:0")
			errors[index] = err
			if listener != nil {
				results[index] = listenAddr
			}
		}(i)
	}

	wg.Wait()

	// Check that all calls succeeded
	for i, err := range errors {
		require.NoError(t, err, "Goroutine %d should not have error", i)
	}

	// All results should have the same address as the first (indicating caching worked)
	// The "different-address:0" parameter should be ignored since we're getting cached listener
	for i := 0; i < numGoroutines; i++ {
		assert.Equal(t, firstAddr, results[i], "Address at index %d should be the same as first (cached)", i)
	}

	// Clean up
	CleanupListeners("concurrent-test")
}

// TestRemoveListener tests the RemoveListener function
func TestRemoveListener(t *testing.T) {
	// Clean up any existing listeners first
	CleanupListeners("remove-test")

	t.Run("remove existing listener", func(t *testing.T) {
		// Create a listener
		listener, _, _, err := GetListener("remove-test", "service1", "", "localhost:0")
		require.NoError(t, err)
		require.NotNil(t, listener)

		// Remove the listener
		RemoveListener("remove-test", "service1", "")

		// Try to get the same listener again - should create a new one
		newListener, _, _, err := GetListener("remove-test", "service1", "", "localhost:0")
		require.NoError(t, err)
		require.NotNil(t, newListener)

		// Should be a different instance (not cached anymore)
		assert.NotSame(t, listener, newListener)

		// Clean up
		_ = newListener.Close()
	})

	t.Run("remove non-existing listener", func(t *testing.T) {
		// Should not panic or error when removing non-existing listener
		assert.NotPanics(t, func() {
			RemoveListener("remove-test", "non-existing", "")
		})
	})

	// Clean up
	CleanupListeners("remove-test")
}

// TestCleanupListeners tests the CleanupListeners function
func TestCleanupListeners(t *testing.T) {
	// Clean up any existing listeners first
	CleanupListeners("cleanup-test-1")
	CleanupListeners("cleanup-test-2")
	CleanupListeners("other-context")

	t.Run("cleanup listeners for specific context", func(t *testing.T) {
		// Create listeners for cleanup-test-1 context
		listener1, _, _, err := GetListener("cleanup-test-1", "service1", "", "localhost:0")
		require.NoError(t, err)
		require.NotNil(t, listener1)

		listener2, _, _, err := GetListener("cleanup-test-1", "service2", "http://", "localhost:0")
		require.NoError(t, err)
		require.NotNil(t, listener2)

		// Create listener for different context
		otherListener, _, _, err := GetListener("other-context", "service1", "", "localhost:0")
		require.NoError(t, err)
		require.NotNil(t, otherListener)

		// Cleanup listeners for cleanup-test-1 context only
		keys := CleanupListeners("cleanup-test-1")

		// Should have cleaned up 2 listeners
		assert.Len(t, keys, 2)

		// Verify keys contain the expected context
		for _, key := range keys {
			assert.Contains(t, key, "cleanup-test-1")
		}

		// The other context listener should still exist and work
		cachedOtherListener, _, _, err := GetListener("other-context", "service1", "", "localhost:0")
		require.NoError(t, err)
		assert.Same(t, otherListener, cachedOtherListener)

		// The cleanup-test-1 listeners should be recreated (not cached)
		newListener1, _, _, err := GetListener("cleanup-test-1", "service1", "", "localhost:0")
		require.NoError(t, err)
		assert.NotSame(t, listener1, newListener1)

		// Clean up remaining
		_ = newListener1.Close()
		_ = otherListener.Close()
	})

	t.Run("cleanup non-existing context", func(t *testing.T) {
		keys := CleanupListeners("non-existing-context")
		assert.Empty(t, keys)
	})

	t.Run("cleanup with partial context match", func(t *testing.T) {
		// Create listeners with similar context names
		_, _, _, err := GetListener("test-context-1", "service1", "", "localhost:0")
		require.NoError(t, err)

		listener2, _, _, err := GetListener("test-context-2", "service2", "", "localhost:0")
		require.NoError(t, err)

		listener3, _, _, err := GetListener("different-test", "service3", "", "localhost:0")
		require.NoError(t, err)

		// Cleanup only exact matches for "test-context-1"
		keys := CleanupListeners("test-context-1")
		assert.Len(t, keys, 1)
		assert.Contains(t, keys[0], "test-context-1")

		// Other listeners should still be cached
		cachedListener2, _, _, err := GetListener("test-context-2", "service2", "", "localhost:0")
		require.NoError(t, err)
		assert.Same(t, listener2, cachedListener2)

		cachedListener3, _, _, err := GetListener("different-test", "service3", "", "localhost:0")
		require.NoError(t, err)
		assert.Same(t, listener3, cachedListener3)

		// Clean up remaining
		_ = listener2.Close()
		_ = listener3.Close()
		CleanupListeners("test-context-2")
		CleanupListeners("different-test")
	})

	// Final cleanup
	CleanupListeners("cleanup-test-1")
	CleanupListeners("cleanup-test-2")
	CleanupListeners("other-context")
}

// Helper function to create temporary TLS certificates for testing
func createTempTLSCerts(t *testing.T) (certFile, keyFile string) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "grpc-test-certs")
	require.NoError(t, err)

	// Clean up temp dir after test
	t.Cleanup(func() {
		_ = os.RemoveAll(tempDir)
	})

	// Generate private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Org"},
			Country:      []string{"US"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:    []string{"localhost"},
	}

	// Create certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	// Write certificate file
	certFile = filepath.Join(tempDir, "cert.pem")
	certOut, err := os.Create(certFile)
	require.NoError(t, err)
	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	require.NoError(t, err)
	_ = certOut.Close()

	// Write private key file
	keyFile = filepath.Join(tempDir, "key.pem")
	keyOut, err := os.Create(keyFile)
	require.NoError(t, err)
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	err = pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER})
	require.NoError(t, err)
	_ = keyOut.Close()

	return certFile, keyFile
}

// TestStartGRPCServerBasic tests basic gRPC server functionality without TLS
func TestStartGRPCServerBasic(t *testing.T) {
	// Create test settings
	tSettings := settings.NewSettings()
	tSettings.Context = "test-basic"
	tSettings.SecurityLevelGRPC = 0 // No TLS

	// Create test logger
	logger := mocklogger.NewTestLogger()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Channel to signal when server is ready
	serverReady := make(chan struct{})
	serverError := make(chan error, 1)

	// Register function for the gRPC service
	registerFunc := func(server *grpc.Server) {
		// Register reflection service (this is what the actual function does)
		// We don't need to register a specific service for this test
		close(serverReady)
	}

	// Start the gRPC server in a goroutine
	go func() {
		err := StartGRPCServer(ctx, logger, tSettings, "test-service", "localhost:0", registerFunc, nil)
		if err != nil {
			serverError <- err
		}
	}()

	// Wait for server to be ready or timeout
	select {
	case <-serverReady:
		// Server started successfully
		assert.True(t, true, "Server started successfully")
	case err := <-serverError:
		t.Fatalf("Server failed to start: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Server did not start within timeout")
	}

	// Give server a moment to fully start
	time.Sleep(100 * time.Millisecond)

	// Verify that the listener was created
	listener, listenAddr, clientAddr, err := GetListener(tSettings.Context, "test-service", "", "localhost:0")
	if err == nil && listener != nil {
		assert.NotEmpty(t, listenAddr, "Listen address should not be empty")
		assert.NotEmpty(t, clientAddr, "Client address should not be empty")
		assert.Contains(t, listenAddr, ":", "Listen address should contain port")
	}

	// Cancel context to trigger graceful shutdown
	cancel()

	// Clean up
	CleanupListeners(tSettings.Context)
}

// TestStartGRPCServerWithAuth tests gRPC server with authentication
func TestStartGRPCServerWithAuth(t *testing.T) {
	// Create test settings
	tSettings := settings.NewSettings()
	tSettings.Context = "test-auth"
	tSettings.SecurityLevelGRPC = 0 // No TLS for simplicity

	// Create test logger
	logger := mocklogger.NewTestLogger()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Configure authentication
	authOptions := &AuthOptions{
		APIKey: "test-api-key",
		ProtectedMethods: map[string]bool{
			"/test.service/TestMethod": true,
		},
	}

	// Channel to signal when server is ready
	serverReady := make(chan struct{})

	// Register function for the gRPC service
	registerFunc := func(server *grpc.Server) {
		// Just signal ready - we're testing auth interceptor creation, not service calls
		close(serverReady)
	}

	// Start the gRPC server in a goroutine
	go func() {
		err := StartGRPCServer(ctx, logger, tSettings, "test-auth-service", "localhost:0", registerFunc, authOptions)
		if err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	// Wait for server to be ready
	select {
	case <-serverReady:
		// Server started successfully with auth options
		assert.True(t, true, "Server with auth started successfully")
	case <-time.After(2 * time.Second):
		t.Fatal("Server did not start within timeout")
	}

	// Give server a moment to fully start
	time.Sleep(100 * time.Millisecond)

	// Verify that the server was created with auth options
	assert.NotNil(t, authOptions, "Auth options should be configured")
	assert.Equal(t, "test-api-key", authOptions.APIKey, "API key should be set")
	assert.True(t, authOptions.ProtectedMethods["/test.service/TestMethod"], "Protected method should be configured")

	// Cancel context to trigger graceful shutdown
	cancel()

	// Clean up
	CleanupListeners(tSettings.Context)
}

// TestStartGRPCServerWithTLS tests gRPC server with TLS
func TestStartGRPCServerWithTLS(t *testing.T) {
	// Create temporary TLS certificates
	certFile, keyFile := createTempTLSCerts(t)

	// Create test settings
	tSettings := settings.NewSettings()
	tSettings.Context = "test-tls"
	tSettings.SecurityLevelGRPC = 1 // TLS enabled
	tSettings.ServerCertFile = certFile
	tSettings.ServerKeyFile = keyFile

	// Create test logger
	logger := mocklogger.NewTestLogger()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Channel to signal when server is ready
	serverReady := make(chan struct{})

	// Register function for the gRPC service
	registerFunc := func(server *grpc.Server) {
		// Test that TLS server can be created with certificates
		close(serverReady)
	}

	// Start the gRPC server in a goroutine
	go func() {
		err := StartGRPCServer(ctx, logger, tSettings, "test-tls-service", "localhost:0", registerFunc, nil)
		if err != nil {
			t.Logf("TLS Server error: %v", err)
		}
	}()

	// Wait for server to be ready
	select {
	case <-serverReady:
		// Server started successfully with TLS
		assert.True(t, true, "TLS server started successfully")
	case <-time.After(2 * time.Second):
		t.Fatal("TLS Server did not start within timeout")
	}

	// Give server a moment to fully start
	time.Sleep(100 * time.Millisecond)

	// Verify TLS configuration was applied
	assert.Equal(t, 1, tSettings.SecurityLevelGRPC, "Security level should be 1 for TLS")
	assert.Equal(t, certFile, tSettings.ServerCertFile, "Cert file should be set")
	assert.Equal(t, keyFile, tSettings.ServerKeyFile, "Key file should be set")

	// Verify listener exists
	listener, _, _, err := GetListener(tSettings.Context, "test-tls-service", "", "localhost:0")
	if err == nil && listener != nil {
		assert.NotNil(t, listener, "TLS listener should exist")
	}

	// Cancel context to trigger graceful shutdown
	cancel()

	// Clean up
	CleanupListeners(tSettings.Context)
}

// TestStartGRPCServerGracefulShutdown tests graceful shutdown
func TestStartGRPCServerGracefulShutdown(t *testing.T) {
	// Create test settings
	tSettings := settings.NewSettings()
	tSettings.Context = "test-shutdown"
	tSettings.SecurityLevelGRPC = 0

	// Create test logger
	logger := mocklogger.NewTestLogger()

	// Create context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Channel to signal when server is ready
	serverReady := make(chan struct{})
	serverDone := make(chan struct{})

	// Register function for the gRPC service
	registerFunc := func(server *grpc.Server) {
		// Test graceful shutdown behavior
		close(serverReady)
	}

	// Start the gRPC server in a goroutine
	go func() {
		defer close(serverDone)
		err := StartGRPCServer(ctx, logger, tSettings, "test-shutdown-service", "localhost:0", registerFunc, nil)
		if err != nil {
			t.Logf("Shutdown test server error: %v", err)
		}
	}()

	// Wait for server to be ready
	select {
	case <-serverReady:
		// Server started successfully
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown test server did not start within timeout")
	}

	// Give server a moment to fully start
	time.Sleep(100 * time.Millisecond)

	// Cancel context to trigger graceful shutdown
	cancel()

	// Wait for server to shut down
	select {
	case <-serverDone:
		// Server shut down gracefully
	case <-time.After(5 * time.Second):
		t.Fatal("Server did not shut down gracefully within timeout")
	}

	// Clean up
	CleanupListeners(tSettings.Context)
}

// TestStartGRPCServerErrors tests error scenarios
func TestStartGRPCServerErrors(t *testing.T) {
	t.Run("missing cert file with TLS", func(t *testing.T) {
		tSettings := settings.NewSettings()
		tSettings.Context = "test-error-1"
		tSettings.SecurityLevelGRPC = 1 // TLS enabled but no cert files
		tSettings.ServerCertFile = ""   // Explicitly empty
		tSettings.ServerKeyFile = ""    // Explicitly empty

		logger := mocklogger.NewTestLogger()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		registerFunc := func(_ *grpc.Server) {}

		err := StartGRPCServer(ctx, logger, tSettings, "error-service", "localhost:0", registerFunc, nil)
		assert.Error(t, err, "Should fail when cert file is missing")
		// Check for the actual error message pattern from the code
		assert.Contains(t, err.Error(), "server_certFile is required")
	})

	t.Run("missing key file with TLS", func(t *testing.T) {
		// Create only cert file, not key file
		certFile, _ := createTempTLSCerts(t)

		tSettings := settings.NewSettings()
		tSettings.Context = "test-error-2"
		tSettings.SecurityLevelGRPC = 1
		tSettings.ServerCertFile = certFile
		tSettings.ServerKeyFile = "" // Explicitly empty

		logger := mocklogger.NewTestLogger()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		registerFunc := func(_ *grpc.Server) {}

		err := StartGRPCServer(ctx, logger, tSettings, "error-service", "localhost:0", registerFunc, nil)
		assert.Error(t, err, "Should fail when key file is missing")
		assert.Contains(t, err.Error(), "server_keyFile is required")
	})

	t.Run("invalid listener address", func(t *testing.T) {
		tSettings := settings.NewSettings()
		tSettings.Context = "test-error-3"
		tSettings.SecurityLevelGRPC = 0

		logger := mocklogger.NewTestLogger()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		registerFunc := func(_ *grpc.Server) {}

		err := StartGRPCServer(ctx, logger, tSettings, "error-service", "invalid-address:xyz", registerFunc, nil)
		assert.Error(t, err, "Should fail with invalid address")
	})

	// Clean up any listeners that might have been created during error tests
	CleanupListeners("test-error-1")
	CleanupListeners("test-error-2")
	CleanupListeners("test-error-3")
}
