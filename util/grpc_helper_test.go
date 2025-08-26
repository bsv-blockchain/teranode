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
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestCreateAuthInterceptor(t *testing.T) {
	// Test constants
	const (
		validAPIKey       = "valid-api-key" // nolint:gosec
		invalidAPIKey     = "invalid-api-key"
		protectedMethod   = "/test.service/ProtectedMethod"
		unprotectedMethod = "/test.service/UnprotectedMethod"

		success = "success"
	)

	// Create a map of protected methods
	protectedMethods := map[string]bool{
		protectedMethod: true,
	}

	// Create the auth interceptor
	interceptor := CreateAuthInterceptor(validAPIKey, protectedMethods)

	// Mock handler that returns success
	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return success, nil
	}

	// Test cases
	tests := []struct {
		name           string
		method         string
		apiKey         string
		includeAPIKey  bool
		expectedError  error
		expectedResult interface{}
	}{
		{
			name:           "Unprotected method should pass without API key",
			method:         unprotectedMethod,
			includeAPIKey:  false,
			expectedError:  nil,
			expectedResult: success,
		},
		{
			name:           "Protected method without API key should fail",
			method:         protectedMethod,
			includeAPIKey:  false,
			expectedError:  status.Error(codes.Unauthenticated, "missing metadata"),
			expectedResult: nil,
		},
		{
			name:           "Protected method with invalid API key should fail",
			method:         protectedMethod,
			apiKey:         invalidAPIKey,
			includeAPIKey:  true,
			expectedError:  status.Error(codes.Unauthenticated, "invalid API key"),
			expectedResult: nil,
		},
		{
			name:           "Protected method with valid API key should pass",
			method:         protectedMethod,
			apiKey:         validAPIKey,
			includeAPIKey:  true,
			expectedError:  nil,
			expectedResult: success,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create context with or without API key
			ctx := context.Background()

			if tc.includeAPIKey {
				md := metadata.New(map[string]string{
					apiKeyHeader: tc.apiKey,
				})
				ctx = metadata.NewIncomingContext(ctx, md)
			}

			// Create UnaryServerInfo with the method
			info := &grpc.UnaryServerInfo{
				FullMethod: tc.method,
			}

			// Call the interceptor
			result, err := interceptor(ctx, "request", info, mockHandler)

			// Check error
			if tc.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, status.Code(tc.expectedError), status.Code(err))
				assert.Equal(t, status.Convert(tc.expectedError).Message(), status.Convert(err).Message())
			} else {
				assert.NoError(t, err)
			}

			// Check result
			assert.Equal(t, tc.expectedResult, result)

			// If successful, check that authentication info was added to context
			if err == nil && tc.method == protectedMethod {
				// Create a new handler that checks the context value
				authCheckHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
					authenticated, ok := ctx.Value(authenticatedKey).(bool)
					assert.True(t, ok, "Context should contain authenticated value")
					assert.True(t, authenticated, "Authenticated value should be true")

					return "success", nil
				}

				// Call the interceptor again with the auth check handler
				_, err = interceptor(ctx, "request", info, authCheckHandler)
				assert.NoError(t, err)
			}
		})
	}
}

func TestCreateAuthInterceptorMissingMetadata(t *testing.T) {
	// Create the auth interceptor
	interceptor := CreateAuthInterceptor("test-key", map[string]bool{"/test.service/ProtectedMethod": true})

	// Create context without metadata
	ctx := context.Background()

	// Create UnaryServerInfo with a protected method
	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.service/ProtectedMethod",
	}

	// Mock handler that should not be called
	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		t.Fatal("Handler should not be called")
		return nil, nil
	}

	// Call the interceptor
	result, err := interceptor(ctx, "request", info, mockHandler)

	// Check error
	assert.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Equal(t, "missing metadata", status.Convert(err).Message())
	assert.Nil(t, result)
}

func TestCreateAuthInterceptorMultipleAPIKeys(t *testing.T) {
	// Create the auth interceptor
	interceptor := CreateAuthInterceptor("test-key", map[string]bool{"/test.service/ProtectedMethod": true})

	// Create context with multiple API keys
	md := metadata.New(map[string]string{})
	md.Append(apiKeyHeader, "wrong-key", "test-key")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// Create UnaryServerInfo with a protected method
	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.service/ProtectedMethod",
	}

	// Mock handler that returns success
	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	// Call the interceptor
	result, err := interceptor(ctx, "request", info, mockHandler)

	// First key is used for validation, which is wrong, so should fail
	assert.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Equal(t, "invalid API key", status.Convert(err).Message())
	assert.Nil(t, result)
}

// Test helper functions

// createTempCertificates creates temporary certificate files for testing TLS functionality.
// Returns the paths to the created certificate and key files, and a cleanup function.
func createTempCertificates(t *testing.T) (certFile, keyFile, caCertFile string, cleanup func()) {
	t.Helper()

	// Create temporary directory
	tempDir := t.TempDir()

	certFile = filepath.Join(tempDir, "cert.pem")
	keyFile = filepath.Join(tempDir, "key.pem")
	caCertFile = filepath.Join(tempDir, "ca-cert.pem")

	// Generate a private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Test Org"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	// Create the certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	// Write certificate to file
	certOut, err := os.Create(certFile)
	require.NoError(t, err)
	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	require.NoError(t, err)
	err = certOut.Close()
	require.NoError(t, err)

	// Write private key to file
	keyOut, err := os.Create(keyFile)
	require.NoError(t, err)
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	err = pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER})
	require.NoError(t, err)
	err = keyOut.Close()
	require.NoError(t, err)

	// Copy certificate as CA cert for testing
	caCertOut, err := os.Create(caCertFile)
	require.NoError(t, err)
	err = pem.Encode(caCertOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	require.NoError(t, err)
	err = caCertOut.Close()
	require.NoError(t, err)

	// Cleanup function removes temporary files (handled by t.TempDir())
	cleanup = func() {
		// TempDir automatically cleans up
	}

	return certFile, keyFile, caCertFile, cleanup
}

// createTestSettings creates a settings.Settings instance with test configuration.
func createTestSettings(tracingEnabled, prometheusEnabled bool, securityLevel int) *settings.Settings {
	return &settings.Settings{
		TracingEnabled:           tracingEnabled,
		UsePrometheusGRPCMetrics: prometheusEnabled,
		SecurityLevelGRPC:        securityLevel,
	}
}

// mockLogger creates a simple logger for testing.
func mockLogger() ulogger.Logger {
	return ulogger.TestLogger{}
}

// resetPrometheusOnce resets the sync.Once variables for testing.
// This is needed to test RegisterPrometheusMetrics multiple times.
func resetPrometheusOnce() {
	prometheusRegisterServerOnce = sync.Once{}
	prometheusRegisterClientOnce = sync.Once{}
}

// PasswordCredentials Tests

func TestNewPassCredentials(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected PasswordCredentials
	}{
		{
			name:     "Empty credentials map",
			input:    map[string]string{},
			expected: PasswordCredentials{},
		},
		{
			name: "Single credential",
			input: map[string]string{
				"username": "testuser",
			},
			expected: PasswordCredentials{
				"username": "testuser",
			},
		},
		{
			name: "Multiple credentials",
			input: map[string]string{
				"username": "testuser",
				"password": "testpass",
				"token":    "abc123",
			},
			expected: PasswordCredentials{
				"username": "testuser",
				"password": "testpass",
				"token":    "abc123",
			},
		},
		{
			name:     "Nil input map",
			input:    nil,
			expected: PasswordCredentials(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NewPassCredentials(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPasswordCredentialsGetRequestMetadata(t *testing.T) {
	tests := []struct {
		name     string
		creds    PasswordCredentials
		expected map[string]string
	}{
		{
			name:     "Empty credentials",
			creds:    PasswordCredentials{},
			expected: map[string]string{},
		},
		{
			name: "Single credential",
			creds: PasswordCredentials{
				"api-key": "secret123",
			},
			expected: map[string]string{
				"api-key": "secret123",
			},
		},
		{
			name: "Multiple credentials",
			creds: PasswordCredentials{
				"username":    "user",
				"password":    "pass",
				"client-name": "test-client",
			},
			expected: map[string]string{
				"username":    "user",
				"password":    "pass",
				"client-name": "test-client",
			},
		},
		{
			name:     "Nil credentials",
			creds:    PasswordCredentials(nil),
			expected: map[string]string(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			result, err := tt.creds.GetRequestMetadata(ctx, "test-uri")

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPasswordCredentialsGetRequestMetadataWithDifferentParams(t *testing.T) {
	creds := PasswordCredentials{
		"test": "value",
	}
	ctx := context.Background()

	// Test that URI parameter doesn't affect the result
	result1, err1 := creds.GetRequestMetadata(ctx)
	result2, err2 := creds.GetRequestMetadata(ctx, "uri1", "uri2", "uri3")

	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, result1, result2)
	assert.Equal(t, map[string]string{"test": "value"}, result1)
}

func TestPasswordCredentialsRequireTransportSecurity(t *testing.T) {
	tests := []struct {
		name  string
		creds PasswordCredentials
	}{
		{
			name:  "Empty credentials",
			creds: PasswordCredentials{},
		},
		{
			name: "Non-empty credentials",
			creds: PasswordCredentials{
				"username": "test",
				"password": "secret",
			},
		},
		{
			name:  "Nil credentials",
			creds: PasswordCredentials(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.creds.RequireTransportSecurity()
			assert.False(t, result, "RequireTransportSecurity should always return false")
		})
	}
}

// loadTLSCredentials Tests

func TestLoadTLSCredentialsSecurityLevel0(t *testing.T) {
	connectionOptions := &ConnectionOptions{
		SecurityLevel: 0,
	}

	// Test client credentials
	clientCreds, err := loadTLSCredentials(connectionOptions, false)
	assert.NoError(t, err)
	assert.NotNil(t, clientCreds)

	// Test server credentials
	serverCreds, err := loadTLSCredentials(connectionOptions, true)
	assert.NoError(t, err)
	assert.NotNil(t, serverCreds)
}

func TestLoadTLSCredentialsSecurityLevel1(t *testing.T) {
	certFile, keyFile, _, cleanup := createTempCertificates(t)
	defer cleanup()

	connectionOptions := &ConnectionOptions{
		SecurityLevel: 1,
		CertFile:      certFile,
		KeyFile:       keyFile,
	}

	// Test server credentials (requires cert/key files)
	serverCreds, err := loadTLSCredentials(connectionOptions, true)
	assert.NoError(t, err)
	assert.NotNil(t, serverCreds)

	// Test client credentials (no cert required)
	clientCreds, err := loadTLSCredentials(connectionOptions, false)
	assert.NoError(t, err)
	assert.NotNil(t, clientCreds)
}

func TestLoadTLSCredentialsSecurityLevel1_ServerMissingCert(t *testing.T) {
	connectionOptions := &ConnectionOptions{
		SecurityLevel: 1,
		CertFile:      "nonexistent.pem",
		KeyFile:       "nonexistent.key",
	}

	// Test server credentials with missing cert files
	serverCreds, err := loadTLSCredentials(connectionOptions, true)
	assert.Error(t, err)
	assert.Nil(t, serverCreds)
	assert.Contains(t, err.Error(), "failed to read key pair")
}

func TestLoadTLSCredentialsSecurityLevel2(t *testing.T) {
	certFile, keyFile, caCertFile, cleanup := createTempCertificates(t)
	defer cleanup()

	connectionOptions := &ConnectionOptions{
		SecurityLevel: 2,
		CertFile:      certFile,
		KeyFile:       keyFile,
		CaCertFile:    caCertFile,
	}

	// Test server credentials
	serverCreds, err := loadTLSCredentials(connectionOptions, true)
	assert.NoError(t, err)
	assert.NotNil(t, serverCreds)

	// Test client credentials
	clientCreds, err := loadTLSCredentials(connectionOptions, false)
	assert.NoError(t, err)
	assert.NotNil(t, clientCreds)
}

func TestLoadTLSCredentialsSecurityLevel2_ClientMissingCACert(t *testing.T) {
	certFile, keyFile, _, cleanup := createTempCertificates(t)
	defer cleanup()

	connectionOptions := &ConnectionOptions{
		SecurityLevel: 2,
		CertFile:      certFile,
		KeyFile:       keyFile,
		CaCertFile:    "nonexistent-ca.pem",
	}

	// Test client credentials with missing CA cert
	clientCreds, err := loadTLSCredentials(connectionOptions, false)
	assert.Error(t, err)
	assert.Nil(t, clientCreds)
	assert.Contains(t, err.Error(), "failed to read ca cert file")
}

func TestLoadTLSCredentialsSecurityLevel2_ClientMissingClientCert(t *testing.T) {
	_, _, caCertFile, cleanup := createTempCertificates(t)
	defer cleanup()

	connectionOptions := &ConnectionOptions{
		SecurityLevel: 2,
		CertFile:      "nonexistent.pem",
		KeyFile:       "nonexistent.key",
		CaCertFile:    caCertFile,
	}

	// Test client credentials with missing client cert
	clientCreds, err := loadTLSCredentials(connectionOptions, false)
	assert.Error(t, err)
	assert.Nil(t, clientCreds)
	assert.Contains(t, err.Error(), "failed to read key pair")
}

func TestLoadTLSCredentialsSecurityLevel3(t *testing.T) {
	certFile, keyFile, caCertFile, cleanup := createTempCertificates(t)
	defer cleanup()

	connectionOptions := &ConnectionOptions{
		SecurityLevel: 3,
		CertFile:      certFile,
		KeyFile:       keyFile,
		CaCertFile:    caCertFile,
	}

	// Test server credentials
	serverCreds, err := loadTLSCredentials(connectionOptions, true)
	assert.NoError(t, err)
	assert.NotNil(t, serverCreds)

	// Test client credentials
	clientCreds, err := loadTLSCredentials(connectionOptions, false)
	assert.NoError(t, err)
	assert.NotNil(t, clientCreds)
}

func TestLoadTLSCredentialsSecurityLevel3_ServerMissingCACert(t *testing.T) {
	certFile, keyFile, _, cleanup := createTempCertificates(t)
	defer cleanup()

	connectionOptions := &ConnectionOptions{
		SecurityLevel: 3,
		CertFile:      certFile,
		KeyFile:       keyFile,
		CaCertFile:    "nonexistent-ca.pem",
	}

	// Test server credentials with missing CA cert
	serverCreds, err := loadTLSCredentials(connectionOptions, true)
	assert.Error(t, err)
	assert.Nil(t, serverCreds)
	assert.Contains(t, err.Error(), "failed to read ca cert file")
}

func TestLoadTLSCredentialsInvalidSecurityLevel(t *testing.T) {
	connectionOptions := &ConnectionOptions{
		SecurityLevel: 5, // Invalid security level
	}

	// Test with invalid security level
	creds, err := loadTLSCredentials(connectionOptions, false)
	assert.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "securityLevel must be 0, 1, 2 or 3")

	// Test server with invalid security level
	creds, err = loadTLSCredentials(connectionOptions, true)
	assert.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "securityLevel must be 0, 1, 2 or 3")
}

func TestLoadTLSCredentialsNegativeSecurityLevel(t *testing.T) {
	connectionOptions := &ConnectionOptions{
		SecurityLevel: -1,
	}

	creds, err := loadTLSCredentials(connectionOptions, false)
	assert.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "securityLevel must be 0, 1, 2 or 3")
}

// retryInterceptor Tests

func TestRetryInterceptorSuccess(t *testing.T) {
	interceptor := retryInterceptor(3, 10*time.Millisecond)

	callCount := 0
	mockInvoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		callCount++
		return nil // Success on first try
	}

	ctx := context.Background()
	err := interceptor(ctx, "/test.service/TestMethod", "request", "reply", nil, mockInvoker)

	assert.NoError(t, err)
	assert.Equal(t, 1, callCount, "Should call invoker exactly once on success")
}

func TestRetryInterceptorRetryOnUnavailable(t *testing.T) {
	maxRetries := 3
	interceptor := retryInterceptor(maxRetries, 1*time.Millisecond)

	callCount := 0
	mockInvoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		callCount++
		if callCount < maxRetries {
			return status.Error(codes.Unavailable, "service unavailable")
		}
		return nil // Success on final retry
	}

	ctx := context.Background()
	err := interceptor(ctx, "/test.service/TestMethod", "request", "reply", nil, mockInvoker)

	assert.NoError(t, err)
	assert.Equal(t, maxRetries, callCount, "Should retry until success")
}

func TestRetryInterceptorRetryOnDeadlineExceeded(t *testing.T) {
	maxRetries := 2
	interceptor := retryInterceptor(maxRetries, 1*time.Millisecond)

	callCount := 0
	mockInvoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		callCount++
		if callCount == 1 {
			return status.Error(codes.DeadlineExceeded, "deadline exceeded")
		}
		return nil // Success on retry
	}

	ctx := context.Background()
	err := interceptor(ctx, "/test.service/TestMethod", "request", "reply", nil, mockInvoker)

	assert.NoError(t, err)
	assert.Equal(t, 2, callCount, "Should retry on DeadlineExceeded")
}

func TestRetryInterceptorNoRetryOnNonTransientError(t *testing.T) {
	interceptor := retryInterceptor(3, 1*time.Millisecond)

	callCount := 0
	expectedErr := status.Error(codes.InvalidArgument, "invalid argument")
	mockInvoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		callCount++
		return expectedErr
	}

	ctx := context.Background()
	err := interceptor(ctx, "/test.service/TestMethod", "request", "reply", nil, mockInvoker)

	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, 1, callCount, "Should not retry on non-transient errors")
}

func TestRetryInterceptorMaxRetriesExceeded(t *testing.T) {
	maxRetries := 2
	interceptor := retryInterceptor(maxRetries, 1*time.Millisecond)

	callCount := 0
	expectedErr := status.Error(codes.Unavailable, "always unavailable")
	mockInvoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		callCount++
		return expectedErr
	}

	ctx := context.Background()
	err := interceptor(ctx, "/test.service/TestMethod", "request", "reply", nil, mockInvoker)

	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, maxRetries, callCount, "Should call invoker exactly maxRetries times")
}

func TestRetryInterceptorBackoffTiming(t *testing.T) {
	backoff := 50 * time.Millisecond
	interceptor := retryInterceptor(3, backoff)

	callCount := 0
	var callTimes []time.Time
	mockInvoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		callCount++
		callTimes = append(callTimes, time.Now())
		if callCount < 3 {
			return status.Error(codes.Unavailable, "unavailable")
		}
		return nil
	}

	ctx := context.Background()
	start := time.Now()
	err := interceptor(ctx, "/test.service/TestMethod", "request", "reply", nil, mockInvoker)

	assert.NoError(t, err)
	assert.Equal(t, 3, callCount)
	assert.Len(t, callTimes, 3)

	// Check that there's approximately the expected backoff between calls
	if len(callTimes) >= 2 {
		timeBetweenCalls := callTimes[1].Sub(callTimes[0])
		assert.True(t, timeBetweenCalls >= backoff,
			"Time between calls should be at least backoff duration")
		assert.True(t, timeBetweenCalls < backoff+10*time.Millisecond,
			"Time between calls should not be much more than backoff duration")
	}

	totalTime := time.Since(start)
	expectedMinTime := 2 * backoff // Two backoffs for three calls
	assert.True(t, totalTime >= expectedMinTime,
		"Total time should be at least 2 * backoff for 3 calls with 2 retries")
}

func TestRetryInterceptorZeroRetries(t *testing.T) {
	interceptor := retryInterceptor(0, 1*time.Millisecond)

	callCount := 0
	expectedErr := status.Error(codes.Unavailable, "unavailable")
	mockInvoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		callCount++
		return expectedErr
	}

	ctx := context.Background()
	err := interceptor(ctx, "/test.service/TestMethod", "request", "reply", nil, mockInvoker)

	// With maxRetries=0, the loop doesn't execute, so no error is returned and no calls are made
	assert.NoError(t, err)
	assert.Equal(t, 0, callCount, "Should not call invoker when maxRetries is 0")
}

func TestRetryInterceptorDifferentErrorCodes(t *testing.T) {
	tests := []struct {
		name        string
		errorCode   codes.Code
		shouldRetry bool
	}{
		{"Unavailable should retry", codes.Unavailable, true},
		{"DeadlineExceeded should retry", codes.DeadlineExceeded, true},
		{"InvalidArgument should not retry", codes.InvalidArgument, false},
		{"NotFound should not retry", codes.NotFound, false},
		{"PermissionDenied should not retry", codes.PermissionDenied, false},
		{"Internal should not retry", codes.Internal, false},
		{"Canceled should not retry", codes.Canceled, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interceptor := retryInterceptor(2, 1*time.Millisecond)

			callCount := 0
			expectedErr := status.Error(tt.errorCode, "test error")
			mockInvoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				callCount++
				return expectedErr
			}

			ctx := context.Background()
			err := interceptor(ctx, "/test.service/TestMethod", "request", "reply", nil, mockInvoker)

			assert.Error(t, err)
			assert.Equal(t, expectedErr, err)

			expectedCalls := 1
			if tt.shouldRetry {
				expectedCalls = 2 // Will retry once
			}
			assert.Equal(t, expectedCalls, callCount)
		})
	}
}

// GetGRPCClient Tests

func TestGetGRPCClientEmptyAddress(t *testing.T) {
	ctx := context.Background()
	connectionOptions := &ConnectionOptions{}
	testSettings := createTestSettings(false, false, 0)

	conn, err := GetGRPCClient(ctx, "", connectionOptions, testSettings)

	assert.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "address is required")
}

func TestGetGRPCClientDefaultMaxMessageSize(t *testing.T) {
	ctx := context.Background()
	connectionOptions := &ConnectionOptions{
		// MaxMessageSize not set, should default to oneGigabyte
	}
	testSettings := createTestSettings(false, false, 0)

	// The function exits early on empty address, so MaxMessageSize won't be set
	conn, err := GetGRPCClient(ctx, "", connectionOptions, testSettings)

	assert.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "address is required")

	// MaxMessageSize remains 0 because function exits early
	assert.Equal(t, 0, connectionOptions.MaxMessageSize)
}

func TestGetGRPCClientSecurityLevelFromSettings(t *testing.T) {
	ctx := context.Background()
	connectionOptions := &ConnectionOptions{
		SecurityLevel: 0, // Should be overridden by settings
	}
	testSettings := createTestSettings(false, false, 2) // Set security level to 2

	// The function exits early on empty address, so SecurityLevel won't be set
	conn, err := GetGRPCClient(ctx, "", connectionOptions, testSettings)

	assert.Error(t, err) // Will fail on empty address
	assert.Nil(t, conn)
	assert.Equal(t, 0, connectionOptions.SecurityLevel) // Remains 0 because function exits early
}

func TestGetGRPCClientSecurityLevelNotOverridden(t *testing.T) {
	ctx := context.Background()
	connectionOptions := &ConnectionOptions{
		SecurityLevel: 1, // Non-zero, should not be overridden
	}
	testSettings := createTestSettings(false, false, 2)

	conn, err := GetGRPCClient(ctx, "", connectionOptions, testSettings)

	assert.Error(t, err) // Will fail on empty address
	assert.Nil(t, conn)
	assert.Equal(t, 1, connectionOptions.SecurityLevel) // Should remain unchanged
}

func TestGetGRPCClientCustomMaxMessageSize(t *testing.T) {
	ctx := context.Background()
	customSize := 2 * 1024 * 1024 // 2MB
	connectionOptions := &ConnectionOptions{
		MaxMessageSize: customSize,
	}
	testSettings := createTestSettings(false, false, 0)

	conn, err := GetGRPCClient(ctx, "", connectionOptions, testSettings)

	assert.Error(t, err) // Will fail on empty address
	assert.Nil(t, conn)
	assert.Equal(t, customSize, connectionOptions.MaxMessageSize) // Should remain custom size
}

// Note: Testing the full GetGRPCClient function with actual connections would require
// a running gRPC server or extensive mocking. The tests above focus on the input
// validation and configuration logic that can be tested without network connections.

// For integration testing of the full function, we would need:
// 1. A test gRPC server
// 2. Valid certificates for TLS testing
// 3. Mocking of the gRPC client creation
//
// These tests verify the core logic and error handling that doesn't require
// an actual network connection.

// getGRPCServer Tests

func TestGetGRPCServerDefaultMaxMessageSize(t *testing.T) {
	connectionOptions := &ConnectionOptions{
		// MaxMessageSize not set, should default to oneGigabyte
		SecurityLevel: 0, // Use insecure for simplicity
	}
	testSettings := createTestSettings(false, false, 0)

	server, err := getGRPCServer(connectionOptions, []grpc.ServerOption{}, testSettings)

	assert.NoError(t, err)
	assert.NotNil(t, server)
	assert.Equal(t, oneGigabyte, connectionOptions.MaxMessageSize)

	// Clean up
	server.Stop()
}

func TestGetGRPCServerCustomMaxMessageSize(t *testing.T) {
	customSize := 5 * 1024 * 1024 // 5MB
	connectionOptions := &ConnectionOptions{
		MaxMessageSize: customSize,
		SecurityLevel:  0, // Use insecure
	}
	testSettings := createTestSettings(false, false, 0)

	server, err := getGRPCServer(connectionOptions, []grpc.ServerOption{}, testSettings)

	assert.NoError(t, err)
	assert.NotNil(t, server)
	assert.Equal(t, customSize, connectionOptions.MaxMessageSize)

	server.Stop()
}

func TestGetGRPCServerWithMaxConnectionAge(t *testing.T) {
	connectionOptions := &ConnectionOptions{
		SecurityLevel:    0,
		MaxConnectionAge: 30 * time.Second,
	}
	testSettings := createTestSettings(false, false, 0)

	server, err := getGRPCServer(connectionOptions, []grpc.ServerOption{}, testSettings)

	assert.NoError(t, err)
	assert.NotNil(t, server)

	server.Stop()
}

func TestGetGRPCServerWithTracingEnabled(t *testing.T) {
	connectionOptions := &ConnectionOptions{
		SecurityLevel: 0,
	}
	testSettings := createTestSettings(true, false, 0) // Enable tracing

	server, err := getGRPCServer(connectionOptions, []grpc.ServerOption{}, testSettings)

	assert.NoError(t, err)
	assert.NotNil(t, server)

	server.Stop()
}

func TestGetGRPCServerWithPrometheusMetrics(t *testing.T) {
	connectionOptions := &ConnectionOptions{
		SecurityLevel: 0,
	}
	testSettings := createTestSettings(false, true, 0) // Enable Prometheus metrics

	server, err := getGRPCServer(connectionOptions, []grpc.ServerOption{}, testSettings)

	assert.NoError(t, err)
	assert.NotNil(t, server)

	server.Stop()
}

func TestGetGRPCServerWithTracingAndMetrics(t *testing.T) {
	connectionOptions := &ConnectionOptions{
		SecurityLevel: 0,
	}
	testSettings := createTestSettings(true, true, 0) // Enable both

	server, err := getGRPCServer(connectionOptions, []grpc.ServerOption{}, testSettings)

	assert.NoError(t, err)
	assert.NotNil(t, server)

	server.Stop()
}

func TestGetGRPCServerWithTLSSecurityLevel1(t *testing.T) {
	certFile, keyFile, _, cleanup := createTempCertificates(t)
	defer cleanup()

	connectionOptions := &ConnectionOptions{
		SecurityLevel: 1,
		CertFile:      certFile,
		KeyFile:       keyFile,
	}
	testSettings := createTestSettings(false, false, 0)

	server, err := getGRPCServer(connectionOptions, []grpc.ServerOption{}, testSettings)

	assert.NoError(t, err)
	assert.NotNil(t, server)

	server.Stop()
}

func TestGetGRPCServerWithTLSError(t *testing.T) {
	connectionOptions := &ConnectionOptions{
		SecurityLevel: 1,
		CertFile:      "nonexistent.pem",
		KeyFile:       "nonexistent.key",
	}
	testSettings := createTestSettings(false, false, 0)

	server, err := getGRPCServer(connectionOptions, []grpc.ServerOption{}, testSettings)

	assert.Error(t, err)
	assert.Nil(t, server)
	assert.Contains(t, err.Error(), "failed to read key pair")
}

func TestGetGRPCServerWithAdditionalOptions(t *testing.T) {
	connectionOptions := &ConnectionOptions{
		SecurityLevel: 0,
	}
	testSettings := createTestSettings(false, false, 0)

	// Add some additional server options
	additionalOpts := []grpc.ServerOption{
		grpc.ConnectionTimeout(5 * time.Second),
	}

	server, err := getGRPCServer(connectionOptions, additionalOpts, testSettings)

	assert.NoError(t, err)
	assert.NotNil(t, server)

	server.Stop()
}

// RegisterPrometheusMetrics and InitGRPCResolver Tests

func TestRegisterPrometheusMetrics(t *testing.T) {
	// Reset the once to ensure we can test it
	resetPrometheusOnce()

	// This function uses sync.Once, so we can't easily test multiple registrations,
	// but we can test that it doesn't panic and completes successfully
	assert.NotPanics(t, func() {
		RegisterPrometheusMetrics()
	})

	// Calling it again should not panic (sync.Once should prevent duplicate registration)
	assert.NotPanics(t, func() {
		RegisterPrometheusMetrics()
	})
}

func TestInitGRPCResolverDefaultResolver(t *testing.T) {
	logger := mockLogger()

	// Test default resolver (empty string or "default")
	assert.NotPanics(t, func() {
		InitGRPCResolver(logger, "")
	})

	assert.NotPanics(t, func() {
		InitGRPCResolver(logger, "default")
	})

	assert.NotPanics(t, func() {
		InitGRPCResolver(logger, "unknown")
	})
}

func TestInitGRPCResolverK8sResolver(t *testing.T) {
	logger := mockLogger()

	// Test k8s resolver
	assert.NotPanics(t, func() {
		InitGRPCResolver(logger, "k8s")
	})
}

func TestInitGRPCResolverKubernetesResolver(t *testing.T) {
	logger := mockLogger()

	// Test kubernetes resolver
	assert.NotPanics(t, func() {
		InitGRPCResolver(logger, "kubernetes")
	})
}

func TestInitGRPCResolverAllResolverTypes(t *testing.T) {
	logger := mockLogger()

	resolverTypes := []string{"", "default", "k8s", "kubernetes", "unknown"}

	for _, resolverType := range resolverTypes {
		t.Run("resolver_type_"+resolverType, func(t *testing.T) {
			assert.NotPanics(t, func() {
				InitGRPCResolver(logger, resolverType)
			})
		})
	}
}

// Integration test that combines multiple components
func TestGRPCHelperIntegration(t *testing.T) {
	// Test that we can use PasswordCredentials with other components
	creds := NewPassCredentials(map[string]string{
		"api-key": "test-key",
	})

	connectionOptions := &ConnectionOptions{
		SecurityLevel: 0,
		Credentials:   creds,
		APIKey:        "test-api",
		MaxRetries:    2,
		RetryBackoff:  10 * time.Millisecond,
	}

	testSettings := createTestSettings(false, false, 0)

	// Test that connection options can be used with TLS credential loading
	tlsCreds, err := loadTLSCredentials(connectionOptions, false)
	assert.NoError(t, err)
	assert.NotNil(t, tlsCreds)

	// Test that retry interceptor can be created
	retryInt := retryInterceptor(connectionOptions.MaxRetries, connectionOptions.RetryBackoff)
	assert.NotNil(t, retryInt)

	// Test that server can be created with these options
	server, err := getGRPCServer(connectionOptions, []grpc.ServerOption{}, testSettings)
	assert.NoError(t, err)
	assert.NotNil(t, server)
	server.Stop()
}
