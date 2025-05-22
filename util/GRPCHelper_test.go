package util

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestCreateAuthInterceptor_MissingMetadata(t *testing.T) {
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

func TestCreateAuthInterceptor_MultipleAPIKeys(t *testing.T) {
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
