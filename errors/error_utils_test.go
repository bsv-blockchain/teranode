package errors

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "network timeout error",
			err:      NewNetworkTimeoutError("request timed out"),
			expected: true,
		},
		{
			name:     "network error",
			err:      NewNetworkError("network unreachable"),
			expected: true,
		},
		{
			name:     "service unavailable",
			err:      NewServiceUnavailableError("service down"),
			expected: true,
		},
		{
			name:     "connection refused",
			err:      NewNetworkConnectionRefusedError("connection refused"),
			expected: true,
		},
		{
			name:     "malicious peer - not retryable",
			err:      NewNetworkPeerMaliciousError("invalid block header"),
			expected: false,
		},
		{
			name:     "invalid response - not retryable",
			err:      NewNetworkInvalidResponseError("malformed response"),
			expected: false,
		},
		{
			name:     "context canceled - not retryable",
			err:      context.Canceled,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetryableError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsNetworkError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "network timeout",
			err:      NewNetworkTimeoutError("timeout"),
			expected: true,
		},
		{
			name:     "network connection refused",
			err:      NewNetworkConnectionRefusedError("refused"),
			expected: true,
		},
		{
			name:     "network invalid response",
			err:      NewNetworkInvalidResponseError("invalid"),
			expected: true,
		},
		{
			name:     "network peer malicious",
			err:      NewNetworkPeerMaliciousError("malicious"),
			expected: true,
		},
		{
			name:     "generic network error",
			err:      NewNetworkError("network error"),
			expected: true,
		},
		{
			name:     "dial tcp error string",
			err:      fmt.Errorf("dial tcp 127.0.0.1:8080: connection refused"),
			expected: true,
		},
		{
			name:     "http error string",
			err:      fmt.Errorf("http request failed"),
			expected: true,
		},
		{
			name:     "non-network error",
			err:      NewProcessingError("processing failed"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsNetworkError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsMaliciousResponseError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "peer malicious error",
			err:      NewNetworkPeerMaliciousError("malicious behavior detected"),
			expected: true,
		},
		{
			name:     "invalid response error",
			err:      NewNetworkInvalidResponseError("invalid protocol"),
			expected: true,
		},
		{
			name:     "malformed string",
			err:      fmt.Errorf("malformed packet received"),
			expected: true,
		},
		{
			name:     "corrupt data string",
			err:      fmt.Errorf("corrupt block header"),
			expected: true,
		},
		{
			name:     "protocol violation string",
			err:      fmt.Errorf("protocol violation detected"),
			expected: true,
		},
		{
			name:     "normal network error",
			err:      NewNetworkTimeoutError("timeout"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsMaliciousResponseError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsContextError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "context canceled",
			err:      context.Canceled,
			expected: true,
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: true,
		},
		{
			name:     "wrapped context canceled",
			err:      NewContextCanceledError("operation canceled"),
			expected: true,
		},
		{
			name:     "wrapped context canceled in service error",
			err:      NewServiceError("service call failed", context.Canceled),
			expected: true,
		},
		{
			name:     "non-context error",
			err:      NewNetworkError("network failed"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsContextError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetErrorCategory(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: "none",
		},
		{
			name:     "context error",
			err:      context.Canceled,
			expected: "context",
		},
		{
			name:     "malicious error",
			err:      NewNetworkPeerMaliciousError("bad peer"),
			expected: "malicious",
		},
		{
			name:     "network error",
			err:      NewNetworkTimeoutError("timeout"),
			expected: "network",
		},
		{
			name:     "block error",
			err:      NewBlockNotFoundError("block missing"),
			expected: "block",
		},
		{
			name:     "transaction error",
			err:      NewTxNotFoundError("tx missing"),
			expected: "transaction",
		},
		{
			name:     "service error",
			err:      NewServiceError("service failed"),
			expected: "service",
		},
		{
			name:     "unknown error",
			err:      fmt.Errorf("some random error"),
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetErrorCategory(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
