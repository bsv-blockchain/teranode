// Package errors provides utilities for categorizing and handling errors in the Teranode system.
package errors

import (
	"context"
	"errors"
	"strings"
)

// IsRetryableError determines if an error is transient and the operation should be retried.
// This includes network timeouts, temporary unavailability, and other transient conditions.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check if context was cancelled - not retryable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Check for specific error codes that are retryable
	var tErr *Error
	if As(err, &tErr) {
		switch tErr.Code() {
		case ERR_NETWORK_TIMEOUT,
			ERR_NETWORK_ERROR,
			ERR_SERVICE_UNAVAILABLE,
			ERR_STORAGE_UNAVAILABLE:
			return true
		case ERR_NETWORK_CONNECTION_REFUSED:
			// Connection refused might be retryable if the service is starting up
			return true
		case ERR_NETWORK_INVALID_RESPONSE,
			ERR_NETWORK_PEER_MALICIOUS:
			// These are not retryable - indicates a problem with the peer
			return false
		}
	}

	return false
}

// IsNetworkError determines if an error is network-related.
// This includes timeouts, connection failures, and invalid responses.
//
// Parameters:
//   - err: Error to check
//
// Returns:
//   - bool: true if error is network-related
func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific network error codes
	var tErr *Error
	if As(err, &tErr) {
		switch tErr.Code() {
		case ERR_NETWORK_ERROR,
			ERR_NETWORK_TIMEOUT,
			ERR_NETWORK_CONNECTION_REFUSED,
			ERR_NETWORK_INVALID_RESPONSE,
			ERR_NETWORK_PEER_MALICIOUS:
			return true
		}
	}

	// Check for common network error strings
	errStr := strings.ToLower(err.Error())
	networkStrings := []string{
		"network",
		"connection",
		"timeout",
		"dial tcp",
		"dial udp",
		"no such host",
		"connection refused",
		"connection reset",
		"broken pipe",
		"eof",
		"http",
		"https",
	}

	for _, s := range networkStrings {
		if strings.Contains(errStr, s) {
			return true
		}
	}

	return false
}

// IsMaliciousResponseError determines if an error indicates a potentially malicious peer.
// This includes invalid responses, protocol violations, and suspicious behavior.
//
// Parameters:
//   - err: Error to check
//
// Returns:
//   - bool: true if error indicates malicious behavior
func IsMaliciousResponseError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific malicious error codes
	var tErr *Error
	if As(err, &tErr) {
		switch tErr.Code() {
		case ERR_NETWORK_PEER_MALICIOUS,
			ERR_NETWORK_INVALID_RESPONSE:
			return true
		}
	}

	// Check for patterns that might indicate malicious behavior
	errStr := strings.ToLower(err.Error())
	maliciousStrings := []string{
		"invalid header",
		"malformed",
		"corrupt",
		"attack",
		"malicious",
		"invalid format",
		"protocol violation",
	}

	for _, s := range maliciousStrings {
		if strings.Contains(errStr, s) {
			return true
		}
	}

	return false
}

// IsTemporaryError determines if an error is temporary and might succeed if retried later.
// This is a subset of retryable errors that are explicitly temporary.
//
// Parameters:
//   - err: Error to check
//
// Returns:
//   - bool: true if error is temporary
func IsTemporaryError(err error) bool {
	if err == nil {
		return false
	}

	// Check if the error implements the temporary interface
	type temporary interface {
		Temporary() bool
	}

	if te, ok := err.(temporary); ok {
		return te.Temporary()
	}

	// Check for specific temporary error codes
	var tErr *Error
	if As(err, &tErr) {
		switch tErr.Code() {
		case ERR_SERVICE_UNAVAILABLE,
			ERR_STORAGE_UNAVAILABLE:
			return true
		}
	}

	return false
}

// IsContextError determines if an error is related to context cancellation or deadline.
//
// Parameters:
//   - err: Error to check
//
// Returns:
//   - bool: true if error is context-related
func IsContextError(err error) bool {
	if err == nil {
		return false
	}

	// Check standard context errors
	if err == context.Canceled || err == context.DeadlineExceeded {
		return true
	}

	// Check for wrapped context errors
	var tErr *Error
	if As(err, &tErr) {
		if tErr.Code() == ERR_CONTEXT_CANCELED || tErr.Code() == ERR_CONTEXT {
			return true
		}
	}

	// Check if the wrapped error is a context error
	if Is(err, context.Canceled) || Is(err, context.DeadlineExceeded) {
		return true
	}

	return false
}

// GetErrorCategory returns a string representing the category of the error.
// This is useful for logging and metrics.
//
// Parameters:
//   - err: Error to categorize
//
// Returns:
//   - string: Error category (e.g., "context", "network", "malicious", "temporary", "unknown")
func GetErrorCategory(err error) string {
	if err == nil {
		return "none"
	}

	if IsContextError(err) {
		return "context"
	}

	if IsMaliciousResponseError(err) {
		return "malicious"
	}

	if IsNetworkError(err) {
		return "network"
	}

	if IsTemporaryError(err) {
		return "temporary"
	}

	var tErr *Error
	if As(err, &tErr) {
		// Group by error code ranges
		code := tErr.Code()
		switch {
		case code >= 10 && code <= 19:
			return "block"
		case code >= 20 && code <= 29:
			return "subtree"
		case code >= 30 && code <= 49:
			return "transaction"
		case code >= 50 && code <= 59:
			return "service"
		case code >= 60 && code <= 69:
			return "storage"
		case code >= 70 && code <= 79:
			return "utxo"
		case code >= 80 && code <= 89:
			return "kafka"
		case code >= 90 && code <= 99:
			return "blob"
		case code >= 100 && code <= 109:
			return "state"
		case code >= 110 && code <= 119:
			return "network"
		}
	}

	return "unknown"
}
