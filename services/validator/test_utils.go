/*
Package validator implements Bitcoin SV transaction validation functionality.

This file provides testing utilities for the validator service, including
HTTP handler access and mock injection capabilities for comprehensive testing
of validator components and integration scenarios.
*/
package validator

import (
	"context"
	"reflect"

	"github.com/labstack/echo/v4"
)

// HTTPHandlers provides access to the server's HTTP handlers for testing purposes.
// This struct allows tests to directly access and invoke HTTP handlers without
// starting a full HTTP server, enabling unit testing of HTTP endpoint logic.
type HTTPHandlers struct {
	server *Server
}

// HTTP returns an instance of HTTPHandlers for accessing HTTP handlers in tests.
// This method provides a testing interface to access the server's HTTP handlers
// without requiring a full HTTP server setup.
//
// Returns:
//   - *HTTPHandlers: Handler accessor for testing HTTP endpoints
func (v *Server) HTTP() *HTTPHandlers {
	return &HTTPHandlers{server: v}
}

func (h *HTTPHandlers) HandleSingleTx(ctx context.Context) echo.HandlerFunc {
	return h.server.handleSingleTx(ctx)
}

func (h *HTTPHandlers) HandleMultipleTx(ctx context.Context) echo.HandlerFunc {
	return h.server.handleMultipleTx(ctx)
}

// SetValidatorForTesting allows injecting a mock validator for testing purposes.
// This function uses reflection to replace the server's private validator field
// with a mock implementation, enabling controlled testing scenarios.
//
// WARNING: This function should only be used in test code as it bypasses
// normal encapsulation and could cause undefined behavior in production.
//
// Parameters:
//   - server: The validator server instance to modify
//   - mockValidator: The mock validator implementation to inject
func SetValidatorForTesting(server *Server, mockValidator Interface) {
	// Use reflection to set the private validator field
	v := reflect.ValueOf(server).Elem()
	field := v.FieldByName("validator")

	// Get a reflect.Value of the mock validator
	mockVal := reflect.ValueOf(mockValidator)

	// Check if we can set the field
	if field.CanSet() {
		field.Set(mockVal)
	}
}
