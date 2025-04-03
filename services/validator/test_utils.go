package validator

import (
	"context"
	"reflect"

	"github.com/labstack/echo/v4"
)

// HTTPHandlers provides access to the server's HTTP handlers for testing
type HTTPHandlers struct {
	server *Server
}

// HTTP returns an instance of HTTPHandlers for accessing HTTP handlers
func (s *Server) HTTP() *HTTPHandlers {
	return &HTTPHandlers{server: s}
}

// HandleSingleTx returns the handler for the /tx endpoint
func (h *HTTPHandlers) HandleSingleTx(ctx context.Context) echo.HandlerFunc {
	return h.server.handleSingleTx(ctx)
}

// HandleMultipleTx returns the handler for the /txs endpoint
func (h *HTTPHandlers) HandleMultipleTx(ctx context.Context) echo.HandlerFunc {
	return h.server.handleMultipleTx(ctx)
}

// SetValidatorForTesting allows injecting a mock validator for testing
// This should only be used in tests
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
