// Package httpimpl provides HTTP handlers for blockchain data retrieval and processing,
// including standardized error handling support.
package httpimpl

import (
	"net/http"
	"strings"

	tnerrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/labstack/echo/v4"
)

// errorResponse defines the standard error response structure used across all API endpoints.
// It provides a consistent format that includes both HTTP and application-level error details.
type errorResponse struct {
	// Status contains the HTTP status code
	// Required: true
	// Example: 400
	Status int32 `json:"status"`

	// Code contains the application-specific error code
	// Required: true
	// Example: 1001
	Code int32 `json:"code"`

	// Err contains the human-readable error message
	// Required: true
	// Example: "invalid block hash format"
	Err string `json:"error"`
}

// sendError standardizes error responses across all API endpoints by creating
// and sending a properly formatted error response with appropriate status codes
// and internal error codes.
//
// Parameters:
//   - c: Echo context containing response writer
//   - status: HTTP status code to return (e.g., 400, 404, 500)
//   - code: Internal application error code for more specific error identification
//   - err: Original error containing message to be returned
//
// Returns:
//   - error: Any error encountered while sending the response
//
// HTTP Status Code Handling:
//   - Preserves provided status code by default
//   - Automatically converts internal server errors (500) to bad requests (400)
//     when they contain gRPC InvalidArgument errors
//
// Example Usage:
//
//	if err != nil {
//	    return sendError(c, http.StatusBadRequest, 1001, errors.New("invalid hash format"))
//	}
//
// Example Response:
//
//	Status: 400 Bad Request
//	Content-Type: application/json
//	Body:
//	  {
//	    "status": 400,
//	    "code": 1001,
//	    "error": "invalid hash format"
//	  }
//
// Notes:
//   - Always returns JSON format
//   - Error messages come directly from err.Error()
//   - Status in response body matches HTTP status code
//   - Supports Swagger documentation generation
func sendError(c echo.Context, status int, code int32, err error) error {
	if status == http.StatusInternalServerError && strings.Contains(err.Error(), "rpc error: code = InvalidArgument desc") {
		status = http.StatusBadRequest
	}

	e := &errorResponse{
		Status: int32(status), //nolint:gosec
		Code:   code,
		Err:    tnerrors.UserMessage(err),
	}

	return c.JSON(status, e)
}
