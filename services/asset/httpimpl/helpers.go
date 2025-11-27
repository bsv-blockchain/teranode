// Package httpimpl provides HTTP handlers for blockchain data retrieval and processing,
// including support for paginated responses.
package httpimpl

import (
	"net/http"
	"strconv"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/labstack/echo/v4"
)

// Pagination represents pagination metadata for API responses that return lists of items.
// It provides information about the current page and total available records.
type Pagination struct {
	// Offset indicates the starting position in the full dataset
	Offset int `json:"offset"`
	// Limit indicates the maximum number of items per page
	Limit int `json:"limit"`
	// TotalRecords indicates the total number of available records
	TotalRecords int `json:"totalRecords"`
}

// ExtendedResponse wraps API response data with pagination information.
// It's used for endpoints that return lists of items requiring pagination.
type ExtendedResponse struct {
	// Data contains the actual response payload
	Data interface{} `json:"data"`
	// Pagination contains the pagination metadata for the response
	Pagination Pagination `json:"pagination"`
}

// getLimitOffset extracts and validates pagination parameters from the request.
// It provides consistent pagination handling across API endpoints.
//
// Parameters:
//   - c: Echo context containing the HTTP request
//
// Returns:
//   - int: Offset value (default: 0)
//   - int: Limit value (default: 20, max: 100)
//   - error: Validation error if parameters are invalid
//
// Query Parameters:
//
//   - offset: Number of items to skip (optional)
//     Example: ?offset=40
//
//   - limit: Maximum number of items to return (optional)
//     Example: ?limit=50
//
// Error Responses:
//   - 400 Bad Request:
//   - Invalid offset parameter format
//   - Invalid limit parameter format
//
// Defaults and Constraints:
//   - Default offset: 0 (first item)
//   - Default limit: 20 items
//   - Maximum limit: 100 items
//
// Example Usage:
//
//	offset, limit, err := h.getLimitOffset(c)
//	if err != nil {
//	    return err
//	}
//	// Use offset and limit for data retrieval
func (h *HTTP) getLimitOffset(c echo.Context) (int, int, error) {
	var err error

	// get subtree information
	offset := 0
	offsetStr := c.QueryParam("offset")

	if offsetStr != "" {
		offset, err = strconv.Atoi(offsetStr)
		if err != nil {
			return 0, 0, echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid offset format", err).Error())
		}
	}

	// Validate offset is non-negative to prevent underflow issues
	if offset < 0 {
		return 0, 0, echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("offset must be non-negative").Error())
	}

	limit := 20
	limitStr := c.QueryParam("limit")

	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			return 0, 0, echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid limit format", err).Error())
		}
	}

	// Validate limit bounds to prevent allocation issues
	if limit < 1 {
		limit = 1
	}

	if limit > 100 {
		limit = 100
	}

	return offset, limit, nil
}
