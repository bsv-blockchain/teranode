// Package httpimpl provides comprehensive HTTP handlers and server implementations for blockchain data retrieval and analysis.
// It serves as the presentation layer for the asset service, exposing blockchain data through a RESTful API.
//
// Key features and capabilities:
// - Multiple response formats (binary, hex, JSON) for versatile data access
// - Comprehensive API endpoints for blocks, transactions, subtrees, and UTXOs
// - Support for both standard and legacy blockchain data formats
// - Built-in health checks and monitoring endpoints
// - Robust error handling and content negotiation
//
// The package uses the Echo web framework for routing and middleware management, providing
// a modern, performant HTTP interface. It translates between external representations and
// internal data models, ensuring consistent behavior across different endpoint patterns.
package httpimpl

// ReadMode defines the format in which data will be returned from the API endpoints.
// It allows clients to request data in different representations while maintaining
// consistent internal processing. This type-safe enumeration implements a control pattern
// for negotiating content format between clients and the server.
//
// The ReadMode influences both the Content-Type header and the serialization method
// used for the response, enabling clients to receive data in their preferred format
// while the server maintains consistent internal processing and storage models.
//
// This approach adheres to RESTful design principles of content negotiation,
// allowing the same resource to be represented in different formats based on
// client requirements without duplicating server-side business logic.
type ReadMode int

const (
	// BINARY_STREAM represents raw binary data output.
	// Returns data as application/octet-stream with unmodified bytes.
	// This mode is optimal for efficient data transfer, especially for large objects
	// like blocks and transactions, where parsing overhead should be minimized.
	BINARY_STREAM ReadMode = iota

	// HEX represents hexadecimal string output.
	// Returns data as text/plain with bytes encoded as hexadecimal string.
	// This mode provides a human-readable representation of binary data while
	// preserving the exact byte sequence, useful for debugging and verification.
	HEX

	// JSON represents structured JSON output.
	// Returns data as application/json with pretty-printing.
	// This mode provides the most accessible format for web applications and
	// interactive exploration, with proper field naming and nested structure.
	JSON
)

// String returns a human-readable representation of the ReadMode.
// This is used for logging, error messages, and debugging purposes.
//
// The string representation provides a clear indication of the active content format
// in log entries and diagnostic messages, making it easier to troubleshoot API usage
// patterns and content negotiation issues. The method ensures that even invalid or
// uninitialized ReadMode values have a meaningful string representation.
//
// Returns:
//   - "BINARY": For BINARY_STREAM mode, indicating raw binary data format
//   - "HEX": For HEX mode, indicating hexadecimal string encoding
//   - "JSON": For JSON mode, indicating structured JSON formatting
//   - "UNKNOWN": For any undefined mode, providing safety for error detection
func (r ReadMode) String() string {
	switch r {
	case BINARY_STREAM:
		return "BINARY"
	case HEX:
		return "HEX"
	case JSON:
		return "JSON"
	default:
		return "UNKNOWN"
	}
}
