// Package httpimpl provides HTTP handlers for blockchain data retrieval and analysis.
package httpimpl

// ReadMode defines the format in which data will be returned from the API endpoints.
// It allows clients to request data in different representations while maintaining
// consistent internal processing.
type ReadMode int

const (
	// BINARY_STREAM represents raw binary data output.
	// Returns data as application/octet-stream with unmodified bytes.
	BINARY_STREAM ReadMode = iota

	// HEX represents hexadecimal string output.
	// Returns data as text/plain with bytes encoded as hexadecimal string.
	HEX

	// JSON represents structured JSON output.
	// Returns data as application/json with pretty-printing.
	JSON
)

// String returns a human-readable representation of the ReadMode.
// This is used for logging and error messages.
//
// Returns:
//   - "BINARY": For BINARY_STREAM mode
//   - "HEX": For HEX mode
//   - "JSON": For JSON mode
//   - "UNKNOWN": For any undefined mode
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
