// Package blob provides blob storage functionality with various storage backend implementations.
package blob

import (
	"context"
	"io"
	"time"

	"github.com/bitcoin-sv/teranode/stores/blob/options"
)

// Store defines the interface for blob storage operations. It provides methods for
// storing, retrieving, and managing blob data with support for TTL and various options.
type Store interface {
	// Health checks the health status of the blob store.
	// Parameters:
	//   - ctx: The context for the operation
	//   - checkLiveness: Whether to perform a liveness check
	// Returns:
	//   - int: HTTP status code indicating health status
	//   - string: Description of the health status
	//   - error: Any error that occurred during the health check
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// Exists checks if a blob exists in the store.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - opts: Optional file options
	// Returns:
	//   - bool: True if the blob exists, false otherwise
	//   - error: Any error that occurred during the check
	Exists(ctx context.Context, key []byte, opts ...options.FileOption) (bool, error)

	// Get retrieves a blob from the store.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - opts: Optional file options
	// Returns:
	//   - []byte: The blob data
	//   - error: Any error that occurred during retrieval
	Get(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)

	// GetHead retrieves the first n bytes of a blob.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - nrOfBytes: Number of bytes to retrieve from the start
	//   - opts: Optional file options
	// Returns:
	//   - []byte: The requested portion of the blob
	//   - error: Any error that occurred during retrieval
	GetHead(ctx context.Context, key []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error)

	// GetIoReader returns an io.ReadCloser for streaming blob data.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - opts: Optional file options
	// Returns:
	//   - io.ReadCloser: Reader for streaming the blob data
	//   - error: Any error that occurred during setup
	GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error)

	// Set stores a blob in the store.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - value: The blob data to store
	//   - opts: Optional file options
	// Returns:
	//   - error: Any error that occurred during storage
	Set(ctx context.Context, key []byte, value []byte, opts ...options.FileOption) error

	// SetFromReader stores a blob from an io.ReadCloser.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - value: Reader providing the blob data
	//   - opts: Optional file options
	// Returns:
	//   - error: Any error that occurred during storage
	SetFromReader(ctx context.Context, key []byte, value io.ReadCloser, opts ...options.FileOption) error

	// SetTTL sets the time-to-live for a blob.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - ttl: The time-to-live duration
	//   - opts: Optional file options
	// Returns:
	//   - error: Any error that occurred during TTL setting
	SetTTL(ctx context.Context, key []byte, ttl time.Duration, opts ...options.FileOption) error

	// GetTTL retrieves the remaining time-to-live for a blob.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - opts: Optional file options
	// Returns:
	//   - time.Duration: The remaining TTL
	//   - error: Any error that occurred during retrieval
	GetTTL(ctx context.Context, key []byte, opts ...options.FileOption) (time.Duration, error)

	// Del deletes a blob from the store.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob to delete
	//   - opts: Optional file options
	// Returns:
	//   - error: Any error that occurred during deletion
	Del(ctx context.Context, key []byte, opts ...options.FileOption) error
	GetHeader(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)
	GetFooterMetaData(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)

	// Close closes the blob store and releases any resources.
	// Parameters:
	//   - ctx: The context for the operation
	// Returns:
	//   - error: Any error that occurred during closure
	Close(ctx context.Context) error
}
