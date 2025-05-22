// Package blob provides blob storage functionality with various storage backend implementations.
package blob

import (
	"context"
	"io"

	"github.com/bitcoin-sv/teranode/stores/blob/options"
)

// Store defines the interface for blob storage operations.
// It provides a unified API for storing, retrieving, and managing blob data across different
// backend implementations. The interface is designed to support the needs of blockchain
// applications with features such as Delete-At-Height (DAH) for automatic expiration,
// partial retrieval for efficient data access, and various configuration options.
//
// Implementations of this interface include:
// - memory: In-memory storage for ephemeral blobs (useful for testing)
// - file: Filesystem-based storage for persistent blobs
// - s3: Amazon S3-compatible storage for cloud-based scalability
// - http: HTTP client for accessing remote blob stores
// - null: No-op implementation for testing and development
//
// The Store interface can be extended with additional capabilities through wrappers:
// - batcher: Efficient batch processing of multiple operations
// - localdah: Delete-At-Height functionality for blockchain-based expiration
// - concurrent: Thread-safe access to the underlying store
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

	// SetDAH sets the delete at height for a blob.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - dah: The delete at height
	//   - opts: Optional file options
	// Returns:
	//   - error: Any error that occurred during DAH setting
	SetDAH(ctx context.Context, key []byte, dah uint32, opts ...options.FileOption) error

	// GetDAH retrieves the remaining time-to-live for a blob.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - opts: Optional file options
	// Returns:
	//   - uint32: The delete at height value
	//   - error: Any error that occurred during retrieval
	GetDAH(ctx context.Context, key []byte, opts ...options.FileOption) (uint32, error)

	// Del deletes a blob from the store.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob to delete
	//   - opts: Optional file options
	// Returns:
	//   - error: Any error that occurred during deletion
	Del(ctx context.Context, key []byte, opts ...options.FileOption) error
	// GetHeader retrieves the header portion of a blob.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - opts: Optional file options
	// Returns:
	//   - []byte: The header data
	//   - error: Any error that occurred during retrieval
	GetHeader(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)

	// GetFooterMetaData retrieves the footer metadata of a blob.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - opts: Optional file options
	// Returns:
	//   - []byte: The footer metadata
	//   - error: Any error that occurred during retrieval
	GetFooterMetaData(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)

	// Close closes the blob store and releases any resources.
	// Parameters:
	//   - ctx: The context for the operation
	// Returns:
	//   - error: Any error that occurred during closure
	Close(ctx context.Context) error
}
