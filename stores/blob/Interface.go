// Package blob provides blob storage functionality with various storage backend implementations.
// It offers a unified interface for storing, retrieving, and managing binary large objects (blobs)
// across different storage backends including memory, file system, S3, and HTTP.
//
// The package is designed to support blockchain data storage requirements with features like:
// - Delete-At-Height (DAH) functionality for automatic blockchain-based data expiration
// - Concurrent access patterns for high-throughput environments
// - Batching capabilities for efficient bulk operations
// - Streaming data access through io.Reader interfaces
// - Range-based content retrieval for partial data access
// - HTTP API for remote blob store interaction
//
// The architecture follows a modular design with a core interface (Store) that can be
// implemented by various backends and enhanced with wrapper implementations that add
// functionality like concurrency control, batching, and DAH management.
package blob

import (
	"context"
	"io"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
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
// - logger: Debug logging for blob operations
//
// The Store interface is designed to be composable, allowing implementations to be
// stacked to provide combined functionality. For example, a file-based store can be
// wrapped with a batcher for improved write performance, then wrapped with a logger
// for debugging blob operations.
//
// All methods accept a context.Context parameter to support cancellation and timeouts,
// which is particularly important for operations that may involve network or disk I/O.
// Most methods also accept optional FileOption parameters that can modify the behavior
// of the operation, such as specifying a Delete-At-Height value or controlling whether
// existing blobs can be overwritten.
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
	//   - fileType: The type of the file
	//   - opts: Optional file options
	// Returns:
	//   - bool: True if the blob exists, false otherwise
	//   - error: Any error that occurred during the check
	Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error)

	// Get retrieves a blob from the store.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - opts: Optional file options
	// Returns:
	//   - []byte: The blob data
	//   - error: Any error that occurred during retrieval
	Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error)

	// GetIoReader returns an io.ReadCloser for streaming blob data.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - opts: Optional file options
	// Returns:
	//   - io.ReadCloser: Reader for streaming the blob data
	//   - error: Any error that occurred during setup
	GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error)

	// Set stores a blob in the store.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - fileType: The type of the file
	//   - value: The blob data to store
	//   - opts: Optional file options
	// Returns:
	//   - error: Any error that occurred during storage
	Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error

	// SetFromReader stores a blob from an io.ReadCloser.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - fileType: The type of the file
	//   - reader: Reader providing the blob data
	//   - opts: Optional file options
	// Returns:
	//   - error: Any error that occurred during storage
	SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error

	// SetDAH sets the delete at height for a blob.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob
	//   - fileType: The type of the file
	//   - dah: The delete at height
	//   - opts: Optional file options
	// Returns:
	//   - error: Any error that occurred during DAH setting
	SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error

	// Del deletes a blob from the store.
	// Parameters:
	//   - ctx: The context for the operation
	//   - key: The key identifying the blob to delete
	//   - fileType: The type of the file
	//   - opts: Optional file options
	// Returns:
	//   - error: Any error that occurred during deletion

	Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error

	// Close closes the blob store and releases any resources.
	// Parameters:
	//   - ctx: The context for the operation
	// Returns:
	//   - error: Any error that occurred during closure
	Close(ctx context.Context) error

	// SetCurrentBlockHeight sets the current block height for the store.
	// Parameters:
	//   - height: The current block height
	SetCurrentBlockHeight(height uint32)
}
