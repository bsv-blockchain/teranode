// Package null implements a null blob storage backend for the blob.Store interface.
//
// The null blob store is a special implementation that acts as a no-op or blackhole
// storage backend. It accepts all write operations but doesn't actually store any data,
// and all read operations return "not found" errors. This implementation is useful for:
//   - Testing scenarios where actual storage is not needed
//   - Performance benchmarking without storage overhead
//   - Configurations where certain blob types should be intentionally discarded
//   - Development environments where storage persistence is not required
//
// The null store implements the complete blob.Store interface but with minimal
// resource usage and no actual storage operations. All write operations succeed
// immediately, and all read operations fail with appropriate errors.
package null

import (
	"context"
	"io"
	"net/http"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// Null implements the blob.Store interface as a no-op or blackhole storage backend.
// It accepts all write operations but doesn't actually store any data, and all read
// operations return "not found" errors. This implementation is useful for testing,
// benchmarking, and development environments where actual storage is not needed.
//
// The Null store maintains minimal state (just a logger and options) and performs
// no actual I/O operations, making it extremely lightweight and fast. It can be used
// as a drop-in replacement for other blob store implementations when storage
// persistence is not required.
type Null struct {
	// logger provides structured logging for store operations
	logger ulogger.Logger
	// options contains configuration options for the store
	options *options.Options
}

// New creates a new null blob store instance.
//
// The null store is a special implementation that acts as a no-op or blackhole
// storage backend. It accepts all write operations but doesn't actually store any data,
// and all read operations return "not found" errors.
//
// Parameters:
//   - logger: Logger instance for store operations
//   - opts: Optional store configuration options
//
// Returns:
//   - *Null: The configured null store instance
//   - error: Always returns nil as creation cannot fail
func New(logger ulogger.Logger, opts ...options.StoreOption) (*Null, error) {
	logger = logger.New("null")

	return &Null{
		options: options.NewStoreOptions(opts...),
		logger:  logger,
	}, nil
}

// Health checks the health status of the null blob store.
// Since the null store has no actual storage backend, it always reports as healthy.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - checkLiveness: Whether to perform a more thorough liveness check (unused in this implementation)
//
// Returns:
//   - int: Always returns http.StatusOK (200)
//   - string: Always returns "Null Store"
//   - error: Always returns nil
func (n *Null) Health(_ context.Context, _ bool) (int, string, error) {
	return http.StatusOK, "Null Store", nil
}

// Close performs any necessary cleanup for the null store.
// Since the null store maintains no resources, this is a no-op.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//
// Returns:
//   - error: Always returns nil
func (n *Null) Close(_ context.Context) error {
	return nil
}

// SetFromReader simulates storing a blob from a streaming reader.
// In the null store implementation, this is a no-op that discards all data.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - key: The key identifying the blob (unused in this implementation)
//   - fileType: The type of the file (unused in this implementation)
//   - reader: Reader providing the blob data (unused and not read)
//   - opts: Optional file options (unused in this implementation)
//
// Returns:
//   - error: Always returns nil, indicating success
func (n *Null) SetFromReader(_ context.Context, _ []byte, _ fileformat.FileType, _ io.ReadCloser, _ ...options.FileOption) error {
	return nil
}

// Set simulates storing a blob.
// In the null store implementation, this is a no-op that discards all data.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - key: The key identifying the blob (unused in this implementation)
//   - fileType: The type of the file (unused in this implementation)
//   - value: The blob data to store (unused and discarded)
//   - opts: Optional file options (unused in this implementation)
//
// Returns:
//   - error: Always returns nil, indicating success
func (n *Null) Set(_ context.Context, _ []byte, _ fileformat.FileType, _ []byte, _ ...options.FileOption) error {
	return nil
}

// SetDAH simulates setting the Delete-At-Height (DAH) value for a blob.
// In the null store implementation, this is a no-op.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - key: The key identifying the blob (unused in this implementation)
//   - fileType: The type of the file (unused in this implementation)
//   - dah: The delete at height value (unused in this implementation)
//   - opts: Optional file options (unused in this implementation)
//
// Returns:
//   - error: Always returns nil, indicating success
func (n *Null) SetDAH(_ context.Context, _ []byte, _ fileformat.FileType, _ uint32, opts ...options.FileOption) error {
	return nil
}

// GetDAH simulates retrieving the Delete-At-Height (DAH) value for a blob.
// In the null store implementation, this always returns 0 as no blobs are stored.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - key: The key identifying the blob (unused in this implementation)
//   - fileType: The type of the file (unused in this implementation)
//   - opts: Optional file options (unused in this implementation)
//
// Returns:
//   - uint32: Always returns 0
//   - error: Always returns nil
func (n *Null) GetDAH(_ context.Context, _ []byte, _ fileformat.FileType, opts ...options.FileOption) (uint32, error) {
	return 0, nil
}

// GetIoReader simulates retrieving a blob as a streaming reader.
// In the null store implementation, this always returns a "not found" error
// as no blobs are actually stored.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - opts: Optional file options
//
// Returns:
//   - io.ReadCloser: Always returns nil
//   - error: Always returns a "not found" error
func (n *Null) GetIoReader(_ context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	merged := options.MergeOptions(n.options, opts)

	fileName, err := merged.ConstructFilename("", key, fileType)
	if err != nil {
		return nil, err
	}

	if n != nil && n.logger != nil {
		n.logger.Debugf("[Null][GetIoReader] blob not found filename=%s type=%s", fileName, fileType)
	}

	return nil, errors.ErrNotFound
}

// Get simulates retrieving a blob.
// In the null store implementation, this always returns a "not found" error
// as no blobs are actually stored. This method delegates to GetIoReader.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - opts: Optional file options
//
// Returns:
//   - []byte: Always returns nil
//   - error: Always returns a "not found" error
func (n *Null) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	_, err := n.GetIoReader(ctx, key, fileType, opts...)
	return nil, err
}

// Exists checks if a blob exists in the store.
// In the null store implementation, this always returns false as no blobs are stored.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - key: The key identifying the blob (unused in this implementation)
//   - fileType: The type of the file (unused in this implementation)
//   - opts: Optional file options (unused in this implementation)
//
// Returns:
//   - bool: Always returns false
//   - error: Always returns nil
func (n *Null) Exists(_ context.Context, _ []byte, _ fileformat.FileType, opts ...options.FileOption) (bool, error) {
	return false, nil
}

// Del simulates deleting a blob from the store.
// In the null store implementation, this is a no-op as no blobs are stored.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - key: The key identifying the blob (unused in this implementation)
//   - fileType: The type of the file (unused in this implementation)
//   - opts: Optional file options (unused in this implementation)
//
// Returns:
//   - error: Always returns nil, indicating success
func (n *Null) Del(_ context.Context, _ []byte, _ fileformat.FileType, opts ...options.FileOption) error {
	return nil
}

// SetCurrentBlockHeight updates the current block height in the store.
// In the null store implementation, this is a no-op as no DAH tracking is performed.
//
// Parameters:
//   - height: The current blockchain height (unused in this implementation)
func (n *Null) SetCurrentBlockHeight(height uint32) {
	// noop
}
