// Package file provides a filesystem-based implementation of the blob.Store interface.
// Package file implements a file-based blob storage backend for the blob.Store interface.
//
// The file-based blob store provides persistent storage of blobs on the local filesystem,
// with support for advanced features such as:
//   - Delete-At-Height (DAH) for automatic cleanup of expired blobs
//   - Optional checksumming for data integrity verification
//   - Streaming read/write operations for memory-efficient handling of large blobs
//   - Fallback to alternative storage locations (persistent subdirectory, longterm store)
//   - Concurrent access management via semaphores
//   - Optional header/footer support for blob metadata
//
// The implementation organizes blobs in a directory structure based on the first few bytes
// of the blob key, which helps distribute files across the filesystem and improves lookup
// performance. It also supports integration with a longterm storage backend for archival
// purposes.
//
// This store is designed for production use in blockchain applications where reliability,
// performance, and automatic cleanup of expired data are important requirements.
package file

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/debugflags"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/semaphore"
)

const checksumExtension = ".sha256"

// File implements the blob.Store interface using the local filesystem for storage.
// It provides a robust, persistent blob storage solution with features like automatic
// cleanup of expired blobs, data integrity verification, and efficient handling of
// large blobs through streaming operations.
//
// The File store organizes blobs in a directory structure based on the first few bytes
// of the blob key to distribute files across the filesystem and improve lookup performance.
// It supports concurrent access through semaphores and can integrate with a longterm
// storage backend for archival purposes.
//
// Configuration options include:
//   - Root directory path for blob storage
//   - Optional persistent subdirectory for important blobs
//   - Checksumming for data integrity verification
//   - Header/footer support for blob metadata
//   - Delete-At-Height (DAH) for automatic cleanup of expired blobs
//   - Integration with longterm storage backend
type File struct {
	// path is the base directory path where blob files are stored
	path string
	// logger provides structured logging for file operations and errors
	logger ulogger.Logger
	// options contains default options for blob operations
	options *options.Options
	// currentBlockHeight tracks the current blockchain height for DAH processing
	currentBlockHeight atomic.Uint32
	// persistSubDir is an optional subdirectory for organization within the base path
	persistSubDir string
	// longtermClient is an optional secondary storage backend for hybrid storage models
	longtermClient longtermStore
	// prunerClient is used to schedule blob deletions with the pruner service
	prunerClient options.PrunerClient
	// storeType is the blob store type enum value for this store
	storeType int32
}

func (s *File) debugEnabled() bool {
	if !debugflags.FileEnabled() || s == nil || s.logger == nil {
		return false
	}

	return s.logger.LogLevel() <= int(gocore.DEBUG)
}

func (s *File) debugf(format string, args ...interface{}) {
	if !s.debugEnabled() {
		return
	}

	s.logger.Debugf(format, args...)
}

func formatKeyHex(key []byte) string {
	return utils.ReverseAndHexEncodeSlice(key)
}

// longtermStore defines the interface for a secondary storage backend that can be used
// in conjunction with the file store for a hybrid storage model. This allows blobs to be
// retrieved from a secondary location if not found in the primary file storage, enabling
// tiered storage architectures with different retention policies and access patterns.
type longtermStore interface {
	// Get retrieves a blob from the longterm store
	Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error)
	// GetIoReader provides streaming access to a blob in the longterm store
	GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error)
	// Exists checks if a blob exists in the longterm store
	Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error)
}

// semaphoreReadCloser wraps an io.ReadCloser and releases the read semaphore when closed.
// This ensures the read permit is held for the entire duration of the read operation,
// not just during file open, making read behavior consistent with write behavior where
// the write permit is held for the entire streaming write operation.
type semaphoreReadCloser struct {
	io.ReadCloser
}

func (r *semaphoreReadCloser) Close() error {
	err := r.ReadCloser.Close()
	// Only release the semaphore permit if the underlying close was successful.
	// This provides natural idempotency: subsequent calls will fail at the OS level
	// (file already closed) and won't release the permit again, avoiding the
	// possible overhead of using a sync.Once to ensure the permit is released exactly once.
	if err == nil {
		releaseReadPermit()
	}
	return err
}

// Semaphore configuration constants
const (
	defaultReadLimit  = 768 // 75% of original 1024 total
	defaultWriteLimit = 256 // 25% of original 1024 total
	MinSemaphoreLimit = 1
	MaxSemaphoreLimit = 256_000
)

// GLOBAL SEMAPHORE DESIGN - ULIMIT PROTECTION AND RACE CONDITION MITIGATION:
//
// These global semaphore variables control concurrency for ALL file store instances to
// protect against Linux ulimit (open file descriptor) exhaustion. Each file operation
// (os.Open, os.Create, os.Stat, etc.) consumes a file descriptor, and exceeding the
// system limit causes "too many open files" errors that can crash the node.
//
// This design choice has important implications:
//
// 1. ULIMIT PROTECTION:
//    The semaphores limit concurrent file operations to prevent exceeding the system's
//    open file descriptor limit (ulimit -n). Total capacity (readLimit + writeLimit)
//    should be well under the system limit to account for other file descriptors
//    (network sockets, log files, etc.). Default: 768 + 256 = 1024.
//    ALL file operations MUST be protected by semaphores, including those in background
//    goroutines (loadDAHs, cleanup), to ensure ulimit protection is comprehensive.
//
// 2. INITIALIZATION REQUIREMENT:
//    InitSemaphores() MUST be called in main() before ANY file store operations begin.
//    The function uses sync.Once to ensure one-time initialization. Calling it after
//    file operations have started may result in some operations using stale references
//    due to Go's memory model not guaranteeing atomicity of variable replacement.
//
// 3. RACE CONDITION RISK:
//    Replacing global variables (as InitSemaphores does) while other goroutines read
//    them creates a data race. This is mitigated by:
//    - Calling InitSemaphores exactly once during startup (sync.Once protection)
//    - Calling it before any file operations that use the semaphores
//    - Not testing the initialization function in the same suite as code using it
//
// 4. INTERNAL VARIANTS PATTERN:
//    To avoid nested semaphore acquisition (which reduces effective capacity and wastes
//    ulimit protection), helper functions have two variants:
//    - Public versions (e.g., readDAHFromFile): acquire semaphore for standalone/background use
//    - Internal versions (e.g., readDAHFromFile_internal): no semaphore, for use within
//      already-protected contexts (API operations like SetFromReader that hold semaphores)
//    This ensures every file operation is protected exactly once, maximizing ulimit protection.
//
// 5. SEPARATE READ/WRITE SEMAPHORES:
//    Using separate semaphores prevents deadlocks where write operations holding the
//    write semaphore are blocked waiting for pipe data, while the operations that
//    would provide that data (ProcessSubtree reads) cannot acquire read slots because
//    they're exhausted. The 768/256 split maintains the original 1024 total capacity
//    while providing independent resource pools for reads vs writes.

// readSemaphore is a global weighted semaphore for controlling concurrent read operations.
// It limits the total number of concurrent read file operations (Get, Exists, etc.) to
// prevent file descriptor exhaustion. Uses golang.org/x/sync/semaphore.Weighted
// for context-aware blocking with proper timeout support. Default: 768 slots.
var readSemaphore *semaphore.Weighted

// writeSemaphore is a global weighted semaphore for controlling concurrent write operations.
// It limits the total number of concurrent write file operations (Set, Del, etc.) to
// prevent file descriptor exhaustion. Separate from readSemaphore to prevent read/write
// deadlocks where writes block on pipe data while reads wait for semaphore slots.
// Default: 256 slots.
var writeSemaphore *semaphore.Weighted

// semaphoreInitOnce ensures InitSemaphores is called exactly once in production.
var semaphoreInitOnce sync.Once

func init() {
	// Initialize with default values. These will only be replaced by InitSemaphores
	// if it's called, and sync.Once ensures thread-safe one-time initialization.
	// Total capacity (768 + 256 = 1024) matches the original single semaphore limit
	// to maintain the same system performance characteristics.
	readSemaphore = semaphore.NewWeighted(defaultReadLimit)
	writeSemaphore = semaphore.NewWeighted(defaultWriteLimit)
}

// InitSemaphores initializes the read and write semaphores with configured limits.
//
// PURPOSE: ULIMIT PROTECTION
// These semaphores protect against Linux ulimit (open file descriptor) exhaustion by
// limiting concurrent file operations. Each file operation consumes a file descriptor,
// and exceeding the system limit (ulimit -n) causes "too many open files" errors that
// can crash the node. The semaphores ensure total concurrent file operations never
// exceed the configured limits.
//
// CRITICAL USAGE REQUIREMENTS:
//  1. MUST be called in main() before creating any file store instances
//  2. MUST be called before any goroutines that perform file operations are started
//  3. Uses sync.Once to ensure one-time execution (subsequent calls are no-ops)
//  4. Replaces global variables - NOT safe to call after file operations begin
//
// RACE CONDITION WARNING:
// This function replaces global variables. Due to Go's memory model, there is no
// way to atomically replace a variable that other goroutines are reading without
// using atomic.Value (which requires changing all semaphore access patterns). Therefore,
// this function MUST be called during startup before any file operations begin. Calling
// it after file operations have started creates a data race where goroutines may read the
// variable while it's being written.
//
// CAPACITY PLANNING:
// Set readLimit + writeLimit well under your system's ulimit to account for other file
// descriptors (network sockets, log files, database connections). Check your ulimit with:
//
//	ulimit -n           # soft limit
//	ulimit -Hn          # hard limit
//
// Monitor actual usage: lsof -p <pid> | wc -l
// Default: 768 + 256 = 1024 (safe for systems with ulimit >= 4096)
//
// VALIDATION:
// The function validates limits and returns an error if they're out of acceptable bounds.
// Valid range: 1 to 10,000 for both read and write limits.
//
// Parameters:
//   - readLimit: Maximum concurrent read operations (must be 1-10000)
//   - writeLimit: Maximum concurrent write operations (must be 1-10000)
//
// Returns:
//   - error: Configuration error if limits are invalid, nil otherwise
//
// Example usage in main():
//
//	func main() {
//	    settings := settings.NewSettings()
//	    if err := file.InitSemaphores(
//	        settings.Block.FileStoreReadConcurrency,
//	        settings.Block.FileStoreWriteConcurrency,
//	    ); err != nil {
//	        panic(fmt.Sprintf("Failed to initialize file store semaphores: %v", err))
//	    }
//	    // ... continue with service initialization
//	}
func InitSemaphores(readLimit, writeLimit int) error {
	var initErr error

	semaphoreInitOnce.Do(func() {
		// Validate read limit
		if readLimit < MinSemaphoreLimit || readLimit > MaxSemaphoreLimit {
			initErr = errors.NewConfigurationError("invalid read limit %d: must be between %d and %d",
				readLimit, MinSemaphoreLimit, MaxSemaphoreLimit)
			return
		}

		// Validate write limit
		if writeLimit < MinSemaphoreLimit || writeLimit > MaxSemaphoreLimit {
			initErr = errors.NewConfigurationError("invalid write limit %d: must be between %d and %d",
				writeLimit, MinSemaphoreLimit, MaxSemaphoreLimit)
			return
		}

		// Create new semaphores with validated limits
		readSemaphore = semaphore.NewWeighted(int64(readLimit))
		writeSemaphore = semaphore.NewWeighted(int64(writeLimit))
	})

	return initErr
}

// acquireReadPermit acquires a single read permit with a timeout.
// This prevents goroutines from blocking indefinitely if the semaphore is full.
func acquireReadPermit(ctx context.Context) error {
	// Create a context with 25 second timeout
	acquireCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	if err := readSemaphore.Acquire(acquireCtx, 1); err != nil {
		if errors.Is(err, context.Canceled) {
			// Context was canceled, propagate the cancellation
			return errors.NewContextCanceledError("[File] read operation canceled while waiting for semaphore permit", err)
		} else if errors.Is(err, context.DeadlineExceeded) {
			return errors.NewServiceUnavailableError("[File] read operation timed out waiting for semaphore permit")
		}

		return errors.NewProcessingError("[File] failed to acquire read permit", err)
	}

	return nil
}

// releaseReadPermit releases a single read permit.
func releaseReadPermit() {
	readSemaphore.Release(1)
}

// acquireWritePermit acquires a single write permit with a timeout.
// This prevents goroutines from blocking indefinitely if the semaphore is full.
func acquireWritePermit(ctx context.Context) error {
	// Create a context with 25 second timeout
	acquireCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	if err := writeSemaphore.Acquire(acquireCtx, 1); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return errors.NewServiceUnavailableError("[File] write operation timed out waiting for semaphore permit")
		}

		return errors.NewStorageError("[File] failed to acquire write permit: %w", err)
	}

	return nil
}

// releaseWritePermit releases a single write permit.
func releaseWritePermit() {
	writeSemaphore.Release(1)
}

// New creates a new filesystem-based blob store with the specified configuration.
// The store is configured via URL parameters in the storeURL, which can specify options
// such as the base directory path, headers, footers, checksumming, and DAH cleaning intervals.
//
// The storeURL format follows the pattern: file:///path/to/storage/directory?param1=value1&param2=value2
// Supported URL parameters include:
// - header: Custom header to prepend to blobs (can be hex-encoded or plain text)
// - eofmarker: Custom footer marker to append to blobs (can be hex-encoded or plain text)
// - checksum: When set to "true", enables SHA256 checksumming of blobs
//
// Parameters:
//   - logger: Logger instance for recording operations and errors
//   - storeURL: URL containing the store configuration and path
//   - opts: Additional store configuration options
//
// Returns:
//   - *File: The configured file store instance
//   - error: Any error that occurred during initialization
func New(logger ulogger.Logger, storeURL *url.URL, opts ...options.StoreOption) (*File, error) {
	if storeURL == nil {
		return nil, errors.NewConfigurationError("storeURL is nil")
	}

	return newStore(logger, storeURL, opts...)
}

func newStore(logger ulogger.Logger, storeURL *url.URL, opts ...options.StoreOption) (*File, error) {
	logger = logger.New("file")

	if storeURL == nil {
		return nil, errors.NewConfigurationError("storeURL is nil")
	}

	var path string
	if storeURL.Host == "." {
		path = storeURL.Path[1:] // relative path
	} else {
		path = storeURL.Path // absolute path
	}

	// create the path if necessary
	if len(path) > 0 {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, errors.NewStorageError("[File] failed to create directory", err)
		}
	}

	storeOpts := options.NewStoreOptions(opts...)

	if hashPrefix := storeURL.Query().Get("hashPrefix"); len(hashPrefix) > 0 {
		val, err := strconv.ParseInt(hashPrefix, 10, 32)
		if err != nil {
			return nil, errors.NewStorageError("[File] failed to parse hashPrefix", err)
		}

		storeOpts.HashPrefix = int(val)
	}

	if hashSuffix := storeURL.Query().Get("hashSuffix"); len(hashSuffix) > 0 {
		val, err := strconv.ParseInt(hashSuffix, 10, 32)
		if err != nil {
			return nil, errors.NewStorageError("[File] failed to parse hashSuffix", err)
		}

		storeOpts.HashPrefix = -int(val)
	}

	// Parse disableDAH URL parameter
	// This can be set via URL (?disableDAH=true/false) or via StoreOption (WithDisableDAH(true/false))
	// URL parameter takes precedence over StoreOption (bidirectional override)
	if disableDAH := storeURL.Query().Get("disableDAH"); disableDAH != "" {
		storeOpts.DisableDAH = disableDAH == "true"
	}

	if len(storeOpts.SubDirectory) > 0 {
		if err := os.MkdirAll(filepath.Join(path, storeOpts.SubDirectory), 0755); err != nil {
			return nil, errors.NewStorageError("[File] failed to create sub directory", err)
		}
	}

	fileStore := &File{
		path:          path,
		logger:        logger,
		options:       storeOpts,
		persistSubDir: storeOpts.PersistSubDir,
		prunerClient:  storeOpts.Pruner,
		storeType:     storeOpts.StoreType,
	}

	// If no pruner client is set, use a null client
	if fileStore.prunerClient == nil {
		fileStore.prunerClient = &options.NullPrunerClient{}
	}

	// Check if longterm storage options are provided
	if storeOpts.PersistSubDir != "" {
		// Validate PersistSubDir doesn't contain path traversal sequences
		if strings.Contains(storeOpts.PersistSubDir, "..") {
			return nil, errors.NewInvalidArgumentError("[File] PersistSubDir contains path traversal sequence")
		}

		// Create persistent subdirectory
		if err := os.MkdirAll(filepath.Join(path, storeOpts.PersistSubDir), 0755); err != nil {
			return nil, errors.NewStorageError("[File] failed to create persist sub directory", err)
		}

		// Initialize longterm storage client if URL is provided
		if storeOpts.LongtermStoreURL != nil {
			var err error

			fileStore.longtermClient, err = newStore(logger, storeOpts.LongtermStoreURL)
			if err != nil {
				return nil, errors.NewStorageError("[File] failed to create longterm client", err)
			}
		}
	}

	if storeOpts.BlockHeightCh != nil {
		go func() {
			for {
				fileStore.SetCurrentBlockHeight(<-storeOpts.BlockHeightCh)
			}
		}()
	}

	return fileStore, nil
}

func (s *File) SetCurrentBlockHeight(height uint32) {
	s.currentBlockHeight.Store(height)
}

// readDAHFromFile_internal reads a DAH value from file WITHOUT semaphore protection.
// Caller must hold appropriate semaphore or be in a context where protection isn't needed.
func (s *File) readDAHFromFile_internal(fileName string) (uint32, error) {
	// read the dah
	dahBytes, err := os.ReadFile(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, errors.NewNotFoundError("[File] DAH file %s not found", fileName)
		}

		return 0, errors.NewStorageError("[File] failed to read DAH file", err)
	}

	// Trim whitespace and validate content
	dahStr := strings.TrimSpace(string(dahBytes))
	if dahStr == "" {
		return 0, errors.NewProcessingError("[File] DAH file %s is empty", fileName)
	}

	dah, err := strconv.ParseUint(dahStr, 10, 32)
	if err != nil {
		return 0, errors.NewProcessingError("[File] failed to parse DAH from %s: %s", fileName, dahStr)
	}

	// Validate DAH value - should never be 0
	if dah == 0 {
		return 0, errors.NewProcessingError("[File] invalid DAH value 0 in file %s", fileName)
	}

	return uint32(dah), nil
}

// readDAHFromFile reads a DAH value from file WITH semaphore protection.
// Use this for background operations or when caller doesn't hold a semaphore.
func (s *File) readDAHFromFile(fileName string) (uint32, error) {
	ctx := context.Background()
	if err := acquireReadPermit(ctx); err != nil {
		return 0, err
	}
	defer releaseReadPermit()

	return s.readDAHFromFile_internal(fileName)
}

// writeDAHToFileInternal writes a DAH value to file WITHOUT semaphore protection.
// Caller must hold appropriate semaphore.
func (s *File) writeDAHToFileInternal(dahFilename string, dah uint32) error {
	// Validate DAH value before writing
	if dah == 0 {
		return errors.NewProcessingError("[File] attempted to write invalid DAH value 0 to file %s", dahFilename)
	}

	dahContent := []byte(strconv.FormatUint(uint64(dah), 10))

	// Write directly to the file
	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err := os.WriteFile(dahFilename, dahContent, 0644); err != nil {
		return errors.NewStorageError("[File][%s] failed to write DAH to file", dahFilename, err)
	}

	return nil
}

// writeDAHToFile writes a DAH value to file WITH semaphore protection.
// Use this when caller doesn't already hold a semaphore.
func (s *File) writeDAHToFile(dahFilename string, dah uint32) error {
	ctx := context.Background()
	if err := acquireWritePermit(ctx); err != nil {
		return err
	}
	defer releaseWritePermit()

	return s.writeDAHToFileInternal(dahFilename, dah)
}

// Health checks the health status of the file-based blob store.
// It verifies that the storage directory exists and is accessible by attempting to
// create a temporary file. This ensures that the store can perform basic read/write
// operations, which is essential for its functionality.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - checkLiveness: Whether to perform a more thorough liveness check (unused in this implementation)
//
// Returns:
//   - int: HTTP status code indicating health status (200 for healthy, 500 for unhealthy)
//   - string: Description of the health status ("OK" or an error message)
//   - error: Any error that occurred during the health check
func (s *File) Health(ctx context.Context, _ bool) (int, string, error) {
	s.debugf("[File] Health check start path=%s", s.path)

	if err := acquireWritePermit(ctx); err != nil {
		return http.StatusServiceUnavailable, "File Store: Write concurrency limit reached", err
	}
	defer releaseWritePermit()

	if err := acquireReadPermit(ctx); err != nil {
		return http.StatusServiceUnavailable, "File Store: Read concurrency limit reached", err
	}
	defer releaseReadPermit()

	// Check if the path exists
	if _, err := os.Stat(s.path); os.IsNotExist(err) {
		return http.StatusInternalServerError, "File Store: Path does not exist", err
	}

	// Create a temporary file to test read/write/delete permissions
	tempFile, err := os.CreateTemp(s.path, "health-check-*.tmp")
	if err != nil {
		return http.StatusInternalServerError, "File Store: Unable to create temporary file", err
	}

	tempFileName := tempFile.Name()
	defer os.Remove(tempFileName) // Ensure the temp file is removed

	// Test write permission
	testData := []byte("health check")
	if _, err := tempFile.Write(testData); err != nil {
		return http.StatusInternalServerError, "File Store: Unable to write to file", err
	}

	tempFile.Close()

	// Test read permission
	readData, err := os.ReadFile(tempFileName)
	if err != nil {
		return http.StatusInternalServerError, "File Store: Unable to read file", err
	}

	if !bytes.Equal(readData, testData) {
		return http.StatusInternalServerError, "File Store: Data integrity check failed", nil
	}

	// Test delete permission
	if err := os.Remove(tempFileName); err != nil {
		return http.StatusInternalServerError, "File Store: Unable to delete file", err
	}

	s.debugf("[File] Health check succeeded path=%s", s.path)
	return http.StatusOK, "File Store: Healthy", nil
}

// Close performs any necessary cleanup for the file store.
// In the current implementation, this is a no-op as the file store doesn't maintain
// any resources that need explicit cleanup beyond what Go's garbage collector handles.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//
// Returns:
//   - error: Always returns nil
func (s *File) Close(_ context.Context) error {
	return nil
}

func (s *File) errorOnOverwrite(filename string, opts *options.Options) error {
	if !opts.AllowOverwrite {
		// Note: No semaphore acquisition here because this is only called from SetFromReader
		// which already holds the write semaphore. The os.Stat call is safe because we're
		// within the write operation's semaphore protection.
		if _, err := os.Stat(filename); err == nil {
			return errors.NewBlobAlreadyExistsError("[File][allowOverwrite] [%s] already exists in store", filename)
		}
	}

	return nil
}

// SetFromReader stores a blob in the file store from a streaming reader.
// This method is more memory-efficient than Set for large blobs as it streams data
// directly to disk without loading the entire blob into memory. It handles file creation,
// directory creation if needed, and optional checksumming based on store configuration.
//
// The method follows these steps:
// 1. Construct the target filename from the key and file type
// 2. Create any necessary parent directories
// 3. Create a temporary file for writing
// 4. Stream data from the reader to the file, applying any headers/footers
// 5. Calculate checksums if enabled
// 6. Rename the temporary file to the final filename
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - reader: Reader providing the blob data
//   - opts: Optional file options
//
// Returns:
//   - error: Any error that occurred during the operation
func (s *File) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	if err := acquireWritePermit(ctx); err != nil {
		return errors.NewStorageError("[File][SetFromReader] failed to acquire write permit", err)
	}
	defer releaseWritePermit()

	keyHex := formatKeyHex(key)
	s.debugf("[File] SetFromReader start key=%s type=%s", keyHex, fileType)

	filename, err := s.constructFilename(key, fileType, opts)
	if err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to get file name", utils.ReverseAndHexEncodeSlice(key), err)
	}

	merged := options.MergeOptions(s.options, opts)

	if err := s.errorOnOverwrite(filename, merged); err != nil {
		return err
	}

	// Generate a cryptographically secure random number
	randNum, err := rand.Int(rand.Reader, big.NewInt(1<<63-1))
	if err != nil {
		return errors.NewStorageError("[File][SetFromReader] failed to generate random number", err)
	}

	tmpFilename := fmt.Sprintf("%s.%d.tmp", filename, randNum)

	// Create the file first
	file, err := os.Create(tmpFilename)
	if err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to create file", filename, err)
	}

	// Track whether we should clean up the temp file on exit.
	// Default to true (cleanup); only set to false on success path after rename.
	cleanupTmpFile := true
	defer func() {
		file.Close()
		if cleanupTmpFile {
			// Remove temp file on any error path to prevent incomplete files
			if removeErr := os.Remove(tmpFilename); removeErr != nil && !os.IsNotExist(removeErr) {
				s.logger.Warnf("[File][SetFromReader] failed to remove temp file %s: %v", tmpFilename, removeErr)
			}
		}
	}()

	// Set up the hasher; keep destination as the raw *os.File so io.Copy can use the ReadFrom fast path
	hasher := sha256.New()

	// Write header unless SkipHeader option is set. Write it to both the file and the hasher.
	if !merged.SkipHeader {
		header := fileformat.NewHeader(fileType)
		if err := header.Write(io.MultiWriter(file, hasher)); err != nil {
			return errors.NewStorageError("[File][SetFromReader] [%s] failed to write header to file", filename, err)
		}
	}

	// Stream the body using io.Copy with io.TeeReader so the file can use ReadFrom fast path while also hashing.
	bytesWritten, err := io.Copy(file, io.TeeReader(reader, hasher))
	if err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to write data to file", filename, err)
	}

	// Validate that actual data was written from the reader
	if bytesWritten == 0 {
		return errors.NewStorageError("[File][SetFromReader] [%s] reader provided zero bytes of data", filename)
	}

	// Success path - don't cleanup temp file, we're about to rename it
	cleanupTmpFile = false

	// rename the file to remove the .tmp extension
	if err = os.Rename(tmpFilename, filename); err != nil {
		// check is some other process has created this file before us
		if _, statErr := os.Stat(filename); statErr != nil {
			// Rename failed and file doesn't exist - clean up temp file
			_ = os.Remove(tmpFilename)
			return errors.NewStorageError("[File][SetFromReader] [%s] failed to rename file from tmp", filename, err)
		} else {
			// Another process created the file - clean up our temp file
			_ = os.Remove(tmpFilename)
			s.logger.Warnf("[File][SetFromReader] [%s] already exists so another process created it first", filename)
		}
	}

	// Write SHA256 hash file
	if err = s.writeHashFile(hasher, filename); err != nil {
		return errors.NewStorageError("[File][SetFromReader] failed to write hash file", err)
	}

	s.debugf("[File] SetFromReader completed key=%s type=%s filename=%s", keyHex, fileType, filename)
	return nil
}

func (s *File) writeHashFile(hasher hash.Hash, filename string) error {
	if hasher == nil {
		return nil
	}

	// Get the base name and extension separately
	base := filepath.Base(filename)

	// Format: "<hash>  <reversed_key>.<extension>\n"
	hashStr := fmt.Sprintf("%x  %s\n", // N.B. The 2 spaces is important for the hash to be valid
		hasher.Sum(nil),
		base)

	hashFilename := filename + checksumExtension
	tmpHashFilename := hashFilename + ".tmp"

	var err error

	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err = os.WriteFile(tmpHashFilename, []byte(hashStr), 0644); err != nil {
		return errors.NewStorageError("[File] failed to write hash file", err)
	}

	if err = os.Rename(tmpHashFilename, hashFilename); err != nil {
		// check is some other process has created this file before us
		if _, statErr := os.Stat(hashFilename); statErr != nil {
			return errors.NewStorageError("[File] failed to rename hash file", err)
		} else {
			s.logger.Warnf("[File] hash file %s already exists so another process created it first", hashFilename)
		}
	}

	return nil
}

// Set stores a blob in the file store.
// This method is a convenience wrapper around SetFromReader that converts the byte slice
// to a reader before delegating to SetFromReader for the actual storage operation.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - value: The blob data to store
//   - opts: Optional file options
//
// Returns:
//   - error: Any error that occurred during the operation
func (s *File) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	keyHex := formatKeyHex(key)
	s.debugf("[File] Set start key=%s type=%s size=%d", keyHex, fileType, len(value))

	reader := io.NopCloser(bytes.NewReader(value))

	err := s.SetFromReader(ctx, key, fileType, reader, opts...)
	if err == nil {
		s.debugf("[File] Set completed key=%s type=%s size=%d", keyHex, fileType, len(value))
	}

	return err
}

func (s *File) constructFilename(key []byte, fileType fileformat.FileType, opts []options.FileOption) (string, error) {
	merged := options.MergeOptions(s.options, opts)

	if merged.SubDirectory != "" {
		if err := os.MkdirAll(filepath.Join(s.path, merged.SubDirectory), 0755); err != nil {
			return "", errors.NewStorageError("[File] failed to create sub directory", err)
		}
	}

	fileName, err := merged.ConstructFilename(s.path, key, fileType)
	if err != nil {
		return "", err
	}

	// Skip DAH functionality entirely if disabled for this store
	// Lifecycle is managed externally (e.g., by Aerospike pruner)
	if merged.DisableDAH {
		return fileName, nil
	}

	dah := merged.DAH

	// If the dah is not set and the block height retention is set, set the dah to the current block height plus the block height retention
	if dah == 0 && merged.BlockHeightRetention > 0 {
		// write DAH to file
		dah = s.currentBlockHeight.Load() + merged.BlockHeightRetention
	}

	if dah > 0 {
		// Schedule deletion with pruner service - this is now mandatory
		// All pruning is centralized through the pruner service
		if s.prunerClient == nil {
			return "", errors.NewConfigurationError("cannot schedule blob deletion: pruner client not configured")
		}

		// Use background context since SetFromReader might be cancelled
		// but we still want the deletion to be scheduled
		bgCtx := context.Background()
		if err := s.prunerClient.ScheduleBlobDeletion(bgCtx, key, fileType, s.storeType, dah); err != nil {
			return "", errors.NewStorageError("failed to schedule blob deletion with pruner", err)
		}
	}

	return fileName, nil
}

// SetDAH sets the Delete-At-Height (DAH) value for a blob in the file store.
// The DAH value determines at which blockchain height the blob will be automatically deleted.
// This implementation stores the DAH value in a separate file with the same name as the blob
// but with a .dah extension, and also maintains an in-memory map of DAH values for quick access.
//
// If the store has DisableDAH=true, this method returns immediately without error, as DAH
// functionality is disabled for this store (lifecycle managed externally).
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - newDAH: The delete at height value
//   - opts: Optional file options
//
// Returns:
//   - error: Any error that occurred during the operation, including if the blob doesn't exist
func (s *File) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, newDAH uint32, opts ...options.FileOption) error {
	// If DAH is disabled for this store, return immediately
	// This store's lifecycle is managed externally (e.g., by Aerospike pruner)
	if s.options.DisableDAH {
		return nil
	}

	if err := acquireWritePermit(ctx); err != nil {
		return errors.NewStorageError("[File][SetDAH] failed to acquire write permit", err)
	}
	defer releaseWritePermit()

	keyHex := formatKeyHex(key)
	s.debugf("[File] SetDAH start key=%s type=%s newDAH=%d", keyHex, fileType, newDAH)

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, key, fileType)
	if err != nil {
		return errors.NewStorageError("[File] failed to get file name", err)
	}

	// Pruner client is now mandatory for DAH operations
	if s.prunerClient == nil {
		return errors.NewConfigurationError("cannot modify DAH: pruner client not configured")
	}

	if newDAH == 0 {
		// Cancel scheduled deletion with pruner
		// CRITICAL: This must succeed to prevent data loss - if cancellation fails,
		// the blob could still be deleted even though we want to persist it forever
		if err := s.prunerClient.CancelBlobDeletion(ctx, key, fileType, s.storeType); err != nil {
			return errors.NewStorageError("failed to cancel blob deletion with pruner (critical for DAH=0)", err)
		}

		return nil
	}

	// make sure the file exists
	if _, err = os.Stat(fileName); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.ErrNotFound
		}

		return errors.NewStorageError("[File][%s] failed to get file info", fileName, err)
	}

	// Schedule deletion with pruner service - this is mandatory
	if err := s.prunerClient.ScheduleBlobDeletion(ctx, key, fileType, s.storeType, newDAH); err != nil {
		return errors.NewStorageError("failed to schedule blob deletion with pruner", err)
	}

	return nil
}

// GetIoReader retrieves a blob from the file store as a streaming reader.
// This method provides memory-efficient access to blob data by returning a file handle
// that can be used to stream the data without loading it entirely into memory. It supports
// fallback to alternative storage locations if the primary file is not found.
//
// The method follows these steps:
// 1. Construct the filename from the key and file type
// 2. Attempt to open the file from the primary storage location
// 3. If not found, try alternative locations (persistent subdirectory, longterm store)
// 4. Validate the file header if headers are enabled
// 5. Return a reader for the file
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - opts: Optional file options
//
// Returns:
//   - io.ReadCloser: Reader for streaming the blob data
//   - error: Any error that occurred during the operation
func (s *File) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	keyHex := formatKeyHex(key)
	s.debugf("[File] GetIoReader start key=%s type=%s", keyHex, fileType)

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, key, fileType)
	if err != nil {
		return nil, err
	}

	f, err := s.openFileWithFallback(ctx, merged, fileName, key, fileType, opts...)
	if err != nil {
		return nil, err
	}

	if err := s.validateFileHeader(f, fileName, fileType); err != nil {
		if closeErr := f.Close(); closeErr != nil {
			s.logger.Warnf("[File][GetIoReader] failed to close file after header validation error: %v", closeErr)
		}
		return nil, err
	}

	s.debugf("[File] GetIoReader result key=%s type=%s filename=%s", keyHex, fileType, fileName)
	return f, nil
}

func (s *File) GetReader(ctx context.Context, key []byte, fileType fileformat.FileType) (io.ReadCloser, error) {
	return s.GetIoReader(ctx, key, fileType)
}

// validateFileHeader reads and validates the file header.
func (s *File) validateFileHeader(f io.Reader, fileName string, fileType fileformat.FileType) error {
	header := &fileformat.Header{}
	if err := header.Read(f); err != nil {
		return errors.NewStorageError("[File][GetIoReader] [%s] missing or invalid header: %v", fileName, err)
	}

	if header.FileType() != fileType {
		return errors.NewStorageError("[File][GetIoReader] [%s] header filetype mismatch: got %s, want %s", fileName, header.FileType(), fileType)
	}

	return nil
}

// openFileWithFallback tries to open the primary file, then persistSubDir, then longtermClient if available.
func (s *File) openFileWithFallback(ctx context.Context, merged *options.Options, fileName string, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	if err := acquireReadPermit(ctx); err != nil {
		return nil, errors.NewStorageError("[File][openFileWithFallback] failed to acquire read permit", err)
	}

	f, err := os.Open(fileName)
	if err == nil {
		return &semaphoreReadCloser{ReadCloser: f}, nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		releaseReadPermit()
		return nil, errors.NewStorageError("[File][openFileWithFallback] [%s] failed to open file", fileName, err)
	}

	// Try persistSubDir if set
	if s.persistSubDir != "" {
		persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), key, fileType)
		if err != nil {
			releaseReadPermit()
			return nil, err
		}

		persistFile, err := os.Open(persistedFilename)
		if err == nil {
			return &semaphoreReadCloser{ReadCloser: persistFile}, nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			releaseReadPermit()
			return nil, errors.NewStorageError("[File][openFileWithFallback] [%s] failed to open file in persist directory", persistedFilename, err)
		}
	}

	// Try longterm storage if available
	if s.longtermClient != nil {
		releaseReadPermit()

		fileReader, err := s.longtermClient.GetIoReader(ctx, key, fileType, opts...)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, errors.ErrNotFound
			}

			return nil, errors.NewStorageError("[File][openFileWithFallback] [%s] unable to open longterm storage file", fileName, err)
		}

		return fileReader, nil
	}

	releaseReadPermit()
	return nil, errors.ErrNotFound
}

// Get retrieves a blob from the file store.
// This method is a convenience wrapper around GetIoReader that reads the entire blob
// into memory and returns it as a byte slice. For large blobs, consider using GetIoReader
// directly to avoid loading the entire blob into memory at once.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - opts: Optional file options
//
// Returns:
//   - []byte: The blob data
//   - error: Any error that occurred during the operation
func (s *File) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	keyHex := formatKeyHex(key)
	s.debugf("[File] Get start key=%s type=%s", keyHex, fileType)

	fileReader, err := s.GetIoReader(ctx, key, fileType, opts...)
	if err != nil {
		return nil, err
	}

	defer fileReader.Close()

	// Read all bytes from the reader
	var fileData bytes.Buffer

	if _, err = io.Copy(&fileData, fileReader); err != nil {
		return nil, errors.NewStorageError("[File][Get] failed to read data from file reader", err)
	}

	data := fileData.Bytes()
	s.debugf("[File] Get result key=%s type=%s bytes=%d", keyHex, fileType, len(data))

	return data, nil
}

// Exists checks if a blob exists in the file store.
// This method attempts to find the blob file in the primary storage location,
// and if not found, checks alternative locations (persistent subdirectory, longterm store).
// It's more efficient than Get as it only checks for existence without reading the file contents.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - opts: Optional file options
//
// Returns:
//   - bool: True if the blob exists, false otherwise
//   - error: Any error that occurred during the check (other than not found errors)
func (s *File) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	if err := acquireReadPermit(ctx); err != nil {
		return false, errors.NewStorageError("[File][Exists] failed to acquire read permit", err)
	}
	defer releaseReadPermit()

	keyHex := formatKeyHex(key)
	s.debugf("[File] Exists start key=%s type=%s", keyHex, fileType)

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, key, fileType)
	if err != nil {
		return false, err
	}

	// check whether the file exists
	fileInfo, err := os.Stat(fileName)
	if err == nil && fileInfo != nil {
		s.debugf("[File] Exists result key=%s type=%s result=true (primary)", keyHex, fileType)
		return true, nil
	}

	// Try persistSubDir if set
	if s.persistSubDir != "" {
		persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), key, fileType)
		if err != nil {
			return false, err
		}

		fileInfo, err = os.Stat(persistedFilename)
		if err == nil && fileInfo != nil {
			s.debugf("[File] Exists result key=%s type=%s result=true (persist)", keyHex, fileType)
			return true, nil
		}
	}

	if s.longtermClient != nil {
		exists, err := s.longtermClient.Exists(ctx, key, fileType, opts...)
		if err == nil {
			s.debugf("[File] Exists result key=%s type=%s result=%t (longterm)", keyHex, fileType, exists)
		}

		return exists, err
	}

	s.debugf("[File] Exists result key=%s type=%s result=false", keyHex, fileType)
	return false, nil
}

// Del deletes a blob from the file store.
// This method removes the blob file and any associated files (such as checksum and DAH files).
// It also removes the DAH entry from the in-memory map if it exists.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: The key identifying the blob to delete
//   - fileType: The type of the file
//   - opts: Optional file options
//
// Returns:
//   - error: Any error that occurred during deletion, or nil if the blob was successfully deleted
//     or didn't exist
func (s *File) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	if err := acquireWritePermit(ctx); err != nil {
		return errors.NewStorageError("[File][Del] failed to acquire write permit", err)
	}
	defer releaseWritePermit()

	keyHex := formatKeyHex(key)
	s.debugf("[File] Del start key=%s type=%s", keyHex, fileType)

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, key, fileType)
	if err != nil {
		s.logger.Debugf("[FILE_DEL] Failed to construct filename: key=%s type=%s error=%v", keyHex, fileType, err)
		return err
	}

	s.logger.Debugf("[File] Del constructed filename: key=%s type=%s file=%s", keyHex, fileType, fileName)
	s.logger.Debugf("[FILE_DEL] Attempting deletion: base_path=%s key=%s file=%s", s.path, keyHex, fileName)

	// remove checksum file, if exists
	checksumFile := fileName + checksumExtension
	if err := os.Remove(checksumFile); err == nil {
		s.logger.Debugf("[FILE_DEL] Deleted checksum file: %s", checksumFile)
	}

	if err = os.Remove(fileName); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// If the file does not exist, consider it deleted
			s.logger.Debugf("[FILE_DEL] File not found (already deleted): %s - treating as success", fileName)
			s.debugf("[File] Del skipped key=%s type=%s reason=file_missing", keyHex, fileType)
			return nil
		}

		s.logger.Debugf("[FILE_DEL] Failed to remove file: %s error=%v", fileName, err)
		return errors.NewStorageError("[File][Del] [%s] failed to remove file", fileName, err)
	}

	s.logger.Debugf("[FILE_DEL] Successfully deleted file: %s", fileName)
	s.debugf("[File] Del completed key=%s type=%s", keyHex, fileType)
	return nil
}

// findFilesByExtension removed - was only used by loadDAHs which has been removed
