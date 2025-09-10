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
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/go-utils"
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
	// fileDAHs maps filenames to their Delete-At-Height values for expiration tracking
	fileDAHs map[string]uint32
	// fileDAHsMu protects concurrent access to the fileDAHs map
	fileDAHsMu sync.Mutex
	// fileDAHsCtxCancel is used to stop the DAH cleanup background process on close
	fileDAHsCtxCancel context.CancelFunc
	// currentBlockHeight tracks the current blockchain height for DAH processing
	currentBlockHeight atomic.Uint32
	// persistSubDir is an optional subdirectory for organization within the base path
	persistSubDir string
	// longtermClient is an optional secondary storage backend for hybrid storage models
	longtermClient longtermStore
	cleanupCh      chan struct{}
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

// fileSemaphore is a global limiting semaphore for file operations to prevent excessive
// concurrent file operations that could impact system performance. It limits the maximum
// number of concurrent file operations to 1024, which helps prevent resource exhaustion
// while still allowing enough parallelism for high-throughput scenarios.
var fileSemaphore = make(chan struct{}, 1024)

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

var fileCleanerOnce sync.Map

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

	options := options.NewStoreOptions(opts...)

	if hashPrefix := storeURL.Query().Get("hashPrefix"); len(hashPrefix) > 0 {
		val, err := strconv.ParseInt(hashPrefix, 10, 64)
		if err != nil {
			return nil, errors.NewStorageError("[File] failed to parse hashPrefix", err)
		}

		options.HashPrefix = int(val)
	}

	if hashSuffix := storeURL.Query().Get("hashSuffix"); len(hashSuffix) > 0 {
		val, err := strconv.ParseInt(hashSuffix, 10, 64)
		if err != nil {
			return nil, errors.NewStorageError("[File] failed to parse hashSuffix", err)
		}

		options.HashPrefix = -int(val)
	}

	if len(options.SubDirectory) > 0 {
		if err := os.MkdirAll(filepath.Join(path, options.SubDirectory), 0755); err != nil {
			return nil, errors.NewStorageError("[File] failed to create sub directory", err)
		}
	}

	fileDAHsCtx, fileDAHsCtxCancel := context.WithCancel(context.Background())

	fileStore := &File{
		path:              path,
		logger:            logger,
		options:           options,
		fileDAHs:          make(map[string]uint32),
		fileDAHsCtxCancel: fileDAHsCtxCancel,
		persistSubDir:     options.PersistSubDir,
		cleanupCh:         make(chan struct{}, 1),
	}

	// Check if longterm storage options are provided
	if options.PersistSubDir != "" {
		// Create persistent subdirectory
		if err := os.MkdirAll(filepath.Join(path, options.PersistSubDir), 0755); err != nil {
			return nil, errors.NewStorageError("[File] failed to create persist sub directory", err)
		}

		// Initialize longterm storage client if URL is provided
		if options.LongtermStoreURL != nil {
			var err error

			fileStore.longtermClient, err = newStore(logger, options.LongtermStoreURL)
			if err != nil {
				return nil, errors.NewStorageError("[File] failed to create longterm client", err)
			}
		}
	}

	if options.BlockHeightCh != nil {
		go func() {
			for {
				select {
				case <-fileDAHsCtx.Done():
					return
				case blockHeight := <-options.BlockHeightCh:
					fileStore.SetCurrentBlockHeight(blockHeight)
				}
			}
		}()
	}

	_, loaded := fileCleanerOnce.LoadOrStore(storeURL.String(), true)
	if !loaded { // i.e. it was stored and not loaded
		// load dah's in the background and start the dah cleaner
		go func() {
			if err := fileStore.loadDAHs(); err != nil {
				fileStore.logger.Warnf("[File] failed to load dahs: %v", err)
			}
		}()
	}

	// start the dah cleaner
	go fileStore.dahCleaner(fileDAHsCtx)

	return fileStore, nil
}

func (s *File) SetCurrentBlockHeight(height uint32) {
	s.currentBlockHeight.Store(height)

	select {
	case s.cleanupCh <- struct{}{}:
	default: // Channel is full; we are already cleaning up.
	}
}

func (s *File) loadDAHs() error {
	s.logger.Infof("[File] Loading file DAHs: %s", s.path)

	// Clean up any leftover .dah.tmp files from incomplete writes
	// Only remove files older than 10 minutes to avoid interfering with active writes
	tmpFiles, err := findFilesByExtension(s.path, "dah.tmp")
	if err == nil && len(tmpFiles) > 0 {
		now := time.Now()
		cleanupThreshold := 10 * time.Minute
		var cleaned int

		for _, tmpFile := range tmpFiles {
			info, err := os.Stat(tmpFile)
			if err != nil {
				continue
			}

			// Check if file is older than the threshold
			if now.Sub(info.ModTime()) > cleanupThreshold {
				if err := os.Remove(tmpFile); err != nil && !os.IsNotExist(err) {
					s.logger.Warnf("[File] failed to remove leftover tmp file: %s", tmpFile)
				} else {
					cleaned++
				}
			}
		}

		if cleaned > 0 {
			s.logger.Infof("[File] Cleaned up %d leftover .dah.tmp files (older than %v)", cleaned, cleanupThreshold)
		}
	}

	// get all files in the directory that end with .dah
	files, err := findFilesByExtension(s.path, ".dah")
	if err != nil {
		return errors.NewStorageError("[File] failed to find DAH files", err)
	}

	var dah uint32

	for _, fileName := range files {
		if fileName[len(fileName)-4:] != ".dah" {
			continue
		}

		dah, err = s.readDAHFromFile(fileName)
		if err != nil {
			// Log the error but continue processing other files
			s.logger.Warnf("[File] error reading DAH file %s: %v", fileName, err)

			// If it's an invalid DAH file (0 or corrupt), remove it
			var terr *errors.Error
			if errors.As(err, &terr) && terr.Code() == errors.ERR_PROCESSING {
				s.logger.Warnf("[File] removing invalid DAH file during initialization: %s", fileName)
				if removeErr := os.Remove(fileName); removeErr != nil && !os.IsNotExist(removeErr) {
					s.logger.Warnf("[File] failed to remove invalid DAH file: %s", fileName)
				}
			}
			continue
		}

		// This should not happen anymore with the validation in readDAHFromFile
		if dah == 0 {
			s.logger.Warnf("[File] unexpected DAH value 0 for file %s", fileName)
			continue
		}

		s.fileDAHsMu.Lock()
		s.fileDAHs[fileName[:len(fileName)-4]] = dah
		s.fileDAHsMu.Unlock()
	}

	return nil
}

func (s *File) readDAHFromFile(fileName string) (uint32, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

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

// writeDAHToFile writes a DAH value to a file with proper hardening
func (s *File) writeDAHToFile(dahFilename string, dah uint32) error {
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

func (s *File) dahCleaner(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.cleanupCh:
			s.cleanupExpiredFiles()
		}
	}
}

func (s *File) cleanupExpiredFiles() {
	s.logger.Debugf("[File] Cleaning file DAHs")

	filesToRemove := s.getExpiredFiles()
	for _, fileName := range filesToRemove {
		s.cleanupExpiredFile(fileName)
	}
}

func (s *File) getExpiredFiles() []string {
	s.fileDAHsMu.Lock()
	filesToRemove := make([]string, 0, len(s.fileDAHs))

	currentBlockHeight := s.currentBlockHeight.Load()

	s.logger.Debugf("[File] current block height is %d", currentBlockHeight)

	for fileName, dah := range s.fileDAHs {
		if dah <= currentBlockHeight {
			filesToRemove = append(filesToRemove, fileName)
			s.logger.Debugf("[File] removing expired file: %s", fileName)
		}
	}
	s.fileDAHsMu.Unlock()

	return filesToRemove
}

func (s *File) cleanupExpiredFile(fileName string) {
	// check if the DAH file still exists, even if the map says it has expired, another process might have updated it
	dah, err := s.readDAHFromFile(fileName + ".dah")
	if err != nil {
		// If it's a processing error (invalid DAH value or corrupt file), clean it up
		if errors.Is(err, errors.ErrProcessing) {
			s.logger.Warnf("[File] invalid DAH file detected during cleanup: %s, error: %v", fileName+".dah", err)
			s.removeDAHFromMap(fileName)

			// Remove the invalid DAH file, but keep the blob files
			if removeErr := os.Remove(fileName + ".dah"); removeErr != nil && !os.IsNotExist(removeErr) {
				s.logger.Warnf("[File] failed to remove invalid DAH file: %s", fileName+".dah")
			}
		} else if errors.Is(err, errors.ErrNotFound) {
			s.removeDAHFromMap(fileName)
			s.logger.Debugf("[File] DAH file not found during cleanup, removing from map: %s", fileName+".dah")
		} else {
			s.logger.Debugf("[File] failed to read DAH from file: %s, error: %v", fileName+".dah", err)
		}
		return
	}

	// This should not happen anymore with the validation in readDAHFromFile
	if dah == 0 {
		s.removeDAHFromMap(fileName)
		s.logger.Warnf("[File] unexpected DAH value 0, removing: %s", fileName+".dah")

		// Remove the invalid DAH file, but keep the blob files
		if err := os.Remove(fileName + ".dah"); err != nil && !os.IsNotExist(err) {
			s.logger.Warnf("[File] failed to remove invalid DAH file: %s", fileName+".dah")
		}

		return
	}

	if !s.shouldRemoveFile(fileName, dah) {
		return
	}

	s.logger.Debugf("[File] removing expired file: %s", fileName)
	s.removeFiles(fileName)
	s.removeDAHFromMap(fileName)
}

func (s *File) shouldRemoveFile(fileName string, fileDAH uint32) bool {
	currentBlockHeight := s.currentBlockHeight.Load()

	if fileDAH > currentBlockHeight {
		// Update the DAH in our map
		s.fileDAHsMu.Lock()
		mapDAH := s.fileDAHs[fileName]
		s.fileDAHs[fileName] = fileDAH
		s.fileDAHsMu.Unlock()

		s.logger.Debugf("[File] DAH file %s has DAH of %d, but map has %d",
			fileName+".dah",
			fileDAH,
			mapDAH)

		return false
	}

	return true
}

func (s *File) removeFiles(fileName string) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	// Use the Del method to allow logger.go to log the removal to help with troubleshooting
	// FileTypeUnknown is "", which will remove the file, checksum and dah files
	// err := s.Del(context.Background(), []byte(fileName), fileformat.FileTypeUnknown)

	if err := os.Remove(fileName); err != nil && !os.IsNotExist(err) {
		s.logger.Warnf("[File] failed to remove file: %s", fileName)
	}

	if err := os.Remove(fileName + ".dah"); err != nil && !os.IsNotExist(err) {
		s.logger.Warnf("[File] failed to remove DAH file: %s", fileName+".dah")
	}

	if err := os.Remove(fileName + checksumExtension); err != nil && !os.IsNotExist(err) {
		s.logger.Warnf("[File] failed to remove checksum file: %s", fileName+checksumExtension)
	}
}

func (s *File) removeDAHFromMap(fileName string) {
	s.fileDAHsMu.Lock()
	delete(s.fileDAHs, fileName)
	s.fileDAHsMu.Unlock()
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
func (s *File) Health(_ context.Context, _ bool) (int, string, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

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
	// stop DAH cleaner
	s.fileDAHsCtxCancel()

	return nil
}

func (s *File) errorOnOverwrite(filename string, opts *options.Options) error {
	if !opts.AllowOverwrite {
		fileSemaphore <- struct{}{}
		defer func() {
			<-fileSemaphore
		}()

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
func (s *File) SetFromReader(_ context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

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
	defer file.Close()

	// Set up the writer and hasher
	writer, hasher := s.createWriter(file)

	// Write header unless SkipHeader option is set
	if !merged.SkipHeader {
		header := fileformat.NewHeader(fileType)

		if err := header.Write(writer); err != nil {
			return errors.NewStorageError("[File][SetFromReader] [%s] failed to write header to file", filename, err)
		}
	}

	if _, err = io.Copy(writer, reader); err != nil {
		return errors.NewStorageError("[File][SetFromReader] [%s] failed to write data to file", filename, err)
	}

	// rename the file to remove the .tmp extension
	if err = os.Rename(tmpFilename, filename); err != nil {
		// check is some other process has created this file before us
		if _, statErr := os.Stat(filename); statErr != nil {
			return errors.NewStorageError("[File][SetFromReader] [%s] failed to rename file from tmp", filename, err)
		} else {
			s.logger.Warnf("[File][SetFromReader] [%s] already exists so another process created it first", filename)
		}
	}

	// Write SHA256 hash file
	if err = s.writeHashFile(hasher, filename); err != nil {
		return errors.NewStorageError("[File][SetFromReader] failed to write hash file", err)
	}

	return nil
}

func (s *File) createWriter(file *os.File) (io.Writer, hash.Hash) {
	// Then set up the writer and hasher
	var (
		writer io.Writer
		hasher hash.Hash
	)

	hasher = sha256.New()
	writer = io.MultiWriter(file, hasher)

	return writer, hasher
}

func (s *File) writeHashFile(hasher hash.Hash, filename string) error {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

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
	reader := io.NopCloser(bytes.NewReader(value))

	return s.SetFromReader(ctx, key, fileType, reader, opts...)
}

func (s *File) constructFilename(key []byte, fileType fileformat.FileType, opts []options.FileOption) (string, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

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

	dah := merged.DAH

	// If the dah is not set and the block height retention is set, set the dah to the current block height plus the block height retention
	if dah == 0 && merged.BlockHeightRetention > 0 {
		// write DAH to file
		dah = s.currentBlockHeight.Load() + merged.BlockHeightRetention
	}

	if dah > 0 {
		dahFilename := fileName + ".dah"
		if err := s.writeDAHToFile(dahFilename, dah); err != nil {
			return "", err
		}

		s.fileDAHsMu.Lock()
		s.fileDAHs[fileName] = dah
		s.fileDAHsMu.Unlock()
	} else {
		// delete DAH file, if it existed
		_ = os.Remove(fileName + ".dah")

		s.fileDAHsMu.Lock()
		delete(s.fileDAHs, fileName)
		s.fileDAHsMu.Unlock()
	}

	return fileName, nil
}

// SetDAH sets the Delete-At-Height (DAH) value for a blob in the file store.
// The DAH value determines at which blockchain height the blob will be automatically deleted.
// This implementation stores the DAH value in a separate file with the same name as the blob
// but with a .dah extension, and also maintains an in-memory map of DAH values for quick access.
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
func (s *File) SetDAH(_ context.Context, key []byte, fileType fileformat.FileType, newDAH uint32, opts ...options.FileOption) error {
	// limit the number of concurrent file operations
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, key, fileType)
	if err != nil {
		return errors.NewStorageError("[File] failed to get file name", err)
	}

	if newDAH == 0 {
		s.fileDAHsMu.Lock()
		delete(s.fileDAHs, fileName)
		s.fileDAHsMu.Unlock()

		// delete the DAH file
		if err = os.Remove(fileName + ".dah"); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}

			return errors.NewStorageError("[File][%s] failed to remove DAH file", fileName, err)
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

	// write DAH to file
	dahFilename := fileName + ".dah"
	if err = s.writeDAHToFile(dahFilename, newDAH); err != nil {
		return err
	}

	s.fileDAHsMu.Lock()
	s.fileDAHs[fileName] = newDAH
	s.fileDAHsMu.Unlock()

	return nil
}

func (s *File) GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error) {
	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, key, fileType)
	if err != nil {
		return 0, err
	}

	// Check if the file (not the DAH file) exists
	exists, err := s.Exists(ctx, key, fileType, opts...)
	if err != nil {
		return 0, err
	}

	// If the file doesn't exist, why are we trying to get the DAH?  Return an error
	if !exists {
		return 0, errors.ErrNotFound
	}

	// Get the DAH from the map
	s.fileDAHsMu.Lock()
	dah, ok := s.fileDAHs[fileName]
	s.fileDAHsMu.Unlock()

	if !ok {
		// check whether the DAH file exists, it could have been created by another process
		dah, err = s.readDAHFromFile(fileName + ".dah")
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				return 0, nil
			}

			return 0, err
		}
	}

	return dah, nil
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
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

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
		f.Close()
		return nil, err
	}

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
	f, err := os.Open(fileName)
	if err == nil {
		return f, nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return nil, errors.NewStorageError("[File][GetIoReader] [%s] failed to open file", fileName, err)
	}

	// Try persistSubDir if set
	if s.persistSubDir != "" {
		persistedFilename, err := merged.ConstructFilename(filepath.Join(s.path, s.persistSubDir), key, fileType)
		if err != nil {
			return nil, err
		}

		persistFile, err := os.Open(persistedFilename)
		if err == nil {
			return persistFile, nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			return nil, errors.NewStorageError("[File][GetIoReader] [%s] failed to open file in persist directory", persistedFilename, err)
		}
	}

	// Try longterm storage if available
	if s.longtermClient != nil {
		fileReader, err := s.longtermClient.GetIoReader(ctx, key, fileType, opts...)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, errors.ErrNotFound
			}

			return nil, errors.NewStorageError("[File][GetIoReader] [%s] unable to open longterm storage file", fileName, err)
		}

		return fileReader, nil
	}

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

	return fileData.Bytes(), nil
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
	// The GetIOReader method will handle file locking and error handling,
	// This method is not updated to require fileType, as Exists is not a getter/setter for content.
	fileReader, err := s.GetIoReader(ctx, key, fileType, opts...)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			// If the file does not exist, return false
			return false, nil
		}

		return false, err
	}

	defer fileReader.Close()

	// If we can read from the fileReader, the file exists
	return true, nil
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
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	s.logger.Debugf("[File] Del: %s", utils.ReverseAndHexEncodeSlice(key))

	merged := options.MergeOptions(s.options, opts)

	fileName, err := merged.ConstructFilename(s.path, key, fileType)
	if err != nil {
		return err
	}

	// remove DAH file, if exists
	_ = os.Remove(fileName + ".dah")

	// remove checksum file, if exists
	_ = os.Remove(fileName + checksumExtension)

	if err = os.Remove(fileName); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// If the file does not exist, consider it deleted
			return nil
		}

		return errors.NewStorageError("[File][Del] [%s] failed to remove file", fileName, err)
	}

	return nil
}

func findFilesByExtension(root, ext string) ([]string, error) {
	fileSemaphore <- struct{}{}
	defer func() {
		<-fileSemaphore
	}()

	var a []string

	useFind := runtime.GOOS == "linux" || runtime.GOOS == "darwin"

	// Check if 'find' is available
	if useFind {
		if _, err := exec.LookPath("find"); err == nil {
			pattern := "*." + ext
			cmd := exec.Command("find", root, "-type", "f", "-name", pattern)

			var out bytes.Buffer

			cmd.Stdout = &out
			if err := cmd.Run(); err != nil {
				return nil, err
			}

			for _, line := range strings.Split(out.String(), "\n") {
				if line != "" {
					a = append(a, line)
				}
			}

			return a, nil
		}
	}

	err := filepath.Walk(root, func(s string, d os.FileInfo, e error) error {
		if e != nil {
			return e
		}

		if filepath.Ext(d.Name()) == ext {
			a = append(a, s)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return a, nil
}
