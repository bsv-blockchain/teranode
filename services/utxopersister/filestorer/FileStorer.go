// Package filestorer provides specialized file storage functionality for the UTXO Persister service.
// It offers a high-level interface for efficiently persisting UTXO data to blob storage with buffering.
// This package is designed to support the storage requirements for UTXO set files, additions, and
// deletions in the Teranode blockchain.
package filestorer

import (
	"bufio"
	"context"
	"io"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/bytesize"
	"github.com/ordishs/go-utils"
)

// FileStorer handles the storage and management of blockchain-related files.
// It provides buffered writing capabilities for efficient I/O operations.
// FileStorer streams data through a pipe to the underlying blob storage,
// which handles temp file creation and atomic rename internally.
type FileStorer struct {
	// logger provides logging functionality
	logger ulogger.Logger

	// store represents the underlying blob storage
	store blob.Store

	// key represents the unique identifier for the file
	key []byte

	// fileType represents the file type
	fileType fileformat.FileType

	// writer is the pipe writer for streaming data to storage
	writer *io.PipeWriter

	// bufferedWriter provides buffered writing capabilities
	bufferedWriter *bufio.Writer

	// wg tracks the background goroutine
	wg sync.WaitGroup

	// mu protects readerError and bufferedWriter writes
	mu sync.Mutex

	// done signals when the background goroutine completes
	done chan struct{}

	// readerError stores any error from the background reader
	readerError error

	// fileOptions contains options for the file operation
	fileOptions []options.FileOption

	// ctx is the context for the operation
	ctx context.Context
}

// NewFileStorer creates a new FileStorer instance with the provided parameters.
// It creates a pipe and spawns a background goroutine that streams data to the blob storage.
// The blob storage (SetFromReader) handles temp file creation and atomic rename internally.
// Returns a pointer to the initialized FileStorer ready for use.
func NewFileStorer(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, key []byte, fileType fileformat.FileType, fileOptions ...options.FileOption) (*FileStorer, error) {
	exists, err := store.Exists(ctx, key, fileType, fileOptions...)
	if err != nil {
		return nil, errors.NewStorageError("error checking if %s.%s exists", key, fileType, err)
	}

	if exists {
		return nil, errors.NewBlobAlreadyExistsError("%s.%s already exists", utils.ReverseAndHexEncodeSlice(key), fileType)
	}

	utxopersisterBufferSize := tSettings.Block.UTXOPersisterBufferSize

	bufferSize, err := bytesize.Parse(utxopersisterBufferSize)
	if err != nil {
		logger.Errorf("error parsing utxoPersister_buffer_size %q: %v", utxopersisterBufferSize, err)

		bufferSize = 1024 * 128 // default to 128KB
	}

	logger.Debugf("Using %s buffer for file storer", bufferSize)

	// Create pipe for streaming data to blob storage
	reader, writer := io.Pipe()

	// Create buffered writer for efficient I/O
	bufferedWriter := bufio.NewWriterSize(writer, bufferSize.Int())

	fs := &FileStorer{
		logger:         logger,
		store:          store,
		key:            key,
		fileType:       fileType,
		writer:         writer,
		bufferedWriter: bufferedWriter,
		done:           make(chan struct{}),
		fileOptions:    fileOptions,
		ctx:            ctx,
	}

	// Start background goroutine to stream data to blob storage
	fs.wg.Add(1)
	go func() {
		defer fs.wg.Done()
		defer close(fs.done)

		// SetFromReader will create its own temp file and handle atomic rename
		// Note: Buffered reader is NOT used on the read side despite having a buffered writer
		// on the write side. This is intentional - adding buffering here causes test hangs
		// when SetFromReader returns errors without consuming the pipe, as the buffered
		// reader's interaction with the pipe can create deadlocks in error scenarios.
		err := store.SetFromReader(ctx, key, fileType, reader, fileOptions...)
		if err != nil {
			fs.mu.Lock()
			fs.readerError = err
			fs.mu.Unlock()
		}
	}()

	return fs, nil
}

// Write writes the provided bytes to the pipe via the buffered writer.
// Returns the number of bytes written and any error encountered.
// If the background reader has encountered an error, it will be returned here.
// This method is safe for concurrent use.
func (f *FileStorer) Write(b []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.readerError != nil {
		return 0, f.readerError
	}

	return f.bufferedWriter.Write(b)
}

// Close finalizes the file storage operation.
// It flushes the buffer, closes the pipe writer, waits for the background goroutine to complete,
// and sets the DAH for the file.
// Returns any error encountered during the closing process.
func (f *FileStorer) Close(ctx context.Context) error {
	// Flush the buffered writer to ensure all data is written to the pipe
	f.mu.Lock()
	flushErr := f.bufferedWriter.Flush()
	f.mu.Unlock()

	if flushErr != nil {
		_ = f.writer.Close()
		return errors.NewStorageError("Error flushing writer", flushErr)
	}

	// Close the pipe writer to signal EOF to the reader
	if err := f.writer.Close(); err != nil {
		return errors.NewStorageError("Error closing writer", err)
	}

	// Wait for the background goroutine to complete
	f.wg.Wait()

	// Check if the background reader encountered an error
	f.mu.Lock()
	readerErr := f.readerError
	f.mu.Unlock()

	if readerErr != nil {
		return errors.NewStorageError("Error in reader goroutine", readerErr)
	}

	// Set DAH to 0 (no expiration) as per the memory about Aerospike DAH usage
	if err := f.store.SetDAH(ctx, f.key, f.fileType, 0); err != nil {
		return errors.NewStorageError("Error setting DAH on file", err)
	}

	if err := f.waitUntilFileIsAvailable(ctx); err != nil {
		f.logger.Warnf("Error waiting for file to be available: %v", err)
	}

	return nil
}

// Abort signals that the write operation should be abandoned and the temp file should not be finalized.
// It closes the pipe writer with an error, causing SetFromReader to receive an error during io.Copy,
// which triggers temp file cleanup in the blob store.
// This should be called instead of Close() when an error occurs during writing.
// It is safe to call Abort() multiple times or after Close().
func (f *FileStorer) Abort(err error) {
	if err == nil {
		err = errors.NewProcessingError("write operation aborted")
	}

	// Close the pipe with an error to signal the background goroutine to abort
	// This will cause io.Copy in SetFromReader to return this error,
	// which triggers temp file cleanup
	_ = f.writer.CloseWithError(err)

	// Wait for the background goroutine to complete
	f.wg.Wait()
}

// waitUntilFileIsAvailable waits for the file to become available in storage.
// It polls the storage system to check if the file exists, retrying multiple times
// with a fixed interval between attempts.
// Returns an error if the file doesn't become available within the maximum number of retries.
// It returns an error if the file doesn't become available within the timeout period.
func (f *FileStorer) waitUntilFileIsAvailable(ctx context.Context) error {
	maxRetries := 10
	retryInterval := 100 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		exists, err := f.store.Exists(ctx, f.key, f.fileType)
		if err != nil {
			return err
		}

		if exists {
			return nil
		}

		time.Sleep(retryInterval)
	}

	return errors.NewStorageError("file %s.%s is not available", utils.ReverseAndHexEncodeSlice(f.key), f.fileType)
}
