// Package filestorer provides specialized file storage functionality for the UTXO Persister service.
// It offers a high-level interface for efficiently persisting UTXO data to blob storage with buffering,
// hashing, and asynchronous writing capabilities. This package is designed to support the storage
// requirements for UTXO set files, additions, and deletions in the Teranode blockchain.
package filestorer

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/bytesize"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
)

// FileStorer handles the storage and management of blockchain-related files.
// It provides buffered writing with concurrent processing, automatic hashing,
// and verification capabilities. FileStorer abstracts the complexity of
// interacting with the underlying blob storage system.
type FileStorer struct {
	// logger provides logging functionality
	logger ulogger.Logger

	// store represents the underlying blob storage
	store blob.Store

	// key represents the unique identifier for the file
	key []byte

	// extension represents the file extension
	extension options.FileExtension

	// writer is the underlying pipe writer
	writer *io.PipeWriter

	// bufferedWriter provides buffered writing capabilities
	bufferedWriter *bufio.Writer

	// hasher provides hashing functionality
	hasher hash.Hash

	// wg manages goroutine synchronization
	wg sync.WaitGroup

	// mu provides mutex locking for thread safety
	mu sync.Mutex

	// done is a channel that signals when the reader goroutine is done
	done chan struct{}

	// readerError stores any error encountered by the reader goroutine
	readerError error
}

// NewFileStorer creates a new FileStorer instance with the provided parameters.
// It sets up an efficient pipeline for writing data with buffering and hashing.
// The function initiates a background goroutine that reads from a pipe and writes to blob storage.
// Returns a pointer to the initialized FileStorer ready for use.
// It initializes the file storage system with buffering and hashing capabilities.
func NewFileStorer(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, key []byte, extension options.FileExtension) (*FileStorer, error) {
	exists, err := store.Exists(ctx, key, options.WithFileExtension(extension))
	if err != nil {
		return nil, errors.NewStorageError("error checking if %s.%s exists", key, extension, err)
	}

	if exists {
		return nil, errors.NewBlobAlreadyExistsError("%s.%s already exists", key, extension)
	}

	utxopersisterBufferSize := tSettings.Block.UTXOPersisterBufferSize

	bufferSize, err := bytesize.Parse(utxopersisterBufferSize)
	if err != nil {
		logger.Errorf("error parsing utxoPersister_buffer_size %q: %v§", utxopersisterBufferSize, err)

		bufferSize = 4096
	}

	logger.Infof("Using %s buffer for file storer", bufferSize)

	// Note that the reader will close when the writer closes and vice versa.
	reader, writer := io.Pipe()

	bufferedReader := io.NopCloser(bufio.NewReaderSize(reader, bufferSize.Int()))

	hasher := sha256.New()
	bufferedWriter := bufio.NewWriterSize(io.MultiWriter(writer, hasher), bufferSize.Int())

	fs := &FileStorer{
		logger:         logger,
		store:          store,
		key:            key,
		extension:      extension,
		hasher:         hasher,
		writer:         writer,
		bufferedWriter: bufferedWriter,
		done:           make(chan struct{}),
	}

	fs.wg.Add(1) // Increment the WaitGroup counter

	go func() {
		defer func() {
			close(fs.done) // Signal that the goroutine is done
			fs.wg.Done()   // Decrement the WaitGroup counter
		}()

		err := store.SetFromReader(ctx, key, bufferedReader, options.WithFileExtension(extension), options.WithDeleteAt(0))
		if err != nil {
			logger.Errorf("%s", errors.NewStorageError("[BlockPersister] error setting additions reader", err))
			fs.mu.Lock()
			fs.readerError = err
			fs.mu.Unlock()
		}

		// Close the reader after we're done with it
		if err := reader.Close(); err != nil {
			logger.Errorf("Failed to close reader: %v", err)
		}
	}()

	return fs, nil
}

// Write writes the provided bytes to the file storage.
// It ensures thread-safety with mutex locking and writes to both the buffered writer
// and the hasher simultaneously through a MultiWriter.
// Returns the number of bytes written and any error encountered.
// It returns the number of bytes written and any error encountered.
func (f *FileStorer) Write(b []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.readerError != nil {
		return 0, f.readerError
	}

	return f.bufferedWriter.Write(b)
}

// Close finalizes the file storage operation and ensures all data is written.
// It flushes the buffer, closes the writer, waits for the background goroutine to complete,
// sets the DAH for the file, and creates a SHA256 checksum file.
// Returns any error encountered during the closing process.
// It returns any error encountered during the closing process.
func (f *FileStorer) Close(ctx context.Context) error {
	// Flush the buffered writer
	if err := f.bufferedWriter.Flush(); err != nil {
		// Even if flush fails, we need to close the pipe writer to prevent deadlocks
		_ = f.writer.Close()

		// Wait for the goroutine to finish
		f.wg.Wait()

		// Check if the reader encountered an error
		f.mu.Lock()
		readerErr := f.readerError
		f.mu.Unlock()

		if readerErr != nil {
			return errors.NewStorageError("Error in reader goroutine", readerErr)
		}

		return errors.NewStorageError("Error flushing writer", err)
	}

	// Close the pipe writer to signal EOF to the reader
	if err := f.writer.Close(); err != nil {
		return errors.NewStorageError("Error closing writer", err)
	}

	// Wait for the goroutine to finish
	f.wg.Wait()

	// Check if the reader encountered an error
	f.mu.Lock()
	readerErr := f.readerError
	f.mu.Unlock()

	if readerErr != nil {
		return errors.NewStorageError("Error in reader goroutine", readerErr)
	}

	// Set DAH to 0 (no expiration) as per the memory about Aerospike DAH usage
	if err := f.store.SetDAH(ctx, f.key, 0, options.WithFileExtension(f.extension)); err != nil {
		return errors.NewStorageError("Error setting DAH on additions file", err)
	}

	if err := f.waitUntilFileIsAvailable(ctx); err != nil {
		f.logger.Warnf("Error waiting for file to be available: %v", err)
	}

	hashData := fmt.Sprintf("%x  %x.%s\n", f.hasher.Sum(nil), bt.ReverseBytes(f.key), f.extension) // N.B. The 2 spaces is important for the hash to be valid

	if err := f.store.Set(
		ctx,
		f.key,
		[]byte(hashData),
		options.WithFileExtension(f.extension+".sha256"),
		options.WithDeleteAt(0),
		options.WithAllowOverwrite(true),
	); err != nil {
		return errors.NewStorageError("error setting sha256 hash", err)
	}

	return nil
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
		exists, err := f.store.Exists(ctx, f.key, options.WithFileExtension(f.extension))
		if err != nil {
			return err
		}

		if exists {
			return nil
		}

		time.Sleep(retryInterval)
	}

	return errors.NewStorageError("file %s.%s is not available", utils.ReverseAndHexEncodeSlice(f.key), f.extension)
}
