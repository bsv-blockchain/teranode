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
	extension string

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
}

// NewFileStorer creates a new FileStorer instance with the provided parameters.
// It sets up an efficient pipeline for writing data with buffering and hashing.
// The function initiates a background goroutine that reads from a pipe and writes to blob storage.
// Returns a pointer to the initialized FileStorer ready for use.
// It initializes the file storage system with buffering and hashing capabilities.
func NewFileStorer(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, key []byte, extension string) *FileStorer {
	utxopersisterBufferSize := tSettings.Block.UTXOPersisterBufferSize

	bufferSize, err := bytesize.Parse(utxopersisterBufferSize)
	if err != nil {
		logger.Errorf("error parsing utxoPersister_buffer_size %q: %v§", utxopersisterBufferSize, err)

		bufferSize = 4096
	}

	logger.Infof("Using %s buffer for file storer", bufferSize)

	reader, writer := io.Pipe()
	hasher := sha256.New()

	bufferedReader := io.NopCloser(bufio.NewReaderSize(reader, bufferSize.Int()))
	bufferedWriter := bufio.NewWriterSize(io.MultiWriter(writer, hasher), bufferSize.Int())

	fs := &FileStorer{
		logger:         logger,
		store:          store,
		key:            key,
		extension:      extension,
		hasher:         hasher,
		writer:         writer,
		bufferedWriter: bufferedWriter,
	}

	fs.wg.Add(1) // Increment the WaitGroup counter

	go func() {
		defer func() {
			if err := reader.Close(); err != nil {
				logger.Errorf("Failed to close reader: %v", err)
			}
			// logger.Infof("Closed reader")
			fs.wg.Done() // Decrement the WaitGroup counter
		}()

		if err := store.SetFromReader(ctx, key, bufferedReader, options.WithFileExtension(extension), options.WithTTL(0)); err != nil {
			if errors.Is(err, errors.ErrBlobAlreadyExists) {
				logger.Warnf("[BlockPersister] File already exists: %v", err)
			} else {
				logger.Errorf("%s", errors.NewStorageError("[BlockPersister] error setting additions reader", err))
			}
		}
	}()

	return fs
}

// Write writes the provided bytes to the file storage.
// It ensures thread-safety with mutex locking and writes to both the buffered writer
// and the hasher simultaneously through a MultiWriter.
// Returns the number of bytes written and any error encountered.
// It returns the number of bytes written and any error encountered.
func (f *FileStorer) Write(b []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.bufferedWriter.Write(b)
}

// Close finalizes the file storage operation and ensures all data is written.
// It flushes the buffer, closes the writer, waits for the background goroutine to complete,
// sets the TTL for the file, and creates a SHA256 checksum file.
// Returns any error encountered during the closing process.
// It returns any error encountered during the closing process.
func (f *FileStorer) Close(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.bufferedWriter.Flush(); err != nil {
		return errors.NewStorageError("Error flushing writer", err)
	}

	// f.logger.Infof("Closed buffered writer")

	if err := f.writer.Close(); err != nil {
		return errors.NewStorageError("Error closing writer", err)
	}

	// f.logger.Infof("Closed underlying writer")

	f.wg.Wait() // Wait for the goroutine to finish

	// f.logger.Infof("Wait group finished")

	if err := f.store.SetTTL(ctx, f.key, 0, options.WithFileExtension(f.extension)); err != nil {
		return errors.NewStorageError("Error setting ttl on additions file", err)
	}

	// f.logger.Infof("Set TTL to 0")

	if err := f.waitUntilFileIsAvailable(ctx, f.extension); err != nil {
		f.logger.Warnf("Error waiting for file to be available: %v", err)
	}

	// f.logger.Infof("File is available")

	hashData := fmt.Sprintf("%x  %x.%s\n", f.hasher.Sum(nil), bt.ReverseBytes(f.key), f.extension) // N.B. The 2 spaces is important for the hash to be valid

	if err := f.store.Set(
		ctx,
		f.key,
		[]byte(hashData),
		options.WithFileExtension(f.extension+".sha256"),
		options.WithTTL(0),
		options.WithAllowOverwrite(true),
	); err != nil {
		return errors.NewStorageError("error setting sha256 hash", err)
	}

	// f.logger.Infof("Set sha256 hash")

	return nil
}

// waitUntilFileIsAvailable waits for the file to become available in storage.
// It polls the storage system to check if the file exists, retrying multiple times
// with a fixed interval between attempts.
// Returns an error if the file doesn't become available within the maximum number of retries.
// It returns an error if the file doesn't become available within the timeout period.
func (f *FileStorer) waitUntilFileIsAvailable(ctx context.Context, extension string) error {
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

	return errors.NewStorageError("file %s.%s is not available", utils.ReverseAndHexEncodeSlice(f.key), extension)
}
