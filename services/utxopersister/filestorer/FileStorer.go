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

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/bytesize"
	"github.com/libsv/go-bt"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type FileStorer struct {
	logger         ulogger.Logger
	store          blob.Store
	key            []byte
	extension      string
	writer         *io.PipeWriter
	bufferedWriter *bufio.Writer
	hasher         hash.Hash
	wg             sync.WaitGroup // Add a WaitGroup to manage the goroutine
	mu             sync.Mutex
}

func NewFileStorer(ctx context.Context, logger ulogger.Logger, store blob.Store, key []byte, extension string) *FileStorer {
	hasher := sha256.New()

	reader, writer := io.Pipe()

	utxopersisterBufferSize, _ := gocore.Config().Get("utxoPersister_buffer_size", "4KB")

	bufferSize, err := bytesize.Parse(utxopersisterBufferSize)
	if err != nil {
		logger.Errorf("error parsing utxoPersister_buffer_size %q: %v§", utxopersisterBufferSize, err)

		bufferSize = 4096
	}

	logger.Infof("Using %s buffer for file storer", bufferSize)

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

		// TODO - we actually want the TTL to  be 1 hour and then we set it to 0 after success.  However, we need
		// to investigate whu TTL files are not being removed from the file system.
		if err := store.SetFromReader(ctx, key, bufferedReader, options.WithFileExtension(extension), options.WithTTL(0)); err != nil {
			logger.Errorf("%s", errors.NewStorageError("[BlockPersister] error setting additions reader", err))
		}
	}()

	return fs
}

func (f *FileStorer) Write(b []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.bufferedWriter.Write(b)
}

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
