// Package blob provides blob storage functionality with various storage backend implementations.
// This file contains the factory functions for creating and configuring blob stores with
// different backends and optional wrapper functionality like batching and Delete-At-Height (DAH).
// The factory pattern used here allows for flexible configuration of blob stores through URL parameters,
// enabling runtime selection of storage backends and features.
package blob

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blob/batcher"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/http"
	"github.com/bsv-blockchain/teranode/stores/blob/localdah"
	storelogger "github.com/bsv-blockchain/teranode/stores/blob/logger"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/s3"
	"github.com/bsv-blockchain/teranode/ulogger"
)

var (
	_ Store = (*batcher.Batcher)(nil)
	_ Store = (*file.File)(nil)
	_ Store = (*http.HTTPStore)(nil)
	_ Store = (*localdah.LocalDAH)(nil)
	_ Store = (*memory.Memory)(nil)
	_ Store = (*null.Null)(nil)
	_ Store = (*s3.S3)(nil)
	_ Store = (*storelogger.Logger)(nil)
)

// NewStore creates a new blob store based on the provided URL scheme and options.
// It supports various storage backends including null, memory, file, http, and s3.
// Parameters:
//   - logger: Logger instance for store operations
//   - storeURL: URL containing the store configuration
//   - opts: Optional store configuration options
//
// Returns:
//   - Store: The configured blob store instance
//   - error: Any error that occurred during store creation
func NewStore(logger ulogger.Logger, storeURL *url.URL, opts ...options.StoreOption) (store Store, err error) {
	switch storeURL.Scheme {
	case "null":
		// Prevent null stores from using DAH functionality
		if storeURL.Query().Get("localDAHStore") != "" {
			return nil, errors.NewStorageError("null store does not support DAH functionality")
		}

		store, err = null.New(logger)
		if err != nil {
			return nil, errors.NewStorageError("error creating null blob store", err)
		}
	case "memory":
		store = memory.New(opts...)

	case "file":
		store, err = file.New(logger, storeURL, opts...)
		if err != nil {
			return nil, errors.NewStorageError("error creating file blob store", err)
		}
	case "http":
		store, err = http.New(logger, storeURL, opts...)
		if err != nil {
			return nil, errors.NewStorageError("error creating http blob store", err)
		}
	case "s3":
		store, err = s3.New(logger, storeURL, opts...)
		if err != nil {
			return nil, errors.NewStorageError("error creating s3 blob store", err)
		}
	default:
		return nil, errors.NewStorageError("unknown store type: %s", storeURL.Scheme)
	}

	if storeURL.Query().Get("batch") == "true" {
		store, err = createBatchedStore(storeURL, store, logger)
		if err != nil {
			return nil, errors.NewStorageError("error creating batch blob store", err)
		}
	}

	if storeURL.Query().Get("localDAHStore") != "" {
		store, err = createDAHStore(storeURL, logger, opts, store)
		if err != nil {
			return nil, errors.NewStorageError("error creating local DAH blob store", err)
		}
	}

	if storeURL.Query().Get("logger") == "true" {
		logger.Infof("enabling blob store logging at DEBUG level")
		store = storelogger.New(logger, store)
	}

	return
}

// createDAHStore wraps a store with Delete-At-Height (DAH) capabilities using a local cache.
// DAH functionality allows blobs to be automatically deleted when a specific blockchain
// height is reached. This implementation uses a local cache to track the DAH values
// for blobs in the underlying store.
//
// The DAH implementation supports various storage backends for the cache itself, specified
// via the localDAHStore query parameter in the URL (e.g., ?localDAHStore=memory://)
//
// Parameters:
//   - storeURL: URL containing DAH store configuration and parameters
//   - logger: Logger instance for DAH operations and error reporting
//   - opts: Store options to be passed to the DAH store and cache store
//   - store: The base store to wrap with DAH functionality
//
// Returns:
//   - Store: The DAH-enabled store instance
//   - error: Any error that occurred during creation, particularly if the local DAH cache
//     cannot be initialized
func createDAHStore(storeURL *url.URL, logger ulogger.Logger, opts []options.StoreOption, store Store) (Store, error) {
	localDAHStorePath := storeURL.Query().Get("localDAHStorePath")

	if len(localDAHStorePath) == 0 {
		localDAHStorePath = "/tmp/localDAH"
	} else if localDAHStorePath[0] != '/' && !strings.HasPrefix(localDAHStorePath, "./") {
		localDAHStorePath = "./" + localDAHStorePath
	}

	u, err := url.Parse("file://" + localDAHStorePath)
	if err != nil {
		return nil, errors.NewStorageError("failed to parse localDAHStorePath", err)
	}

	var dahStore Store

	dahStore, err = file.New(logger, u, opts...)
	if err != nil {
		return nil, errors.NewStorageError("failed to create file store", err)
	}

	store, err = localdah.New(logger.New("localDAH"), dahStore, store)
	if err != nil {
		return nil, errors.NewStorageError("failed to create localDAH store", err)
	}

	return store, nil
}

// createBatchedStore wraps a store with batching capabilities for improved performance.
// Batching allows multiple blob operations to be processed as a group, which can
// significantly improve throughput and reduce overhead, especially for storage backends
// with high per-operation costs like network or disk I/O.
//
// The batcher is configured through URL query parameters:
//   - batchSize: Maximum number of operations in a batch (default: 1000)
//   - batchInterval: Maximum time in milliseconds before a batch is processed (default: 100ms)
//   - batchWorkers: Number of worker goroutines for processing batches (default: 10)
//
// Parameters:
//   - storeURL: URL containing batch configuration parameters
//   - store: The base store to wrap with batching capabilities
//   - logger: Logger instance for batch operations, monitoring, and error reporting
//
// Returns:
//   - Store: The batched store instance with configured batch parameters
//   - error: Any error that occurred during creation, particularly if the batcher
//     cannot be properly configured with the provided parameters
func createBatchedStore(storeURL *url.URL, store Store, logger ulogger.Logger) (Store, error) {
	sizeInBytes := 4 * 1024 * 1024

	sizeString := storeURL.Query().Get("sizeInBytes")
	if sizeString != "" {
		var err error

		sizeInBytes, err = strconv.Atoi(sizeString)
		if err != nil {
			return nil, errors.NewConfigurationError("error parsing batch size", err)
		}
	}

	writeKeys := false
	if storeURL.Query().Get("writeKeys") == "true" {
		writeKeys = true
	}

	store = batcher.New(logger, store, sizeInBytes, writeKeys)

	return store, nil
}
