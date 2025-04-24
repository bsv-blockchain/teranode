// Package blob provides blob storage functionality with various storage backend implementations.
package blob

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/batcher"
	"github.com/bitcoin-sv/teranode/stores/blob/file"
	"github.com/bitcoin-sv/teranode/stores/blob/http"
	"github.com/bitcoin-sv/teranode/stores/blob/localdah"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/null"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/blob/s3"
	"github.com/bitcoin-sv/teranode/ulogger"
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

	return
}

// createDAHStore wraps a store with DAH capabilities using a local cache.
// Parameters:
//   - storeURL: URL containing DAH store configuration
//   - logger: Logger instance for DAH operations
//   - opts: Store options
//   - store: The base store to wrap
//
// Returns:
//   - Store: The DAH-enabled store instance
//   - error: Any error that occurred during creation
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

// createBatchedStore wraps a store with batching capabilities.
// Parameters:
//   - storeURL: URL containing batch configuration
//   - store: The base store to wrap
//   - logger: Logger instance for batch operations
//
// Returns:
//   - Store: The batched store instance
//   - error: Any error that occurred during creation
func createBatchedStore(storeURL *url.URL, store Store, logger ulogger.Logger) (Store, error) {
	sizeInBytes := int64(4 * 1024 * 1024)

	sizeString := storeURL.Query().Get("sizeInBytes")
	if sizeString != "" {
		var err error

		sizeInBytes, err = strconv.ParseInt(sizeString, 10, 64)
		if err != nil {
			return nil, errors.NewConfigurationError("error parsing batch size", err)
		}
	}

	writeKeys := false
	if storeURL.Query().Get("writeKeys") == "true" {
		writeKeys = true
	}

	store = batcher.New(logger, store, int(sizeInBytes), writeKeys)

	return store, nil
}
