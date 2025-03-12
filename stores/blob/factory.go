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
	"github.com/bitcoin-sv/teranode/stores/blob/localttl"
	"github.com/bitcoin-sv/teranode/stores/blob/lustre"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/null"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/blob/s3"
	"github.com/bitcoin-sv/teranode/ulogger"
)

// NewStore creates a new blob store based on the provided URL scheme and options.
// It supports various storage backends including null, memory, file, http, s3, and lustre.
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
		// Prevent null stores from using TTL functionality
		if storeURL.Query().Get("localTTLStore") != "" {
			return nil, errors.NewStorageError("null store does not support TTL functionality")
		}

		store, err = null.New(logger)
		if err != nil {
			return nil, errors.NewStorageError("error creating null blob store", err)
		}
	case "memory":
		store = memory.New()

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
	case "lustre":
		// storeUrl is an s3 url
		// lustre://s3.com/teranode?localDir=/data/subtrees&localPersist=s3
		dir := storeURL.Query().Get("localDir")
		persistDir := storeURL.Query().Get("localPersist")

		store, err = lustre.New(logger, storeURL, dir, persistDir, opts...)
		if err != nil {
			return nil, errors.NewStorageError("error creating lustre blob store", err)
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

	if storeURL.Query().Get("localTTLStore") != "" {
		store, err = createTTLStore(storeURL, logger, opts, store)
		if err != nil {
			return nil, errors.NewStorageError("error creating local TTL blob store", err)
		}
	}

	return
}

// createTTLStore wraps a store with TTL capabilities using a local cache.
// Parameters:
//   - storeURL: URL containing TTL store configuration
//   - logger: Logger instance for TTL operations
//   - opts: Store options
//   - store: The base store to wrap
//
// Returns:
//   - Store: The TTL-enabled store instance
//   - error: Any error that occurred during creation
func createTTLStore(storeURL *url.URL, logger ulogger.Logger, opts []options.StoreOption, store Store) (Store, error) {
	localTTLStorePath := storeURL.Query().Get("localTTLStorePath")

	if len(localTTLStorePath) == 0 {
		localTTLStorePath = "/tmp/localTTL"
	} else if localTTLStorePath[0] != '/' && !strings.HasPrefix(localTTLStorePath, "./") {
		localTTLStorePath = "./" + localTTLStorePath
	}

	u, err := url.Parse("file://" + localTTLStorePath)
	if err != nil {
		return nil, errors.NewStorageError("failed to parse localTTLStorePath", err)
	}

	var ttlStore Store

	ttlStore, err = file.New(logger, u, opts...)
	if err != nil {
		return nil, errors.NewStorageError("failed to create file store", err)
	}

	store, err = localttl.New(logger.New("localTTL"), ttlStore, store)
	if err != nil {
		return nil, errors.NewStorageError("failed to create localTTL store", err)
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
