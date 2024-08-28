package blob

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/batcher"
	"github.com/bitcoin-sv/ubsv/stores/blob/file"
	"github.com/bitcoin-sv/ubsv/stores/blob/localttl"
	"github.com/bitcoin-sv/ubsv/stores/blob/lustre"
	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/blob/null"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/blob/s3"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func NewStore(logger ulogger.Logger, storeURL *url.URL, opts ...options.Options) (store Store, err error) {
	switch storeURL.Scheme {
	case "null":
		store, err = null.New(logger)
		if err != nil {
			return nil, errors.NewStorageError("error creating null blob store", err)
		}
	case "memory":
		store = memory.New()

	case "file":
		store, err = file.New(logger, []string{GetPathFromURL(storeURL)}, opts...)
		if err != nil {
			return nil, errors.NewStorageError("error creating file blob store", err)
		}
	case "s3":
		store, err = s3.New(logger, storeURL, opts...)
		if err != nil {
			return nil, errors.NewStorageError("error creating s3 blob store", err)
		}
	case "lustre":
		// storeUrl is an s3 url
		// lustre://s3.com/ubsv?localDir=/data/subtrees&localPersist=s3
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
		store, err = createBatchedStore(storeURL, err, store, logger)
		if err != nil {
			return nil, errors.NewStorageError("error creating batch blob store", err)
		}
	}

	if storeURL.Query().Get("localTTLStore") != "" {
		store, err = createTTLStore(storeURL, err, logger, opts, store)
		if err != nil {
			return nil, errors.NewStorageError("error creating local TTL blob store", err)
		}
	}

	return
}

func createTTLStore(storeURL *url.URL, err error, logger ulogger.Logger, opts []options.Options, store Store) (Store, error) {
	localTTLStorePath := storeURL.Query().Get("localTTLStorePath")
	if localTTLStorePath == "" {
		localTTLStorePath = "/tmp/localTTL"
	}

	localTTLStorePaths := strings.Split(localTTLStorePath, "|")
	for i, item := range localTTLStorePaths {
		localTTLStorePaths[i] = strings.TrimSpace(item)
	}

	var ttlStore Store

	ttlStore, err = file.New(logger, localTTLStorePaths, opts...)
	if err != nil {
		return nil, errors.NewStorageError("failed to create file store", err)
	}

	store, err = localttl.New(logger.New("localTTL"), ttlStore, store)
	if err != nil {
		return nil, errors.NewStorageError("failed to create localTTL store", err)
	}
	return store, nil
}

func createBatchedStore(storeURL *url.URL, err error, store Store, logger ulogger.Logger) (Store, error) {
	sizeInBytes := int64(4 * 1024 * 1024)

	sizeString := storeURL.Query().Get("sizeInBytes")
	if sizeString != "" {
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

func GetPathFromURL(u *url.URL) string {
	if u.Host == "." {
		return u.Path[1:] // relative path
	}

	return u.Path // absolute path
}
