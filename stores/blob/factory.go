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

// NewStore
// TODO add options to all stores
func NewStore(logger ulogger.Logger, storeUrl *url.URL, opts ...options.Options) (store Store, err error) {
	switch storeUrl.Scheme {
	case "null":
		store, err = null.New(logger)
		if err != nil {
			return nil, errors.NewStorageError("error creating null blob store", err)
		}
	case "memory":
		store = memory.New()

	case "file":
		store, err = file.New(logger, []string{GetPathFromURL(storeUrl)})
		if err != nil {
			return nil, errors.NewStorageError("error creating file blob store", err)
		}
	case "s3":
		store, err = s3.New(logger, storeUrl, opts...)
		if err != nil {
			return nil, errors.NewStorageError("error creating s3 blob store", err)
		}
	case "lustre":
		// storeUrl is an s3 url
		// lustre://s3.com/ubsv?localDir=/data/subtrees&localPersist=s3
		dir := storeUrl.Query().Get("localDir")
		persistDir := storeUrl.Query().Get("localPersist")
		store, err = lustre.New(logger, storeUrl, dir, persistDir)
		if err != nil {
			return nil, errors.NewStorageError("error creating lustre blob store", err)
		}
	default:
		return nil, errors.NewStorageError("unknown store type: %s", storeUrl.Scheme)
	}

	if storeUrl.Query().Get("batch") == "true" {
		sizeInBytes := int64(4 * 1024 * 1024) // 4MB
		sizeString := storeUrl.Query().Get("sizeInBytes")
		if sizeString != "" {
			sizeInBytes, err = strconv.ParseInt(sizeString, 10, 64)
			if err != nil {
				return nil, errors.NewConfigurationError("error parsing batch size", err)
			}
		}

		writeKeys := false
		if storeUrl.Query().Get("writeKeys") == "true" {
			writeKeys = true
		}

		store = batcher.New(logger, store, int(sizeInBytes), writeKeys)
	}

	if storeUrl.Query().Get("localTTLStore") != "" {
		localTTLStorePath := storeUrl.Query().Get("localTTLStorePath")
		if localTTLStorePath == "" {
			localTTLStorePath = "/tmp/localTTL"
		}

		localTTLStorePaths := strings.Split(localTTLStorePath, "|")
		for i, item := range localTTLStorePaths {
			localTTLStorePaths[i] = strings.TrimSpace(item)
		}

		var ttlStore Store
		ttlStore, err = file.New(logger, localTTLStorePaths)
		if err != nil {
			return nil, errors.NewStorageError("failed to create file store", err)
		}
		store, err = localttl.New(logger.New("localTTL"), ttlStore, store)
		if err != nil {
			return nil, errors.NewStorageError("failed to create localTTL store", err)
		}
	}

	return
}

func GetPathFromURL(u *url.URL) string {
	if u.Host == "." {
		return u.Path[1:] // relative path
	}
	return u.Path // absolute path
}
