package blob

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/badger"
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
			return nil, fmt.Errorf("error creating null blob store: %v", err)
		}
	case "memory":
		store = memory.New()

	case "file":
		// TODO make this more generic, you should be able to pass in an absolute path
		store, err = file.New(logger, "."+storeUrl.Path) // relative
		if err != nil {
			return nil, fmt.Errorf("error creating file blob store: %v", err)
		}
	case "badger":
		// TODO make this more generic, you should be able to pass in an absolute path
		store, err = badger.New(logger, "."+storeUrl.Path) // relative
		if err != nil {
			return nil, fmt.Errorf("error creating badger blob store: %v", err)
		}
	case "s3":
		store, err = s3.New(logger, storeUrl, opts...)
		if err != nil {
			return nil, fmt.Errorf("error creating s3 blob store: %v", err)
		}
	case "lustre":
		// storeUrl is an s3 url
		// lustre://s3.com/ubsv?localDir=/data/subtrees&localPersist=s3
		dir := storeUrl.Query().Get("localDir")
		persistDir := storeUrl.Query().Get("localPersist")
		store, err = lustre.New(logger, storeUrl, dir, persistDir)
		if err != nil {
			return nil, fmt.Errorf("error creating lustre blob store: %v", err)
		}
	default:
		return nil, fmt.Errorf("unknown store type: %s", storeUrl.Scheme)
	}

	if storeUrl.Query().Get("batch") == "true" {
		sizeInBytes := int64(4 * 1024 * 1024) // 4MB
		sizeString := storeUrl.Query().Get("sizeInBytes")
		if sizeString != "" {
			sizeInBytes, err = strconv.ParseInt(sizeString, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing batch size: %v", err)
			}
		}

		writeKeys := false
		if storeUrl.Query().Get("writeKeys") == "true" {
			writeKeys = true
		}

		store = batcher.New(logger, store, int(sizeInBytes), writeKeys)
	}

	if storeUrl.Query().Get("localTTLStore") != "" {
		ttlStoreType := storeUrl.Query().Get("localTTLStore")
		localTTLStorePath := storeUrl.Query().Get("localTTLStorePath")
		if localTTLStorePath == "" {
			localTTLStorePath = "/tmp/localTTL"
		}

		localTTLStorePaths := strings.Split(localTTLStorePath, "|")
		for i, item := range localTTLStorePaths {
			localTTLStorePaths[i] = strings.TrimSpace(item)
		}

		var ttlStore Store
		if ttlStoreType == "badger" {
			if len(localTTLStorePaths) > 1 {
				return nil, errors.New(errors.ERR_INVALID_ARGUMENT, "badger store only supports one path")
			}
			ttlStore, err = badger.New(logger, localTTLStorePath)
			if err != nil {
				return nil, errors.New(errors.ERR_STORAGE_ERROR, "failed to create badger store", err)
			}
		} else {
			// default is file store
			ttlStore, err = file.New(logger, localTTLStorePath, localTTLStorePaths)
			if err != nil {
				return nil, errors.New(errors.ERR_STORAGE_ERROR, "failed to create file store", err)
			}
		}

		store, err = localttl.New(logger.New("localTTL"), ttlStore, store)
		if err != nil {
			return nil, errors.New(errors.ERR_STORAGE_ERROR, "failed to create localTTL store", err)
		}
	}

	return
}
