package blob

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/stores/blob/badger"
	"github.com/bitcoin-sv/ubsv/stores/blob/batcher"
	"github.com/bitcoin-sv/ubsv/stores/blob/file"
	"github.com/bitcoin-sv/ubsv/stores/blob/gcs"
	"github.com/bitcoin-sv/ubsv/stores/blob/kinesiss3"
	"github.com/bitcoin-sv/ubsv/stores/blob/localttl"
	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/blob/minio"
	"github.com/bitcoin-sv/ubsv/stores/blob/null"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/blob/s3"
	"github.com/bitcoin-sv/ubsv/stores/blob/seaweedfs"
	"github.com/bitcoin-sv/ubsv/stores/blob/seaweedfss3"
	"github.com/bitcoin-sv/ubsv/stores/blob/sql"
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
		store, err = file.New(logger, "."+storeUrl.Path) // relative
		if err != nil {
			return nil, fmt.Errorf("error creating file blob store: %v", err)
		}
	case "badger":
		store, err = badger.New(logger, "."+storeUrl.Path) // relative
		if err != nil {
			return nil, fmt.Errorf("error creating badger blob store: %v", err)
		}
	case "postgres", "sqlite", "sqlitememory":
		store, err = sql.New(logger, storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating sql blob store: %v", err)
		}
	case "gcs":
		store, err = gcs.New(logger, strings.Replace(storeUrl.Path, "/", "", 1))
		if err != nil {
			return nil, fmt.Errorf("error creating gcs blob store: %v", err)
		}
	case "minio":
		fallthrough
	case "minios":
		store, err = minio.New(logger, storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating minio blob store: %v", err)
		}
	case "s3":
		store, err = s3.New(logger, storeUrl, opts...)
		if err != nil {
			return nil, fmt.Errorf("error creating s3 blob store: %v", err)
		}
	case "kinesiss3":
		store, err = kinesiss3.New(logger, storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating kinesiss3 blob store: %v", err)
		}
	case "seaweedfs":
		store, err = seaweedfs.New(logger, storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating seaweedfs blob store: %v", err)
		}
	case "seaweedfss3":
		store, err = seaweedfss3.New(logger, storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating seaweedfss3 blob store: %v", err)
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
				return nil, errors.New("badger store only supports one path")
			}
			ttlStore, err = badger.New(logger, localTTLStorePath)
			if err != nil {
				return nil, errors.Join(errors.New("failed to create badger store"), err)
			}
		} else {
			// default is file store
			ttlStore, err = file.New(logger, localTTLStorePath, localTTLStorePaths)
			if err != nil {
				return nil, errors.Join(errors.New("failed to create file store"), err)
			}
		}

		store, err = localttl.New(logger.New("localTTL"), ttlStore, store)
		if err != nil {
			return nil, errors.Join(errors.New("failed to create localTTL store"), err)
		}
	}

	return
}
