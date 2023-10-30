package blob

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/stores/blob/badger"
	"github.com/bitcoin-sv/ubsv/stores/blob/batcher"
	"github.com/bitcoin-sv/ubsv/stores/blob/file"
	"github.com/bitcoin-sv/ubsv/stores/blob/gcs"
	"github.com/bitcoin-sv/ubsv/stores/blob/kinesiss3"
	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/blob/minio"
	"github.com/bitcoin-sv/ubsv/stores/blob/null"
	"github.com/bitcoin-sv/ubsv/stores/blob/s3"
	"github.com/bitcoin-sv/ubsv/stores/blob/seaweedfs"
	"github.com/bitcoin-sv/ubsv/stores/blob/seaweedfss3"
	"github.com/bitcoin-sv/ubsv/stores/blob/sql"
	"github.com/ordishs/gocore"
)

func NewStore(storeUrl *url.URL) (store Store, err error) {
	switch storeUrl.Scheme {
	case "null":
		store, err = null.New()
		if err != nil {
			return nil, fmt.Errorf("error creating null blob store: %v", err)
		}
	case "memory":
		store = memory.New()
	case "file":
		store, err = file.New("." + storeUrl.Path) // relative
		if err != nil {
			return nil, fmt.Errorf("error creating file blob store: %v", err)
		}
	case "badger":
		store, err = badger.New("." + storeUrl.Path) // relative
		if err != nil {
			return nil, fmt.Errorf("error creating badger blob store: %v", err)
		}
	case "postgres", "sqlite", "sqlitememory":
		store, err = sql.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating sql blob store: %v", err)
		}
	case "gcs":
		store, err = gcs.New(strings.Replace(storeUrl.Path, "/", "", 1))
		if err != nil {
			return nil, fmt.Errorf("error creating gcs blob store: %v", err)
		}
	case "minio":
		fallthrough
	case "minios":
		store, err = minio.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating minio blob store: %v", err)
		}
	case "s3":
		store, err = s3.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating s3 blob store: %v", err)
		}
	case "kinesiss3":
		store, err = kinesiss3.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating kinesiss3 blob store: %v", err)
		}
	case "seaweedfs":
		store, err = seaweedfs.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating seaweedfs blob store: %v", err)
		}
	case "seaweedfss3":
		store, err = seaweedfss3.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating seaweedfss3 blob store: %v", err)
		}
	default:
		return nil, fmt.Errorf("unknown store type: %s", storeUrl.Scheme)
	}

	if storeUrl.Query().Get("batch") == "true" {
		logger := gocore.Log("batcher")

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

	return
}
