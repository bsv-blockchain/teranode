package blob

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/bitcoin-sv/ubsv/stores/blob/badger"
	"github.com/bitcoin-sv/ubsv/stores/blob/file"
	"github.com/bitcoin-sv/ubsv/stores/blob/gcs"
	"github.com/bitcoin-sv/ubsv/stores/blob/kinesiss3"
	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/blob/minio"
	"github.com/bitcoin-sv/ubsv/stores/blob/null"
	"github.com/bitcoin-sv/ubsv/stores/blob/s3"
	"github.com/bitcoin-sv/ubsv/stores/blob/sql"
)

func NewStore(storeUrl *url.URL) (store Store, err error) {
	switch storeUrl.Scheme {
	case "null":
		store, err = null.New()
		if err != nil {
			return nil, fmt.Errorf("error creating null block store: %v", err)
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
			return nil, fmt.Errorf("error creating badger block store: %v", err)
		}
	case "postgres", "sqlite", "sqlitememory":
		store, err = sql.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating sql block store: %v", err)
		}
	case "gcs":
		store, err = gcs.New(strings.Replace(storeUrl.Path, "/", "", 1))
		if err != nil {
			return nil, fmt.Errorf("error creating gcs block store: %v", err)
		}
	case "minio":
		fallthrough
	case "minios":
		store, err = minio.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating minio block store: %v", err)
		}
	case "s3":
		store, err = s3.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating s3 block store: %v", err)
		}
	case "kinesiss3":
		store, err = kinesiss3.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating kinesiss3 block store: %v", err)
		}
	default:
		return nil, fmt.Errorf("unknown store type: %s", storeUrl.Scheme)
	}

	return
}
