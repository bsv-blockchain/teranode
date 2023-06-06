package propagation

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/TAAL-GmbH/ubsv/stores/blob"
	"github.com/TAAL-GmbH/ubsv/stores/blob/badger"
	"github.com/TAAL-GmbH/ubsv/stores/blob/gcs"
	"github.com/TAAL-GmbH/ubsv/stores/blob/minio"
	"github.com/TAAL-GmbH/ubsv/stores/blob/null"
)

func NewStore(storeUrl *url.URL) (propagationStore blob.Store, err error) {
	switch storeUrl.Scheme {
	case "null":
		propagationStore, err = null.New()
		if err != nil {
			return nil, fmt.Errorf("error creating null block store: %v", err)
		}
	case "badger":
		propagationStore, err = badger.New("." + storeUrl.Path) // relative
		if err != nil {
			return nil, fmt.Errorf("error creating badger block store: %v", err)
		}
	case "gcs":
		propagationStore, err = gcs.New(strings.Replace(storeUrl.Path, "/", "", 1))
		if err != nil {
			return nil, fmt.Errorf("error creating gcs block store: %v", err)
		}
	case "minio":
		fallthrough
	case "minios":
		propagationStore, err = minio.New(storeUrl)
		if err != nil {
			return nil, fmt.Errorf("error creating minio block store: %v", err)
		}
	default:
		return nil, fmt.Errorf("unknown store type: %s", storeUrl.Scheme)
	}

	return
}
