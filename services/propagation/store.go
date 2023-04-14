package propagation

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/TAAL-GmbH/ubsv/services/propagation/store"
	"github.com/TAAL-GmbH/ubsv/services/propagation/store/badger"
	"github.com/TAAL-GmbH/ubsv/services/propagation/store/gcs"
)

func NewStore(storeUrl *url.URL) (propagationStore store.TransactionStore, err error) {
	switch storeUrl.Scheme {
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
	default:
		return nil, fmt.Errorf("unknown store type: %s", storeUrl.Scheme)
	}

	return
}
