package store

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/nullstore"
)

func init() {
	availableDatabases["null"] = func(url *url.URL) (txmeta.Store, error) {
		return nullstore.New(), nil
	}
}
