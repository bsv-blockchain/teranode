package store

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/badger"
)

func init() {
	availableDatabases["badger"] = func(url *url.URL) (txmeta.Store, error) {
		return badger.New("." + url.Path)
	}
}
