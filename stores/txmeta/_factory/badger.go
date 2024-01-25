package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/badger"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["badger"] = func(logger ulogger.Logger, url *url.URL) (txmeta.Store, error) {
		return badger.New(logger, "."+url.Path)
	}
}
