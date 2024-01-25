package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/nullstore"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["null"] = func(logger ulogger.Logger, url *url.URL) (txmeta.Store, error) {
		return nullstore.New(logger), nil
	}
}
