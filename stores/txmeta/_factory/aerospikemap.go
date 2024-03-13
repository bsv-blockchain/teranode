// //go:build aerospike

package _factory

import (
	"github.com/bitcoin-sv/ubsv/stores/txmeta/aerospikemap"
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["aerospikemap"] = func(logger ulogger.Logger, url *url.URL) (txmeta.Store, error) {
		return aerospikemap.New(logger, url)
	}
}
