// //go:build aerospike

package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/aerospike"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["aerospike"] = func(logger ulogger.Logger, url *url.URL) (txmeta.Store, error) {
		return aerospike.New(logger, url)
	}
}
