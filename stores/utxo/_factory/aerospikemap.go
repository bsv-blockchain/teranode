// //go:build aerospike

package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospikemap"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["aerospikemap"] = func(logger ulogger.Logger, url *url.URL) (utxo.Interface, error) {
		return aerospikemap.New(logger, url)
	}
}
