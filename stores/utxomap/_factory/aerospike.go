// //go:build aerospike

package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["aerospike"] = func(logger ulogger.Logger, url *url.URL) (utxo.Interface, error) {
		return aerospike.New(logger, url)
	}
}
