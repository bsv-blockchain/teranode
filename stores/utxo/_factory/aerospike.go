// //go:build aerospike

package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"
)

func init() {
	availableDatabases["aerospike"] = func(url *url.URL) (utxo.Interface, error) {
		return aerospike.New(url)
	}
}
