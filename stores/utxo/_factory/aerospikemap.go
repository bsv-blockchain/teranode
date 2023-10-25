// //go:build aerospike

package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospikemap"
)

func init() {
	availableDatabases["aerospikemap"] = func(url *url.URL) (utxo.Interface, error) {
		return aerospikemap.New(url)
	}
}
