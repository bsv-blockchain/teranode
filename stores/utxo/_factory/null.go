package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/nullstore"
)

func init() {
	availableDatabases["null"] = func(url *url.URL) (utxo.Interface, error) {
		return nullstore.NewNullStore()
	}
}
