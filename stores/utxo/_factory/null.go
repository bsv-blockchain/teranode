package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/nullstore"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["null"] = func(_ ulogger.Logger, url *url.URL) (utxo.Interface, error) {
		return nullstore.NewNullStore()
	}
}
