package utxo

import (
	"net/url"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/nullstore"
)

func init() {
	availableDatabases["null"] = func(url *url.URL) (utxostore.Interface, error) {
		return nullstore.NewNullStore()
	}
}
