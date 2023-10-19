//go:build aerospike

package utxo

import (
	"net/url"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospikemap"
)

func init() {
	availableDatabases["aerospikemap"] = func(url *url.URL) (utxostore.Interface, error) {
		return aerospikemap.New(url)
	}
}
