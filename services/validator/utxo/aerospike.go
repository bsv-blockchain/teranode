//go:build aerospike

package utxo

import (
	"net/url"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"
)

func init() {
	availableDatabases["aerospike"] = func(url *url.URL) (utxostore.Interface, error) {
		return aerospike.New(url)
	}
}
