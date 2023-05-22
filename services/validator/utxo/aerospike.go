//go:build aerospike

package utxo

import (
	"net/url"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/store/aerospike"
)

func init() {
	availableDatabases["aerospike"] = func(url *url.URL) (store.UTXOStore, error) {
		return aerospike.New(url)
	}
}
