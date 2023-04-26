//go:build aerospike

package utxo

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/store/aerospike"
)

func init() {
	availableDatabases["aerospike"] = func(url *url.URL) (store.UTXOStore, error) {
		port, err := strconv.Atoi(url.Port())
		if err != nil {
			return nil, err
		}

		if len(url.Path) < 1 {
			return nil, fmt.Errorf("aerospike namespace not found")
		}
		return aerospike.New(url.Hostname(), port, url.Path[1:])
	}
}
