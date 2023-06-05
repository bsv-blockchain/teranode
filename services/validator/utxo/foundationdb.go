//go:build foundationdb

package utxo

import (
	"net/url"
	"strconv"

	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/stores/utxo/foundationdb"
)

func init() {
	availableDatabases["foundationdb"] = func(url *url.URL) (utxostore.UTXOStore, error) {
		port, err := strconv.Atoi(url.Port())
		if err != nil {
			return nil, err
		}

		password, _ := url.User.Password()
		return foundationdb.New(url.Hostname(), port, url.User.String(), password)
	}
}
