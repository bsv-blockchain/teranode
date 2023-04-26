//go:build foundationdb

package utxo

import (
	"net/url"
	"strconv"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/store/foundationdb"
)

func init() {
	availableDatabases["foundationdb"] = func(url *url.URL) (store.UTXOStore, error) {
		port, err := strconv.Atoi(url.Port())
		if err != nil {
			return nil, err
		}

		password, _ := url.User.Password()
		return foundationdb.New(url.Hostname(), port, url.User.String(), password)
	}
}
