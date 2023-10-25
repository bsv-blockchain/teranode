// //go:build memory

package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/sql"
)

func init() {
	availableDatabases["postgres"] = func(url *url.URL) (utxo.Interface, error) {
		return sql.New(url)
	}
	availableDatabases["sqlitememory"] = func(url *url.URL) (utxo.Interface, error) {
		return sql.New(url)
	}
	availableDatabases["sqlite"] = func(url *url.URL) (utxo.Interface, error) {
		return sql.New(url)
	}
}
