package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/sql"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["postgres"] = func(logger ulogger.Logger, url *url.URL) (utxo.Store, error) {
		return sql.New(logger, url)
	}
	availableDatabases["sqlitememory"] = func(logger ulogger.Logger, url *url.URL) (utxo.Store, error) {
		return sql.New(logger, url)
	}
	availableDatabases["sqlite"] = func(logger ulogger.Logger, url *url.URL) (utxo.Store, error) {
		return sql.New(logger, url)
	}
}
