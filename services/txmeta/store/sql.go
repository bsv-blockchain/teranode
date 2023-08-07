package store

import (
	"net/url"

	"github.com/TAAL-GmbH/ubsv/stores/txmeta"
	"github.com/TAAL-GmbH/ubsv/stores/txmeta/sql"
)

func init() {
	availableDatabases["postgres"] = func(url *url.URL) (txmeta.Store, error) {
		return sql.New(url)
	}
	availableDatabases["sqlite"] = func(url *url.URL) (txmeta.Store, error) {
		return sql.New(url)
	}
	availableDatabases["sqlitememory"] = func(url *url.URL) (txmeta.Store, error) {
		return sql.New(url)
	}
}
