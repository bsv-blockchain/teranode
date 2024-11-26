package blockchain

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/errors"

	"github.com/bitcoin-sv/ubsv/stores/blockchain/sql"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func NewStore(logger ulogger.Logger, storeURL *url.URL) (Store, error) {
	switch storeURL.Scheme {
	case "postgres":
		fallthrough
	case "sqlitememory":
		fallthrough
	case "sqlite":
		return sql.New(logger, storeURL)
	}

	return nil, errors.NewStorageError("unknown scheme: %s", storeURL.Scheme)
}
