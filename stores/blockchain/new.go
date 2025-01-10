package blockchain

import (
	"net/url"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"

	"github.com/bitcoin-sv/teranode/stores/blockchain/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
)

func NewStore(logger ulogger.Logger, storeURL *url.URL, tSettings *settings.Settings) (Store, error) {
	switch storeURL.Scheme {
	case "postgres":
		fallthrough
	case "sqlitememory":
		fallthrough
	case "sqlite":
		return sql.New(logger, storeURL, tSettings)
	}

	return nil, errors.NewStorageError("unknown scheme: %s", storeURL.Scheme)
}
