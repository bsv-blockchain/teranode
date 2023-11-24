package blockchain

import (
	"fmt"
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/blockchain/sql"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func NewStore(logger ulogger.Logger, storeUrl *url.URL) (Store, error) {
	switch storeUrl.Scheme {
	case "postgres":
		fallthrough
	case "sqlitememory":
		fallthrough
	case "sqlite":
		return sql.New(logger, storeUrl)
	}

	return nil, fmt.Errorf("unknown scheme: %s", storeUrl.Scheme)
}
