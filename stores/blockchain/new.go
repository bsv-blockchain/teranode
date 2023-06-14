package blockchain

import (
	"fmt"
	"net/url"

	"github.com/TAAL-GmbH/ubsv/stores/blockchain/sql"
	"github.com/ordishs/go-utils"
)

func NewStore(_ utils.Logger, storeUrl *url.URL) (Store, error) {
	switch storeUrl.Scheme {
	case "postgres":
		fallthrough
	case "sqlitememory":
		fallthrough
	case "sqlite":
		return sql.New(storeUrl)
	}

	return nil, fmt.Errorf("unknown scheme: %s", storeUrl.Scheme)
}
