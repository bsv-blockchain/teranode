package store

import (
	"fmt"
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/ordishs/go-utils"
)

var availableDatabases = map[string]func(url *url.URL) (txmeta.Store, error){}

func New(logger utils.Logger, url *url.URL) (txmeta.Store, error) {
	dbInit, ok := availableDatabases[url.Scheme]
	if ok {
		logger.Infof("[TxMetaStore] connecting to %s service at %s (%s)", url.Scheme, url.Host, url.Path)
		return dbInit(url)
	}

	return nil, fmt.Errorf("unknown scheme: %s", url.Scheme)
}
