package _factory

import (
	"fmt"
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

var availableDatabases = map[string]func(logger ulogger.Logger, url *url.URL) (txmeta.Store, error){}

func New(logger ulogger.Logger, url *url.URL) (txmeta.Store, error) {
	dbInit, ok := availableDatabases[url.Scheme]
	if ok {
		logger.Infof("[TxMetaStore] connecting to %s service at %s (%s)", url.Scheme, url.Host, url.Path)
		return dbInit(logger, url)
	}

	return nil, fmt.Errorf("unknown scheme: %s", url.Scheme)
}
