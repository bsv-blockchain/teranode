package store

import (
	"fmt"
	"net/url"

	"github.com/TAAL-GmbH/ubsv/stores/txstatus"
	"github.com/ordishs/go-utils"
)

var availableDatabases = map[string]func(url *url.URL) (txstatus.Store, error){}

func New(logger utils.Logger, url *url.URL) (txstatus.Store, error) {
	dbInit, ok := availableDatabases[url.Scheme]
	if ok {
		logger.Infof("[TxStatusStore] connecting to %s service at %s (%s)", url.Scheme, url.Host, url.Path)
		return dbInit(url)
	}

	return nil, fmt.Errorf("unknown scheme: %s", url.Scheme)
}
