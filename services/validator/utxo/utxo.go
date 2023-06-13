package utxo

import (
	"fmt"
	"net/url"
	"strconv"

	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/ordishs/go-utils"
)

var availableDatabases = map[string]func(url *url.URL) (utxostore.Interface, error){}

func NewStore(logger utils.Logger, url *url.URL) (utxostore.Interface, error) {
	port, err := strconv.Atoi(url.Port())
	if err != nil {
		return nil, err
	}

	dbInit, ok := availableDatabases[url.Scheme]
	if ok {
		logger.Infof("[Interface] connecting to %s service at %s:%d", url.Scheme, url.Hostname(), port)
		return dbInit(url)
	}

	return nil, fmt.Errorf("unknown scheme: %s", url.Scheme)
}
