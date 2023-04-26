package utxo

import (
	"fmt"
	"net/url"
	"strconv"

	store "github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/ordishs/go-utils"
)

var availableDatabases = map[string]func(url *url.URL) (store.UTXOStore, error){}

func NewStore(logger utils.Logger, url *url.URL) (store.UTXOStore, error) {
	port, err := strconv.Atoi(url.Port())
	if err != nil {
		return nil, err
	}

	dbInit, ok := availableDatabases[url.Scheme]
	if ok {
		logger.Infof("[UTXOStore] connecting to %s service at %s:%d", url.Scheme, url.Hostname(), port)
		return dbInit(url)
	}

	return nil, fmt.Errorf("unknown scheme: %s", url.Scheme)
}
