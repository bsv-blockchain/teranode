package utxo

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/TAAL-GmbH/ubsv/services/blockchain"
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
		logger.Infof("[UTXOStore] connecting to %s service at %s:%d", url.Scheme, url.Hostname(), port)
		utxoStore, err := dbInit(url)
		if err != nil {
			return nil, err
		}

		// get the latest block height to compare against lock time utxos
		blockchainClient, err := blockchain.NewClient()
		if err != nil {
			panic(err)
		}
		bestBlockHeaderCh, err := blockchainClient.SubscribeBestBlockHeader(context.Background())
		if err != nil {
			panic(err)
		}
		go func() {
			for {
				select {
				case header := <-bestBlockHeaderCh:
					_ = utxoStore.SetBlockHeight(header.Height)
				}
			}
		}()

		return utxoStore, nil
	}

	return nil, fmt.Errorf("unknown scheme: %s", url.Scheme)
}
