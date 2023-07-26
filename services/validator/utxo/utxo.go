package utxo

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/TAAL-GmbH/ubsv/services/blockchain/blockchain_api"
	"github.com/TAAL-GmbH/ubsv/stores/utxo"
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
		var utxoStore utxo.Interface
		var blockchainClient blockchain.ClientI
		var blockchainSubscriptionCh chan *model.Notification

		// TODO retry on connection failure

		logger.Infof("[UTXOStore] connecting to %s service at %s:%d", url.Scheme, url.Hostname(), port)
		utxoStore, err = dbInit(url)
		if err != nil {
			return nil, err
		}

		// get the latest block height to compare against lock time utxos
		blockchainClient, err = blockchain.NewClient()
		if err != nil {
			panic(err)
		}
		blockchainSubscriptionCh, err = blockchainClient.Subscribe(context.Background())
		if err != nil {
			panic(err)
		}

		go func() {
			var height uint32
			for {
				select {
				case notification := <-blockchainSubscriptionCh:
					if notification.Type == int32(blockchain_api.Type_Block) {
						_, height, err = blockchainClient.GetBestBlockHeader(context.Background())
						if err != nil {
							logger.Errorf("[UTXOStore] error getting best block header: %v", err)
							continue
						}
						_ = utxoStore.SetBlockHeight(height)
					}
				}
			}
		}()

		return utxoStore, nil
	}

	return nil, fmt.Errorf("unknown scheme: %s", url.Scheme)
}
