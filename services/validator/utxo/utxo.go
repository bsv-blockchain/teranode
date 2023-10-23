package utxo

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/ordishs/go-utils"
)

var availableDatabases = map[string]func(url *url.URL) (utxostore.Interface, error){}

func NewStore(ctx context.Context, logger utils.Logger, storeUrl *url.URL, source string, startBlockchainListener ...bool) (utxostore.Interface, error) {

	var port int
	var err error

	if storeUrl.Port() != "" {
		port, err = strconv.Atoi(storeUrl.Port())
		if err != nil {
			return nil, err
		}
	}

	dbInit, ok := availableDatabases[storeUrl.Scheme]
	if ok {
		var utxoStore utxostore.Interface
		var blockchainClient blockchain.ClientI
		var blockchainSubscriptionCh chan *model.Notification

		// TODO retry on connection failure

		logger.Infof("[UTXOStore] connecting to %s service at %s:%d", storeUrl.Scheme, storeUrl.Hostname(), port)
		utxoStore, err = dbInit(storeUrl)
		if err != nil {
			return nil, err
		}

		startBlockchain := true
		if len(startBlockchainListener) > 0 {
			startBlockchain = startBlockchainListener[0]
		}

		if startBlockchain {
			// get the latest block height to compare against lock time utxos
			blockchainClient, err = blockchain.NewClient(ctx)
			if err != nil {
				panic(err)
			}
			blockchainSubscriptionCh, err = blockchainClient.Subscribe(ctx, "UTXOStore")
			if err != nil {
				panic(err)
			}

			_, meta, err := blockchainClient.GetBestBlockHeader(ctx)
			if err != nil {
				logger.Errorf("[UTXOStore] error getting best block header for %s: %v", source, err)
			} else {
				logger.Debugf("[UTXOStore] setting block height to %d", meta.Height)
				_ = utxoStore.SetBlockHeight(meta.Height)
			}

			logger.Infof("[UTXOStore] starting block height subscription for: %s", source)
			go func() {
				for {
					select {
					case <-ctx.Done():
						logger.Infof("[UTXOStore] shutting down block height subscription for: %s", source)
						return
					case notification := <-blockchainSubscriptionCh:
						if notification.Type == model.NotificationType_Block {
							_, meta, err := blockchainClient.GetBestBlockHeader(ctx)
							if err != nil {
								logger.Errorf("[UTXOStore] error getting best block header for %s: %v", source, err)
								continue
							}
							logger.Debugf("[UTXOStore] setting block height to %d", meta.Height)
							_ = utxoStore.SetBlockHeight(meta.Height)
						}
					}
				}
			}()
		}

		return utxoStore, nil
	}

	return nil, fmt.Errorf("unknown scheme: %s", storeUrl.Scheme)
}

func BlockHeightListener(ctx context.Context, logger utils.Logger, utxoStore utxostore.Interface, source string) {
	// get the latest block height to compare against lock time utxos
	blockchainClient, err := blockchain.NewClient(ctx)
	if err != nil {
		panic(err)
	}
	blockchainSubscriptionCh, err := blockchainClient.Subscribe(ctx, source)
	if err != nil {
		panic(err)
	}

	go func() {

		for {
			select {
			case <-ctx.Done():
				logger.Infof("[UTXOStore] shutting down block height subscription for: %s", source)
				return
			case notification := <-blockchainSubscriptionCh:
				if notification.Type == model.NotificationType_Block {
					_, meta, err := blockchainClient.GetBestBlockHeader(ctx)
					if err != nil {
						logger.Errorf("[UTXOStore] error getting best block header for %s: %v", source, err)
						continue
					}
					_ = utxoStore.SetBlockHeight(meta.Height)
				}
			}
		}
	}()
}
