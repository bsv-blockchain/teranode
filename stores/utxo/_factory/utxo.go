package _factory

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

var availableDatabases = map[string]func(logger ulogger.Logger, url *url.URL) (utxo.Store, error){}

func NewStore(ctx context.Context, logger ulogger.Logger, storeUrl *url.URL, source string, startBlockchainListener ...bool) (utxo.Store, error) {

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
		var utxoStore utxo.Store
		var blockchainClient blockchain.ClientI
		var blockchainSubscriptionCh chan *model.Notification

		// TODO retry on connection failure

		logger.Infof("[UTXOStore] connecting to %s service at %s:%d", storeUrl.Scheme, storeUrl.Hostname(), port)
		utxoStore, err = dbInit(logger, storeUrl)
		if err != nil {
			return nil, err
		}

		startBlockchain := true
		if len(startBlockchainListener) > 0 {
			startBlockchain = startBlockchainListener[0]
		}

		if startBlockchain {
			// get the latest block height to compare against lock time utxos
			blockchainClient, err = blockchain.NewClient(ctx, logger)
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
				if err = setBlockHeight(ctx, logger, utxoStore, source, meta.Height); err != nil {
					logger.Errorf("[UTXOStore] error setting block height for %s: %v", source, err)
				}
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
								logger.Warnf("[UTXOStore] could not get best block header for %s: %v", source, err)
								continue
							}
							logger.Debugf("[UTXOStore] setting block height to %d", meta.Height)
							if err = setBlockHeight(ctx, logger, utxoStore, source, meta.Height); err != nil {
								logger.Errorf("[UTXOStore] error setting block height for %s: %v", source, err)
							}
						}
					}
				}
			}()
		}

		return utxoStore, nil
	}

	return nil, fmt.Errorf("unknown scheme: %s", storeUrl.Scheme)
}

func setBlockHeight(_ context.Context, _ ulogger.Logger, utxoStore utxo.Store, _ string, blockHeight uint32) error {
	return utxoStore.SetBlockHeight(blockHeight)
}
