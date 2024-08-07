package _factory

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"net/url"
	"strconv"
)

var availableDatabases = map[string]func(ctx context.Context, logger ulogger.Logger, url *url.URL) (utxo.Store, error){}

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
		utxoStore, err = dbInit(ctx, logger, storeUrl)
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

			blockHeight, medianBlockTime, err := blockchainClient.GetBestHeightAndTime(ctx)
			if err != nil {
				logger.Errorf("[UTXOStore] error getting best height and time for %s: %v", source, err)
			} else {
				logger.Debugf("[UTXOStore] setting block height to %d", blockHeight)
				if err = utxoStore.SetBlockHeight(blockHeight); err != nil {
					logger.Errorf("[UTXOStore] error setting block height for %s: %v", source, err)
				}
				logger.Debugf("[UTXOStore] setting median block time to %d", medianBlockTime)
				if err = utxoStore.SetMedianBlockTime(medianBlockTime); err != nil {
					logger.Errorf("[UTXOStore] error setting median block time for %s: %v", source, err)
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
							blockHeight, medianBlockTime, err = blockchainClient.GetBestHeightAndTime(ctx)
							if err != nil {
								logger.Errorf("[UTXOStore] error getting best height and time for %s: %v", source, err)
							} else {
								logger.Debugf("[UTXOStore] updated block height to %d and median time to %d for %s", blockHeight, medianBlockTime, source)
								if err = utxoStore.SetBlockHeight(blockHeight); err != nil {
									logger.Errorf("[UTXOStore] error setting block height for %s: %v", source, err)
								}
								if err = utxoStore.SetMedianBlockTime(medianBlockTime); err != nil {
									logger.Errorf("[UTXOStore] error setting median block time for %s: %v", source, err)
								}
							}
						}
					}
				}
			}()
		}

		return utxoStore, nil
	}

	return nil, errors.NewProcessingError("unknown scheme: %s", storeUrl.Scheme)
}
