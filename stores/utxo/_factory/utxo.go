package _factory

import (
	"context"
	"net/url"
	"strconv"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	storelogger "github.com/bitcoin-sv/ubsv/stores/utxo/logger"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

var availableDatabases = map[string]func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error){}

func NewStore(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, source string, startBlockchainListener ...bool) (utxo.Store, error) {
	var port int

	var err error

	storeURL := tSettings.UtxoStore.UtxoStore

	if storeURL.Port() != "" {
		port, err = strconv.Atoi(storeURL.Port())
		if err != nil {
			return nil, err
		}
	}

	dbInit, ok := availableDatabases[storeURL.Scheme]
	if ok {
		var utxoStore utxo.Store

		var blockchainClient blockchain.ClientI

		var blockchainSubscriptionCh chan *blockchain.Notification

		// TODO retry on connection failure

		logger.Infof("[UTXOStore] connecting to %s service at %s:%d", storeURL.Scheme, storeURL.Hostname(), port)

		utxoStore, err = dbInit(ctx, logger, tSettings, storeURL)
		if err != nil {
			return nil, err
		}

		if storeURL.Query().Get("logging") == "true" {
			utxoStore = storelogger.New(ctx, logger, utxoStore)
		}

		startBlockchain := true
		if len(startBlockchainListener) > 0 {
			startBlockchain = startBlockchainListener[0]
		}

		if startBlockchain {
			// get the latest block height to compare against lock time utxos
			blockchainClient, err = blockchain.NewClient(ctx, logger, tSettings, "stores/utxo/factory")
			if err != nil {
				return nil, errors.NewServiceError("error creating blockchain client", err)
			}

			blockchainSubscriptionCh, err = blockchainClient.Subscribe(ctx, "UTXOStore")
			if err != nil {
				return nil, errors.NewServiceError("error subscribing to blockchain", err)
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

	return nil, errors.NewProcessingError("unknown scheme: %s", storeURL.Scheme)
}
