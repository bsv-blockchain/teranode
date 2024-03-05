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

var availableDatabases = map[string]func(logger ulogger.Logger, url *url.URL) (utxo.Interface, error){}

func NewStore(ctx context.Context, logger ulogger.Logger, storeUrl *url.URL, source string, startBlockchainListener ...bool) (utxo.Interface, error) {

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
		var utxoStore utxo.Interface
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
				// TODO change this back to Debug
				logger.Warnf("[UTXOStore] setting block height to %d", meta.Height)
				setBlockHeight(ctx, logger, utxoStore, source, meta.Height)
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
							// TODO change this back to Debug
							logger.Warnf("[UTXOStore] setting block height to %d", meta.Height)
							setBlockHeight(ctx, logger, utxoStore, source, meta.Height)
						}
					}
				}
			}()
		}

		return utxoStore, nil
	}

	return nil, fmt.Errorf("unknown scheme: %s", storeUrl.Scheme)
}

func BlockHeightListener(ctx context.Context, logger ulogger.Logger, utxoStore utxo.Interface, source string) {
	// get the latest block height to compare against lock time utxos
	blockchainClient, err := blockchain.NewClient(ctx, logger)
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
					go func() {
						// trying to keep up, when we use GetBestBlockHeader it is often very slightly behind
						// causing errors saving coinbase splitting tx. This is an experiment.
						// _, meta, err := blockchainClient.GetBlockHeader(ctx, notification.Hash)
						_, meta, err := blockchainClient.GetBestBlockHeader(ctx)
						if err != nil {
							logger.Errorf("[UTXOStore] error getting best block header for %s: %v", source, err)
						} else {
							setBlockHeight(ctx, logger, utxoStore, source, meta.Height)
						}
					}()
				}
			}
		}
	}()
}

func setBlockHeight(ctx context.Context, logger ulogger.Logger, utxoStore utxo.Interface, source string, blockHeight uint32) error {
	if blockHeight > utxoStore.GetBlockHeight() {
		return utxoStore.SetBlockHeight(blockHeight)
	}
}
