// Package factory provides a factory for creating UTXO store implementations.
// It supports multiple database backends through build tags and connection URLs.
//
// # Supported Backends
//
// The following storage backends are available:
//   - Aerospike (build tag: aerospike): "aerospike://host:port/namespace/set"
//   - PostgreSQL: "postgres://user:pass@host:port/dbname"
//   - SQLite: "sqlite://path/to/file.db"
//   - SQLite Memory: "sqlitememory://"
//   - In-Memory (build tag: memory): "memory://" (for testing)
//
// # Usage
//
//	import (
//	    "github.com/bitcoin-sv/ubsv/stores/utxo/factory"
//	    "github.com/bitcoin-sv/ubsv/settings"
//	)
//
//	// Initialize from settings
//	store, err := factory.NewStore(ctx, logger, settings, "service-name")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Use the store
//	metadata, err := store.Create(ctx, tx, blockHeight)
//
// # Features
//
// The factory provides:
//   - Automatic database connection management
//   - Optional logging via URL query parameter "logging=true"
//   - Automatic block height updates via blockchain subscription
//   - Graceful shutdown handling
//
// # Configuration
//
// Store configuration is handled through the settings package and connection URLs.
// The URL format depends on the chosen backend. Connection parameters can be
// specified as URL query parameters.
//
// Example URLs:
//   postgres://user:pass@localhost:5432/utxo?sslmode=disable&logging=true
//   aerospike://localhost:3000/test/utxos?logging=true
//
// # Block Height Management
//
// By default, the factory sets up a blockchain subscription to automatically
// update the store's block height and median time. This can be disabled by
// passing false as the startBlockchainListener parameter to NewStore.
//
// # Logging
//
// Logging can be enabled by adding logging=true to the connection URL:
//
// When enabled, all store operations will be logged with:
//   - Operation name
//   - Parameters
//   - Duration
//   - Error status

package factory

import (
	"context"
	"net/url"
	"strconv"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	storelogger "github.com/bsv-blockchain/teranode/stores/utxo/logger"
	"github.com/bsv-blockchain/teranode/ulogger"
)

var availableDatabases = map[string]func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error){}

// NewStore creates a new UTXO store implementation based on the settings.
// The source parameter is used for logging purposes.
// The startBlockchainListener parameter controls whether to set up automatic
// block height updates (defaults to true if not specified).
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

			blockchainSubscriptionCh, err = blockchainClient.Subscribe(ctx, blockchain.SubscriberUTXOStore)
			if err != nil {
				return nil, errors.NewServiceError("error subscribing to blockchain", err)
			}

			blockHeight, medianBlockTime, err := blockchainClient.GetBestHeightAndTime(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Infof("[UTXOStore] error getting best height and time for %s: %v", source, err)
				} else {
					logger.Warnf("[UTXOStore] error getting best height and time for %s: %v", source, err)
				}
			} else if blockHeight > 0 {
				logger.Debugf("[UTXOStore] setting block height to %d", blockHeight)

				if err = utxoStore.SetBlockHeight(blockHeight); err != nil {
					logger.Errorf("[UTXOStore] error setting block height for %s: %v", source, err)
				}

				logger.Debugf("[UTXOStore] setting median block time to %d", medianBlockTime)

				if err = utxoStore.SetMedianBlockTime(medianBlockTime); err != nil {
					logger.Errorf("[UTXOStore] error setting median block time for %s: %v", source, err)
				}
			} else {
				logger.Infof("[UTXOStore] skipping block height initialization for %s (height is 0)", source)
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
								if errors.Is(err, context.Canceled) {
									logger.Infof("[UTXOStore] error getting best height and time for %s: %v", source, err)
								} else {
									logger.Errorf("[UTXOStore] error getting best height and time for %s: %v", source, err)
								}
							} else if blockHeight > 0 {
								logger.Debugf("[UTXOStore] updated block height to %d and median time to %d for %s", blockHeight, medianBlockTime, source)

								if err = utxoStore.SetBlockHeight(blockHeight); err != nil {
									logger.Errorf("[UTXOStore] error setting block height for %s: %v", source, err)
								}

								if err = utxoStore.SetMedianBlockTime(medianBlockTime); err != nil {
									logger.Errorf("[UTXOStore] error setting median block time for %s: %v", source, err)
								}
							} else {
								logger.Infof("[UTXOStore] skipping block height update for %s (height is 0)", source)
							}
						}
					}
				}
			}()
		}

		return utxoStore, nil
	}

	return nil, errors.NewProcessingError("utxostore: unknown scheme: %s", storeURL.Scheme)
}
