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
//	metadata, _, err := store.SpendAndCreate(ctx, tx, blockHeight, utxo.WithCreateOnly())
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
	"sort"
	"strconv"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	storelogger "github.com/bsv-blockchain/teranode/stores/utxo/logger"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const errGettingBestHeightAndTime = "[UTXOStore] error getting best height and time for %s: %v"

var availableDatabases = map[string]func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error){}

// earlyDAHBoundarySetter is implemented by stores supporting below-checkpoint
// early-DAH (currently the postgres store). This is a genuine optional
// interface, checked via type assertion below — unlike SupportsOutpointOnlySpend,
// which is a required method on the utxo.Store interface itself (always
// present, never asserted for). A store that doesn't implement
// earlyDAHBoundarySetter simply leaves the boundary unset; a store wrapped by
// stores/utxo/logger.Store gets this forwarded to the wrapped store (see that
// package's SetEarlyDAHBoundary), so logging wrapping does not by itself
// disarm the feature.
type earlyDAHBoundarySetter interface{ SetEarlyDAHBoundary(uint32) }

// earlyDAHRatchet climbs the hardcoded checkpoint list as the header chain
// grows, publishing the highest CONFIRMED checkpoint height to the store as
// the early-DAH boundary. During legacy IBD headers are downloaded in windows
// just ahead of the block tip, so a single latch on only the highest
// checkpoint (mainnet: 945,000) would stay closed for nearly the whole sync —
// the ratchet instead opens at the first checkpoint (546) within moments and
// climbs from there. Safety is per-checkpoint: once the header chain reaches
// and matches checkpoint N, validation rejects any chain not passing through
// it, so every block at-or-below N is reorg-protected regardless of the
// checkpoints above N.
//
// Fail-safe: probe errors and missing headers leave the boundary at the last
// confirmed checkpoint (or unset) and retry on a later notification. A HASH
// MISMATCH at a checkpoint height is different — the header chain contradicts
// a hardcoded checkpoint, which validation should make impossible — so the
// ratchet logs it and stops permanently without advancing (the already
// published boundary, proven by its own checkpoint, remains).
type earlyDAHRatchet struct {
	done   bool                  // fully climbed, feature off, or store can't use it
	next   int                   // index into cps of the next unconfirmed checkpoint
	cps    []chaincfg.Checkpoint // ascending by height
	setter earlyDAHBoundarySetter
}

// newEarlyDAHRatchet builds the ratchet, resolving the applicability checks
// once. done==true from the start when the feature is off, the store cannot
// accept a boundary, or the chain has no checkpoints (e.g. teratestnet).
func newEarlyDAHRatchet(tSettings *settings.Settings, store any) *earlyDAHRatchet {
	r := &earlyDAHRatchet{}

	if !tSettings.UtxoStore.EarlyDAHBelowCheckpoint {
		r.done = true
		return r
	}

	setter, ok := store.(earlyDAHBoundarySetter)
	if !ok {
		r.done = true
		return r
	}

	cps := tSettings.ChainCfgParams.Checkpoints
	if len(cps) == 0 {
		r.done = true
		return r
	}

	r.setter = setter
	r.cps = make([]chaincfg.Checkpoint, len(cps))
	copy(r.cps, cps)
	sort.Slice(r.cps, func(i, j int) bool { return r.cps[i].Height < r.cps[j].Height })

	return r
}

// probe advances the ratchet as far as the current header chain allows. Each
// confirmed checkpoint costs one header lookup; the loop stops at the first
// checkpoint whose header is not yet available, so steady-state cost is one
// RPC per block notification until the top checkpoint confirms, then zero.
func (r *earlyDAHRatchet) probe(ctx context.Context, logger ulogger.Logger, client blockchain.ClientI) {
	for !r.done {
		cp := r.cps[r.next]
		if cp.Hash == nil {
			logger.Warnf("[UTXOStore] early-DAH ratchet: checkpoint at height %d has no hash, stopping", cp.Height)
			r.done = true

			return
		}

		headers, _, err := client.GetBlockHeadersFromHeight(ctx, uint32(cp.Height), 1) //nolint:gosec
		if err != nil || len(headers) == 0 {
			return // header chain not there yet (or transient error) — retry on a later notification
		}

		if !headers[0].Hash().IsEqual(cp.Hash) {
			logger.Warnf("[UTXOStore] early-DAH ratchet: header at checkpoint height %d does not match pinned hash, stopping at last confirmed boundary", cp.Height)
			r.done = true

			return
		}

		r.setter.SetEarlyDAHBoundary(uint32(cp.Height)) //nolint:gosec
		logger.Infof("[UTXOStore] early-DAH boundary ratcheted to checkpoint height %d", cp.Height)

		r.next++
		if r.next >= len(r.cps) {
			logger.Infof("[UTXOStore] early-DAH boundary fully latched at highest checkpoint height %d", cp.Height)
			r.done = true
		}
	}
}

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
					logger.Infof(errGettingBestHeightAndTime, source, err)
				} else {
					logger.Warnf(errGettingBestHeightAndTime, source, err)
				}
			} else if blockHeight > 0 {
				logger.Debugf("[UTXOStore] setting block state to height %d, median time %d", blockHeight, medianBlockTime)

				// Both values come from the same chain tip, so publish them as
				// one atomic snapshot (issue 1443): two separate setter calls
				// leave a window where GetBlockState readers pair a new height
				// with a stale median time.
				if err = utxoStore.SetBlockState(blockHeight, medianBlockTime); err != nil {
					logger.Errorf("[UTXOStore] error setting block state for %s: %v", source, err)
				}
			} else {
				logger.Infof("[UTXOStore] skipping block height initialization for %s (height is 0)", source)
			}

			earlyDAH := newEarlyDAHRatchet(tSettings, utxoStore)
			earlyDAH.probe(ctx, logger, blockchainClient)

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
									logger.Infof(errGettingBestHeightAndTime, source, err)
								} else {
									logger.Errorf(errGettingBestHeightAndTime, source, err)
								}
							} else if blockHeight > 0 {
								logger.Debugf("[UTXOStore] updated block height to %d and median time to %d for %s", blockHeight, medianBlockTime, source)

								// One atomic snapshot per tip; see the comment on the
								// initialisation path above (issue 1443).
								if err = utxoStore.SetBlockState(blockHeight, medianBlockTime); err != nil {
									logger.Errorf("[UTXOStore] error setting block state for %s: %v", source, err)
								}
							} else {
								logger.Infof("[UTXOStore] skipping block height update for %s (height is 0)", source)
							}

							earlyDAH.probe(ctx, logger, blockchainClient)
						}
					}
				}
			}()
		}

		return utxoStore, nil
	}

	return nil, errors.NewProcessingError("utxostore: unknown scheme: %s", storeURL.Scheme)
}
