package model

import (
	"context"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

type txMinedStatus interface {
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error)
}

type txMinedMessage struct {
	ctx           context.Context
	logger        ulogger.Logger
	txMetaStore   txMinedStatus
	block         *Block
	blockID       uint32
	chainBlockIDs []uint32
	unsetMined    bool
	done          chan error
}

var (
	txMinedChan      = make(chan *txMinedMessage, 1024)
	txMinedOnce      sync.Once
	workerSettings   *settings.Settings
	workerSettingsMu sync.RWMutex

	// prometheus metrics
	prometheusUpdateTxMinedCh       prometheus.Counter
	prometheusUpdateTxMinedQueue    prometheus.Gauge
	prometheusUpdateTxMinedDuration prometheus.Histogram
)

func setWorkerSettings(tSettings *settings.Settings) {
	workerSettingsMu.Lock()
	defer workerSettingsMu.Unlock()

	workerSettings = tSettings
}

func getWorkerSettings() *settings.Settings {
	workerSettingsMu.Lock()
	defer workerSettingsMu.Unlock()

	return workerSettings
}

func initWorker(tSettings *settings.Settings) {
	setWorkerSettings(tSettings)

	prometheusUpdateTxMinedCh = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "model",
		Name:      "update_tx_mined_ch",
		Help:      "Number of tx mined messages sent to the worker",
	})
	prometheusUpdateTxMinedQueue = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "model",
		Name:      "update_tx_mined_queue",
		Help:      "Number of tx mined messages in the queue",
	})
	prometheusUpdateTxMinedDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "model",
		Name:      "update_tx_mined_duration",
		Help:      "Duration of updating tx mined status",
		Buckets:   util.MetricsBucketsSeconds,
	})

	go func() {
		for msg := range txMinedChan {
			func() {
				// Recover from any panic to prevent the worker from dying
				defer func() {
					if r := recover(); r != nil {
						msg.logger.Errorf("[UpdateTxMinedStatus] worker panic recovered: %v", r)
						msg.done <- errors.NewProcessingError("[UpdateTxMinedStatus] worker panic: %v", r)
					}
				}()

				chainBlockIDsMap := make(map[uint32]bool, len(msg.chainBlockIDs))
				for _, bID := range msg.chainBlockIDs {
					chainBlockIDsMap[bID] = true
				}

				if err := updateTxMinedStatus(
					msg.ctx,
					msg.logger,
					getWorkerSettings(),
					msg.txMetaStore,
					msg.block,
					msg.blockID,
					chainBlockIDsMap,
					msg.unsetMined,
				); err != nil {
					msg.done <- err
				} else {
					msg.done <- nil
				}
			}()

			prometheusUpdateTxMinedQueue.Set(float64(len(txMinedChan)))
		}
	}()
}

func UpdateTxMinedStatus(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, txMetaStore txMinedStatus,
	block *Block, blockID uint32, chainBlockIDs []uint32, unsetMined ...bool) error {
	// start the worker, if not already started
	txMinedOnce.Do(func() { initWorker(tSettings) })

	startTime := time.Now()
	defer func() {
		prometheusUpdateTxMinedDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	done := make(chan error)

	unsetTxMined := false
	if len(unsetMined) > 0 {
		unsetTxMined = unsetMined[0]
	}

	txMinedChan <- &txMinedMessage{
		ctx:           ctx,
		logger:        logger,
		txMetaStore:   txMetaStore,
		block:         block,
		blockID:       blockID,
		chainBlockIDs: chainBlockIDs,
		unsetMined:    unsetTxMined, // whether to unset the mined status
		done:          done,
	}

	prometheusUpdateTxMinedCh.Inc()

	return <-done
}

func updateTxMinedStatus(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, txMetaStore txMinedStatus,
	block *Block, blockID uint32, chainBlockIDsMap map[uint32]bool, unsetMined bool) (err error) {
	ctx, _, endSpan := tracing.Tracer("model").Start(ctx, "updateTxMinedStatus",
		tracing.WithHistogram(prometheusUpdateTxMinedDuration),
		tracing.WithTag("txid", block.Hash().String()),
		tracing.WithDebugLogMessage(logger, "[UpdateTxMinedStatus] [%s] blockID %d for %d subtrees", block.Hash().String(), blockID, len(block.Subtrees)),
	)
	defer endSpan(err)

	if !tSettings.UtxoStore.UpdateTxMinedStatus {
		return nil
	}

	maxMinedBatchSize := tSettings.UtxoStore.MaxMinedBatchSize

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, tSettings.UtxoStore.MaxMinedRoutines)

	maxRetries := 10

	for subtreeIdx, subtree := range block.SubtreeSlices {
		subtreeIdx := subtreeIdx
		subtree := subtree

		if subtree == nil {
			return errors.NewProcessingError("[UpdateTxMinedStatus][%s] missing subtree %d of %d", block.String(), subtreeIdx, len(block.Subtrees))
		}

		minedBlockInfo := utxo.MinedBlockInfo{
			BlockID:     blockID,
			BlockHeight: block.Height,
			SubtreeIdx:  subtreeIdx,
			UnsetMined:  unsetMined,
		}

		g.Go(func() error {
			hashes := make([]*chainhash.Hash, 0, maxMinedBatchSize)

			for idx := 0; idx < len(subtree.Nodes); idx++ {
				if subtree.Nodes[idx].Hash.IsEqual(subtreepkg.CoinbasePlaceholderHash) {
					if subtreeIdx != 0 || idx != 0 {
						logger.Warnf("[UpdateTxMinedStatus][%s] bad coinbase placeholder position within block - subtree #%d, node #%d - ignoring", block.Hash().String(), subtreeIdx, idx)
					}

					continue
				}

				hashes = append(hashes, &subtree.Nodes[idx].Hash)

				if idx > 0 && idx%maxMinedBatchSize == 0 {
					batchNr := idx / maxMinedBatchSize
					batchTotal := len(subtree.Nodes) / maxMinedBatchSize

					logger.Debugf("[UpdateTxMinedStatus][%s][%s] for %d hashes, batch %d of %d", block.String(), block.Subtrees[subtreeIdx].String(), len(hashes), batchNr, batchTotal)

					retries := 0

					for {
						// Check if context is canceled before attempting
						select {
						case <-gCtx.Done():
							return errors.NewProcessingError("[UpdateTxMinedStatus][%s] context canceled during retry", block.Hash().String(), gCtx.Err())
						default:
						}

						blockIDsMap, err := txMetaStore.SetMinedMulti(gCtx, hashes, minedBlockInfo)
						if err != nil {
							if retries >= maxRetries {
								return errors.NewProcessingError("[UpdateTxMinedStatus][%s] error setting mined tx", block.Hash().String(), err)
							} else {
								backoff := time.Duration(1+(200*retries)) * time.Millisecond
								logger.Warnf("[UpdateTxMinedStatus][%s] error setting mined tx, retrying in %s: %v", block.Hash().String(), backoff.String(), err)

								// Use a timer with context cancellation check
								timer := time.NewTimer(backoff)
								select {
								case <-gCtx.Done():
									timer.Stop()
									return errors.NewProcessingError("[UpdateTxMinedStatus][%s] context canceled during backoff", block.Hash().String(), gCtx.Err())
								case <-timer.C:
								}
							}
						} else {
							// check that all blockIDs are not already on our chain
							if len(chainBlockIDsMap) > 0 {
								for _, bIDs := range blockIDsMap {
									for _, bID := range bIDs {
										if _, exists := chainBlockIDsMap[bID]; exists && bID != blockID {
											// this transaction is already on our chain, the block is invalid
											return errors.NewBlockInvalidError("[UpdateTxMinedStatus][%s] block contains a transaction already on our chain, blockID %d", block.Hash().String(), bID)
										}
									}
								}
							}

							break
						}

						retries++
					}

					hashes = make([]*chainhash.Hash, 0, maxMinedBatchSize)
				}
			}

			if len(hashes) > 0 {
				retries := 0

				for {
					// Check if context is canceled before attempting
					select {
					case <-gCtx.Done():
						return errors.NewProcessingError("[UpdateTxMinedStatus][%s] context canceled during remainder retry", block.Hash().String(), gCtx.Err())
					default:
					}

					logger.Debugf("[UpdateTxMinedStatus][%s][%s] for %d remainder hashes", block.String(), block.Subtrees[subtreeIdx].String(), len(hashes))

					blockIDsMap, err := txMetaStore.SetMinedMulti(gCtx, hashes, minedBlockInfo)
					if err != nil {
						if retries >= maxRetries {
							return errors.NewProcessingError("[UpdateTxMinedStatus][%s] error setting remainder batch mined tx", block.Hash().String(), err)
						} else {
							backoff := time.Duration(1+(200*retries)) * time.Millisecond
							logger.Warnf("[UpdateTxMinedStatus][%s] error setting remainder batch mined tx, retrying in %s: %v", block.Hash().String(), backoff.String(), err)

							// Use a timer with context cancellation check
							timer := time.NewTimer(backoff)
							select {
							case <-gCtx.Done():
								timer.Stop()
								return errors.NewProcessingError("[UpdateTxMinedStatus][%s] context canceled during remainder backoff", block.Hash().String(), gCtx.Err())
							case <-timer.C:
							}
						}

						retries++
					} else {
						// check that all blockIDs are not already on our chain
						if len(chainBlockIDsMap) > 0 {
							for hash, bIDs := range blockIDsMap {
								for _, bID := range bIDs {
									if _, exists := chainBlockIDsMap[bID]; exists && bID != blockID {
										// this transaction is already on our chain, the block is invalid
										return errors.NewBlockInvalidError("[UpdateTxMinedStatus][%s] block contains a transaction already on our chain: %s, blockID %d", block.Hash().String(), hash.String(), bID)
									}
								}
							}
						}

						break
					}
				}
			}

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("[UpdateTxMinedStatus][%s] error updating tx mined status", block.Hash().String(), err)
	}

	return nil
}
