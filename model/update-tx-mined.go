package model

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

type txMinedStatus interface {
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error)
}

type txMinedMessage struct {
	ctx            context.Context
	logger         ulogger.Logger
	txMetaStore    txMinedStatus
	block          *Block
	blockID        uint32
	chainBlockIDs  []uint32
	onLongestChain bool
	unsetMined     bool
	done           chan error
}

var (
	txMinedChan      = make(chan *txMinedMessage, 1024)
	txMinedOnce      sync.Once
	workerSettings   *settings.Settings
	workerSettingsMu sync.RWMutex

	// inFlightBlocks tracks blocks currently being processed to prevent duplicate processing
	inFlightBlocks   = make(map[uint32]bool)
	inFlightBlocksMu sync.Mutex

	// prometheus metrics
	prometheusUpdateTxMinedCh         prometheus.Counter
	prometheusUpdateTxMinedQueue      prometheus.Gauge
	prometheusUpdateTxMinedDuration   prometheus.Histogram
	prometheusUpdateTxMinedDuplicates prometheus.Counter
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
	prometheusUpdateTxMinedDuplicates = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "model",
		Name:      "update_tx_mined_duplicates",
		Help:      "Number of duplicate tx mined update attempts blocked",
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
					msg.onLongestChain,
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
	block *Block, blockID uint32, chainBlockIDs []uint32, onLongestChain bool, unsetMined ...bool) error {
	// start the worker, if not already started
	txMinedOnce.Do(func() { initWorker(tSettings) })

	// Check if this block is already being processed and mark it as in-flight atomically
	inFlightBlocksMu.Lock()
	if inFlightBlocks[blockID] {
		inFlightBlocksMu.Unlock()
		logger.Infof("[UpdateTxMinedStatus][%s] blockID %d is already being processed, ignoring duplicate call", block.Hash().String(), blockID)
		prometheusUpdateTxMinedDuplicates.Inc()
		return errors.NewBlockParentNotMinedError("[UpdateTxMinedStatus][%s] blockID %d is already being processed", block.Hash().String(), blockID)
	}
	// Mark block as in-flight immediately to prevent duplicate processing
	inFlightBlocks[blockID] = true
	inFlightBlocksMu.Unlock()

	// Ensure block is removed from in-flight tracking when done
	defer func() {
		inFlightBlocksMu.Lock()
		delete(inFlightBlocks, blockID)
		inFlightBlocksMu.Unlock()
	}()

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
		ctx:            ctx,
		logger:         logger,
		txMetaStore:    txMetaStore,
		block:          block,
		blockID:        blockID,
		chainBlockIDs:  chainBlockIDs,
		onLongestChain: onLongestChain,
		unsetMined:     unsetTxMined, // whether to unset the mined status
		done:           done,
	}

	prometheusUpdateTxMinedCh.Inc()

	return <-done
}

func updateTxMinedStatus(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, txMetaStore txMinedStatus,
	block *Block, blockID uint32, chainBlockIDsMap map[uint32]bool, onLongestChain, unsetMined bool) (err error) {
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

	var (
		blockInvalidError   error
		blockInvalidErrorMu = sync.Mutex{}
		setMinedErrorCount  = atomic.Uint64{}
	)

	for subtreeIdx, subtree := range block.SubtreeSlices {
		subtreeIdx := subtreeIdx
		subtree := subtree

		if subtree == nil {
			if unsetMined {
				// if unsetting, we can ignore missing subtrees
				logger.Warnf("[UpdateTxMinedStatus][%s] unsetting mined status, missing subtree %d of %d - ignoring", block.String(), subtreeIdx, len(block.Subtrees))
				continue
			}

			return errors.NewProcessingError("[UpdateTxMinedStatus][%s] missing subtree %d of %d", block.String(), subtreeIdx, len(block.Subtrees))
		}

		minedBlockInfo := utxo.MinedBlockInfo{
			BlockID:        blockID,
			BlockHeight:    block.Height,
			SubtreeIdx:     subtreeIdx,
			OnLongestChain: onLongestChain,
			UnsetMined:     unsetMined,
		}

		g.Go(func() error {
			gCtx, _, endSpan := tracing.Tracer("model").Start(gCtx, "updateTxMinedStatus",
				tracing.WithDebugLogMessage(logger, "[UpdateTxMinedStatus][%s][%s] starting processing", block.String(), block.Subtrees[subtreeIdx].String()),
			)
			defer endSpan()

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

					// Check if context is canceled before attempting
					select {
					case <-gCtx.Done():
						return errors.NewProcessingError("[UpdateTxMinedStatus][%s] context canceled during retry", block.Hash().String(), gCtx.Err())
					default:
					}

					blockIDsMap, err := txMetaStore.SetMinedMulti(gCtx, hashes, minedBlockInfo)
					if err != nil {
						// Log error, increment counter, and continue processing all transactions
						logger.Warnf("[UpdateTxMinedStatus][%s] error setting mined tx for batch %d/%d: %v", block.Hash().String(), batchNr, batchTotal, err)
						setMinedErrorCount.Add(1)
					} else {
						if !minedBlockInfo.UnsetMined {
							// check that all blockIDs are not already on our chain
							if len(chainBlockIDsMap) > 0 {
								for hash, bIDs := range blockIDsMap {
									for _, bID := range bIDs {
										if _, exists := chainBlockIDsMap[bID]; exists && bID != blockID {
											// this transaction is already on our chain, the block is invalid
											blockInvalidErrorMu.Lock()
											blockInvalidError = errors.NewBlockInvalidError("[UpdateTxMinedStatus][%s] block contains a transaction already on our chain: %s, blockID %d", block.Hash().String(), hash.String(), bID)
											blockInvalidErrorMu.Unlock()
										}
									}
								}
							}
						}
					}

					hashes = hashes[:0] // reuse the slice, just reset length
				}
			}

			if len(hashes) > 0 {
				// Check if context is canceled before attempting
				select {
				case <-gCtx.Done():
					return errors.NewProcessingError("[UpdateTxMinedStatus][%s] context canceled during remainder SetMinedMulti", block.Hash().String(), gCtx.Err())
				default:
				}

				logger.Debugf("[UpdateTxMinedStatus][%s][%s] for %d remainder hashes", block.String(), block.Subtrees[subtreeIdx].String(), len(hashes))

				blockIDsMap, err := txMetaStore.SetMinedMulti(gCtx, hashes, minedBlockInfo)
				if err != nil {
					// Log error, increment counter, and continue processing all transactions
					logger.Warnf("[UpdateTxMinedStatus][%s] error setting mined tx for remainder batch: %v", block.Hash().String(), err)
					setMinedErrorCount.Add(1)
				} else {
					if !minedBlockInfo.UnsetMined {
						// check that all blockIDs are not already on our chain
						if len(chainBlockIDsMap) > 0 {
							for hash, bIDs := range blockIDsMap {
								for _, bID := range bIDs {
									if _, exists := chainBlockIDsMap[bID]; exists && bID != blockID {
										// this transaction is already on our chain, the block is invalid
										blockInvalidErrorMu.Lock()
										blockInvalidError = errors.NewBlockInvalidError("[UpdateTxMinedStatus][%s] block contains a transaction already on our chain: %s, blockID %d", block.Hash().String(), hash.String(), bID)
										blockInvalidErrorMu.Unlock()
									}
								}
							}
						}
					}

				}
			}

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("[UpdateTxMinedStatus][%s] error updating tx mined status", block.Hash().String(), err)
	}

	// Check if there were any SetMinedMulti errors
	if setMinedErrorCount.Load() > 0 {
		if unsetMined {
			// For invalid blocks, we've already logged the errors - continue without error
			logger.Warnf("[UpdateTxMinedStatus][%s] completed with %d SetMinedMulti errors for invalid block (already logged)", block.Hash().String(), setMinedErrorCount.Load())
		} else {
			// For valid blocks, SetMinedMulti errors are critical - return error
			return errors.NewProcessingError("[UpdateTxMinedStatus][%s] failed to set mined status for %d batches", block.Hash().String(), setMinedErrorCount.Load())
		}
	}

	// if the block was found to be invalid, return that error
	if blockInvalidError != nil {
		return blockInvalidError
	}

	return nil
}
