package model

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

type txMinedStatus interface {
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error
}

type txMinedMessage struct {
	ctx         context.Context
	logger      ulogger.Logger
	txMetaStore txMinedStatus
	block       *Block
	blockID     uint32
	done        chan error
}

var (
	txMinedChan = make(chan *txMinedMessage, 1024)
	txMinedOnce sync.Once

	// prometheus metrics
	prometheusUpdateTxMinedCh       prometheus.Counter
	prometheusUpdateTxMinedQueue    prometheus.Gauge
	prometheusUpdateTxMinedDuration prometheus.Histogram
)

func initWorker() {
	prometheusUpdateTxMinedCh = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "model",
		Name:      "update_tx_mined_ch",
		Help:      "Number of tx mined messages sent to the worker",
	})
	prometheusUpdateTxMinedQueue = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "model",
		Name:      "update_tx_mined_queue",
		Help:      "Number of tx mined messages in the queue",
	})
	prometheusUpdateTxMinedDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "model",
		Name:      "update_tx_mined_duration",
		Help:      "Duration of updating tx mined status",
		Buckets:   util.MetricsBucketsSeconds,
	})

	go func() {
		for msg := range txMinedChan {
			if err := updateTxMinedStatus(
				msg.ctx,
				msg.logger,
				msg.txMetaStore,
				msg.block,
				msg.blockID,
			); err != nil {
				msg.done <- err
			} else {
				msg.done <- nil
			}

			prometheusUpdateTxMinedQueue.Set(float64(len(txMinedChan)))
		}
	}()
}

func UpdateTxMinedStatus(ctx context.Context, logger ulogger.Logger, txMetaStore txMinedStatus, block *Block, blockID uint32) error {

	// start the worker, if not already started
	txMinedOnce.Do(initWorker)

	startTime := time.Now()
	defer func() {
		prometheusUpdateTxMinedDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	done := make(chan error)

	txMinedChan <- &txMinedMessage{
		ctx:         ctx,
		logger:      logger,
		txMetaStore: txMetaStore,
		block:       block,
		blockID:     blockID,
		done:        done,
	}

	prometheusUpdateTxMinedCh.Inc()

	return <-done
}

func updateTxMinedStatus(ctx context.Context, logger ulogger.Logger, txMetaStore txMinedStatus, block *Block, blockID uint32) error {
	timeStart := time.Now()
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "UpdateTxMinedStatus")
	defer func() {
		span.Finish()
	}()

	logger.Infof("[UpdateTxMinedStatus][%s] blockID %d for %d subtrees", block.Hash().String(), blockID, len(block.Subtrees))

	updateTxMinedStatusEnabled := gocore.Config().GetBool("utxostore_updateTxMinedStatus", true)
	if !updateTxMinedStatusEnabled {
		return nil
	}

	if blockID > 0 {
		// mark coinbase tx as mined
		// NOTE: it's possible this block is not the tip of the chain so the coinbaseTx may not exist in the utxo store
		// therefore we log the error rather than return an error in this case only
		coinbaseHashes := []*chainhash.Hash{block.CoinbaseTx.TxIDChainHash()}
		logger.Debugf("[UpdateTxMinedStatus][%s] SetMinedMulti for coinbase tx %s in block ID %d", block.Hash().String(), block.CoinbaseTx.TxIDChainHash().String(), blockID)
		if err := txMetaStore.SetMinedMulti(ctx, coinbaseHashes, blockID); err != nil {
			logger.Warnf("[UpdateTxMinedStatus][%s] failed to set mined info on coinbase tx: %v", block.Hash().String(), err)
		}
	}

	maxMinedRoutines, _ := gocore.Config().GetInt("utxostore_maxMinedRoutines", 128)
	maxMinedBatchSize, _ := gocore.Config().GetInt("utxostore_maxMinedBatchSize", 1024)

	g, gCtx := errgroup.WithContext(spanCtx)
	g.SetLimit(maxMinedRoutines)

	maxRetries := 10
	for subtreeIdx, subtree := range block.SubtreeSlices {
		subtreeIdx := subtreeIdx
		subtree := subtree
		g.Go(func() error {
			hashes := make([]*chainhash.Hash, 0, maxMinedBatchSize)
			for idx := 0; idx < len(subtree.Nodes); idx++ {
				if subtreeIdx == 0 && idx == 0 {
					continue
				}

				if idx > 0 && idx%maxMinedBatchSize == 0 {
					logger.Debugf("[UpdateTxMinedStatus][%s] SetMinedMulti for %d hashes, batch %d, for subtree %s in block %d", block.Hash().String(), len(hashes), idx/maxMinedBatchSize, block.Subtrees[subtreeIdx].String(), blockID)
					retries := 0
					for {
						if err := txMetaStore.SetMinedMulti(gCtx, hashes, blockID); err != nil {
							if retries >= maxRetries {
								return fmt.Errorf("[UpdateTxMinedStatus][%s] error setting mined tx: %v", block.Hash().String(), err)
							} else {
								backoff := time.Duration(1+(2*retries)) * time.Second
								logger.Warnf("[UpdateTxMinedStatus][%s] error setting mined tx, retrying in %s: %v", block.Hash().String(), backoff.String(), err)
								time.Sleep(backoff)
							}
						} else {
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
					logger.Debugf("[UpdateTxMinedStatus][%s] SetMinedMulti for %d hashes, remainder batch, for subtree %s in block %d", block.Hash().String(), len(hashes), block.Subtrees[subtreeIdx].String(), blockID)
					if err := txMetaStore.SetMinedMulti(gCtx, hashes, blockID); err != nil {
						if retries >= maxRetries {
							return fmt.Errorf("[UpdateTxMinedStatus][%s] error setting remainder batch mined tx: %v", block.Hash().String(), err)
						} else {
							backoff := time.Duration(1+(2*retries)) * time.Second
							logger.Warnf("[UpdateTxMinedStatus][%s] error setting remainder batch mined tx, retrying in %s: %v", block.Hash().String(), backoff.String(), err)
							time.Sleep(backoff)
						}
						retries++
					} else {
						break
					}
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("[UpdateTxMinedStatus][%s] error updating tx mined status: %w", block.Hash().String(), err)
	}

	logger.Infof("[UpdateTxMinedStatus][%s] blockID %d DONE in %s", block.Hash().String(), blockID, time.Since(timeStart).String())

	return nil
}
