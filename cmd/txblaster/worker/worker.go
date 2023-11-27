package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitcoin-sv/ubsv/services/coinbase"
	"github.com/bitcoin-sv/ubsv/services/p2p"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"
)

var (
	prometheusWorkers               prometheus.Gauge
	prometheusProcessedTransactions prometheus.Counter
	prometheusInvalidTransactions   prometheus.Counter
	prometheusTransactionDuration   prometheus.Histogram
	prometheusTransactionSize       prometheus.Histogram
	prometheusWorkerErrors          *prometheus.CounterVec
	// prometheusTransactionErrors     *prometheus.CounterVec
)

// ContextKey type
// Create type to avoid collisions with context.withSpan
type ContextKey int

// ContextAccountIDKey constant
const (
	ContextDetails ContextKey = iota
	ContextTxid
	ContextRetry
)

func init() {
	prometheusWorkers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "tx_blaster_workers",
			Help: "Number of workers running",
		},
	)
	prometheusProcessedTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tx_blaster_processed_transactions",
			Help: "Number of transactions processed by the tx blaster",
		},
	)
	prometheusInvalidTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tx_blaster_invalid_transactions",
			Help: "Number of transactions found invalid by the tx blaster",
		},
	)
	prometheusTransactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "tx_blaster_transactions_duration",
			Help: "Duration of transaction processing by the tx blaster",
		},
	)
	prometheusTransactionSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "tx_blaster_transactions_size",
			Help: "Size of transactions processed by the tx blaster",
		},
	)
	prometheusWorkerErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tx_blaster_worker_errors",
			Help: "Number of tx blaster worker errors",
		},
		[]string{
			"function", //function raising the error
			"error",    // error returned
		},
	)
	// prometheusTransactionErrors = promauto.NewCounterVec(
	// 	prometheus.CounterOpts{
	// 		Name: "tx_blaster_errors",
	// 		Help: "Number of tx blaster errors",
	// 	},
	// 	[]string{
	// 		"function", //function raising the error
	// 		"error",    // error returned
	// 	},
	// )
}

type Ipv6MulticastMsg struct {
	Conn            *net.UDPConn
	IDBytes         []byte
	TxExtendedBytes []byte
}

type Worker struct {
	logger            ulogger.Logger
	rateLimiter       *rate.Limiter
	coinbaseClient    *coinbase.Client
	distributor       *distributor.Distributor
	kafkaProducer     sarama.SyncProducer
	kafkaTopic        string
	ipv6MulticastConn *net.UDPConn
	ipv6MulticastChan chan Ipv6MulticastMsg
	printProgress     uint64
	logIdsCh          chan string
	totalTransactions *atomic.Uint64
	globalStartTime   *time.Time
	utxoChan          chan *bt.UTXO
	startTime         time.Time
	unlocker          bt.UnlockerGetter
	address           *bscript.Address
	topic             *pubsub.Topic
	sentTxCache       *RollingCache
}

func NewWorker(
	logger ulogger.Logger,
	rateLimit float64,
	coinbaseClient *coinbase.Client,
	distributor *distributor.Distributor,
	kafkaProducer sarama.SyncProducer,
	kafkaTopic string,
	ipv6MulticastConn *net.UDPConn,
	ipv6MulticastChan chan Ipv6MulticastMsg,
	printProgress uint64,
	logIdsCh chan string,
	totalTransactions *atomic.Uint64,
	globalStartTime *time.Time,
	topic *pubsub.Topic,
) (*Worker, error) {

	// Generate a random private key
	privateKey, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		return nil, err
	}

	unlockerGetter := unlocker.Getter{PrivateKey: privateKey}

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		return nil, fmt.Errorf("can't create coinbase address: %v", err)
	}

	var rateLimiter *rate.Limiter
	if rateLimit > 0 {
		var rateLimitDuration time.Duration
		if rateLimit < 1 {
			rateLimitDuration = time.Second * time.Duration(1/rateLimit)
		} else {
			rateLimitDuration = time.Second / time.Duration(rateLimit)
		}
		rateLimiter = rate.NewLimiter(rate.Every(rateLimitDuration), 1)
	}

	return &Worker{
		logger:            logger,
		rateLimiter:       rateLimiter,
		coinbaseClient:    coinbaseClient,
		distributor:       distributor,
		kafkaProducer:     kafkaProducer,
		kafkaTopic:        kafkaTopic,
		ipv6MulticastConn: ipv6MulticastConn,
		ipv6MulticastChan: ipv6MulticastChan,
		unlocker:          &unlockerGetter,
		printProgress:     printProgress,
		totalTransactions: totalTransactions,
		logIdsCh:          logIdsCh,
		globalStartTime:   globalStartTime,
		address:           address,
		utxoChan:          make(chan *bt.UTXO, 10000),
		topic:             topic,
		sentTxCache:       NewRollingCache(1000),
	}, nil
}

func (w *Worker) Init(ctx context.Context) (err error) {
	timeStart := time.Now()
	w.startTime = timeStart

	tx, err := w.coinbaseClient.RequestFunds(ctx, w.address.AddressString, true)
	if err != nil {
		return fmt.Errorf("error getting utxo from coinbaseTracker %s: %v", w.address.AddressString, err)
	}

	w.sentTxCache.Add(tx.TxIDChainHash().String())
	_, err = w.distributor.SendTransaction(ctx, tx)
	if err != nil {
		return fmt.Errorf("error sending funding transaction %s: %v", tx.TxIDChainHash().String(), err)
	}

	w.logger.Debugf(" \U0001fa99  Got tx from faucet txid:%s with %d outputs", tx.TxIDChainHash().String(), len(tx.Outputs))

	for i, output := range tx.Outputs {
		w.utxoChan <- &bt.UTXO{
			TxIDHash:      tx.TxIDChainHash(),
			Vout:          uint32(i),
			LockingScript: output.LockingScript,
			Satoshis:      output.Satoshis,
		}
	}
	// Put the first utxo on the channel

	return nil
}

func (w *Worker) Start(ctx context.Context) (err error) {
	start := time.Now()

	prometheusWorkers.Inc()
	defer func() {
		prometheusWorkers.Dec()
		if err != nil {
			prometheusWorkerErrors.WithLabelValues("Start", err.Error()).Inc()
		}
	}()

	var utxo *bt.UTXO
	var tx *bt.Tx
	var previousUtxo *bt.UTXO
	var retries int
	var counterLoad uint64
	var txPs float64
	var ts float64
	if w.topic != nil {
		sub, err := w.topic.Subscribe()
		if err != nil {
			panic(err)
		}

		go func() {
			defer sub.Cancel()
			var rejectedTxMsg p2p.RejectedTxMessage
			// Continuously check messages
			for {
				msg, err := sub.Next(ctx)
				w.logger.Errorf("Error reading next rejected tx message: %+v", err)
				if err != nil {

					return
				}
				rejectedTxMsg = p2p.RejectedTxMessage{}
				err = json.Unmarshal(msg.Data, &rejectedTxMsg)
				if err != nil {
					w.logger.Errorf("json unmarshal error: ", err)
					continue
				}
				w.logger.Debugf("Rejected tx msg: txId %s\n", rejectedTxMsg.TxId)
				if w.sentTxCache.Contains(rejectedTxMsg.TxId) {
					w.logger.Errorf("Rejected txId %s found in sentTxCache", rejectedTxMsg.TxId)
					// use error channel to kill worker
					return
				}
			}
		}()

	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case utxo = <-w.utxoChan:
			tx, err = w.sendTransactionFromUtxo(ctx, utxo)
			if err != nil {
				w.logger.Errorf("error sending transaction: %v", err)
				// resend parent and retry transaction
				if previousUtxo != nil {
					retries = 0
					for {
						w.logger.Infof("resending parent transaction: %s", previousUtxo.TxIDHash.String())
						_, err = w.sendTransactionFromUtxo(ctx, previousUtxo)
						if err == nil {
							// parent was re-sent, re-send this transaction
							_, err = w.sendTransactionFromUtxo(ctx, utxo)
							if err != nil {
								w.logger.Errorf("error resending parent transaction: %v", err)
								if retries > 3 {
									// return kills the worker
									return err
								}
							}
							break
						} else {
							w.logger.Errorf("error resending parent transaction: %v", err)
							if retries > 3 {
								// return kills the worker
								return err
							}
						}
						retries++
						time.Sleep(1 * time.Second)
					}
				} else {
					return err
				}
			}

			counterLoad = counter.Add(1)
			if w.printProgress > 0 && counterLoad%w.printProgress == 0 {
				txPs = float64(0)
				ts = time.Since(*w.globalStartTime).Seconds()
				if ts > 0 {
					txPs = float64(counterLoad) / ts
				}
				fmt.Printf("Time for %d transactions: %.2fs (%d tx/s)\r", counterLoad, time.Since(*w.globalStartTime).Seconds(), int(txPs))
			}

			// increment prometheus counter
			prometheusProcessedTransactions.Inc()
			prometheusTransactionSize.Observe(float64(len(tx.ExtendedBytes())))
			prometheusTransactionDuration.Observe(float64(time.Since(start).Microseconds()))
			w.totalTransactions.Add(1)

			btUtxo := &bt.UTXO{
				TxIDHash:      tx.TxIDChainHash(),
				Vout:          0,
				LockingScript: tx.Outputs[0].LockingScript,
				Satoshis:      tx.Outputs[0].Satoshis,
			}

			w.utxoChan <- btUtxo
			previousUtxo = btUtxo

			if w.rateLimiter != nil {
				_ = w.rateLimiter.Wait(ctx)
			}
		}
	}
}

func (w *Worker) sendTransactionFromUtxo(ctx context.Context, utxo *bt.UTXO) (tx *bt.Tx, err error) {
	tx = bt.NewTx()
	err = tx.FromUTXOs(utxo)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		return tx, fmt.Errorf("error adding utxo to tx: %v", err)
	}

	err = tx.AddP2PKHOutputFromAddress(w.address.AddressString, utxo.Satoshis)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		return tx, fmt.Errorf("error adding output to tx: %v", err)
	}

	if err = tx.FillAllInputs(ctx, w.unlocker); err != nil {
		prometheusInvalidTransactions.Inc()
		return tx, fmt.Errorf("error filling tx inputs: %v", err)
	}

	w.sentTxCache.Add(tx.TxIDChainHash().String())
	if _, err = w.distributor.SendTransaction(ctx, tx); err != nil {
		// return tx, fmt.Errorf("error sending transaction #%d: %v", counter.Load(), err)
		utxoHash, _ := util.UTXOHashFromInput(tx.Inputs[0])
		w.logger.Fatalf("error sending transaction: #%d txId: %s parentTxId: %s vout: %s hash: %s", counter.Load(), tx.TxIDChainHash().String(), utxo.TxIDHash.String(), utxo.Vout, utxoHash.String())
	}

	return tx, nil
}

var counter atomic.Uint64

// func (w *Worker) publishToKafka(producer sarama.SyncProducer, topic string, txIDBytes []byte, txExtendedBytes []byte) error {
// 	// partition is the first byte of the txid - max 2^8 partitions = 256
// 	partitions, _ := gocore.Config().GetInt("validator_kafkaPartitions", 1)
// 	partition := binary.LittleEndian.Uint32(txIDBytes) % uint32(partitions)
// 	_, _, err := producer.SendMessage(&sarama.ProducerMessage{
// 		Topic:     topic,
// 		Partition: int32(partition),
// 		Key:       sarama.ByteEncoder(txIDBytes),
// 		Value:     sarama.ByteEncoder(txExtendedBytes),
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	counterLoad := counter.Add(1)
// 	if w.printProgress > 0 && counterLoad%w.printProgress == 0 {
// 		txPs := float64(0)
// 		ts := time.Since(*w.globalStartTime).Seconds()
// 		if ts > 0 {
// 			txPs = float64(counterLoad) / ts
// 		}
// 		fmt.Printf("Time for %d transactions to Kafka: %.2fs (%d tx/s)\r", counterLoad, time.Since(*w.globalStartTime).Seconds(), int(txPs))
// 	}

// 	return nil
// }

// func (w *Worker) sendOnIpv6Multicast(conn *net.UDPConn, IDBytes []byte, txExtendedBytes []byte) error {
// 	w.ipv6MulticastChan <- Ipv6MulticastMsg{
// 		Conn:            conn,
// 		IDBytes:         IDBytes,
// 		TxExtendedBytes: txExtendedBytes,
// 	}

// 	counterLoad := counter.Add(1)
// 	if w.printProgress > 0 && counterLoad%w.printProgress == 0 {
// 		txPs := float64(0)
// 		ts := time.Since(*w.globalStartTime).Seconds()
// 		if ts > 0 {
// 			txPs = float64(counterLoad) / ts
// 		}
// 		fmt.Printf("Time for %d transactions to ipv6: %.2fs (%d tx/s)\r", counterLoad, time.Since(*w.globalStartTime).Seconds(), int(txPs))
// 	}

// 	return nil
// }
