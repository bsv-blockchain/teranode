package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/util"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/coinbase"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/bitcoin-sv/ubsv/util/p2p"
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
	prometheusInternalErrors        prometheus.Counter
	prometheusTransactionDuration   prometheus.Histogram
	prometheusTransactionSize       prometheus.Histogram
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

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
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
	prometheusInternalErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tx_blaster_internal_server_errors",
			Help: "Number of transactions found failing at server end",
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
}

type Ipv6MulticastMsg struct {
	Conn            *net.UDPConn
	IDBytes         []byte
	TxExtendedBytes []byte
}

type Worker struct {
	logger            ulogger.Logger
	rateLimiter       *rate.Limiter
	iterations        int
	coinbaseClient    *coinbase.Client
	distributors      []*distributor.Distributor
	kafkaProducer     util.KafkaProducerI
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
	iterations int,
	coinbaseClient *coinbase.Client,
	txDistributors []*distributor.Distributor,
	kafkaProducer util.KafkaProducerI,
	kafkaTopic string,
	ipv6MulticastConn *net.UDPConn,
	ipv6MulticastChan chan Ipv6MulticastMsg,
	printProgress uint64,
	logIdsCh chan string,
	totalTransactions *atomic.Uint64,
	globalStartTime *time.Time,
	topic *pubsub.Topic,
	useQuic bool,
) (*Worker, error) {
	initPrometheusMetrics()

	// Generate a random private key
	privateKey, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		return nil, err
	}

	unlockerGetter := unlocker.Getter{PrivateKey: privateKey}

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		return nil, errors.NewProcessingError("can't create coinbase address", err)
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

	//var rollingCache *RollingCache
	//if useQuic {
	//	rollingCache = NewRollingCache(100)
	//}

	return &Worker{
		logger:            logger,
		rateLimiter:       rateLimiter,
		iterations:        iterations,
		coinbaseClient:    coinbaseClient,
		distributors:      txDistributors,
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
		utxoChan:          make(chan *bt.UTXO, 1000),
		topic:             topic,
		sentTxCache:       nil,
	}, nil
}

func (w *Worker) Init(ctx context.Context) (err error) {
	w.startTime = time.Now()

	tx, err := w.coinbaseClient.RequestFunds(ctx, w.address.AddressString, true)
	if err != nil {
		return errors.NewServiceError("error getting utxo from coinbaseTracker %s", w.address.AddressString, err)
	}

	//if w.sentTxCache != nil {
	//	w.sentTxCache.Add(tx.TxIDChainHash().String())
	//}

	for outerRetry := 0; outerRetry < 3; outerRetry++ {

		//nolint:gosec // G404: Use of weak random number generator is acceptable here, not security-sensitive
		index := rand.Intn(len(w.distributors))

		responses, err := w.distributors[index].SendTransaction(ctx, tx)
		if err == nil {
			break
		}

		if errors.Is(err, errors.ErrServiceError) {
			return errors.NewServiceError("error sending funding transaction %s", tx.TxIDChainHash().String(), err)
		}

		// Go through each response and check for ErrBadRequest errors
		for _, response := range responses {
			if errors.Is(response.Error, errors.ErrServiceError) {
				return errors.NewServiceError("error sending funding transaction %s", tx.TxIDChainHash().String(), response.Error)
			}
		}

		if outerRetry == 2 { // Last retry
			return errors.NewServiceError("error sending funding transaction %s", tx.TxIDChainHash().String(), err)
		}

		// Retry in 5 seconds
		time.Sleep(5 * time.Second)
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
			w.logger.Errorf("Worker error: %v", err)
		}
	}()

	var utxo *bt.UTXO
	var tx *bt.Tx
	var counterLoad uint64
	var txPs float64
	var ts float64
	if w.topic != nil {
		sub, err := w.topic.Subscribe()
		if err != nil {
			return errors.NewServiceUnavailableError("error subscribing to topic", err)
		}

		go func() {
			defer sub.Cancel()
			var rejectedTxMsg p2p.RejectedTxMessage
			// Continuously check messages
			for i := 0; ; i++ {
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
				//if w.sentTxCache != nil && w.sentTxCache.Contains(rejectedTxMsg.TxId) {
				//	w.logger.Errorf("Rejected txId %s found in sentTxCache", rejectedTxMsg.TxId)
				//	// TODO (I think) use error channel to kill worker
				//	return
				//}
			}
		}()

	}
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case utxo = <-w.utxoChan:
			tx, err = w.sendTransactionFromUtxo(ctx, utxo)
			if err != nil {
				return errors.NewTxError("error sending transaction from utxo %s:%d", utxo.TxIDHash.String(), utxo.Vout, err)
			}

			counterLoad = counter.Add(1)
			if w.printProgress > 0 && counterLoad%w.printProgress == 0 {
				txPs = float64(0)
				ts = time.Since(*w.globalStartTime).Seconds()
				if ts > 0 {
					txPs = float64(counterLoad) / ts
				}
				// this needs to be a printf, since we are updating the same line
				fmt.Printf("Time for %d transactions: %.2fs (%d tx/s)\r", counterLoad, time.Since(*w.globalStartTime).Seconds(), int(txPs))
			}

			// increment prometheus counter
			prometheusProcessedTransactions.Inc()
			prometheusTransactionSize.Observe(float64(len(tx.ExtendedBytes())))
			prometheusTransactionDuration.Observe(float64(time.Since(start).Microseconds()))
			w.totalTransactions.Add(1)

			w.utxoChan <- &bt.UTXO{
				TxIDHash:      tx.TxIDChainHash(),
				Vout:          0,
				LockingScript: tx.Outputs[0].LockingScript,
				Satoshis:      tx.Outputs[0].Satoshis,
			}

			if w.rateLimiter != nil {
				_ = w.rateLimiter.Wait(ctx)
			}

		}

		if w.iterations >= 0 && i+1 >= w.iterations {
			return nil // Return nil to exit the worker after the specified iterations
		}
	}
}

func (w *Worker) sendTransactionFromUtxo(ctx context.Context, utxo *bt.UTXO) (tx *bt.Tx, err error) {
	tx = bt.NewTx()
	err = tx.FromUTXOs(utxo)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		return nil, errors.NewTxError("error adding utxo to tx", err)
	}

	err = tx.AddP2PKHOutputFromAddress(w.address.AddressString, utxo.Satoshis)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		return nil, errors.NewTxError("error adding output to tx", err)
	}

	if err = tx.FillAllInputs(ctx, w.unlocker); err != nil {
		prometheusInvalidTransactions.Inc()
		return nil, errors.NewTxError("error filling tx inputs", err)
	}

	//if w.sentTxCache != nil {
	//	w.sentTxCache.Add(tx.TxIDChainHash().String())
	//}

	// select 1 distributor at random
	//nolint:gosec //  G404: Use of weak random number generator (math/rand instead of crypto/rand) (gosec)
	d := w.distributors[rand.Intn(len(w.distributors))]
	if responses, err := d.SendTransaction(ctx, tx); err != nil {
		if errors.Is(err, errors.ErrTxInvalid) {
			prometheusInvalidTransactions.Inc()
		} else {
			// Go through each response and check for ErrBadRequest errors
			for _, response := range responses {
				if errors.Is(response.Error, errors.ErrTxInvalid) {
					prometheusInvalidTransactions.Inc()
				}
			}
		}

		if errors.Is(err, errors.ErrServiceError) {
			prometheusInternalErrors.Inc()
		} else {
			// Go through each response and check for ErrBadRequest errors
			for _, response := range responses {
				if errors.Is(response.Error, errors.ErrServiceError) {
					prometheusInternalErrors.Inc()
				}
			}
		}

		return nil, errors.NewTxError("error sending transaction #%d", counter.Load(), err)
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
