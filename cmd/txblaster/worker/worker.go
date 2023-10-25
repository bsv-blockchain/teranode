package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitcoin-sv/ubsv/services/coinbase"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/go-utils"
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
	logger             utils.Logger
	rateLimiter        *rate.Limiter
	propagationServers map[string]propagation_api.PropagationAPIClient
	kafkaProducer      sarama.SyncProducer
	kafkaTopic         string
	ipv6MulticastConn  *net.UDPConn
	ipv6MulticastChan  chan Ipv6MulticastMsg
	printProgress      uint64
	logIdsCh           chan string
	totalTransactions  *atomic.Uint64
	globalStartTime    *time.Time
	utxoChan           chan *bt.UTXO
	startTime          time.Time
	unlocker           bt.UnlockerGetter
	address            *bscript.Address
}

func NewWorker(
	logger utils.Logger,
	rateLimiter *rate.Limiter,
	propagationServers map[string]propagation_api.PropagationAPIClient,
	kafkaProducer sarama.SyncProducer,
	kafkaTopic string,
	ipv6MulticastConn *net.UDPConn,
	ipv6MulticastChan chan Ipv6MulticastMsg,
	printProgress uint64,
	logIdsCh chan string,
	totalTransactions *atomic.Uint64,
	globalStartTime *time.Time,
) (*Worker, error) {

	// Generate a random private key
	privateKey, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		return nil, err
	}

	unlocker := unlocker.Getter{PrivateKey: privateKey}

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		return nil, fmt.Errorf("can't create coinbase address: %v", err)
	}

	return &Worker{
		logger:             logger,
		rateLimiter:        rateLimiter,
		propagationServers: propagationServers,
		kafkaProducer:      kafkaProducer,
		kafkaTopic:         kafkaTopic,
		ipv6MulticastConn:  ipv6MulticastConn,
		ipv6MulticastChan:  ipv6MulticastChan,
		unlocker:           &unlocker,
		printProgress:      printProgress,
		totalTransactions:  totalTransactions,
		logIdsCh:           logIdsCh,
		globalStartTime:    globalStartTime,
		address:            address,
		utxoChan:           make(chan *bt.UTXO, 10),
	}, nil
}

func (w *Worker) Init(ctx context.Context) error {
	var err error

	prometheusWorkers.Inc()
	defer func() {
		prometheusWorkers.Dec()
		if err != nil {
			prometheusWorkerErrors.WithLabelValues("Start", err.Error()).Inc()
		}
	}()

	coinbaseClient, err := coinbase.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("error creating coinbase tracker client: %v", err)
	}

	timeStart := time.Now()
	w.startTime = timeStart

	tx, err := coinbaseClient.RequestFunds(ctx, w.address.AddressString)
	if err != nil {
		return fmt.Errorf("error getting utxo from coinbaseTracker: %v", err)
	}

	w.logger.Debugf(" \U0001fa99  Got tx from faucet txid:%s", tx.TxIDChainHash().String())

	// Put the first utxo on the channel
	w.utxoChan <- &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: tx.Outputs[0].LockingScript,
		Satoshis:      tx.Outputs[0].Satoshis,
	}

	return nil
}

func (w *Worker) Start(ctx context.Context) error {
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case utxo := <-w.utxoChan:
			if w.rateLimiter != nil {
				_ = w.rateLimiter.Wait(ctx)
			}

			tx := bt.NewTx()
			_ = tx.FromUTXOs(utxo)
			_ = tx.AddP2PKHOutputFromAddress(w.address.AddressString, utxo.Satoshis)

			if err := tx.FillAllInputs(ctx, w.unlocker); err != nil {
				prometheusInvalidTransactions.Inc()
				return fmt.Errorf("error filling initial inputs: %v", err)
			}

			if err := w.sendTransaction(ctx, tx); err != nil {
				return fmt.Errorf("error sending initial transaction: %v", err)
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
		}
	}
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

func (w *Worker) sendTransaction(ctx context.Context, tx *bt.Tx) error {
	traceSpan := tracing.Start(ctx, "txBlaster:sendTransaction")
	defer traceSpan.Finish()

	traceSpan.SetTag("progname", "txblaster")
	traceSpan.SetTag("txid", tx.TxIDChainHash().String())
	traceSpan.SetTag("transport", "grpc")

	var wg sync.WaitGroup
	errorCount := 0

	for address, propagationServer := range w.propagationServers {
		a := address
		p := propagationServer

		wg.Add(1)

		go func() {
			defer wg.Done()

			_, err := p.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
				Tx: tx.ExtendedBytes(),
			})

			if err != nil {
				if !errors.Is(err, context.Canceled) {
					w.logger.Errorf("error sending transaction to %s: %v", a, err)
					errorCount++
				}
			}
		}()
	}
	wg.Wait()

	if float32(errorCount)/float32(len(w.propagationServers)) > 0.5 {
		return fmt.Errorf("error sending transaction to more than half of the propagation servers")
	}

	counterLoad := counter.Add(1)
	if w.printProgress > 0 && counterLoad%w.printProgress == 0 {
		txPs := float64(0)
		ts := time.Since(*w.globalStartTime).Seconds()
		if ts > 0 {
			txPs = float64(counterLoad) / ts
		}
		fmt.Printf("Time for %d transactions: %.2fs (%d tx/s)\r", counterLoad, time.Since(*w.globalStartTime).Seconds(), int(txPs))
	}

	return nil
}
