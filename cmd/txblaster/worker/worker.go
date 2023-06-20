package worker

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TAAL-GmbH/ubsv/cmd/txblaster/extra"
	"github.com/TAAL-GmbH/ubsv/services/propagation/propagation_api"
	"github.com/TAAL-GmbH/ubsv/services/seeder/seeder_api"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"
)

var (
	prometheusProcessedTransactions prometheus.Counter
	prometheusInvalidTransactions   prometheus.Counter
	prometheusTransactionDuration   prometheus.Histogram
	prometheusTransactionSize       prometheus.Histogram
)

func init() {

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
}

type ipv6MulticastMsg struct {
	conn            *net.UDPConn
	IDBytes         []byte
	txExtendedBytes []byte
}

type Worker struct {
	utxoChan             chan *bt.UTXO
	startTime            time.Time
	numberOfOutputs      int
	numberOfTransactions uint32
	satoshisPerOutput    uint64
	seeder               seeder_api.SeederAPIClient
	rateLimiter          *rate.Limiter
	propagationServer    propagation_api.PropagationAPIClient
	kafkaProducer        sarama.SyncProducer
	kafkaTopic           string
	ipv6MulticastConn    *net.UDPConn
	ipv6MulticastChan    chan ipv6MulticastMsg
	printProgress        uint64
}

func NewWorker(
	numberOfOutputs int,
	numberOfTransactions uint32,
	satoshisPerOutput uint64,
	seeder seeder_api.SeederAPIClient,
	rateLimiter *rate.Limiter,
	propagationServer propagation_api.PropagationAPIClient,
	kafkaProducer sarama.SyncProducer,
	kafkaTopic string,
	ipv6MulticastConn *net.UDPConn,
	printProgress uint64,
) *Worker {

	// logger.Infof("Received transaction with txid %x and %d outputs", res.Txid, res.NumberOfOutputs)

	return &Worker{
		utxoChan:             make(chan *bt.UTXO, numberOfOutputs*2),
		numberOfOutputs:      numberOfOutputs,
		numberOfTransactions: numberOfTransactions,
		satoshisPerOutput:    satoshisPerOutput,
		seeder:               seeder,
		rateLimiter:          rateLimiter,
		propagationServer:    propagationServer,
		kafkaProducer:        kafkaProducer,
		kafkaTopic:           kafkaTopic,
		ipv6MulticastConn:    ipv6MulticastConn,
		ipv6MulticastChan:    make(chan ipv6MulticastMsg),
		printProgress:        printProgress,
	}
}

func (w *Worker) Start(ctx context.Context) error {
	// create new private key
	keySet, err := extra.New()
	if err != nil {
		return err
	}

	if _, err := w.seeder.CreateSpendableTransactions(ctx, &seeder_api.CreateSpendableTransactionsRequest{
		PrivateKey:           keySet.PrivateKey.Serialise(),
		NumberOfTransactions: w.numberOfTransactions,
		NumberOfOutputs:      uint32(w.numberOfOutputs),
		SatoshisPerOutput:    w.satoshisPerOutput,
	}); err != nil {
		return err
	}

	res, err := w.seeder.NextSpendableTransaction(ctx, &seeder_api.NextSpendableTransactionRequest{
		PrivateKey: keySet.PrivateKey.Serialise(),
	})
	if err != nil {
		return err
	}

	privateKey, _ := bec.PrivKeyFromBytes(bec.S256(), res.PrivateKey)

	script, err := bscript.NewP2PKHFromPubKeyBytes(privateKey.PubKey().SerialiseCompressed())
	if err != nil {
		panic(err)
	}

	go func(numberOfOutputs uint32) {
		// logger.Infof("Starting to send %d outputs to txChan", numberOfOutputs)
		for i := uint32(0); i < numberOfOutputs; i++ {
			u := &bt.UTXO{
				TxID:          bt.ReverseBytes(res.Txid),
				Vout:          i,
				LockingScript: script,
				Satoshis:      res.SatoshisPerOutput,
			}

			w.utxoChan <- u
		}
	}(res.NumberOfOutputs)

	w.startTime = time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil
		case utxo := <-w.utxoChan:
			if w.rateLimiter != nil {
				_ = w.rateLimiter.Wait(ctx)
			}

			err := w.fireTransactions(ctx, utxo, keySet)
			if err != nil {
				return fmt.Errorf("ERROR in fire transactions: %v", err)
			}
		}
	}
}

func (w *Worker) fireTransactions(ctx context.Context, u *bt.UTXO, keySet *extra.KeySet) error {
	timeStart := time.Now()

	tx := bt.NewTx()
	if err := tx.FromUTXOs(u); err != nil {
		prometheusInvalidTransactions.Inc()
		return err
	}

	if err := tx.PayTo(keySet.Script, u.Satoshis); err != nil {
		prometheusInvalidTransactions.Inc()
		return err
	}

	unlockerGetter := unlocker.Getter{PrivateKey: keySet.PrivateKey}
	if err := tx.FillAllInputs(ctx, &unlockerGetter); err != nil {
		prometheusInvalidTransactions.Inc()
		return err
	}

	if w.kafkaProducer != nil {
		err := w.publishToKafka(w.kafkaProducer, w.kafkaTopic, tx.TxIDBytes(), tx.ExtendedBytes())
		if err != nil {
			prometheusInvalidTransactions.Inc()
			return err
		}

	} else if w.ipv6MulticastConn != nil {
		err := w.sendOnIpv6Multicast(w.ipv6MulticastConn, tx.TxIDBytes(), tx.ExtendedBytes())
		if err != nil {
			prometheusInvalidTransactions.Inc()
			return err
		}

	} else {
		err := w.sendTransaction(ctx, tx.TxID(), tx.ExtendedBytes())
		if err != nil {
			prometheusInvalidTransactions.Inc()
			return err
		}
	}

	// increment prometheus counter
	prometheusProcessedTransactions.Inc()
	prometheusTransactionSize.Observe(float64(len(tx.ExtendedBytes())))
	prometheusTransactionDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	w.utxoChan <- &bt.UTXO{
		TxID:          tx.TxIDBytes(),
		Vout:          0,
		LockingScript: keySet.Script,
		Satoshis:      u.Satoshis,
	}

	return nil
}

var counter atomic.Uint64

func (w *Worker) publishToKafka(producer sarama.SyncProducer, topic string, txIDBytes []byte, txExtendedBytes []byte) error {
	// partition is the first byte of the txid - max 2^8 partitions = 256
	partitions, _ := gocore.Config().GetInt("validator_kafkaPartitions", 1)
	partition := binary.LittleEndian.Uint32(txIDBytes) % uint32(partitions)
	_, _, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic:     topic,
		Partition: int32(partition),
		Key:       sarama.ByteEncoder(txIDBytes),
		Value:     sarama.ByteEncoder(txExtendedBytes),
	})
	if err != nil {
		return err
	}

	counterLoad := counter.Add(1)
	if w.printProgress > 0 && counterLoad%w.printProgress == 0 {
		txPs := float64(0)
		ts := time.Since(w.startTime).Seconds()
		if ts > 0 {
			txPs = float64(counterLoad) / ts
		}
		fmt.Printf("Time for %d transactions to Kafka: %.2fs (%d tx/s)\r", counterLoad, time.Since(w.startTime).Seconds(), int(txPs))
	}

	return nil
}

func (w *Worker) sendOnIpv6Multicast(conn *net.UDPConn, IDBytes []byte, txExtendedBytes []byte) error {
	w.ipv6MulticastChan <- ipv6MulticastMsg{
		conn:            conn,
		IDBytes:         IDBytes,
		txExtendedBytes: txExtendedBytes,
	}

	counterLoad := counter.Add(1)
	if w.printProgress > 0 && counterLoad%w.printProgress == 0 {
		txPs := float64(0)
		ts := time.Since(w.startTime).Seconds()
		if ts > 0 {
			txPs = float64(counterLoad) / ts
		}
		fmt.Printf("Time for %d transactions to ipv6: %.2fs (%d tx/s)\r", counterLoad, time.Since(w.startTime).Seconds(), int(txPs))
	}

	return nil
}

func (w *Worker) sendTransaction(ctx context.Context, txID string, txExtendedBytes []byte) error {
	traceSpan := tracing.Start(ctx, "txBlaster:sendTransaction")
	defer traceSpan.Finish()

	traceSpan.SetTag("progname", "txblaster")
	traceSpan.SetTag("txid", txID)

	traceSpan.SetTag("transport", "grpc")

	if _, err := w.propagationServer.Set(traceSpan.Ctx, &propagation_api.SetRequest{
		Tx: txExtendedBytes,
	}); err != nil {
		return fmt.Errorf("error sending transaction to propagation server: %v", err)
	}

	counterLoad := counter.Add(1)
	if w.printProgress > 0 && counterLoad%w.printProgress == 0 {
		txPs := float64(0)
		ts := time.Since(w.startTime).Seconds()
		if ts > 0 {
			txPs = float64(counterLoad) / ts
		}
		fmt.Printf("Time for %d transactions: %.2fs (%d tx/s)\r", counterLoad, time.Since(w.startTime).Seconds(), int(txPs))
	}

	return nil
}
