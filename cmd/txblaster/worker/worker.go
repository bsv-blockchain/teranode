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

	"github.com/TAAL-GmbH/ubsv/services/coinbasetracker/coinbasetracker_api"
	"github.com/TAAL-GmbH/ubsv/services/propagation/propagation_api"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/go-utils"
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
	prometheusTransactionErrors     *prometheus.CounterVec
	logger                          = gocore.Log("worker", gocore.NewLogLevelFromString("DEBUG"))
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
	prometheusTransactionErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tx_blaster_errors",
			Help: "Number of tx blaster errors",
		},
		[]string{
			"function", //function raising the error
			"error",    // error returned
		},
	)
}

type Ipv6MulticastMsg struct {
	Conn            *net.UDPConn
	IDBytes         []byte
	TxExtendedBytes []byte
}

type Worker struct {
	utxoChan              chan *bt.UTXO
	startTime             time.Time
	numberOfOutputs       int
	numberOfTransactions  uint32
	satoshisPerOutput     uint64
	privateKey            *bec.PrivateKey
	address               string
	rateLimiter           *rate.Limiter
	propagationServers    []propagation_api.PropagationAPIClient
	kafkaProducer         sarama.SyncProducer
	kafkaTopic            string
	ipv6MulticastConn     *net.UDPConn
	ipv6MulticastChan     chan Ipv6MulticastMsg
	printProgress         uint64
	logIdsCh              chan string
	coinbaseTrackerClient coinbasetracker_api.CoinbasetrackerAPIClient
}

func NewWorker(
	numberOfOutputs int,
	numberOfTransactions uint32,
	satoshisPerOutput uint64,
	coinbasePrivKey string,
	rateLimiter *rate.Limiter,
	propagationServers []propagation_api.PropagationAPIClient,
	kafkaProducer sarama.SyncProducer,
	kafkaTopic string,
	ipv6MulticastConn *net.UDPConn,
	ipv6MulticastChan chan Ipv6MulticastMsg,
	printProgress uint64,
	logIdsCh chan string,
) *Worker {

	privateKey, err := wif.DecodeWIF(coinbasePrivKey)
	if err != nil {
		panic("can't decode coinbase priv key")
	}
	coinbaseAddr, err := bscript.NewAddressFromPublicKey(privateKey.PrivKey.PubKey(), true)

	if err != nil {
		panic(err)
	}

	return &Worker{
		utxoChan:             make(chan *bt.UTXO, numberOfOutputs*2),
		numberOfOutputs:      numberOfOutputs,
		numberOfTransactions: numberOfTransactions,
		satoshisPerOutput:    satoshisPerOutput,
		privateKey:           privateKey.PrivKey,
		address:              coinbaseAddr.AddressString,
		rateLimiter:          rateLimiter,
		propagationServers:   propagationServers,
		kafkaProducer:        kafkaProducer,
		kafkaTopic:           kafkaTopic,
		ipv6MulticastConn:    ipv6MulticastConn,
		ipv6MulticastChan:    ipv6MulticastChan,
		printProgress:        printProgress,
		logIdsCh:             logIdsCh,
	}
}

func (w *Worker) Start(ctx context.Context) error {

	keysetScript, err := bscript.NewP2PKHFromPubKeyEC(w.privateKey.PubKey())
	if err != nil {
		return err
	}
	keySet := &extra.KeySet{
		PrivateKey: w.privateKey,
		PublicKey:  w.privateKey.PubKey(),
		Script:     keysetScript,
	}

	coinbaseTrackerAddr, ok := gocore.Config().Get("coinbasetracker_grpcAddress")
	if !ok {
		panic("no coinbasetracker_grpcAddress setting found")
	}

	conn, err := utils.GetGRPCClient(ctx, coinbaseTrackerAddr, &utils.ConnectionOptions{})
	if err != nil {
		logger.Errorf("error creating connection for coinbaseTracker %+v: %v", coinbaseTrackerAddr, err)
		panic("error creating connection for coinbaseTracker")
	}
	w.coinbaseTrackerClient = coinbasetracker_api.NewCoinbasetrackerAPIClient(conn)
	logger.Debugf("coinbaseAddr: %s", w.address)

	utxos, err := w.getUtxosFromCoinbaseTracker(50)
	if err != nil {
		logger.Errorf("error getting utxos from coinbase %+v", err)

		panic("error getting Utxos from coinbaseTracker")
	}
	logger.Debugf("received utxos %+v", utxos)

	script, err := bscript.NewP2PKHFromPubKeyBytes(w.privateKey.PubKey().SerialiseCompressed())
	if err != nil {
		prometheusTransactionErrors.WithLabelValues("Start", err.Error()).Inc()
		logger.Errorf("Failed to create private key from pub key: %v", err)
		panic(err)
	}

	var totalSatoshis uint64
	inputUtxos := make([]*coinbasetracker_api.Utxo, 0)
	for _, utxo := range utxos {
		if utxo.Satoshis == 0 {
			continue
		}
		if utxo.Satoshis > w.satoshisPerOutput {
			//  if utxo amount  > satoshisPerOutput divide it into multiple outputs
			// 1. we get 500000000 satoshis from coinbase
			// 2. numberOfOutputs is 100. This number should override the satoshisPerOuptut
			// 3. we divide 500000000 / 100 = 5000000
			// if numberOfOutputs is 10
			// and satoshisPerOuptut is 10
			// and we have 15 satoshis in the utxo
			// we want 1 output of 10 and change of 5

			actualOutputs, change := w.calculateOutputs(utxo.Satoshis)
			go func(numberOfOutputs int, txId []byte) {

				logger.Infof("Starting to send %d outputs to txChan", numberOfOutputs)
				for i := 0; i < numberOfOutputs; i++ {

					u := &bt.UTXO{
						TxID:          bt.ReverseBytes(txId),
						Vout:          uint32(i),
						LockingScript: script,
						Satoshis:      w.satoshisPerOutput,
					}

					w.utxoChan <- u
				}
				if change > 0 {
					u := &bt.UTXO{
						TxID:          bt.ReverseBytes(txId),
						Vout:          uint32(numberOfOutputs),
						LockingScript: script,
						Satoshis:      change,
					}

					w.utxoChan <- u
				}

			}(int(actualOutputs), utxo.TxId)

			// 4. we send 100 outputs of 5000000 satoshis each
		} else if utxo.Satoshis == w.satoshisPerOutput {
			// if utxo amount == satoshisPerOutput send it directly
			go func(numberOfOutputs int, txId []byte) {
				logger.Infof("Starting to send %d outputs to txChan", numberOfOutputs)
				for i := 0; i < numberOfOutputs; i++ {

					u := &bt.UTXO{
						TxID:          bt.ReverseBytes(txId),
						Vout:          uint32(i),
						LockingScript: script,
						Satoshis:      w.satoshisPerOutput,
					}

					w.utxoChan <- u
				}
			}(w.numberOfOutputs, utxo.TxId)

		} else if utxo.Satoshis < w.satoshisPerOutput {
			// if utxo amout < satoshisPerOutput add it to the next utxo
			totalSatoshis += utxo.Satoshis
			inputUtxos = append(inputUtxos, utxo)
			// TODO: implememt this case
			_ = inputUtxos
			continue
		}

	}

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

func (w *Worker) calculateOutputs(utxoSats uint64) (uint64, uint64) {

	// Calculate maximum satoshis required
	maxSatoshisRequired := uint64(w.numberOfOutputs) * w.satoshisPerOutput

	var actualOutputs, change uint64

	if utxoSats < maxSatoshisRequired {
		actualOutputs = utxoSats / w.satoshisPerOutput
		change = utxoSats % w.satoshisPerOutput
	} else {
		actualOutputs = uint64(w.numberOfOutputs)
		change = utxoSats - maxSatoshisRequired
	}
	return actualOutputs, change
}

func (w *Worker) getUtxosFromCoinbaseTracker(amount uint64) ([]*coinbasetracker_api.Utxo, error) {
	ctx := context.Background()
	var resp *coinbasetracker_api.GetUtxoResponse
	var err error
	for i := 0; i < 10; i++ {
		resp, err = w.coinbaseTrackerClient.GetUtxos(ctx, &coinbasetracker_api.GetUtxoRequest{
			Address: w.address,
			Amount:  amount,
		})
		if err == nil {
			break
		}
		t := time.NewTimer(time.Second * 1)
		<-t.C
		logger.Debugf("retrying GetUtxos %d time", i+1)
	}

	if resp == nil || resp.Utxos == nil {
		return nil, fmt.Errorf("no utxos received from coinbasetracker")
	}

	utxos := make([]*coinbasetracker_api.Utxo, 0)
	utxos = append(utxos, resp.Utxos...)

	return utxos, nil
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

	if w.logIdsCh != nil {
		w.logIdsCh <- tx.TxID()
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
	w.ipv6MulticastChan <- Ipv6MulticastMsg{
		Conn:            conn,
		IDBytes:         IDBytes,
		TxExtendedBytes: txExtendedBytes,
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

	var propagationError error = nil
	for _, propagationServer := range w.propagationServers {
		if _, err := propagationServer.Set(traceSpan.Ctx, &propagation_api.SetRequest{
			Tx: txExtendedBytes,
		}); err != nil {
			propagationError = err
		}
	}
	if propagationError != nil {
		return fmt.Errorf("error sending transaction to propagation server: %s", propagationError.Error())
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
