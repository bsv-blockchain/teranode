package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TAAL-GmbH/ubsv/cmd/txblaster/extra"
	_ "github.com/TAAL-GmbH/ubsv/k8sresolver"
	"github.com/TAAL-GmbH/ubsv/services/propagation/propagation_api"
	"github.com/TAAL-GmbH/ubsv/services/seeder/seeder_api"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/time/rate"
)

var logger utils.Logger
var startTime time.Time
var propagationServer propagation_api.PropagationAPIClient
var seederServer seeder_api.SeederAPIClient
var txChan chan *bt.UTXO
var printProgress uint64
var useHTTP bool
var httpURL string
var httpClient *http.Client

// Name used by build script for the binaries. (Please keep on single line)
const progname = "tx-blaster"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string
var kafkaProducer sarama.SyncProducer
var kafkaTopic string

var (
	prometheusProcessedTransactions prometheus.Counter
	prometheusInvalidTransactions   prometheus.Counter
	prometheusTransactionDuration   prometheus.Histogram
	prometheusTransactionSize       prometheus.Histogram
)

func init() {
	gocore.SetInfo(progname, version, commit)
	logger = gocore.Log("txblaster", gocore.NewLogLevelFromString("debug"))

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

func main() {
	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	workers := flag.Int("workers", runtime.NumCPU(), "how many workers to use for blasting")
	rateLimit := flag.Int("limit", -1, "rate limit tx/s")
	useHTTPFlag := flag.Bool("http", false, "use http instead of grpc to send transactions")
	printFlag := flag.Int("print", 0, "print out progress every x transactions")
	kafka := flag.String("kafka", "", "Kafka server URL - if applicable")

	flag.Parse()

	prometheusEndpoint, ok := gocore.Config().Get("prometheusEndpoint")
	if ok && prometheusEndpoint != "" {
		logger.Infof("Starting prometheus endpoint on %s", prometheusEndpoint)
		http.Handle(prometheusEndpoint, promhttp.Handler())
	}

	if kafka != nil && *kafka != "" {
		kafkaURL, err := url.Parse(*kafka)
		if err != nil {
			log.Fatalf("unable to parse kafka url: %v", err)
		}

		brokersUrl := []string{kafkaURL.Host}

		config := sarama.NewConfig()
		config.Version = sarama.V2_1_0_0

		var clusterAdmin sarama.ClusterAdmin
		clusterAdmin, err = sarama.NewClusterAdmin(strings.Split(kafkaURL.Host, ","), config)
		if err != nil {
			log.Fatal("Error while creating cluster admin: ", err.Error())
		}
		defer func() { _ = clusterAdmin.Close() }()

		partitions, _ := gocore.Config().GetInt("validator_kafkaPartitions", 1)
		replicationFactor, _ := gocore.Config().GetInt("validator_kafkaReplicationFactor", 1)
		_ = clusterAdmin.CreateTopic("txs", &sarama.TopicDetail{
			NumPartitions:     int32(partitions),
			ReplicationFactor: int16(replicationFactor),
		}, false)

		producer, err := ConnectProducer(brokersUrl)
		if err != nil {
			log.Fatalf("unable to connect to kafka: %v", err)
		}
		defer producer.Close()

		kafkaProducer = producer
		kafkaTopic = kafkaURL.Path[1:]
	}

	printProgress = uint64(*printFlag)

	if gocore.Config().GetBool("use_open_tracing", true) {
		logger.Infof("Starting open tracing")
		// closeTracer := tracing.InitOtelTracer()
		_, closer, err := utils.InitGlobalTracer("tx-blaster")
		if err != nil {
			panic(err)
		}

		defer closer.Close()
	}

	if *useHTTPFlag {
		useHTTP = *useHTTPFlag
		propagationHTTPAddress, ok := gocore.Config().Get("propagation_httpAddress")
		if !ok {
			panic("propagation_httpAddress not found in config")
		}
		httpURL = propagationHTTPAddress

		tr := &http.Transport{
			MaxConnsPerHost:   99999,
			DisableKeepAlives: false,
		}
		httpClient = &http.Client{Transport: tr}
	}

	go func() {
		_ = http.ListenAndServe("localhost:9199", nil)
	}()

	seederGrpcAddress, ok := gocore.Config().Get("seeder_grpcAddress")
	if !ok {
		panic("no seeder_grpcAddress setting found")
	}

	sConn, err := utils.GetGRPCClient(context.Background(), seederGrpcAddress, &utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		MaxRetries:  3,
	})
	if err != nil {
		panic(err)
	}
	seederServer = seeder_api.NewSeederAPIClient(sConn)

	propagationGrpcAddress, ok := gocore.Config().Get("propagation_grpcAddress")
	if !ok {
		panic("no propagation_grpcAddress setting found")
	}

	pConn, err := utils.GetGRPCClient(context.Background(), propagationGrpcAddress, &utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		MaxRetries:  3,
	})
	if err != nil {
		panic(err)
	}
	propagationServer = propagation_api.NewPropagationAPIClient(pConn)

	numberOfOutputs, _ := gocore.Config().GetInt("number_of_outputs", 10_000)

	txChan = make(chan *bt.UTXO, numberOfOutputs*2)

	// create new private key
	keySet, err := extra.New()
	if err != nil {
		panic(err)
	}

	numberOfTransactions := uint32(1)
	satoshisPerOutput := uint64(1000)

	logger.Infof("Asking seeder to create %d transaction(s) with %d outputs of %d satoshis each",
		numberOfTransactions,
		numberOfOutputs,
		satoshisPerOutput,
	)

	if _, err := seederServer.CreateSpendableTransactions(context.Background(), &seeder_api.CreateSpendableTransactionsRequest{
		PrivateKey:           keySet.PrivateKey.Serialise(),
		NumberOfTransactions: numberOfTransactions,
		NumberOfOutputs:      uint32(numberOfOutputs),
		SatoshisPerOutput:    satoshisPerOutput,
	}); err != nil {
		panic(err)
	}

	res, err := seederServer.NextSpendableTransaction(context.Background(), &seeder_api.NextSpendableTransactionRequest{
		PrivateKey: keySet.PrivateKey.Serialise(),
	})

	if err != nil {
		panic(err)
	}

	logger.Infof("Received transaction with txid %x and %d outputs", res.Txid, res.NumberOfOutputs)

	privateKey, _ := bec.PrivKeyFromBytes(bec.S256(), res.PrivateKey)

	script, err := bscript.NewP2PKHFromPubKeyBytes(privateKey.PubKey().SerialiseCompressed())
	if err != nil {
		panic(err)
	}

	go func(numberOfOutputs uint32) {
		logger.Infof("Starting to send %d outputs to txChan", numberOfOutputs)
		for i := uint32(0); i < numberOfOutputs; i++ {
			u := &bt.UTXO{
				TxID:          bt.ReverseBytes(res.Txid),
				Vout:          i,
				LockingScript: script,
				Satoshis:      res.SatoshisPerOutput,
			}

			txChan <- u
		}
		logger.Infof("Finished sending %d outputs to txChan", numberOfOutputs)
	}(res.NumberOfOutputs)

	startTime = time.Now()

	if *rateLimit > 1 {
		rateLimitDuration := (time.Duration(*workers) * time.Second) / (time.Duration(*rateLimit))
		fmt.Printf("Starting %d workers with rate limit of %d tx/s (%s)\n", *workers, *rateLimit, rateLimitDuration)
		for i := 0; i < *workers; i++ {
			go txWorkerLimited(keySet, rateLimitDuration, txChan)
		}
	} else {
		fmt.Printf("Starting %d workers\n", *workers)
		for i := 0; i < *workers; i++ {
			go txWorker(keySet, txChan)
		}
	}

	select {}
}

func txWorker(keySet *extra.KeySet, txChan <-chan *bt.UTXO) {
	for utxo := range txChan {
		err := fireTransactions(utxo, keySet)
		if err != nil {
			fmt.Printf("ERROR in fire transactions: %v\n", err)
		}
	}
}

func txWorkerLimited(keySet *extra.KeySet, rateLimitDuration time.Duration, txChan <-chan *bt.UTXO) {
	limiter := rate.NewLimiter(rate.Every(rateLimitDuration), 1)
	for utxo := range txChan {
		_ = limiter.Wait(context.Background())
		err := fireTransactions(utxo, keySet)
		if err != nil {
			fmt.Printf("ERROR in fire transactions: %v\n", err)
		}
	}
}

func fireTransactions(u *bt.UTXO, keySet *extra.KeySet) error {
	timeStart := time.Now()

	tx := bt.NewTx()
	_ = tx.FromUTXOs(u)

	_ = tx.PayTo(keySet.Script, u.Satoshis)

	unlockerGetter := unlocker.Getter{PrivateKey: keySet.PrivateKey}
	if err := tx.FillAllInputs(context.Background(), &unlockerGetter); err != nil {
		prometheusInvalidTransactions.Inc()
		return err
	}

	if kafkaProducer != nil {
		err := publishToKafka(kafkaProducer, kafkaTopic, tx.TxIDBytes(), tx.ExtendedBytes())
		if err != nil {
			prometheusInvalidTransactions.Inc()
			return err
		}

	} else {
		err := sendTransaction(tx.TxID(), tx.ExtendedBytes())
		if err != nil {
			prometheusInvalidTransactions.Inc()
			return err
		}
	}

	// increment prometheus counter
	prometheusProcessedTransactions.Inc()
	prometheusTransactionSize.Observe(float64(len(tx.ExtendedBytes())))
	prometheusTransactionDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	txChan <- &bt.UTXO{
		TxID:          tx.TxIDBytes(),
		Vout:          0,
		LockingScript: keySet.Script,
		Satoshis:      u.Satoshis,
	}

	return nil
}

var counter atomic.Uint64

func publishToKafka(producer sarama.SyncProducer, topic string, txIDBytes []byte, txExtendedBytes []byte) error {
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
	if printProgress > 0 && counterLoad%printProgress == 0 {
		txPs := float64(0)
		ts := time.Since(startTime).Seconds()
		if ts > 0 {
			txPs = float64(counterLoad) / ts
		}
		fmt.Printf("Time for %d transactions to Kafka: %.2fs (%d tx/s)\r", counterLoad, time.Since(startTime).Seconds(), int(txPs))
	}

	return nil
}

func sendTransaction(txID string, txExtendedBytes []byte) error {
	traceSpan := tracing.Start(context.Background(), "txBlaster:sendTransaction")
	defer traceSpan.Finish()

	traceSpan.SetTag("progname", "txblaster")
	traceSpan.SetTag("txid", txID)

	if useHTTP {
		traceSpan.SetTag("transport", "http")

		req, err := http.NewRequestWithContext(traceSpan.Ctx, "POST", httpURL, bytes.NewBuffer(txExtendedBytes))
		if err != nil {
			return fmt.Errorf("error creating http request: %v", err)
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("error sending transaction: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			var body []byte
			if body, err = io.ReadAll(resp.Body); err != nil {
				return fmt.Errorf("error sending transaction: %v", resp.Status)
			}
			return fmt.Errorf("error sending transaction: %v - %s", resp.Status, body)
		}
	} else {
		traceSpan.SetTag("transport", "grpc")

		if _, err := propagationServer.Set(traceSpan.Ctx, &propagation_api.SetRequest{
			Tx: txExtendedBytes,
		}); err != nil {
			return fmt.Errorf("error sending transaction to propagation server: %v", err)
		}
	}

	counterLoad := counter.Add(1)
	if printProgress > 0 && counterLoad%printProgress == 0 {
		txPs := float64(0)
		ts := time.Since(startTime).Seconds()
		if ts > 0 {
			txPs = float64(counterLoad) / ts
		}
		fmt.Printf("Time for %d transactions: %.2fs (%d tx/s)\r", counterLoad, time.Since(startTime).Seconds(), int(txPs))
	}

	return nil
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Partitioner = sarama.NewManualPartitioner
	// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
