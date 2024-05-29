package propagation_blaster

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/bitcoin-sv/ubsv/errors"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/services/propagation"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc/resolver"
)

var (
	version                         string
	commit                          string
	incompleteTx, _                 = bt.NewTxFromString("010000000000000000ef0193a35408b6068499e0d5abd799d3e827d9bfe70c9b75ebe209c91d25072326510000000000ffffffff00e1f505000000001976a914c0a3c167a28cabb9fbb495affa0761e6e74ac60d88ac02404b4c00000000001976a91404ff367be719efa79d76e4416ffb072cd53b208888acde94a905000000001976a91404d03f746652cfcb6cb55119ab473a045137d26588ac00000000")
	w                               *wif.WIF
	counter                         atomic.Int64
	prometheusWorkers               prometheus.Gauge
	prometheusProcessedTransactions prometheus.Counter
	workerCount                     int
	grpcClient                      propagation_api.PropagationAPIClient
	// streamClient                    *propagation.StreamingClient
	broadcastProtocol string
	httpUrl           *url.URL
	// errorCh                         chan error
	bufferSize int
)

func Init() {
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

	httpAddr, ok := gocore.Config().Get("tx_blaster_profilerAddr")
	if !ok {
		log.Printf("Profiler address not set, defaulting to localhost:6060")
		httpAddr = "localhost:6060"
	}

	prometheusEndpoint, ok := gocore.Config().Get("prometheusEndpoint")
	if ok && prometheusEndpoint != "" {
		http.Handle(prometheusEndpoint, promhttp.Handler())
		log.Printf("Prometheus metrics available at http://%s%s", httpAddr, prometheusEndpoint)
	}

	server := &http.Server{
		Addr:         httpAddr,
		Handler:      nil,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("Profiler available at http://%s/debug/pprof", httpAddr)
	go func() {
		log.Printf("%v", server.ListenAndServe())

	}()

	grpcResolver, _ := gocore.Config().Get("grpc_resolver")
	if grpcResolver == "k8s" {
		log.Printf("[VALIDATOR] Using k8s resolver for clients")
		resolver.SetDefaultScheme("k8s")
	} else if grpcResolver == "kubernetes" {
		log.Printf("[VALIDATOR] Using kubernetes resolver for clients")
		kuberesolver.RegisterInClusterWithSchema("k8s")
	}
}

func Start() {
	flag.IntVar(&workerCount, "workers", 1, "Set worker count")
	flag.StringVar(&broadcastProtocol, "broadcast", "grpc", "Broadcast to propagation server using (disabled|grpc|stream|http)")
	flag.IntVar(&bufferSize, "buffer_size", 0, "Buffer size")
	flag.Parse()

	logger := ulogger.New("propagation_blaster")

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	switch broadcastProtocol {
	case "http":
		httpAddresses, _ := gocore.Config().GetMulti("propagation_httpAddresses", "|")
		httpUrl, _ = url.Parse(httpAddresses[0])
		log.Printf("Using HTTP propagation server: %v", httpUrl)

	case "grpc":
		prom := gocore.Config().GetBool("use_prometheus_grpc_metrics", true)
		log.Printf("Using prometheus grpc metrics: %v", prom)

		propagationServerAddr, _ := gocore.Config().GetMulti("propagation_grpcAddresses", "|")
		conn, err := util.GetGRPCClient(context.Background(), propagationServerAddr[0], &util.ConnectionOptions{
			Prometheus: prom,
			MaxRetries: 3,
		})
		if err != nil {
			panic(err)
		}
		grpcClient = propagation_api.NewPropagationAPIClient(conn)
	}

	var err error

	w, err = wif.DecodeWIF("cNGwGSc7KRrTmdLUZ54fiSXWbhLNDc2Eg5zNucgQxyQCzuQ5YRDq")
	if err != nil {
		panic(err)
	}

	go func() {
		start := time.Now()

		for range time.NewTicker(5 * time.Second).C {
			elapsed := time.Since(start)
			log.Printf("TPS: %s\n", FormatFloat(float64(counter.Swap(0))/float64(elapsed.Milliseconds())*1000))

			start = time.Now()
		}
	}()

	switch broadcastProtocol {
	case "disabled":
		log.Printf("Starting %d non-broadcaster worker(s)", workerCount)
	case "grpc":
		log.Printf("Starting %d broadcasting worker(s)", workerCount)
	case "stream":
		log.Printf("Starting %d stream worker(s)", workerCount)
	case "http":
		log.Printf("Starting %d http-broadcaster worker(s)", workerCount)
	default:
		panic("Unknown broadcast protocol")
	}

	for i := 0; i < workerCount; i++ {
		go worker(logger)
	}

	<-make(chan struct{})
}

func worker(logger ulogger.Logger) {
	prometheusWorkers.Inc()
	defer func() {
		prometheusWorkers.Dec()
	}()

	var streamingClient *propagation.StreamingClient
	if broadcastProtocol == "stream" {
		var err error
		ctx := context.Background()
		streamingClient, err = propagation.NewStreamingClient(ctx, logger, bufferSize)
		if err != nil {
			panic(err)
		}
	}

	for {
		// Create a new transaction
		tx := incompleteTx.Clone()

		ul := unlocker.Getter{PrivateKey: w.PrivKey}
		if err := tx.FillAllInputs(context.Background(), &ul); err != nil {
			panic(err)
		}

		if broadcastProtocol == "stream" {
			if err := streamingClient.ProcessTransaction(tx.ExtendedBytes()); err != nil {
				panic(err)
			}
		} else {
			if err := sendToPropagationServer(context.Background(), logger, tx.ExtendedBytes()); err != nil {
				panic(err)
			}
		}

		if broadcastProtocol != "disabled" {
			prometheusProcessedTransactions.Inc()
		}
		counter.Add(1)
	}
}

func sendToPropagationServer(ctx context.Context, logger ulogger.Logger, txExtendedBytes []byte) error {
	switch broadcastProtocol {

	case "disabled":

		return nil

	case "http":

		req, err := http.NewRequest("POST", fmt.Sprintf("%s/tx", httpUrl.String()), bytes.NewReader(txExtendedBytes))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/octet-stream")

		// Create an HTTP httpClient and send the request
		httpClient := &http.Client{}
		resp, err := httpClient.Do(req)
		if err != nil {
			return errors.New(errors.ERR_PROCESSING, "error sending request: %v", err)
		}
		defer resp.Body.Close()

		// Check the response status
		if resp.StatusCode != http.StatusOK {
			return errors.New(errors.ERR_SERVICE_ERROR, "POST request failed with status: %v", resp.Status)
		}

		return nil

	case "grpc":

		_, err := grpcClient.ProcessTransactionDebug(ctx, &propagation_api.ProcessTransactionRequest{
			Tx: txExtendedBytes,
		})

		return err
	}

	return nil
}

func FormatFloat(f float64) string {
	intPart := int(f)
	decimalPart := int((f - float64(intPart)) * 100)

	var sb strings.Builder
	count := 0
	for intPart > 0 {
		if count > 0 && count%3 == 0 {
			sb.WriteString(",")
		}
		digit := intPart % 10
		sb.WriteString(fmt.Sprintf("%d", digit))
		intPart /= 10
		count++
	}

	reversedIntPart := []rune(sb.String())
	for i, j := 0, len(reversedIntPart)-1; i < j; i, j = i+1, j-1 {
		reversedIntPart[i], reversedIntPart[j] = reversedIntPart[j], reversedIntPart[i]
	}

	return fmt.Sprintf("%s.%02d", string(reversedIntPart), decimalPart)
}
