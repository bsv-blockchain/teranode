//go:build native

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"net/http"
	_ "net/http/pprof"
	"net/url"

	"github.com/bitcoin-sv/ubsv/native"
	"github.com/bitcoin-sv/ubsv/services/propagation"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	incompleteTx, _                 = bt.NewTxFromString("010000000000000000ef0193a35408b6068499e0d5abd799d3e827d9bfe70c9b75ebe209c91d25072326510000000000ffffffff00e1f505000000001976a914c0a3c167a28cabb9fbb495affa0761e6e74ac60d88ac02404b4c00000000001976a91404ff367be719efa79d76e4416ffb072cd53b208888acde94a905000000001976a91404d03f746652cfcb6cb55119ab473a045137d26588ac00000000")
	w                               *wif.WIF
	counter                         atomic.Int64
	prometheusWorkers               prometheus.Gauge
	prometheusProcessedTransactions prometheus.Counter
	workerCount                     int
	broadcast                       bool
	grpcClient                      propagation_api.PropagationAPIClient
	useHttp                         bool
	httpUrl                         *url.URL
	useStream                       bool
	streamOnce                      sync.Once
	txCh                            chan []byte
	errorCh                         chan error
)

func init() {
	unlocker.InjectExternalSignerFn(native.SignMessage)

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

	log.Printf("Profiler available at http://%s/debug/pprof", httpAddr)
	go func() {
		log.Printf("%v", http.ListenAndServe(httpAddr, nil))

	}()

}

func main() {
	flag.IntVar(&workerCount, "workers", 1, "Set worker count")
	flag.BoolVar(&broadcast, "broadcast", false, "Broadcast to propagation server")
	flag.BoolVar(&useHttp, "http", false, "Use HTTP instead of gRPC")
	flag.BoolVar(&useStream, "stream", false, "Use stream instead of unary")
	flag.Parse()

	if useHttp {
		httpAddresses, _ := gocore.Config().GetMulti("propagation_httpAddresses", "|")
		httpUrl, _ = url.Parse(httpAddresses[0])
		log.Printf("Using HTTP propagation server: %v", httpUrl)

	} else {
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

	if broadcast {
		log.Printf("Starting %d broadcasting worker(s)", workerCount)
	} else if useStream {
		log.Printf("Starting %d stream worker(s)", workerCount)
	} else {
		log.Printf("Starting %d non-broadcaster worker(s)", workerCount)
	}

	for i := 0; i < workerCount; i++ {
		go worker()
	}

	<-make(chan struct{})
}

func worker() {
	prometheusWorkers.Inc()
	defer func() {
		prometheusWorkers.Dec()
	}()

	for {
		// Create a new transaction
		tx := incompleteTx.Clone()

		ul := unlocker.Getter{PrivateKey: w.PrivKey}
		if err := tx.FillAllInputs(context.Background(), &ul); err != nil {
			panic(err)
		}

		if broadcast {
			if err := sendToPropagationServer(context.Background(), tx.ExtendedBytes()); err != nil {
				panic(err)
			}
		}

		prometheusProcessedTransactions.Inc()
		counter.Add(1)
	}
}

func sendToPropagationServer(ctx context.Context, txExtendedBytes []byte) error {
	if useHttp {
		req, err := http.NewRequest("POST", fmt.Sprintf("%s/tx", httpUrl.String()), bytes.NewReader(txExtendedBytes))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/octet-stream")

		// Create an HTTP client and send the request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("Error sending request: %v", err)
		}
		defer resp.Body.Close()

		// Check the response status
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("POST request failed with status: %v", resp.Status)
		}

		return nil

	} else if useStream {

		streamOnce.Do(func() {
			var err error
			txCh, errorCh, err = propagation.GetProcessTransactionChannel(context.Background())
			if err != nil {
				panic(err)
			}

			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case err := <-errorCh:
						panic(err)
					}
				}
			}()
		})

		txCh <- txExtendedBytes

	} else {
		_, err := grpcClient.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
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
