package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"
	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/stores/utxo/nullstore"
	"github.com/bitcoin-sv/ubsv/stores/utxo/redis"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"

	"github.com/bitcoin-sv/ubsv/stores/utxo/scylla"

	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	version                          string
	commit                           string
	counter                          atomic.Int64
	prometheusUtxoStoreBlasterDelete prometheus.Histogram
	prometheusUtxoStoreBlasterStore  prometheus.Histogram
	prometheusUtxoStoreBlasterSpend  prometheus.Histogram
	workerCount                      int
	storeType                        string
	storeFn                          func() (utxo.Interface, error)
)

func init() {
	prometheusUtxoStoreBlasterDelete = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "utxostore_blaster",
			Name:      "res_delete",
			Help:      "Time to delete from utxostore",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusUtxoStoreBlasterStore = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "utxostore_blaster",
			Name:      "res_store",
			Help:      "Time to store utxo in utxostore",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusUtxoStoreBlasterSpend = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "utxostore_blaster",
			Name:      "res_spend",
			Help:      "Time to spend utxo in utxostore",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	httpAddr, ok := gocore.Config().Get("profilerAddr")
	if !ok {
		log.Printf("Profiler address not set, defaulting to localhost:6060")
		httpAddr = "localhost:6060"
	}

	prometheusEndpoint, ok := gocore.Config().Get("prometheusEndpoint")
	if ok && prometheusEndpoint != "" {
		http.Handle(prometheusEndpoint, promhttp.Handler())
		log.Printf("Prometheus metrics available at http://%s%s", httpAddr, prometheusEndpoint)
	}

	// start dummy health check...
	http.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	log.Printf("Profiler available at http://%s/debug/pprof", httpAddr)
	go func() {
		log.Printf("%v", http.ListenAndServe(httpAddr, nil))
	}()
}

func main() {
	flag.IntVar(&workerCount, "workers", 1, "Set worker count")
	flag.StringVar(&storeType, "store", "null", "Set store type (redis|redis-ring|redis-cluster|memory|aerospike|scylla|null)")
	flag.Parse()

	logger := gocore.Log("utxostore_blaster")

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	switch storeType {
	case "redis":
		storeFn = func() (utxo.Interface, error) {
			u, _, _ := gocore.Config().GetURL("utxostore")
			return redis.NewRedis(u)
		}
		log.Printf("Starting redis utxostore-blaster with %d worker(s)", workerCount)
	case "redis-ring":
		storeFn = func() (utxo.Interface, error) {
			u, _, _ := gocore.Config().GetURL("utxostore")
			return redis.NewRedisRing(u)
		}
		log.Printf("Starting redis-ring utxostore-blaster with %d worker(s)", workerCount)
	case "redis-cluster":
		storeFn = func() (utxo.Interface, error) {
			u, _, _ := gocore.Config().GetURL("utxostore")
			return redis.NewRedisCluster(u)
		}
		log.Printf("Starting redis-cluster utxostore-blaster with %d worker(s)", workerCount)
	case "memory":
		storeFn = func() (utxo.Interface, error) {
			return memory.New(false), nil
		}
		log.Printf("Starting memory utxostore-blaster with %d worker(s)", workerCount)
	case "null":
		storeFn = func() (utxo.Interface, error) {
			return nullstore.NewNullStore()
		}
		log.Printf("Starting null utxostore-blaster with %d worker(s)", workerCount)
	case "aerospike":
		storeFn = func() (utxo.Interface, error) {
			u, _, _ := gocore.Config().GetURL("utxostore")
			return aerospike.New(u)
		}
		log.Printf("Starting aerospike utxostore-blaster with %d worker(s)", workerCount)
	case "scylla":
		u, _, _ := gocore.Config().GetURL("utxostore")
		storeFn = func() (utxo.Interface, error) {
			return scylla.NewScylla(u)
		}
		log.Printf("Starting scylla utxostore-blaster url %v with %d worker(s)", u, workerCount)
	default:
		panic(fmt.Sprintf("Unknown store type: %s", storeType))
	}

	go func() {
		start := time.Now()

		for range time.NewTicker(5 * time.Second).C {
			elapsed := time.Since(start)
			log.Printf("TPS: %s\n", FormatFloat(float64(counter.Swap(0))/float64(elapsed.Milliseconds())*1000))

			start = time.Now()
		}
	}()

	for i := 0; i < workerCount; i++ {
		go worker(logger)
	}

	<-make(chan struct{})
}

func worker(logger utils.Logger) {
	utxostore, err := storeFn()
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	for {
		btTx := bt.NewTx()
		_ = btTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 10000)

		utxoHash, err := util.UTXOHashFromOutput(btTx.TxIDChainHash(), btTx.Outputs[0], 0)

		spend := &utxo.Spend{
			TxID:         btTx.TxIDChainHash(),
			Vout:         0,
			Hash:         utxoHash,
			SpendingTxID: utxoHash,
		}

		// Delete the txid
		timeStart := time.Now()
		if err = utxostore.Delete(ctx, spend); err != nil {
			panic(err)
		}
		prometheusUtxoStoreBlasterDelete.Observe(float64(time.Since(timeStart).Microseconds()))

		// Store the txid
		timeStart = time.Now()
		if err = utxostore.Store(ctx, btTx); err != nil {
			panic(err)
		}
		prometheusUtxoStoreBlasterStore.Observe(float64(time.Since(timeStart).Microseconds()))

		// Spend the txid
		timeStart = time.Now()
		if err = utxostore.Spend(ctx, []*utxo.Spend{spend}); err != nil {
			panic(err)
		}
		prometheusUtxoStoreBlasterSpend.Observe(float64(time.Since(timeStart).Microseconds()))

		counter.Add(1)
	}
}

func generateRandomBytes() []byte {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
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
