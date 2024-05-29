package utxostore_blaster

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"

	"strings"
	"sync"
	"sync/atomic"
	"time"

	utxo "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"

	//"github.com/bitcoin-sv/ubsv/stores/utxomap/memory"
	//"github.com/bitcoin-sv/ubsv/stores/utxomap/nullstore"
	//"github.com/bitcoin-sv/ubsv/stores/utxomap/redis"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"

	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	version string
	commit  string
	counter atomic.Int64
	// prometheusUtxoStoreBlasterDelete prometheus.Histogram
	prometheusUtxoStoreBlasterStore prometheus.Histogram
	prometheusUtxoStoreBlasterSpend prometheus.Histogram
	workerCount                     int
	storeSpendDelay                 time.Duration
	storeType                       string

	storeFn   func() (utxo.Store, error)
	storeOnce sync.Once
)

func Init() {
	// prometheusUtxoStoreBlasterDelete = promauto.NewHistogram(
	// 	prometheus.HistogramOpts{
	// 		Namespace: "utxostore_blaster",
	// 		Name:      "res_delete",
	// 		Help:      "Time to delete from utxostore",
	// 		Buckets:   util.MetricsBucketsMilliSeconds,
	// 	},
	// )
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
	server := &http.Server{
		Addr:              httpAddr,
		ReadHeaderTimeout: 90 * time.Second, // pprof takes 30 seconds to create a profile
	}
	go func() {
		log.Printf("%v", server.ListenAndServe())
	}()
}

func Start() {
	flag.IntVar(&workerCount, "workers", 1, "Set worker count")
	flag.StringVar(&storeType, "store", "null", "Set store type (memory|aerospike|redis|redis-cluster|redis-ring|null)")

	delay := flag.String("storeSpendDelay", "0s", "Set delay between store and spend")
	if delay != nil {
		d, err := time.ParseDuration(*delay)
		if err != nil {
			panic(err)
		}

		storeSpendDelay = d
	}

	flag.Parse()

	logger := ulogger.New("utxostore_blaster")

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	switch storeType {
	//case "memory":
	//	storeFn = func() (utxo.Interface, error) {
	//		return memory.New(false), nil
	//	}
	//	log.Printf("Starting memory utxostore-blaster with %d worker(s)", workerCount)
	//
	//case "null":
	//	storeFn = func() (utxo.Interface, error) {
	//		return nullstore.NewNullStore()
	//	}
	//	log.Printf("Starting null utxostore-blaster with %d worker(s)", workerCount)

	case "aerospike":
		var store utxo.Store
		var err error
		done := make(chan struct{})

		storeOnce.Do(func() {
			defer close(done)
			u, _, _ := gocore.Config().GetURL("utxoblaster_utxostore_aerospike")
			store, err = aerospike.New(logger, u)
		})

		storeFn = func() (utxo.Store, error) {
			<-done // Wait for initialization to complete
			return store, err
		}
		log.Printf("Starting aerospike utxostore-blaster with %d worker(s)", workerCount)

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

func worker(logger ulogger.Logger) {
	utxostore, err := storeFn()
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	for {
		privateKey, err := bec.NewPrivateKey(bec.S256())
		if err != nil {
			panic(err)
		}

		walletAddress, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
		if err != nil {
			panic(fmt.Errorf("can't create coinbase address: %v", err))
		}

		btTx := bt.NewTx()
		_ = btTx.PayToAddress(walletAddress.AddressString, 10000)

		utxoHash, err := util.UTXOHashFromOutput(btTx.TxIDChainHash(), btTx.Outputs[0], 0)
		if err != nil {
			panic(err)
		}

		spend := &utxo.Spend{
			TxID:         btTx.TxIDChainHash(),
			Vout:         0,
			Hash:         utxoHash,
			SpendingTxID: utxoHash,
		}

		// Delete the txid
		// timeStart := time.Now()
		// if err = utxostore.Delete(ctx, btTx); err != nil {
		// 	logger.Fatalf("Failed to Delete %s: %v", btTx.TxIDChainHash().String(), err)
		// }
		// prometheusUtxoStoreBlasterDelete.Observe(float64(time.Since(timeStart).Microseconds()))

		// Store the txid
		timeStart := time.Now()
		if _, err = utxostore.Create(ctx, btTx); err != nil {
			logger.Fatalf("Failed to Store %s: %v", btTx.TxIDChainHash().String(), err)
		}
		prometheusUtxoStoreBlasterStore.Observe(float64(time.Since(timeStart).Microseconds()))

		time.Sleep(storeSpendDelay)

		// Spend the txid
		timeStart = time.Now()
		if err = utxostore.Spend(ctx, []*utxo.Spend{spend}); err != nil {
			logger.Errorf("couldn't spend utxo: %+v", err)
			// panic(err)
		}
		prometheusUtxoStoreBlasterSpend.Observe(float64(time.Since(timeStart).Microseconds()))

		counter.Add(1)
	}
}

//func generateRandomBytes() []byte {
//	b := make([]byte, 32)
//	_, err := rand.Read(b)
//	if err != nil {
//		panic(err)
//	}
//	return b
//}

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
