package blockassembly_blaster

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc/resolver"
)

var (
	version                       string
	commit                        string
	counter                       atomic.Int64
	prometheusBlockAssemblerAddTx prometheus.Counter
	workerCount                   int
	grpcClient                    blockassembly_api.BlockAssemblyAPIClient
	broadcastProtocol             string
	batchSize                     int
)

func Init() {
	prometheusBlockAssemblerAddTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "block_assembler_add_tx",
			Help:      "Number of txs added to the block assembler",
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

	server := &http.Server{
		Addr:         httpAddr,
		Handler:      nil, // nil uses http.DefaultServeMux
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
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
	flag.StringVar(&broadcastProtocol, "broadcast", "grpc", "Broadcast to blockassembly server using (disabled|grpc|frpc|http)")
	flag.IntVar(&batchSize, "batch_size", 0, "Batch size [0 for no batching]")
	flag.Parse()

	logger := ulogger.New("block_assembly_blaster")

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	switch broadcastProtocol {
	case "grpc":
		prom := gocore.Config().GetBool("use_prometheus_grpc_metrics", true)
		log.Printf("Using prometheus grpc metrics: %v", prom)

		grpcAddr, _ := gocore.Config().Get("blockassembly_grpcAddress")
		conn, err := util.GetGRPCClient(context.Background(), grpcAddr, &util.ConnectionOptions{
			Prometheus: prom,
			MaxRetries: 3,
		})
		if err != nil {
			panic(err)
		}
		grpcClient = blockassembly_api.NewBlockAssemblyAPIClient(conn)
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
	case "frpc":
		log.Printf("Starting %d frpc-broadcaster worker(s)", workerCount)
	default:
		panic("Unknown broadcast protocol")
	}

	for i := 0; i < workerCount; i++ {
		go worker(logger)
	}

	<-make(chan struct{})
}

func worker(logger ulogger.Logger) {
	var batchCounter = 0
	txRequests := make([]*blockassembly_api.AddTxRequest, batchSize)

	for {
		// Create a dummy txid
		txid := generateRandomBytes()

		// Create a dummy utxoHash
		utxoHash := generateRandomBytes()

		req := &blockassembly_api.AddTxRequest{
			Txid:  txid,
			Fee:   10,
			Size:  100,
			Utxos: [][]byte{utxoHash},
		}

		prometheusBlockAssemblerAddTx.Inc()
		counter.Add(1)

		if broadcastProtocol == "disabled" {
			return
		}
		ctx, ctxCancelFunc := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))

		if batchSize == 0 {
			if err := sendToBlockAssemblyServer(ctx, logger, req); err != nil {
				panic(err)
			}
		} else {
			txRequests[batchCounter] = req
			batchCounter++
			if batchCounter == batchSize {
				batchReq := &blockassembly_api.AddTxBatchRequest{
					TxRequests: txRequests,
				}
				if err := sendBatchToBlockAssemblyServer(ctx, logger, batchReq); err != nil {
					panic(err)
				}
				batchCounter = 0
			}
		}
		ctxCancelFunc()
	}
}

func sendToBlockAssemblyServer(ctx context.Context, logger ulogger.Logger, req *blockassembly_api.AddTxRequest) error {
	switch broadcastProtocol {

	case "disabled":
		return nil

	case "grpc":
		_, err := grpcClient.AddTx(ctx, req)
		return err

	}

	return nil
}

func sendBatchToBlockAssemblyServer(ctx context.Context, logger ulogger.Logger, req *blockassembly_api.AddTxBatchRequest) error {
	switch broadcastProtocol {

	case "grpc":
		_, err := grpcClient.AddTxBatch(ctx, req)
		return err

	}

	return nil
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
