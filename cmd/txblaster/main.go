package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitcoin-sv/ubsv/cmd/txblaster/extra"
	"github.com/bitcoin-sv/ubsv/cmd/txblaster/worker"
	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sercand/kuberesolver/v5"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/resolver"
	"storj.io/drpc/drpcconn"
)

var logger utils.Logger

var printProgress uint64

// Name used by build script for the binaries. (Please keep on single line)
const progname = "tx-blaster"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string
var kafkaProducer sarama.SyncProducer
var kafkaTopic string
var ipv6MulticastConn *net.UDPConn
var coinbasePrivKey string
var ipv6MulticastChan = make(chan worker.Ipv6MulticastMsg)
var totalTransactions atomic.Uint64
var startTime time.Time

func init() {
	gocore.SetInfo(progname, version, commit)

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger = gocore.Log("txblast", gocore.NewLogLevelFromString(logLevelStr))

	var found bool
	coinbasePrivKey, found = gocore.Config().Get("coinbase_wallet_privkey")
	if !found {
		log.Fatal(errors.New("coinbase_wallet_privkey not found in config"))
	}
}

func main() {
	_ = os.Chdir("../../")

	ctx, cancelFunc := context.WithCancel(context.Background())

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs

		cancelFunc() // cancel the contexts and wait for all to stop
		time.Sleep(1 * time.Second)
		logger.Infof("TX Blaster finished, total transactions: ~%d", totalTransactions.Load())
		os.Exit(0)
	}()

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	workers := flag.Int("workers", runtime.NumCPU(), "how many workers to use for blasting")
	rateLimit := flag.Int("limit", -1, "rate limit tx/s")
	printFlag := flag.Int("print", 0, "print out progress every x transactions")
	bufferSize := flag.Int("buffer", -1, "buffer size for txs chan")
	kafka := flag.String("kafka", "", "Kafka server URL - if applicable")
	ipv6Address := flag.String("ipv6Address", "", "IPv6 multicast address - if applicable")
	ipv6Interface := flag.String("ipv6Interface", "en0", "IPv6 multicast interface - if applicable")
	profileAddress := flag.String("profile", "", "use this profile port instead of the default")
	logIds := flag.Bool("log", false, "log tx ids")

	flag.Parse()

	prometheusEndpoint, ok := gocore.Config().Get("prometheusEndpoint")
	if ok && prometheusEndpoint != "" {
		logger.Infof("Starting prometheus endpoint on %s", prometheusEndpoint)
		http.Handle(prometheusEndpoint, promhttp.Handler())
	}

	g, ctx := errgroup.WithContext(ctx)

	if kafka != nil && *kafka != "" {
		logger.Infof("Connecting to kafka at %s", *kafka)
		kafkaURL, err := url.Parse(*kafka)
		if err != nil {
			log.Fatalf("unable to parse kafka url: %v", err)
		}

		clusterAdmin, producer, err := util.ConnectToKafka(kafkaURL)
		if err != nil {
			log.Fatalf("unable to connect to kafka: %v", err)
		}

		defer func() {
			_ = clusterAdmin.Close()
			_ = producer.Close()
		}()

		kafkaProducer = producer
		kafkaTopic = kafkaURL.Path[1:]
	}

	if ipv6Address != nil && *ipv6Address != "" {
		logger.Infof("Using %s ipv6Address", *ipv6Address)
		logger.Infof("Using ipv6 multicast interface %s at address %s", *ipv6Interface, *ipv6Address)
		en0, err := net.InterfaceByName(*ipv6Interface)
		if err != nil {
			log.Fatalf("error resolving interface: %v", err)
		}

		addr := &net.UDPAddr{
			IP:   net.ParseIP(*ipv6Address),
			Port: 9999,
			Zone: en0.Name,
		}

		logger.Infof("Starting IPv6 multicast on %s", addr.String())
		ipv6MulticastConn, err = net.DialUDP("udp6", nil, addr)
		if err != nil {
			log.Fatalf("error dialing address: %v", err)
		}

		go func() {
			for {
				msg := <-ipv6MulticastChan

				r := bytes.NewReader(msg.TxExtendedBytes)
				msgTx := &wire.MsgExtendedTx{}
				err = msgTx.Deserialize(r)
				if err != nil {
					logger.Errorf("error deserializing tx %s: %v", utils.ReverseAndHexEncodeSlice(msg.IDBytes), err)
					continue
				}

				if err = wire.WriteMessage(msg.Conn, msgTx, wire.ProtocolVersion, wire.MainNet); err != nil {
					if errors.Is(err, io.EOF) {
						logger.Infof("[%s] Connection closed", msg.Conn.RemoteAddr())
						continue
					}
					logger.Errorf("[%s] Failed to write message: %v", msg.Conn.RemoteAddr(), err)
				}
			}
		}()
	}

	printProgress = uint64(*printFlag)

	go func() {
		var profilerAddr string
		var startProfiler bool

		if profileAddress != nil && *profileAddress != "" {
			profilerAddr, startProfiler = *profileAddress, true
		} else {
			profilerAddr, startProfiler = gocore.Config().Get("tx_blaster_profilerAddr", ":9191")
		}

		if startProfiler {
			logger.Infof("Starting profile on http://%s/debug/pprof", profilerAddr)
			logger.Fatalf("%v", http.ListenAndServe(profilerAddr, nil))
		}
	}()

	if gocore.Config().GetBool("use_open_tracing", true) {
		logger.Infof("Starting open tracing")
		// closeTracer := tracing.InitOtelTracer()
		_, closer, err := util.InitGlobalTracer("tx-blaster")
		if err != nil {
			panic(err)
		}

		defer closer.Close()
	}

	grpcResolver, _ := gocore.Config().Get("grpc_resolver")
	if grpcResolver == "k8s" {
		logger.Infof("[VALIDATOR] Using k8s resolver for clients")
		resolver.SetDefaultScheme("k8s")
	} else if grpcResolver == "kubernetes" {
		logger.Infof("[VALIDATOR] Using kubernetes resolver for clients")
		kuberesolver.RegisterInClusterWithSchema("k8s")
	}

	propagationServers := setPropagationServers(ctx)
	if len(propagationServers) == 0 {
		panic("No suitable propagation server connection found")
	}

	logger.Infof("Using %d propagation servers: %+v", len(propagationServers), propagationServers)

	numberOfOutputs, _ := gocore.Config().GetInt("number_of_outputs", 10_000)
	satoshisPerOutput, _ := gocore.Config().GetInt("satoshis_per_output", 1000)
	numberOfTransactions := uint32(1)

	logger.Infof("Each worker will ask tracker to create %d transaction(s) with %d outputs of %d satoshis each",
		numberOfTransactions,
		numberOfOutputs,
		satoshisPerOutput,
	)

	var rateLimiter *rate.Limiter

	if *rateLimit > 1 {

		rateLimitDuration := time.Duration(*workers) * time.Second / time.Duration(*rateLimit)
		rateLimiter = rate.NewLimiter(rate.Every(rateLimitDuration), 1)

		logger.Infof("Starting %d workers with rate limit of %d tx/s (%s)", *workers, *rateLimit, rateLimitDuration)
	} else {
		logger.Infof("Starting %d workers", *workers)
	}

	var logIdsFile chan string
	if *logIds {
		logFile, err := os.OpenFile("data/txblaster.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}

		logIdsFile = make(chan string, 100000)
		go func() {
			for id := range logIdsFile {
				_, _ = logFile.WriteString(id + "\n")
			}
		}()
	}

	logLevelStr, _ := gocore.Config().Get("logLevel", "INFO")

	startTime = time.Now()
	for i := 0; i < *workers; i++ {
		i := i
		g.Go(func() error {
			// run a worker forever
			for {
				logger.Infof("starting worker %d", i)
				workerLogger := gocore.Log(fmt.Sprintf("wrk_%d", i), gocore.NewLogLevelFromString(logLevelStr))

				w, err := worker.NewWorker(
					workerLogger,
					numberOfOutputs,
					numberOfTransactions,
					uint64(satoshisPerOutput),
					coinbasePrivKey,
					rateLimiter,
					propagationServers,
					kafkaProducer,
					kafkaTopic,
					ipv6MulticastConn,
					ipv6MulticastChan,
					printProgress,
					logIdsFile,
					&totalTransactions,
					&startTime,
					*bufferSize,
				)
				if err != nil {
					return err
				}

				err = w.Start(ctx)
				if err != nil {
					logger.Errorf("error from worker: %v", err)
				}

				time.Sleep(1 * time.Second)
			}
		})
	}

	// start http health check server
	http.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	if err := g.Wait(); err != nil {
		logger.Errorf("error occurred in tx blaster: %v", err)
	}
}

func setPropagationServers(ctx context.Context) map[string]extra.PropagationServer {
	propagationServers := make(map[string]extra.PropagationServer)

	propagationGrpcAddresses, okGrpc := gocore.Config().GetMulti("propagation_grpcAddresses", "|")
	if !okGrpc {
		panic("no propagation_grpcAddresses setting found")
	}

	for _, propagationGrpcAddress := range propagationGrpcAddresses {
		pConn, err := util.GetGRPCClient(ctx, propagationGrpcAddress, &util.ConnectionOptions{
			OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
			Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
			MaxRetries:  3,
		})
		if err != nil {
			panic(err)
		}

		host := strings.Split(propagationGrpcAddress, ":")[0]
		propagationServers[host] = extra.PropagationServer{
			GRPC: propagation_api.NewPropagationAPIClient(pConn),
		}
	}

	if propagationDrpcAddress, ok := gocore.Config().Get("propagation_drpcAddress"); ok {
		rawConn, err := net.Dial("tcp", propagationDrpcAddress)
		if err != nil {
			panic(err)
		}
		conn := drpcconn.New(rawConn)
		drpcClient := propagation_api.NewDRPCPropagationAPIClient(conn)

		host := strings.Split(propagationDrpcAddress, ":")[0]
		if p, ok := propagationServers[host]; ok {
			p.DRPC = drpcClient
		} else {
			propagationServers[host] = extra.PropagationServer{
				DRPC: drpcClient,
			}
		}
	}

	if propagationFrpcAddress, ok := gocore.Config().Get("propagation_frpcAddress"); ok {
		client, err := propagation_api.NewClient(nil, nil)
		if err != nil {
			panic(err)
		}

		err = client.Connect(propagationFrpcAddress)
		if err != nil {
			panic(err)
		} else {
			host := strings.Split(propagationFrpcAddress, ":")[0]
			if p, ok := propagationServers[host]; ok {
				p.FRPC = client
			} else {
				propagationServers[host] = extra.PropagationServer{
					FRPC: client,
				}
			}
		}
	}

	return propagationServers
}
