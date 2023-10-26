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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitcoin-sv/ubsv/cmd/txblaster/worker"
	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sercand/kuberesolver/v5"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/resolver"
)

const progname = "tx-blaster"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string

var logger utils.Logger

var printProgress uint64

var kafkaProducer sarama.SyncProducer
var kafkaTopic string
var ipv6MulticastConn *net.UDPConn
var ipv6MulticastChan = make(chan worker.Ipv6MulticastMsg)
var totalTransactions atomic.Uint64
var startTime time.Time

func init() {
	gocore.SetInfo(progname, version, commit)

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger = gocore.Log("txblast", gocore.NewLogLevelFromString(logLevelStr))
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

	propagationServers := distributor.GetPropagationGRPCAddresses()
	if len(propagationServers) == 0 {
		panic("No suitable propagation server connection found")
	}

	logger.Infof("Using %d propagation servers: %+v", len(propagationServers), propagationServers)

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

		logger.Infof("starting worker %d", i)
		workerLogger := gocore.Log(fmt.Sprintf("wrk_%d", i), gocore.NewLogLevelFromString(logLevelStr))

		w, err := worker.NewWorker(
			workerLogger,
			rateLimiter,
			kafkaProducer,
			kafkaTopic,
			ipv6MulticastConn,
			ipv6MulticastChan,
			printProgress,
			logIdsFile,
			&totalTransactions,
			&startTime,
		)
		if err != nil {
			logger.Errorf("Could not initialise worker %d: %w", i, err)
			return
		}

		err = w.Init(ctx)
		if err != nil {
			logger.Errorf("Could not initialise worker %d: %w", i, err)
			return
		}

		g.Go(func() error {
			if err := w.Start(ctx); err != nil {
				return fmt.Errorf("error from worker: %v", err)
			}

			return nil
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
