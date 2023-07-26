package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TAAL-GmbH/ubsv/cmd/txblaster/worker"
	_ "github.com/TAAL-GmbH/ubsv/k8sresolver"
	"github.com/TAAL-GmbH/ubsv/services/propagation/propagation_api"
	"github.com/TAAL-GmbH/ubsv/services/seeder/seeder_api"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var logger utils.Logger

// var propagationServers []propagation_api.PropagationAPIClient
// var seederServer seeder_api.SeederAPIClient
var printProgress uint64

// Name used by build script for the binaries. (Please keep on single line)
const progname = "tx-blaster"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string
var kafkaProducer sarama.SyncProducer
var kafkaTopic string
var ipv6MulticastConn *net.UDPConn

var ipv6MulticastChan = make(chan worker.Ipv6MulticastMsg)

func init() {
	gocore.SetInfo(progname, version, commit)
	logger = gocore.Log("work", gocore.NewLogLevelFromString("debug"))
}

func main() {
	_ = os.Chdir("../../")

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

	g, ctx := errgroup.WithContext(context.Background())

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
			profilerAddr, startProfiler = gocore.Config().Get("profilerAddr")
		}

		if startProfiler {
			logger.Infof("Starting profile on http://%s/debug/pprof", profilerAddr)
			logger.Fatalf("%v", http.ListenAndServe(profilerAddr, nil))
		}
	}()

	if gocore.Config().GetBool("use_open_tracing", true) {
		logger.Infof("Starting open tracing")
		// closeTracer := tracing.InitOtelTracer()
		_, closer, err := utils.InitGlobalTracer("tx-blaster")
		if err != nil {
			panic(err)
		}

		defer closer.Close()
	}

	go func() {
		profilerAddr, _ := gocore.Config().Get("tx_blaster_profilerAddr", ":9091")
		_ = http.ListenAndServe(profilerAddr, nil)
	}()

	// (ok)
	seederGrpcAddresses := []string{}
	if addresses, ok := gocore.Config().Get("txblaster_seeder_grpcTargets"); ok {
		seederGrpcAddresses = append(seederGrpcAddresses, strings.Split(addresses, "|")...)
	} else {
		if address, ok := gocore.Config().Get("seeder_grpcAddress"); ok {
			seederGrpcAddresses = append(seederGrpcAddresses, address)
		} else {
			panic("no seeder_grpcAddress setting found")
		}
	}

	seederServers := []seeder_api.SeederAPIClient{}
	for _, seederGrpcAddress := range seederGrpcAddresses {
		sConn, err := utils.GetGRPCClient(ctx, seederGrpcAddress, &utils.ConnectionOptions{
			OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
			MaxRetries:  3,
		})
		if err != nil {
			panic(err)
		}
		seederServers = append(seederServers, seeder_api.NewSeederAPIClient(sConn))
	}

	if len(seederServers) == 0 {
		panic("No suitable seeder server connection found")
	}

	// (ok)

	propagationGrpcAddresses := []string{}
	if addresses, ok := gocore.Config().Get("txblaster_propagation_grpcTargets"); ok {
		propagationGrpcAddresses = append(propagationGrpcAddresses, strings.Split(addresses, "|")...)
	} else {
		if address, ok := gocore.Config().Get("propagation_grpcAddress"); ok {
			propagationGrpcAddresses = append(propagationGrpcAddresses, address)
		} else {
			panic("no propagation_grpcAddress setting found")
		}
	}

	propagationServers := []propagation_api.PropagationAPIClient{}
	for _, propagationGrpcAddress := range propagationGrpcAddresses {
		pConn, err := utils.GetGRPCClient(ctx, propagationGrpcAddress, &utils.ConnectionOptions{
			OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
			MaxRetries:  3,
		})
		if err != nil {
			panic(err)
		}
		propagationServers = append(propagationServers, propagation_api.NewPropagationAPIClient(pConn))
	}

	if len(propagationServers) == 0 {
		panic("No suitable propagation server connnection found")
	}

	numberOfOutputs, _ := gocore.Config().GetInt("number_of_outputs", 10_000)
	numberOfTransactions := uint32(1)
	satoshisPerOutput := uint64(1000)

	logger.Infof("Each worker with ask seeder to create %d transaction(s) with %d outputs of %d satoshis each",
		numberOfTransactions,
		numberOfOutputs,
		satoshisPerOutput,
	)

	var rateLimiter *rate.Limiter

	if *rateLimit > 1 {
		rateLimitDuration := (time.Duration(*workers) * time.Second) / (time.Duration(*rateLimit))
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
			for {
				select {
				case id := <-logIdsFile:
					_, _ = logFile.WriteString(id + "\n")
				}
			}
		}()
	}

	for i := 0; i < *workers; i++ {
		w := worker.NewWorker(
			numberOfOutputs,
			uint32(numberOfTransactions),
			satoshisPerOutput,
			seederServers,
			rateLimiter,
			propagationServers,
			kafkaProducer,
			kafkaTopic,
			ipv6MulticastConn,
			ipv6MulticastChan,
			printProgress,
			logIdsFile,
		)

		g.Go(func() error {
			// (ok) return w.Start(ctx)
			return w.Start(context.Background())
		})
	}

	// start http health check server
	http.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	if err := g.Wait(); err != nil {
		log.Fatalf("Error occurred in tx blaster: %v\n%s", err, debug.Stack())
		panic(err)
	}
}
