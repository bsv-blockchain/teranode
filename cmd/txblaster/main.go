package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/TAAL-GmbH/ubsv/cmd/txblaster/extra"
	"github.com/TAAL-GmbH/ubsv/services/propagation/propagation_api"
	"github.com/TAAL-GmbH/ubsv/services/seeder/seeder_api"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/time/rate"
)

const NUMBER_OF_OUTPUTS = 10_000

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

func init() {
	gocore.SetInfo(progname, version, commit)
	logger = gocore.Log("txblaster", gocore.NewLogLevelFromString("debug"))
}

func main() {
	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	workers := flag.Int("workers", runtime.NumCPU(), "how many workers to use for blasting")
	rateLimit := flag.Int("limit", -1, "rate limit tx/s")
	useHTTPFlag := flag.Bool("http", false, "use http instead of grpc to send transactions")
	printFlag := flag.Int("print", 0, "print out progress every x transactions")

	flag.Parse()

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
		_ = http.ListenAndServe("localhost:9099", nil)
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

	txChan = make(chan *bt.UTXO, NUMBER_OF_OUTPUTS)

	// create new private key
	keySet, err := extra.New()
	if err != nil {
		panic(err)
	}

	numberOfTransactions := uint32(1)
	satoshisPerOutput := uint64(1000)
	numberOfOutputs, _ := gocore.Config().GetInt("number_of_outputs", 10_000)

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
	tx := bt.NewTx()
	_ = tx.FromUTXOs(u)

	_ = tx.PayTo(keySet.Script, u.Satoshis)

	unlockerGetter := unlocker.Getter{PrivateKey: keySet.PrivateKey}
	if err := tx.FillAllInputs(context.Background(), &unlockerGetter); err != nil {
		return err
	}

	err := sendTransaction(tx)
	if err != nil {
		return err
	}

	txChan <- &bt.UTXO{
		TxID:          tx.TxIDBytes(),
		Vout:          0,
		LockingScript: keySet.Script,
		Satoshis:      u.Satoshis,
	}

	return nil
}

var counter atomic.Uint64

func sendTransaction(tx *bt.Tx) error {
	traceSpan := tracing.Start(context.Background(), "txBlaster:sendTransaction")
	defer traceSpan.Finish()

	traceSpan.SetTag("progname", "txblaster")
	traceSpan.SetTag("txid", tx.TxID())

	if useHTTP {
		traceSpan.SetTag("transport", "http")

		req, err := http.NewRequestWithContext(traceSpan.Ctx, "POST", httpURL, bytes.NewBuffer(tx.ExtendedBytes()))
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
			Tx: tx.ExtendedBytes(),
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
