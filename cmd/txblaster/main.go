package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/propagation/propagation_api"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/go-bitcoin"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
)

var logger utils.Logger
var rpcURL *url.URL
var startTime time.Time
var propagationServer propagation_api.PropagationAPIClient
var txChan chan *bt.UTXO
var printProgress uint64
var useHTTP bool
var httpURL string
var httpClient *http.Client

func init() {
	logger = gocore.Log("txblaster", gocore.NewLogLevelFromString("debug"))
}

func main() {
	workers := flag.Int("workers", runtime.NumCPU(), "how many workers to use for blasting")
	rateLimit := flag.Int("limit", -1, "rate limit tx/s")
	useHTTPFlag := flag.Bool("http", false, "use http instead of grpc to send transactions")
	printFlag := flag.Int("print", 0, "print out progress every x transactions")
	useTracer := flag.Bool("tracer", false, "start tracer")

	flag.Parse()

	printProgress = uint64(*printFlag)

	if *useTracer {
		logger.Infof("Starting tracer")
		closeTracer := tracing.InitOtelTracer()
		defer closeTracer()
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
		_ = http.ListenAndServe(":9099", nil)
	}()

	bitcoinRpcUri := os.Getenv("BITCOIN_RPC_URI")

	if bitcoinRpcUri == "" {
		panic("You must set BITCOIN_RPC_URI environment variable")
	}

	var err error
	rpcURL, err = url.Parse(bitcoinRpcUri)
	if err != nil {
		panic(err)
	}

	propagationGrpcAddress, ok := gocore.Config().Get("propagation_grpcAddress")
	if !ok {
		panic("no propagation_grpcAddress setting found")
	}

	conn, err := utils.GetGRPCClient(context.Background(), propagationGrpcAddress, &utils.ConnectionOptions{
		Tracer: *useTracer,
	})
	if err != nil {
		panic(err)
	}
	propagationServer = propagation_api.NewPropagationAPIClient(conn)

	txChan = make(chan *bt.UTXO, 10_000)

	// create new private key
	keySet, err := New()
	if err != nil {
		panic(err)
	}

	address := keySet.Address(false)

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

	var u *bt.UTXO
	u, err = sendToAddress(address, 50_000_000)
	if err != nil {
		panic(err)
	}

	startTime = time.Now()
	err = fireTransactions(u, keySet)
	if err != nil {
		fmt.Printf("ERROR in fire transactions: %v", err)
	}

	select {}
}

func txWorker(keySet *KeySet, txChan <-chan *bt.UTXO) {
	for utxo := range txChan {
		err := fireTransactions(utxo, keySet)
		if err != nil {
			fmt.Printf("ERROR in fire transactions: %v", err)
		}
	}
}

func txWorkerLimited(keySet *KeySet, rateLimitDuration time.Duration, txChan <-chan *bt.UTXO) {
	limiter := rate.NewLimiter(rate.Every(rateLimitDuration), 1)
	for utxo := range txChan {
		_ = limiter.Wait(context.Background())
		err := fireTransactions(utxo, keySet)
		if err != nil {
			fmt.Printf("ERROR in fire transactions: %v", err)
		}
	}
}

func fireTransactions(u *bt.UTXO, keyset *KeySet) error {
	tx := bt.NewTx()
	_ = tx.FromUTXOs(u)

	nrOutputs := u.Satoshis
	if nrOutputs > 1000 {
		nrOutputs = 1000
	}

	//fmt.Printf("Firing %d outputs, Satoshis: %d - %d\n", nrOutputs, u.Satoshis, u.Satoshis/nrOutputs)
	for i := uint64(0); i < nrOutputs; i++ {
		_ = tx.PayTo(keyset.Script, u.Satoshis/nrOutputs) // add 1 satoshi to allow for our longer OP_RETURN
	}
	unlockerGetter := unlocker.Getter{PrivateKey: keyset.PrivateKey}
	if err := tx.FillAllInputs(context.Background(), &unlockerGetter); err != nil {
		return err
	}

	err := sendTransaction(tx)
	if err != nil {
		return err
	}

	go func(txOutputs []*bt.Output) {
		for vout, output := range txOutputs {
			txChan <- &bt.UTXO{
				TxID:          tx.TxIDBytes(),
				Vout:          uint32(vout),
				LockingScript: output.LockingScript,
				Satoshis:      output.Satoshis,
			}
		}
	}(tx.Outputs)

	return nil
}

var counter atomic.Uint64

func sendTransaction(tx *bt.Tx) error {
	ctx, span := otel.Tracer("").Start(context.Background(), "txBlaster:sendTransaction")
	defer span.End()

	span.SetAttributes(attribute.String("progname", "txblaster"))
	span.AddEvent("sendTransaction", trace.WithAttributes(attribute.String("txid", tx.TxID())))

	if useHTTP {
		span.SetAttributes(attribute.String("transport", "http"))

		req, err := http.NewRequestWithContext(ctx, "POST", httpURL, bytes.NewBuffer(tx.ExtendedBytes()))
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
		span.SetAttributes(attribute.String("transport", "grpc"))

		if _, err := propagationServer.Set(ctx, &propagation_api.SetRequest{
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

func sendToAddress(address string, satoshis uint64) (*bt.UTXO, error) {
	client, err := bitcoin.NewFromURL(rpcURL, false)
	if err != nil {
		logger.Fatalf("Could not create bitcoin client: %v", err)
	}

	amount := float64(satoshis) / 1e8

	txid, err := client.SendToAddress(address, amount)
	if err != nil {
		return nil, err
	}

	tx, err := client.GetRawTransaction(txid)
	if err != nil {
		return nil, err
	}

	btTx, err := bt.NewTxFromString(tx.Hex)
	if err != nil {
		return nil, err
	}

	// enrich the transaction with parent locking script and satoshis
	var parentTx *bitcoin.RawTransaction
	for idx, input := range tx.Vin {
		parentTx, err = client.GetRawTransaction(input.Txid)
		if err != nil {
			return nil, err
		}
		btTx.Inputs[idx].PreviousTxScript, _ = bscript.NewFromHexString(parentTx.Vout[input.Vout].ScriptPubKey.Hex)
		btTx.Inputs[idx].PreviousTxSatoshis = uint64(parentTx.Vout[input.Vout].Value * 1e8)
	}

	err = sendTransaction(btTx)
	if err != nil {
		return nil, err
	}

	for i, vout := range tx.Vout {
		if vout.ScriptPubKey.Addresses[0] == address {
			txIDBytes, _ := hex.DecodeString(txid)
			lockingScript, _ := bscript.NewFromHexString(vout.ScriptPubKey.Hex)
			return &bt.UTXO{
				TxID:          txIDBytes,
				Vout:          uint32(i),
				Satoshis:      satoshis,
				LockingScript: lockingScript,
			}, nil
		}
	}

	return nil, errors.New("utxo not found in tx")
}
