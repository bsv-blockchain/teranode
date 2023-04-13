package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/propagation/propagation_api"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/go-bitcoin"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var logger utils.Logger
var rpcURL *url.URL
var startTime time.Time
var propagationServer propagation_api.PropagationAPIClient

func init() {
	logger = gocore.Log("txblaster", gocore.NewLogLevelFromString("debug"))

	bitcoinRpcUri := os.Getenv("BITCOIN_RPC_URI")

	var err error
	rpcURL, err = url.Parse(bitcoinRpcUri)
	if err != nil {
		panic(err)
	}

	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100 * 1024 * 1024)), // 100MB, TODO make configurable
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	propagationGrpcAddress, ok := gocore.Config().Get("propagation_grpcAddress")
	if !ok {
		panic("no propagation_grpcAddress setting found")
	}
	conn, err := grpc.Dial(propagationGrpcAddress, opts...)
	if err != nil {
		panic(err)
	}
	propagationServer = propagation_api.NewPropagationAPIClient(conn)
}

func main() {
	// create new private key
	keySet, err := New()
	if err != nil {
		panic(err)
	}

	address := keySet.Address(false)

	for {
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
	}

	select {}
}

func fireTransactions(u *bt.UTXO, keyset *KeySet) error {
	tx := bt.NewTx()
	_ = tx.FromUTXOs(u)

	nrOutputs := u.Satoshis
	if nrOutputs > 1000 {
		nrOutputs = 1000
	}

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

	for vout, output := range tx.Outputs {
		go func(vout int, output bt.Output) {
			err = fireTransactions(&bt.UTXO{
				TxID:          tx.TxIDBytes(),
				Vout:          uint32(vout),
				LockingScript: output.LockingScript,
				Satoshis:      output.Satoshis,
			}, keyset)
		}(vout, *output)
	}

	return nil
}

var counter atomic.Uint64

func sendTransaction(tx *bt.Tx) error {
	counter.Add(1)

	if _, err := propagationServer.Set(context.Background(), &propagation_api.SetRequest{
		Tx: tx.Bytes(),
	}); err != nil {
		return errors.New(fmt.Sprintf("error sending transaction to propagation server: %v", err))
	}

	counterLoad := counter.Load()
	if counterLoad%1000 == 0 {
		fmt.Printf("Time for %d transactions: %s\n", counterLoad, time.Since(startTime))
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
