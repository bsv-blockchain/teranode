package tna

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
)

func TestTxsReceivedAllNodes(t *testing.T) {
	ctx := context.Background()

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("test", ulogger.WithLevel(logLevelStr))

	// Send transactions

	txDistributor, _ := distributor.NewDistributor(ctx, logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	coinbaseClient := framework.Nodes[0].CoinbaseClient
	coinbasePrivKey, _ := gocore.Config().Get("coinbase_wallet_private_key")
	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	if err != nil {
		t.Errorf("Failed to decode Coinbase private key: %v", err)
	}

	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)

	privateKey, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		t.Errorf("Failed to generate private key: %v", err)
	}

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		t.Errorf("Failed to create address: %v", err)
	}

	tx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		t.Errorf("Failed to request funds: %v", err)
	}
	fmt.Printf("Transaction: %s %s\n", tx.TxIDChainHash(), tx.TxID())

	_, err = txDistributor.SendTransaction(ctx, tx)
	if err != nil {
		t.Errorf("Failed to send transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %v\n", tx.TxIDChainHash(), len(tx.Outputs))

	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: tx.Outputs[0].LockingScript,
		Satoshis:      tx.Outputs[0].Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	if err != nil {
		t.Errorf("Error adding UTXO to transaction: %s\n", err)
	}

	err = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	if err != nil {
		t.Errorf("Error adding output to transaction: %v", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		t.Errorf("Error filling transaction inputs: %v", err)
	}

	_, err = txDistributor.SendTransaction(ctx, newTx)
	if err != nil {
		t.Errorf("Failed to send new transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())
	time.Sleep(5 * time.Second)

	// If Block Assembly removes correctly the Tx, it means that Tx exists

	errTx0 := framework.Nodes[0].BlockassemblyClient.RemoveTx(ctx, newTx.TxIDChainHash())

	if errTx0 != nil {
		t.Errorf("Error Tx0 not present: %v", errTx0)
	}

	errTx1 := framework.Nodes[1].BlockassemblyClient.RemoveTx(ctx, newTx.TxIDChainHash())

	if errTx1 != nil {
		t.Errorf("Error Tx1 not present: %v", errTx1)
	}

	errTx2 := framework.Nodes[2].BlockassemblyClient.RemoveTx(ctx, newTx.TxIDChainHash())

	if errTx2 != nil {
		t.Errorf("Error Tx2 not present: %v", errTx2)
	}
}
