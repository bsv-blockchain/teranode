//go:build e2eTest

package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/coinbase"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

var (
	framework *tf.BitcoinTestFramework
)

func TestMain(m *testing.M) {
	// setupBitcoinTestFramework()
	// defer tearDownBitcoinTestFramework()

	m.Run()

	// os.Exit(exitCode)
}

func setupBitcoinTestFramework() {
	framework = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml"})
	m := map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tc1",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tc1",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tc1",
	}
	if err := framework.SetupNodes(m); err != nil {
		fmt.Printf("Error setting up nodes: %v\n", err)
		os.Exit(1)
	}
}

func tearDownBitcoinTestFramework() {
	if err := framework.StopNodes(); err != nil {
		fmt.Printf("Error stopping nodes: %v\n", err)
	}
}

func TestPropagation(t *testing.T) {
	ctx := context.Background()
	// url := "http://localhost:18090"

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("txblast", ulogger.WithLevel(logLevelStr))

	txDistributor, _ := distributor.NewDistributor(logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	coinbaseClient, err := coinbase.NewClientWithAddress(ctx, logger, "localhost:18093")
	if err != nil {
		t.Fatalf("Failed to create Coinbase client: %v", err)
	}

	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	fmt.Printf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	// coinbasePrivKey, _ := gocore.Config().Get("coinbase_wallet_private_key")
	// coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	// if err != nil {
	// 	t.Fatalf("Failed to decode Coinbase private key: %v", err)
	// }

	// coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)

	// privateKey, err := bec.NewPrivateKey(bec.S256())
	// if err != nil {
	// 	t.Fatalf("Failed to generate private key: %v", err)
	// }

	// address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	// if err != nil {
	// 	t.Fatalf("Failed to create address: %v", err)
	// }

	// tx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	// if err != nil {
	// 	t.Fatalf("Failed to request funds: %v", err)
	// }

	// _, err = txDistributor.SendTransaction(ctx, tx)
	// if err != nil {
	// 	t.Fatalf("Failed to send transaction: %v", err)
	// }

	// output := tx.Outputs[0]
	// utxo := &bt.UTXO{
	// 	TxIDHash:      tx.TxIDChainHash(),
	// 	Vout:          uint32(0),
	// 	LockingScript: output.LockingScript,
	// 	Satoshis:      output.Satoshis,
	// }

	// newTx := bt.NewTx()
	// err = newTx.FromUTXOs(utxo)
	// if err != nil {
	// 	t.Fatalf("Error adding UTXO to transaction: %s\n", err)
	// }

	// err = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	// if err != nil {
	// 	t.Fatalf("Error adding output to transaction: %v", err)
	// }

	// err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	// if err != nil {
	// 	t.Fatalf("Error filling transaction inputs: %v", err)
	// }

	// _, err = txDistributor.SendTransaction(ctx, newTx)
	// if err != nil {
	// 	t.Fatalf("Failed to send new transaction: %v", err)
	// }

	// height, _ := getBlockHeight(url)
	// fmt.Printf("Block height: %d\n", height)
	// for {
	// 	newHeight, _ := getBlockHeight(url)
	// 	if newHeight > height {
	// 		break
	// 	}
	// 	time.Sleep(1 * time.Second)
	// }

	// utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	// fmt.Printf("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)
	// assert.Less(t, utxoBalanceBefore, utxoBalanceAfter)
	require.NotEmptyf(t, txDistributor.GetPropagationGRPCAddresses(), "txDistributor.GetPropagationGRPCAddresses() is nil")
}

// func getBlockHeight(url string) (int, error) {
// 	resp, err := http.Get(url + "/api/v1/lastblocks?n=1")
// 	if err != nil {
// 		fmt.Printf("Error getting block height: %s\n", err)
// 		return 0, err
// 	}
// 	defer resp.Body.Close()
// 	if resp.StatusCode != http.StatusOK {
// 		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
// 	}

// 	var blocks []struct {
// 		Height int `json:"height"`
// 	}
// 	if err := json.NewDecoder(resp.Body).Decode(&blocks); err != nil {
// 		return 0, err
// 	}

// 	if len(blocks) == 0 {
// 		return 0, fmt.Errorf("no blocks found in response")
// 	}

// 	return blocks[0].Height, nil
// }
