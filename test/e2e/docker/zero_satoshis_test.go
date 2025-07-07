//go:build test_smoke || test_functional

// How to run each test:
// Clean up docker containers before running the test manually
// $ cd test/smoke/
// $ go test -v -run "^TestZeroSatoshisTestSuite$/TestZeroSatoshisOutput$" -tags test_functional

package smoke

import (
	"errors"
	"fmt"
	"testing"
	"time"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/stubs"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ZeroSatoshisTestSuite struct {
	helper.TeranodeTestSuite
}

func TestZeroSatoshisTestSuite(t *testing.T) {
	suite.Run(t, &ZeroSatoshisTestSuite{})
}

func (suite *ZeroSatoshisTestSuite) TestZeroSatoshisOutput() {
	t := suite.T()
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	//logger := testEnv.Logger

	txDistributor := testEnv.Nodes[0].DistributorClient

	coinbaseClient := testEnv.Nodes[0].CoinbaseClient
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey := testEnv.Nodes[0].Settings.Coinbase.WalletPrivateKey
	if coinbasePrivKey == "" {
		t.Errorf("Coinbase private key is not set")
	}

	coinbasePrivateKey, err := bec.PrivateKeyFromWif(coinbasePrivKey)
	if err != nil {
		t.Errorf("Failed to decode Coinbase private key: %v", err)
	}

	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PubKey(), true)

	privateKey, err := bec.NewPrivateKey()
	if err != nil {
		t.Errorf("Failed to generate private key: %v", err)
	}

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		t.Errorf("Failed to create address: %v", err)
	}

	var tx *bt.Tx

	timeout := time.After(10 * time.Second)

	err2 := coinbaseClient.SetMalformedUTXOConfig(ctx, 100, stubs.MalformationType(0))
	errorUnwrap := errors.Unwrap(err2)
	t.Logf("err: %v", errorUnwrap)
	require.NoError(t, errorUnwrap, "Failed to set malformed utxo config: %v", errorUnwrap)

loop:
	for tx == nil || err != nil {
		select {
		case <-timeout:
			break loop
		default:
			tx, err = coinbaseClient.RequestFunds(ctx, address.AddressString, true)
			if err != nil {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	require.NoError(t, err, "Failed to request funds: %v", err)

	t.Logf("Transaction: %s %s\n", tx.TxIDChainHash(), tx.TxID())

	// print tx outputs
	// for _, output := range tx.Outputs {
	// 	t.Logf("Output: %s %d", output.LockingScript, output.Satoshis)
	// }

	// check tx is malformed
	require.Equal(t, uint64(0), tx.Outputs[0].Satoshis, "Transaction output satoshis should be 0")

	_, err = txDistributor.SendTransaction(ctx, tx)
	if err != nil {
		t.Errorf("Failed to send transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %v\n", tx.TxIDChainHash(), len(tx.Outputs))
	output := tx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
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

	// send the transaction. We expect this to fail if the tx is malformed
	_, err = txDistributor.SendTransaction(ctx, newTx)
	require.Error(t, err, "Transaction should be rejected")
	require.Contains(t, err.Error(), "transaction fee is too low")

	txDistributor.TriggerBatcher() // just in case there is a delay in processing txs
}
