//go:build test_tnb || debug

// How to run:
// go test -v -timeout 30s -tags "test_tnb" -run ^TestUnspentTransactionOutputsWithPostgres$

package tnb

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/utxo"
	utils "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/require"
)

func TestValidatedTxShouldSpendInputsWithPostgres(t *testing.T) {
	ctx := context.Background()

	td := utils.SetupPostgresTestDaemon(t, ctx, "spend-inputs")

	// Generate initial blocks
	_, err := td.CallRPC("generate", []interface{}{101})
	require.NoError(t, err)

	// Create key pairs for testing
	privateKey, _ := bec.NewPrivateKey(bec.S256())
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	// Create another address for second output
	privateKey2, _ := bec.NewPrivateKey(bec.S256())
	address2, _ := bscript.NewAddressFromPublicKey(privateKey2.PubKey(), true)

	// Get funds from coinbase
	block1, err := td.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	coinbaseTx := block1.CoinbaseTx

	w, err := wif.DecodeWIF(td.Settings.BlockAssembly.MinerWalletPrivateKeys[0])
	require.NoError(t, err)

	coinbaseTxPrivateKey := w.PrivKey

	// Create a transaction with multiple outputs
	tx := bt.NewTx()
	err = tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	// Add two outputs with different amounts
	amount1 := uint64(10000)
	amount2 := uint64(20000)
	err = tx.AddP2PKHOutputFromAddress(address.AddressString, amount1)
	require.NoError(t, err)
	err = tx.AddP2PKHOutputFromAddress(address2.AddressString, amount2)
	require.NoError(t, err)

	// Sign and send the transaction
	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: coinbaseTxPrivateKey})
	require.NoError(t, err)
	_, err = td.DistributorClient.SendTransaction(ctx, tx)
	require.NoError(t, err, "Failed to send transaction")

	// Check if the tx is into the UTXOStore
	utxos, errTxRes := td.UtxoStore.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, errTxRes, "Failed to get utxo")

	if utxos.Tx != nil {
		spentCoinbaseOutput := utxos.Tx.Outputs[0]
		t.Logf("UTXO #%d: Value=%d Satoshis, Script=%x\n", 0, spentCoinbaseOutput.Satoshis, *spentCoinbaseOutput.LockingScript)
		utxoHash, _ := util.UTXOHashFromOutput(coinbaseTx.TxIDChainHash(), spentCoinbaseOutput, uint32(0))
		spend := &utxo.Spend{
			TxID:     coinbaseTx.TxIDChainHash(),
			Vout:     uint32(0),
			UTXOHash: utxoHash,
		}
		spendStatus, err := td.UtxoStore.GetSpend(ctx, spend)
		require.NoError(t, err)
		t.Logf("UTXO #%d spend status: %+v\n", 0, spendStatus)
		require.Equal(t, spendStatus.Status, 1)
		require.Equal(t, spendStatus.SpendingTxID, tx.TxIDChainHash())
	} else {
		t.Logf("No tx found into meta.Data")
	}
}
