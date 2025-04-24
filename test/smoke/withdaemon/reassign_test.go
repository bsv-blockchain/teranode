//go:build test_docker_daemon || debug

package smoke

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/require"
)

func TestShouldAllowReassign(t *testing.T) {
	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       false,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// Set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	_, err = td.CallRPC("generate", []interface{}{101})
	require.NoError(t, err)

	// Generate private keys and addresses for Alice, Bob, and Charles
	alicePrivateKey := td.GetPrivateKey(t)

	_, bob, _ := helper.GeneratePrivateKeyAndAddress()
	charlesPrivatekey, charles, _ := helper.GeneratePrivateKeyAndAddress()

	// Get coinbase transaction from block 1
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create parent transaction with outputs to Alice
	parentTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 1)
	require.NoError(t, err)

	// Create Alice to Bob transaction
	aliceToBobTx := bt.NewTx()
	utxos := &bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      parentTx.Outputs[0].Satoshis,
	}

	err = aliceToBobTx.FromUTXOs(utxos)
	require.NoError(t, err)

	err = aliceToBobTx.AddP2PKHOutputFromAddress(bob.AddressString, 10000)
	require.NoError(t, err)

	err = aliceToBobTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: alicePrivateKey})
	require.NoError(t, err)

	// Send Alice to Bob transaction
	_, err = td.DistributorClient.SendTransaction(td.Ctx, aliceToBobTx)
	require.NoError(t, err)

	// Mine a block and wait for processing
	_, err = td.CallRPC("generate", []interface{}{1})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)

	// Create throwaway transaction (Alice to Charles)
	throwawayTx := bt.NewTx()
	err = throwawayTx.FromUTXOs(utxos)
	require.NoError(t, err)

	err = throwawayTx.AddP2PKHOutputFromAddress(charles.AddressString, 1000)
	require.NoError(t, err)

	err = throwawayTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: alicePrivateKey})
	require.NoError(t, err)

	// Create reassign transaction (Charles spending throwaway tx output)
	reassignTx := bt.NewTx()
	reassignUtxo := &bt.UTXO{
		TxIDHash:      throwawayTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: throwawayTx.Outputs[0].LockingScript,
		Satoshis:      throwawayTx.Outputs[0].Satoshis,
	}

	err = reassignTx.FromUTXOs(reassignUtxo)
	require.NoError(t, err)

	err = reassignTx.AddP2PKHOutputFromAddress(bob.AddressString, 1000)
	require.NoError(t, err)

	err = reassignTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: charlesPrivatekey})
	require.NoError(t, err)

	// Freeze UTXO of Alice-Bob transaction
	aliceBobUtxoHash, err := util.UTXOHashFromOutput(aliceToBobTx.TxIDChainHash(), aliceToBobTx.Outputs[0], 0)
	require.NoError(t, err)

	spend := &utxo.Spend{
		TxID:     aliceToBobTx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: aliceBobUtxoHash,
	}

	err = td.UtxoStore.FreezeUTXOs(td.Ctx, []*utxo.Spend{spend}, td.Settings)
	require.NoError(t, err)

	amendedOutputScript := &bt.Output{
		Satoshis:      aliceToBobTx.Outputs[0].Satoshis,
		LockingScript: throwawayTx.Outputs[0].LockingScript,
	}

	// Reassign the UTXO to Charles
	reassignUtxoHash, err := util.UTXOHashFromOutput(aliceToBobTx.TxIDChainHash(), amendedOutputScript, 0)
	require.NoError(t, err)

	newSpend := &utxo.Spend{
		TxID:     aliceToBobTx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: reassignUtxoHash,
	}

	err = td.UtxoStore.ReAssignUTXO(td.Ctx, spend, newSpend, td.Settings)
	require.NoError(t, err)

	// Try to spend the reassigned UTXO before reassignment height - should fail
	charlesSpendingTx := bt.NewTx()
	charlesUtxo := &bt.UTXO{
		TxIDHash:      aliceToBobTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: throwawayTx.Outputs[0].LockingScript,
		Satoshis:      aliceToBobTx.Outputs[0].Satoshis,
	}

	err = charlesSpendingTx.FromUTXOs(charlesUtxo)
	require.NoError(t, err)

	err = charlesSpendingTx.AddP2PKHOutputFromAddress(bob.AddressString, 100)
	require.NoError(t, err)

	err = charlesSpendingTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: charlesPrivatekey})
	require.NoError(t, err)

	_, err = td.DistributorClient.SendTransaction(td.Ctx, charlesSpendingTx)
	require.Error(t, err, "Transaction should be rejected since UTXO is not spendable until block 1000")

	// Generate 1000 blocks to reach reassignment height
	_, err = td.CallRPC("generate", []interface{}{1000})
	require.NoError(t, err)

	// Now try spending the reassigned UTXO - should succeed
	_, err = td.DistributorClient.SendTransaction(td.Ctx, charlesSpendingTx)
	require.NoError(t, err)
} 