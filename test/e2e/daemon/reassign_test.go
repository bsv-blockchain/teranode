package smoke

import (
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/test/utils/transactions"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/require"
)

func TestShouldAllowReassign(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// Set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{2})
	require.NoError(t, err)

	// Generate private keys and addresses for Alice, Bob, and Charles
	alicePrivateKey := td.GetPrivateKey(t)

	bobPrivateKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)
	bob := bobPrivateKey.PubKey()

	charlesPrivatekey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)
	charles := charlesPrivatekey.PubKey()

	// Get coinbase transaction from block 1
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create parent transaction with outputs to Alice
	parentTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 1)
	require.NoError(t, err)

	aliceToBobTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0, alicePrivateKey),
		transactions.WithP2PKHOutputs(1, 10000, bob),
	)

	// Send Alice to Bob transaction
	_, err = td.DistributorClient.SendTransaction(td.Ctx, aliceToBobTx)
	require.NoError(t, err)

	// Mine a block and wait for processing
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err)

	throwawayTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0, alicePrivateKey),
		transactions.WithP2PKHOutputs(1, 10000, charles),
	)

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

	err = charlesSpendingTx.AddP2PKHOutputFromPubKeyBytes(bob.SerialiseCompressed(), 100)
	require.NoError(t, err)

	err = charlesSpendingTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: charlesPrivatekey})
	require.NoError(t, err)

	_, err = td.DistributorClient.SendTransaction(td.Ctx, charlesSpendingTx)
	require.Error(t, err, "Transaction should be rejected since UTXO is not spendable until block 1000")

	// Generate 1000 blocks to reach reassignment height
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1000})
	require.NoError(t, err)

	// Now try spending the reassigned UTXO - should succeed
	_, err = td.DistributorClient.SendTransaction(td.Ctx, charlesSpendingTx)
	require.NoError(t, err)
}
