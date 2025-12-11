package smoke

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/stretchr/testify/require"
)

func TestUTXOValidation(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()
	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
	})

	defer td.Stop(t)

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate blocks
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	block1, err := td.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	parenTx := block1.CoinbaseTx

	pk, err := bec.PrivateKeyFromWif(td.Settings.BlockAssembly.MinerWalletPrivateKeys[0])
	require.NoError(t, err)

	publicKey := pk.PubKey().Compressed()

	utxo := &bt.UTXO{
		TxIDHash:      parenTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: parenTx.Outputs[0].LockingScript,
		Satoshis:      parenTx.Outputs[0].Satoshis,
	}

	tx := bt.NewTx()

	err = tx.FromUTXOs(utxo)
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromPubKeyBytes(publicKey, 10000)
	require.NoError(t, err)

	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: pk})
	require.NoError(t, err)

	err = td.PropagationClient.ProcessTransaction(ctx, tx)
	require.NoError(t, err)

	utxo = &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: tx.Outputs[0].LockingScript,
		Satoshis:      tx.Outputs[0].Satoshis,
	}

	// Create another transaction
	anotherTx := bt.NewTx()
	err = anotherTx.FromUTXOs(utxo)
	require.NoError(t, err)

	err = anotherTx.AddP2PKHOutputFromPubKeyBytes(publicKey, 10000)
	require.NoError(t, err)

	err = anotherTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: pk})
	require.NoError(t, err)

	_, err = td.UtxoStore.Spend(ctx, tx, 1)
	require.NoError(t, err)

	err = td.PropagationClient.ProcessTransaction(ctx, anotherTx)

	require.Nil(t, err)
}

func TestScriptValidation(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()
	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
	})

	defer td.Stop(t)

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate blocks
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	block1, err := td.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	parenTx := block1.CoinbaseTx

	// Create a valid key pair for testing
	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	utxo := &bt.UTXO{
		TxIDHash:      parenTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: parenTx.Outputs[0].LockingScript,
		Satoshis:      parenTx.Outputs[0].Satoshis,
	}

	invalidTx := bt.NewTx()
	err = invalidTx.FromUTXOs(utxo)
	require.NoError(t, err)

	err = invalidTx.AddP2PKHOutputFromAddress(address.AddressString, 10000)
	require.NoError(t, err)

	// Use a different private key to create an invalid signature
	wrongPrivateKey, _ := bec.NewPrivateKey()
	err = invalidTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: wrongPrivateKey})
	require.NoError(t, err)

	err = td.PropagationClient.ProcessTransaction(ctx, invalidTx)
	require.Error(t, err, "Transaction with invalid signature should be rejected")
	require.Contains(t, err.Error(), "failed to validate transaction", "Error should indicate script validation failure")

	t.Log("Script validation test completed successfully")
}
