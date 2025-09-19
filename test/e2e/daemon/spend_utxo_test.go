package smoke

import (
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/stretchr/testify/require"
)

// TestShouldAllowSpendAllUtxos tests that we can spend all UTXOs with multiple transactions
func TestShouldAllowSpendAllUtxos(t *testing.T) {
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
	td.MineAndWait(t, 101)

	// Generate private key and address for recipient
	privateKey1, err := bec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")

	address1, err := bscript.NewAddressFromPublicKey(privateKey1.PubKey(), true)
	require.NoError(t, err, "Failed to create address")

	// Get coinbase transaction from block 1
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err, "Failed to get block 1")

	parentTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 10)
	require.NoError(t, err, "Failed to create parent transaction")

	// Split outputs into two parts
	firstSet := len(parentTx.Outputs) - 2
	secondSet := firstSet

	createAndSendTx := func(outputs []*bt.Output, startIndex int) (*bt.Tx, error) {
		td.Logger.Infof("Creating and sending transaction with %d outputs", len(outputs))

		spendingTx := bt.NewTx()
		totalSatoshis := uint64(0)
		utxos := make([]*bt.UTXO, 0)

		for i, output := range outputs {
			idx := i + startIndex
			utxo := &bt.UTXO{
				TxIDHash:      parentTx.TxIDChainHash(),
				Vout:          uint32(idx),
				LockingScript: output.LockingScript,
				Satoshis:      output.Satoshis,
			}
			utxos = append(utxos, utxo)
			totalSatoshis += output.Satoshis
		}

		err := spendingTx.FromUTXOs(utxos...)
		require.NoError(t, err, "Error adding UTXOs to transaction")

		fee := uint64(1000)
		amountToSend := totalSatoshis - fee

		err = spendingTx.AddP2PKHOutputFromAddress(address1.AddressString, amountToSend)
		require.NoError(t, err, "Error adding output to transaction")

		err = spendingTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: td.GetPrivateKey(t)})
		require.NoError(t, err, "Error filling transaction inputs")

		_, err = td.DistributorClient.SendTransaction(td.Ctx, spendingTx)
		if err != nil {
			return nil, err
		}

		return spendingTx, nil
	}

	// Create and send first transaction
	tx1, err := createAndSendTx(parentTx.Outputs[:firstSet], 0)
	require.NoError(t, err, "Failed to create and send first transaction")
	td.Logger.Infof("First Transaction sent: %s", tx1.TxID())

	// Create and send second transaction
	tx2, err := createAndSendTx(parentTx.Outputs[secondSet:], secondSet)
	require.NoError(t, err, "Failed to create and send second transaction")
	td.Logger.Infof("Second Transaction sent: %s", tx2.TxID())

	// Mine a block
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to mine block")

	// Verify both transactions are in block
	block102, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 102)
	require.NoError(t, err)

	err = block102.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	err = block102.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)
}
