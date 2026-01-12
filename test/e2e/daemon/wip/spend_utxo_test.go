package smoke

import (
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestShouldAllowSpendAllUtxos tests that we can spend all UTXOs with multiple transactions
func TestShouldAllowSpendAllUtxos(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// aerospike
	utxoStoreURL, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err, "Failed to setup Aerospike container")
	parsedURL, err := url.Parse(utxoStoreURL)
	require.NoError(t, err, "Failed to parse UTXO store URL")
	t.Cleanup(func() {
		_ = teardown()
	})

	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.UtxoStore.UtxoBatchSize = 2
				s.UtxoStore.UtxoStore = parsedURL
				s.GlobalBlockHeightRetention = 1
			},
		),
	})

	defer td.Stop(t)

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// create parent with 100 ouputs
	outputAmount := coinbaseTx.Outputs[0].Satoshis / 100

	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(100, outputAmount),
	)

	err = td.PropagationClient.ProcessTransaction(td.Ctx, parentTx)
	require.NoError(t, err)

	err = td.WaitForTransactionInBlockAssembly(parentTx, 10*time.Second)
	require.NoError(t, err)

	// print the parent tx
	rawTx := GetRawTx(t, td.UtxoStore, *parentTx.TxIDChainHash())
	PrintRawTx(t, "Parent Tx", rawTx.(map[string]interface{}))

	childTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(10, parentTx.Outputs[0].Satoshis/20),
	)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx)
	require.NoError(t, err)

	// creat another child tx
	childTx2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 1),
		transactions.WithP2PKHOutputs(10, parentTx.Outputs[1].Satoshis/20),
	)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx2)
	require.NoError(t, err)

	// verify the child tx is in the block assembly
	err = td.WaitForTransactionInBlockAssembly(childTx, 10*time.Second)
	require.NoError(t, err)

	err = td.WaitForTransactionInBlockAssembly(childTx2, 10*time.Second)
	require.NoError(t, err)

	// print the child tx
	rawTx = GetRawTx(t, td.UtxoStore, *childTx.TxIDChainHash())
	PrintRawTx(t, "Child Tx", rawTx.(map[string]interface{}))

	// spend all of the child tx
	grandchildTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(childTx, 0),
		transactions.WithInput(childTx, 1),
		transactions.WithInput(childTx, 2),
		transactions.WithInput(childTx, 3),
		transactions.WithInput(childTx, 4),
		transactions.WithInput(childTx, 5),
		transactions.WithInput(childTx, 6),
		transactions.WithInput(childTx, 7),
		transactions.WithInput(childTx, 8),
		transactions.WithInput(childTx, 9),
		transactions.WithP2PKHOutputs(10, childTx.Outputs[0].Satoshis),
	)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, grandchildTx)
	require.NoError(t, err)

	err = td.WaitForTransactionInBlockAssembly(grandchildTx, 10*time.Second)
	require.NoError(t, err)

	// print the grandchild tx
	rawTx = GetRawTx(t, td.UtxoStore, *grandchildTx.TxIDChainHash())
	PrintRawTx(t, "Grandchild Tx", rawTx.(map[string]interface{}))

	// mine a block
	td.MineAndWait(t, 2) // height 4

	// wait until the block height is 4
	// get the best block and keep checking the height
	// use ticker to check the height
	//timeout 10 seconds
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
waitLoop:
	for {
		select {
		case <-ticker.C:
			_, meta, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
			require.NoError(t, err)
			if meta.Height >= 4 {
				break waitLoop
			}
		case <-timeout.C:
			require.Fail(t, "Timeout waiting for block height to be 4")
		}
	}

	// delete at height should be set for the child tx
	rawTx = GetRawTx(t, td.UtxoStore, *childTx.TxIDChainHash())
	PrintRawTx(t, "Child Tx", rawTx.(map[string]interface{}))
	require.Equal(t, 6, rawTx.(map[string]interface{})[fields.DeleteAtHeight.String()])

	td.MineAndWait(t, 2)

waitLoop2:
	for {
		select {
		case <-ticker.C:
			_, meta, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
			require.NoError(t, err)
			if meta.Height >= 6 {
				break waitLoop2
			}
		case <-timeout.C:
			require.Fail(t, "Timeout waiting for block height to be 6")
		}
	}

	_, err = td.UtxoStore.Get(td.Ctx, childTx.TxIDChainHash())
	require.Error(t, err)

	// rawTx = GetRawTx(t, td.UtxoStore, *parentTx.TxIDChainHash())
	// PrintRawTx(t, "Parent Tx", rawTx.(map[string]interface{}))

	// the other child should not be deleted
	rawTx = GetRawTx(t, td.UtxoStore, *childTx2.TxIDChainHash())
	PrintRawTx(t, "Child Tx2", rawTx.(map[string]interface{}))
	require.Equal(t, rawTx.(map[string]interface{})[fields.DeleteAtHeight.String()], nil)

	// Spend the other child tx
	grandchildTx2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(childTx2, 0),
		transactions.WithP2PKHOutputs(1, childTx2.Outputs[0].Satoshis),
	)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, grandchildTx2)
	require.NoError(t, err)

	err = td.WaitForTransactionInBlockAssembly(grandchildTx2, 10*time.Second)
	require.NoError(t, err)
}
