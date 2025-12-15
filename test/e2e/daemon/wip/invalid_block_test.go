package smoke

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	primitives "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	testSettings "github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/testcontainers"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrphanTx(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		// EnableFullLogging: true,
		SettingsOverrideFunc: func(settings *settings.Settings) {
			// settings.Asset.HTTPPort = 18090
			settings.Validator.UseLocalValidator = true
			settings.TracingEnabled = true
			settings.TracingSampleRate = 1.0
		},
	})

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableP2P: true,
		// EnableFullLogging: true,
		SettingsOverrideFunc: func(settings *settings.Settings) {
			// settings.Asset.HTTPPort = 28090
			settings.Validator.UseLocalValidator = true
			settings.TracingEnabled = true
			settings.TracingSampleRate = 1.0
		},
	})

	tracer := tracing.Tracer("testDaemon")

	node1Ctx, _, endSpan1 := tracer.Start(
		context.Background(),
		"TestOrphanTx",
		tracing.WithTag("node", "node1"),
	)

	node2Ctx, _, endSpan2 := tracer.Start(
		context.Background(),
		"TestOrphanTx",
		tracing.WithTag("node", "node2"),
	)

	t.Cleanup(func() {
		endSpan2()
		endSpan1()

		node2.Stop(t)
		node1.Stop(t)
	})

	// Generate 2 blocks on node 1
	_, err := node1.CallRPC(node1Ctx, "generate", []any{2})
	require.NoError(t, err)

	block2, err := node1.BlockchainClient.GetBlockByHeight(node1Ctx, 2)
	require.NoError(t, err)

	// Wait for block assembler to catch up on node 1
	node1.WaitForBlockHeight(t, block2, 1*time.Second)

	// Node 2 should have received notifications for these blocks over P2P.
	// Wait for block assembler to catch up on node 2
	// node2.WaitForBlockHeight(t, block2, 10*time.Second, true)

	// Get block 1's coinbase tx from node 1 and create a transaction that spends the 0th output of block 1's coinbase
	block1, err := node1.BlockchainClient.GetBlockByHeight(node1Ctx, 1)
	require.NoError(t, err)

	coinbaseTxFromNode1, err := getTx(t, node1.AssetURL, block1.CoinbaseTx.TxID())
	require.NoError(t, err)

	// Create a transaction that spends the 0th output of block 1's coinbase parentTx
	// parentTx := node1.CreateTransaction(t, coinbaseTxFromNode1)
	parentTx := bt.NewTx()

	err = parentTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTxFromNode1.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTxFromNode1.Outputs[0].LockingScript,
		Satoshis:      coinbaseTxFromNode1.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = parentTx.AddP2PKHOutputFromPubKeyBytes(node1.GetPrivateKey(t).PubKey().Compressed(), 10000)
	require.NoError(t, err)

	require.NoError(t, parentTx.FillAllInputs(node1Ctx, &unlocker.Getter{PrivateKey: node1.GetPrivateKey(t)}))

	// Check that the transaction is signed
	assert.Greater(t, len(parentTx.Inputs[0].UnlockingScript.Bytes()), 0)
	t.Logf("parentTx: %s", parentTx.TxID())

	childTx := node1.CreateTransaction(t, parentTx)
	t.Logf("childTx: %s", childTx.TxID())

	err = node1.PropagationClient.ProcessTransaction(node1Ctx, parentTx)
	require.NoError(t, err)

	err = node2.PropagationClient.ProcessTransaction(node2Ctx, parentTx)
	require.NoError(t, err)

	// Now we've sent the tx to both nodes, let's retrieve them to check they have been stored
	txFromNode1, err := getTx(t, node1.AssetURL, parentTx.TxIDChainHash().String())
	require.NoError(t, err)

	node1.VerifyInBlockAssembly(t, parentTx)

	txFromNode2, err := getTx(t, node2.AssetURL, parentTx.TxIDChainHash().String())
	require.NoError(t, err)

	node2.VerifyInBlockAssembly(t, parentTx)

	assert.Equal(t, txFromNode1.String(), txFromNode2.String())

	// restart node 1
	node1.Stop(t, true)

	node1.ResetServiceManagerContext(t)

	node1 = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		SkipRemoveDataDir: true, // we are re-starting so don't delete data dir
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 18090
			settings.Validator.UseLocalValidator = true
			settings.TracingEnabled = true
			settings.TracingSampleRate = 1.0
		},
	})

	node1.WaitForBlockHeight(t, block2, 1*time.Second)

	// send a child tx to both
	err = node1.PropagationClient.ProcessTransaction(node1Ctx, childTx)
	require.NoError(t, err)

	err = node2.PropagationClient.ProcessTransaction(node2Ctx, childTx)
	require.NoError(t, err)

	// verify the child tx is in the block assembly
	node1.VerifyInBlockAssembly(t, childTx)
	node1.VerifyNotInBlockAssembly(t, parentTx)

	node2.VerifyInBlockAssembly(t, childTx)
	node2.VerifyInBlockAssembly(t, parentTx)

	// generate a block on node1
	_, err = node1.CallRPC(node1Ctx, "generate", []any{1})
	require.NoError(t, err)

	// verify the child tx is in the utxo store
	utxo, err := node1.UtxoStore.Get(node1Ctx, childTx.TxIDChainHash())
	require.NoError(t, err)

	assert.Equal(t, uint32(3), utxo.BlockHeights[0], "childtx should be (incorrectly) mined in block 3 on node1")

	utxo, err = node1.UtxoStore.Get(node1Ctx, parentTx.TxIDChainHash())
	require.NoError(t, err)

	assert.Equal(t, 0, len(utxo.BlockHeights), "parentTx should not be mined on node1")

	block3, err := node1.BlockchainClient.GetBlockByHeight(node1Ctx, 3)
	require.NoError(t, err)

	err = block3.GetAndValidateSubtrees(node1Ctx, node1.Logger, node1.SubtreeStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	err = block3.CheckMerkleRoot(node1Ctx)
	require.NoError(t, err)

	subtree, err := block3.GetSubtrees(node1Ctx, node1.Logger, node1.SubtreeStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	blFound := false

	for i := 0; i < len(subtree); i++ {
		st := subtree[i]
		for _, node := range st.Nodes {
			t.Logf("node.Hash: %s", node.Hash.String())
			t.Logf("tx.TxIDChainHash().String(): %s", childTx.TxIDChainHash().String())

			if node.Hash.String() == parentTx.TxIDChainHash().String() {
				blFound = true
				break
			}
		}
	}

	require.False(t, blFound)

	// get the block height on node2
	blockHeightNode2, _, err := node2.BlockchainClient.GetBestHeightAndTime(node2Ctx)
	require.NoError(t, err)

	assert.Equal(t, uint32(2), blockHeightNode2, "Block height on node2 should be 2")

	utxo, err = node2.UtxoStore.Get(node2Ctx, childTx.TxIDChainHash())
	require.NoError(t, err)

	require.Equal(t, len(utxo.BlockHeights), 0, "childtx on node2 should have no block heights")
	// assert.Equal(t, uint32(3), utxo.BlockHeights[0], "childtx on node2 should have block height 3")

	utxo, err = node2.UtxoStore.Get(node2Ctx, parentTx.TxIDChainHash())
	require.NoError(t, err)

	assert.Nil(t, utxo.BlockHeights, "parentTx on node2 should have no block heights")
}

func getTx(t *testing.T, assetURL string, txID string) (*bt.Tx, error) {
	resp, err := http.Get(fmt.Sprintf("%s/api/v1/tx/%s", assetURL, txID))
	require.NoError(t, err)

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, errRead := io.ReadAll(resp.Body)
		if errRead != nil {
			return nil, errors.NewProcessingError("expected status code %d, got %d. Additionally, failed to read response body: %v", http.StatusOK, resp.StatusCode, errRead)
		}

		return nil, errors.NewProcessingError("expected status code %d, got %d. Response body: %s", http.StatusOK, resp.StatusCode, string(bodyBytes))
	}

	var tx bt.Tx

	_, err = tx.ReadFrom(resp.Body)
	require.NoError(t, err)

	return &tx, nil
}

func TestInvalidBlockWithContainer(t *testing.T) {
	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		Path:        "../..",
		ComposeFile: "docker-compose-host.yml",
		Profiles:    []string{"teranode1", "teranode2"},
		HealthServicePorts: []testcontainers.ServicePort{
			{ServiceName: "teranode1", Port: 18000},
			{ServiceName: "teranode2", Port: 28000},
		},
	})
	require.NoError(t, err)

	defer tc.Cleanup(t)

	node1 := tc.GetNodeClients(t, "docker.host.teranode1")
	node2 := tc.GetNodeClients(t, "docker.host.teranode2")

	_, err = helper.CallRPC(node1.RPCURL.String(), "generate", []any{101})
	require.NoError(t, err, "Failed to generate blocks")

	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 101, 20*time.Second)
	require.NoError(t, err)

	err = helper.WaitForNodeBlockHeight(t.Context(), node2.BlockchainClient, 101, 20*time.Second)
	require.NoError(t, err)

	block1, err := node1.BlockchainClient.GetBlockByHeight(t.Context(), 1)
	require.NoError(t, err)

	coinbaseTxFromNode1 := block1.CoinbaseTx

	t.Logf("coinbaseTxFromNode1: %s", coinbaseTxFromNode1.String())
	t.Logf("parentTx: %s", coinbaseTxFromNode1.TxIDChainHash())

	_, err = getTx(t, fmt.Sprintf("http://localhost:%d", 18090), coinbaseTxFromNode1.TxIDChainHash().String())
	require.NoError(t, err)

	_, err = getTx(t, fmt.Sprintf("http://localhost:%d", 28090), coinbaseTxFromNode1.TxIDChainHash().String())
	require.NoError(t, err)

	tx := bt.NewTx()

	err = tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTxFromNode1.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTxFromNode1.Outputs[0].LockingScript,
		Satoshis:      coinbaseTxFromNode1.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	privKey, err := primitives.PrivateKeyFromWif(node1.Settings.BlockAssembly.MinerWalletPrivateKeys[0])
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromPubKeyBytes(privKey.PubKey().Compressed(), 10000)
	require.NoError(t, err)

	require.NoError(t, tx.FillAllInputs(t.Context(), &unlocker.Getter{PrivateKey: privKey}))

	// Check that the transaction is signed
	assert.Greater(t, len(tx.Inputs[0].UnlockingScript.Bytes()), 0)

	err = node2.PropagationClient.ProcessTransaction(t.Context(), tx)
	require.NoError(t, err)

	_, err = getTx(t, fmt.Sprintf("http://localhost:%d", 28090), tx.TxIDChainHash().String())
	require.NoError(t, err)

	err = node1.PropagationClient.ProcessTransaction(t.Context(), tx)
	require.NoError(t, err)

	_, err = getTx(t, fmt.Sprintf("http://localhost:%d", 18090), tx.TxIDChainHash().String())
	require.NoError(t, err)

	_, err = helper.CallRPC(node1.RPCURL.String(), "generate", []any{1})
	require.NoError(t, err)

	tc.StopNode(t, "teranode1")
	tc.StartNode(t, "teranode1")

	// create a child tx
	childTx := bt.NewTx()

	err = childTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: tx.Outputs[0].LockingScript,
		Satoshis:      tx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = childTx.AddP2PKHOutputFromPubKeyBytes(privKey.PubKey().Compressed(), 1000)
	require.NoError(t, err)

	require.NoError(t, childTx.FillAllInputs(t.Context(), &unlocker.Getter{PrivateKey: privKey}))

	// Check that the transaction is signed
	assert.Greater(t, len(childTx.Inputs[0].UnlockingScript.Bytes()), 0)

	err = node1.PropagationClient.ProcessTransaction(t.Context(), childTx)
	require.NoError(t, err)

	err = node2.PropagationClient.ProcessTransaction(t.Context(), childTx)
	require.NoError(t, err)

	_, err = getTx(t, fmt.Sprintf("http://localhost:%d", 18090), childTx.TxIDChainHash().String())
	require.NoError(t, err)

	_, err = getTx(t, fmt.Sprintf("http://localhost:%d", 28090), childTx.TxIDChainHash().String())
	require.NoError(t, err)
}

func TestOrphanTxWithSingleNode(t *testing.T) {
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:     true,
		UTXOStoreType: "aerospike",
		SettingsOverrideFunc: testSettings.ComposeSettings(
			testSettings.SystemTestSettings(),
		),
	})
	// is stopped manually

	// Generate 2 blocks on node 1
	_, err := node1.CallRPC(node1.Ctx, "generate", []any{2})
	require.NoError(t, err)

	block1, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 1)
	require.NoError(t, err)

	coinbaseTxFromNode1, err := getTx(t, node1.AssetURL, block1.CoinbaseTx.TxID())
	require.NoError(t, err)

	// Create a transaction that spends the 0th output of block 1's coinbase parentTx
	parentTx := bt.NewTx()

	err = parentTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTxFromNode1.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTxFromNode1.Outputs[0].LockingScript,
		Satoshis:      coinbaseTxFromNode1.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = parentTx.AddP2PKHOutputFromPubKeyBytes(node1.GetPrivateKey(t).PubKey().Compressed(), 10000)
	require.NoError(t, err)

	require.NoError(t, parentTx.FillAllInputs(node1.Ctx, &unlocker.Getter{PrivateKey: node1.GetPrivateKey(t)}))

	// Check that the transaction is signed
	assert.Greater(t, len(parentTx.Inputs[0].UnlockingScript.Bytes()), 0)
	t.Logf("parentTx: %s", parentTx.TxID())

	childTx := node1.CreateTransaction(t, parentTx)
	t.Logf("childTx: %s", childTx.TxID())

	err = node1.PropagationClient.ProcessTransaction(node1.Ctx, parentTx)
	require.NoError(t, err)

	node1.Stop(t)
	node1.ResetServiceManagerContext(t)
	node1 = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		SkipRemoveDataDir: true, // we are re-starting so don't delete data dir
		SettingsOverrideFunc: testSettings.ComposeSettings(
			testSettings.SystemTestSettings(),
		),
	})

	defer node1.Stop(t)

	err = node1.PropagationClient.ProcessTransaction(node1.Ctx, childTx)
	require.NoError(t, err)

	block2, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 2)
	require.NoError(t, err)

	_, block3 := node1.CreateTestBlock(t, block2, 3, childTx)

	require.Error(t, node1.BlockValidationClient.ProcessBlock(node1.Ctx, block3, block3.Height, "", "legacy"))

	bestHeight, _, err := node1.BlockchainClient.GetBestHeightAndTime(node1.Ctx)
	require.NoError(t, err)

	assert.Equal(t, uint32(2), bestHeight)
}

/*
We need:
2 nodes
parent TX
childTx1
competing childTx2
To reproduce:
generate some blocks
send the parent to both, have it mined
send childTx1 to node 1 only
send childTx2 to node 2 only
generate a few blocks so delete at height is hit, confirm the parent has expired on both node 1 and node 2
mine a block on node 1, which would include childTx1
confirm node 2 is now in a broken state because it can't spend childTx1 because parentTx is gone
*/

func TestUnminedConflictResolution(t *testing.T) {
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		// EnableFullLogging: true,
		SettingsOverrideFunc: func(settings *settings.Settings) {
			// settings.Asset.HTTPPort = 18090
			settings.Validator.UseLocalValidator = true
			settings.GlobalBlockHeightRetention = 1
			settings.BlockValidation.OptimisticMining = false
		},
	})

	defer node1.Stop(t)

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		// EnableFullLogging: true,
		SettingsOverrideFunc: func(settings *settings.Settings) {
			// settings.Asset.HTTPPort = 28090
			settings.Validator.UseLocalValidator = true
			settings.GlobalBlockHeightRetention = 1
			settings.BlockValidation.OptimisticMining = false
		},
	})

	defer node2.Stop(t)

	// Generate 2 blocks on node 1
	_, err := node1.CallRPC(node1.Ctx, "generate", []any{2})
	require.NoError(t, err)

	block2, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 2)
	require.NoError(t, err)

	// Node 2 should have received notifications for these 2 blocks over P2P.
	// Wait for block assembler to catch up on node 2
	node2.WaitForBlockHeight(t, block2, 10*time.Second, true)

	// Get block 1's coinbase tx from node 1 and create a transaction that spends the 0th output of block 1's coinbase
	block1, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 1)
	require.NoError(t, err)

	coinbaseTxFromNode1, err := getTx(t, node1.AssetURL, block1.CoinbaseTx.TxID())
	require.NoError(t, err)

	// Create a transaction that spends the 0th output of block 1's coinbase parentTx
	// parentTx := node1.CreateTransaction(t, coinbaseTxFromNode1)
	parentTx := bt.NewTx()

	err = parentTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTxFromNode1.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTxFromNode1.Outputs[0].LockingScript,
		Satoshis:      coinbaseTxFromNode1.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = parentTx.AddP2PKHOutputFromPubKeyBytes(node1.GetPrivateKey(t).PubKey().Compressed(), 10000)
	require.NoError(t, err)

	require.NoError(t, parentTx.FillAllInputs(node1.Ctx, &unlocker.Getter{PrivateKey: node1.GetPrivateKey(t)}))

	// Check that the transaction is signed
	assert.Greater(t, len(parentTx.Inputs[0].UnlockingScript.Bytes()), 0)
	t.Logf("parentTx: %s", parentTx.TxID())

	err = node1.PropagationClient.ProcessTransaction(node1.Ctx, parentTx)
	require.NoError(t, err)

	err = node2.PropagationClient.ProcessTransaction(node2.Ctx, parentTx)
	require.NoError(t, err)

	// Now we've sent the tx to both nodes, let's retrieve them to check they have been stored
	txFromNode1, err := getTx(t, node1.AssetURL, parentTx.TxIDChainHash().String())
	require.NoError(t, err)

	txFromNode2, err := getTx(t, node2.AssetURL, parentTx.TxIDChainHash().String())
	require.NoError(t, err)

	assert.Equal(t, txFromNode1.String(), txFromNode2.String())

	// generate 1 block
	_, err = node1.CallRPC(node1.Ctx, "generate", []any{1})
	require.NoError(t, err)

	childTx1 := node1.CreateTransaction(t, parentTx)
	t.Logf("childTx1: %s", childTx1.TxID())

	childTx2 := node2.CreateTransaction(t, parentTx)
	t.Logf("childTx2: %s", childTx2.TxID())

	err = node1.PropagationClient.ProcessTransaction(node1.Ctx, childTx1)
	require.NoError(t, err)

	err = node2.PropagationClient.ProcessTransaction(node2.Ctx, childTx2)
	require.NoError(t, err)

	_, err = node1.UtxoStore.Get(node1.Ctx, parentTx.TxIDChainHash())
	require.NoError(t, err)

	_, err = node2.UtxoStore.Get(node2.Ctx, parentTx.TxIDChainHash())
	require.NoError(t, err)

	t.Logf("Restart node1")

	// Reset block assembly
	// err = node1.BlockAssemblyClient.ResetBlockAssembly(node1.Ctx)
	// require.NoError(t, err)

	err = node1.BlockAssemblyClient.RemoveTx(node1.Ctx, childTx1.TxIDChainHash())
	require.NoError(t, err)

	// restart node 1
	// node1.Stop(t)
	// node1.ResetServiceManagerContext(t)
	// node1 = daemon.NewTestDaemon(t, daemon.TestOptions{
	// 	EnableRPC:  true,
	// 	EnableP2P:  true,
	// 	SkipRemoveDataDir: true,
	// 	EnableFullLogging: true,
	// 	SettingsContext: "docker.host.teranode1.daemon",
	// 	SettingsOverrideFunc: func(settings *settings.Settings) {
	// 		settings.Asset.HTTPPort = 18090
	// 		settings.Validator.UseLocalValidator = true
	// 		settings.UtxoStore.BlockHeightRetention = 1
	// 	},
	// })

	// defer node1.Stop(t)

	t.Logf("Get childTx1 from node1")

	_, err = node1.UtxoStore.Get(node1.Ctx, childTx1.TxIDChainHash())
	require.NoError(t, err)

	t.Logf("Get childTx2 from node2")

	_, err = node2.UtxoStore.Get(node2.Ctx, childTx2.TxIDChainHash())
	require.NoError(t, err)

	t.Logf("Generate 2 blocks")

	// generate 2 blocks so that the parentTx expires on both nodes
	// _, err = node1.CallRPC(node1.Ctx, "generate", []any{2})
	// require.NoError(t, err)

	blocksToGenerate := node1.Settings.GlobalBlockHeightRetention
	node1.MineAndWait(t, blocksToGenerate+1)

	t.Logf("Get block 5 from node1")

	block5, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 5)
	require.NoError(t, err)

	t.Logf("Wait for block 5 on node2")
	node2.WaitForBlockHeight(t, block5, 10*time.Second, true)

	// Waiting for the cleanup to complete, a helper to wait for the cleanup to complete is needed
	time.Sleep(10 * time.Second)

	t.Logf("Get parentTx from node1")

	_, err = node1.UtxoStore.Get(node1.Ctx, parentTx.TxIDChainHash())
	require.Error(t, err)

	t.Logf("Get parentTx from node2")

	// _, err = node2.UtxoStore.Get(node2.Ctx, parentTx.TxIDChainHash())
	// require.Error(t, err)

	// generate 1 block on node 2
	_, err = node2.CallRPC(node2.Ctx, "generate", []any{1})
	require.NoError(t, err)

	time.Sleep(10 * time.Second)

	bestHeight, _, err := node1.BlockchainClient.GetBestHeightAndTime(node1.Ctx)
	require.NoError(t, err)

	assert.Equal(t, uint32(5), bestHeight) // TODO: this should be 6

	bestHeight, _, err = node2.BlockchainClient.GetBestHeightAndTime(node2.Ctx)
	require.NoError(t, err)

	assert.Equal(t, uint32(6), bestHeight)
}
