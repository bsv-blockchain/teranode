//go:build test_nightly || test_docker_daemon || debug

package smoke

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/test/testcontainers"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrphanTx(t *testing.T) {
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:  true,
		EnableP2P:  true,
		UseTracing: false,
		// EnableFullLogging: true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 18090
			settings.Validator.UseLocalValidator = true
		},
	})

	defer node1.Stop(t)

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableP2P:         true,
		SkipRemoveDataDir: true,
		UseTracing:        false,
		// EnableFullLogging: true,
		SettingsContext: "docker.host.teranode2.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 28090
			settings.Validator.UseLocalValidator = true
		},
	})

	defer node2.Stop(t)

	// Generate 101 blocks on node 1
	_, err := node1.CallRPC("generate", []any{2})
	require.NoError(t, err)

	block2, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 2)
	require.NoError(t, err)

	// Wait for block assembler to catch up on node 1
	node1.WaitForBlockHeight(t, block2, 1*time.Second)

	// Node 2 should have received notifications for these 101 blocks over P2P.
	// Wait for block assembler to catch up on node 2
	node2.WaitForBlockHeight(t, block2, 10*time.Second, true)

	// Get block 1 from both nodes and check they are the same
	block1, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 1)
	require.NoError(t, err)

	block1b, err := node2.BlockchainClient.GetBlockByHeight(node2.Ctx, 1)
	require.NoError(t, err)

	assert.Equal(t, block1.Hash().String(), block1b.Hash().String())

	// Get block 1's coinbase tx from both nodes and check they are the same
	coinbaseTxFromNode1, err := getTx(t, node1.AssetURL, block1.CoinbaseTx.TxID())
	require.NoError(t, err)

	coinbaseTxFromNode2, err := getTx(t, node2.AssetURL, block1.CoinbaseTx.TxID())
	require.NoError(t, err)

	assert.Equal(t, coinbaseTxFromNode1.String(), coinbaseTxFromNode2.String())

	// Create a transaction that spends the 0th output of block 1's coinbase parentTx
	parentTx := bt.NewTx()

	err = parentTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTxFromNode1.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTxFromNode1.Outputs[0].LockingScript,
		Satoshis:      coinbaseTxFromNode1.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = parentTx.AddP2PKHOutputFromPubKeyBytes(node1.GetPrivateKey(t).PubKey().SerialiseCompressed(), 10000)
	require.NoError(t, err)

	require.NoError(t, parentTx.FillAllInputs(node1.Ctx, &unlocker.Getter{PrivateKey: node1.GetPrivateKey(t)}))

	// Check that the transaction is signed
	assert.Greater(t, len(parentTx.Inputs[0].UnlockingScript.Bytes()), 0)

	err = node1.PropagationClient.ProcessTransaction(node1.Ctx, parentTx)
	require.NoError(t, err)

	err = node2.PropagationClient.ProcessTransaction(node2.Ctx, parentTx)
	require.NoError(t, err)

	// Now we've sent the tx to both nodes, let's retrieve them to check they have been stored
	txFromNode1, err := getTx(t, node1.AssetURL, parentTx.TxIDChainHash().String())
	require.NoError(t, err)

	node2.VerifyInBlockAssembly(t, parentTx)

	txFromNode2, err := getTx(t, node2.AssetURL, parentTx.TxIDChainHash().String())
	require.NoError(t, err)

	assert.Equal(t, txFromNode1.String(), txFromNode2.String())

	//restart node 1
	node1.Stop(t)
	node1.ResetServiceManagerContext(t)
	node1 = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:  true,
		EnableP2P:  true,
		UseTracing: false,
		SkipRemoveDataDir: true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 18090
			settings.Validator.UseLocalValidator = true
		},
	})

	defer node1.Stop(t)

	node1.WaitForBlockHeight(t, block2, 1*time.Second)

	childtx := bt.NewTx()

	err = childtx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      parentTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = childtx.AddP2PKHOutputFromPubKeyBytes(node1.GetPrivateKey(t).PubKey().SerialiseCompressed(), 1000)
	require.NoError(t, err)

	require.NoError(t, childtx.FillAllInputs(node1.Ctx, &unlocker.Getter{PrivateKey: node1.GetPrivateKey(t)}))

	// Check that the transaction is signed
	assert.Greater(t, len(childtx.Inputs[0].UnlockingScript.Bytes()), 0)

	err = node1.PropagationClient.ProcessTransaction(node1.Ctx, childtx)
	require.NoError(t, err)

	err = node2.PropagationClient.ProcessTransaction(node2.Ctx, childtx)
	require.NoError(t, err)

	// send a child tx to both
	childtxFromNode1, err := getTx(t, node1.AssetURL, childtx.TxIDChainHash().String())
	require.NoError(t, err)

	node1.VerifyInBlockAssembly(t, childtx)
	node1.VerifyNotInBlockAssembly(t, parentTx)

	childtxFromNode2, err := getTx(t, node2.AssetURL, childtx.TxIDChainHash().String())
	require.NoError(t, err)

	node2.VerifyInBlockAssembly(t, childtx)
	node2.VerifyInBlockAssembly(t, parentTx)

	assert.Equal(t, childtxFromNode1.String(), childtxFromNode2.String())

	_, err = node1.CallRPC("generate", []any{1})
	require.NoError(t, err)	

	utxo, err := node1.UtxoStore.Get(node1.Ctx, childtx.TxIDChainHash())
	require.NoError(t, err)

	assert.Equal(t, utxo.BlockHeights[0], uint32(3))

	utxo, err = node1.UtxoStore.Get(node1.Ctx, parentTx.TxIDChainHash())
	require.NoError(t, err)

	//nolint:unconvert
	assert.Equal(t, utxo.BlockHeights, []uint32([]uint32(nil)))

	block3, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 3)
	require.NoError(t, err)

	err = block3.GetAndValidateSubtrees(node1.Ctx, node1.Logger, node1.SubtreeStore, nil)
	require.NoError(t, err)

	err = block3.CheckMerkleRoot(node1.Ctx)
	require.NoError(t, err)
	
	fallbackGetFunc := func(subtreeHash chainhash.Hash) error {
		return block3.SubTreesFromBytes(subtreeHash[:])
	}

	subtree, err := block3.GetSubtrees(node1.Ctx, node1.Logger, node1.SubtreeStore, fallbackGetFunc)
	require.NoError(t, err)

	blFound := false
	for i := 0; i < len(subtree); i++ {
		st := subtree[i]
		for _, node := range st.Nodes {
			t.Logf("node.Hash: %s", node.Hash.String())
			t.Logf("tx.TxIDChainHash().String(): %s", childtx.TxIDChainHash().String())

			if node.Hash.String() == parentTx.TxIDChainHash().String() {
				blFound = true
				break
			}
		}
	}
	
	require.False(t, blFound)

	// get the block height on node2
	blockHeightNode2, _, err := node2.BlockchainClient.GetBestHeightAndTime(node2.Ctx)
	require.NoError(t, err)

	assert.Equal(t, blockHeightNode2, uint32(3))

	utxo, err = node2.UtxoStore.Get(node2.Ctx, childtx.TxIDChainHash())
	require.NoError(t, err)

	assert.Equal(t, utxo.BlockHeights[0], uint32(3))

	utxo, err = node2.UtxoStore.Get(node2.Ctx, parentTx.TxIDChainHash())
	require.NoError(t, err)

	//nolint:unconvert
	assert.Equal(t, utxo.BlockHeights, []uint32([]uint32(nil)))
}

func getTx(t *testing.T, assetURL string, txID string) (*bt.Tx, error) {
	resp, err := http.Get(fmt.Sprintf("%s/api/v1/tx/%s", assetURL, txID))
	require.NoError(t, err)

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.NewProcessingError("expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	var tx bt.Tx

	_, err = tx.ReadFrom(resp.Body)
	require.NoError(t, err)

	return &tx, nil
}

func TestInvalidBlockWithContainer(t *testing.T) {
	err := os.RemoveAll("../../data")
	require.NoError(t, err)

	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		ComposeFile: "../../docker-compose-host.yml",
	})
	require.NoError(t, err)

	node1 := tc.GetNodeClients(t, "docker.host.teranode1")
	node2 := tc.GetNodeClients(t, "docker.host.teranode2")

	_, err = helper.CallRPC(node1.RPCURL.String(), "generate", []any{110})
	require.NoError(t, err, "Failed to generate blocks")

	// tc.StopNode(t, "teranode-1")
	// tc.StartNode(t, "teranode-1")
	
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 101, 20*time.Second)
	require.NoError(t, err)

	err = helper.WaitForNodeBlockHeight(t.Context(), node2.BlockchainClient, 101, 20*time.Second)
	require.NoError(t, err)

	block1, err := node1.BlockchainClient.GetBlockByHeight(t.Context(), 1)
	require.NoError(t, err)

	coinbaseTxFromNode1 := block1.CoinbaseTx

	tx := bt.NewTx()

	err = tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTxFromNode1.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTxFromNode1.Outputs[0].LockingScript,
		Satoshis:      coinbaseTxFromNode1.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	privKey, err := wif.DecodeWIF(node1.Settings.BlockAssembly.MinerWalletPrivateKeys[0])
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromPubKeyBytes(privKey.PrivKey.PubKey().SerialiseCompressed(), 10000)
	require.NoError(t, err)

	require.NoError(t, tx.FillAllInputs(t.Context(), &unlocker.Getter{PrivateKey: privKey.PrivKey}))

	// Check that the transaction is signed
	assert.Greater(t, len(tx.Inputs[0].UnlockingScript.Bytes()), 0)

	err = node1.PropagationClient.ProcessTransaction(t.Context(), tx)
	require.NoError(t, err)

	err = node2.PropagationClient.ProcessTransaction(t.Context(), tx)
	require.NoError(t, err)

	_, err = getTx(t, fmt.Sprintf("http://localhost:%d", 18090), tx.TxIDChainHash().String())
	require.NoError(t, err)

	_, err = getTx(t, fmt.Sprintf("http://localhost:%d", 28090), tx.TxIDChainHash().String())
	require.NoError(t, err)

	time.Sleep(10 * time.Second)

	_, err = helper.CallRPC(node1.RPCURL.String(), "generate", []any{1})
	require.NoError(t, err)

	tc.StopNode(t, "teranode-1")
	tc.StartNode(t, "teranode-1")

	// create a child tx
	childTx := bt.NewTx()

	err = childTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: tx.Outputs[0].LockingScript,
		Satoshis:      tx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = childTx.AddP2PKHOutputFromPubKeyBytes(privKey.PrivKey.PubKey().SerialiseCompressed(), 1000)
	require.NoError(t, err)

	require.NoError(t, childTx.FillAllInputs(t.Context(), &unlocker.Getter{PrivateKey: privKey.PrivKey}))	

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