// How to run:
// go test -v -timeout 30s -run ^TestCheckPrevBlockHash$
// go test -v -timeout 30s -run ^TestPrevBlockHashAfterReorg$

package smoke

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/stretchr/testify/require"
)

func TestCheckPrevBlockHash(t *testing.T) {
	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "docker.host.teranode1.daemon",
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

	// Generate priv and pub key
	inputPrivKey := td.GetPrivateKey(t)

	outPrivKey, err := bec.NewPrivateKey()
	require.NoError(t, err)

	outPubKey := outPrivKey.PubKey()

	parenTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 1)
	require.NoError(t, err)

	tx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parenTx, 0, inputPrivKey),
		transactions.WithP2PKHOutputs(1, 10000, outPubKey),
	)

	// Send transaction
	_, err = td.DistributorClient.SendTransaction(td.Ctx, tx)
	require.NoError(t, err)

	// Mine the block with the new transaction
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to mine block")

	// Get the current best block header
	bestBlockHeader, _, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get best block header")

	// Get mining candidate with no additional transactions
	mc, errMc := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, errMc, "Failed to get mining candidate")

	// Convert the previous hash from the mining candidate to a chainhash.Hash
	prevHash, err := chainhash.NewHash(mc.PreviousHash)
	require.NoError(t, err, "Failed to create hash from previous hash bytes")

	// Verify that the mining candidate's previous hash matches the current best block
	require.Equal(t, bestBlockHeader.String(), prevHash.String(),
		"Mining candidate's previous hash does not match current best block")
}

func TestPrevBlockHashAfterReorg(t *testing.T) {
	ctx := context.Background()

	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "docker.host.teranode1.daemon",
	})

	defer node1.Stop(t)

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "docker.host.teranode2.daemon",
	})

	defer node2.Stop(t)

	// set run state
	// Generate blocks on node1 and node2
	_, err := node1.CallRPC(node1.Ctx, "generate", []any{101})
	require.NoError(t, err)

	_, err = node2.CallRPC(node2.Ctx, "generate", []any{101})
	require.NoError(t, err)

	// get block height
	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	// Generate priv and pub key for sending tx

	inputPrivKey1 := node1.GetPrivateKey(t)

	outPrivKey1, err := bec.NewPrivateKey()
	require.NoError(t, err)

	outPubKey1 := outPrivKey1.PubKey()

	parenTx, err := node1.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 1)
	require.NoError(t, err)

	tx := node1.CreateTransactionWithOptions(t,
		transactions.WithInput(parenTx, 0, inputPrivKey1),
		transactions.WithP2PKHOutputs(1, 10000, outPubKey1),
	)

	// Send transaction
	_, err = node1.DistributorClient.SendTransaction(node1.Ctx, tx)
	require.NoError(t, err, "Failed to send transactions")

	// Generate blocks on node1 and node2
	_, err = node1.CallRPC(node1.Ctx, "generate", []any{1})
	require.NoError(t, err)

	_, err = node2.CallRPC(node2.Ctx, "generate", []any{5})
	require.NoError(t, err)

	// Get the current best block header from node1 after reorg
	bestBlockHeader, _, err := node1.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get best block header after reorg")

	// Get a new mining candidate from node1
	mc, err := node1.BlockAssemblyClient.GetMiningCandidate(ctx)
	require.NoError(t, err, "Failed to get mining candidate after reorg")

	// Convert the previous hash from the mining candidate to a chainhash.Hash
	prevHash, err := chainhash.NewHash(mc.PreviousHash)
	require.NoError(t, err, "Failed to create hash from previous hash bytes")

	// Verify that node0's mining candidate now references the tip of the longer chain
	require.Equal(t, bestBlockHeader.String(), prevHash.String(),
		"Mining candidate's previous hash does not match the new best block after reorg")
}
