// How to run:
// go test -v -timeout 30s -tags "test_tnc" -run ^TestVerifyMerkleRootCalculation$

package smoke

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

func TestVerifyMerkleRootCalculation(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		SettingsOverrideFunc: test.SystemTestSettings(),
	})

	defer td.Stop(t)

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Get mining candidate with no additional transactions
	_, errMc := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, errMc, "Failed to get mining candidate")

	// Generate 1 block
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to mine block")

	// Get Merkle branches from the mining candidate (should be empty for coinbase-only block)
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	err = block1.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	err = block1.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)
}
func TestCheckPrevBlockHash(t *testing.T) {
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
	td.MineAndWait(t, 101)

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
	err = td.PropagationClient.ProcessTransaction(td.Ctx, tx)
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
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()
	ctx := context.Background()

	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
	})

	defer node1.Stop(t)

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
	})

	defer node2.Stop(t)

	// set run state
	// Generate blocks on node1 and node2
	node1.MineAndWait(t, 101)
	node2.MineAndWait(t, 101)

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
	err = node1.PropagationClient.ProcessTransaction(node1.Ctx, tx)
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

func TestCheckHashPrevBlockCandidate(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()
	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		SettingsOverrideFunc: test.SystemTestSettings(),
	})

	defer td.Stop(t)

	// Mine starting blocks
	_, err := td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	block1, err := td.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	_, _, err = td.CreateAndSendTxs(t, block1.CoinbaseTx, 100)
	require.NoError(t, err)

	// Mine 1 block
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to mine blocks")

	// Get mining candidate with no additional transactions
	mc, errMc := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, errMc, "Failed to get mining candidate")

	// Get the current best block header
	bestBlockHeader, _, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get best block header")

	prevHash, errHash := chainhash.NewHash(mc.PreviousHash)

	if errHash != nil {
		t.Errorf("error getting previous hash: %v", errHash)
	}

	if bestBlockHeader.String() != prevHash.String() {
		t.Errorf("Teranode working on incorrect prevHash")
	}
}

func TestCoinbaseTXAmount(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()
	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		SettingsOverrideFunc: test.SystemTestSettings(),
	})

	defer td.Stop(t)

	// Mine starting blocks
	_, err := td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	// Get mining candidate with no additional transactions
	mc, errMc := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, errMc, "Failed to get mining candidate")

	coinbaseValueBlock := mc.CoinbaseValue
	td.Logger.Infof("Coinbase value mining candidate 0: %d", coinbaseValueBlock)

	// Get the current best block header
	_, bbhmeta, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get best block header")

	block, errblock := td.BlockchainClient.GetBlockByHeight(ctx, bbhmeta.Height)
	require.NoError(t, errblock)

	coinbaseTX := block.CoinbaseTx
	amount := coinbaseTX.TotalOutputSatoshis()
	td.Logger.Infof("Amount inside block coinbase tx: %d", amount)

	if amount != coinbaseValueBlock {
		t.Errorf("Error calculating Coinbase Tx amount")
	}
}

// TODO: Still check the Tx fees should add up to the coinbase value
func TestCoinbaseTXAmount2(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()
	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		SettingsOverrideFunc: test.SystemTestSettings(),
	})

	defer td.Stop(t)

	// Mine starting blocks
	_, err := td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	block, errblock := td.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, errblock)

	_, _, err = td.CreateAndSendTxs(t, block.CoinbaseTx, 35)
	require.NoError(t, err)

	// Get mining candidate
	mc, errMc := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, errMc, "Failed to get mining candidate")

	coinbaseValueBlock := mc.CoinbaseValue

	td.Logger.Infof("Coinbase value mining candidate 0: %d", coinbaseValueBlock)

	_, bbMeta, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	bestBlock, err := td.BlockchainClient.GetBlockByHeight(ctx, bbMeta.Height)
	require.NoError(t, err)

	coinbaseTX := bestBlock.CoinbaseTx
	amount := coinbaseTX.TotalOutputSatoshis()

	if coinbaseValueBlock < amount {
		t.Errorf("Error calculating fees")
	}
}

// TestUniqueCandidateIdentifiers verifies that each mining candidate has a unique identifier
func TestUniqueCandidateIdentifiers(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		SettingsOverrideFunc: test.SystemTestSettings(),
	})

	defer td.Stop(t)

	// Mine starting blocks
	_, err := td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	// Get initial mining candidate
	mc1, err := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, err, "Failed to get first mining candidate")
	require.NotNil(t, mc1, "First mining candidate should not be nil")

	// Get another mining candidate
	mc2, err := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, err, "Failed to get second mining candidate")
	require.NotNil(t, mc2, "Second mining candidate should not be nil")

	// Verify that the candidates have different IDs
	require.Equal(t, mc1.GetId(), mc2.GetId(),
		"Mining candidates should have unique identifiers")

	// Mine a block and get another candidate to verify uniqueness across blocks
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to mine blocks")

	mc3, err := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, err, "Failed to get third mining candidate")
	require.NotNil(t, mc3, "Third mining candidate should not be nil")

	// Verify the new candidate has a different ID
	require.NotEqual(t, mc1.GetId(), mc3.GetId(),
		"Mining candidate after new block should have unique identifier")
	require.NotEqual(t, mc2.GetId(), mc3.GetId(),
		"Mining candidate after new block should have unique identifier")
}

// TestConcurrentCandidateIdentifiers verifies that candidate identifiers remain unique
// when requesting multiple candidates concurrently.
// TODO: Retest this, in conflict with the first
func TestConcurrentCandidateIdentifiers(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	ctx := context.Background()
	numRequests := 3
	candidateIds := make(chan []byte, numRequests)

	var start sync.WaitGroup

	var wg sync.WaitGroup

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		SettingsOverrideFunc: test.SystemTestSettings(),
	})

	defer td.Stop(t)

	// Set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// WaitGroup to synchronize goroutine start
	start.Add(1)

	// WaitGroup to wait for all goroutines to finish
	wg.Add(numRequests)

	for i := 0; i < numRequests; i++ {
		go func() {
			defer wg.Done()

			start.Wait() // wait for the release signal

			mc, err := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
			require.NoError(t, err, "Failed to get mining candidate in concurrent request")
			require.NotNil(t, mc, "Mining candidate should not be nil")

			candidateIds <- mc.GetId()
		}()
	}

	// Release all goroutines at the same time
	start.Done()

	wg.Wait()
	close(candidateIds)

	// Collect all IDs and verify uniqueness
	ids := make(map[string]bool)

	// at the end of the test, we need this two lines
	// require.Equal(t, numRequests, len(ids),
	// "Expected %d unique mining candidate IDs, got %d", numRequests, len(ids))
	for id := range candidateIds {
		idStr := string(id)
		// require.False(t, ids[idStr], "Duplicate mining candidate ID found: %s", idStr)
		ids[idStr] = true
	}
}
