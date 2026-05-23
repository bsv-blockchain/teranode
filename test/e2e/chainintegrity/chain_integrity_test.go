package chainintegrity

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

const (
	// Test configuration
	coinbaseMaturity = 5  // Blocks needed before coinbase can be spent
	targetBlocks     = 10 // Total blocks to generate during test (must be > coinbaseMaturity+1)
	txsPerBlock      = 10 // Transactions to generate before mining a block
	numWorkers       = 2  // Transaction workers per node
)

// TestChainIntegrity3Nodes tests chain integrity across 3 nodes with transaction load
// This test replaces the docker-compose-3blasters.yml approach with a pure Go implementation
func TestChainIntegrity3Nodes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chain integrity test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	t.Log("=== Starting Chain Integrity Test with 3 Nodes ===")

	// Phase 1: Start node 0 and mine initial blocks
	t.Log("Phase 1: Starting node 0 and mining initial blocks...")
	node0 := createNode(t, ctx, 1) // nodeNumber 1 = teranode1
	defer node0.Stop(t)

	// Mine to coinbase maturity on node 0
	mineToMaturity(t, node0)
	t.Logf("Node 0 mined to height %d", coinbaseMaturity+1)

	// Phase 2: Start transaction blasting on node 0
	t.Log("Phase 2: Starting transaction blaster on node 0...")
	blasters := startBlasters(t, ctx, []*daemon.TestDaemon{node0})

	// Phase 3: Generate some blocks with transactions on node 0
	t.Log("Phase 3: Generating blocks with transactions on node 0...")
	generateBlocksWithTransactions(t, ctx, []*daemon.TestDaemon{node0}, blasters, targetBlocks)

	// Stop blasters before starting other nodes
	stopBlasters(blasters)
	t.Logf("Node 0 now at height %d with transactions", targetBlocks)

	// Phase 4: Start nodes 1 and 2, connect them to node 0
	t.Log("Phase 4: Starting nodes 1 and 2 and connecting to node 0...")
	node1 := createNode(t, ctx, 2) // nodeNumber 2 = teranode2
	defer node1.Stop(t)
	node2 := createNode(t, ctx, 3) // nodeNumber 3 = teranode3
	defer node2.Stop(t)

	// Connect nodes to node 0
	node1.InjectPeer(t, node0)
	t.Log("Node 1 connected to Node 0")
	node2.InjectPeer(t, node0)
	t.Log("Node 2 connected to Node 0")

	// Get node 0's best block hash for sync target
	node0BestHeader, _, err := node0.BlockchainClient.GetBestBlockHeader(node0.Ctx)
	require.NoError(t, err)
	t.Logf("Node 0 best block hash: %s", node0BestHeader.Hash().String())

	// Phase 5: Wait for all nodes to sync to node 0's best block
	t.Log("Phase 5: Waiting for all nodes to sync to node 0's best block...")
	node1.WaitForBlockhash(t, node0BestHeader.Hash(), 3*time.Minute)
	t.Log("Node 1 synced")
	node2.WaitForBlockhash(t, node0BestHeader.Hash(), 3*time.Minute)
	t.Log("Node 2 synced")

	nodes := []*daemon.TestDaemon{node0, node1, node2}

	// Phase 6: Verify chain integrity
	t.Log("Phase 6: Verifying chain integrity across all nodes...")
	verifyChainIntegrity(t, nodes)

	t.Log("=== Chain Integrity Test Completed Successfully ===")
}

// createNode creates a single TestDaemon instance with the given node number
func createNode(t *testing.T, _ context.Context, nodeNumber int) *daemon.TestDaemon {
	t.Logf("Creating node %d...", nodeNumber)

	// Use Aerospike for production-like testing
	storeType := "aerospike"

	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		EnableP2P:            true,
		EnableValidator:      true,
		EnableBlockPersister: true, // Enable block persister for integrity verification
		UTXOStoreType:        storeType,
		SkipRemoveDataDir:    nodeNumber > 1, // Only first node cleans the shared parent data dir; subsequent nodes share it
		EnableDebugLogging:   true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			test.MultiNodeSettings(nodeNumber),
			func(s *settings.Settings) {
				// Additional overrides specific to this test
				s.P2P.PeerCacheDir = t.TempDir()
				s.ChainCfgParams.CoinbaseMaturity = coinbaseMaturity
				s.P2P.SyncCoordinatorPeriodicEvaluationInterval = 1 * time.Second
				s.GlobalBlockHeightRetention = 100 // Keep blocks longer for integrity verification
			},
		),
		FSMState: blockchain.FSMStateRUNNING,
	})

	t.Logf("Node %d created successfully (ClientName: %s, P2P Port: %d, DataFolder: %s, HealthCheck: %s)",
		nodeNumber, node.Settings.ClientName, node.Settings.P2P.Port,
		node.Settings.DataFolder, node.Settings.HealthCheckHTTPListenAddress)

	return node
}

// mineToMaturity mines blocks until coinbase maturity is reached
func mineToMaturity(t *testing.T, node *daemon.TestDaemon) {
	node.MineAndWait(t, coinbaseMaturity+1)
}

// startBlasters creates and starts transaction blasters for node 0 only (the miner)
// Node 0 has the coinbase transactions that can be spent
func startBlasters(t *testing.T, ctx context.Context, nodes []*daemon.TestDaemon) []*Blaster {
	// Only node 0 can blast transactions since it has the coinbase private key
	node := nodes[0]

	// Get the spendable coinbase from block 1 (mined to maturity already)
	block1, err := node.BlockchainClient.GetBlockByHeight(node.Ctx, 1)
	require.NoError(t, err)
	coinbaseTx := block1.CoinbaseTx

	t.Logf("Starting blaster for node 1 with coinbase: %s", coinbaseTx.TxIDChainHash().String())

	blaster := NewBlaster(t, node, coinbaseTx, numWorkers, txsPerBlock)
	blaster.Start(ctx)

	return []*Blaster{blaster}
}

// stopBlasters stops all transaction blasters
func stopBlasters(blasters []*Blaster) {
	for _, blaster := range blasters {
		if blaster != nil {
			blaster.Stop()
		}
	}
}

// generateBlocksWithTransactions generates blocks while transactions are being created
func generateBlocksWithTransactions(t *testing.T, ctx context.Context, nodes []*daemon.TestDaemon, blasters []*Blaster, targetHeight uint32) {
	// Get current height
	currentHeight, _, err := nodes[0].BlockchainClient.GetBestHeightAndTime(nodes[0].Ctx)
	require.NoError(t, err)

	blocksToMine := int(targetHeight - currentHeight)
	t.Logf("Mining %d blocks from height %d to %d", blocksToMine, currentHeight, targetHeight)

	// Mine blocks periodically while blasters are running
	for i := 0; i < blocksToMine; i++ {
		// Mine a block on node 0
		nodes[0].MineAndWait(t, 1)

		var txCount uint64
		for _, blaster := range blasters {
			txCount += blaster.GetTotalTxCount()
		}
		t.Logf("Progress: %d/%d blocks mined, %d total transactions created", i+1, blocksToMine, txCount)
	}

	// Final stats
	var totalTxs uint64
	for _, blaster := range blasters {
		totalTxs += blaster.GetTotalTxCount()
	}
	t.Logf("Block generation complete: %d blocks mined, %d total transactions", blocksToMine, totalTxs)
}

// blockSubtree holds information about a block and its subtree for tracking transaction locations
type blockSubtree struct {
	Block   chainhash.Hash
	Subtree chainhash.Hash
	Index   int
}

// verifyChainIntegrity verifies that all nodes have identical blockchain state
// This is the comprehensive verification matching the original compose/chainintegrity implementation
func verifyChainIntegrity(t *testing.T, nodes []*daemon.TestDaemon) {
	t.Log("Fetching block headers from all nodes...")

	// Get best block from each node
	type nodeChainInfo struct {
		headers []*model.BlockHeader
		metas   []*model.BlockHeaderMeta
	}

	allChains := make([]nodeChainInfo, len(nodes))

	for i, node := range nodes {
		// Get best block
		bestHeader, bestMeta, err := node.BlockchainClient.GetBestBlockHeader(node.Ctx)
		require.NoError(t, err)

		// Get all headers from genesis
		headers, metas, err := node.BlockchainClient.GetBlockHeaders(node.Ctx, bestHeader.Hash(), 100000)
		require.NoError(t, err)

		t.Logf("Node %d: height=%d, %d block headers retrieved", i+1, bestMeta.Height, len(headers))

		allChains[i] = nodeChainInfo{
			headers: headers,
			metas:   metas,
		}
	}

	// Verify all nodes have the same number of blocks
	require.Equal(t, len(allChains[0].headers), len(allChains[1].headers), "Node 1 and Node 2 have different block counts")
	require.Equal(t, len(allChains[0].headers), len(allChains[2].headers), "Node 1 and Node 3 have different block counts")

	// Verify block hashes match across all nodes
	t.Log("Verifying block hashes match across all nodes...")
	mismatches := 0
	for i := 0; i < len(allChains[0].headers); i++ {
		hash1 := allChains[0].headers[i].Hash()
		hash2 := allChains[1].headers[i].Hash()
		hash3 := allChains[2].headers[i].Hash()

		if !hash1.IsEqual(hash2) || !hash1.IsEqual(hash3) {
			t.Errorf("Block %d hash mismatch: node1=%s, node2=%s, node3=%s",
				i, hash1.String(), hash2.String(), hash3.String())
			mismatches++
		}
	}

	require.Equal(t, 0, mismatches, "Found %d block hash mismatches", mismatches)
	t.Logf("✓ Block hash consensus verified: All %d blocks match across 3 nodes", len(allChains[0].headers))

	// Verify block header chain linkage (same for all nodes since hashes match)
	t.Log("Verifying block header chain linkage...")
	verifyBlockHeaderChain(t, allChains[0].headers)

	// Deep integrity verification on ALL nodes
	for i, node := range nodes {
		t.Logf("Performing deep chain integrity verification on node %d...", i+1)
		verifyNodeIntegrity(t, node, allChains[i].headers, allChains[i].metas)
		t.Logf("✓ Node %d integrity verified", i+1)
	}

	// Block persister verification on ALL nodes
	t.Log("Verifying block persister files on all nodes...")
	for i, node := range nodes {
		t.Logf("Verifying block persister files on node %d...", i+1)
		verifyBlockPersisterFiles(t, node, allChains[i].headers, allChains[i].metas)
		t.Logf("✓ Node %d block persister files verified", i+1)
	}

	t.Log("=== All chain integrity checks passed ===")
}

// verifyBlockHeaderChain verifies the block header chain is properly linked
func verifyBlockHeaderChain(t *testing.T, headers []*model.BlockHeader) {
	var previousBlockHeader *model.BlockHeader

	for _, blockHeader := range headers {
		if previousBlockHeader != nil {
			if !previousBlockHeader.HashPrevBlock.IsEqual(blockHeader.Hash()) {
				t.Errorf("Block header chain broken: block %s does not link to previous block %s (expected %s)",
					blockHeader.Hash(), previousBlockHeader.Hash(), previousBlockHeader.HashPrevBlock)
			}
		}
		previousBlockHeader = blockHeader
	}
	t.Log("✓ Block header chain linkage verified")
}

// verifyNodeIntegrity performs comprehensive integrity checks on a single node
// This matches the original checkNodeIntegrity function from compose/chainintegrity
//
//nolint:gocognit // cognitive complexity matches original implementation
func verifyNodeIntegrity(t *testing.T, node *daemon.TestDaemon, blockHeaders []*model.BlockHeader, blockMetas []*model.BlockHeaderMeta) {
	ctx := node.Ctx

	transactionMap := make(map[chainhash.Hash]blockSubtree)
	missingParents := make(map[chainhash.Hash]blockSubtree)

	t.Logf("Checking %d blocks for integrity...", len(blockHeaders))

	// Genesis script to identify genesis block coinbase
	genesisScript := "04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e" +
		"206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73"

	// Range through block headers in reverse order (oldest first)
	for i := len(blockHeaders) - 1; i >= 0; i-- {
		blockHeader := blockHeaders[i]
		height := blockMetas[i].Height
		blockFees := uint64(0)

		block, err := node.BlockchainClient.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			t.Errorf("Failed to get block %s: %v", blockHeader.Hash(), err)
			continue
		}

		// Verify coinbase transaction exists and is valid
		if block.CoinbaseTx == nil || !block.CoinbaseTx.IsCoinbase() {
			t.Errorf("Block %s does not have a valid coinbase transaction", block.Hash())
			continue
		}

		// Skip genesis block coinbase for UTXO checks
		if block.CoinbaseTx.Inputs[0].UnlockingScript.String() != genesisScript {
			// Verify coinbase UTXOs exist in UTXO store
			verifyCoinbaseUTXOs(t, ctx, node, block)

			// Verify coinbase height matches block height
			coinbaseHeight, err := util.ExtractCoinbaseHeight(block.CoinbaseTx)
			if err != nil {
				t.Errorf("Failed to extract coinbase height from block %s: %v", block.Hash(), err)
			} else if coinbaseHeight != height {
				t.Errorf("Coinbase height %d does not match block height %d", coinbaseHeight, height)
			}

			// Add coinbase to transaction map
			transactionMap[*block.CoinbaseTx.TxIDChainHash()] = blockSubtree{Block: *block.Hash()}

			// Verify subtrees
			for _, subtreeHash := range block.Subtrees {
				subtreeFees := verifySubtree(t, ctx, node, block, subtreeHash, transactionMap, missingParents)
				blockFees += subtreeFees
			}
		}

		// Verify block reward (fees + subsidy = coinbase outputs)
		blockReward := block.CoinbaseTx.TotalOutputSatoshis()
		blockSubsidy := util.GetBlockSubsidyForHeight(height, node.Settings.ChainCfgParams)

		if blockFees+blockSubsidy != blockReward {
			t.Errorf("Block %s has incorrect reward: fees(%d) + subsidy(%d) = %d, but coinbase outputs = %d",
				block.Hash(), blockFees, blockSubsidy, blockFees+blockSubsidy, blockReward)
		}
	}

	if len(missingParents) > 0 {
		t.Errorf("Found %d missing parent transactions (topological order violations)", len(missingParents))
	}

	t.Logf("✓ Node integrity verified: checked %d blocks, %d unique transactions", len(blockHeaders), len(transactionMap))
}

// verifyCoinbaseUTXOs verifies all coinbase outputs exist in the UTXO store
func verifyCoinbaseUTXOs(t *testing.T, ctx context.Context, node *daemon.TestDaemon, block *model.Block) {
	for vout, output := range block.CoinbaseTx.Outputs {
		utxoHash, err := util.UTXOHashFromOutput(block.CoinbaseTx.TxIDChainHash(), output, uint32(vout))
		if err != nil {
			t.Errorf("Failed to get UTXO hash for coinbase output %d in %s: %v", vout, block.CoinbaseTx.TxIDChainHash(), err)
			continue
		}

		utxo, err := node.UtxoStore.GetSpend(ctx, &utxostore.Spend{
			TxID:     block.CoinbaseTx.TxIDChainHash(),
			Vout:     uint32(vout),
			UTXOHash: utxoHash,
		})
		if err != nil {
			t.Errorf("Failed to get coinbase UTXO %s from store: %v", utxoHash, err)
			continue
		}

		if utxo == nil {
			t.Errorf("Coinbase UTXO %s does not exist in UTXO store", utxoHash)
		}
	}
}

// verifySubtree verifies a subtree and returns the total fees
//
//nolint:gocognit // cognitive complexity matches original implementation
func verifySubtree(t *testing.T, ctx context.Context, node *daemon.TestDaemon, block *model.Block,
	subtreeHash *chainhash.Hash, transactionMap map[chainhash.Hash]blockSubtree,
	missingParents map[chainhash.Hash]blockSubtree) uint64 {

	subtreeReader, err := node.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		t.Errorf("Failed to get subtree %s for block %s: %v", subtreeHash, block.Hash(), err)
		return 0
	}
	defer closeReader(subtreeReader)

	subtree, err := subtreepkg.NewSubtreeFromReader(subtreeReader)
	if err != nil || subtree == nil {
		t.Errorf("Failed to parse subtree %s for block %s: %v", subtreeHash, block.Hash(), err)
		return 0
	}

	subtreeFees := uint64(0)

	for nodeIdx, subtreeNode := range subtree.Nodes {
		// Skip coinbase placeholder
		if subtreepkg.CoinbasePlaceholderHash.Equal(subtreeNode.Hash) {
			continue
		}

		// Check for duplicate transactions
		if previousBlock, ok := transactionMap[subtreeNode.Hash]; ok {
			t.Errorf("Transaction %s already exists in subtree %s in block %s",
				subtreeNode.Hash, previousBlock.Subtree, previousBlock.Block)
		} else {
			transactionMap[subtreeNode.Hash] = blockSubtree{
				Block:   *block.Hash(),
				Subtree: *subtreeHash,
				Index:   nodeIdx,
			}
		}

		// Get transaction from UTXO store
		txMeta, err := node.UtxoStore.Get(ctx, &subtreeNode.Hash, fields.Tx, fields.BlockIDs)
		if err != nil {
			t.Errorf("Failed to get transaction %s from UTXO store: %v", subtreeNode.Hash, err)
			continue
		}

		if txMeta == nil || txMeta.Tx == nil {
			t.Errorf("Transaction %s not found in UTXO store", subtreeNode.Hash)
			continue
		}

		btTx := txMeta.Tx

		// Verify topological order and parent UTXO spending
		verifyTransactionInputs(t, ctx, node, btTx, block, subtreeHash, nodeIdx, transactionMap, missingParents)

		// Verify all outputs exist in UTXO store
		verifyTransactionOutputs(t, ctx, node, btTx)

		// Calculate fees (skip coinbase)
		if !btTx.IsCoinbase() {
			fees, err := util.GetFees(btTx)
			if err != nil {
				t.Errorf("Failed to calculate fees for transaction %s: %v", btTx.TxIDChainHash(), err)
				continue
			}
			subtreeFees += fees
		}

		// Check if this was a missing parent
		if childBlock, ok := missingParents[subtreeNode.Hash]; ok {
			t.Logf("Found previously missing parent %s in block %s, subtree %s:%d (child was in block %s, subtree %s:%d)",
				subtreeNode.Hash, block.Hash(), subtreeHash, nodeIdx, childBlock.Block, childBlock.Subtree, childBlock.Index)
		}
	}

	// Verify subtree fees match
	if subtreeFees != subtree.Fees {
		t.Errorf("Subtree %s has incorrect fees: calculated %d, recorded %d", subtreeHash, subtreeFees, subtree.Fees)
	}

	return subtreeFees
}

// verifyTransactionInputs verifies the topological order and parent UTXO spending
func verifyTransactionInputs(t *testing.T, ctx context.Context, node *daemon.TestDaemon, btTx *bt.Tx,
	block *model.Block, subtreeHash *chainhash.Hash, nodeIdx int,
	transactionMap map[chainhash.Hash]blockSubtree, missingParents map[chainhash.Hash]blockSubtree) {

	for _, input := range btTx.Inputs {
		inputHash := chainhash.Hash(input.PreviousTxID())

		// Skip zero hash (genesis coinbase)
		if inputHash.Equal(chainhash.Hash{}) {
			continue
		}

		// Check topological order: parent should already be in transaction map
		if _, ok := transactionMap[inputHash]; !ok {
			missingParents[inputHash] = blockSubtree{
				Block:   *block.Hash(),
				Subtree: *subtreeHash,
				Index:   nodeIdx,
			}
			t.Errorf("Parent %s does not appear before transaction %s in block %s, subtree %s:%d",
				inputHash, btTx.TxIDChainHash(), block.Hash(), subtreeHash, nodeIdx)
			continue
		}

		// Verify parent UTXO is marked as spent by this transaction
		utxoHash, err := util.UTXOHashFromInput(input)
		if err != nil {
			t.Errorf("Failed to get UTXO hash for input %s in transaction %s: %v",
				input.PreviousTxIDChainHash(), btTx.TxIDChainHash(), err)
			continue
		}

		utxo, err := node.UtxoStore.GetSpend(ctx, &utxostore.Spend{
			TxID:         input.PreviousTxIDChainHash(),
			SpendingData: spendpkg.NewSpendingData(btTx.TxIDChainHash(), 0),
			Vout:         input.PreviousTxOutIndex,
			UTXOHash:     utxoHash,
		})
		if err != nil {
			t.Errorf("Failed to get parent UTXO %s from store: %v", utxoHash, err)
			continue
		}

		if utxo == nil {
			t.Errorf("Parent UTXO %s does not exist in UTXO store", utxoHash)
		} else if utxo.SpendingData == nil {
			t.Errorf("Parent UTXO %s is not marked as spent", utxoHash)
		} else if utxo.SpendingData.TxID == nil {
			t.Errorf("Parent UTXO %s spending data has no TxID", utxoHash)
		} else if !utxo.SpendingData.TxID.IsEqual(btTx.TxIDChainHash()) {
			t.Errorf("Parent UTXO %s (%s:%d) is spent by %s instead of %s",
				utxoHash, input.PreviousTxIDChainHash(), input.PreviousTxOutIndex,
				utxo.SpendingData.TxID, btTx.TxIDChainHash())
		}
	}
}

// verifyTransactionOutputs verifies all transaction outputs exist in the UTXO store
func verifyTransactionOutputs(t *testing.T, ctx context.Context, node *daemon.TestDaemon, btTx *bt.Tx) {
	for vout, output := range btTx.Outputs {
		utxoHash, err := util.UTXOHashFromOutput(btTx.TxIDChainHash(), output, uint32(vout))
		if err != nil {
			t.Errorf("Failed to get UTXO hash for output %d in transaction %s: %v",
				vout, btTx.TxIDChainHash(), err)
			continue
		}

		utxo, err := node.UtxoStore.GetSpend(ctx, &utxostore.Spend{
			TxID:     btTx.TxIDChainHash(),
			Vout:     uint32(vout),
			UTXOHash: utxoHash,
		})
		if err != nil {
			t.Errorf("Failed to get UTXO %s from store: %v", utxoHash, err)
			continue
		}

		if utxo == nil {
			t.Errorf("UTXO %s (tx %s output %d) does not exist in UTXO store",
				utxoHash, btTx.TxIDChainHash(), vout)
		}
	}
}

// closeReader safely closes an io.ReadCloser
func closeReader(r io.ReadCloser) {
	if r != nil {
		_ = r.Close()
	}
}

// verifyBlockPersisterFiles verifies that block persister has correctly persisted all block-related files
// This includes block files, subtree data files, and UTXO additions/deletions files.
func verifyBlockPersisterFiles(t *testing.T, node *daemon.TestDaemon, blockHeaders []*model.BlockHeader, blockMetas []*model.BlockHeaderMeta) {
	ctx := node.Ctx

	blockStore, err := node.GetBlockStore()
	if err != nil {
		t.Errorf("Failed to get block store: %v", err)
		return
	}

	t.Logf("Verifying block persister files for %d blocks...", len(blockHeaders))

	// Genesis script to identify genesis block
	genesisScript := "04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e" +
		"206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73"

	blocksVerified := 0
	utxoAdditionsVerified := 0
	utxoDeletionsVerified := 0
	subtreeDataFilesVerified := 0

	// Range through block headers in reverse order (oldest first)
	for i := len(blockHeaders) - 1; i >= 0; i-- {
		blockHeader := blockHeaders[i]
		blockHash := blockHeader.Hash()
		height := blockMetas[i].Height

		block, err := node.BlockchainClient.GetBlock(ctx, blockHash)
		if err != nil {
			t.Errorf("Failed to get block %s: %v", blockHash, err)
			continue
		}

		// Skip genesis block for persister checks
		if block.CoinbaseTx.Inputs[0].UnlockingScript.String() == genesisScript {
			continue
		}

		// Wait for block to be persisted (with timeout)
		err = waitForBlockPersisted(ctx, blockStore, blockHash, 30*time.Second)
		if err != nil {
			t.Errorf("Block %s (height %d) was not persisted within timeout: %v", blockHash, height, err)
			continue
		}

		// Verify block file exists and is readable
		verifyBlockFile(t, ctx, blockStore, block)
		blocksVerified++

		// Verify subtree data files exist
		for _, subtreeHash := range block.Subtrees {
			if verifySubtreeDataFile(t, ctx, node.SubtreeStore, subtreeHash, blockHash) {
				subtreeDataFilesVerified++
			}
		}

		// Verify UTXO additions and deletions files
		additionsOK, deletionsOK := verifyUTXOFiles(t, ctx, node, blockStore, block, height)
		if additionsOK {
			utxoAdditionsVerified++
		}
		if deletionsOK {
			utxoDeletionsVerified++
		}
	}

	t.Logf("✓ Block persister verification complete: %d blocks, %d utxo-additions, %d utxo-deletions, %d subtree-data files",
		blocksVerified, utxoAdditionsVerified, utxoDeletionsVerified, subtreeDataFilesVerified)
}

// waitForBlockPersisted waits for a block file to exist in the blob store.
func waitForBlockPersisted(ctx context.Context, blockStore blob.Store, blockHash *chainhash.Hash, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	checkInterval := 500 * time.Millisecond

	for time.Now().Before(deadline) {
		exists, err := blockStore.Exists(ctx, blockHash.CloneBytes(), fileformat.FileTypeBlock)
		if err == nil && exists {
			return nil
		}
		time.Sleep(checkInterval)
	}

	return context.DeadlineExceeded
}

// verifyBlockFile verifies that the block file exists and can be read and parsed correctly.
func verifyBlockFile(t *testing.T, ctx context.Context, blockStore blob.Store, block *model.Block) {
	blockHash := block.Hash()

	// Check block file exists
	exists, err := blockStore.Exists(ctx, blockHash.CloneBytes(), fileformat.FileTypeBlock)
	if err != nil {
		t.Errorf("Failed to check if block file exists for %s: %v", blockHash, err)
		return
	}
	if !exists {
		t.Errorf("Block file does not exist for %s", blockHash)
		return
	}

	// Read and verify block data can be parsed
	blockData, err := blockStore.Get(ctx, blockHash.CloneBytes(), fileformat.FileTypeBlock)
	if err != nil {
		t.Errorf("Failed to read block data for %s: %v", blockHash, err)
		return
	}
	if len(blockData) == 0 {
		t.Errorf("Block data is empty for %s", blockHash)
		return
	}

	// Verify the block data can be parsed as a valid block
	parsedBlock, err := model.NewBlockFromBytes(blockData)
	if err != nil {
		t.Errorf("Failed to parse block data for %s: %v", blockHash, err)
		return
	}

	// Verify parsed block hash matches
	if !parsedBlock.Hash().IsEqual(blockHash) {
		t.Errorf("Parsed block hash mismatch: expected %s, got %s", blockHash, parsedBlock.Hash())
	}
}

// verifySubtreeDataFile verifies that the subtree data file exists.
func verifySubtreeDataFile(t *testing.T, ctx context.Context, subtreeStore blob.Store, subtreeHash *chainhash.Hash, blockHash *chainhash.Hash) bool {
	exists, err := subtreeStore.Exists(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
	if err != nil {
		t.Errorf("Failed to check if subtree data file exists for %s in block %s: %v", subtreeHash, blockHash, err)
		return false
	}
	if !exists {
		t.Errorf("Subtree data file does not exist for %s in block %s", subtreeHash, blockHash)
		return false
	}
	return true
}

// verifyUTXOFiles verifies that UTXO additions and deletions files exist and contain valid data.
//
//nolint:gocognit // complexity is acceptable for comprehensive verification
func verifyUTXOFiles(t *testing.T, ctx context.Context, node *daemon.TestDaemon, blockStore blob.Store, block *model.Block, height uint32) (additionsOK, deletionsOK bool) {
	blockHash := block.Hash()

	// Wait for UTXO files to be persisted
	deadline := time.Now().Add(30 * time.Second)
	checkInterval := 500 * time.Millisecond

	for time.Now().Before(deadline) {
		additionsExists, err1 := blockStore.Exists(ctx, blockHash.CloneBytes(), fileformat.FileTypeUtxoAdditions)
		deletionsExists, err2 := blockStore.Exists(ctx, blockHash.CloneBytes(), fileformat.FileTypeUtxoDeletions)

		if err1 == nil && err2 == nil && additionsExists && deletionsExists {
			break
		}
		time.Sleep(checkInterval)
	}

	// Verify UTXO additions file
	additionsExists, err := blockStore.Exists(ctx, blockHash.CloneBytes(), fileformat.FileTypeUtxoAdditions)
	if err != nil {
		t.Errorf("Failed to check if utxo-additions file exists for block %s: %v", blockHash, err)
	} else if !additionsExists {
		t.Errorf("utxo-additions file does not exist for block %s", blockHash)
	} else {
		// Read and verify additions content
		utxoSet, err := utxopersister.GetUTXOSet(ctx, node.Logger, node.Settings, blockStore, blockHash)
		if err != nil {
			t.Errorf("Failed to create UTXO set reader for block %s: %v", blockHash, err)
		} else {
			additionsReader, err := utxoSet.GetUTXOAdditionsReader(ctx)
			if err != nil {
				t.Errorf("Failed to get utxo-additions reader for block %s: %v", blockHash, err)
			} else {
				defer additionsReader.Close()
				for {
					utxoWrapper, err := utxopersister.NewUTXOWrapperFromReader(ctx, additionsReader)
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Errorf("Failed to read UTXO wrapper from additions file for block %s: %v", blockHash, err)
						break
					}

					// Verify height matches
					if utxoWrapper.Height != height {
						t.Errorf("UTXO height mismatch in block %s: expected %d, got %d", blockHash, height, utxoWrapper.Height)
					}
				}
				additionsOK = true
			}
		}
	}

	// Verify UTXO deletions file
	deletionsExists, err := blockStore.Exists(ctx, blockHash.CloneBytes(), fileformat.FileTypeUtxoDeletions)
	if err != nil {
		t.Errorf("Failed to check if utxo-deletions file exists for block %s: %v", blockHash, err)
	} else if !deletionsExists {
		t.Errorf("utxo-deletions file does not exist for block %s", blockHash)
	} else {
		// Read and verify deletions content
		utxoSet, err := utxopersister.GetUTXOSet(ctx, node.Logger, node.Settings, blockStore, blockHash)
		if err != nil {
			t.Errorf("Failed to create UTXO set reader for deletions for block %s: %v", blockHash, err)
		} else {
			deletionsReader, err := utxoSet.GetUTXODeletionsReader(ctx)
			if err != nil {
				t.Errorf("Failed to get utxo-deletions reader for block %s: %v", blockHash, err)
			} else {
				defer deletionsReader.Close()
				for {
					deletion, err := utxopersister.NewUTXODeletionFromReader(deletionsReader)
					if err == io.EOF {
						break
					}
					if err != nil {
						break
					}

					// Check for EOF marker (32 zero bytes)
					isEOFMarker := true
					for _, b := range deletion.TxID[:] {
						if b != 0 {
							isEOFMarker = false
							break
						}
					}
					if isEOFMarker {
						break
					}
				}
				deletionsOK = true
			}
		}
	}

	return additionsOK, deletionsOK
}
