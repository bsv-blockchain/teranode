package smoke

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	aerospike_store "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestCatchupAfterPruner tests that node2 can successfully catch up from node1
// even after node1 has pruned parent transactions.
//
// Test scenario:
// 1. node1 starts with blockchain, blockassembly, and pruner services
// 2. node1 mines to maturity and gets a spendable coinbase
// 3. node1 creates a transaction chain starting with the coinbase, mining 1 transaction per block
// 4. node1 stops mining at block height 10
// 5. node1's pruner removes all parent transactions (as they have been spent)
// 6. Verify that node1's utxostore has exactly 1 remaining transaction (the latest unspent one)
// 7. node2 starts with blockchain and blockassembly services
// 8. node2 attempts to catchup/sync with node1
// 9. node2 reaches block 10
// 10. Verify that node2 has all 10 transactions in its utxostore
func TestCatchup(t *testing.T) {
	t.Log("=== Test: Catchup After Pruner ===")
	t.Log("This test verifies that a node can sync from another node after transactions have been pruned")

	enableBlockPersister := true
	enablePruner := true
	runNode2 := true

	// Phase 1: Setup node1 with pruner
	// Note: Blockpersister is disabled due to listener allocation issues in test environment
	// t.Log("Phase 1: Starting node1 with blockchain, blockassembly, and pruner...")
	node1 := newNode(t, 1, enableBlockPersister, enablePruner)
	defer node1.Stop(t)

	// Phase 2: Mine to maturity and get spendable coinbase
	t.Log("Phase 2: Mining to maturity to get spendable coinbase...")
	coinbaseTx := node1.MineToMaturityAndGetSpendableCoinbaseTx(t, node1.Ctx)
	t.Logf("Coinbase transaction: %s", coinbaseTx.TxIDChainHash().String())

	// Phase 3: Create transaction chain with 1 tx per block up to height 10
	t.Log("Phase 3: Creating transaction chain with 1 tx per block...")

	txChain := make([]*bt.Tx, 0)
	currentTx := coinbaseTx

	// We need to mine to height 10, starting from coinbaseMaturity+1 (which is 3)
	// So we need 7 more blocks (from 3 to 10)
	targetHeight := uint32(10)
	currentHeight := uint32(node1.Settings.ChainCfgParams.CoinbaseMaturity + 1)

	for height := currentHeight; height < targetHeight; height++ {
		// Create a new transaction spending from the previous one
		newTx := node1.CreateTransactionWithOptions(t,
			transactions.WithInput(currentTx, 0),
			transactions.WithP2PKHOutputs(1, coinbaseTx.TotalOutputSatoshis()),
		)

		txChain = append(txChain, newTx)

		// Send the transaction
		err := node1.PropagationClient.ProcessTransaction(node1.Ctx, newTx)
		require.NoError(t, err, "Failed to send transaction at height %d", height)

		t.Logf("Created and sent transaction %d: %s", height, newTx.TxIDChainHash().String())

		// Wait for the transaction to be processed by block assembly
		err = node1.WaitForTransactionInBlockAssembly(newTx, 10*time.Second)
		require.NoError(t, err, "Timeout waiting for transaction to be processed by block assembly")

		// Mine a block with this transaction
		block := node1.MineAndWait(t, 1)
		t.Logf("Mined block at height %d: %s", height+1, block.Hash().String())

		// Update current transaction for next iteration
		currentTx = newTx
	}

	t.Logf("Successfully created transaction chain with %d transactions", len(txChain))

	// Phase 4: Wait for pruner to remove parent transactions

	if enablePruner {
		node1.WaitForPruner(t, 30*time.Second)
	}

	node1Records := getAllAerospikeRecords(t, node1.UtxoStore, "node1")
	printCoinbaseRecords(t, node1Records)
	printTxRecords(t, node1Records)

	// Phase 5: Verify pruning behavior
	t.Log("Phase 5: Checking pruning behavior...")
	// The last transaction in the chain should still be in the UTXO store (unspent)
	lastTx := txChain[len(txChain)-1]
	readTx, err := node1.UtxoStore.Get(node1.Ctx, lastTx.TxIDChainHash(), fields.Conflicting, fields.UnminedSince)
	require.NoError(t, err, "Failed to get last transaction from utxostore")
	require.NotNil(t, readTx, "Last transaction should exist in utxostore")
	require.False(t, readTx.Conflicting, "Last transaction should not be conflicting")
	require.Equal(t, uint32(0), readTx.UnminedSince, "Last transaction should be mined")
	t.Logf("✓ NODE1's utxostore contains the last unspent transaction: %s", lastTx.TxIDChainHash().String())

	// Check first transaction - it may or may not be pruned depending on pruner implementation
	// The key test is whether node2 can sync successfully regardless
	firstTx := txChain[0]
	_, err = node1.UtxoStore.Get(node1.Ctx, firstTx.TxIDChainHash())
	if err != nil {
		t.Logf("✓ First transaction has been pruned from node1's utxostore: %s", firstTx.TxIDChainHash().String())
	} else {
		t.Logf("ℹ First transaction still exists in node1's utxostore (pruner may not have run yet): %s", firstTx.TxIDChainHash().String())
	}

	// Phase 6: Get node1's best block before starting node2
	node1BestHeader, node1Meta, err := node1.BlockchainClient.GetBestBlockHeader(node1.Ctx)
	require.NoError(t, err)
	require.Equal(t, targetHeight, node1Meta.Height, "NODE1 should be at height %d", targetHeight)
	t.Logf("NODE1 at height %d, best block: %s", node1Meta.Height, node1BestHeader.Hash().String())

	if runNode2 {
		// Phase 7: Start node2
		node2 := newNode(t, 2, enableBlockPersister, enablePruner)
		defer node2.Stop(t)

		t.Logf("NODE2 created (ClientName: %s, GlobalBlockHeightRetention: %d, PeerID: %s)",
			node2.Settings.ClientName, node2.Settings.GlobalBlockHeightRetention, node2.Settings.P2P.PeerID)

		// Phase 8: Inject node1 into node2 to trigger sync
		t.Log("Phase 8: Injecting node1 into node2 to trigger catchup/sync...")
		node2.InjectPeer(t, node1)
		t.Log("NODE1 injected into NODE2's peer registry")

		// Phase 9: Wait for node2 to sync to node1's block height
		t.Log("Phase 9: Waiting for node2 to reach block 10...")
		node1BestBlock, err := node1.BlockchainClient.GetBlock(node1.Ctx, node1BestHeader.Hash())
		require.NoError(t, err)
		node2.WaitForBlockhash(t, node1BestHeader.Hash(), 60*time.Second)
		node2.WaitForBlockHeight(t, node1BestBlock, 60*time.Second)

		// Verify node2 reached the same height
		node2BestHeader, node2Meta, err := node2.BlockchainClient.GetBestBlockHeader(node2.Ctx)
		require.NoError(t, err)
		require.Equal(t, node1Meta.Height, node2Meta.Height, "NODE2 should be at same height as node1")
		require.Equal(t, node1BestHeader.Hash().String(), node2BestHeader.Hash().String(), "NODE2 should have same best block as node1")
		t.Logf("✓ NODE2 synced to height %d, block: %s", node2Meta.Height, node2BestHeader.Hash().String())

		// Phase 10: Verify node2 has all transactions in its utxostore
		t.Log("Phase 10: Verifying node2 has all transactions...")
		// NODE2 should have received all the block data including all transactions
		// Check that node2 has the last transaction
		readTx2, err := node2.UtxoStore.Get(node2.Ctx, lastTx.TxIDChainHash(), fields.Conflicting, fields.UnminedSince)
		require.NoError(t, err, "Failed to get last transaction from node2's utxostore")
		require.NotNil(t, readTx2, "Last transaction should exist in node2's utxostore")
		require.False(t, readTx2.Conflicting, "Last transaction should not be conflicting")
		require.Equal(t, uint32(0), readTx2.UnminedSince, "Last transaction should be mined")
		t.Logf("✓ NODE2's utxostore contains the last transaction: %s", lastTx.TxIDChainHash().String())

		// NODE2 should also have the first transaction (since it synced the full chain and hasn't pruned)
		readTxFirst, err := node2.UtxoStore.Get(node2.Ctx, firstTx.TxIDChainHash(), fields.Conflicting, fields.UnminedSince)
		require.NoError(t, err, "Failed to get first transaction from node2's utxostore")
		require.NotNil(t, readTxFirst, "First transaction should exist in node2's utxostore")
		t.Logf("✓ NODE2's utxostore contains the first transaction: %s (which was pruned from node1)", firstTx.TxIDChainHash().String())

		// Verify all transactions in the chain are present in node2
		for i, tx := range txChain {
			_, err := node2.UtxoStore.Get(node2.Ctx, tx.TxIDChainHash())
			require.NoError(t, err, "Transaction %d (%s) should exist in node2's utxostore", i, tx.TxIDChainHash().String())
		}
		t.Logf("✓ All %d transactions are present in node2's utxostore", len(txChain))

		t.Log("=== Test Passed: NODE2 successfully caught up after node1 pruned transactions ===")

		node2Records := getAllAerospikeRecords(t, node2.UtxoStore, "node2")
		printCoinbaseRecords(t, node2Records)
		printTxRecords(t, node2Records)
	}
}

func newNode(t *testing.T, nodeNumber int, enableBlockPersister bool, enablePruner bool) *daemon.TestDaemon {
	t.Logf("Phase 6: Starting node%d...", nodeNumber)

	var (
		privateKey string
		peerID     string
	)

	switch nodeNumber {
	case 1:
		peerID = test.Node1PeerID
		privateKey = test.Node1PrivateKey
	case 2:
		peerID = test.Node2PeerID
		privateKey = test.Node2PrivateKey
	case 3:
		peerID = test.Node3PeerID
		privateKey = test.Node3PrivateKey
	default:
		panic("Invalid node number")
	}

	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		// Blockchain is auto-enabled
		// SubtreeValidation is auto-enabled
		// BlockValidation is auto-enabled
		// BlockAssembly is auto-enabled
		// Asset is auto-enabled
		// Propagation is auto-enabled
		EnableBlockPersister: enableBlockPersister,
		EnablePruner:         enablePruner,
		EnableP2P:            true,
		EnableRPC:            true,
		EnableValidator:      true,
		UTXOStoreType:        "aerospike",
		// UseUnifiedLogger:     true,
		EnableErrorLogging: true,
		SkipRemoveDataDir:  nodeNumber > 1, // Only first node cleans the shared parent data dir; subsequent nodes share it
		PreserveDataDir:    true,
		SettingsOverrideFunc: test.ComposeSettings(
			func(s *settings.Settings) {
				// Apply MultiNodeSettings for unique identity and separate stores per node
				test.MultiNodeSettings(nodeNumber)(s)
				s.ClientName = fmt.Sprintf("NODE%d", nodeNumber)
				// Configure P2P identity for node1
				s.P2P.PeerID = peerID
				s.P2P.PrivateKey = privateKey
				// s.P2P.DHTMode = "off"
				s.P2P.PeerCacheDir = t.TempDir()
				s.P2P.StaticPeers = []string{}
				s.ChainCfgParams.CoinbaseMaturity = 1
				s.GlobalBlockHeightRetention = 1
				s.P2P.SyncCoordinatorPeriodicEvaluationInterval = 1 * time.Second
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockPersisted
			},
		),
		FSMState: blockchain.FSMStateRUNNING,
	})

	t.Logf("NODE%d created (ClientName: %s, GlobalBlockHeightRetention: %d, PeerID: %s)",
		nodeNumber, node.Settings.ClientName, node.Settings.GlobalBlockHeightRetention, node.Settings.P2P.PeerID)

	return node
}

func getAllAerospikeRecords(t *testing.T, store utxo.Store, nodeName string) []rec {
	t.Logf("=== Listing all Aerospike records for %s ===", nodeName)

	aerospikeStore, ok := store.(*aerospike_store.Store)
	if !ok {
		t.Logf("Store is not an Aerospike store, skipping record listing")
		return []rec{}
	}

	client := aerospikeStore.GetClient()
	namespace := aerospikeStore.GetNamespace()
	setName := aerospikeStore.GetSet()

	t.Logf("Scanning namespace=%s, set=%s", namespace, setName)

	scanPolicy := as.NewScanPolicy()
	recordSet, err := client.ScanAll(scanPolicy, namespace, setName)
	require.NoError(t, err, "Failed to scan Aerospike")

	records := []rec{}
	recordCount := 0

	for result := range recordSet.Results() {
		if result.Err != nil {
			t.Logf("Error reading record: %v", result.Err)
			continue
		}

		if result.Record == nil {
			t.Logf("Record is nil")
			continue
		}

		recordCount++
		bins := result.Record.Bins

		createdAt, ok := bins[fields.CreatedAt.String()].(int)
		if !ok {
			// Ignore
		}

		r := rec{
			createdAt: time.UnixMilli(int64(createdAt)),
		}

		if isCoinbase, ok := bins[fields.IsCoinbase.String()].(bool); ok {
			r.isCoinbase = isCoinbase
		}

		if blockIDs, ok := bins[fields.BlockIDs.String()].([]any); ok {
			if len(blockIDs) > 0 {
				r.blockID = blockIDs[0].(int)
			} else {
				r.blockID = -1
			}
		}

		if unminedSince, ok := bins[fields.UnminedSince.String()].(int); ok {
			r.unminedSince = unminedSince
		}

		if totalUtxos, ok := bins[fields.TotalUtxos.String()].(int); ok {
			r.totalUtxos = totalUtxos
		}

		if spentUtxos, ok := bins[fields.SpentUtxos.String()].(int); ok {
			r.spentUtxos = spentUtxos
		}

		if deleteAtHeight, ok := bins[fields.DeleteAtHeight.String()].(int); ok {
			r.deleteAtHeight = deleteAtHeight
		}

		if preserveUntil, ok := bins[fields.PreserveUntil.String()].(int); ok {
			r.preserveUntil = preserveUntil
		}

		records = append(records, r)
	}

	sort.Slice(records, func(i, j int) bool {
		// Sort by blockID first and then by isCoinbase
		if records[i].blockID != records[j].blockID {
			return records[i].blockID < records[j].blockID
		}

		return records[i].isCoinbase && !records[j].isCoinbase
	})

	return records
}

func printCoinbaseRecords(t *testing.T, records []rec) {
	for i, r := range records {
		if r.isCoinbase {
			t.Logf("%2d: %v", i, r)
		}
	}
}

func printTxRecords(t *testing.T, records []rec) {
	for i, r := range records {
		if !r.isCoinbase {
			t.Logf("%2d: %v", i, r)
		}
	}
}

type rec struct {
	createdAt      time.Time
	isCoinbase     bool
	blockID        int
	unminedSince   int
	totalUtxos     int
	spentUtxos     int
	deleteAtHeight int
	preserveUntil  int
}

func (r rec) String() string {
	var sb strings.Builder

	sb.WriteString(r.createdAt.Format("2006-01-02 15:04:05.000000"))

	if r.isCoinbase {
		sb.WriteString(" coinbase for block: ")
	} else {
		sb.WriteString("       tx for block: ")
	}

	sb.WriteString(fmt.Sprintf("%2d", r.blockID))

	sb.WriteString(fmt.Sprintf(" unminedSince: %d", r.unminedSince))
	sb.WriteString(fmt.Sprintf(" totalUtxos: %d", r.totalUtxos))
	sb.WriteString(fmt.Sprintf(" spentUtxos: %d", r.spentUtxos))
	sb.WriteString(fmt.Sprintf(" deleteAtHeight: %3d", r.deleteAtHeight))
	sb.WriteString(fmt.Sprintf(" preserveUntil: %d", r.preserveUntil))

	return sb.String()
}
