package subtreeprocessor

import (
	"bytes"
	"encoding/binary"
	"net/url"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	blockchainstore "github.com/bitcoin-sv/teranode/stores/blockchain"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_subtreeprocessor ./test/...

var (
	coinbaseTx, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1a03a403002f746572616e6f64652f9f9fba46d5a08a6be11ddb2dffffffff0a0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac00000000")

	hash1 = chainhash.HashH([]byte("tx1"))
	hash2 = chainhash.HashH([]byte("tx2"))

	node1 = util.SubtreeNode{
		Hash:        hash1,
		Fee:         1,
		SizeInBytes: 1,
	}
	node2 = util.SubtreeNode{
		Hash:        hash2,
		Fee:         1,
		SizeInBytes: 1,
	}
	parents = []chainhash.Hash{hash1, hash2}
)

func Test_DeserializeHashesFromReaderIntoBuckets(t *testing.T) {
	size := 1024 * 1024
	subtreeBytes := generateLargeSubtreeBytes(t, size)
	r := bytes.NewReader(subtreeBytes)

	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	buckets, _, err := subtreeprocessor.DeserializeHashesFromReaderIntoBuckets(r, 16)
	require.NoError(t, err)

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)

	assert.Equal(t, 16, len(buckets))
}

func Test_AddTx(t *testing.T) {
	t.Run("Process transaction into subtrees and currentTxMap", func(t *testing.T) {
		stp, _, _ := initSubtreeProcessor(t)

		waitForSubtreeProcessorQueueToEmpty(t, stp)
		assert.Equal(t, uint64(1), stp.TxCount(), "Expected tx count to be 1 at startup")

		stp.Add(node1, parents)
		stp.Add(node2, parents)

		waitForSubtreeProcessorQueueToEmpty(t, stp)
		assert.Equal(t, uint64(3), stp.TxCount(), "Expected tx count to be 2 after adding a transaction")

		txMap := stp.GetCurrentTxMap()
		// currentTxMap should contain 2 transactions, it does not contain the coinbase placeholder
		assert.Equal(t, 2, txMap.Length())

		tx1NodeParents, ok := txMap.Get(node1.Hash)
		require.True(t, ok)
		assert.Equal(t, parents, tx1NodeParents, "Expected tx1 node to be in the currentTxMap")

		tx2NodeParents, ok := txMap.Get(node2.Hash)
		require.True(t, ok)
		assert.Equal(t, parents, tx2NodeParents, "Expected tx1 node to be in the currentTxMap")
	})
}

func Test_MoveBlock(t *testing.T) {
	t.Run("Move blocks and check if the subtree processor is in the correct state", func(t *testing.T) {
		stp, subtreeStore, subtrees, nrTransactions, subtreeSize, expectedSubtrees := initMoveBlock(t)

		txMap := stp.GetCurrentTxMap()
		assert.Equal(t, nrTransactions, txMap.Length())

		subtreeHashes := storeMoveBlockSubtrees(t, subtreeStore, subtrees, txMap)

		block := &model.Block{
			Height:     123,
			CoinbaseTx: coinbaseTx,
			Subtrees:   subtreeHashes,
		}

		checkMoveBlockProcessing(t, stp, block, nrTransactions, subtreeSize, expectedSubtrees)
	})

	t.Run("Move blocks with different subtrees", func(t *testing.T) {
		stp, subtreeStore, subtrees, nrTransactions, subtreeSize, expectedSubtrees := initMoveBlock(t)

		txMap := stp.GetCurrentTxMap()
		assert.Equal(t, nrTransactions, txMap.Length())

		_ = storeMoveBlockSubtrees(t, subtreeStore, subtrees, txMap)

		// create 1 subtree with all the transactions and process the block
		newSubtree, err := util.NewTreeByLeafCount(1024)
		require.NoError(t, err)

		require.NoError(t, newSubtree.AddCoinbaseNode())

		for _, subtree := range subtrees {
			for _, node := range subtree.Nodes {
				if !node.Hash.IsEqual(util.CoinbasePlaceholderHash) {
					err = newSubtree.AddSubtreeNode(node)
					require.NoError(t, err)
				}
			}
		}

		newSubtreeHashes := storeMoveBlockSubtrees(t, subtreeStore, []*util.Subtree{newSubtree}, txMap)

		block := &model.Block{
			Height:     123,
			CoinbaseTx: coinbaseTx,
			Subtrees:   newSubtreeHashes,
		}

		checkMoveBlockProcessing(t, stp, block, nrTransactions, subtreeSize, expectedSubtrees)
	})
}

func storeMoveBlockSubtrees(t *testing.T, subtreeStore *memory.Memory, subtrees []*util.Subtree, txMap *util.SyncedMap[chainhash.Hash, []chainhash.Hash]) []*chainhash.Hash {
	subtreeHashes := make([]*chainhash.Hash, 0, len(subtrees))

	for _, subtree := range subtrees {
		// put the subtrees in the store
		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		require.NoError(t, subtreeStore.Set(t.Context(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree")))

		subtreeMeta := util.NewSubtreeMeta(subtree)
		for idx, node := range subtree.Nodes {
			if !node.Hash.IsEqual(util.CoinbasePlaceholderHash) {
				parents, ok := txMap.Get(node.Hash)
				require.True(t, ok)

				require.NoError(t, subtreeMeta.SetParentTxHashes(idx, parents))
			}
		}

		// put the subtree meta in the store
		subtreeMetaBytes, err := subtreeMeta.Serialize()
		require.NoError(t, err)

		require.NoError(t, subtreeStore.Set(t.Context(), subtree.RootHash()[:], subtreeMetaBytes, options.WithFileExtension("meta")))

		subtreeHashes = append(subtreeHashes, subtree.RootHash())
	}

	return subtreeHashes
}

func checkMoveBlockProcessing(t *testing.T, stp *subtreeprocessor.SubtreeProcessor, block *model.Block, nrTransactions int, subtreeSize int, expectedSubtrees int) {
	coinbaseTx2 := block.CoinbaseTx.Clone()
	coinbaseTx2.Version = 2

	block2 := &model.Block{
		Height:     123,
		CoinbaseTx: coinbaseTx2,
		Subtrees:   []*chainhash.Hash{},
	}

	err := stp.MoveForwardBlock(block)
	require.NoError(t, err)

	waitForSubtreeProcessorQueueToEmpty(t, stp)

	expectedTransactionsInSubtreeProcessor := nrTransactions - (subtreeSize * expectedSubtrees) + 1 // 128 is the initial subtree size, + 1 for the coinbase tx

	assert.Equal(t, uint64(expectedTransactionsInSubtreeProcessor)+1, stp.TxCount(), "Expected tx count to be + 1 for the new coinbase tx")

	txMap := stp.GetCurrentTxMap()
	assert.Equal(t, expectedTransactionsInSubtreeProcessor, txMap.Length())

	require.NoError(t, stp.CheckSubtreeProcessor())

	// move back the block and make sure all the transactions are put back
	require.NoError(t, stp.Reorg([]*model.Block{block}, []*model.Block{block2}))

	assert.Equal(t, uint64(nrTransactions)+1, stp.TxCount(), "Expected tx count to be + 1 for the new coinbase tx")

	txMap = stp.GetCurrentTxMap()
	assert.Equal(t, nrTransactions, txMap.Length())

	require.NoError(t, stp.CheckSubtreeProcessor())

	// move back again and make sure all the transactions are processed
	require.NoError(t, stp.Reorg([]*model.Block{block2}, []*model.Block{block}))

	assert.Equal(t, uint64(expectedTransactionsInSubtreeProcessor)+1, stp.TxCount(), "Expected tx count to be + 1 for the new coinbase tx")

	txMap = stp.GetCurrentTxMap()
	assert.Equal(t, expectedTransactionsInSubtreeProcessor, txMap.Length())

	require.NoError(t, stp.CheckSubtreeProcessor())
}

func initMoveBlock(t *testing.T) (*subtreeprocessor.SubtreeProcessor, *memory.Memory, []*util.Subtree, int, int, int) {
	stp, subtreeStore, newSubtreeChan := initSubtreeProcessor(t)
	subtrees := make([]*util.Subtree, 0)

	gotAllSubtrees := make(chan bool)

	go func() {
		for subtreeRequest := range newSubtreeChan {
			subtrees = append(subtrees, subtreeRequest.Subtree)

			if len(subtrees) == 7 {
				gotAllSubtrees <- true
			}
		}
	}()

	waitForSubtreeProcessorQueueToEmpty(t, stp)
	assert.Equal(t, uint64(1), stp.TxCount(), "Expected tx count to be 1 at startup")

	nrTransactions := 1000
	subtreeSize := 128
	expectedSubtrees := nrTransactions / subtreeSize

	hashes := make(map[chainhash.Hash]int)

	// add lots of transactions
	for i := 0; i < nrTransactions; i++ {
		hash := chainhash.HashH([]byte("tx" + string(rune(i))))

		hashes[hash] = i

		node := util.SubtreeNode{
			Hash:        hash,
			Fee:         1,
			SizeInBytes: 1,
		}

		stp.Add(node, []chainhash.Hash{hash1, hash2})
	}

	waitForSubtreeProcessorQueueToEmpty(t, stp)

	<-gotAllSubtrees

	assert.Equal(t, 7, len(subtrees))
	assert.Equal(t, uint64(nrTransactions+1), stp.TxCount(), "Expected tx count to be + 1 for the new coinbase tx")

	return stp, subtreeStore, subtrees, nrTransactions, subtreeSize, expectedSubtrees
}

func waitForSubtreeProcessorQueueToEmpty(t *testing.T, stp *subtreeprocessor.SubtreeProcessor) {
	t.Helper()

	// Wait for the queue to be empty
	for {
		if stp.QueueLength() == 0 {
			break
		}
	}

	// Check if the queue is empty
	if stp.QueueLength() != 0 {
		t.Fatalf("Expected queue length to be 0, but got %d", stp.QueueLength())
	}

	time.Sleep(100 * time.Millisecond) // Give some time for the queue to process
}

func initSubtreeProcessor(t *testing.T) (*subtreeprocessor.SubtreeProcessor, *memory.Memory, chan subtreeprocessor.NewSubtreeRequest) {
	blobStore, utxoStore, tSettings, blockchainClient, _, err := initStores()
	require.NoError(t, err)

	newSubtreeChan := make(chan subtreeprocessor.NewSubtreeRequest, 1)

	subtreeProcessor, err := subtreeprocessor.NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, blobStore, blockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	return subtreeProcessor, blobStore, newSubtreeChan
}

func initStores() (*memory.Memory, *utxostore.Memory, *settings.Settings, blockchain.ClientI, *blockassembly.BlockAssembly, error) {
	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})

	tracing.SetGlobalMockTracer()

	tSettings := test.CreateBaseTestSettings()
	tSettings.Policy.BlockMaxSize = 1000000
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	tSettings.BlockAssembly.ResetWaitCount = 0
	tSettings.BlockAssembly.ResetWaitDuration = 0
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 128

	blockchainStoreURL, _ := url.Parse("sqlitememory://")
	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, blockchainStoreURL, tSettings)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockchainStore, nil, nil)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	return blobStore, utxoStore, tSettings, blockchainClient, nil, nil
}

func generateLargeSubtreeBytes(t *testing.T, size int) []byte {
	st, err := util.NewIncompleteTreeByLeafCount(size)
	require.NoError(t, err)

	var bb [32]byte
	for i := 0; i < size; i++ {
		// int to bytes
		//nolint:gosec
		binary.LittleEndian.PutUint32(bb[:], uint32(i))
		_ = st.AddNode(bb, uint64(i), uint64(i))
	}

	ser, err := st.Serialize()
	require.NoError(t, err)

	return ser
}
