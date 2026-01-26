package subtreeprocessor

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/url"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_subtreeprocessor ./test/...

var (
	coinbaseTx, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1a03a403002f746572616e6f64652f9f9fba46d5a08a6be11ddb2dffffffff0a0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac00000000")

	hash1 = chainhash.HashH([]byte("tx1"))
	hash2 = chainhash.HashH([]byte("tx2"))

	node1 = subtreepkg.Node{
		Hash:        hash1,
		Fee:         1,
		SizeInBytes: 1,
	}
	node2 = subtreepkg.Node{
		Hash:        hash2,
		Fee:         1,
		SizeInBytes: 1,
	}
	parents = subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{hash1, hash2}}
)

func Test_DeserializeHashesFromReaderIntoBuckets(t *testing.T) {
	size := 1024 * 1024
	subtreeBytes := generateLargeSubtreeBytes(t, size)
	r := bytes.NewReader(subtreeBytes)

	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	buckets := make(map[uint16][]chainhash.Hash, 16)
	for i := 0; i < 16; i++ {
		buckets[uint16(i)] = make([]chainhash.Hash, 0)
	}

	conflictingNodes := make([]chainhash.Hash, 0)

	err := subtreeprocessor.DeserializeHashesFromReaderIntoBuckets(r, 16, &buckets, &conflictingNodes)
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

		stp.AddBatch([]subtreepkg.Node{node1, node2}, []*subtreepkg.TxInpoints{&parents, &parents})

		waitForSubtreeProcessorQueueToEmpty(t, stp)
		assert.Equal(t, uint64(3), stp.TxCount(), "Expected tx count to be 2 after adding a transaction")

		txMap := stp.GetCurrentTxMap()
		// currentTxMap should contain 2 transactions, it does not contain the coinbase placeholder
		assert.Equal(t, 2, txMap.Length())

		tx1NodeParents, ok := txMap.Get(node1.Hash)
		require.True(t, ok)
		assert.Equal(t, parents, *tx1NodeParents, "Expected tx1 node to be in the currentTxMap")

		tx2NodeParents, ok := txMap.Get(node2.Hash)
		require.True(t, ok)
		assert.Equal(t, parents, *tx2NodeParents, "Expected tx1 node to be in the currentTxMap")
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
		newSubtree, err := subtreepkg.NewTreeByLeafCount(1024)
		require.NoError(t, err)

		require.NoError(t, newSubtree.AddCoinbaseNode())

		for _, subtree := range subtrees {
			for _, node := range subtree.Nodes {
				if !node.Hash.IsEqual(subtreepkg.CoinbasePlaceholderHash) {
					err = newSubtree.AddSubtreeNode(node)
					require.NoError(t, err)
				}
			}
		}

		newSubtreeHashes := storeMoveBlockSubtrees(t, subtreeStore, []*subtreepkg.Subtree{newSubtree}, txMap)

		block := &model.Block{
			Height:     123,
			CoinbaseTx: coinbaseTx,
			Subtrees:   newSubtreeHashes,
		}

		checkMoveBlockProcessing(t, stp, block, nrTransactions, subtreeSize, expectedSubtrees)
	})
}

func storeMoveBlockSubtrees(t *testing.T, subtreeStore *memory.Memory, subtrees []*subtreepkg.Subtree, txMap subtreeprocessor.TxInpointsMap) []*chainhash.Hash {
	subtreeHashes := make([]*chainhash.Hash, 0, len(subtrees))

	for _, subtree := range subtrees {
		// put the subtrees in the store
		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		require.NoError(t, subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

		subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)

		for idx, node := range subtree.Nodes {
			if !node.Hash.IsEqual(subtreepkg.CoinbasePlaceholderHash) {
				txInpoints, ok := txMap.Get(node.Hash)
				require.True(t, ok)

				require.NoError(t, subtreeMeta.SetTxInpoints(idx, *txInpoints))
			}
		}

		// put the subtree meta in the store
		subtreeMetaBytes, err := subtreeMeta.Serialize()
		require.NoError(t, err)

		require.NoError(t, subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

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

	assert.Equal(t, uint64(expectedTransactionsInSubtreeProcessor)+1, stp.TxCount(), "Expected tx count to be + 1 for the new coinbase tx") //nolint:gosec

	txMap := stp.GetCurrentTxMap()
	assert.Equal(t, expectedTransactionsInSubtreeProcessor, txMap.Length())

	require.NoError(t, stp.CheckSubtreeProcessor())

	// move back the block and make sure all the transactions are put back
	require.NoError(t, stp.Reorg([]*model.Block{block}, []*model.Block{block2}))

	assert.Equal(t, uint64(nrTransactions)+1, stp.TxCount(), "Expected tx count to be + 1 for the new coinbase tx") //nolint:gosec

	txMap = stp.GetCurrentTxMap()
	assert.Equal(t, nrTransactions, txMap.Length())

	require.NoError(t, stp.CheckSubtreeProcessor())

	// move back again and make sure all the transactions are processed
	require.NoError(t, stp.Reorg([]*model.Block{block2}, []*model.Block{block}))

	assert.Equal(t, uint64(expectedTransactionsInSubtreeProcessor)+1, stp.TxCount(), "Expected tx count to be + 1 for the new coinbase tx") //nolint:gosec

	txMap = stp.GetCurrentTxMap()
	assert.Equal(t, expectedTransactionsInSubtreeProcessor, txMap.Length())

	require.NoError(t, stp.CheckSubtreeProcessor())
}

func initMoveBlock(t *testing.T) (*subtreeprocessor.SubtreeProcessor, *memory.Memory, []*subtreepkg.Subtree, int, int, int) {
	stp, subtreeStore, newSubtreeChan := initSubtreeProcessor(t)
	subtrees := make([]*subtreepkg.Subtree, 0)

	gotAllSubtrees := make(chan bool)

	go func() {
		for subtreeRequest := range newSubtreeChan {
			subtrees = append(subtrees, subtreeRequest.Subtree)

			// Send success response to prevent deadlock
			if subtreeRequest.ErrChan != nil {
				subtreeRequest.ErrChan <- nil
			}

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

		node := subtreepkg.Node{
			Hash:        hash,
			Fee:         1,
			SizeInBytes: 1,
		}

		stp.AddBatch([]subtreepkg.Node{node}, []*subtreepkg.TxInpoints{{ParentTxHashes: []chainhash.Hash{hash1, hash2}, Idxs: [][]uint32{{0, 1}, {2, 3}}}})
	}

	waitForSubtreeProcessorQueueToEmpty(t, stp)

	<-gotAllSubtrees

	assert.Equal(t, 7, len(subtrees))
	assert.Equal(t, uint64(nrTransactions+1), stp.TxCount(), "Expected tx count to be + 1 for the new coinbase tx") //nolint:gosec

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
	blobStore, utxoStore, tSettings, blockchainClient, _, err := initStores(t)
	require.NoError(t, err)

	newSubtreeChan := make(chan subtreeprocessor.NewSubtreeRequest, 1)

	ctx := t.Context()
	subtreeProcessor, err := subtreeprocessor.NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, blobStore, blockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)
	subtreeProcessor.Start(ctx)

	return subtreeProcessor, blobStore, newSubtreeChan
}

func initStores(t *testing.T) (*memory.Memory, utxo.Store, *settings.Settings, blockchain.ClientI, *blockassembly.BlockAssembly, error) {
	blobStore := memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Policy.BlockMaxSize = 1000000
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 128

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	tracing.SetupMockTracer()

	blockchainStoreURL, _ := url.Parse("sqlitememory://")

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, blockchainStoreURL, tSettings)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockchainStore, nil, nil)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	return blobStore, utxoStore, tSettings, blockchainClient, nil, nil
}

func generateLargeSubtreeBytes(t *testing.T, size int) []byte {
	st, err := subtreepkg.NewIncompleteTreeByLeafCount(size)
	require.NoError(t, err)

	var (
		bb [32]byte
		i  uint64
	)

	for i = 0; i < uint64(size); i++ { //nolint:gosec
		binary.LittleEndian.PutUint32(bb[:], uint32(i)) //nolint:gosec
		_ = st.AddNode(bb, i, i)
	}

	ser, err := st.Serialize()
	require.NoError(t, err)

	return ser
}
