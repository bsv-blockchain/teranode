package subtreeprocessor

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	st "github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_subtreeprocessor ./test/...

var (
	prevBlockHeader = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567890,
		Bits:           model.NBit{},
		Nonce:          1234,
	}

	blockHeader = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevBlockHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567890,
		Bits:           model.NBit{},
		Nonce:          1234,
	}
)

func TestMoveForwardBlockLarge(t *testing.T) {

	n := 1049576
	txIds := make([]string, n)

	for i := 0; i < n; i++ {
		txid, err := generateTxID()
		if err != nil {
			t.Errorf("error generating txid: %s", err)
		}

		txIds[i] = txid
	}

	newSubtreeChan := make(chan st.NewSubtreeRequest)

	var wg sync.WaitGroup

	wg.Add(4) // we are expecting 4 subtrees

	go func() {
		for {
			// just read the subtrees of the processor
			subtreeRequest := <-newSubtreeChan

			// Send success response to prevent deadlock
			if subtreeRequest.ErrChan != nil {
				subtreeRequest.ErrChan <- nil
			}

			wg.Done()
		}
	}()

	subtreeStore := blob_memory.New()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 262144

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	// Create a mock blockchain client
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	stp, _ := st.NewSubtreeProcessor(
		context.Background(),
		ulogger.TestLogger{},
		tSettings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	stp.Start(context.Background())

	for i, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		if i == 0 {
			stp.GetCurrentSubtree().ReplaceRootNode(hash, 0, 0)
		} else {
			stp.AddBatch([]subtreepkg.Node{{Hash: *hash, Fee: 1}}, []*subtreepkg.TxInpoints{{ParentTxHashes: []chainhash.Hash{}}})
		}
	}

	wg.Wait()
	// sleep for 1 second
	// this is to make sure the subtrees are added to the chain
	time.Sleep(1 * time.Second)

	// there should be 4 chained subtrees
	assert.Equal(t, 4, len(stp.GetChainedSubtrees()))
	// one of the subtrees should contain 262144 items
	assert.Equal(t, 262144, stp.GetChainedSubtrees()[0].Size())
	// there should be no remaining items in the current subtree
	assert.Equal(t, 1000, stp.GetCurrentSubtree().Length())

	stp.SetCurrentItemsPerFile(65536)
	_ = stp.GetUtxoStore().SetBlockHeight(1)
	//nolint:gosec
	_ = stp.GetUtxoStore().SetMedianBlockTime(uint32(time.Now().Unix()))

	wg.Add(8) // we are expecting 4 subtrees

	stp.InitCurrentBlockHeader(prevBlockHeader)

	timeStart := time.Now()

	// moveForwardBlock saying the last subtree in the block was number 2 in the chainedSubtree slice
	// this means half the subtrees will be moveForwardBlock
	// new items per file is 65536 so there should be 8 subtrees in the chain
	err = stp.MoveForwardBlock(&model.Block{
		Header: blockHeader,
		Subtrees: []*chainhash.Hash{
			stp.GetChainedSubtrees()[0].RootHash(),
			stp.GetChainedSubtrees()[1].RootHash(),
		},
		CoinbaseTx: coinbaseTx,
	})

	wg.Wait()
	fmt.Printf("moveForwardBlock took %s\n", time.Since(timeStart))

	time.Sleep(1 * time.Second)

	require.NoError(t, err)
	assert.Equal(t, 8, len(stp.GetChainedSubtrees()))
	// one of the subtrees should contain 262144 items
	assert.Equal(t, 65536, stp.GetChainedSubtrees()[0].Size())
	assert.Equal(t, 1001, stp.GetCurrentSubtree().Length())
}

func TestSubtreeProcessor_CreateTransactionMap(t *testing.T) {
	t.Run("large", func(t *testing.T) {

		newSubtreeChan := make(chan st.NewSubtreeRequest)
		subtreeStore := blob_memory.New()

		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)

		tSettings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)

		// Create a mock blockchain client
		mockBlockchainClient := &blockchain.Mock{}

		stp, _ := st.NewSubtreeProcessor(
			context.Background(),
			ulogger.TestLogger{},
			tSettings,
			subtreeStore,
			mockBlockchainClient,
			utxoStore,
			newSubtreeChan,
		)
		stp.Start(context.Background())

		subtreeSize := uint64(1024 * 1024)
		nrSubtrees := 10

		subtrees := make([]*subtreepkg.Subtree, nrSubtrees)

		block := &model.Block{
			Header:     prevBlockHeader,
			Subtrees:   []*chainhash.Hash{},
			CoinbaseTx: coinbaseTx,
		}

		for i := 0; i < nrSubtrees; i++ {
			subtree := createSubtree(t, subtreeSize, i == 0)
			subtreeBytes, err := subtree.Serialize()
			require.NoError(t, err)
			err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
			require.NoError(t, err)

			block.Subtrees = append(block.Subtrees, subtree.RootHash())
			subtrees[i] = subtree
		}

		blockSubtreesMap := make(map[chainhash.Hash]int, len(block.Subtrees))
		for idx, subtree := range block.Subtrees {
			blockSubtreesMap[*subtree] = idx
		}

		_ = blockSubtreesMap

		f, _ := os.Create("cpu.prof")
		defer f.Close()

		_ = pprof.StartCPUProfile(f)
		start := time.Now()

		transactionMap, _, err := stp.CreateTransactionMap(context.Background(), blockSubtreesMap, nrSubtrees, uint64(nrSubtrees)*subtreeSize)
		require.NoError(t, err)

		pprof.StopCPUProfile()
		t.Logf("Time taken: %s\n", time.Since(start))

		f, _ = os.Create("mem.prof")
		defer f.Close()
		_ = pprof.WriteHeapProfile(f)
		//nolint:gosec
		assert.Equal(t, int(subtreeSize)*nrSubtrees, transactionMap.Length())

		start = time.Now()

		var wg sync.WaitGroup

		for _, subtree := range subtrees {
			wg.Add(1)

			go func(subtree *subtreepkg.Subtree) {
				defer wg.Done()
				//nolint:gosec
				for i := 0; i < int(subtreeSize); i++ {
					assert.True(t, transactionMap.Exists(subtree.Nodes[i].Hash))
				}
			}(subtree)
		}

		wg.Wait()
		t.Logf("Time taken to read: %s\n", time.Since(start))
	})
}

// generateTxID generates a random 32-byte hexadecimal string.
func generateTxID() (string, error) {
	b := make([]byte, 32)

	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", b), nil
}

func createSubtree(t *testing.T, length uint64, createCoinbase bool) *subtreepkg.Subtree {
	//nolint:gosec
	subtree, err := subtreepkg.NewTreeByLeafCount(int(length))
	require.NoError(t, err)

	start := uint64(0)

	if createCoinbase {
		err = subtree.AddCoinbaseNode()
		require.NoError(t, err)

		start = 1
	}

	for i := start; i < length; i++ {
		txHash, err := generateTxHash()
		require.NoError(t, err)
		err = subtree.AddNode(txHash, i, i)
		require.NoError(t, err)
	}
	// fmt.Println("done with subtree: ", subtree)
	return subtree
}

// generateTxID generates a random chainhash.Hash.
func generateTxHash() (chainhash.Hash, error) {
	b := make([]byte, 32)

	_, err := rand.Read(b)
	if err != nil {
		return chainhash.Hash{}, err
	}

	return chainhash.Hash(b), nil
}
