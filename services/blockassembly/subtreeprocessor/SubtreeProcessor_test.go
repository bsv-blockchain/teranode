package subtreeprocessor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	blob_memory "github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/null"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var (
	// Fill the array with 0xFF
	coinbaseHash, _ = chainhash.NewHashFromStr("8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87")
	coinbaseTx, _   = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1a03a403002f746572616e6f64652f9f9fba46d5a08a6be11ddb2dffffffff0a0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac00000000")
	coinbaseTx2, _  = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fc00002f6d312d65752fec97bce568b53123b2adfe06ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	coinbaseTx3, _  = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb00002f6d312d65752f622127f93431de4016036b10ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	// coinbaseTx4, _  = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fa00002f6d312d65752f83df000138d5f03188eb156effffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
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

	nextBlockHeader = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  blockHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567890,
		Bits:           model.NBit{},
		Nonce:          1234,
	}

	aBlockHeader = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{'a'},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567890,
		Bits:           model.NBit{},
		Nonce:          1234,
	}

	txIds = []string{
		"fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4",
		"6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4",
		"e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d",
	}
)

func TestRotate(t *testing.T) {
	newSubtreeChan := make(chan NewSubtreeRequest)
	endTestChan := make(chan bool)

	go func() {
		for {
			subtreeRequest := <-newSubtreeChan
			subtree := subtreeRequest.Subtree
			assert.Equal(t, 4, subtree.Length())
			assert.Equal(t, uint64(3), subtree.Fees)

			// Test the merkle root with the coinbase placeholder
			merkleRoot := subtree.RootHash()
			assert.Equal(t, "fd8e7ab196c23534961ef2e792e13426844f831e83b856aa99998ab9908d854f", merkleRoot.String())

			// Test the merkle root with the coinbase placeholder replaced
			merkleRoot = subtree.ReplaceRootNode(coinbaseHash, 0, 0)
			assert.Equal(t, "f3e94742aca4b5ef85488dc37c06c3282295ffec960994b2c0d5ac2a25a95766", merkleRoot.String())

			endTestChan <- true
		}
	}()

	settings := test.CreateBaseTestSettings()
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	stp, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, nil, nil, nil, newSubtreeChan)

	for _, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		stp.Add(util.SubtreeNode{Hash: *hash, Fee: 1})
	}

	<-endTestChan

	assert.Equal(t, 0, stp.currentSubtree.Length())
	assert.Equal(t, 1, len(stp.chainedSubtrees))

	// Add one more txid to trigger the rotate
	// hash, err := chainhash.NewHashFromStr("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	// require.NoError(t, err)
	// hash, err := chainhash.NewHashFromStr("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	// require.NoError(t, err)

	// TODO there is a race condition here (only in the test) that needs to be fixed, but don't want to change the code
	// in the subtree processor, because this is a performance issue
	// stp.Add(util.SubtreeNode{Hash: *hash, Fee: 1})
	// time.Sleep(100 * time.Millisecond)
	// assert.Equal(t, 1, stp.currentSubtree.Length())

	// Still 1 because the tree is not yet complete
	assert.Equal(t, 1, len(stp.chainedSubtrees))
}

func Test_RemoveTxFromSubtrees(t *testing.T) {
	t.Run("remove transaction from subtrees", func(t *testing.T) {
		newSubtreeChan := make(chan NewSubtreeRequest)
		subtreeStore := blob_memory.New()
		utxosStore := memory.New(ulogger.TestLogger{})
		tSettings := test.CreateBaseTestSettings()
		tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

		stp, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, nil, utxosStore, newSubtreeChan)

		// add some random nodes to the subtrees
		for i := uint64(0); i < 42; i++ {
			_ = stp.addNode(util.SubtreeNode{Hash: chainhash.HashH([]byte(fmt.Sprintf("tx-%d", i))), Fee: i}, true)
		}

		// check the length of the subtrees
		assert.Len(t, stp.chainedSubtrees, 10)
		assert.Len(t, stp.currentSubtree.Nodes, 3)

		// get the middle transaction from the middle subtree
		txHash := stp.chainedSubtrees[5].Nodes[2].Hash

		// Remove a transaction from the subtree
		err := stp.removeTxFromSubtrees(context.Background(), txHash)
		require.NoError(t, err)

		// check that the txHash node has been replaced
		assert.NotEqual(t, stp.chainedSubtrees[5].Nodes[2].Hash, txHash)

		// check the length of the subtrees again
		assert.Len(t, stp.chainedSubtrees, 10)
		assert.Len(t, stp.currentSubtree.Nodes, 2)
	})
}

func TestReChainSubtrees(t *testing.T) {
	// Create a SubtreeProcessor
	newSubtreeChan := make(chan NewSubtreeRequest)
	subtreeStore, _ := null.New(ulogger.TestLogger{})
	utxosStore := memory.New(ulogger.TestLogger{})
	tSettings := test.CreateBaseTestSettings()
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	stp, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, nil, utxosStore, newSubtreeChan)

	// add some random nodes to the subtrees
	for i := uint64(0); i < 42; i++ {
		_ = stp.addNode(util.SubtreeNode{Hash: chainhash.HashH([]byte(fmt.Sprintf("tx-%d", i))), Fee: i}, true)
	}

	assert.Len(t, stp.chainedSubtrees, 10)
	assert.Len(t, stp.currentSubtree.Nodes, 3)

	// check the fee in the middle node the middle subtree
	assert.Len(t, stp.chainedSubtrees[5].Nodes, 4)
	assert.Equal(t, uint64(21), stp.chainedSubtrees[5].Nodes[2].Fee)

	// remove this node
	err := stp.chainedSubtrees[5].RemoveNodeAtIndex(2)
	require.NoError(t, err)

	// check the fee in the middle node the middle subtree, should be different
	assert.Equal(t, uint64(22), stp.chainedSubtrees[5].Nodes[2].Fee)

	// chainedSubtrees[5] should have 3 nodes, instead of 4
	assert.Len(t, stp.chainedSubtrees[5].Nodes, 3)

	// Call reChainSubtrees
	err = stp.reChainSubtrees(5)
	require.NoError(t, err)

	// chainedSubtrees[5] should have 4 nodes again
	assert.Len(t, stp.chainedSubtrees[5].Nodes, 4)

	// currentSubtree should have 2 nodes
	assert.Len(t, stp.currentSubtree.Nodes, 2)

	// all chainedSubtrees should have 4 nodes
	for i := 0; i < 10; i++ {
		assert.Len(t, stp.chainedSubtrees[i].Nodes, 4)
	}
}

func TestGetMerkleProofForCoinbase(t *testing.T) {
	// block 69060
	txIDs := []string{
		"4ebd5a35e6b73a5f8e1a3621dba857239538c1b1d26364913f14c85b04e208fc",
		"1c518b6671f8d349e96c56d4e7fe831a46f398c4bb46ca7778b2152ee6ba6f27",
		"1e7aa360e3e84aff86515e66976b5b12e622c134b776242927e62de7effdc989",
		"344efe10fa4084c7f4f17c91bf3da72b9139c342aea074d75d8656a99ac3693f",
		"59814dce8ee8f9149074da4d0528fd3593b418b36b73ffafc15436646aa23c26",
		"687ea838f8b8d2924ff99859c37edd33dcd8069bfd5e92aca66734580aa29c94",
		"9f312fb2b31b6b511fabe0934a98c4d5ac4421b4bc99312f25e6c104912d9159",
		"c1d3a483ff04b90ab103d62afb3423447981d59f8e96b29022bc39c62ed9d9ab",
		"c467b87936d3ffd5b2e03a4dbde5cd66910a245b56c8cddff7eafa776ba39bbf",
		"07c09335f887a2da94efbc9730106abb1b50cc63a95b24edc9f8bb3e63c380c7",
		"1bb1b0ffdd0fa0450f900f647e713855e76e2b17683372741b6ef29575ddc99b",
		"6a613e159f1a9dbfa0321b657103f55dc28ecee201a2a43ab833c2e0c95117db",
		"fe1345fb6b3efe6225e95dc9324f6d21ddb304ad76d92381d999abec07161c7f",
		"e61cb73244eba0b774e640fef50818322842b41d89f7daa0c771b6e9fc2c6c34",
		"2a73facff0bc80e1b32d9dc6ff24fa75b711de5987eb30bbd34109bfa06de352",
		"f923a14068167a9107a0b7cd6102bfa5c0a4c8a72726a82f12e91009fd7e33be",
	}

	t.Run("merkle proof for coinbase", func(t *testing.T) {
		newSubtreeChan := make(chan NewSubtreeRequest)

		var wg sync.WaitGroup

		wg.Add(2) // we are expecting 2 subtrees

		go func() {
			for {
				// just read the subtrees of the processor
				<-newSubtreeChan
				wg.Done()
			}
		}()

		settings := test.CreateBaseTestSettings()
		settings.BlockAssembly.InitialMerkleItemsPerSubtree = 8

		stp, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, nil, nil, nil, newSubtreeChan)

		for i, txid := range txIDs {
			hash, err := chainhash.NewHashFromStr(txid)
			require.NoError(t, err)

			if i == 0 {
				stp.currentSubtree.ReplaceRootNode(hash, 0, 0)
			} else {
				stp.Add(util.SubtreeNode{Hash: *hash, Fee: 1})
			}
		}

		wg.Wait()
		assertMerkleProof(t, stp)
	})

	t.Run("merkle proof for coinbase with 4 subtrees", func(t *testing.T) {
		newSubtreeChan := make(chan NewSubtreeRequest)

		var wg sync.WaitGroup

		wg.Add(4) // we are expecting 4 subtrees

		go func() {
			for {
				// just read the subtrees of the processor
				<-newSubtreeChan
				wg.Done()
			}
		}()

		settings := test.CreateBaseTestSettings()
		settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

		stp, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, nil, nil, nil, newSubtreeChan)

		for i, txid := range txIDs {
			hash, err := chainhash.NewHashFromStr(txid)
			require.NoError(t, err)

			if i == 0 {
				stp.currentSubtree.ReplaceRootNode(hash, 0, 0)
			} else {
				stp.Add(util.SubtreeNode{Hash: *hash, Fee: 1})
			}
		}

		wg.Wait()
		assertMerkleProof(t, stp)
	})
}

// assertMerkleProof verifies merkle proof correctness in tests.
//
// Parameters:
//   - t: Testing instance
//   - stp: Subtree processor instance
func assertMerkleProof(t *testing.T, stp *SubtreeProcessor) {
	// get the merkle proof for the coinbase
	merkleProof, err := util.GetMerkleProofForCoinbase(stp.chainedSubtrees) //
	require.NoError(t, err)

	assert.Len(t, merkleProof, 4)

	// https://api.whatsonchain.com/v1/bsv/main/tx/4ebd5a35e6b73a5f8e1a3621dba857239538c1b1d26364913f14c85b04e208fc/proof
	assert.Equal(t, "1c518b6671f8d349e96c56d4e7fe831a46f398c4bb46ca7778b2152ee6ba6f27", merkleProof[0].String())
	assert.Equal(t, "3d50320a08dce920978ddbfa2fc069f4c939126ce51d9e3fd658b88f0147da02", merkleProof[1].String())
	assert.Equal(t, "f2168f2ee84f04ff5c49bf9e0043099667d48c013cf7b32dbc09ab75ef0bed68", merkleProof[2].String())
	assert.Equal(t, "a2a7873a982e3112bc98960e06971bf2b7e62a56585d324bd1ac7d9a6d79cce8", merkleProof[3].String())
}

/*
*
The moveForwardBlock method will also resize the current subtree and all the subtrees in the chain from the last one that was in the block.
*
*/
func TestMoveForwardBlock(t *testing.T) {
	n := 18
	txIds := make([]string, n)

	for i := 0; i < n; i++ {
		txid, err := generateTxID()
		if err != nil {
			t.Errorf("error generating txid: %s", err)
		}

		txIds[i] = txid
	}

	newSubtreeChan := make(chan NewSubtreeRequest)

	var wg sync.WaitGroup

	wg.Add(4) // we are expecting 4 subtrees

	go func() {
		for {
			// just read the subtrees of the processor
			<-newSubtreeChan
			wg.Done()
		}
	}()

	logger := ulogger.TestLogger{}
	subtreeStore, _ := null.New(logger)
	utxosStore := memory.New(logger)

	settings := test.CreateBaseTestSettings()
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	stp, _ := NewSubtreeProcessor(context.Background(), logger, settings, subtreeStore, nil, utxosStore, newSubtreeChan)

	for i, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		if i == 0 {
			stp.currentSubtree.ReplaceRootNode(hash, 0, 0)
		} else {
			stp.Add(util.SubtreeNode{Hash: *hash, Fee: 1})
		}
	}

	wg.Wait()

	// this is to make sure the subtrees are added to the chain
	for stp.txCount.Load() < 17 {
		time.Sleep(10 * time.Millisecond)
	}

	// there should be 4 chained subtrees
	assert.Equal(t, 4, len(stp.chainedSubtrees))
	assert.Equal(t, 4, stp.chainedSubtrees[0].Size())
	assert.Equal(t, 2, stp.currentSubtree.Length())

	stp.currentItemsPerFile = 2
	_ = stp.utxoStore.SetBlockHeight(1)
	//nolint:gosec
	_ = stp.utxoStore.SetMedianBlockTime(uint32(time.Now().Unix()))

	// moveForwardBlock saying the last subtree in the block was number 2 in the chainedSubtree slice
	// this means half the subtrees will be moveForwardBlock
	// new items per file is 2 so there should be 4 subtrees in the chain
	wg.Add(5) // we are expecting 2 more subtrees

	stp.SetCurrentBlockHeader(prevBlockHeader)
	err := stp.MoveForwardBlock(&model.Block{
		Header: blockHeader,
		Subtrees: []*chainhash.Hash{
			stp.chainedSubtrees[0].RootHash(),
			stp.chainedSubtrees[1].RootHash(),
		},
		CoinbaseTx: coinbaseTx,
	})
	require.NoError(t, err)
	// wg.Wait()

	// we added the coinbase placeholder
	assert.Equal(t, 5, len(stp.chainedSubtrees))
	assert.Equal(t, 2, stp.chainedSubtrees[0].Size())
	assert.Equal(t, 1, stp.currentSubtree.Length())
}

// TestMoveForwardBlock tests the moveForwardBlock method
// bug #1817
func TestMoveForwardBlock_LeftInQueue(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	subtreeStore := blob_memory.New()
	utxosStore := memory.New(logger)

	subtreeHash, _ := chainhash.NewHashFromStr("fd61a79793c4fb02ba14b85df98f5f60f727be359089d8fa125c4ce37945106b")

	subtreeBytes, err := os.ReadFile("./testdata/fd61a79793c4fb02ba14b85df98f5f60f727be359089d8fa125c4ce37945106b.subtree")
	require.NoError(t, err)

	err = subtreeStore.Set(ctx, subtreeHash.CloneBytes(), subtreeBytes, options.WithFileExtension("subtree"))
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings()
	tSettings.BlockAssembly.DoubleSpendWindow = 2 * time.Second
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 32

	subtreeProcessor, err := NewSubtreeProcessor(ctx, logger, tSettings, subtreeStore, nil, utxosStore, nil)
	require.NoError(t, err)

	hash, _ := chainhash.NewHashFromStr("6affcabb2013261e764a5d4286b463b11127f4fd1de05368351530ddb3f19942")
	subtreeProcessor.Add(util.SubtreeNode{Hash: *hash, Fee: 1, SizeInBytes: 294})

	// we should not have the transaction in the subtrees yet, it should be stuck in the queue
	assert.Equal(t, 1, subtreeProcessor.GetCurrentLength())
	// assert.Equal(t, subtreeHash.String(), subtreeProcessor.currentSubtree.RootHash().String())

	// Move up the block
	blockBytes, err := hex.DecodeString("000000206a21d13c3d2656557493b4652f67a763f835b86bf90107a60f412c290000000083ba48026c405d5a4b4d5aa3f10cee9de605a012e9a25f72a19aa9fe123380c689505c67c874461cc6dda18002fde501016b104579e34c5c12fad8899035be27f7605f8ff95db814ba02fbc49397a761fd01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1903af32190000000000205f7c477c327c437c5f200001000000ffffffff01e50b5402000000001976a9147a112f6a373b80b4ebb2b02acef97f35aef7494488ac00000000feaf321900")
	require.NoError(t, err)

	block, err := model.NewBlockFromBytes(blockBytes, tSettings)
	require.NoError(t, err)

	err = subtreeProcessor.MoveForwardBlock(block)
	require.NoError(t, err)

	assert.Len(t, subtreeProcessor.chainedSubtrees, 0)
	assert.Len(t, subtreeProcessor.currentSubtree.Nodes, 1)
	assert.Equal(t, *util.CoinbasePlaceholderHash, subtreeProcessor.currentSubtree.Nodes[0].Hash)
}

func TestIncompleteSubtreeMoveForwardBlock(t *testing.T) {
	n := 17
	txIds := make([]string, n)

	for i := 0; i < n; i++ {
		txid, err := generateTxID()
		if err != nil {
			t.Errorf("error generating txid: %s", err)
		}

		txIds[i] = txid
	}

	newSubtreeChan := make(chan NewSubtreeRequest)

	var wg sync.WaitGroup

	wg.Add(4) // we are expecting 4 subtrees

	go func() {
		for {
			// just read the subtrees of the processor
			<-newSubtreeChan
			wg.Done()
		}
	}()

	subtreeStore, _ := null.New(ulogger.TestLogger{})
	utxosStore := memory.New(ulogger.TestLogger{})

	settings := test.CreateBaseTestSettings()
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	stp, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, subtreeStore, nil, utxosStore, newSubtreeChan)

	for i, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		if i == 0 {
			stp.currentSubtree.ReplaceRootNode(hash, 0, 0)
		} else {
			stp.Add(util.SubtreeNode{Hash: *hash, Fee: 1})
		}
	}

	wg.Wait()

	// this is to make sure the subtrees are added to the chain
	for stp.txCount.Load() < 16 {
		time.Sleep(100 * time.Millisecond)
	}

	// there should be 4 chained subtrees
	assert.Equal(t, 4, len(stp.chainedSubtrees))
	assert.Equal(t, 4, stp.chainedSubtrees[0].Size())
	// and 1 tx in the current subtree
	assert.Equal(t, 1, stp.currentSubtree.Length())

	stp.currentItemsPerFile = 2
	_ = stp.utxoStore.SetBlockHeight(1)
	//nolint:gosec
	_ = stp.utxoStore.SetMedianBlockTime(uint32(time.Now().Unix()))

	wg.Add(5) // we are expecting 4 subtrees

	stp.SetCurrentBlockHeader(prevBlockHeader)
	// moveForwardBlock saying the last subtree in the block was number 2 in the chainedSubtree slice
	// this means half the subtrees will be moveForwardBlock
	// new items per file is 2 so there should be 5 subtrees in the chain
	err := stp.MoveForwardBlock(&model.Block{
		Header: blockHeader,
		Subtrees: []*chainhash.Hash{
			stp.chainedSubtrees[0].RootHash(),
			stp.chainedSubtrees[1].RootHash(),
		},
		CoinbaseTx: coinbaseTx,
	})
	require.NoError(t, err)

	wg.Wait()
	assert.Equal(t, 5, len(stp.chainedSubtrees))
	assert.Equal(t, 0, stp.currentSubtree.Length())
}

// current subtree should have 1 tx which due to the new added coinbase placeholder
func TestSubtreeMoveForwardBlockNewCurrent(t *testing.T) {
	n := 16
	txIds := make([]string, n)

	for i := 0; i < n; i++ {
		txid, err := generateTxID()
		if err != nil {
			t.Errorf("error generating txid: %s", err)
		}

		txIds[i] = txid
	}

	newSubtreeChan := make(chan NewSubtreeRequest)

	var wg sync.WaitGroup

	wg.Add(4) // we are expecting 4 subtrees

	go func() {
		for {
			// just read the subtrees of the processor
			<-newSubtreeChan
			wg.Done()
		}
	}()

	subtreeStore, _ := null.New(ulogger.TestLogger{})
	utxosStore := memory.New(ulogger.TestLogger{})

	settings := test.CreateBaseTestSettings()
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	stp, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, subtreeStore, nil, utxosStore, newSubtreeChan)

	for i, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		if i == 0 {
			stp.currentSubtree.ReplaceRootNode(hash, 0, 0)
		} else {
			stp.Add(util.SubtreeNode{Hash: *hash, Fee: 1})
		}
	}

	wg.Wait()
	// sleep for 1 second
	// this is to make sure the subtrees are added to the chain
	time.Sleep(1 * time.Second)

	// there should be 4 chained subtrees
	assert.Equal(t, 4, len(stp.chainedSubtrees))
	assert.Equal(t, 4, stp.chainedSubtrees[0].Size())
	// and 0 tx in the current subtree
	assert.Equal(t, 0, stp.currentSubtree.Length())

	stp.currentItemsPerFile = 2
	_ = stp.utxoStore.SetBlockHeight(1)
	//nolint:gosec
	_ = stp.utxoStore.SetMedianBlockTime(uint32(time.Now().Unix()))

	wg.Add(4) // we are expecting 4 subtrees

	stp.SetCurrentBlockHeader(prevBlockHeader)
	// moveForwardBlock saying the last subtree in the block was number 2 in the chainedSubtree slice
	// this means half the subtrees will be moveForwardBlock
	// new items per file is 2 so there should be 4 subtrees in the chain
	err := stp.MoveForwardBlock(&model.Block{
		Header: blockHeader,
		Subtrees: []*chainhash.Hash{
			stp.chainedSubtrees[0].RootHash(),
			stp.chainedSubtrees[1].RootHash(),
		},
		CoinbaseTx: coinbaseTx,
	})

	wg.Wait()
	require.NoError(t, err)
	assert.Equal(t, 4, len(stp.chainedSubtrees))
	assert.Equal(t, 1, stp.currentSubtree.Length())
}

func TestCompareMerkleProofsToSubtrees(t *testing.T) {
	coinbaseHash, _ := chainhash.NewHashFromStr("cc6da767edfd473466d70a747348eee48f649d3173f762be0f41ac3bd418e681")

	hashes := make([]*chainhash.Hash, 8)
	hashes[0], _ = chainhash.NewHashFromStr("97af9ad3583e2f83fc1e44e475e3a3ee31ec032449cc88b491479ef7d187c115")
	hashes[1], _ = chainhash.NewHashFromStr("7ce05dda56bc523048186c0f0474eb21c92fe35de6d014bd016834637a3ed08d")
	hashes[2], _ = chainhash.NewHashFromStr("3070fb937289e24720c827cbc24f3fce5c361cd7e174392a700a9f42051609e0")
	hashes[3], _ = chainhash.NewHashFromStr("d3cde0ab7142cc99acb31c5b5e1e941faed1c5cf5f8b63ed663972845d663487")
	hashes[4], _ = chainhash.NewHashFromStr("87af9ad3583e2f83fc1e44e475e3a3ee31ec032449cc88b491479ef7d187c115")
	hashes[5], _ = chainhash.NewHashFromStr("6ce05dda56bc523048186c0f0474eb21c92fe35de6d014bd016834637a3ed08d")
	hashes[6], _ = chainhash.NewHashFromStr("2070fb937289e24720c827cbc24f3fce5c361cd7e174392a700a9f42051609e0")
	hashes[7], _ = chainhash.NewHashFromStr("c3cde0ab7142cc99acb31c5b5e1e941faed1c5cf5f8b63ed663972845d663487")

	expectedMerkleRoot := "5411959ea670debee6ab3408b1a51b67f175e30fccf4fac48f55f20b1e935705"

	var wg sync.WaitGroup

	wg.Add(2)

	newSubtreeChan := make(chan NewSubtreeRequest)

	go func() {
		// just read and discard
		for {
			<-newSubtreeChan
			wg.Done()
		}
	}()

	settings := test.CreateBaseTestSettings()
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	subtreeProcessor, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, nil, nil, nil, newSubtreeChan, WithBatcherSize(1))

	for i, hash := range hashes {
		if i == 0 {
			subtreeProcessor.currentSubtree.ReplaceRootNode(hash, 0, 0)
		} else {
			subtreeProcessor.Add(util.SubtreeNode{Hash: *hash, Fee: 111})
		}
	}
	// add 1 more hash to create the second subtree
	subtreeProcessor.Add(util.SubtreeNode{Hash: *hashes[0], Fee: 111})

	wg.Wait()

	subtrees := subtreeProcessor.GetCompletedSubtreesForMiningCandidate()
	assert.Len(t, subtrees, 2)

	coinbaseMerkleProof, err := util.GetMerkleProofForCoinbase(subtrees)
	require.NoError(t, err)

	cmp := make([]string, len(coinbaseMerkleProof))

	cmpB := make([][]byte, len(coinbaseMerkleProof))

	for idx, hash := range coinbaseMerkleProof {
		cmp[idx] = hash.String()
		cmpB[idx] = hash.CloneBytes()
	}

	assert.Equal(t, []string{
		"7ce05dda56bc523048186c0f0474eb21c92fe35de6d014bd016834637a3ed08d",
		"c32db78e5f8437648888713982ea3d49628dbde0b4b48857147f793b55d26f09",
		"1e3cfb94c292e8fc2ac692c4c4db4ea73784978ff47424668233a7f491e218a3",
	}, cmp)

	merkleRootFromProofs := util.BuildMerkleRootFromCoinbase(coinbaseHash[:], cmpB)
	assert.Equal(t, expectedMerkleRoot, utils.ReverseAndHexEncodeSlice(merkleRootFromProofs))

	topTree, err := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(subtrees)))
	require.NoError(t, err)

	for idx, subtree := range subtrees {
		if idx == 0 {
			subtree.ReplaceRootNode(coinbaseHash, 0, 0)
		}

		err = topTree.AddNode(*subtree.RootHash(), 1, 0)

		require.NoError(t, err)
	}

	calculatedMerkleRoot := topTree.RootHash()
	assert.Equal(t, expectedMerkleRoot, calculatedMerkleRoot.String())
}

func TestSubtreeProcessor_getRemainderTxHashes(t *testing.T) {
	t.Run("no remainder", func(t *testing.T) {
		txIDs := []string{
			"4ebd5a35e6b73a5f8e1a3621dba857239538c1b1d26364913f14c85b04e208fc",
			"1c518b6671f8d349e96c56d4e7fe831a46f398c4bb46ca7778b2152ee6ba6f27",
			"1e7aa360e3e84aff86515e66976b5b12e622c134b776242927e62de7effdc989",
			"344efe10fa4084c7f4f17c91bf3da72b9139c342aea074d75d8656a99ac3693f",
			"59814dce8ee8f9149074da4d0528fd3593b418b36b73ffafc15436646aa23c26",
			"687ea838f8b8d2924ff99859c37edd33dcd8069bfd5e92aca66734580aa29c94",
			"9f312fb2b31b6b511fabe0934a98c4d5ac4421b4bc99312f25e6c104912d9159",
			"c1d3a483ff04b90ab103d62afb3423447981d59f8e96b29022bc39c62ed9d9ab",
			"c467b87936d3ffd5b2e03a4dbde5cd66910a245b56c8cddff7eafa776ba39bbf",
			"07c09335f887a2da94efbc9730106abb1b50cc63a95b24edc9f8bb3e63c380c7",
			"1bb1b0ffdd0fa0450f900f647e713855e76e2b17683372741b6ef29575ddc99b",
			"6a613e159f1a9dbfa0321b657103f55dc28ecee201a2a43ab833c2e0c95117db",
			"fe1345fb6b3efe6225e95dc9324f6d21ddb304ad76d92381d999abec07161c7f",
			"e61cb73244eba0b774e640fef50818322842b41d89f7daa0c771b6e9fc2c6c34",
			"2a73facff0bc80e1b32d9dc6ff24fa75b711de5987eb30bbd34109bfa06de352",
			"f923a14068167a9107a0b7cd6102bfa5c0a4c8a72726a82f12e91009fd7e33be",
		}

		newSubtreeChan := make(chan NewSubtreeRequest)

		go func() {
			// just read and discard
			for {
				<-newSubtreeChan
			}
		}()

		tSettings := test.CreateBaseTestSettings()
		tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

		subtreeProcessor, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, tSettings, nil, nil, nil, newSubtreeChan)

		hashes := make([]*chainhash.Hash, len(txIDs))

		for idx, txid := range txIDs {
			hash, _ := chainhash.NewHashFromStr(txid)
			hashes[idx] = hash
			_ = subtreeProcessor.addNode(util.SubtreeNode{Hash: *hash, Fee: 1}, false)
		}

		assert.Equal(t, 4, len(subtreeProcessor.chainedSubtrees))

		chainedSubtrees := make([]*util.Subtree, 0, len(subtreeProcessor.chainedSubtrees)+1)
		chainedSubtrees = append(chainedSubtrees, subtreeProcessor.chainedSubtrees...)
		chainedSubtrees = append(chainedSubtrees, subtreeProcessor.currentSubtree)

		transactionMap := util.NewSplitSwissMap(4)
		losingTxHashesMap := util.NewSplitSwissMap(4) // these are conflicting txs that should be removed

		var err error
		subtreeProcessor.currentSubtree, err = util.NewTree(4)
		require.NoError(t, err)

		subtreeProcessor.chainedSubtrees = make([]*util.Subtree, 0)

		_ = subtreeProcessor.currentSubtree.AddCoinbaseNode()

		err = subtreeProcessor.processRemainderTxHashes(context.Background(), chainedSubtrees, transactionMap, losingTxHashesMap, false)
		require.NoError(t, err)

		remainder := make([]util.SubtreeNode, 0)

		for _, subtree := range subtreeProcessor.chainedSubtrees {
			remainder = append(remainder, subtree.Nodes...)
		}

		remainder = append(remainder, subtreeProcessor.currentSubtree.Nodes...)

		assert.Equal(t, 17, len(remainder))

		for idx, txHash := range remainder {
			if idx == 0 {
				continue
			}

			assert.Equal(t, txIDs[idx-1], txHash.Hash.String())
		}

		_ = transactionMap.Put(*hashes[3], 0)
		_ = transactionMap.Put(*hashes[7], 0)
		_ = transactionMap.Put(*hashes[11], 0)
		_ = transactionMap.Put(*hashes[15], 0)

		expectedTxIDs := []string{
			"4ebd5a35e6b73a5f8e1a3621dba857239538c1b1d26364913f14c85b04e208fc",
			"1c518b6671f8d349e96c56d4e7fe831a46f398c4bb46ca7778b2152ee6ba6f27",
			"1e7aa360e3e84aff86515e66976b5b12e622c134b776242927e62de7effdc989",
			// "344efe10fa4084c7f4f17c91bf3da72b9139c342aea074d75d8656a99ac3693f",
			"59814dce8ee8f9149074da4d0528fd3593b418b36b73ffafc15436646aa23c26",
			"687ea838f8b8d2924ff99859c37edd33dcd8069bfd5e92aca66734580aa29c94",
			"9f312fb2b31b6b511fabe0934a98c4d5ac4421b4bc99312f25e6c104912d9159",
			// c1d3a483ff04b90ab103d62afb3423447981d59f8e96b29022bc39c62ed9d9ab",
			"c467b87936d3ffd5b2e03a4dbde5cd66910a245b56c8cddff7eafa776ba39bbf",
			"07c09335f887a2da94efbc9730106abb1b50cc63a95b24edc9f8bb3e63c380c7",
			"1bb1b0ffdd0fa0450f900f647e713855e76e2b17683372741b6ef29575ddc99b",
			// "6a613e159f1a9dbfa0321b657103f55dc28ecee201a2a43ab833c2e0c95117db",
			"fe1345fb6b3efe6225e95dc9324f6d21ddb304ad76d92381d999abec07161c7f",
			"e61cb73244eba0b774e640fef50818322842b41d89f7daa0c771b6e9fc2c6c34",
			"2a73facff0bc80e1b32d9dc6ff24fa75b711de5987eb30bbd34109bfa06de352",
			// "f923a14068167a9107a0b7cd6102bfa5c0a4c8a72726a82f12e91009fd7e33be",
		}

		subtreeProcessor.currentSubtree, err = util.NewTree(4)
		require.NoError(t, err)

		subtreeProcessor.chainedSubtrees = make([]*util.Subtree, 0)

		_ = subtreeProcessor.currentSubtree.AddCoinbaseNode()

		err = subtreeProcessor.processRemainderTxHashes(context.Background(), chainedSubtrees, transactionMap, losingTxHashesMap, false)
		require.NoError(t, err)

		remainder = make([]util.SubtreeNode, 0)
		for _, subtree := range subtreeProcessor.chainedSubtrees {
			remainder = append(remainder, subtree.Nodes...)
		}

		remainder = append(remainder, subtreeProcessor.currentSubtree.Nodes...)

		assert.Equal(t, 13, len(remainder)) // 3 removed

		for idx, txHash := range remainder {
			if idx == 0 {
				continue
			}

			assert.Equal(t, expectedTxIDs[idx-1], txHash.Hash.String())
		}
	})
}

func BenchmarkBlockAssembler_AddTx(b *testing.B) {
	newSubtreeChan := make(chan NewSubtreeRequest)

	go func() {
		for {
			<-newSubtreeChan
		}
	}()

	settings := test.CreateBaseTestSettings()
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 1024

	stp, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, nil, nil, nil, newSubtreeChan)

	txHashes := make([]*chainhash.Hash, 100_000)

	for i := 0; i < 100_000; i++ {
		txid := make([]byte, 32)
		_, _ = rand.Read(txid)
		hash, _ := chainhash.NewHash(txid)
		txHashes[i] = hash
	}

	b.ResetTimer()

	for i := 0; i < 100_000; i++ {
		stp.Add(util.SubtreeNode{Hash: *txHashes[i], Fee: 1})
	}
}

// generateTxID generates a random transaction ID (32-byte hexadecimal string) for testing.
//
// Returns:
//   - string: Generated transaction ID
//   - error: Any error encountered during generation
func generateTxID() (string, error) {
	b := make([]byte, 32)

	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", b), nil
}

// generateTxHash generates a random transaction hash for testing.
//
// Returns:
//   - chainhash.Hash: Generated transaction hash
//   - error: Any error encountered during generation
func generateTxHash() (chainhash.Hash, error) {
	b := make([]byte, 32)

	_, err := rand.Read(b)
	if err != nil {
		return chainhash.Hash{}, err
	}

	return chainhash.Hash(b), nil
}

func TestSubtreeProcessor_moveBackBlock(t *testing.T) {
	t.Run("small", func(t *testing.T) {
		n := 18
		txHashes := make([]chainhash.Hash, n)

		for i := 0; i < n; i++ {
			txHash, err := generateTxHash()
			if err != nil {
				t.Errorf("error generating txid: %s", err)
			}

			txHashes[i] = txHash
		}

		newSubtreeChan := make(chan NewSubtreeRequest)
		processingDone := make(chan struct{})

		var wg sync.WaitGroup

		wg.Add(4) // we are expecting 4 subtrees

		go func() {
			for {
				select {
				case <-newSubtreeChan:
					wg.Done()
				case <-processingDone:
					return
				}
			}
		}()

		subtreeStore := blob_memory.New()
		utxosStore := memory.New(ulogger.TestLogger{})

		settings := test.CreateBaseTestSettings()
		settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

		stp, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, subtreeStore, nil, utxosStore, newSubtreeChan)

		for _, txHash := range txHashes {
			stp.Add(util.SubtreeNode{Hash: txHash, Fee: 1})
		}

		wg.Wait()

		// this is to make sure the subtrees are added to the chain
		for stp.txCount.Load() < 17 {
			time.Sleep(100 * time.Millisecond)
		}

		// there should be 4 chained subtrees
		assert.Equal(t, 4, len(stp.chainedSubtrees))
		assert.Equal(t, 4, stp.chainedSubtrees[0].Size())
		assert.Equal(t, 3, stp.currentSubtree.Length())

		// create 2 subtrees from the previous block
		subtree1 := createSubtree(t, 4, true)
		subtreeBytes, err := subtree1.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(context.Background(), subtree1.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
		require.NoError(t, err)

		subtree2 := createSubtree(t, 4, false)
		subtreeBytes, err = subtree2.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(context.Background(), subtree2.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
		require.NoError(t, err)

		_, _ = utxosStore.Create(context.Background(), coinbaseTx, 0)

		stp.SetCurrentBlockHeader(blockHeader)

		fmt.Println("stp len", len(stp.chainedSubtrees))
		fmt.Println("current subtree len: ", stp.currentSubtree.Length())

		err = stp.moveBackBlock(context.Background(), &model.Block{
			Header: prevBlockHeader,
			Subtrees: []*chainhash.Hash{
				subtree1.RootHash(),
				subtree2.RootHash(),
			},
			CoinbaseTx: coinbaseTx,
		})
		require.NoError(t, err)

		// Wait for any background processing to complete
		time.Sleep(100 * time.Millisecond)
		close(processingDone)

		fmt.Println("stp len2", len(stp.chainedSubtrees))
		fmt.Println("current subtree len: ", stp.currentSubtree.Length())

		assert.Equal(t, 6, len(stp.chainedSubtrees))
		assert.Equal(t, 4, stp.chainedSubtrees[0].Size())
		assert.Equal(t, 2, stp.currentSubtree.Length())

		// check that the nodes from subtree1 and subtree2 are the first nodes
		for i := 0; i < 4; i++ {
			assert.Equal(t, subtree1.Nodes[i], stp.chainedSubtrees[0].Nodes[i])
		}

		for i := 0; i < 4; i++ {
			assert.Equal(t, subtree2.Nodes[i], stp.chainedSubtrees[1].Nodes[i])
		}

		// check that the remaining nodes are the same as the original nodes
		for idx, txHash := range txHashes {
			shouldBeInSubtree := 2 + idx/4
			shouldBeInNode := idx % 4

			if shouldBeInSubtree > len(stp.chainedSubtrees)-1 {
				assert.Equal(t, txHash, stp.currentSubtree.Nodes[shouldBeInNode].Hash)
			} else {
				assert.Equal(t, txHash, stp.chainedSubtrees[shouldBeInSubtree].Nodes[shouldBeInNode].Hash)
			}
		}
	})
}

func TestMoveBackBlocks(t *testing.T) {
	t.Run("multiple blocks", func(t *testing.T) {
		n := 34 // Number of transactions
		txHashes := make([]chainhash.Hash, n)

		for i := 0; i < n; i++ {
			txHash, err := generateTxHash()
			if err != nil {
				t.Errorf("error generating txid: %s", err)
			}

			txHashes[i] = txHash
		}

		newSubtreeChan := make(chan NewSubtreeRequest)

		var wg sync.WaitGroup

		wg.Add(8) // we are expecting 8 subtrees (2 blocks with 4 subtrees each)

		go func() {
			for {
				<-newSubtreeChan
				// fmt.Println("subtreee", subtreee.Subtree.Length())
				wg.Done()
			}
		}()

		subtreeStore := blob_memory.New()
		utxosStore := memory.New(ulogger.TestLogger{})

		settings := test.CreateBaseTestSettings()
		settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

		stp, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, subtreeStore, nil, utxosStore, newSubtreeChan)

		for _, txHash := range txHashes {
			stp.Add(util.SubtreeNode{Hash: txHash, Fee: 1})
		}

		wg.Wait()

		// Ensure subtrees are added to the chain
		for stp.txCount.Load() < 34 {
			time.Sleep(100 * time.Millisecond)
		}

		// there should be 8 chained subtrees
		assert.Equal(t, 8, len(stp.chainedSubtrees))
		// subtrees should be 4 in size
		assert.Equal(t, 4, stp.chainedSubtrees[0].Size())
		// current subtree should have 1 + 2 = 3 txs
		assert.Equal(t, 3, stp.currentSubtree.Length())

		// create 2 subtrees from previous blocks
		subtree1 := createSubtree(t, 4, true)
		subtreeBytes, err := subtree1.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(context.Background(), subtree1.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
		require.NoError(t, err)

		subtree2 := createSubtree(t, 4, false)
		subtreeBytes, err = subtree2.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(context.Background(), subtree2.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
		require.NoError(t, err)

		subtree3 := createSubtree(t, 4, true)
		subtreeBytes, err = subtree3.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(context.Background(), subtree3.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
		require.NoError(t, err)

		_, _ = utxosStore.Create(context.Background(), coinbaseTx, 0)
		_, _ = utxosStore.Create(context.Background(), coinbaseTx2, 0)
		_, _ = utxosStore.Create(context.Background(), coinbaseTx3, 0)

		stp.SetCurrentBlockHeader(nextBlockHeader)

		moveBackBlock1 := &model.Block{
			Header: blockHeader,
			Subtrees: []*chainhash.Hash{
				subtree1.RootHash(),
			},
			CoinbaseTx: coinbaseTx,
		}

		moveBackBlock2 := &model.Block{
			Header: prevBlockHeader,
			Subtrees: []*chainhash.Hash{
				subtree2.RootHash(),
			},
			CoinbaseTx: coinbaseTx2,
		}

		moveBackBlock3 := &model.Block{
			Header: aBlockHeader,
			Subtrees: []*chainhash.Hash{
				subtree3.RootHash(),
			},
			CoinbaseTx: coinbaseTx3,
		}

		// err = stp.moveBackBlock(context.Background(), moveBackBlock1)
		// require.NoError(t, err)

		// err = stp.moveBackBlock(context.Background(), moveBackBlock2)
		// require.NoError(t, err)

		// err = stp.moveBackBlock(context.Background(), moveBackBlock3)
		// require.NoError(t, err)

		err = stp.moveBackBlocks(context.Background(), []*model.Block{moveBackBlock1, moveBackBlock2, moveBackBlock3})
		require.NoError(t, err)

		assert.Equal(t, 11, len(stp.chainedSubtrees))
		assert.Equal(t, 4, stp.chainedSubtrees[0].Size())
		assert.Equal(t, 0, stp.currentSubtree.Length())
	})
}

// createSubtree creates a test subtree with specified parameters.
//
// Parameters:
//   - t: Testing instance
//   - length: Number of transactions in subtree
//   - createCoinbase: Whether to include coinbase transaction
//
// Returns:
//   - *util.Subtree: Created test subtree
func createSubtree(t *testing.T, length uint64, createCoinbase bool) *util.Subtree {
	//nolint:gosec
	subtree, err := util.NewTreeByLeafCount(int(length))
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

func TestSubtreeProcessor_CreateTransactionMap(t *testing.T) {
	t.Run("small", func(t *testing.T) {
		newSubtreeChan := make(chan NewSubtreeRequest)
		subtreeStore := blob_memory.New()
		utxosStore := memory.New(ulogger.TestLogger{})

		settings := test.CreateBaseTestSettings()
		stp, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, subtreeStore, nil, utxosStore, newSubtreeChan)

		subtree1 := createSubtree(t, 4, true)
		subtreeBytes, err := subtree1.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(context.Background(), subtree1.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
		require.NoError(t, err)

		subtree2 := createSubtree(t, 4, false)
		subtreeBytes, err = subtree2.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(context.Background(), subtree2.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
		require.NoError(t, err)

		block := &model.Block{
			Header: prevBlockHeader,
			Subtrees: []*chainhash.Hash{
				subtree1.RootHash(),
				subtree2.RootHash(),
			},
			CoinbaseTx: coinbaseTx,
		}

		blockSubtreesMap := make(map[chainhash.Hash]int, len(block.Subtrees))

		for idx, subtree := range block.Subtrees {
			blockSubtreesMap[*subtree] = idx
		}

		_ = blockSubtreesMap

		transactionMap, conflictingNodes, err := stp.CreateTransactionMap(context.Background(), blockSubtreesMap, len(block.Subtrees))
		require.NoError(t, err)

		_ = conflictingNodes

		assert.Equal(t, 8, transactionMap.Length())

		for i := 0; i < 4; i++ {
			assert.True(t, transactionMap.Exists(subtree1.Nodes[i].Hash))
		}

		for i := 0; i < 4; i++ {
			assert.True(t, transactionMap.Exists(subtree2.Nodes[i].Hash))
		}
	})
}

// BenchmarkAddNode tests node addition performance.
func BenchmarkAddNode(b *testing.B) {
	g, stp, txHashes := initTestAddNodeBenchmark(b)

	startTime := time.Now()

	b.ResetTimer()

	for i, txHash := range txHashes {
		stp.Add(util.SubtreeNode{Hash: txHash, Fee: uint64(i)}) // nolint:gosec
	}

	err := g.Wait()
	require.NoError(b, err)

	fmt.Printf("Time taken: %s\n", time.Since(startTime))
}

func BenchmarkAddNodeWithMap(b *testing.B) {
	g, stp, txHashes := initTestAddNodeBenchmark(b)

	_ = stp.Remove(txHashes[1000])
	_ = stp.Remove(txHashes[2000])
	_ = stp.Remove(txHashes[3000]) //nolint:gosec
	_ = stp.Remove(txHashes[4000])

	for i := 0; i < 4; i++ {
		txHash, err := generateTxHash()
		require.NoError(b, err)

		txHashes = append(txHashes, txHash)
	}

	startTime := time.Now()

	b.ResetTimer()

	for i, txHash := range txHashes {
		stp.Add(util.SubtreeNode{Hash: txHash, Fee: uint64(i)}) //nolint:gosec
	}

	err := g.Wait()
	require.NoError(b, err)

	fmt.Printf("Time taken: %s\n", time.Since(startTime))
}

// initTestAddNodeBenchmark initializes benchmark environment for AddNode testing.
//
// Parameters:
//   - t: Benchmark testing instance
//
// Returns:
//   - *errgroup.Group: Error group for concurrent operations
//   - *SubtreeProcessor: Processor instance for testing
//   - []chainhash.Hash: Test transaction hashes
func initTestAddNodeBenchmark(b *testing.B) (*errgroup.Group, *SubtreeProcessor, []chainhash.Hash) {
	newSubtreeChan := make(chan NewSubtreeRequest)
	g := errgroup.Group{}
	nrSubtreesExpected := 10
	n := 0

	g.Go(func() error {
		for {
			<-newSubtreeChan

			n++

			if n == nrSubtreesExpected {
				return nil
			}
		}
	})

	settings := test.CreateBaseTestSettings()
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 1048576
	settings.BlockAssembly.DoubleSpendWindow = 0

	stp, _ := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, nil, nil, nil, newSubtreeChan)

	nrTxs := 1_048_576
	txHashes := make([]chainhash.Hash, 10*nrTxs)

	for i := 0; i < (10*nrTxs)-1; i++ {
		txHash, err := generateTxHash()
		require.NoError(b, err)

		txHashes[i] = txHash
	}

	return &g, stp, txHashes
}

func TestSubtreeProcessor_DynamicSizeAdjustment(t *testing.T) {
	t.Run("size adjusts based on block timing", func(t *testing.T) {
		// Setup
		settings := test.CreateBaseTestSettings()
		settings.BlockAssembly.UseDynamicSubtreeSize = true
		settings.BlockAssembly.InitialMerkleItemsPerSubtree = 1024

		newSubtreeChan := make(chan NewSubtreeRequest)
		subtreeStore := blob_memory.New()
		utxosStore := memory.New(ulogger.TestLogger{})
		mockBlockchainClient := &blockchain.Mock{}

		stp, err := NewSubtreeProcessor(
			context.Background(),
			ulogger.TestLogger{},
			settings,
			subtreeStore,
			mockBlockchainClient,
			utxosStore,
			newSubtreeChan,
		)
		require.NoError(t, err)

		// Set initial block header to start timing
		fmt.Printf("DEBUG: Setting initial block header\n")
		stp.SetCurrentBlockHeader(blockHeader)
		initialSize := stp.currentItemsPerFile
		fmt.Printf("DEBUG: Initial size: %d\n", initialSize)

		// Create multiple blocks to establish a pattern of fast subtree creation
		startTime := time.Now()

		for i := 0; i < 3; i++ {
			// Reset block intervals at start of each block
			if i == 0 {
				stp.blockIntervals = make([]time.Duration, 0)
			}

			// Set block start time
			blockStartTime := startTime.Add(time.Duration(i) * 2 * time.Second)
			fmt.Printf("DEBUG: Block %d start time: %v\n", i, blockStartTime)
			stp.blockStartTime = blockStartTime

			// Create subtrees in this block
			for j := 0; j < 5; j++ {
				txHash, err := generateTxHash()
				require.NoError(t, err)

				node := util.SubtreeNode{
					Hash: txHash,
				}

				err = stp.addNode(node, true)
				require.NoError(t, err)
			}

			// Record that we created 5 subtrees in 2 seconds = 400ms per subtree
			stp.subtreesInBlock = 5
			interval := time.Duration(2) * time.Second / time.Duration(5) // 2s/5 subtrees = 400ms per subtree
			stp.blockIntervals = append(stp.blockIntervals, interval)
			fmt.Printf("DEBUG: Block %d end, subtrees=%d, duration=%v, interval=%v, intervals=%v\n",
				i, stp.subtreesInBlock, time.Duration(2)*time.Second, interval, stp.blockIntervals)

			// Move to next block with simulated time passage
			// Each block takes 2 seconds and has 5 subtrees = 2.5 subtrees/sec
			newHeader := &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  blockHeader.Hash(),
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      blockHeader.Timestamp + uint32(i+1)*2, //nolint:gosec
				Bits:           model.NBit{},
				Nonce:          1234,
			}

			// Set the new header after recording intervals
			stp.SetCurrentBlockHeader(newHeader)
			blockHeader = newHeader
		}

		// Since we're creating subtrees 2.5x faster than target (2.5/sec vs 1/sec),
		// expect size to increase
		newSize := stp.currentItemsPerFile
		fmt.Printf("DEBUG: Final size: initial=%d, final=%d\n", initialSize, newSize)
		assert.Greater(t, newSize, initialSize, "subtree size should increase when creating too quickly")
		assert.Equal(t, 0, newSize&(newSize-1), "new size should be power of 2")
		assert.GreaterOrEqual(t, newSize, 1024, "new size should not be smaller than 1024")
	})
}

func TestSubtreeProcessor_DynamicSizeAdjustmentFast(t *testing.T) {
	t.Run("size increases when creating subtrees too quickly", func(t *testing.T) {
		// Setup
		settings := test.CreateBaseTestSettings()
		settings.BlockAssembly.UseDynamicSubtreeSize = true
		settings.BlockAssembly.InitialMerkleItemsPerSubtree = 1024

		newSubtreeChan := make(chan NewSubtreeRequest)
		subtreeStore := blob_memory.New()
		utxosStore := memory.New(ulogger.TestLogger{})
		mockBlockchainClient := &blockchain.Mock{}

		stp, err := NewSubtreeProcessor(
			context.Background(),
			ulogger.TestLogger{},
			settings,
			subtreeStore,
			mockBlockchainClient,
			utxosStore,
			newSubtreeChan,
		)
		require.NoError(t, err)

		// Set initial block header to start timing
		fmt.Printf("DEBUG: Setting initial block header\n")
		stp.SetCurrentBlockHeader(blockHeader)
		initialSize := stp.currentItemsPerFile
		fmt.Printf("DEBUG: Initial size: %d\n", initialSize)

		// Create multiple blocks to establish a pattern of fast subtree creation
		startTime := time.Now()

		for i := 0; i < 3; i++ {
			// Reset block intervals at start of each block
			if i == 0 {
				stp.blockIntervals = make([]time.Duration, 0)
			}

			// Set block start time
			blockStartTime := startTime.Add(time.Duration(i) * 2 * time.Second)
			fmt.Printf("DEBUG: Block %d start time: %v\n", i, blockStartTime)
			stp.blockStartTime = blockStartTime

			// Create subtrees in this block
			for j := 0; j < 5; j++ {
				txHash, err := generateTxHash()
				require.NoError(t, err)

				node := util.SubtreeNode{
					Hash: txHash,
				}

				err = stp.addNode(node, true)
				require.NoError(t, err)
			}

			// Move to next block with simulated time passage
			// Each block takes 2 seconds and has 5 subtrees = 2.5 subtrees/sec
			newHeader := &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  blockHeader.Hash(),
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      blockHeader.Timestamp + uint32(i+1)*2, //nolint:gosec
				Bits:           model.NBit{},
				Nonce:          1234,
			}

			fmt.Printf("DEBUG: Block %d end, subtrees=%d, duration=%v\n", i, stp.subtreesInBlock, time.Duration(2)*time.Second)
			stp.subtreesInBlock = 5                                                                        // We created 5 subtrees in this block
			stp.blockIntervals = append(stp.blockIntervals, time.Duration(2)*time.Second/time.Duration(5)) // 2s/5 subtrees = 400ms per subtree
			fmt.Printf("DEBUG: Block intervals after block %d: %v\n", i, stp.blockIntervals)
			stp.SetCurrentBlockHeader(newHeader)
			blockHeader = newHeader
		}

		// Since we're creating subtrees 2.5x faster than target (2.5/sec vs 1/sec),
		// expect size to increase
		newSize := stp.currentItemsPerFile
		fmt.Printf("DEBUG: Final size: initial=%d, final=%d\n", initialSize, newSize)
		assert.Greater(t, newSize, initialSize, "subtree size should increase when creating too quickly")
		assert.Equal(t, 0, newSize&(newSize-1), "new size should be power of 2")
		assert.GreaterOrEqual(t, newSize, 1024, "new size should not be smaller than 1024")
		fmt.Printf("DEBUG: Final size: initial=%d, final=%d\n", initialSize, newSize)
	})
}
