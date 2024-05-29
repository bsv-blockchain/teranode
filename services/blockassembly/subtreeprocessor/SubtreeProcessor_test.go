package subtreeprocessor

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/bitcoin-sv/ubsv/model"
	blob_memory "github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/blob/null"
	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// Fill the array with 0xFF
	coinbaseHash, _ = chainhash.NewHashFromStr("8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87")
	coinbaseTx, _   = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1a03a403002f746572616e6f64652f9f9fba46d5a08a6be11ddb2dffffffff0a0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac00000000")

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

	txIds = []string{
		"fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4",
		"6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4",
		"e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d",
	}
)

func TestRotate(t *testing.T) {
	_ = os.Setenv("initial_merkle_items_per_subtree", "4")

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

	stp := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, nil, nil, newSubtreeChan)

	for _, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		stp.Add(util.SubtreeNode{Hash: *hash, Fee: 1})
	}

	<-endTestChan

	assert.Equal(t, 0, stp.currentSubtree.Length())
	assert.Equal(t, 1, len(stp.chainedSubtrees))

	// Add one more txid to trigger the rotate
	//hash, err := chainhash.NewHashFromStr("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	//require.NoError(t, err)

	// TODO there is a race condition here (only in the test) that needs to be fixed, but don't want to change the code
	// in the subtree processor, because this is a performance issue
	//stp.Add(util.SubtreeNode{Hash: *hash, Fee: 1})
	//time.Sleep(100 * time.Millisecond)
	//assert.Equal(t, 1, stp.currentSubtree.Length())

	// Still 1 because the tree is not yet complete
	assert.Equal(t, 1, len(stp.chainedSubtrees))
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

		_ = os.Setenv("initial_merkle_items_per_subtree", "8")
		stp := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, nil, nil, newSubtreeChan)
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

		_ = os.Setenv("initial_merkle_items_per_subtree", "4")
		stp := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, nil, nil, newSubtreeChan)
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
The moveUpBlock method will also resize the current subtree and all the subtrees in the chain from the last one that was in the block.
*
*/
func TestMoveUpBlock(t *testing.T) {

	n := 18
	txIds := make([]string, n)

	for i := 0; i < n; i++ {
		txid, err := generateTxID()
		if err != nil {
			t.Errorf("error generating txid: %s", err)
		}

		txIds[i] = txid
	}

	_ = os.Setenv("initial_merkle_items_per_subtree", "4")
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

	stp := NewSubtreeProcessor(context.Background(), logger, subtreeStore, utxosStore, newSubtreeChan)
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
		time.Sleep(100 * time.Millisecond)
	}

	// there should be 4 chained subtrees
	assert.Equal(t, 4, len(stp.chainedSubtrees))
	assert.Equal(t, 4, stp.chainedSubtrees[0].Size())
	assert.Equal(t, 2, stp.currentSubtree.Length())

	stp.currentItemsPerFile = 2

	// moveUpBlock saying the last subtree in the block was number 2 in the chainedSubtree slice
	// this means half the subtrees will be moveUpBlock
	// new items per file is 2 so there should be 4 subtrees in the chain
	wg.Add(5) // we are expecting 2 more subtrees

	stp.SetCurrentBlockHeader(prevBlockHeader)
	err := stp.MoveUpBlock(&model.Block{
		Header: blockHeader,
		Subtrees: []*chainhash.Hash{
			stp.chainedSubtrees[0].RootHash(),
			stp.chainedSubtrees[1].RootHash(),
		},
		CoinbaseTx: coinbaseTx,
	})
	require.NoError(t, err)
	//wg.Wait()

	// we added the coinbase placeholder
	assert.Equal(t, 5, len(stp.chainedSubtrees))
	assert.Equal(t, 2, stp.chainedSubtrees[0].Size())
	assert.Equal(t, 1, stp.currentSubtree.Length())
}

func TestIncompleteSubtreeMoveUpBlock(t *testing.T) {

	n := 17
	txIds := make([]string, n)

	for i := 0; i < n; i++ {
		txid, err := generateTxID()
		if err != nil {
			t.Errorf("error generating txid: %s", err)
		}

		txIds[i] = txid
	}

	_ = os.Setenv("initial_merkle_items_per_subtree", "4")
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

	stp := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, subtreeStore, utxosStore, newSubtreeChan)
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

	wg.Add(5) // we are expecting 4 subtrees

	stp.SetCurrentBlockHeader(prevBlockHeader)
	// moveUpBlock saying the last subtree in the block was number 2 in the chainedSubtree slice
	// this means half the subtrees will be moveUpBlock
	// new items per file is 2 so there should be 5 subtrees in the chain
	err := stp.MoveUpBlock(&model.Block{
		Header: blockHeader,
		Subtrees: []*chainhash.Hash{
			stp.chainedSubtrees[0].RootHash(),
			stp.chainedSubtrees[1].RootHash(),
		},
		CoinbaseTx: coinbaseTx,
	})
	wg.Wait()
	require.NoError(t, err)
	assert.Equal(t, 5, len(stp.chainedSubtrees))
	assert.Equal(t, 0, stp.currentSubtree.Length())
}

// current subtree should have 1 tx which due to the new added coinbase placeholder
func TestSubtreeMoveUpBlockNewCurrent(t *testing.T) {

	n := 16
	txIds := make([]string, n)

	for i := 0; i < n; i++ {
		txid, err := generateTxID()
		if err != nil {
			t.Errorf("error generating txid: %s", err)
		}

		txIds[i] = txid
	}

	_ = os.Setenv("initial_merkle_items_per_subtree", "4")
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

	stp := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, subtreeStore, utxosStore, newSubtreeChan)
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

	wg.Add(4) // we are expecting 4 subtrees

	stp.SetCurrentBlockHeader(prevBlockHeader)
	// moveUpBlock saying the last subtree in the block was number 2 in the chainedSubtree slice
	// this means half the subtrees will be moveUpBlock
	// new items per file is 2 so there should be 4 subtrees in the chain
	err := stp.MoveUpBlock(&model.Block{
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

func TestMoveUpBlockLarge(t *testing.T) {
	util.SkipVeryLongTests(t)

	n := 1049576
	txIds := make([]string, n)

	for i := 0; i < n; i++ {
		txid, err := generateTxID()
		if err != nil {
			t.Errorf("error generating txid: %s", err)
		}

		txIds[i] = txid
	}

	_ = os.Setenv("initial_merkle_items_per_subtree", "262144")
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

	stp := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, subtreeStore, utxosStore, newSubtreeChan)
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
	// one of the subtrees should contain 262144 items
	assert.Equal(t, 262144, stp.chainedSubtrees[0].Size())
	// there should be no remaining items in the current subtree
	assert.Equal(t, 1000, stp.currentSubtree.Length())

	stp.currentItemsPerFile = 65536

	wg.Add(8) // we are expecting 4 subtrees

	stp.SetCurrentBlockHeader(prevBlockHeader)

	timeStart := time.Now()

	// moveUpBlock saying the last subtree in the block was number 2 in the chainedSubtree slice
	// this means half the subtrees will be moveUpBlock
	// new items per file is 65536 so there should be 8 subtrees in the chain
	err := stp.MoveUpBlock(&model.Block{
		Header: blockHeader,
		Subtrees: []*chainhash.Hash{
			stp.chainedSubtrees[0].RootHash(),
			stp.chainedSubtrees[1].RootHash(),
		},
		CoinbaseTx: coinbaseTx,
	})
	wg.Wait()
	fmt.Printf("moveUpBlock took %s\n", time.Since(timeStart))

	time.Sleep(1 * time.Second)

	require.NoError(t, err)
	assert.Equal(t, 8, len(stp.chainedSubtrees))
	// one of the subtrees should contain 262144 items
	assert.Equal(t, 65536, stp.chainedSubtrees[0].Size())
	assert.Equal(t, 1001, stp.currentSubtree.Length())
}

func TestCompareMerkleProofsToSubtrees(t *testing.T) {
	_ = os.Setenv("initial_merkle_items_per_subtree", "4")

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

	subtreeProcessor := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, nil, nil, newSubtreeChan, WithBatcherSize(1))
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

func Test_txIDAndFeeBatch(t *testing.T) {
	util.SkipVeryLongTests(t)

	batcher := newTxIDAndFeeBatch(1000)
	var wg sync.WaitGroup
	batchCount := atomic.Uint64{}
	for i := 0; i < 10_000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1_000; j++ {
				batch := batcher.add(&txIDAndFee{
					node: util.SubtreeNode{
						Hash:        chainhash.Hash{},
						Fee:         1,
						SizeInBytes: 2,
					},
				})
				if batch != nil {
					batchCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, uint64(10_000), batchCount.Load())
}

func TestSubtreeProcessor_getRemainderTxHashes(t *testing.T) {
	t.Run("no remainder", func(t *testing.T) {
		_ = os.Setenv("initial_merkle_items_per_subtree", "4")

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
		subtreeProcessor := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, nil, nil, newSubtreeChan)

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

		var err error
		subtreeProcessor.currentSubtree, err = util.NewTree(4)
		require.NoError(t, err)
		subtreeProcessor.chainedSubtrees = make([]*util.Subtree, 0)
		_ = subtreeProcessor.currentSubtree.AddNode(model.CoinbasePlaceholder, 0, 0)

		err = subtreeProcessor.processRemainderTxHashes(context.Background(), chainedSubtrees, transactionMap, false)
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
			//"344efe10fa4084c7f4f17c91bf3da72b9139c342aea074d75d8656a99ac3693f",
			"59814dce8ee8f9149074da4d0528fd3593b418b36b73ffafc15436646aa23c26",
			"687ea838f8b8d2924ff99859c37edd33dcd8069bfd5e92aca66734580aa29c94",
			"9f312fb2b31b6b511fabe0934a98c4d5ac4421b4bc99312f25e6c104912d9159",
			//"c1d3a483ff04b90ab103d62afb3423447981d59f8e96b29022bc39c62ed9d9ab",
			"c467b87936d3ffd5b2e03a4dbde5cd66910a245b56c8cddff7eafa776ba39bbf",
			"07c09335f887a2da94efbc9730106abb1b50cc63a95b24edc9f8bb3e63c380c7",
			"1bb1b0ffdd0fa0450f900f647e713855e76e2b17683372741b6ef29575ddc99b",
			//"6a613e159f1a9dbfa0321b657103f55dc28ecee201a2a43ab833c2e0c95117db",
			"fe1345fb6b3efe6225e95dc9324f6d21ddb304ad76d92381d999abec07161c7f",
			"e61cb73244eba0b774e640fef50818322842b41d89f7daa0c771b6e9fc2c6c34",
			"2a73facff0bc80e1b32d9dc6ff24fa75b711de5987eb30bbd34109bfa06de352",
			//"f923a14068167a9107a0b7cd6102bfa5c0a4c8a72726a82f12e91009fd7e33be",
		}

		subtreeProcessor.currentSubtree, err = util.NewTree(4)
		require.NoError(t, err)
		subtreeProcessor.chainedSubtrees = make([]*util.Subtree, 0)
		_ = subtreeProcessor.currentSubtree.AddNode(model.CoinbasePlaceholder, 0, 0)

		err = subtreeProcessor.processRemainderTxHashes(context.Background(), chainedSubtrees, transactionMap, false)
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
	_ = os.Setenv("initial_merkle_items_per_subtree", "1024")

	newSubtreeChan := make(chan NewSubtreeRequest)
	go func() {
		for {
			<-newSubtreeChan
		}
	}()

	stp := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, nil, nil, newSubtreeChan)

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

// generateTxID generates a random 32-byte hexadecimal string.
func generateTxID() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", b), nil
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

func TestSubtreeProcessor_moveDownBlock(t *testing.T) {
	t.Run("small", func(t *testing.T) {
		_ = os.Setenv("initial_merkle_items_per_subtree", "4")

		n := 18
		txHashes := make([]chainhash.Hash, n)

		for i := 0; i < n; i++ {
			txHash, err := generateTxHash()
			if err != nil {
				t.Errorf("error generating txid: %s", err)
			}

			txHashes[i] = txHash
			//fmt.Printf("created txHash: %s\n", txHash.String())
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

		subtreeStore := blob_memory.New()
		utxosStore := memory.New(ulogger.TestLogger{})

		stp := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, subtreeStore, utxosStore, newSubtreeChan)
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
		err = subtreeStore.Set(context.Background(), subtree1.RootHash()[:], subtreeBytes)
		require.NoError(t, err)

		subtree2 := createSubtree(t, 4, false)
		subtreeBytes, err = subtree2.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(context.Background(), subtree2.RootHash()[:], subtreeBytes)
		require.NoError(t, err)

		_, _ = utxosStore.Create(context.Background(), coinbaseTx)

		stp.SetCurrentBlockHeader(blockHeader)
		err = stp.moveDownBlock(context.Background(), &model.Block{
			Header: prevBlockHeader,
			Subtrees: []*chainhash.Hash{
				subtree1.RootHash(),
				subtree2.RootHash(),
			},
			CoinbaseTx: coinbaseTx,
		})
		require.NoError(t, err)

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

func createSubtree(t *testing.T, length uint64, createCoinbase bool) *util.Subtree {
	subtree, err := util.NewTreeByLeafCount(int(length))
	require.NoError(t, err)
	start := uint64(0)
	if createCoinbase {
		err = subtree.AddNode(*model.CoinbasePlaceholderHash, 0, 0)
		require.NoError(t, err)
		start = 1
	}
	for i := start; i < length; i++ {
		txHash, err := generateTxHash()
		require.NoError(t, err)
		err = subtree.AddNode(txHash, i, i)
		require.NoError(t, err)
		//fmt.Printf("created subtree1 txHash: %s\n", txHash.String())
	}

	return subtree
}

func TestSubtreeProcessor_createTransactionMap(t *testing.T) {
	t.Run("small", func(t *testing.T) {
		newSubtreeChan := make(chan NewSubtreeRequest)
		subtreeStore := blob_memory.New()
		utxosStore := memory.New(ulogger.TestLogger{})

		stp := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, subtreeStore, utxosStore, newSubtreeChan)

		subtree1 := createSubtree(t, 4, true)
		subtreeBytes, err := subtree1.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(context.Background(), subtree1.RootHash()[:], subtreeBytes)
		require.NoError(t, err)

		subtree2 := createSubtree(t, 4, false)
		subtreeBytes, err = subtree2.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(context.Background(), subtree2.RootHash()[:], subtreeBytes)
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

		transactionMap, err := stp.createTransactionMap(context.Background(), blockSubtreesMap)
		require.NoError(t, err)

		assert.Equal(t, 8, transactionMap.Length())
		for i := 0; i < 4; i++ {
			assert.True(t, transactionMap.Exists(subtree1.Nodes[i].Hash))
		}
		for i := 0; i < 4; i++ {
			assert.True(t, transactionMap.Exists(subtree2.Nodes[i].Hash))
		}
	})

	t.Run("large", func(t *testing.T) {
		util.SkipVeryLongTests(t)

		newSubtreeChan := make(chan NewSubtreeRequest)
		subtreeStore := blob_memory.New()
		utxosStore := memory.New(ulogger.TestLogger{})

		stp := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, subtreeStore, utxosStore, newSubtreeChan)

		subtreeSize := uint64(1024 * 1024)
		nrSubtrees := 10

		subtrees := make([]*util.Subtree, nrSubtrees)

		block := &model.Block{
			Header:     prevBlockHeader,
			Subtrees:   []*chainhash.Hash{},
			CoinbaseTx: coinbaseTx,
		}
		for i := 0; i < nrSubtrees; i++ {
			subtree := createSubtree(t, subtreeSize, i == 0)
			subtreeBytes, err := subtree.Serialize()
			require.NoError(t, err)
			err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes)
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

		transactionMap, err := stp.createTransactionMap(context.Background(), blockSubtreesMap)
		require.NoError(t, err)

		pprof.StopCPUProfile()
		t.Logf("Time taken: %s\n", time.Since(start))

		f, _ = os.Create("mem.prof")
		defer f.Close()
		_ = pprof.WriteHeapProfile(f)

		assert.Equal(t, int(subtreeSize)*nrSubtrees, transactionMap.Length())

		start = time.Now()
		var wg sync.WaitGroup
		for _, subtree := range subtrees {
			wg.Add(1)
			go func(subtree *util.Subtree) {
				defer wg.Done()
				for i := 0; i < int(subtreeSize); i++ {
					assert.True(t, transactionMap.Exists(subtree.Nodes[i].Hash))
				}
			}(subtree)
		}
		wg.Wait()
		t.Logf("Time taken to read: %s\n", time.Since(start))
	})
}

func Test_AddNode_Benchmark(t *testing.T) {
	util.SkipVeryLongTests(t)

	g, stp, txHashes := initTestAddNodeBenchmark(t)

	startTime := time.Now()

	for i, txHash := range txHashes {
		stp.Add(util.SubtreeNode{Hash: txHash, Fee: uint64(i)})
	}

	err := g.Wait()
	require.NoError(t, err)

	fmt.Printf("Time taken: %s\n", time.Since(startTime))
}

func Test_AddNodeWithMap_Benchmark(t *testing.T) {
	util.SkipVeryLongTests(t)

	g, stp, txHashes := initTestAddNodeBenchmark(t)

	_ = stp.Remove(txHashes[1000])
	_ = stp.Remove(txHashes[2000])
	_ = stp.Remove(txHashes[3000])
	_ = stp.Remove(txHashes[4000])

	for i := 0; i < 4; i++ {
		txHash, err := generateTxHash()
		require.NoError(t, err)
		txHashes = append(txHashes, txHash)
	}

	startTime := time.Now()

	for i, txHash := range txHashes {
		stp.Add(util.SubtreeNode{Hash: txHash, Fee: uint64(i)})
	}

	err := g.Wait()
	require.NoError(t, err)

	fmt.Printf("Time taken: %s\n", time.Since(startTime))
}

func Test_DeserializeHashesFromReaderIntoBuckets(t *testing.T) {
	util.SkipLongTests(t)

	size := 1024 * 1024
	subtreeBytes := generateLargeSubtreeBytes(t, size)
	r := bytes.NewReader(subtreeBytes)

	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	buckets, err := DeserializeHashesFromReaderIntoBuckets(r, 16)
	require.NoError(t, err)

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)

	assert.Equal(t, 16, len(buckets))
}

func generateLargeSubtreeBytes(t *testing.T, size int) []byte {
	st, err := util.NewIncompleteTreeByLeafCount(size)
	require.NoError(t, err)

	var bb [32]byte
	for i := 0; i < size; i++ {
		// int to bytes
		binary.LittleEndian.PutUint32(bb[:], uint32(i))
		_ = st.AddNode(bb, uint64(i), uint64(i))
	}

	ser, err := st.Serialize()
	require.NoError(t, err)

	return ser
}

func initTestAddNodeBenchmark(t *testing.T) (*errgroup.Group, *SubtreeProcessor, []chainhash.Hash) {
	_ = os.Setenv("initial_merkle_items_per_subtree", "1048576")
	_ = os.Setenv("double_spend_window_millis", "0")

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

	stp := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, nil, nil, newSubtreeChan)

	nrTxs := 1_048_576
	txHashes := make([]chainhash.Hash, 10*nrTxs)
	for i := 0; i < (10*nrTxs)-1; i++ {
		txHash, err := generateTxHash()
		require.NoError(t, err)
		txHashes[i] = txHash
	}

	return &g, stp, txHashes
}
