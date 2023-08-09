package subtreeprocessor

import (
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/stores/blob/null"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-p2p"
	"github.com/ordishs/go-utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (

	// Fill the array with 0xFF
	coinbaseHash, _ = chainhash.NewHashFromStr("8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87")

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

	newSubtreeChan := make(chan *util.Subtree)
	endTestChan := make(chan bool)

	go func() {
		for {
			subtree := <-newSubtreeChan
			assert.Equal(t, 4, subtree.Length())
			assert.Equal(t, uint64(3), subtree.Fees)

			// Test the merkle root with the coinbase placeholder
			merkleRoot := subtree.RootHash()
			assert.Equal(t, "fd8e7ab196c23534961ef2e792e13426844f831e83b856aa99998ab9908d854f", merkleRoot.String())

			// Test the merkle root with the coinbase placeholder replaced
			merkleRoot = subtree.ReplaceRootNode(coinbaseHash, 0)
			assert.Equal(t, "f3e94742aca4b5ef85488dc37c06c3282295ffec960994b2c0d5ac2a25a95766", merkleRoot.String())

			endTestChan <- true
		}
	}()

	stp := NewSubtreeProcessor(p2p.TestLogger{}, nil, newSubtreeChan)

	waitCh := make(chan struct{})
	defer close(waitCh)

	for _, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		stp.Add(*hash, 1, waitCh)
		<-waitCh
	}

	assert.Equal(t, 0, stp.currentSubtree.Length())

	assert.Equal(t, 1, len(stp.chainedSubtrees))

	// Add one more txid to trigger the rotate
	hash, err := chainhash.NewHashFromStr("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	require.NoError(t, err)

	stp.Add(*hash, 1, waitCh)
	<-waitCh

	assert.Equal(t, 1, stp.currentSubtree.Length())

	// Still 1 because the tree is not yet complete
	assert.Equal(t, 1, len(stp.chainedSubtrees))

	<-endTestChan
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
		newSubtreeChan := make(chan *util.Subtree)
		var wg sync.WaitGroup
		wg.Add(2) // we are expecting 2 subtrees
		go func() {
			for {
				// just read the subtrees of the processor
				<-newSubtreeChan
				wg.Done()
			}
		}()

		waitCh := make(chan struct{})
		defer close(waitCh)

		_ = os.Setenv("initial_merkle_items_per_subtree", "8")
		stp := NewSubtreeProcessor(p2p.TestLogger{}, nil, newSubtreeChan)
		for i, txid := range txIDs {
			hash, err := chainhash.NewHashFromStr(txid)
			require.NoError(t, err)

			if i == 0 {
				stp.currentSubtree.ReplaceRootNode(hash, 0)
			} else {
				stp.Add(*hash, 1, waitCh)
				<-waitCh
			}
		}
		wg.Wait()
		assertMerkleProof(t, stp)
	})

	t.Run("merkle proof for coinbase with 4 subtrees", func(t *testing.T) {
		newSubtreeChan := make(chan *util.Subtree)
		var wg sync.WaitGroup
		wg.Add(4) // we are expecting 4 subtrees
		go func() {
			for {
				// just read the subtrees of the processor
				<-newSubtreeChan
				wg.Done()
			}
		}()

		waitCh := make(chan struct{})
		defer close(waitCh)

		_ = os.Setenv("initial_merkle_items_per_subtree", "4")
		stp := NewSubtreeProcessor(p2p.TestLogger{}, nil, newSubtreeChan)
		for i, txid := range txIDs {
			hash, err := chainhash.NewHashFromStr(txid)
			require.NoError(t, err)

			if i == 0 {
				stp.currentSubtree.ReplaceRootNode(hash, 0)
			} else {
				stp.Add(*hash, 1, waitCh)
				<-waitCh
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
	newSubtreeChan := make(chan *util.Subtree)
	var wg sync.WaitGroup
	wg.Add(4) // we are expecting 4 subtrees
	go func() {
		for {
			// just read the subtrees of the processor
			<-newSubtreeChan
			wg.Done()
		}
	}()

	waitCh := make(chan struct{})
	defer close(waitCh)

	subtreeStore, _ := null.New()

	stp := NewSubtreeProcessor(p2p.TestLogger{}, subtreeStore, newSubtreeChan)
	for i, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		if i == 0 {
			stp.currentSubtree.ReplaceRootNode(hash, 0)
		} else {
			stp.Add(*hash, 1, waitCh)
			<-waitCh
		}
	}
	wg.Wait()
	// sleep for 1 second
	// this is to make sure the subtrees are added to the chain
	time.Sleep(1 * time.Second)

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
	newSubtreeChan := make(chan *util.Subtree)
	var wg sync.WaitGroup
	wg.Add(4) // we are expecting 4 subtrees
	go func() {
		for {
			// just read the subtrees of the processor
			<-newSubtreeChan
			wg.Done()
		}
	}()

	waitCh := make(chan struct{})
	defer close(waitCh)

	stp := NewSubtreeProcessor(p2p.TestLogger{}, nil, newSubtreeChan)
	for i, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		if i == 0 {
			stp.currentSubtree.ReplaceRootNode(hash, 0)
		} else {
			stp.Add(*hash, 1, waitCh)
			<-waitCh
		}
	}
	wg.Wait()
	// sleep for 1 second
	// this is to make sure the subtrees are added to the chain
	time.Sleep(1 * time.Second)

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
	newSubtreeChan := make(chan *util.Subtree)
	var wg sync.WaitGroup
	wg.Add(4) // we are expecting 4 subtrees
	go func() {
		for {
			// just read the subtrees of the processor
			<-newSubtreeChan
			wg.Done()
		}
	}()

	waitCh := make(chan struct{})
	defer close(waitCh)

	stp := NewSubtreeProcessor(p2p.TestLogger{}, nil, newSubtreeChan)
	for i, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		if i == 0 {
			stp.currentSubtree.ReplaceRootNode(hash, 0)
		} else {
			stp.Add(*hash, 1, waitCh)
			<-waitCh
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
	})
	wg.Wait()
	require.NoError(t, err)
	assert.Equal(t, 4, len(stp.chainedSubtrees))
	assert.Equal(t, 1, stp.currentSubtree.Length())
}

func TestMoveUpBlockLarge(t *testing.T) {
	util.SkipLongTests(t)

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
	newSubtreeChan := make(chan *util.Subtree)
	var wg sync.WaitGroup
	wg.Add(4) // we are expecting 4 subtrees
	go func() {
		for {
			// just read the subtrees of the processor
			<-newSubtreeChan
			wg.Done()
		}
	}()

	waitCh := make(chan struct{})
	defer close(waitCh)

	stp := NewSubtreeProcessor(p2p.TestLogger{}, nil, newSubtreeChan)
	for i, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		if i == 0 {
			stp.currentSubtree.ReplaceRootNode(hash, 0)
		} else {
			stp.Add(*hash, 1, waitCh)
			<-waitCh
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
	// moveUpBlock saying the last subtree in the block was number 2 in the chainedSubtree slice
	// this means half the subtrees will be moveUpBlock
	// new items per file is 65536 so there should be 8 subtrees in the chain
	err := stp.MoveUpBlock(&model.Block{
		Header: blockHeader,
		Subtrees: []*chainhash.Hash{
			stp.chainedSubtrees[0].RootHash(),
			stp.chainedSubtrees[1].RootHash(),
		},
	})
	wg.Wait()
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
	newSubtreeChan := make(chan *util.Subtree)
	go func() {
		// just read and discard
		for {
			<-newSubtreeChan
			wg.Done()
		}
	}()

	subtreeProcessor := NewSubtreeProcessor(p2p.TestLogger{}, nil, newSubtreeChan)
	for i, hash := range hashes {
		if i == 0 {
			subtreeProcessor.currentSubtree.ReplaceRootNode(hash, 0)
		} else {
			subtreeProcessor.Add(*hash, 111)
		}
	}
	// add 1 more hash to create the second subtree
	subtreeProcessor.Add(*hashes[0], 111)

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

	topTree := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(subtrees)))
	for idx, subtree := range subtrees {
		if idx == 0 {
			subtree.ReplaceRootNode(coinbaseHash, 0)
		}
		err = topTree.AddNode(subtree.RootHash(), 1)
		require.NoError(t, err)
	}

	calculatedMerkleRoot := topTree.RootHash()
	assert.Equal(t, expectedMerkleRoot, calculatedMerkleRoot.String())

}

func BenchmarkBlockAssembler_AddTx(b *testing.B) {
	_ = os.Setenv("initial_merkle_items_per_subtree", "1024")

	newSubtreeChan := make(chan *util.Subtree)
	go func() {
		for {
			_ = <-newSubtreeChan
		}
	}()

	stp := NewSubtreeProcessor(p2p.TestLogger{}, nil, newSubtreeChan)

	txHashes := make([]*chainhash.Hash, 100_000)
	for i := 0; i < 100_000; i++ {
		txid := make([]byte, 32)
		_, _ = rand.Read(txid)
		hash, _ := chainhash.NewHash(txid)
		txHashes[i] = hash
	}

	b.ResetTimer()

	for i := 0; i < 100_000; i++ {
		stp.Add(*txHashes[i], 1, nil)
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
