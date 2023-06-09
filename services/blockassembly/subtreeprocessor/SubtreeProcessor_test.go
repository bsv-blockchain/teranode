package subtreeprocessor

import (
	"os"
	"testing"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (

	// Fill the array with 0xFF
	coinbaseHash, _ = chainhash.NewHashFromStr("8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87")

	txIds []string = []string{
		"fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4",
		"6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4",
		"e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d",
	}

	expectedMerkleRoot = "e9b915f49bde65e53f1ca83d0d7589d613362edb0ac0ceeff5b348fe111e8a0e"
)

func TestRotate(t *testing.T) {
	os.Setenv("merkle_items_per_subtree", "4")

	newSubTreeChan := make(chan *util.SubTree)
	endTestChan := make(chan bool)

	go func() {
		for {
			subTree := <-newSubTreeChan
			assert.Equal(t, 4, subTree.Length())
			assert.Equal(t, uint64(3), subTree.Fees)

			// merkleRoot := stp.currentSubTree.ReplaceRootNode(*coinbaseHash)
			// assert.Equal(t, expectedMerkleRoot, utils.ReverseAndHexEncodeHash(merkleRoot))

			endTestChan <- true
		}
	}()

	stp := NewSubtreeProcessor(newSubTreeChan)

	waitCh := make(chan struct{})
	defer close(waitCh)

	// Add a placeholder for the coinbase
	stp.Add(model.CoinbasePlaceholder, 0, waitCh)
	<-waitCh

	for _, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		stp.Add(*hash, 1, waitCh)
		<-waitCh
	}

	assert.Equal(t, 0, stp.currentSubTree.Length())

	assert.Equal(t, 1, len(stp.chainedSubTrees))

	// Add one more txid to trigger the rotate
	hash, err := chainhash.NewHashFromStr("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	require.NoError(t, err)

	stp.Add(*hash, 1, waitCh)
	<-waitCh

	assert.Equal(t, 1, stp.currentSubTree.Length())

	// Still 1 because the tree is not yet complete
	assert.Equal(t, 1, len(stp.chainedSubTrees))

	<-endTestChan
}
