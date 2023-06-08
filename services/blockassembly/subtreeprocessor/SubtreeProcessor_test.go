package subtreeprocessor

import (
	"os"
	"testing"
	"time"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	txIds []string = []string{
		"fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4",
		"6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4",
		"e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d",
	}

	expectedMerkleRoot = "e9b915f49bde65e53f1ca83d0d7589d613362edb0ac0ceeff5b348fe111e8a0e"
)

func TestRotate(t *testing.T) {
	os.Setenv("merkle_items_per_subtree", "4")

	stp := NewSubtreeProcessor()

	// Add a placeholder for the coinbase
	stp.Add(chainhash.Hash{}, 0)

	for _, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		stp.Add(*hash, 1)
	}

	time.Sleep(300 * time.Millisecond)

	require.Equal(t, 4, len(stp.txIDs))

	assert.Equal(t, uint64(3), stp.totalFees)

	merkleRoot, err := stp.MerkleRoot(nil)
	require.NoError(t, err)

	assert.Equal(t, expectedMerkleRoot, merkleRoot.String())

	// Add one more txid to trigger the rotate
	hash, err := chainhash.NewHashFromStr("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	require.NoError(t, err)

	stp.Add(*hash, 1)

	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, 1, len(stp.txIDs))
}
