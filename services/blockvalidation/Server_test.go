package blockvalidation

import (
	"testing"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bc"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	coinbaseStr = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a010000004341041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84ac00000000"

	txIds []string = []string{
		"8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", // Coinbase
		"fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4",
		"6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4",
		"e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d",
	}

	expectedMerkleRoot                        = "f3e94742aca4b5ef85488dc37c06c3282295ffec960994b2c0d5ac2a25a95766"
	expectedMerkleRootWithCoinbasePlaceholder = "e9b915f49bde65e53f1ca83d0d7589d613362edb0ac0ceeff5b348fe111e8a0e"
	prevBlockHashStr                          = "000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250"
	bitsStr                                   = "1b04864c"
)

func TestMerkleRoot(t *testing.T) {
	subtrees := make([]*util.SubTree, 2)

	subtrees[0] = util.NewTree(1) // height = 1
	subtrees[1] = util.NewTree(1) // height = 1

	var empty [32]byte
	err := subtrees[0].AddNode(empty, 0)
	require.NoError(t, err)

	hash1, err := chainhash.NewHashFromStr(txIds[1])
	require.NoError(t, err)
	err = subtrees[0].AddNode(*hash1, 1)
	require.NoError(t, err)

	hash2, err := chainhash.NewHashFromStr(txIds[2])
	require.NoError(t, err)
	err = subtrees[1].AddNode(*hash2, 1)
	require.NoError(t, err)

	hash3, err := chainhash.NewHashFromStr(txIds[3])
	require.NoError(t, err)
	err = subtrees[1].AddNode(*hash3, 1)
	require.NoError(t, err)

	coinbaseTx, err := bt.NewTxFromString(coinbaseStr)
	require.NoError(t, err)
	assert.Equal(t, txIds[0], coinbaseTx.TxID())

	prevBlockHash, err := chainhash.NewHashFromStr(prevBlockHashStr)
	if err != nil {
		t.Fail()
	}

	merkleRoot, err := chainhash.NewHashFromStr(expectedMerkleRoot)
	if err != nil {
		t.Fail()
	}
	bits, err := chainhash.NewHashFromStr(bitsStr)
	if err != nil {
		t.Fail()
	}

	block := &model.Block{
		Header: &bc.BlockHeader{
			Version:        1,
			Time:           1293623863,
			Nonce:          274148111,
			HashPrevBlock:  prevBlockHash.CloneBytes(),
			HashMerkleRoot: merkleRoot.CloneBytes(),
			Bits:           bits.CloneBytes(),
		},
		SubTrees:   subtrees,
		CoinbaseTx: coinbaseTx,
	}

	blockValidationService, err := New(p2p.TestLogger{})
	require.NoError(t, err)

	t.Log(blockValidationService.checkMerkleRoot(block))

}
