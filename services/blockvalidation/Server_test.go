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
	coinbaseTx, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a010000004341041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84ac00000000")

	txIds []string = []string{
		"8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", // Coinbase
		"fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4",
		"6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4",
		"e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d",
	}

	merkleRoot, _ = chainhash.NewHashFromStr("f3e94742aca4b5ef85488dc37c06c3282295ffec960994b2c0d5ac2a25a95766")

	prevBlockHashStr = "000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250"
	bitsStr          = "1b04864c"
)

func TestOneTransaction(t *testing.T) {
	subtrees := make([]*util.Subtree, 1)

	subtrees[0] = util.NewTree(1)

	var empty [32]byte
	err := subtrees[0].AddNode(empty, 0)
	require.NoError(t, err)

	blockValidationService, err := New(p2p.TestLogger{})
	require.NoError(t, err)

	block := &model.Block{
		Header: &bc.BlockHeader{
			HashMerkleRoot: bt.ReverseBytes(coinbaseTx.TxIDBytes()),
		},
		Subtrees:   subtrees,
		CoinbaseTx: coinbaseTx,
	}

	err = blockValidationService.CheckMerkleRoot(block)
	assert.NoError(t, err)
}

func TestTwoTransactions(t *testing.T) {
	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff07044c86041b0147ffffffff0100f2052a01000000434104ad3b4c6ee28cb0c438c87b4efe1c36e1e54c10efc690f24c2c02446def863c50e9bf482647727b415aa81b45d0f7aa42c2cb445e4d08f18b49c027b58b6b4041ac00000000")
	coinbaseTxID, _ := chainhash.NewHashFromStr("de2c2e8628ab837ceff3de0217083d9d5feb71f758a5d083ada0b33a36e1b30e")
	txid1, _ := chainhash.NewHashFromStr("89878bfd69fba52876e5217faec126fc6a20b1845865d4038c12f03200793f48")
	expectedMerkleRoot, _ := chainhash.NewHashFromStr("7a059188283323a2ef0e02dd9f8ba1ac550f94646290d0a52a586e5426c956c5")

	assert.Equal(t, coinbaseTxID.String(), coinbaseTx.TxID())

	subtrees := make([]*util.Subtree, 1)
	subtrees[0] = util.NewTree(1)

	var empty [32]byte
	err := subtrees[0].AddNode(empty, 0)
	require.NoError(t, err)

	err = subtrees[0].AddNode([32]byte(txid1.CloneBytes()), 0)
	require.NoError(t, err)

	blockValidationService, err := New(p2p.TestLogger{})
	require.NoError(t, err)

	block := &model.Block{
		Header: &bc.BlockHeader{
			HashMerkleRoot: expectedMerkleRoot.CloneBytes(),
		},
		Subtrees:   subtrees,
		CoinbaseTx: coinbaseTx,
	}

	err = blockValidationService.CheckMerkleRoot(block)
	assert.NoError(t, err)
}

func TestMerkleRoot(t *testing.T) {
	subtrees := make([]*util.Subtree, 2)

	subtrees[0] = util.NewTreeByLeafCount(2) // height = 1
	subtrees[1] = util.NewTreeByLeafCount(2) // height = 1

	err := subtrees[0].AddNode(model.CoinbasePlaceholder, 0)
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

	assert.Equal(t, txIds[0], coinbaseTx.TxID())

	prevBlockHash, err := chainhash.NewHashFromStr(prevBlockHashStr)
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
		Subtrees:   subtrees,
		CoinbaseTx: coinbaseTx,
	}

	blockValidationService, err := New(p2p.TestLogger{})
	require.NoError(t, err)

	err = blockValidationService.CheckMerkleRoot(block)
	assert.NoError(t, err)
}
