package util

import (
	"testing"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTree(t *testing.T) {
	st := NewTree(20)
	if st.Size() != 1048576 {
		t.Errorf("expected size to be 1048576, got %d", st.Size())
	}
}

func TestRootHash(t *testing.T) {
	st := NewTree(2)
	if st.Size() != 4 {
		t.Errorf("expected size to be 4, got %d", st.Size())
	}
	hash1, _ := chainhash.NewHashFromStr("97af9ad3583e2f83fc1e44e475e3a3ee31ec032449cc88b491479ef7d187c115")
	hash2, _ := chainhash.NewHashFromStr("7ce05dda56bc523048186c0f0474eb21c92fe35de6d014bd016834637a3ed08d")
	hash3, _ := chainhash.NewHashFromStr("3070fb937289e24720c827cbc24f3fce5c361cd7e174392a700a9f42051609e0")
	hash4, _ := chainhash.NewHashFromStr("d3cde0ab7142cc99acb31c5b5e1e941faed1c5cf5f8b63ed663972845d663487")
	_ = st.AddNodes([]*chainhash.Hash{
		hash1,
		hash2,
		hash3,
		hash4,
	})
	rootHash := st.RootHash()
	assert.Equal(t, "b47df6aa4fe0a1d3841c635444be4e33eb8cdc2f2e929ced06d0a8454fb28225", rootHash.String())
}

func TestRootHashSimon(t *testing.T) {
	st := NewTree(2)
	if st.Size() != 4 {
		t.Errorf("expected size to be 4, got %d", st.Size())
	}

	hash1, _ := chainhash.NewHashFromStr("8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87")
	hash2, _ := chainhash.NewHashFromStr("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	hash3, _ := chainhash.NewHashFromStr("6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4")
	hash4, _ := chainhash.NewHashFromStr("e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d")
	_ = st.AddNodes([]*chainhash.Hash{
		hash1,
		hash2,
		hash3,
		hash4,
	})
	rootHash := st.RootHash()
	assert.Equal(t, "f3e94742aca4b5ef85488dc37c06c3282295ffec960994b2c0d5ac2a25a95766", rootHash.String())
}

func TestTwoTransactions(t *testing.T) {
	st := NewTree(1)
	if st.Size() != 2 {
		t.Errorf("expected size to be 4, got %d", st.Size())
	}

	hash1, _ := chainhash.NewHashFromStr("de2c2e8628ab837ceff3de0217083d9d5feb71f758a5d083ada0b33a36e1b30e")
	hash2, _ := chainhash.NewHashFromStr("89878bfd69fba52876e5217faec126fc6a20b1845865d4038c12f03200793f48")

	_ = st.AddNodes([]*chainhash.Hash{
		hash1,
		hash2,
	})

	rootHash := st.RootHash()
	assert.Equal(t, "7a059188283323a2ef0e02dd9f8ba1ac550f94646290d0a52a586e5426c956c5", rootHash.String())
}

func TestSubtree_GetMerkleProof(t *testing.T) {
	st := NewTree(3)
	if st.Size() != 8 {
		t.Errorf("expected size to be 4, got %d", st.Size())
	}

	txIDS := []string{
		"4634057867994ae379e82b408cc9eb145a6e921b95ca38f2ced7eb880685a290",
		"7f87fe1100963977975cef49344e442b4fa3dd9d41de19bc94609c100210ca05",
		"a28c1021f07263101f5a5052c6a7bdc970ac1d0ab09d8d20aa7a4a61ad9d6597",
		"dcd31c71368f757f65105d68ee1a2e5598db84900e28dabecba23651c5cda468",
		"7bac32882547cbb540914f48c6ac99ac682ef001c3aa3d4dcdb5951c8db79678",
		"67c0f4eb336057ecdf940497a75fcbd1a131e981edf568b54eed2f944889e441",
	}

	var txHash *chainhash.Hash
	for _, txID := range txIDS {
		txHash, _ = chainhash.NewHashFromStr(txID)
		_ = st.AddNode(txHash, 101)
	}

	proof, err := st.GetMerkleProof(1)
	require.NoError(t, err)
	assert.Equal(t, 3, len(proof))
	assert.Equal(t, "4634057867994ae379e82b408cc9eb145a6e921b95ca38f2ced7eb880685a290", proof[0].String())
	assert.Equal(t, "a9e6413abb02b534ff5250cbabdc673480656d0e053cfd23fd010241d5e045f2", proof[1].String())
	assert.Equal(t, "63fd0f07ff87223f688d0809f46a8118f185bab04d300406513acdc8832bad5e", proof[2].String())
	assert.Equal(t, "68e239fc6684a224142add79ebed60569baedf667c6be03a5f8719aba44a488b", st.RootHash().String())

	proof, err = st.GetMerkleProof(4)
	require.NoError(t, err)
	assert.Equal(t, 3, len(proof))
	assert.Equal(t, "67c0f4eb336057ecdf940497a75fcbd1a131e981edf568b54eed2f944889e441", proof[0].String())
	assert.Equal(t, "e2a6065233b307b77a5f73f9f27843d42e48d5e061567416b4508517ef2dd452", proof[1].String())
	assert.Equal(t, "bfd8a13a5cb1ba128319ee95e09a7e2ff67a52d0c9af8485bfffae737e32d6bf", proof[2].String())
	assert.Equal(t, "68e239fc6684a224142add79ebed60569baedf667c6be03a5f8719aba44a488b", st.RootHash().String())

	proof, err = st.GetMerkleProof(6)
	require.Error(t, err) // out of range
	assert.Len(t, proof, 0)
}

func Test_Serialize(t *testing.T) {
	t.Run("Serialize", func(t *testing.T) {
		st := NewTree(2)
		if st.Size() != 4 {
			t.Errorf("expected size to be 4, got %d", st.Size())
		}

		hash1, _ := chainhash.NewHashFromStr("8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87")
		hash2, _ := chainhash.NewHashFromStr("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		hash3, _ := chainhash.NewHashFromStr("6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4")
		hash4, _ := chainhash.NewHashFromStr("e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d")
		_ = st.AddNodes([]*chainhash.Hash{
			hash1,
			hash2,
			hash3,
			hash4,
		})
		serializedBytes, err := st.Serialize()
		require.NoError(t, err)

		newSubtree := NewTree(2)
		err = newSubtree.Deserialize(serializedBytes)
		require.NoError(t, err)
		assert.Equal(t, st.Fees, newSubtree.Fees)
		assert.Equal(t, st.Size(), newSubtree.Size())
		assert.Equal(t, st.RootHash(), newSubtree.RootHash())

		assert.Equal(t, len(st.Nodes), len(newSubtree.Nodes))
		for i := 0; i < len(st.Nodes); i++ {
			assert.Equal(t, st.Nodes[i].String(), newSubtree.Nodes[i].String())
		}
	})

	t.Run("Serialize with conflicting", func(t *testing.T) {
		st := NewTree(2)
		if st.Size() != 4 {
			t.Errorf("expected size to be 4, got %d", st.Size())
		}

		hash1, _ := chainhash.NewHashFromStr("8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87")
		hash2, _ := chainhash.NewHashFromStr("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
		hash3, _ := chainhash.NewHashFromStr("6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4")
		hash4, _ := chainhash.NewHashFromStr("e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d")
		_ = st.AddNodes([]*chainhash.Hash{
			hash1,
			hash2,
			hash3,
			hash4,
		})

		st.ConflictingNodes = []*chainhash.Hash{
			hash3,
		}

		serializedBytes, err := st.Serialize()
		require.NoError(t, err)

		newSubtree := NewTree(2)
		err = newSubtree.Deserialize(serializedBytes)
		require.NoError(t, err)
		assert.Equal(t, st.Fees, newSubtree.Fees)
		assert.Equal(t, st.Size(), newSubtree.Size())
		assert.Equal(t, st.RootHash(), newSubtree.RootHash())

		assert.Equal(t, len(st.Nodes), len(newSubtree.Nodes))
		for i := 0; i < len(st.Nodes); i++ {
			assert.Equal(t, st.Nodes[i].String(), newSubtree.Nodes[i].String())
		}

		assert.Equal(t, len(st.ConflictingNodes), len(newSubtree.ConflictingNodes))
		for i := 0; i < len(st.ConflictingNodes); i++ {
			assert.Equal(t, st.ConflictingNodes[i].String(), newSubtree.ConflictingNodes[i].String())
		}
	})
}
