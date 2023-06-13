package util

import (
	"fmt"
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
}

// func TestDifference(t *testing.T) {
// 	timeStart := time.Now()

// subtree, err := loadIds(20)
// 	require.NoError(t, err)

// 	ids, err := loadList("block.bin")
// 	require.NoError(t, err)

// 	nodeIds := NewSwissMap(len(ids))
// 	for _, id := range ids {
// 		err = nodeIds.Put(id)
// 		require.NoError(t, err)
// 	}

// 	fmt.Printf("Loading data took %s\n", time.Since(timeStart))

// 	timeStart = time.Now()

// 	diff, err :=subtree.Difference(nodeIds)
// 	require.NoError(t, err)

// 	fmt.Printf("Difference took %s\n", time.Since(timeStart))

// 	assert.Equal(t, 0, len(diff))
// }

//func TestGenerateData(t *testing.T) {
//	err := generateTestSets()
//	require.NoError(t, err)
//}

// func BenchmarkSubtree_RootHash(b *testing.B) {
// subtree, err := loadIds(18)
// 	require.NoError(b, err)

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		_ =subtree.RootHash()
// 	subtree.rootHash = [32]byte{}
// 	}
// }

// func loadList(filename string) ([][32]byte, error) {
// 	f, err := os.Open(filename)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer f.Close()

// 	r := bufio.NewReader(f)

// 	ids := make([][32]byte, 60_000_000)
// 	// read 32 bytes at a time
// 	var id [32]byte
// 	for {
// 		n, err := r.Read(id[:])
// 		if err != nil || n != 32 {
// 			break
// 		}
// 		ids = append(ids, id)
// 	}

// 	return ids, nil
// }

//func generateTestSets() error {
//	f, err := os.Create("ids-20.txt")
//	if err != nil {
//		return err
//	}
//	defer f.Close()
//	w := bufio.NewWriter(f)
//
//	var f2 *os.File
//	f2, err = os.Create("block.bin")
//	if err != nil {
//		return err
//	}
//	defer f2.Close()
//	w2 := bufio.NewWriter(f2)
//
//	nrOfIds := 60_000_000
//	for i := 0; i < nrOfIds; i++ {
//		txID := make([]byte, 32)
//		_, _ = rand.Read(txID)
//
//		if i%(nrOfIds/1_000_000) == 0 {
//			_, err = w.WriteString(utils.ReverseAndHexEncodeHash([32]byte(txID)) + "\n")
//			if err != nil {
//				return err
//			}
//		}
//		_, err = w2.Write(txID)
//		if err != nil {
//			return err
//		}
//	}
//
//	return nil
//}

func TestIsPowerOf2(t *testing.T) {
	// Testing the function
	numbers := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1048576, 70368744177664}
	for _, num := range numbers {
		assert.True(t, isPowerOfTwo(num), fmt.Sprintf("%d should be a power of 2", num))
	}

	numbers = []int{-1, 0, 41, 13}
	for _, num := range numbers {
		assert.False(t, isPowerOfTwo(num), fmt.Sprintf("%d should be a power of 2", num))
	}

}
