package main

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
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
	_ = st.AddNode(*hash1, 101)
	_ = st.AddNode(*hash2, 102)
	_ = st.AddNode(*hash3, 103)
	_ = st.AddNode(*hash4, 104)
	rootHash := st.RootHash()
	assert.Equal(t, "b47df6aa4fe0a1d3841c635444be4e33eb8cdc2f2e929ced06d0a8454fb28225", utils.ReverseAndHexEncodeHash(rootHash))
	assert.Equal(t, uint64(410), st.Fees)
}

func TestDifference(t *testing.T) {
	timeStart := time.Now()

	subtree, err := loadIds(20)
	require.NoError(t, err)

	ids, err := loadList("block.bin")
	require.NoError(t, err)

	nodeIds := NewSwissMap(len(ids))
	for _, id := range ids {
		err = nodeIds.Put(id)
		require.NoError(t, err)
	}

	fmt.Printf("Loading data took %s\n", time.Since(timeStart))

	timeStart = time.Now()

	diff, err := subtree.Difference(nodeIds)
	require.NoError(t, err)

	fmt.Printf("Difference took %s\n", time.Since(timeStart))

	assert.Equal(t, 0, len(diff))
}

func TestGenerateData(t *testing.T) {
	err := generateTestSets()
	require.NoError(t, err)
}

func BenchmarkSubtree_RootHash(b *testing.B) {
	subtree, err := loadIds(18)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = subtree.RootHash()
		subtree.rootHash = [32]byte{}
	}
}

func loadList(filename string) ([][32]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(f)

	ids := make([][32]byte, 60_000_000)
	// read 32 bytes at a time
	var id [32]byte
	for {
		n, err := r.Read(id[:])
		if err != nil || n != 32 {
			break
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func generateTestSets() error {
	f, err := os.Create("ids-20.txt")
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	var f2 *os.File
	f2, err = os.Create("block.bin")
	if err != nil {
		return err
	}
	defer f2.Close()
	w2 := bufio.NewWriter(f2)

	nrOfIds := 60_000_000
	for i := 0; i < nrOfIds; i++ {
		txID := make([]byte, 32)
		_, _ = rand.Read(txID)

		if i%(nrOfIds/1_000_000) == 0 {
			_, err = w.WriteString(utils.ReverseAndHexEncodeHash([32]byte(txID)) + "\n")
			if err != nil {
				return err
			}
		}

		_, err = w2.Write(txID)
		if err != nil {
			return err
		}
	}

	return nil
}
