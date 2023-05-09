package main

import (
	"testing"

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
		t.Errorf("expected size to be 1048576, got %d", st.Size())
	}
	hash1, _ := chainhash.NewHashFromStr("97af9ad3583e2f83fc1e44e475e3a3ee31ec032449cc88b491479ef7d187c115")
	hash2, _ := chainhash.NewHashFromStr("7ce05dda56bc523048186c0f0474eb21c92fe35de6d014bd016834637a3ed08d")
	hash3, _ := chainhash.NewHashFromStr("3070fb937289e24720c827cbc24f3fce5c361cd7e174392a700a9f42051609e0")
	hash4, _ := chainhash.NewHashFromStr("d3cde0ab7142cc99acb31c5b5e1e941faed1c5cf5f8b63ed663972845d663487")
	_ = st.AddNodes([][32]byte{
		*hash1,
		*hash2,
		*hash3,
		*hash4,
	})
	rootHash := st.RootHash()
	assert.Equal(t, "b47df6aa4fe0a1d3841c635444be4e33eb8cdc2f2e929ced06d0a8454fb28225", utils.ReverseAndHexEncodeHash(rootHash))
}

func BenchmarkSubTree_RootHash(b *testing.B) {
	subTree, err := loadIds(18)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = subTree.RootHash()
		subTree.rootHash = [32]byte{}
	}
}
