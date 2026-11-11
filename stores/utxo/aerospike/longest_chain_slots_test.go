package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestLongestChainSlots_ReuseAllocatesNothing pins, without a server, that a warmed slot takes a new
// hash with no allocation and that the key it rebuilds in place has the digest of a fresh key.
func TestLongestChainSlots_ReuseAllocatesNothing(t *testing.T) {
	const namespace, setName, size = "test", "utxo", 4

	slots := newLongestChainSlots(size, aerospike.NewBatchWritePolicy(), nil)

	hashes := make([]chainhash.Hash, 2*size)
	for i := range hashes {
		hashes[i] = chainhash.HashH([]byte{byte(i)})
	}

	for k := 0; k < size; k++ {
		require.NoError(t, slots.set(k, &hashes[k], namespace, setName))
	}

	round := 0
	allocs := testing.AllocsPerRun(100, func() {
		round++
		for k := 0; k < size; k++ {
			_ = slots.set(k, &hashes[(round%2)*size+k], namespace, setName)
		}
	})
	require.Zero(t, allocs)

	for k := 0; k < size; k++ {
		require.NoError(t, slots.set(k, &hashes[k], namespace, setName))

		want, err := aerospike.NewKey(namespace, setName, hashes[k][:])
		require.NoError(t, err)
		require.Equal(t, want.Digest(), slots.keys[k].Digest())
		require.Same(t, slots.keys[k], slots.records[k].BatchRec().Key)
	}
}
