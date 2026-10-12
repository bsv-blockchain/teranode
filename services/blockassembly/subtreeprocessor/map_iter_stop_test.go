package subtreeprocessor

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestSplitSwissMap_IterHonoursStop pins the dolthub/swiss contract the callback
// follows: it returns stop, so true ends the iteration. The map is split into
// many buckets; a wrapper that ignored the return value would visit about one
// entry per bucket instead of stopping.
func TestSplitSwissMap_IterHonoursStop(t *testing.T) {
	const entries = 3000

	m := NewSplitSwissMap(1024, entries)

	for i := 0; i < entries; i++ {
		require.NoError(t, m.Put(chainhash.HashH([]byte{byte(i), byte(i >> 8), 0x17})))
	}

	require.Equal(t, entries, m.Length())

	t.Run("false visits every entry once", func(t *testing.T) {
		seen := map[chainhash.Hash]int{}

		m.Iter(func(h chainhash.Hash, _ struct{}) bool {
			seen[h]++
			return false
		})

		require.Len(t, seen, entries)

		for _, c := range seen {
			require.Equal(t, 1, c)
		}
	})

	for _, k := range []int{1, 2, 7, 500, entries} {
		t.Run(fmt.Sprintf("stop after %d visits", k), func(t *testing.T) {
			visits := 0

			m.Iter(func(chainhash.Hash, struct{}) bool {
				visits++
				return visits >= k
			})

			require.Equal(t, k, visits)
		})
	}
}
