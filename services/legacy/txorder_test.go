package legacy

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestParentsFirst(t *testing.T) {
	h := func(b byte) chainhash.Hash { return chainhash.Hash{b} }

	tests := []struct {
		name    string
		hashes  []chainhash.Hash
		parents map[chainhash.Hash][]chainhash.Hash
		want    []int
	}{
		{
			name:   "no dependencies keeps order",
			hashes: []chainhash.Hash{h(3), h(1), h(2)},
			want:   []int{0, 1, 2},
		},
		{
			name:    "reversed chain",
			hashes:  []chainhash.Hash{h(3), h(2), h(1)},
			parents: map[chainhash.Hash][]chainhash.Hash{h(3): {h(2)}, h(2): {h(1)}},
			want:    []int{2, 1, 0},
		},
		{
			name:   "diamond",
			hashes: []chainhash.Hash{h(4), h(2), h(3), h(1)},
			parents: map[chainhash.Hash][]chainhash.Hash{
				h(4): {h(2), h(3)},
				h(2): {h(1)},
				h(3): {h(1)},
			},
			want: []int{3, 1, 2, 0},
		},
		{
			name:    "parents outside the set are ignored",
			hashes:  []chainhash.Hash{h(2), h(1)},
			parents: map[chainhash.Hash][]chainhash.Hash{h(2): {h(9)}, h(1): {h(8)}},
			want:    []int{0, 1},
		},
		{
			name:    "duplicate hash",
			hashes:  []chainhash.Hash{h(2), h(1), h(1)},
			parents: map[chainhash.Hash][]chainhash.Hash{h(2): {h(1)}},
			want:    []int{1, 0, 2},
		},
		{
			name:    "cycle terminates",
			hashes:  []chainhash.Hash{h(1), h(2)},
			parents: map[chainhash.Hash][]chainhash.Hash{h(1): {h(2)}, h(2): {h(1)}},
			want:    []int{1, 0},
		},
		{
			name:   "empty",
			hashes: nil,
			want:   []int{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parentsFirst(tc.hashes, func(i int) []chainhash.Hash { return tc.parents[tc.hashes[i]] })
			require.Equal(t, tc.want, got)
		})
	}
}

// TestParentsFirst_LongChain checks a chain as deep as the rebroadcast queue
// can hold, queued child-first.
func TestParentsFirst_LongChain(t *testing.T) {
	const n = maxRebroadcastInventory

	hashes := make([]chainhash.Hash, n)
	for i := range hashes {
		hashes[i] = chainhash.Hash{byte(i), byte(i >> 8), 0xcc}
	}

	// hashes[i]'s parent is hashes[i+1], so the input is fully reversed.
	order := parentsFirst(hashes, func(i int) []chainhash.Hash {
		if i+1 < n {
			return []chainhash.Hash{hashes[i+1]}
		}

		return nil
	})

	require.Len(t, order, n)

	for pos, i := range order {
		require.Equal(t, n-1-pos, i)
	}
}
