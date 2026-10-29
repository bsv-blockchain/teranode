package utxoset

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestConflictingPlanTakesLeafGroupsLockOrder pins SetConflicting's flip to the lock order every
// other batch writer takes: its leaf groups are leafGroups' groups, leaf for leaf and txid for
// txid, and each packed-key range stays with its own txid.
func TestConflictingPlanTakesLeafGroupsLockOrder(t *testing.T) {
	named := make([]chainhash.Hash, 0, 200)

	for i := 0; i < 200; i++ {
		var seed [8]byte

		binary.LittleEndian.PutUint64(seed[:], uint64(i)) //nolint:gosec // a small loop counter

		named = append(named, chainhash.Hash(sha256.Sum256(seed[:])))
	}

	p := (&Store{}).planConflicting(named, nil)

	want := leafGroups(p.kTxid)
	require.Len(t, p.leaves, len(want))

	for g := range want {
		require.Equal(t, want[g].leaf, p.leaves[g].leaf, "group %d", g)
		require.Equal(t, want[g].txids, p.leaves[g].txids, "group %d", g)

		if g > 0 {
			require.Less(t, p.leaves[g-1].leaf, p.leaves[g].leaf, "ascending leaf order")
		}

		for j, txid := range p.leaves[g].txids {
			require.Equal(t, Pack(txid, 0), p.leaves[g].lo[j])
			require.Equal(t, Pack(txid, ^uint32(0)), p.leaves[g].hi[j])
		}
	}
}
