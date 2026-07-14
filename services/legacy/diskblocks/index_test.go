package diskblocks

import (
	"os"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func h(b byte) chainhash.Hash {
	var x chainhash.Hash
	x[0] = b
	return x
}

// chainRefs builds a simple linear chain 0..n-1 where block k's hash is h(k+1)
// and its prev is h(k) (genesis prev = zero hash h(0)).
func chainRefs(n int) map[chainhash.Hash]*BlockRef {
	m := map[chainhash.Hash]*BlockRef{}
	for k := 0; k < n; k++ {
		hash := h(byte(k + 1))
		prev := h(byte(k)) // h(0) is the zero-ish genesis parent
		m[hash] = &BlockRef{Hash: hash, PrevHash: prev, Height: uint32(k), HaveData: true}
	}
	return m
}

func TestSelectChainEmpty(t *testing.T) {
	_, err := selectChain(map[chainhash.Hash]*BlockRef{}, 0)
	require.Error(t, err)
}

func TestSelectChainLinear(t *testing.T) {
	got, err := selectChain(chainRefs(5), 0)
	require.NoError(t, err)
	require.Len(t, got, 5)
	for k := 0; k < 5; k++ {
		require.Equal(t, uint32(k), got[k].Height) // ordered genesis -> tip
	}
}

func TestSelectChainStopAtHeight(t *testing.T) {
	got, err := selectChain(chainRefs(10), 3)
	require.NoError(t, err)
	require.Len(t, got, 4) // heights 0,1,2,3
	require.Equal(t, uint32(3), got[len(got)-1].Height)
}

func TestSelectChainFrontierGap(t *testing.T) {
	// Sealed chain 0..4; a stale higher record at height 7 whose ancestry is
	// missing must NOT be chosen — the best complete chain ends at height 4.
	refs := chainRefs(5)
	orphan := h(99)
	refs[orphan] = &BlockRef{Hash: orphan, PrevHash: h(98), Height: 7, HaveData: true}
	got, err := selectChain(refs, 0)
	require.NoError(t, err)
	require.Len(t, got, 5)
	require.Equal(t, uint32(4), got[len(got)-1].Height)
}

func TestReadChainIntegration(t *testing.T) {
	dir := os.Getenv("TERANODE_TEST_SVNODE_DIR")
	if dir == "" {
		t.Skip("set TERANODE_TEST_SVNODE_DIR to a stopped SV Node datadir to run")
	}
	idx, err := OpenIndex(dir)
	require.NoError(t, err)
	defer idx.Close()

	chain, err := idx.ReadChain(100)
	require.NoError(t, err)
	require.NotEmpty(t, chain)
	require.Equal(t, uint32(0), chain[0].Height)
	require.LessOrEqual(t, chain[len(chain)-1].Height, uint32(100))
}
