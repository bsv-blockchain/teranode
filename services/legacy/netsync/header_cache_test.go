package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// chainOfHeaders builds n headers that genuinely link, each one's PrevBlock
// being the hash of the one before, starting from parent.
//
// They must genuinely link rather than merely differ: the cache's whole job is
// to refuse a batch that does not, so a fixture with fake linkage would leave
// every Fill test passing for the wrong reason.
func chainOfHeaders(parent chainhash.Hash, n int) []*wire.BlockHeader {
	headers := make([]*wire.BlockHeader, 0, n)
	prev := parent

	for i := 0; i < n; i++ {
		h := &wire.BlockHeader{
			Version:    1,
			PrevBlock:  prev,
			MerkleRoot: chainhash.Hash{byte(i), byte(i >> 8)},
			Bits:       0x1d00ffff,
			Nonce:      uint32(i),
		}
		headers = append(headers, h)
		prev = h.BlockHash()
	}

	return headers
}

func TestHeaderCache_NamesEveryHeightInTheBatch(t *testing.T) {
	parent := chainhash.Hash{0xaa}
	headers := chainOfHeaders(parent, 5)

	c := newHeaderCache()
	require.True(t, c.Fill(parent, 101, headers), "a batch that links to the parent must be accepted")

	require.Equal(t, 5, c.Len())

	for i, h := range headers {
		got, ok := c.At(int32(101 + i))
		require.True(t, ok, "height %d must be named", 101+i)
		require.Equal(t, h.BlockHash(), got)
	}
}

func TestHeaderCache_RefusesABatchThatDoesNotLinkToTheParent(t *testing.T) {
	parent := chainhash.Hash{0xaa}
	headers := chainOfHeaders(chainhash.Hash{0xbb}, 3)

	c := newHeaderCache()
	require.False(t, c.Fill(parent, 101, headers),
		"a batch whose first header names a different parent describes a chain this node is not on")
	require.Zero(t, c.Len(), "and it must leave nothing behind")
}

func TestHeaderCache_RefusesABatchThatBreaksItsOwnChain(t *testing.T) {
	parent := chainhash.Hash{0xaa}
	headers := chainOfHeaders(parent, 4)

	// Break the link between the second and third header, leaving the first two
	// honest. A cache that only checked the first header would accept this.
	headers[2].PrevBlock = chainhash.Hash{0xcc}

	c := newHeaderCache()
	require.False(t, c.Fill(parent, 101, headers),
		"a batch that does not link internally cannot name heights, because the heights after the break are guesses")
	require.Zero(t, c.Len())
}

func TestHeaderCache_ReplacesRatherThanMerges(t *testing.T) {
	first := chainhash.Hash{0xaa}
	c := newHeaderCache()
	require.True(t, c.Fill(first, 101, chainOfHeaders(first, 5)))

	second := chainhash.Hash{0xdd}
	require.True(t, c.Fill(second, 900, chainOfHeaders(second, 2)))

	require.Equal(t, 2, c.Len(), "a fill is a replacement, not a merge")

	_, ok := c.At(101)
	require.False(t, ok, "the old batch must be gone, or a stale height could be named from a chain we left")
}

func TestHeaderCache_ARefusedFillLeavesThePreviousBatchIntact(t *testing.T) {
	first := chainhash.Hash{0xaa}
	c := newHeaderCache()
	require.True(t, c.Fill(first, 101, chainOfHeaders(first, 5)))

	// A batch that links to the parent but breaks its own chain partway through,
	// the subtler of the two refusal modes: the caller does everything right up
	// to the point where it doesn't.
	second := chainhash.Hash{0xdd}
	headers := chainOfHeaders(second, 4)
	headers[2].PrevBlock = chainhash.Hash{0xcc}

	require.False(t, c.Fill(second, 900, headers),
		"a batch that breaks its own chain must be refused")

	require.Equal(t, 5, c.Len(), "a refused fill must not disturb the size of the batch already held")

	for i := 0; i < 5; i++ {
		got, ok := c.At(int32(101 + i))
		require.True(t, ok, "height %d from the previous batch must still be named", 101+i)
		require.Equal(t, chainOfHeaders(first, 5)[i].BlockHash(), got)
	}
}

func TestHeaderCache_TopIsTheHighestHeightNamed(t *testing.T) {
	parent := chainhash.Hash{0xaa}
	c := newHeaderCache()

	_, ok := c.Top()
	require.False(t, ok, "an empty cache names no top")

	require.True(t, c.Fill(parent, 101, chainOfHeaders(parent, 5)))

	top, ok := c.Top()
	require.True(t, ok)
	require.Equal(t, int32(105), top)
}

func TestHeaderCache_DiscardEmptiesIt(t *testing.T) {
	parent := chainhash.Hash{0xaa}
	c := newHeaderCache()
	require.True(t, c.Fill(parent, 101, chainOfHeaders(parent, 5)))

	c.Discard()

	require.Zero(t, c.Len())

	_, ok := c.At(103)
	require.False(t, ok)
}

func TestHeaderCache_IsSafeOnANilReceiver(t *testing.T) {
	var c *headerCache

	require.False(t, c.Fill(chainhash.Hash{}, 1, nil))
	require.Zero(t, c.Len())
	c.Discard()

	_, ok := c.At(1)
	require.False(t, ok)

	_, ok = c.Top()
	require.False(t, ok)
}
