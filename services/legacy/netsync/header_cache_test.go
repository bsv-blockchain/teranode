package netsync

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
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

// minedRun is linkedRun with every header ground to its own declared target,
// judged by model.BlockHeader.HasMetTargetDifficulty rather than by the code
// under test, the way mineRegtestPoW does. Needed by any test that gives the
// cache a proof-of-work ceiling: linkedRun's unmined 0x207fffff headers pass a
// roughly 2^255 target about half the time, and chainOfHeaders' 0x1d00ffff ones
// never do, so a pow-limit-bearing cache fed either would be flaky or always
// refused.
func minedRun(t *testing.T, parent chainhash.Hash, count int, bits uint32) ([]*wire.BlockHeader, []chainhash.Hash) {
	t.Helper()

	headers := make([]*wire.BlockHeader, 0, count)
	hashes := make([]chainhash.Hash, 0, count)

	prev := parent

	for i := 0; i < count; i++ {
		header := wire.NewBlockHeader(1, &prev, &chainhash.Hash{byte(i)}, bits, 0)
		mineWireHeader(t, header)

		hash := header.BlockHash()
		headers = append(headers, header)
		hashes = append(hashes, hash)
		prev = hash
	}

	return headers, hashes
}

// mineWireHeader bumps header.Nonce until model's own HasMetTargetDifficulty
// accepts the header against its declared Bits.
func mineWireHeader(t *testing.T, header *wire.BlockHeader) {
	t.Helper()

	for nonce := uint32(0); nonce < 10_000_000; nonce++ {
		header.Nonce = nonce

		if wireHeaderMeetsItsTarget(t, header) {
			return
		}
	}

	t.Fatalf("could not find a nonce meeting target %08x", header.Bits)
}

// wireHeaderMeetsItsTarget is HasMetTargetDifficulty over a wire header, the
// independent judge of the work Fill is being tested on.
func wireHeaderMeetsItsTarget(t *testing.T, header *wire.BlockHeader) bool {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, header.Serialize(&buf))

	modelHeader, err := model.NewBlockHeaderFromBytes(buf.Bytes())
	require.NoError(t, err)

	ok, _, _ := modelHeader.HasMetTargetDifficulty()

	return ok
}

// TestHeaderCache_RefusesAHeaderThatDoesNotMeetItsOwnTarget is SV Node's
// CheckProofOfWork run by Fill: a batch in which one header's hash exceeds the
// target its own nBits declares is refused whole and nothing is cached. The
// control run, every header mined, is accepted by the same kind of cache, and a
// cache without a ceiling accepts the spoiled batch too, because it checks
// linkage alone, as every test cache does.
func TestHeaderCache_RefusesAHeaderThatDoesNotMeetItsOwnTarget(t *testing.T) {
	parent := chainhash.Hash{0xc0}
	ceiling := model.PowLimitCeiling(&chaincfg.RegressionNetParams)
	require.NotNil(t, ceiling)

	headers, _ := minedRun(t, parent, 5, 0x207fffff)

	c := newHeaderCache().WithPowLimit(ceiling)
	require.True(t, c.Fill(parent, 101, headers), "a run of mined headers is accepted")
	require.Equal(t, 5, c.Len())

	// Spoil one header: move its nonce until its hash exceeds the target, then
	// relink and re-mine the headers after it so the only fault in the batch is
	// that one header's work.
	spoiled, _ := minedRun(t, parent, 5, 0x207fffff)

	for wireHeaderMeetsItsTarget(t, spoiled[2]) {
		spoiled[2].Nonce++
	}

	for i := 3; i < len(spoiled); i++ {
		spoiled[i].PrevBlock = spoiled[i-1].BlockHash()
		mineWireHeader(t, spoiled[i])
	}

	refusing := newHeaderCache().WithPowLimit(ceiling)
	require.False(t, refusing.Fill(parent, 101, spoiled), "one header without work refuses the whole batch")
	require.Zero(t, refusing.Len(), "and nothing is cached")

	linkageOnly := newHeaderCache()
	require.True(t, linkageOnly.Fill(parent, 101, spoiled), "without a ceiling Fill checks linkage alone")
}

// TestHeaderCache_RefusesATargetEasierThanTheChainCeiling is the range half of
// CheckProofOfWork: a header that declares a target easier than the chain's
// limit is refused even when its hash meets that trivial target. This is the
// free half of GHSA-gggq-8f59-4jm9, a 2000-header run for the price of hashing
// it, which the ceiling takes away. Without a ceiling the same run is accepted,
// pinning that the default stays linkage-only.
func TestHeaderCache_RefusesATargetEasierThanTheChainCeiling(t *testing.T) {
	parent := chainhash.Hash{0xc1}

	// 0x207fffff is regtest's minimum; on mainnet it is far above the limit.
	headers, _ := minedRun(t, parent, 4, 0x207fffff)

	mainnet := newHeaderCache().WithPowLimit(model.PowLimitCeiling(&chaincfg.MainNetParams))
	require.False(t, mainnet.Fill(parent, 101, headers), "a target easier than mainnet's limit is refused however well it is met")
	require.Zero(t, mainnet.Len())

	noCeiling := newHeaderCache()
	require.True(t, noCeiling.Fill(parent, 101, headers), "no ceiling, no proof-of-work check")
	require.Equal(t, 4, noCeiling.Len())
}
