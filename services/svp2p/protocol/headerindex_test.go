package protocol

import (
	"fmt"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// nonceCounter hands out ever-increasing nonces so headers built within one
// test never collide on hash, regardless of how many chains/forks a test
// builds from the same genesis.
type nonceCounter struct{ next uint32 }

func (c *nonceCounter) take() uint32 {
	c.next++
	return c.next
}

func testGenesis() *wire.BlockHeader {
	zero := chainhash.Hash{}
	return wire.NewBlockHeader(1, &zero, &zero, 0x1d00ffff, 0)
}

// childOf returns a header extending parent, with a unique nonce so its
// hash never collides with a sibling built from the same parent.
func childOf(parent *wire.BlockHeader, nonce uint32) *wire.BlockHeader {
	return childOfBits(parent, nonce, difficulty1Bits)
}

// childOfBits is childOf with the difficulty target chosen by the caller, so
// a test can build two branches of unequal work.
func childOfBits(parent *wire.BlockHeader, nonce uint32, bits uint32) *wire.BlockHeader {
	prevHash := parent.BlockHash()
	zero := chainhash.Hash{}

	return wire.NewBlockHeader(1, &prevHash, &zero, bits, nonce)
}

// The two targets every work-based test in this package builds branches from.
// Their hand-derived GetBlockProof values are pinned by TestBlockProof below.
const (
	// difficulty1Bits is the difficulty-1 target, worth 4_295_032_833 per
	// header.
	difficulty1Bits = uint32(0x1d00ffff)

	// heavyBits is a target worth 8_589_934_591 per header, just under twice
	// difficulty1Bits.
	heavyBits = uint32(0x1d008000)
)

const (
	difficulty1Work = int64(4_295_032_833)
	heavyWork       = int64(8_589_934_591)
)

// buildChainBits is buildChain with the branch's difficulty target chosen by
// the caller.
func buildChainBits(t *testing.T, idx *HeaderIndex, nc *nonceCounter, from *wire.BlockHeader, count int, bits uint32) []*wire.BlockHeader {
	t.Helper()

	headers := make([]*wire.BlockHeader, 0, count)
	prev := from

	for i := 0; i < count; i++ {
		h := childOfBits(prev, nc.take(), bits)

		connected, err := idx.AddHeader(h)
		require.NoError(t, err)
		require.True(t, connected)

		headers = append(headers, h)
		prev = h
	}

	return headers
}

// TestBlockProof pins the block_index.cpp GetBlockProof port
// (block_index.cpp:114-125) against values derived by hand from the compact
// target, independent of any big.Int code this package calls.
func TestBlockProof(t *testing.T) {
	tests := []struct {
		name string
		bits uint32
		want string
	}{
		{
			// 0x1d00ffff: exponent 0x1d = 29, mantissa 0x00ffff = 65535, so
			// target = 65535 * 256^(29-3) = (2^16 - 1) * 2^208.
			// GetBlockProof is floor(2^256 / (target + 1)). Take
			// q = 4295032833; then q * 65535 = 2^48 - 1, so
			//   q * (target + 1) = (2^48 - 1) * 2^208 + q = 2^256 - 2^208 + q,
			// which is below 2^256 because 2^208 > q, while
			//   (q + 1) * (target + 1) = 2^256 + 65534 * 2^208 + q + 1
			// is above it. The floor is therefore exactly q.
			name: "difficulty-1 target",
			bits: 0x1d00ffff,
			want: "4295032833",
		},
		{
			// 0x1d008000: exponent 29, mantissa 0x008000 = 2^15, so
			// target = 2^15 * 2^208 = 2^223.
			// (2^33) * (2^223 + 1) = 2^256 + 2^33 is above 2^256, and
			// (2^33 - 1) * (2^223 + 1) = 2^256 + 2^33 - 2^223 - 1 is below it
			// because 2^223 > 2^33. The floor is 2^33 - 1 = 8589934591.
			name: "heavier target",
			bits: 0x1d008000,
			want: "8589934591",
		},
		{
			// 0x207fffff (the regtest power limit): exponent 0x20 = 32,
			// mantissa 0x7fffff = 2^23 - 1, so
			// target = (2^23 - 1) * 2^232 = 2^255 - 2^232.
			// 2 * (target + 1) = 2^256 - 2^233 + 2 is below 2^256 and
			// 3 * (target + 1) is above it, since 3 * 2^255 alone exceeds
			// 2^256. The floor is 2.
			name: "regtest power limit",
			bits: 0x207fffff,
			want: "2",
		},
		{
			// 0x03000001: exponent 3, mantissa 1. This is the small-exponent
			// branch, where the mantissa is shifted right by 8 * (3 -
			// exponent) instead of left — here by nothing, so target = 1.
			// floor(2^256 / 2) = 2^255. No target this low is reachable from
			// the network; the case is here so the branch is covered.
			name: "smallest exponent, target 1",
			bits: 0x03000001,
			want: "57896044618658097711785492504343953926634992332820282019728792003956564819968",
		},
		{
			// arith_uint256::SetCompact reports fNegative when the sign bit
			// (0x00800000) is set on a non-zero mantissa, and GetBlockProof
			// returns 0 for it.
			name: "negative-encoded target contributes no work",
			bits: 0x1d80ffff,
			want: "0",
		},
		{
			// exponent 0x22 = 34 with mantissa 0x010000 puts the target past
			// 2^256, which is arith_uint256::SetCompact's fOverflow: 0 work.
			name: "overflowing target contributes no work",
			bits: 0x22010000,
			want: "0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want, ok := new(big.Int).SetString(tc.want, 10)
			require.True(t, ok)

			require.Equal(t, 0, want.Cmp(blockProof(tc.bits)),
				"want %s, got %s", want, blockProof(tc.bits))
		})
	}
}

// TestAddHeader_AccumulatesChainWork pins block_index.h SetChainWork:
// nChainWork = (pprev ? pprev->nChainWork : 0) + GetBlockProof(*this). Genesis
// carries its own proof and nothing else; every child adds its own on top of
// its parent's total.
func TestAddHeader_AccumulatesChainWork(t *testing.T) {
	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	root, ok := idx.Lookup(genesis.BlockHash())
	require.True(t, ok)
	require.Equal(t, big.NewInt(difficulty1Work), root.ChainWork,
		"genesis has no parent, so its chain work is its own proof")

	// Three difficulty-1 headers, then two heavy ones: the running total is
	// the hand-derived per-header proof summed along the chain.
	light := buildChain(t, idx, nc, genesis, 3)
	heavy := buildChainBits(t, idx, nc, light[2], 2, heavyBits)

	wants := []struct {
		header *wire.BlockHeader
		want   int64
	}{
		{light[0], 2 * difficulty1Work},
		{light[1], 3 * difficulty1Work},
		{light[2], 4 * difficulty1Work},
		{heavy[0], 4*difficulty1Work + heavyWork},
		{heavy[1], 4*difficulty1Work + 2*heavyWork},
	}

	for i, w := range wants {
		n, found := idx.Lookup(w.header.BlockHash())
		require.True(t, found)
		require.Equal(t, big.NewInt(w.want), n.ChainWork, "chain work at index %d", i)
	}
}

// TestAddHeader_TipFollowsMostWork pins the block_index_store.h SetBestHeader
// rule and its CBlockIndexWorkComparator (block_index.h:1225-1260): "First
// sort by most total work", with the sequence-id tail leaving the first-seen
// branch in place on a tie. It replaces the Phase 2 height rule, and with it
// the reorg case Phase 2 Task 3 deferred: a shorter branch that carries more
// work must take the tip.
func TestAddHeader_TipFollowsMostWork(t *testing.T) {
	// Branch weights, all derived from the pinned per-header proofs above:
	//   3 light headers above genesis: 3 * 4_295_032_833 = 12_885_098_499
	//   2 heavy headers above genesis: 2 * 8_589_934_591 = 17_179_869_182
	// The heavy branch is one block shorter than the light branch and still
	// outweighs it.
	const (
		lightLen = 3
		heavyLen = 2
	)

	t.Run("a taller but lighter branch does not take the tip", func(t *testing.T) {
		genesis := testGenesis()
		nc := &nonceCounter{}

		idx, err := NewHeaderIndex(genesis)
		require.NoError(t, err)

		heavy := buildChainBits(t, idx, nc, genesis, heavyLen, heavyBits)
		heavyTip := heavy[len(heavy)-1]

		hash, height := idx.Tip()
		require.Equal(t, heavyTip.BlockHash(), hash)
		require.Equal(t, int32(heavyLen), height)

		light := buildChainBits(t, idx, nc, genesis, lightLen, difficulty1Bits)
		lightTip := light[len(light)-1]

		lightNode, ok := idx.Lookup(lightTip.BlockHash())
		require.True(t, ok)
		require.Equal(t, int32(lightLen), lightNode.Height,
			"the lighter branch is taller than the heavy one")

		hash, height = idx.Tip()
		require.Equal(t, heavyTip.BlockHash(), hash, "the taller branch carries less work")
		require.Equal(t, int32(heavyLen), height)
	})

	t.Run("a shorter but heavier branch takes the tip", func(t *testing.T) {
		genesis := testGenesis()
		nc := &nonceCounter{}

		idx, err := NewHeaderIndex(genesis)
		require.NoError(t, err)

		light := buildChainBits(t, idx, nc, genesis, lightLen, difficulty1Bits)
		lightTip := light[len(light)-1]

		hash, height := idx.Tip()
		require.Equal(t, lightTip.BlockHash(), hash)
		require.Equal(t, int32(lightLen), height)

		heavy := buildChainBits(t, idx, nc, genesis, heavyLen, heavyBits)
		heavyTip := heavy[len(heavy)-1]

		hash, height = idx.Tip()
		require.Equal(t, heavyTip.BlockHash(), hash, "the heavier branch takes the tip although it is shorter")
		require.Equal(t, int32(heavyLen), height)
	})

	t.Run("equal work leaves the first-seen branch on the tip", func(t *testing.T) {
		genesis := testGenesis()
		nc := &nonceCounter{}

		idx, err := NewHeaderIndex(genesis)
		require.NoError(t, err)

		first := buildChainBits(t, idx, nc, genesis, lightLen, difficulty1Bits)
		firstTip := first[len(first)-1]

		second := buildChainBits(t, idx, nc, genesis, lightLen, difficulty1Bits)
		secondTip := second[len(second)-1]
		require.NotEqual(t, firstTip.BlockHash(), secondTip.BlockHash())

		firstNode, ok := idx.Lookup(firstTip.BlockHash())
		require.True(t, ok)

		secondNode, ok := idx.Lookup(secondTip.BlockHash())
		require.True(t, ok)
		require.Equal(t, firstNode.ChainWork, secondNode.ChainWork, "both branches carry the same work")

		hash, _ := idx.Tip()
		require.Equal(t, firstTip.BlockHash(), hash, "a tie keeps the branch that arrived first")
	})
}

// buildChain extends the index from "from" for "count" headers, requiring
// every AddHeader to connect. It returns the built headers in order.
func buildChain(t *testing.T, idx *HeaderIndex, nc *nonceCounter, from *wire.BlockHeader, count int) []*wire.BlockHeader {
	t.Helper()

	headers := make([]*wire.BlockHeader, 0, count)
	prev := from

	for i := 0; i < count; i++ {
		h := childOf(prev, nc.take())

		connected, err := idx.AddHeader(h)
		require.NoError(t, err)
		require.True(t, connected)

		headers = append(headers, h)
		prev = h
	}

	return headers
}

func TestNewHeaderIndex(t *testing.T) {
	t.Run("seeds tip at genesis", func(t *testing.T) {
		genesis := testGenesis()

		idx, err := NewHeaderIndex(genesis)
		require.NoError(t, err)

		hash, height := idx.Tip()
		require.Equal(t, genesis.BlockHash(), hash)
		require.Equal(t, int32(0), height)

		n, ok := idx.Lookup(hash)
		require.True(t, ok)
		require.Equal(t, int32(0), n.Height)
		require.Equal(t, chainhash.Hash{}, n.ParentHash)
	})

	t.Run("rejects a nil genesis header", func(t *testing.T) {
		idx, err := NewHeaderIndex(nil)
		require.Error(t, err)
		require.Nil(t, idx)
	})
}

func TestAddHeader_UnknownParent(t *testing.T) {
	genesis := testGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	// A header whose PrevBlock is not in the index anywhere: connected must
	// come back false, the tip must stay exactly where it was, and the
	// orphan must not be reachable via Lookup.
	orphanParent := chainhash.Hash{0xAA}
	zero := chainhash.Hash{}
	orphan := wire.NewBlockHeader(1, &orphanParent, &zero, 0x1d00ffff, 1)

	connected, err := idx.AddHeader(orphan)
	require.NoError(t, err)
	require.False(t, connected)

	hash, height := idx.Tip()
	require.Equal(t, genesis.BlockHash(), hash)
	require.Equal(t, int32(0), height)

	_, ok := idx.Lookup(orphan.BlockHash())
	require.False(t, ok)
}

func TestAddHeader_NilHeader(t *testing.T) {
	idx, err := NewHeaderIndex(testGenesis())
	require.NoError(t, err)

	connected, err := idx.AddHeader(nil)
	require.Error(t, err)
	require.False(t, connected)
}

func TestAddHeader_DuplicateIsIdempotent(t *testing.T) {
	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	headers := buildChain(t, idx, nc, genesis, 1)

	beforeHash, beforeHeight := idx.Tip()

	// SVNode's mapBlockIndex lookup finds the existing node for a header it
	// has already accepted; re-submitting it must report connected without
	// mutating anything.
	connected, err := idx.AddHeader(headers[0])
	require.NoError(t, err)
	require.True(t, connected)

	afterHash, afterHeight := idx.Tip()
	require.Equal(t, beforeHash, afterHash)
	require.Equal(t, beforeHeight, afterHeight)
}

// TestAddHeader_SideChainDoesNotMoveTipUnlessLonger builds every branch at the
// same difficulty target, where height and cumulative work rank identically, so
// it pins the same outcomes under the work-based tip rule as it did under the
// Phase 2 height rule. The cases where the two rules disagree are
// TestAddHeader_TipFollowsMostWork.
func TestAddHeader_SideChainDoesNotMoveTipUnlessLonger(t *testing.T) {
	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	// Main chain: genesis -> 1 -> 2 -> 3 -> 4 -> 5.
	main := buildChain(t, idx, nc, genesis, 5)
	mainTip := main[len(main)-1]

	tipHash, tipHeight := idx.Tip()
	require.Equal(t, mainTip.BlockHash(), tipHash)
	require.Equal(t, int32(5), tipHeight)

	// Side chain forks off height 2 and only reaches height 4 (2 -> 3' -> 4'):
	// shorter than the main tip, so it must extend the tree without moving
	// the tip.
	forkPoint := main[1] // height 2
	side := buildChain(t, idx, nc, forkPoint, 2)
	sideTip := side[len(side)-1]

	n, ok := idx.Lookup(sideTip.BlockHash())
	require.True(t, ok)
	require.Equal(t, int32(4), n.Height)

	tipHash, tipHeight = idx.Tip()
	require.Equal(t, mainTip.BlockHash(), tipHash)
	require.Equal(t, int32(5), tipHeight)

	// Extend the side chain past the main tip (4' -> 5' -> 6'): at equal
	// difficulty the taller branch also carries the most work, so the tip
	// must now switch.
	longerSide := buildChain(t, idx, nc, sideTip, 2)
	newSideTip := longerSide[len(longerSide)-1]

	tipHash, tipHeight = idx.Tip()
	require.Equal(t, newSideTip.BlockHash(), tipHash)
	require.Equal(t, int32(6), tipHeight)
}

func TestLocator_HeightSequences(t *testing.T) {
	// Expected sequences derived directly from chain.cpp CChain::GetLocator:
	// push the current height, stop at height 0 (genesis is always last),
	// step back by nStep each time, nStep doubles once vHave holds more
	// than 10 entries.
	tests := []struct {
		name           string
		tipHeight      int32
		expectedHeight []int32
	}{
		{"height 0 is genesis only", 0, []int32{0}},
		{"height 5 all linear", 5, []int32{5, 4, 3, 2, 1, 0}},
		{"height 10 all linear", 10, []int32{10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}},
		{"height 11 starts doubling on the 12th entry", 11, []int32{11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}},
		{
			"height 100 exponential tail", 100,
			[]int32{100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 87, 83, 75, 59, 27, 0},
		},
		{
			"height 10000 exponential tail", 10000,
			[]int32{
				10000, 9999, 9998, 9997, 9996, 9995, 9994, 9993, 9992, 9991, 9990, 9989,
				9987, 9983, 9975, 9959, 9927, 9863, 9735, 9479, 8967, 7943, 5895, 1799, 0,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			genesis := testGenesis()
			nc := &nonceCounter{}

			idx, err := NewHeaderIndex(genesis)
			require.NoError(t, err)

			var tipHeader *wire.BlockHeader = genesis
			if tc.tipHeight > 0 {
				chain := buildChain(t, idx, nc, genesis, int(tc.tipHeight))
				tipHeader = chain[len(chain)-1]
			}

			locator := idx.Locator()

			gotHeights := heightsOf(t, idx, locator)
			require.Equal(t, tc.expectedHeight, gotHeights)
			require.Equal(t, genesis.BlockHash(), locator[len(locator)-1])
			require.Equal(t, tipHeader.BlockHash(), locator[0])

			// LocatorFrom(tip hash) must agree with Locator() exactly.
			fromTip := idx.LocatorFrom(tipHeader.BlockHash())
			require.Equal(t, locator, fromTip)
		})
	}
}

// heightsOf maps each locator hash back to its node height, for asserting
// exact locator content rather than just its length.
func heightsOf(t *testing.T, idx *HeaderIndex, hashes []chainhash.Hash) []int32 {
	t.Helper()

	heights := make([]int32, len(hashes))

	for i, h := range hashes {
		n, ok := idx.Lookup(h)
		require.True(t, ok)
		heights[i] = n.Height
	}

	return heights
}

func TestLookup_ParentHashWalksOneStepAtATime(t *testing.T) {
	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	// chain[0] is height 1, chain[1] is height 2; chain[0]'s parent is genesis.
	chain := buildChain(t, idx, nc, genesis, 2)

	// A consumer in another package can walk toward genesis using only the
	// exported HeaderNode: look up a hash, then look up its ParentHash.
	n, ok := idx.Lookup(chain[1].BlockHash())
	require.True(t, ok)
	require.Equal(t, chain[0].BlockHash(), n.ParentHash)

	parent, ok := idx.Lookup(n.ParentHash)
	require.True(t, ok)
	require.Equal(t, genesis.BlockHash(), parent.ParentHash)

	grandparent, ok := idx.Lookup(parent.ParentHash)
	require.True(t, ok)
	require.Equal(t, genesis.BlockHash(), grandparent.Hash)
	require.Equal(t, chainhash.Hash{}, grandparent.ParentHash) // genesis has no parent
}

func TestLocatorFrom_UnknownHashReturnsNil(t *testing.T) {
	idx, err := NewHeaderIndex(testGenesis())
	require.NoError(t, err)

	unknown := chainhash.Hash{0xBB}
	require.Nil(t, idx.LocatorFrom(unknown))
}

func TestAncestor(t *testing.T) {
	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	chain := buildChain(t, idx, nc, genesis, 5)
	tip := chain[len(chain)-1]

	t.Run("finds an ancestor at a lower height", func(t *testing.T) {
		n, ok := idx.Ancestor(tip.BlockHash(), 2)
		require.True(t, ok)
		require.Equal(t, chain[1].BlockHash(), n.Hash) // height 2 is chain[1]
	})

	t.Run("returns itself at its own height", func(t *testing.T) {
		n, ok := idx.Ancestor(tip.BlockHash(), 5)
		require.True(t, ok)
		require.Equal(t, tip.BlockHash(), n.Hash)
	})

	t.Run("rejects a height above the node", func(t *testing.T) {
		_, ok := idx.Ancestor(chain[1].BlockHash(), 5)
		require.False(t, ok)
	})

	t.Run("rejects a negative height", func(t *testing.T) {
		_, ok := idx.Ancestor(tip.BlockHash(), -1)
		require.False(t, ok)
	})

	t.Run("rejects an unknown hash", func(t *testing.T) {
		_, ok := idx.Ancestor(chainhash.Hash{0xCC}, 0)
		require.False(t, ok)
	})
}

// naivePrevWalk walks pprev links directly from n, the pre-skiplist algorithm
// this task replaces. It is the test's independent authority for what
// Ancestor and the pskip pointers should compute, so it must not call
// ancestorLocked or share any logic with it.
func naivePrevWalk(n *node, height int32) (*node, bool) {
	if height < 0 || height > n.height {
		return nil, false
	}

	for n.height > height {
		if n.prev == nil {
			return nil, false
		}

		n = n.prev
	}

	return n, true
}

// sampleHeightsFor returns a deterministic set of heights to probe against a
// chain of length maxHeight: the boundaries (0, 1, height-1, height), every
// power of two up to maxHeight, and several heights spread across the range
// by dividing maxHeight by small primes.
func sampleHeightsFor(maxHeight int32) []int32 {
	set := map[int32]struct{}{
		0: {}, 1: {}, maxHeight - 1: {}, maxHeight: {},
	}

	for p := int32(1); p <= maxHeight; p *= 2 {
		set[p] = struct{}{}
	}

	for _, prime := range []int32{3, 5, 7, 11, 13, 17, 19, 23, 29, 31} {
		set[maxHeight/prime] = struct{}{}
	}

	heights := make([]int32, 0, len(set))

	for h := range set {
		if h >= 0 && h <= maxHeight {
			heights = append(heights, h)
		}
	}

	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })

	return heights
}

// TestAncestor_DeepChainMatchesNaiveWalk builds a 100_000-header chain (the
// depth this task's brief measured at testnet, where the pprev-only walk
// costs hours of pure pointer-chasing over a full IBD) and cross-checks
// idx.Ancestor, which is now skiplist-accelerated, against naivePrevWalk at
// many heights: both must agree at every sampled height, and both must
// reject out-of-range heights identically.
func TestAncestor_DeepChainMatchesNaiveWalk(t *testing.T) {
	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	const chainLen = 100_000

	chain := buildChain(t, idx, nc, genesis, chainLen)
	tip := chain[len(chain)-1]

	idx.mu.Lock()
	tipNode := idx.nodes[tip.BlockHash()]
	idx.mu.Unlock()
	require.Equal(t, int32(chainLen), tipNode.height)

	for _, h := range sampleHeightsFor(chainLen) {
		h := h

		t.Run(fmt.Sprintf("height=%d", h), func(t *testing.T) {
			want, wantOK := naivePrevWalk(tipNode, h)

			got, gotOK := idx.Ancestor(tip.BlockHash(), h)

			require.Equal(t, wantOK, gotOK)

			if wantOK {
				require.Equal(t, want.hash, got.Hash)
				require.Equal(t, want.height, got.Height)
			}
		})
	}

	_, ok := idx.Ancestor(tip.BlockHash(), chainLen+1)
	require.False(t, ok, "height above the tip must be rejected")

	_, ok = idx.Ancestor(tip.BlockHash(), -1)
	require.False(t, ok, "a negative height must be rejected")
}

// TestSkipPointer_MatchesGetSkipHeightRecurrence checks every node's pskip
// against the block_index.cpp BuildSkipNL recurrence directly: pskip must be
// exactly the node at height getSkipHeight(n.height), reached by walking
// pprev from n.prev — the same computation BuildSkipNL performs at insert
// time via pprev->GetAncestor(GetSkipHeight(nHeight)).
func TestSkipPointer_MatchesGetSkipHeightRecurrence(t *testing.T) {
	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	const chainLen = 5_000

	chain := buildChain(t, idx, nc, genesis, chainLen)

	idx.mu.Lock()
	defer idx.mu.Unlock()

	genesisNode := idx.nodes[genesis.BlockHash()]
	require.Nil(t, genesisNode.skip, "genesis has no pprev, so BuildSkipNL leaves pskip nil")

	for _, h := range chain {
		n := idx.nodes[h.BlockHash()]

		wantHeight := getSkipHeight(n.height)

		want, ok := naivePrevWalk(n.prev, wantHeight)
		require.True(t, ok, "height %d must be reachable from the parent of height %d", wantHeight, n.height)

		require.NotNil(t, n.skip, "node at height %d must have a skip pointer", n.height)
		require.Equal(t, want.hash, n.skip.hash)
		require.Equal(t, wantHeight, n.skip.height)
	}
}

// TestGetSkipHeight checks the recurrence itself against block_index.cpp
// GetSkipHeight (block_index.cpp:22-33) for known inputs, independent of any
// tree structure.
func TestGetSkipHeight(t *testing.T) {
	tests := []struct {
		height int32
		want   int32
	}{
		{0, 0},
		{1, 0},
		{2, 0},
		{3, 1},
		{4, 0},
		{5, 1},
		{6, 4},
		{7, 1},
		{8, 0},
		{9, 1},
		{10, 8},
		{1023, 1017},
		{1024, 0},
		{1025, 1},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("height=%d", tc.height), func(t *testing.T) {
			require.Equal(t, tc.want, getSkipHeight(tc.height))
		})
	}
}

// TestHeaderIndex_ConcurrentReadsDuringWrites is the documented exception to
// the project's default of avoiding t.Parallel()/goroutines in tests: it
// specifically exercises the "safe for concurrent reads under one mutex"
// contract by running several readers against a concurrent writer under
// -race.
//
// testify's require.* calls t.FailNow, which the testing package forbids
// calling from any goroutine but the test's own; so the writer and readers
// below record outcomes into plain values instead of asserting inline, and
// every require.* call happens back on the test's own goroutine after
// wg.Wait() has joined all of them.
func TestHeaderIndex_ConcurrentReadsDuringWrites(t *testing.T) {
	genesis := testGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	const chainLen = 500

	// Precompute the chain so the writer goroutine's loop body is just the
	// AddHeader call under test, not hashing.
	nc := &nonceCounter{}
	headers := make([]*wire.BlockHeader, 0, chainLen)
	prev := genesis

	for i := 0; i < chainLen; i++ {
		h := childOf(prev, nc.take())
		headers = append(headers, h)
		prev = h
	}

	var wg sync.WaitGroup

	done := make(chan struct{})
	addErrs := make([]error, chainLen)
	addConnected := make([]bool, chainLen)

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(done)

		for i, h := range headers {
			connected, addErr := idx.AddHeader(h)
			addErrs[i] = addErr
			addConnected[i] = connected
		}
	}()

	// Readers hammer every read method concurrently with the writer. Tip's
	// returned hash must always resolve via Lookup and Ancestor at its own
	// height, at every point in the walk: AddHeader only ever appends nodes,
	// so this invariant holds under any interleaving.
	//
	// Each snapshot's ChainWork is dereferenced rather than just fetched, and
	// that read is the point. HeaderNode shares one *big.Int with the tree
	// instead of copying it, on the promise that a node's chainWork is
	// written once at insert and never again — a promise nothing in the type
	// enforces. Reading the pointee here is what puts it under the race
	// detector: a future write to a published node's chainWork races these
	// loads, and with only the pointer copied around, -race would never see
	// it. Cmp reads the whole magnitude, not just its length.
	var (
		readerInconsistent atomic.Bool

		// Every node in this index carries at least genesis's own proof, so
		// a snapshot below it is either uninitialised or torn.
		minWork = big.NewInt(difficulty1Work)
	)

	workValid := func(n HeaderNode) bool {
		return n.ChainWork != nil && n.ChainWork.Cmp(minWork) >= 0
	}

	const readerCount = 8

	for i := 0; i < readerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
				}

				hash, height := idx.Tip()

				n, ok := idx.Lookup(hash)
				if !ok || !workValid(n) {
					readerInconsistent.Store(true)
				}

				anc, ok := idx.Ancestor(hash, height)
				if !ok || !workValid(anc) {
					readerInconsistent.Store(true)
				}

				_ = idx.Locator()
			}
		}()
	}

	wg.Wait()

	for i := range addErrs {
		require.NoError(t, addErrs[i])
		require.True(t, addConnected[i])
	}

	require.False(t, readerInconsistent.Load(), "a concurrent reader observed an inconsistent index")

	hash, height := idx.Tip()
	require.Equal(t, headers[len(headers)-1].BlockHash(), hash)
	require.Equal(t, int32(chainLen), height)

	// The whole chain runs at one difficulty, so the tip's accumulated work is
	// the genesis node plus every header the writer added.
	tip, ok := idx.Lookup(hash)
	require.True(t, ok)
	require.Equal(t, big.NewInt((chainLen+1)*difficulty1Work), tip.ChainWork)
}

// TestForkPoint pins validation.cpp FindForkInGlobalIndex's three answers
// against the branch ending at the tip the caller names.
func TestForkPoint(t *testing.T) {
	genesis := testGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	nc := &nonceCounter{}
	main := buildChain(t, idx, nc, genesis, 8)
	side := buildChain(t, idx, nc, main[2], 3) // forks off height 3

	tip := main[7].BlockHash() // height 8, the active tip for every case below

	t.Run("the first locator hash on the active chain wins", func(t *testing.T) {
		// Locators are newest-first, and the walk takes the first hit, so a
		// locator naming heights 6 then 4 forks at 6.
		fork, ok := idx.ForkPoint(tip, []chainhash.Hash{main[5].BlockHash(), main[3].BlockHash()})
		require.True(t, ok)
		require.Equal(t, main[5].BlockHash(), fork.Hash)
		require.Equal(t, int32(6), fork.Height)
	})

	t.Run("a hash we hold off the active chain is skipped", func(t *testing.T) {
		fork, ok := idx.ForkPoint(tip, []chainhash.Hash{side[2].BlockHash(), main[1].BlockHash()})
		require.True(t, ok)
		require.Equal(t, main[1].BlockHash(), fork.Hash)
	})

	t.Run("a hash descending from the tip answers the tip", func(t *testing.T) {
		// The peer is ahead of us on our own chain: serving resumes at our tip.
		fork, ok := idx.ForkPoint(main[4].BlockHash(), []chainhash.Hash{main[7].BlockHash()})
		require.True(t, ok)
		require.Equal(t, main[4].BlockHash(), fork.Hash)
	})

	t.Run("a locator naming nothing we hold answers genesis", func(t *testing.T) {
		fork, ok := idx.ForkPoint(tip, []chainhash.Hash{{0x01}, {0x02}})
		require.True(t, ok)
		require.Equal(t, genesis.BlockHash(), fork.Hash)
		require.Equal(t, int32(0), fork.Height)
	})

	t.Run("an empty locator answers genesis", func(t *testing.T) {
		fork, ok := idx.ForkPoint(tip, nil)
		require.True(t, ok)
		require.Equal(t, genesis.BlockHash(), fork.Hash)
	})

	t.Run("an unknown tip has no chain to locate on", func(t *testing.T) {
		_, ok := idx.ForkPoint(chainhash.Hash{0xFF}, []chainhash.Hash{genesis.BlockHash()})
		require.False(t, ok)
	})
}

// TestActiveChainNext pins chain.h CChain::Next: "the successor of a block in
// this chain, or nullptr if the given index is not found or is the tip".
func TestActiveChainNext(t *testing.T) {
	genesis := testGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	nc := &nonceCounter{}
	main := buildChain(t, idx, nc, genesis, 5)
	side := buildChain(t, idx, nc, main[1], 2)

	tip := main[4].BlockHash()

	next, ok := idx.ActiveChainNext(tip, main[1].BlockHash())
	require.True(t, ok)
	require.Equal(t, main[2].BlockHash(), next.Hash)

	next, ok = idx.ActiveChainNext(tip, genesis.BlockHash())
	require.True(t, ok)
	require.Equal(t, main[0].BlockHash(), next.Hash)

	// The tip itself has no successor.
	_, ok = idx.ActiveChainNext(tip, tip)
	require.False(t, ok)

	// A header we hold on another branch is not on this chain.
	_, ok = idx.ActiveChainNext(tip, side[0].BlockHash())
	require.False(t, ok)

	// Neither hash may be unknown.
	_, ok = idx.ActiveChainNext(tip, chainhash.Hash{0xAA})
	require.False(t, ok)

	_, ok = idx.ActiveChainNext(chainhash.Hash{0xAA}, tip)
	require.False(t, ok)
}

// TestActiveChainRange pins the serving walk: the range starts at start, is
// bounded by both limit and the tip, and stops dead at a start that sits off
// the branch.
func TestActiveChainRange(t *testing.T) {
	genesis := testGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	nc := &nonceCounter{}
	main := buildChain(t, idx, nc, genesis, 20)
	side := buildChain(t, idx, nc, main[2], 4)

	tip := main[19].BlockHash()

	hashesOf := func(headers []*wire.BlockHeader) []chainhash.Hash {
		out := make([]chainhash.Hash, 0, len(headers))
		for _, h := range headers {
			out = append(out, h.BlockHash())
		}

		return out
	}

	headerHashes := func(headers []wire.BlockHeader) []chainhash.Hash {
		out := make([]chainhash.Hash, 0, len(headers))
		for i := range headers {
			out = append(out, headers[i].BlockHash())
		}

		return out
	}

	t.Run("limit bounds the range", func(t *testing.T) {
		got := idx.ActiveChainHeaders(tip, main[4].BlockHash(), 6)
		require.Equal(t, hashesOf(main[4:10]), headerHashes(got))

		require.Equal(t, hashesOf(main[4:10]), idx.ActiveChainHashes(tip, main[4].BlockHash(), 6))
	})

	t.Run("the tip bounds the range", func(t *testing.T) {
		got := idx.ActiveChainHashes(tip, main[17].BlockHash(), 500)
		require.Equal(t, hashesOf(main[17:20]), got)
	})

	t.Run("a shorter active tip bounds the range", func(t *testing.T) {
		got := idx.ActiveChainHashes(main[9].BlockHash(), main[4].BlockHash(), 500)
		require.Equal(t, hashesOf(main[4:10]), got)
	})

	t.Run("genesis is a valid start", func(t *testing.T) {
		got := idx.ActiveChainHashes(tip, genesis.BlockHash(), 3)
		require.Equal(t, []chainhash.Hash{genesis.BlockHash(), main[0].BlockHash(), main[1].BlockHash()}, got)
	})

	t.Run("a start off the branch yields that header alone", func(t *testing.T) {
		// CChain::Next answers nullptr for an index the chain does not hold,
		// which ends the serving loop after its first iteration.
		got := idx.ActiveChainHashes(tip, side[1].BlockHash(), 500)
		require.Equal(t, []chainhash.Hash{side[1].BlockHash()}, got)
	})

	t.Run("a non-positive limit yields nothing", func(t *testing.T) {
		require.Empty(t, idx.ActiveChainHashes(tip, main[0].BlockHash(), 0))
		require.Empty(t, idx.ActiveChainHeaders(tip, main[0].BlockHash(), -1))
	})

	t.Run("an unknown hash yields nothing", func(t *testing.T) {
		require.Nil(t, idx.ActiveChainHashes(tip, chainhash.Hash{0xAA}, 10))
		require.Nil(t, idx.ActiveChainHeaders(chainhash.Hash{0xAA}, main[0].BlockHash(), 10))
	})

	t.Run("the headers carry the stored header, not just its hash", func(t *testing.T) {
		got := idx.ActiveChainHeaders(tip, main[6].BlockHash(), 2)
		require.Len(t, got, 2)
		require.Equal(t, *main[6], got[0])
		require.Equal(t, *main[7], got[1])
	})
}

// TestLocatorFrom_StaysOnTheRequestedBranch closes the Phase 2 Task 2 minor
// the phase-2 ledger recorded as "locator tests assert heights not branch
// identity (add LocatorFrom side-chain case later)". TestLocator_HeightSequences
// above pins the height sequence chain.cpp CChain::GetLocator produces and
// pins LocatorFrom(tip) against Locator(), but every hash it walks is on the
// one and only branch in the index, so nothing there can tell a locator built
// from the requested node apart from one built from the tip.
//
// The rule under test is chain.cpp CChain::GetLocator's walk itself: it steps
// back through pprev from the node it was given. Above the fork that is the
// side branch's own ancestry; at and below the fork the two branches share
// nodes, so the same hashes must appear.
func TestLocatorFrom_StaysOnTheRequestedBranch(t *testing.T) {
	const (
		mainLen    = 40
		forkHeight = 8
		sideLen    = mainLen - forkHeight
	)

	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	main := buildChain(t, idx, nc, genesis, mainLen)
	side := buildChain(t, idx, nc, main[forkHeight-1], sideLen)

	mainTip := main[len(main)-1]
	sideTip := side[len(side)-1]

	// Equal difficulty and equal height, so the branches carry equal work and
	// the first-seen one keeps the tip (AddHeader's strictly-greater test).
	// That is what makes the tip locator and the side-branch locator
	// distinguishable at all.
	tipHash, tipHeight := idx.Tip()
	require.Equal(t, mainTip.BlockHash(), tipHash)
	require.Equal(t, int32(mainLen), tipHeight)

	sideNode, ok := idx.Lookup(sideTip.BlockHash())
	require.True(t, ok)
	require.Equal(t, int32(mainLen), sideNode.Height, "both branch tips sit at the same height")

	// byHeight maps a height to the hash on each branch, so every locator
	// entry can be checked for branch identity and not only for height.
	mainAt := func(height int32) chainhash.Hash {
		if height == 0 {
			return genesis.BlockHash()
		}

		return main[height-1].BlockHash()
	}

	sideAt := func(height int32) chainhash.Hash {
		if height <= forkHeight {
			return mainAt(height)
		}

		return side[height-forkHeight-1].BlockHash()
	}

	locator := idx.LocatorFrom(sideTip.BlockHash())
	require.NotEmpty(t, locator)
	require.Equal(t, sideTip.BlockHash(), locator[0], "the walk starts at the node it was asked for")
	require.Equal(t, genesis.BlockHash(), locator[len(locator)-1], "genesis is always last")

	// The height sequence is a property of GetLocator's stepping, not of the
	// branch, so the side branch must produce exactly the sequence the main
	// chain at the same height does.
	require.Equal(t, heightsOf(t, idx, idx.Locator()), heightsOf(t, idx, locator))

	var aboveFork int

	for _, hash := range locator {
		n, found := idx.Lookup(hash)
		require.True(t, found)

		require.Equal(t, sideAt(n.Height), hash,
			"locator entry at height %d must be the side branch's own node", n.Height)

		if n.Height > forkHeight {
			aboveFork++

			require.NotEqual(t, mainAt(n.Height), hash,
				"above the fork the side branch and the main chain differ at height %d", n.Height)

			continue
		}

		require.Equal(t, mainAt(n.Height), hash,
			"at and below the fork both branches share the node at height %d", n.Height)
	}

	require.Greater(t, aboveFork, 1,
		"the fixture must put more than one locator entry above the fork, or branch identity is untested")

	require.NotEqual(t, idx.Locator(), locator, "the tip locator and the side-branch locator must differ")
}

// TestSkipPointer_SideChainFollowsItsOwnAncestry closes the Phase 2 Task 6b
// minor: "explicit side-chain pskip recurrence row would make
// skip-follows-pprev-ancestry explicit".
// TestSkipPointer_MatchesGetSkipHeightRecurrence above checks the recurrence
// on a single linear chain, where the node at a given height is unique — so it
// cannot distinguish "pskip is the ancestor of pprev" from "pskip is whatever
// node sits at that height".
//
// block_index.cpp BuildSkipNL (block_index.cpp:107-112) is
// pprev->GetAncestor(GetSkipHeight(nHeight)): the jump is anchored on the
// node's OWN parent, so a side-branch node's pskip is a side-branch node
// wherever the skip height is above the fork. GetAncestor over that side
// branch must then agree with a plain pprev walk at every height.
func TestSkipPointer_SideChainFollowsItsOwnAncestry(t *testing.T) {
	const (
		mainLen    = 300
		forkHeight = 9
		sideLen    = mainLen - forkHeight
	)

	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	main := buildChain(t, idx, nc, genesis, mainLen)
	side := buildChain(t, idx, nc, main[forkHeight-1], sideLen)

	sideTip := side[len(side)-1]

	idx.mu.Lock()
	defer idx.mu.Unlock()

	var skipsAboveFork int

	for _, h := range side {
		n := idx.nodes[h.BlockHash()]

		wantHeight := getSkipHeight(n.height)

		// The naive walk starts at the node's OWN parent, which is what pins
		// the branch: on the side branch it can never reach a main-chain node
		// above the fork.
		want, ok := naivePrevWalk(n.prev, wantHeight)
		require.True(t, ok, "height %d must be reachable from the parent of height %d", wantHeight, n.height)

		require.NotNil(t, n.skip, "node at height %d must have a skip pointer", n.height)
		require.Equal(t, want.hash, n.skip.hash)
		require.Equal(t, wantHeight, n.skip.height)

		if wantHeight > forkHeight {
			skipsAboveFork++

			require.NotEqual(t, main[wantHeight-1].BlockHash(), n.skip.hash,
				"the skip pointer of the side-branch node at height %d must not land on the main chain", n.height)
		}
	}

	require.Greater(t, skipsAboveFork, 1,
		"the fixture must produce more than one skip target above the fork, or branch ancestry is untested")

	// And the accelerated descent must agree with the plain walk over the
	// side branch at every sampled height, main-chain nodes included below
	// the fork.
	sideTipNode := idx.nodes[sideTip.BlockHash()]
	require.Equal(t, int32(mainLen), sideTipNode.height)

	for _, height := range sampleHeightsFor(mainLen) {
		want, wantOK := naivePrevWalk(sideTipNode, height)

		got, gotOK := ancestorLocked(sideTipNode, height)
		require.Equal(t, wantOK, gotOK)

		if wantOK {
			require.Equal(t, want.hash, got.hash, "descent from the side tip to height %d", height)
		}
	}
}
