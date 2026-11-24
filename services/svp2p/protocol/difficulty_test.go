package protocol

import (
	"math/big"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// daaChain builds the synthetic header index bitcoin-sv
// src/test/pow_tests.cpp builds with BlockIndexStore plus its GetBlockIndex
// helper: every header carries a timestamp, an nBits and a distinct nonce, and
// no proof of work is ground. The DAA reads only heights, timestamps, nBits
// and cumulative chain work, so a chain with no valid proof of work exercises
// it exactly as the C++ test does.
type daaChain struct {
	idx   *HeaderIndex
	tip   *wire.BlockHeader
	count uint32
}

// newDaaChain seeds the index with the C++ test's genesis header: nTime
// 1269211443, the given nBits, and no parent.
func newDaaChain(t *testing.T, genesisTime int64, bits uint32) *daaChain {
	t.Helper()

	zero := chainhash.Hash{}

	genesis := &wire.BlockHeader{
		Version:    1,
		PrevBlock:  zero,
		MerkleRoot: zero,
		Timestamp:  time.Unix(genesisTime, 0),
		Bits:       bits,
	}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	return &daaChain{idx: idx, tip: genesis, count: 1}
}

// extend is pow_tests.cpp GetBlockIndex: a child whose timestamp is the
// parent's plus nTimeInterval (which may be negative) and whose nNonce is the
// store's block count, so every synthetic header hashes differently.
func (c *daaChain) extend(t *testing.T, interval int64, bits uint32) {
	t.Helper()

	prev := c.tip.BlockHash()

	child := &wire.BlockHeader{
		Version:   1,
		PrevBlock: prev,
		Timestamp: c.tip.Timestamp.Add(time.Duration(interval) * time.Second),
		Bits:      bits,
		Nonce:     c.count,
	}

	connected, err := c.idx.AddHeader(child)
	require.NoError(t, err)
	require.True(t, connected)

	c.tip = child
	c.count++
}

// nextCash is GetNextCashWorkRequired(blocks.Tip(), &blkHeaderDummy, config).
// blkHeaderDummy leaves nTime at zero, which only the min-difficulty branch
// reads, and that branch is off for the mainnet params these vectors use.
func (c *daaChain) nextCash(t *testing.T, params *chaincfg.Params) uint32 {
	t.Helper()

	return c.nextCashAt(t, params, c.tip)
}

func (c *daaChain) nextCashAt(t *testing.T, params *chaincfg.Params, header *wire.BlockHeader) uint32 {
	t.Helper()

	parent, ok := c.idx.Lookup(header.BlockHash())
	require.True(t, ok)

	bits, validated := getNextCashWorkRequired(c.idx, params, parent, 0)
	require.True(t, validated, "the DAA must have enough ancestor context at height %d", parent.Height)

	return bits
}

// daaTestParams is the DummyConfig(CBaseChainParams::MAIN) the C++ DAA test
// runs against: the mainnet powLimit and 10-minute target spacing, no
// min-difficulty rule and no regtest retarget bypass. DaaForkHeight is zeroed
// so the DAA applies at every height of the synthetic chain, which is what the
// C++ test achieves by calling GetNextCashWorkRequired directly.
func daaTestParams() *chaincfg.Params {
	params := chaincfg.MainNetParams
	params.DaaForkHeight = 0

	return &params
}

// TestCompactFromTarget pins the arith_uint256::GetCompact port against
// targets whose compact form go-chaincfg states independently in PowLimitBits,
// plus the mantissa carry the source handles with `if (nCompact &
// 0x00800000)`.
func TestCompactFromTarget(t *testing.T) {
	// chaincfg states these two independently as PowLimitBits.
	require.Equal(t, chaincfg.MainNetParams.PowLimitBits, compactFromTarget(chaincfg.MainNetParams.PowLimit))
	require.Equal(t, chaincfg.RegressionNetParams.PowLimitBits, compactFromTarget(chaincfg.RegressionNetParams.PowLimit))

	// Zero encodes as zero, the source's nSize == 0 case.
	require.Equal(t, uint32(0), compactFromTarget(big.NewInt(0)))

	// 0x800000 needs the sign-bit carry: the mantissa would collide with the
	// 0x00800000 sign bit, so the source shifts it down a byte and bumps the
	// exponent, giving 0x04008000 rather than 0x03800000.
	require.Equal(t, uint32(0x04008000), compactFromTarget(big.NewInt(0x800000)))

	// A three-byte value below the sign bit keeps exponent 3.
	require.Equal(t, uint32(0x037fffff), compactFromTarget(big.NewInt(0x7fffff)))

	// Round-trip a handful of real mainnet nBits through both directions.
	for _, bits := range []uint32{0x1d00ffff, 0x1c0fe7b1, 0x1c0db19f, 0x1c2f13b9, 0x207fffff} {
		require.Equal(t, bits, compactFromTarget(testTarget(bits)), "round trip of %#x", bits)
	}
}

// TestGetNextCashWorkRequired_CashDifficultyVectors replays bitcoin-sv
// src/test/pow_tests.cpp BOOST_AUTO_TEST_CASE(cash_difficulty_test) block for
// block. Every expected nBits below is the literal value that test asserts, so
// the table comes from the C++ source and not from this port.
func TestGetNextCashWorkRequired_CashDifficultyVectors(t *testing.T) {
	params := daaTestParams()

	powLimit := params.PowLimit
	powLimitBits := compactFromTarget(powLimit)
	require.Equal(t, uint32(0x1d00ffff), powLimitBits)

	// arith_uint256 currentPow = powLimit >> 4;
	currentPow := new(big.Int).Rsh(powLimit, 4)
	initialBits := compactFromTarget(currentPow)

	chain := newDaaChain(t, 1269211443, initialBits)

	// Pile up some blocks every 10 mins to establish some history.
	// for (size_t i = 1; i < 2050; ++i)
	for i := 1; i < 2050; i++ {
		chain.extend(t, 600, initialBits)
	}

	// uint32_t nBits = GetNextCashWorkRequired(blocks[2049], ...)
	tip, ok := chain.idx.Lookup(chain.tip.BlockHash())
	require.True(t, ok)
	require.Equal(t, int32(2049), tip.Height)

	nBits := chain.nextCash(t, params)

	// Difficulty stays the same as long as we produce a block every 10 mins.
	for j := 0; j < 10; j++ {
		chain.extend(t, 600, nBits)
		require.Equal(t, nBits, chain.nextCash(t, params))
	}

	// Make sure we skip over blocks that are out of wack. To do so, we produce
	// a block that is far in the future, and then produce a block with the
	// expected timestamp.
	chain.extend(t, 6000, nBits)
	require.Equal(t, nBits, chain.nextCash(t, params))

	chain.extend(t, 2*600-6000, nBits)
	require.Equal(t, nBits, chain.nextCash(t, params))

	// The system should continue unaffected by the block with a bogous
	// timestamps.
	for j := 0; j < 20; j++ {
		chain.extend(t, 600, nBits)
		require.Equal(t, nBits, chain.nextCash(t, params))
	}

	// We start emitting blocks slightly faster. The first block has no impact.
	chain.extend(t, 550, nBits)
	require.Equal(t, nBits, chain.nextCash(t, params))

	// Now we should see difficulty increase slowly.
	for j := 0; j < 10; j++ {
		chain.extend(t, 550, nBits)
		nextBits := chain.nextCash(t, params)

		currentTarget := testTarget(nBits)
		nextTarget := testTarget(nextBits)

		// Make sure that difficulty increases very slowly.
		require.Negative(t, nextTarget.Cmp(currentTarget))
		require.Negative(t,
			new(big.Int).Sub(currentTarget, nextTarget).Cmp(new(big.Int).Rsh(currentTarget, 10)))

		nBits = nextBits
	}

	// Check the actual value.
	require.Equal(t, uint32(0x1c0fe7b1), nBits)

	// If we dramatically shorten block production, difficulty increases faster.
	for j := 0; j < 20; j++ {
		chain.extend(t, 10, nBits)
		nextBits := chain.nextCash(t, params)

		currentTarget := testTarget(nBits)
		nextTarget := testTarget(nextBits)

		// Make sure that difficulty increases faster.
		require.Negative(t, nextTarget.Cmp(currentTarget))
		require.Negative(t,
			new(big.Int).Sub(currentTarget, nextTarget).Cmp(new(big.Int).Rsh(currentTarget, 4)))

		nBits = nextBits
	}

	// Check the actual value.
	require.Equal(t, uint32(0x1c0db19f), nBits)

	// We start to emit blocks significantly slower. The first block has no
	// impact.
	chain.extend(t, 6000, nBits)
	nBits = chain.nextCash(t, params)

	// Check the actual value.
	require.Equal(t, uint32(0x1c0d9222), nBits)

	// If we dramatically slow down block production, difficulty decreases.
	for j := 0; j < 93; j++ {
		chain.extend(t, 6000, nBits)
		nextBits := chain.nextCash(t, params)

		currentTarget := testTarget(nBits)
		nextTarget := testTarget(nextBits)

		// Check the difficulty decreases.
		require.LessOrEqual(t, nextTarget.Cmp(powLimit), 0)
		require.Positive(t, nextTarget.Cmp(currentTarget))
		require.Negative(t,
			new(big.Int).Sub(nextTarget, currentTarget).Cmp(new(big.Int).Rsh(currentTarget, 3)))

		nBits = nextBits
	}

	// Check the actual value.
	require.Equal(t, uint32(0x1c2f13b9), nBits)

	// Due to the window of time being bounded, next block's difficulty actually
	// gets harder.
	chain.extend(t, 6000, nBits)
	nBits = chain.nextCash(t, params)
	require.Equal(t, uint32(0x1c2ee9bf), nBits)

	// And goes down again. It takes a while due to the window being bounded and
	// the skewed block causes 2 blocks to get out of the window.
	for j := 0; j < 192; j++ {
		chain.extend(t, 6000, nBits)
		nextBits := chain.nextCash(t, params)

		currentTarget := testTarget(nBits)
		nextTarget := testTarget(nextBits)

		// Check the difficulty decreases.
		require.LessOrEqual(t, nextTarget.Cmp(powLimit), 0)
		require.Positive(t, nextTarget.Cmp(currentTarget))
		require.Negative(t,
			new(big.Int).Sub(nextTarget, currentTarget).Cmp(new(big.Int).Rsh(currentTarget, 3)))

		nBits = nextBits
	}

	// Check the actual value.
	require.Equal(t, uint32(0x1d00ffff), nBits)

	// Once the difficulty reached the minimum allowed level, it doesn't get any
	// easier.
	for j := 0; j < 5; j++ {
		chain.extend(t, 6000, nBits)
		nextBits := chain.nextCash(t, params)

		// Check the difficulty stays constant.
		require.Equal(t, powLimitBits, nextBits)

		nBits = nextBits
	}
}

// TestGetNextWorkRequired_MinDifficultyBranch pins the
// fPowAllowMinDifficultyBlocks branch of GetNextCashWorkRequired: on a network
// that allows it, a header whose timestamp is more than 2 * nPowTargetSpacing
// past its parent's may claim the powLimit, and the DAA is not consulted.
func TestGetNextWorkRequired_MinDifficultyBranch(t *testing.T) {
	// Testnet-shaped: min difficulty allowed, retargeting on, DAA from the
	// start of the synthetic chain.
	params := chaincfg.TestNetParams
	params.DaaForkHeight = 0

	initialBits := compactFromTarget(new(big.Int).Rsh(params.PowLimit, 4))

	chain := newDaaChain(t, 1269211443, initialBits)
	for i := 0; i < 300; i++ {
		chain.extend(t, 600, initialBits)
	}

	parent, ok := chain.idx.Lookup(chain.tip.BlockHash())
	require.True(t, ok)

	powLimitBits := compactFromTarget(params.PowLimit)

	// The DAA answer for this parent, with a header timestamp inside the
	// window, is not the powLimit — otherwise the row below proves nothing.
	inWindow, validated := GetNextWorkRequired(chain.idx, &params, parent, parent.Time+1200)
	require.True(t, validated)
	require.NotEqual(t, powLimitBits, inWindow)

	// pow.cpp: "If the new block's timestamp is more than 2 * 10 minutes then
	// allow mining of a min-difficulty block." The comparison is strict, so
	// exactly 2 * nPowTargetSpacing stays on the DAA path.
	relaxed, validated := GetNextWorkRequired(chain.idx, &params, parent, parent.Time+1201)
	require.True(t, validated)
	require.Equal(t, powLimitBits, relaxed)

	// Mainnet does not allow it, so the same late timestamp keeps the DAA
	// answer.
	mainnet := daaTestParams()
	mainnetChain := newDaaChain(t, 1269211443, compactFromTarget(new(big.Int).Rsh(mainnet.PowLimit, 4)))

	for i := 0; i < 300; i++ {
		mainnetChain.extend(t, 600, compactFromTarget(new(big.Int).Rsh(mainnet.PowLimit, 4)))
	}

	mainnetParent, ok := mainnetChain.idx.Lookup(mainnetChain.tip.BlockHash())
	require.True(t, ok)

	late, validated := GetNextWorkRequired(mainnetChain.idx, mainnet, mainnetParent, mainnetParent.Time+100000)
	require.True(t, validated)
	require.NotEqual(t, compactFromTarget(mainnet.PowLimit), late)
}

// TestGetNextWorkRequired_NoRetargeting pins pow.cpp's "Special rule for
// regtest: we never retarget", which answers the parent's own nBits and takes
// precedence over every other branch, including the min-difficulty one.
func TestGetNextWorkRequired_NoRetargeting(t *testing.T) {
	params := chaincfg.RegressionNetParams
	require.True(t, params.NoDifficultyAdjustment)

	chain := newDaaChain(t, 1269211443, params.PowLimitBits)
	for i := 0; i < 200; i++ {
		chain.extend(t, 600, params.PowLimitBits)
	}

	parent, ok := chain.idx.Lookup(chain.tip.BlockHash())
	require.True(t, ok)

	bits, validated := GetNextWorkRequired(chain.idx, &params, parent, parent.Time+600)
	require.True(t, validated)
	require.Equal(t, params.PowLimitBits, bits)

	// Regtest also allows min-difficulty blocks, but fPowNoRetargeting is
	// tested first in pow.cpp, so a late timestamp changes nothing.
	late, validated := GetNextWorkRequired(chain.idx, &params, parent, parent.Time+100000)
	require.True(t, validated)
	require.Equal(t, params.PowLimitBits, late)

	// A parent below the DAA context depth still answers, because the
	// no-retarget branch needs no ancestors at all.
	shallow, ok := chain.idx.Ancestor(chain.tip.BlockHash(), 2)
	require.True(t, ok)

	bits, validated = GetNextWorkRequired(chain.idx, &params, shallow, shallow.Time+600)
	require.True(t, validated)
	require.Equal(t, params.PowLimitBits, bits)
}

// TestGetNextWorkRequired_HistoricErasNotValidated pins the deliberate scope
// limit of this port: below daaHeight the original retarget and the EDA are
// never evaluated, so the caller is told the header carries no contextual
// difficulty answer rather than being handed a wrong one.
func TestGetNextWorkRequired_HistoricErasNotValidated(t *testing.T) {
	params := chaincfg.MainNetParams
	require.Positive(t, params.DaaForkHeight)

	chain := newDaaChain(t, 1269211443, params.PowLimitBits)
	for i := 0; i < 200; i++ {
		chain.extend(t, 600, params.PowLimitBits)
	}

	parent, ok := chain.idx.Lookup(chain.tip.BlockHash())
	require.True(t, ok)
	require.Less(t, parent.Height, int32(params.DaaForkHeight))

	_, validated := GetNextWorkRequired(chain.idx, &params, parent, parent.Time+600)
	require.False(t, validated, "a pre-DAA parent must not be contextually validated")
}

// TestGetNextWorkRequired_InsufficientContext pins the policy for a parent
// whose 147-block DAA window is not fully in the index: the check is skipped,
// not failed.
func TestGetNextWorkRequired_InsufficientContext(t *testing.T) {
	params := daaTestParams()

	initialBits := compactFromTarget(new(big.Int).Rsh(params.PowLimit, 4))

	chain := newDaaChain(t, 1269211443, initialBits)
	for i := int32(0); i < daaMinParentHeight; i++ {
		chain.extend(t, 600, initialBits)
	}

	// GetSuitableBlock asserts a height of at least 3 for both the last and
	// the first block of the window, so the shallowest parent the DAA can be
	// computed for sits at 144 + 3.
	for height := int32(0); height < daaMinParentHeight; height++ {
		parent, ok := chain.idx.Ancestor(chain.tip.BlockHash(), height)
		require.True(t, ok)

		_, validated := GetNextWorkRequired(chain.idx, params, parent, parent.Time+600)
		require.False(t, validated, "height %d is below the DAA context depth", height)
	}

	deepEnough, ok := chain.idx.Ancestor(chain.tip.BlockHash(), daaMinParentHeight)
	require.True(t, ok)

	_, validated := GetNextWorkRequired(chain.idx, params, deepEnough, deepEnough.Time+600)
	require.True(t, validated, "height %d has the full window", daaMinParentHeight)

	// A HeaderNode that never came out of the index carries no chain work and
	// no ancestors, so it is reported as uncheckable rather than panicking.
	_, validated = GetNextWorkRequired(chain.idx, params, HeaderNode{Height: 600000}, 0)
	require.False(t, validated)
}
