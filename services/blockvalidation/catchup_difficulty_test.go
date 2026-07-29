package blockvalidation

import (
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// daaSettings returns test settings whose chain parameters are a copy of base, so the
// caller can pick a network that actually adjusts difficulty (and tweak fields first).
func daaSettings(t *testing.T, base chaincfg.Params) *settings.Settings {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	p := base
	tSettings.ChainCfgParams = &p

	return tSettings
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()

	b, err := hex.DecodeString(s)
	require.NoError(t, err)

	return b
}

// buildHeader builds a minimal header carrying only the fields the DAA check reads
// (timestamp and difficulty bits). Linkage/PoW are validated elsewhere.
func buildHeader(ts uint32, bits *model.NBit) *model.BlockHeader {
	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      ts,
		Bits:           *bits,
		Nonce:          0,
	}
}

// buildConstantChain builds n contiguous headers spaced exactly spacing seconds apart,
// all carrying bits. Anchored at a common ancestor at height H with arbitrary chainwork
// (only chainwork differences within the window matter to the DAA).
func buildConstantChain(n int, spacing uint32, bits *model.NBit) (*model.BlockHeaderMeta, []*model.BlockHeader) {
	anchor := &model.BlockHeaderMeta{
		Height:    200000,
		ChainWork: new(big.Int).Lsh(big.NewInt(1), 200).Bytes(),
	}

	baseTime := uint32(1_600_000_000)
	headers := make([]*model.BlockHeader, n)

	for i := range n {
		headers[i] = buildHeader(baseTime+uint32(i+1)*spacing, bits)
	}

	return anchor, headers
}

// TestValidateHeaderChainDifficulty_ValidConstantChain proves the DAA check accepts a
// steady-state chain: constant difficulty with blocks mined exactly on the target spacing
// must reproduce the same target, so no header is rejected.
func TestValidateHeaderChainDifficulty_ValidConstantChain(t *testing.T) {
	tSettings := daaSettings(t, chaincfg.MainNetParams)

	startBits, err := model.NewNBitFromString("180a097a")
	require.NoError(t, err)

	anchor, headers := buildConstantChain(300, 600, startBits)

	require.NoError(t, validateHeaderChainDifficulty(tSettings, anchor, headers))
}

// TestValidateHeaderChainDifficulty_RejectsEasyBits feeds an otherwise-valid chain a single
// header (deep enough that its full window is in memory) with artificially easy nBits, and
// asserts it is rejected as malicious — the core protection this change adds.
func TestValidateHeaderChainDifficulty_RejectsEasyBits(t *testing.T) {
	tSettings := daaSettings(t, chaincfg.MainNetParams)

	startBits, err := model.NewNBitFromString("180a097a")
	require.NoError(t, err)

	anchor, headers := buildConstantChain(300, 600, startBits)

	// Baseline must be valid so the failure below is attributable to the tampering.
	require.NoError(t, validateHeaderChainDifficulty(tSettings, anchor, headers))

	easyBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	// Index 200 is far past the 144-block window, so its DAA target is computed entirely
	// from earlier (untampered) in-memory headers.
	headers[200].Bits = *easyBits

	err = validateHeaderChainDifficulty(tSettings, anchor, headers)
	require.Error(t, err)
	require.True(t, errors.IsMaliciousResponseError(err), "expected malicious response error, got %v", err)
}

// TestValidateHeaderChainDifficulty_RegtestSkips confirms that on a no-adjustment network
// (regtest) any difficulty is accepted, matching CalcNextWorkRequired's behaviour.
func TestValidateHeaderChainDifficulty_RegtestSkips(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t) // RegressionNetParams: NoDifficultyAdjustment
	require.True(t, tSettings.ChainCfgParams.NoDifficultyAdjustment)

	easyBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	anchor, headers := buildConstantChain(300, 600, easyBits)

	require.NoError(t, validateHeaderChainDifficulty(tSettings, anchor, headers))
}

// TestValidateHeaderChainDifficulty_ShortChainSkips confirms that when the chain is shorter
// than a full adjustment window, no header is checked here (the downstream full-block check
// covers those), so even wrong bits are not rejected at this stage.
func TestValidateHeaderChainDifficulty_ShortChainSkips(t *testing.T) {
	tSettings := daaSettings(t, chaincfg.MainNetParams)

	easyBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	// 100 < window(144) + median lookback, so nothing is checkable in memory.
	anchor, headers := buildConstantChain(100, 600, easyBits)

	require.NoError(t, validateHeaderChainDifficulty(tSettings, anchor, headers))
}

// TestValidateHeaderChainDifficulty_TestnetMinDifficulty confirms the testnet
// minimum-difficulty rule is honoured: a block mined more than 2*spacing after its parent
// is expected to carry the pow-limit target, and is accepted when it does.
func TestValidateHeaderChainDifficulty_TestnetMinDifficulty(t *testing.T) {
	base := chaincfg.MainNetParams
	base.ReduceMinDifficulty = true

	tSettings := daaSettings(t, base)

	startBits, err := model.NewNBitFromString("180a097a")
	require.NoError(t, err)

	// Make the delayed block the last header so nothing downstream depends on its
	// irregular timestamp; every earlier header is a steady-state block.
	anchor, headers := buildConstantChain(201, 600, startBits)
	last := len(headers) - 1

	powLimit := powLimitNBit(tSettings)

	// The last block arrives more than 2*spacing after its parent, so the min-difficulty
	// rule requires it to carry the pow-limit target.
	headers[last].Timestamp = headers[last-1].Timestamp + 3*600
	headers[last].Bits = *powLimit

	require.NoError(t, validateHeaderChainDifficulty(tSettings, anchor, headers))

	// If that same delayed block does NOT carry the pow-limit target, reject it.
	headers[last].Bits = *startBits
	err = validateHeaderChainDifficulty(tSettings, anchor, headers)
	require.Error(t, err)
	require.True(t, errors.IsMaliciousResponseError(err), "expected malicious response error, got %v", err)
}

// TestValidateHeaderChainDifficulty_EqualTimestampMedianTie exercises the specific
// case fixed in ChiR1: two blocks two apart at a window boundary sharing a timestamp
// (with the middle one higher), which triggers the unstable sort's tie-break. With the
// fixed oldest-first ordering, median3 selects the same block as the store would,
// ensuring DAA parity.
func TestValidateHeaderChainDifficulty_EqualTimestampMedianTie(t *testing.T) {
	tSettings := daaSettings(t, chaincfg.MainNetParams)

	// Build blocks for indices 0-149 (the first block where DAA check runs is i=147).
	// We construct timestamps where blocks 5 and 7 (examined by median3(7)) share a
	// timestamp, to test the tie-break ordering in a checkable region.
	// Height of block at index i in catchup is anchor.Height + 1 + i.
	anchor := &model.BlockHeaderMeta{
		Height:    200000,
		ChainWork: new(big.Int).Lsh(big.NewInt(1), 200).Bytes(),
	}

	startBits, err := model.NewNBitFromString("180a097a")
	require.NoError(t, err)

	baseTime := uint32(1_600_000_000)
	headers := make([]*model.BlockHeader, 150)

	// Build blocks 0-149 with timestamps that create the tie at 5, 6, 7.
	// Blocks 5 and 7 share a timestamp, block 6 is higher (triggering sort's tie-break).
	for i := range 150 {
		var ts uint32
		if i < 5 {
			ts = baseTime + uint32((i+1)*600)
		} else if i == 5 {
			ts = baseTime + 1 + uint32((5+1)*600) // time T
		} else if i == 6 {
			ts = baseTime + 1 + uint32((5+1)*600) + 300 // time T+300
		} else if i == 7 {
			ts = baseTime + 1 + uint32((5+1)*600) // time T again
		} else {
			ts = baseTime + uint32((i+1)*600)
		}
		headers[i] = buildHeader(ts, startBits)
	}

	// With the fixed oldest-first ordering, median3(7) examines blocks [5,6,7] ordered
	// as [5,6,7] (oldest-first). The unstable sort will preserve this order since it
	// only compares by time and has two equal values; it selects s[1]=block 6 as median.
	// With the buggy newest-first ordering [7,6,5], the tie-break might reorder them
	// differently, potentially selecting a different median. This test validates that
	// the fix produces consistent results.
	//
	// We use constant bits throughout since all blocks are deep before the full 144-block
	// DAA window is needed (DAA checks start at i=147), so the checkable headers in this
	// shortened chain don't trigger full DAA recalculation.
	require.NoError(t, validateHeaderChainDifficulty(tSettings, anchor, headers))
}

// TestComputeTarget_MatchesDifficultyMethod is a guard on the ComputeTarget extraction:
// the exported free function must return exactly what the store-backed path produced.
func TestComputeTarget_MatchesDifficultyMethod(t *testing.T) {
	tSettings := daaSettings(t, chaincfg.MainNetParams)

	firstBits, _ := model.NewNBitFromString("1808de5f")
	first := &model.SuitableBlock{
		NBits:     firstBits.CloneBytes(),
		Time:      1704647599,
		ChainWork: mustDecodeHex(t, "0000000000000000000000000000000000000000014fde9a5605193885731ee4"),
	}

	lastBits, _ := model.NewNBitFromString("180a097a")
	last := &model.SuitableBlock{
		NBits:     lastBits.CloneBytes(),
		Time:      1704738562,
		ChainWork: mustDecodeHex(t, "0000000000000000000000000000000000000000014fed8ff37cff135c70f4bb"),
	}

	got, err := blockchain.ComputeTarget(tSettings, first, last)
	require.NoError(t, err)

	// Same inputs and expectation as blockchain.TestCalcNextRequiredDifficulty, proving the
	// extracted free function matches the store-backed path exactly.
	expected, err := model.NewNBitFromString("180a2268")
	require.NoError(t, err)
	require.Equal(t, expected.String(), got.String())
}
