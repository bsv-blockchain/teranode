package blockchain

import (
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/bitcoin-sv/teranode/services/blockchain/work"
)

// TestVerifyBlocks650284To650286Standalone verifies chainwork calculations with all data hardcoded
// This test does not require database access and shows exactly where the chainwork error begins
func TestVerifyBlocks650284To650286Standalone(t *testing.T) {
	t.Logf("=== Standalone Verification of Blocks 650,284, 650,285, and 650,286 ===\n")

	// Test data structure with all data from database and API
	type blockData struct {
		height             int64
		hash               string // from database (big-endian)
		nBitsHex           string // from database (little-endian hex)
		nBits              uint32 // parsed value
		storedChainworkHex string // from database
		correctChainwork   string // from WhatsOnChain API
		prevChainwork      string // correct chainwork of previous block
	}

	// All data extracted from database and verified against WhatsOnChain API
	blocks := []blockData{
		{
			height:             650284,
			hash:               "000000000000000002a0c3c4772a2e0570fb0f4157b4c30d6657352def6cb807",
			nBitsHex:           "0f090418",                                                         // little-endian from DB
			nBits:              0x1804090f,                                                         // parsed to big-endian
			storedChainworkHex: "0000000000000000000000000000000000000000011840cdf295813706aeb278", // from DB
			correctChainwork:   "0000000000000000000000000000000000000000011840cdf295813706aeb278", // from API
			prevChainwork:      "00000000000000000000000000000000000000000118408e824026832f755a95", // block 650283
		},
		{
			height:             650285,
			hash:               "0000000000000000033f8440b2c5bf38690b0709c1b299448b1c283bb7a72fba",
			nBitsHex:           "da0d0418",                                                         // little-endian from DB
			nBits:              0x18040dda,                                                         // parsed to big-endian
			storedChainworkHex: "00000000000000000000000000000000000000000118410d17eab99b3f9f8488", // from DB
			correctChainwork:   "00000000000000000000000000000000000000000118410d17eab99b3f9f8488", // from API
			prevChainwork:      "0000000000000000000000000000000000000000011840cdf295813706aeb278", // block 650284
		},
		{
			height:             650286,
			hash:               "000000000000000000119a1c58bc787a372a011d838f3061f94eca7d99718013",
			nBitsHex:           "00000418",                                                         // little-endian from DB
			nBits:              0x18040000,                                                         // parsed to big-endian - THE SPECIAL VALUE!
			storedChainworkHex: "00000000000000000000000000000000000000000118414d17eab99b3f9f8488", // from DB (WRONG!)
			correctChainwork:   "00000000000000000000000000000000000000000118414d17eab99b3f9f8487", // from API (CORRECT)
			prevChainwork:      "00000000000000000000000000000000000000000118410d17eab99b3f9f8488", // block 650285
		},
	}

	for _, block := range blocks {
		t.Logf("Block %d:", block.height)
		t.Logf("  Hash: %s", block.hash[:32]+"...")
		t.Logf("  nBits (hex from DB): %s (little-endian)", block.nBitsHex)
		t.Logf("  nBits (parsed):      0x%08x", block.nBits)

		// Parse nBits from little-endian hex (as stored in database)
		nBitsBytes, _ := hex.DecodeString(block.nBitsHex)
		var parsedNBits uint32
		if len(nBitsBytes) == 4 {
			// Convert from little-endian bytes to uint32
			parsedNBits = uint32(nBitsBytes[0]) | uint32(nBitsBytes[1])<<8 |
				uint32(nBitsBytes[2])<<16 | uint32(nBitsBytes[3])<<24
		}

		// Verify parsing matches expected
		if parsedNBits != block.nBits {
			t.Errorf("  ✗ nBits parsing error! Expected 0x%08x, got 0x%08x", block.nBits, parsedNBits)
		}

		// Calculate work using both formulas
		prevChainwork, _ := new(big.Int).SetString(block.prevChainwork, 16)

		// With +1 (correct formula)
		workWithFix := work.CalcBlockWork(block.nBits)
		chainworkWithFix := new(big.Int).Add(prevChainwork, workWithFix)

		// Without +1 (buggy formula)
		workWithoutFix := calculateWorkWithoutPlusOneStandalone(block.nBits)
		chainworkWithoutFix := new(big.Int).Add(prevChainwork, workWithoutFix)

		// Parse stored and correct chainwork
		storedWork, _ := new(big.Int).SetString(block.storedChainworkHex, 16)
		correctWork, _ := new(big.Int).SetString(block.correctChainwork, 16)

		t.Logf("\n  Work calculations:")
		t.Logf("    Block work (with +1):    %064x", workWithFix)
		t.Logf("    Block work (without +1): %064x", workWithoutFix)

		// Check if formulas differ
		workDiff := new(big.Int).Sub(workWithoutFix, workWithFix)
		if workDiff.Cmp(big.NewInt(0)) == 0 {
			t.Logf("    ✓ Both formulas give SAME work (no error possible)")
		} else {
			t.Logf("    ✗ FORMULAS DIFFER by %d", workDiff)
		}

		t.Logf("\n  Chainwork comparison:")
		t.Logf("    Previous block:       %s", block.prevChainwork)
		t.Logf("    Correct (from API):   %s", block.correctChainwork)
		t.Logf("    Calc with +1:         %064x", chainworkWithFix)
		t.Logf("    Calc without +1:      %064x", chainworkWithoutFix)
		t.Logf("    Stored in DB:         %s", block.storedChainworkHex)

		// Check matches
		calcWithFixMatches := chainworkWithFix.Cmp(correctWork) == 0
		calcWithoutFixMatches := chainworkWithoutFix.Cmp(correctWork) == 0
		storedMatchesCorrect := storedWork.Cmp(correctWork) == 0
		storedMatchesWithout := storedWork.Cmp(chainworkWithoutFix) == 0

		t.Logf("\n  Results:")
		if calcWithFixMatches {
			t.Logf("    ✓ Calculation WITH +1 matches correct chainwork")
		} else {
			t.Logf("    ✗ Calculation WITH +1 does NOT match")
		}

		if calcWithoutFixMatches {
			t.Logf("    ✓ Calculation WITHOUT +1 matches correct chainwork")
		} else {
			t.Logf("    ✗ Calculation WITHOUT +1 does NOT match")
		}

		if storedMatchesCorrect {
			t.Logf("    ✓ Stored chainwork is CORRECT")
		} else {
			diff := new(big.Int).Sub(storedWork, correctWork)
			t.Logf("    ✗ Stored chainwork is WRONG (off by %d)", diff)
			if storedMatchesWithout {
				t.Logf("    >>> Database was populated using formula WITHOUT +1")
			}
		}

		// Analyze why this block causes an error
		analyzeBlock(t, block.nBits)
		t.Logf("")
	}

	// Final summary
	t.Logf("=== SUMMARY ===")
	t.Logf("Block 650,284: ✓ Correct - both formulas give same result")
	t.Logf("Block 650,285: ✓ Correct - both formulas give same result")
	t.Logf("Block 650,286: ✗ ERROR BEGINS HERE")
	t.Logf("  - nBits 0x18040000 has mantissa 0x040000 (exactly 2^18)")
	t.Logf("  - Creates target that divides evenly into 2^256")
	t.Logf("  - First block where formulas give different results")
	t.Logf("  - Database chainwork is 1 higher than correct")
	t.Logf("\nThis proves the database was populated with the buggy formula missing +1")
}

// calculateWorkWithoutPlusOneStandalone calculates work using the buggy formula (without +1)
func calculateWorkWithoutPlusOneStandalone(nBits uint32) *big.Int {
	// Extract mantissa and exponent from compact format
	mantissa := nBits & 0x007fffff
	exponent := uint(nBits >> 24)

	// Calculate target
	target := big.NewInt(int64(mantissa))
	if exponent > 3 {
		target.Lsh(target, 8*(exponent-3))
	}

	// Calculate work WITHOUT the +1 (buggy formula)
	maxTarget := new(big.Int).Lsh(big.NewInt(1), 256)
	work := new(big.Int).Div(maxTarget, target)

	return work
}

// analyzeBlock explains why this block's nBits causes different results
func analyzeBlock(t *testing.T, nBits uint32) {
	mantissa := nBits & 0x007fffff
	exponent := uint(nBits >> 24)

	// Calculate target
	target := big.NewInt(int64(mantissa))
	if exponent > 3 {
		target.Lsh(target, 8*(exponent-3))
	}

	// Check if 2^256 divides evenly by target
	maxTarget := new(big.Int).Lsh(big.NewInt(1), 256)
	quotient, remainder := new(big.Int).DivMod(maxTarget, target, new(big.Int))

	t.Logf("\n  Technical analysis:")
	t.Logf("    Mantissa: 0x%06x", mantissa)
	t.Logf("    Exponent: %d", exponent)
	t.Logf("    Target: %064x", target)

	if remainder.Cmp(big.NewInt(0)) == 0 {
		t.Logf("    >>> PERFECT DIVISOR DETECTED!")
		t.Logf("    >>> 2^256 / target = %064x (no remainder)", quotient)
		t.Logf("    >>> This causes different results with/without +1")

		// Check if mantissa is a power of 2
		if mantissa != 0 && (mantissa&(mantissa-1)) == 0 {
			// Find which power of 2
			power := 0
			for m := mantissa; m > 1; m >>= 1 {
				power++
			}
			t.Logf("    >>> Mantissa 0x%06x = 2^%d (power of 2)", mantissa, power)
		}
	} else {
		t.Logf("    Normal block: 2^256 / target has remainder")
		t.Logf("    Both formulas produce same result due to integer truncation")
	}
}
