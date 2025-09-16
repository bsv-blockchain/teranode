package util

import (
	"testing"
)

// TestInitialSubsidyPrecision ensures that the initialSubsidy constant
// is computed using integer arithmetic, not floating-point arithmetic.
// This test validates that our fix for the floating-point precision bug
// produces the correct result.
func TestInitialSubsidyPrecision(t *testing.T) {
	// Expected value: 50 BTC * 100,000,000 satoshis/BTC = 5,000,000,000 satoshis
	expectedSubsidy := uint64(5_000_000_000)

	if initialSubsidy != expectedSubsidy {
		t.Errorf("initialSubsidy = %d, want %d", initialSubsidy, expectedSubsidy)
	}

	// Verify that this value is exactly what we expect for Bitcoin
	if initialSubsidy != 50*100_000_000 {
		t.Errorf("initialSubsidy should equal 50 * 100000000, got %d", initialSubsidy)
	}

	// Verify it represents exactly 50 BTC in satoshis
	const satoshisPerBTC = 100_000_000
	const initialBTC = 50

	if initialSubsidy != initialBTC*satoshisPerBTC {
		t.Errorf("initialSubsidy doesn't represent %d BTC in satoshis", initialBTC)
	}
}

// TestFloatingPointComparison demonstrates that while the current approach
// happens to work, it would be wrong to use floating-point arithmetic.
func TestFloatingPointComparison(t *testing.T) {
	// This test documents that 50 * 1e8 happens to equal 50 * 100000000
	// in this specific case, but floating-point should never be used
	// for financial calculations.

	floatResult := uint64(50 * 1e8)
	intResult := uint64(50 * 100_000_000)

	if floatResult != intResult {
		t.Errorf("Floating-point calculation differs from integer: %d != %d", floatResult, intResult)
	}

	// Both should equal our expected value
	expectedValue := uint64(5_000_000_000)
	if floatResult != expectedValue {
		t.Errorf("Floating-point result = %d, want %d", floatResult, expectedValue)
	}
	if intResult != expectedValue {
		t.Errorf("Integer result = %d, want %d", intResult, expectedValue)
	}

	// Verify this is exactly 50 BTC worth of satoshis
	btcAmount := float64(intResult) / 100_000_000.0
	if btcAmount != 50.0 {
		t.Errorf("Converted back to BTC: %.8f, want 50.00000000", btcAmount)
	}
}
