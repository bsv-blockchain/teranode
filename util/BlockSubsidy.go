package util

import "github.com/bitcoin-sv/teranode/chaincfg"

// GetBlockSubsidyForHeight func
func GetBlockSubsidyForHeight(height uint32, params *chaincfg.Params) uint64 {
	halvings := height / uint32(params.SubsidyReductionInterval) //nolint:gosec
	// Force block reward to zero when right shift is undefined.
	if halvings >= 64 {
		return 0
	}

	subsidy := uint64(50 * 1e8)

	// Subsidy is cut in half every 210,000 blocks which will occur approximately every 4 years.
	subsidy >>= halvings

	return subsidy
}
