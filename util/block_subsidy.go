package util

import (
	"log"

	"github.com/bsv-blockchain/go-chaincfg"
)

const (
	// initialSubsidy is the initial block subsidy in satoshis (50 BTC = 50 * 100,000,000 satoshis)
	initialSubsidy uint64 = 50 * 100_000_000 // Do not use 50 * 1e8 here as it will default to a float result
	// maxHalvings is the maximum number of halvings before subsidy becomes 0
	maxHalvings uint64 = 64
	// minReasonableSubsidy is the minimum reasonable subsidy for early halvings (0.01 BTC)
	minReasonableSubsidy uint64 = 1_000_000
	// maxReasonableHalvings is the maximum number of halvings for validation warnings
	maxReasonableHalvings uint64 = 10
)

// GetBlockSubsidyForHeight calculates the block subsidy (coinbase reward) for a given block height.
// The subsidy starts at 50 BTC and halves every SubsidyReductionInterval blocks (typically 210,000).
// Returns 0 if the halvings exceed 64 or if chain parameters are invalid.
func GetBlockSubsidyForHeight(height uint32, params *chaincfg.Params) uint64 {
	// Validate input parameters
	if params == nil {
		log.Printf("ERROR: ChainCfgParams is nil - this should never happen")
		return 0
	}

	if params.SubsidyReductionInterval == 0 {
		log.Printf("WARNING: SubsidyReductionInterval is 0 - returning 0 to avoid division by zero")
		return 0
	}

	halvings := uint64(height) / uint64(params.SubsidyReductionInterval)

	// Force block reward to zero when right shift is undefined.
	if halvings >= maxHalvings {
		return 0
	}

	subsidy := initialSubsidy

	// Subsidy is cut in half every 210,000 blocks which will occur approximately every 4 years.
	subsidy >>= halvings

	// Additional validation - check for unexpected low values
	if subsidy < minReasonableSubsidy && halvings < maxReasonableHalvings {
		log.Printf("WARNING: Suspicious low subsidy %d for height %d (halvings=%d) - potential bug!", subsidy, height, halvings)
	}

	return subsidy
}
