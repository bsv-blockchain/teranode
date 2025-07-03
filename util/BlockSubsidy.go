package util

import (
	"log"

	"github.com/bsv-blockchain/go-chaincfg"
)

// GetBlockSubsidyForHeight func
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

	halvings := height / params.SubsidyReductionInterval

	// Force block reward to zero when right shift is undefined.
	if halvings >= 64 {
		return 0
	}

	subsidy := uint64(50 * 1e8)

	// Subsidy is cut in half every 210,000 blocks which will occur approximately every 4 years.
	subsidy >>= halvings

	// Additional validation - check for unexpected low values
	if subsidy < 1000000 && halvings < 10 { // Less than 0.01 BTC for early halvings is suspicious
		log.Printf("WARNING: Suspicious low subsidy %d for height %d (halvings=%d) - potential bug!", subsidy, height, halvings)
	}

	return subsidy
}
