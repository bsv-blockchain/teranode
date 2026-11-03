package netsync

import (
	"math/big"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
)

// minimumChainWork is SV Node v1.1.0's consensus.nMinimumChainWork for the
// chain, or nil for a chain SV Node does not define. It belongs in go-chaincfg's
// Params beside the checkpoints; until it is there, this table carries it.
//
//   - mainnet: src/chainparams.cpp:113-114
//   - testnet: src/chainparams.cpp:379-380
//   - stn: src/chainparams.cpp:275, zero
//   - regtest: src/chainparams.cpp:503, zero
//
// Zero is returned as nil: a gate at zero refuses nothing.
func minimumChainWork(params *chaincfg.Params) *big.Int {
	if params == nil {
		return nil
	}

	var hex string

	switch params.Net {
	case wire.MainNet:
		hex = "000000000000000000000000000000000000000000a0f3064330647e2f6c4828"
	case wire.TestNet:
		hex = "00000000000000000000000000000000000000000000002a650f6ff7649485da"
	default:
		return nil
	}

	work, ok := new(big.Int).SetString(hex, 16)
	if !ok {
		return nil
	}

	return work
}
