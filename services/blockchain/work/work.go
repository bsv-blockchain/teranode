// Package work provides utilities for calculating blockchain proof-of-work values.
//
// This package implements the work calculation algorithms used in the Bitcoin blockchain
// to determine the cumulative proof-of-work for blocks and chains. The work calculation
// is fundamental to the consensus mechanism, as it determines which chain has the most
// accumulated computational effort and should be considered the valid chain.
//
// The work value represents the expected number of hash operations required to produce
// a block with the given difficulty target. Higher work values indicate more computational
// effort was expended to create the block.
package work

import (
	"math/big"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// CalculateWork calculates the cumulative work for a block given the previous work and difficulty.
// This function computes the total proof-of-work by adding the work required for the current
// block (based on its difficulty target) to the cumulative work of all previous blocks.
//
// The work calculation follows the Bitcoin protocol where work is inversely proportional
// to the difficulty target. A lower target (higher difficulty) requires more work to find
// a valid block hash. The formula used is:
//
//	work = 2^256 / target
//	cumulative_work = previous_work + work
//
// This cumulative work value is used by the consensus algorithm to determine which chain
// has the most proof-of-work and should be considered the main chain.
//
// Parameters:
//   - prevWork: The cumulative work hash from all previous blocks in the chain
//   - nBits: The difficulty target for the current block in compact representation
//
// Returns:
//   - *chainhash.Hash: The new cumulative work value as a hash
//   - error: Any error encountered during the calculation (currently always nil)
func CalculateWork(prevWork *chainhash.Hash, nBits model.NBit) (*chainhash.Hash, error) {
	target := nBits.CalculateTarget()

	// Work done is proportional to 1/difficulty
	work := new(big.Int).Div(new(big.Int).Lsh(big.NewInt(1), 256), target)

	// Add to previous work
	newWork := new(big.Int).Add(new(big.Int).SetBytes(bt.ReverseBytes(prevWork.CloneBytes())), work)

	b := bt.ReverseBytes(newWork.Bytes())
	hash := &chainhash.Hash{}
	copy(hash[:], b)

	return hash, nil
}
