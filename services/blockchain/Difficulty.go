// Package blockchain provides functionality for managing the Bitcoin blockchain.
package blockchain

import (
	"context"
	"encoding/binary"
	"math/big"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/go-utils"
)

// DifficultyAdjustmentWindow defines the number of blocks to consider for difficulty adjustment.
const DifficultyAdjustmentWindow = 144

// Removed global variables bigOne and oneLsh256 as they are no longer needed
// The CalcWork function has been moved to the work package as CalcBlockWork

// Difficulty handles the calculation and management of blockchain mining difficulty.
// Implements the Bitcoin SV Difficulty Adjustment Algorithm (DAA) with caching for performance.
//
// Thread-safe: uses sync.RWMutex to protect cached values.
type Difficulty struct {
	powLimitnBits *model.NBit
	// lastSlowBlockHash    *chainhash.Hash
	logger            ulogger.Logger
	store             blockchain_store.Store
	settings          *settings.Settings
	mu                sync.RWMutex
	lastBlockHash     *chainhash.Hash
	lastComputednBits *model.NBit
}

// NewDifficulty creates a new Difficulty instance with the provided dependencies.
func NewDifficulty(store blockchain_store.Store, logger ulogger.Logger, tSettings *settings.Settings) (*Difficulty, error) {
	d := &Difficulty{}
	d.settings = tSettings

	bytesLittleEndian := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesLittleEndian, tSettings.ChainCfgParams.PowLimitBits)
	d.powLimitnBits, _ = model.NewNBitFromSlice(bytesLittleEndian)

	d.logger = logger
	d.store = store

	return d, nil
}

// CalcNextWorkRequired calculates the required proof of work for the next block.
// Parameters:
//   - ctx: Context for the operation
//   - blockHeader: Block header to calculate the next difficulty for
//   - blockHeight: Height of the block
//   - currentBlockHeight: Current block time for testnet difficulty calculation
//
// Returns the calculated NBit target difficulty.
func (d *Difficulty) CalcNextWorkRequired(ctx context.Context, blockHeader *model.BlockHeader, blockHeight uint32, currentBlockTime int64) (*model.NBit, error) {
	// If regtest we don't adjust the difficulty
	if d.settings.ChainCfgParams.NoDifficultyAdjustment {
		return &blockHeader.Bits, nil
	}

	// Special difficulty rule for testnet:
	// If the new block's timestamp is more than 2 * target spacing after the previous block,
	// allow mining of a min-difficulty block.
	// This matches bitcoin-sv's pow.cpp implementation.
	if d.settings.ChainCfgParams.ReduceMinDifficulty {
		prevBlockTime := int64(blockHeader.Timestamp)
		targetSpacing := int64(d.settings.ChainCfgParams.TargetTimePerBlock.Seconds())

		if currentBlockTime > prevBlockTime+(2*targetSpacing) {
			d.logger.Debugf("[Difficulty] Testnet minimum difficulty rule triggered: new block time %d > prev %d + 2*%d", currentBlockTime, prevBlockTime, targetSpacing)
			return d.powLimitnBits, nil
		}
	}

	if blockHeight < uint32(DifficultyAdjustmentWindow)+4 {
		d.logger.Debugf("[Difficulty] not enough blocks to calculate difficulty adjustment")
		// not enough blocks to calculate difficulty adjustment
		// set to start difficulty

		return d.powLimitnBits, nil
	}

	// Simple cache: if we're calculating for the same block header as last time, return cached result
	if !d.settings.ChainCfgParams.ReduceMinDifficulty && d.settings.BlockAssembly.DifficultyCache {
		// if bestBlockHash is set and it's the same as the blockHeader.Hash(), we don't need to recalculate the difficulty,
		// just send the one we have if it's set
		d.mu.RLock()
		if d.lastBlockHash != nil && d.lastBlockHash.IsEqual(blockHeader.Hash()) {
			d.logger.Debugf("[Difficulty] blockHash is the same as last calculation, returning cached difficulty")

			if d.lastComputednBits != nil {
				cachedBits := d.lastComputednBits
				d.mu.RUnlock()
				return cachedBits, nil
			}
		}
		d.mu.RUnlock()
	}

	d.logger.Debugf("[Difficulty] blockHeader.Hash: %s, blockHeight: %d, blockHeader.Time: %d", blockHeader.Hash().String(), blockHeight, blockHeader.Timestamp)

	lastSuitableBlock, err := d.store.GetSuitableBlock(ctx, blockHeader.Hash())
	if err != nil {
		return nil, errors.NewStorageError("[Difficulty] error getting suitable block", err)
	}

	if lastSuitableBlock == nil {
		return nil, errors.NewProcessingError("[Difficulty] lastSuitableBlock is nil", nil)
	}

	d.logger.Debugf("[Difficulty] lastSuitableBlock.Hash: %s, lastSuitableBlock.Height: %d, lastSuitableBlock.Time: %d", utils.ReverseAndHexEncodeSlice(lastSuitableBlock.Hash), lastSuitableBlock.Height, lastSuitableBlock.Time)

	ancestorHash, err := d.store.GetHashOfAncestorBlock(ctx, blockHeader.Hash(), DifficultyAdjustmentWindow)
	if err != nil {
		// Only use fallback if chain is too short (ErrNotFound).
		// For other errors (timeouts, DB errors), we must return an error
		// to avoid calculating incorrect difficulty.
		if errors.Is(err, errors.ErrNotFound) {
			d.logger.Debugf("[Difficulty] chain too short to get ancestor %d blocks back, using current block hash", DifficultyAdjustmentWindow)
			ancestorHash = blockHeader.Hash()
		} else {
			// Don't silently fall back for timeouts or other errors - this causes wrong difficulty!
			return nil, errors.NewStorageError("[Difficulty] error getting ancestor block (not using fallback to prevent incorrect difficulty)", err)
		}
	}

	d.logger.Debugf("[Difficulty] ancestorHash: %s", ancestorHash.String())

	firstSuitableBlock, err := d.store.GetSuitableBlock(ctx, ancestorHash)
	if err != nil {
		return nil, errors.NewStorageError("[Difficulty] error getting suitable block", err)
	}

	if firstSuitableBlock == nil {
		return d.powLimitnBits, nil
	}

	d.logger.Debugf("[Difficulty] firstSuitableBlock.Hash: %s, firstSuitableBlock.Height: %d, firstSuitableBlock.Time: %d", utils.ReverseAndHexEncodeSlice(firstSuitableBlock.Hash), firstSuitableBlock.Height, firstSuitableBlock.Time)

	nBits, err := d.computeTarget(firstSuitableBlock, lastSuitableBlock)
	if err != nil {
		return nil, errors.NewProcessingError("[Difficulty] error calculating next required difficulty", err)
	}

	d.mu.Lock()
	d.lastComputednBits = nBits
	d.lastBlockHash = blockHeader.Hash()
	d.mu.Unlock()

	return nBits, nil
}

// computeTarget calculates the target difficulty based on the first and last suitable blocks.
// This implements the DAA (Difficulty Adjustment Algorithm) as used in bitcoin-sv.
// The algorithm uses chainwork differences to calculate the new target, similar to
// the ComputeTarget function in bitcoin-sv's pow.cpp.
// Parameters:
//   - suitableFirstBlock: First block in the difficulty adjustment window
//   - suitableLastBlock: Last block in the difficulty adjustment window
func (d *Difficulty) computeTarget(suitableFirstBlock *model.SuitableBlock, suitableLastBlock *model.SuitableBlock) (*model.NBit, error) {
	lastSuitableBits, _ := model.NewNBitFromSlice(suitableLastBlock.NBits)
	// If regtest we don't adjust the difficulty
	if d.settings.ChainCfgParams.NoDifficultyAdjustment {
		d.logger.Debugf("no difficulty adjustment - returning %v", lastSuitableBits)
		return lastSuitableBits, nil
	}

	// Implement the DAA algorithm using chainwork (similar to bitcoin-sv's ComputeTarget)
	firstChainwork := new(big.Int).SetBytes(suitableFirstBlock.ChainWork)
	lastChainwork := new(big.Int).SetBytes(suitableLastBlock.ChainWork)

	work := new(big.Int).Sub(lastChainwork, firstChainwork)
	d.logger.Debugf("work: %s", work.String())

	// In order to avoid difficulty cliffs, we bound the amplitude of the
	// adjustment we are going to do.
	d.logger.Debugf("suitableLastBlock.Height: %d, suitableFirstBlock.Height: %d", suitableLastBlock.Height, suitableFirstBlock.Height)
	d.logger.Debugf("suitableLastBlock.Time: %d, suitableFirstBlock.Time: %d", suitableLastBlock.Time, suitableFirstBlock.Time)

	duration := int64(suitableLastBlock.Time - suitableFirstBlock.Time)
	if duration > 288*int64(d.settings.ChainCfgParams.TargetTimePerBlock.Seconds()) {
		d.logger.Debugf("duration %d is greater than 288 * target time per block %d - setting to 288 * target time per block", duration, d.settings.ChainCfgParams.TargetTimePerBlock.Seconds())
		duration = 288 * int64(d.settings.ChainCfgParams.TargetTimePerBlock.Seconds())
	} else if duration < 72*int64(d.settings.ChainCfgParams.TargetTimePerBlock.Seconds()) {
		d.logger.Debugf("duration %d is less than 72 * target time per block %d - setting to 72 * target time per block", duration, d.settings.ChainCfgParams.TargetTimePerBlock.Seconds())
		duration = 72 * int64(d.settings.ChainCfgParams.TargetTimePerBlock.Seconds())
	}

	// Calculate the projected work by multiplying the current work by the target time per block (in seconds).
	projectedWork := new(big.Int).Mul(work, big.NewInt(int64(d.settings.ChainCfgParams.TargetTimePerBlock.Seconds())))

	// Divide the projected work by the actual time duration between the blocks to get the adjusted work.
	// check if duration is zero
	if duration == 0 {
		d.logger.Debugf("duration is zero - returning %v", lastSuitableBits)
		return lastSuitableBits, nil
	}

	pw := new(big.Int).Div(projectedWork, big.NewInt(duration))

	// Calculate the new difficulty target using the formula: target = (2^256 - work) / work
	// This is mathematically equivalent to the C++ implementation in bitcoin-sv's pow.cpp
	// which uses: return (-work) / work;
	// In 256-bit unsigned arithmetic, -work is equivalent to 2^256 - work (two's complement)
	// This formula effectively computes the inverse of work scaled to fit within 256 bits

	// Calculate 2^256, which is the maximum possible value in a 256-bit space (used for Bitcoin's difficulty target).
	e := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)

	// Subtract the adjusted work from 2^256 to get the new numerator for the target calculation.
	nt := new(big.Int).Sub(e, pw)

	// Calculate the new target by dividing the result by the adjusted work. This gives the new difficulty target.
	// check if pw is zero
	if pw.Sign() == 0 {
		d.logger.Debugf("pw is zero, - returning %v", lastSuitableBits)
		return lastSuitableBits, nil
	}

	newTarget := new(big.Int).Div(nt, pw)

	// NOTE: The squaring bug that was here has been removed (issue #3772)
	// The buggy lines were: newTarget.Div(newTarget.Mul(newTarget, precision), precision)
	// This was incorrectly squaring the target, making it much larger (easier difficulty)

	// clip again if above minimum target (too easy)
	if newTarget.Cmp(d.settings.ChainCfgParams.PowLimit) > 0 {
		d.logger.Debugf("new target would be above pow limit, set to pow limit")
		newTarget.Set(d.settings.ChainCfgParams.PowLimit)
	}

	// Convert back to compact format
	nBitsUint, err := BigToCompact(newTarget)
	if err != nil {
		return nil, err
	}

	nb, err := model.NewNBitFromSlice(uint32ToBytes(nBitsUint))
	if err != nil {
		return nil, err
	}

	return nb, nil
}

// BigToCompact converts a whole number N to a compact representation using
// an unsigned 32-bit number.  The compact representation only provides 23 bits
// of precision, so values larger than (2^23 - 1) only encode the most
// significant digits of the number.  Seepadding, padding)ToBig for details.
func BigToCompact(n *big.Int) (uint32, error) {
	// No need to do any work if it's zero.
	if n.Sign() == 0 {
		return 0, nil
	}

	// Since the base for the exponent is 256, the exponent can be treated
	// as the number of bytes.  So, shift the number right or left
	// accordingly.  This is equivalent to:
	// mantissa = mantissa / 256^(exponent-3)
	var (
		mantissa uint32
		err      error
	)

	exponent := uint(len(n.Bytes()))
	if exponent <= 3 {
		mantissa, err = safeconversion.BigWordToUint32(n.Bits()[0])
		if err != nil {
			return 0, err
		}

		mantissa <<= 8 * (3 - exponent)
	} else {
		// Use a copy to avoid modifying the caller's original number.
		tn := new(big.Int).Set(n)

		shiftedBits := tn.Rsh(tn, 8*(exponent-3)).Bits()
		// if shiftedBits is not empty, convert the first bit to uint32
		if len(shiftedBits) > 0 {
			mantissa, err = safeconversion.BigWordToUint32(shiftedBits[0])
			if err != nil {
				return 0, err
			}
		} else { // if  shiftedBits is empty (i.e. len(shiftedBits) == 0), the shifted result is zero, so mantissa is 0
			mantissa = 0
		}
	}

	// When the mantissa already has the sign bit set, the number is too
	// large to fit into the available 23-bits, so divide the number by 256
	// and increment the exponent accordingly.
	if mantissa&0x00800000 != 0 {
		mantissa >>= 8
		exponent++
	}

	// Pack the exponent, sign bit, and mantissa into an unsigned 32-bit
	// int and return it.

	var exponentUint32 uint32

	exponentUint32, err = safeconversion.UintToUint32(exponent << 24)
	if err != nil {
		return 0, err
	}

	compact := exponentUint32 | mantissa
	if n.Sign() < 0 {
		compact |= 0x00800000
	}

	return compact, nil
}

// uint32ToBytes converts a uint32 to a little-endian byte slice.
func uint32ToBytes(value uint32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, value)

	return bytes
}

// CalcWork has been moved to the work package as CalcBlockWork.
// Use work.CalcBlockWork(bits) instead.

// CompactToBig converts a compact representation of a whole number N to an
// unsigned 32-bit number.  The representation is similar to IEEE754 floating
// point numbers.
//
// Like IEEE754 floating point, there are three basic components: the sign,
// the exponent, and the mantissa.  They are broken out as follows:
//
//   - the most significant 8 bits represent the unsigned base 256 exponent
//
//   - bit 23 (the 24th bit) represents the sign bit
//
//   - the least significant 23 bits represent the mantissa
//
//     -------------------------------------------------
//     |   Exponent     |    Sign    |    Mantissa     |
//     -------------------------------------------------
//     | 8 bits [31-24] | 1 bit [23] | 23 bits [22-00] |
//     -------------------------------------------------
//
// The formula to calculate N is:
//
//	N = (-1^sign) * mantissa * 256^(exponent-3)
//
// This compact form is only used in bitcoin to encode unsigned 256-bit numbers
// which represent difficulty targets, thus there really is not a need for a
// sign bit, but it is implemented here to stay consistent with bitcoind.
func CompactToBig(compact uint32) *big.Int {
	// Extract the mantissa, sign bit, and exponent.
	mantissa := compact & 0x007fffff
	isNegative := compact&0x00800000 != 0
	exponent := uint(compact >> 24)

	// Since the base for the exponent is 256, the exponent can be treated
	// as the number of bytes to represent the full 256-bit number.  So,
	// treat the exponent as the number of bytes and shift the mantissa
	// right or left accordingly.  This is equivalent to:
	// N = mantissa * 256^(exponent-3)
	var bn *big.Int

	if exponent <= 3 {
		mantissa >>= 8 * (3 - exponent)
		bn = big.NewInt(int64(mantissa))
	} else {
		bn = big.NewInt(int64(mantissa))
		bn.Lsh(bn, 8*(exponent-3))
	}

	// Make it negative if the sign bit is set.
	if isNegative {
		bn = bn.Neg(bn)
	}

	return bn
}

// ValidateBlockHeaderDifficulty validates that a block's difficulty matches the expected difficulty.
// Parameters:
//   - ctx: Context for the operation
//   - newBlock: Block to validate
//   - previousBlock: Parent block of the block being validated
//   - currentBlockTime: Current block time for testnet difficulty calculation
func (d *Difficulty) ValidateBlockHeaderDifficulty(ctx context.Context, newBlock, previousBlock *model.Block, currentBlockTime int64) error {
	// Calculate the expected difficulty for the new block
	expectedNBits, err := d.CalcNextWorkRequired(ctx, previousBlock.Header, previousBlock.Height, currentBlockTime)
	if err != nil {
		return errors.NewError("failed to calculate expected difficulty: %v", err)
	}

	// Compare the expected difficulty with the difficulty in the block header
	if newBlock.Header.Bits != *expectedNBits {
		return errors.NewError("block header difficulty is incorrect: expected %v, got %v", expectedNBits, newBlock.Header.Bits)
	}

	return nil
}

// ResetCache clears any cached values inside Difficulty that depend on the
// current best chain tip. This should be called when the best tip may have
// changed due to invalidation, revalidation, or reorgs to avoid returning
// stale results from CalcNextWorkRequired.
func (d *Difficulty) ResetCache() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastBlockHash = nil
	d.lastComputednBits = nil
}
