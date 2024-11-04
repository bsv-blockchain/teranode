package blockchain

import (
	"context"
	"encoding/binary"
	"math/big"
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/exp/rand"
)

const DifficultyAdjustmentWindow = 144

var (
	// bigOne is 1 represented as a big.Int.  It is defined here to avoid
	// the overhead of creating it multiple times.
	bigOne = big.NewInt(1)

	// oneLsh256 is 1 shifted left 256 bits.  It is defined here to avoid
	// the overhead of creating it multiple times.
	oneLsh256 = new(big.Int).Lsh(bigOne, 256)
	_         = oneLsh256
)

type Difficulty struct {
	difficultyAdjustment bool
	powLimitnBits        *model.NBit
	// lastSlowBlockHash    *chainhash.Hash
	logger            ulogger.Logger
	store             blockchain_store.Store
	chainParams       *chaincfg.Params
	bestBlockHash     *chainhash.Hash
	lastComputednBits *model.NBit
}

func NewDifficulty(store blockchain_store.Store, logger ulogger.Logger, params *chaincfg.Params) (*Difficulty, error) {
	d := &Difficulty{}
	d.difficultyAdjustment = gocore.Config().GetBool("difficulty_adjustment", false)
	d.chainParams = params

	bytesLittleEndian := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesLittleEndian, params.PowLimitBits)
	d.powLimitnBits, _ = model.NewNBitFromSlice(bytesLittleEndian)

	d.logger = logger
	d.store = store

	return d, nil
}

func (d *Difficulty) CalcNextWorkRequired(ctx context.Context, bestBlockHeader *model.BlockHeader, bestBlockHeight uint32) (*model.NBit, error) {
	initialBlockCount, _ := gocore.Config().GetInt("mine_initial_blocks_count", 200)

	//nolint:gosec // Ignore G115: integer overflow conversion
	if gocore.Config().GetBool("mine_initial_blocks", false) && bestBlockHeight < uint32(initialBlockCount) {
		// set to start difficulty
		d.logger.Debugf("mining initial blocks")

		return d.powLimitnBits, nil
	}

	// If regest or simnet we don't adjust the difficulty
	if d.chainParams.NoDifficultyAdjustment {
		return &bestBlockHeader.Bits, nil
	}

	// if bestBlockHash is set and it's the same as the bestBlockHeader.Hash(), we don't need to recalculate the difficulty,
	// just send the one we have if it's set
	if d.bestBlockHash != nil && d.bestBlockHash.IsEqual(bestBlockHeader.Hash()) {
		d.logger.Debugf("bestBlockHash is set and it's the same as the bestBlockHeader.Hash(), returning last computed difficulty")

		if d.lastComputednBits != nil {
			return d.lastComputednBits, nil
		}
	}

	if bestBlockHeight < uint32(DifficultyAdjustmentWindow)+4 {
		d.logger.Debugf("not enough blocks to calculate difficulty adjustment")
		// not enough blocks to calculate difficulty adjustment
		// set to start difficulty

		return d.powLimitnBits, nil
	}

	now := time.Now()

	// For networks that support it, allow special reduction of the
	// required difficulty once too much time has elapsed without
	// mining a block.
	if d.chainParams.ReduceMinDifficulty {
		// Return minimum difficulty when more than the desired
		// amount of time has elapsed without mining a block.
		reductionTime := d.chainParams.MinDiffReductionTime.Seconds()

		// Add a random additional time of +/- 1 minute
		// this is to prevent testnet nodes all mining their own blocks at the same time
		randomAdjustment := time.Duration(rand.Int63n(120)-60) * time.Second
		reductionTime += randomAdjustment.Seconds()
		d.logger.Debugf("Adjusted reduction time by %v seconds", randomAdjustment.Seconds())

		allowMinTime := bestBlockHeader.Timestamp + uint32(reductionTime)
		if now.Unix() > int64(allowMinTime) {
			bytesLittleEndian := make([]byte, 4)
			binary.LittleEndian.PutUint32(bytesLittleEndian, d.chainParams.PowLimitBits)
			nBits, _ := model.NewNBitFromSlice(bytesLittleEndian)

			return nBits, nil
		}
	}

	lastSuitableBlock, err := d.store.GetSuitableBlock(ctx, bestBlockHeader.Hash())
	if err != nil {
		return nil,
			errors.NewStorageError("error getting suitable block", err)
	}

	if lastSuitableBlock == nil {
		return nil, errors.NewProcessingError("lastSuitableBlock is nil", nil)
	}

	ancestorHash, err := d.store.GetHashOfAncestorBlock(ctx, bestBlockHeader.Hash(), DifficultyAdjustmentWindow)
	if err != nil {
		// could be that we don't have a long enough chain to get the ancestor
		d.logger.Debugf("error getting ancestor block: %v", err)

		ancestorHash = bestBlockHeader.Hash()
	}

	firstSuitableBlock, err := d.store.GetSuitableBlock(ctx, ancestorHash)
	if err != nil {
		return nil, errors.NewStorageError("error getting suitable block", err)
	}

	if firstSuitableBlock == nil {
		return d.powLimitnBits, nil
	}

	nBits, err := d.computeTarget(firstSuitableBlock, lastSuitableBlock)
	if err != nil {
		return nil, errors.NewProcessingError("error calculating next required difficulty", err)
	}

	d.lastComputednBits = nBits

	return nBits, nil
}

// calcNextRequiredDifficulty calculates the required difficulty for the block
// after the passed previous block node based on the difficulty retarget rules.
// This function differs from the exported CalcNextRequiredDifficulty in that
// the exported version uses the current best chain as the previous block node
// while this function accepts any block node.
func (d *Difficulty) computeTarget(suitableFirstBlock *model.SuitableBlock, suitableLastBlock *model.SuitableBlock) (*model.NBit, error) {
	lastSuitableBits, _ := model.NewNBitFromSlice(suitableLastBlock.NBits)
	// If regest or simnet we don't adjust the difficulty
	if d.chainParams.NoDifficultyAdjustment {
		d.logger.Debugf("no difficulty adjustment - returning %v", lastSuitableBits)
		return lastSuitableBits, nil
	}

	// For networks that support it, allow special reduction of the
	// required difficulty once too much time has elapsed without
	// mining a block.
	if d.chainParams.ReduceMinDifficulty {
		// Return minimum difficulty when more than the desired
		// amount of time has elapsed without mining a block.
		reductionTime := int64(d.chainParams.MinDiffReductionTime /
			time.Second)
		allowMinTime := int64(suitableLastBlock.Time) + reductionTime
		elapsedTime := time.Now().Unix() - int64(suitableLastBlock.Time)

		if elapsedTime > allowMinTime {
			d.logger.Debugf("more than %d seconds have elapsed without mining a block, returning powLimitBits", d.chainParams.MinDiffReductionTime)

			bytesLittleEndian := make([]byte, 4)
			binary.LittleEndian.PutUint32(bytesLittleEndian, d.chainParams.PowLimitBits)
			powLimitBits, _ := model.NewNBitFromSlice(bytesLittleEndian)

			return powLimitBits, nil
		}
	}

	// Get the block node at the beginning of the window (n-144)
	// firstNode := lastNode.RelativeAncestor(DifficultyAdjustmentWindow)
	// if firstNode == nil {
	// 	return nil, errors.New(errors.ERR_ERROR, "unable to obtain previous retarget block")
	// }

	// Find the suitable blocks to use as the first and last nodes for the
	// purpose of the difficulty calculation. A suitable block is the median
	// timestamp out of the three prior.
	// suitableLastNode, err := b.getSuitableBlock(lastNode)
	// if err != nil {
	// 	return nil, err
	// }
	// suitableFirstNode, err := b.getSuitableBlock(firstNode)
	// if err != nil {
	// 	return nil, err
	// }
	lastChainwork := new(big.Int).SetBytes(suitableLastBlock.ChainWork)
	firstChainwork := new(big.Int).SetBytes(suitableFirstBlock.ChainWork)

	work := new(big.Int).Sub(lastChainwork, firstChainwork)
	d.logger.Debugf("work: %s", work.String())
	// In order to avoid difficulty cliffs, we bound the amplitude of the
	// adjustment we are going to do.
	d.logger.Debugf("suitableLastBlock.Height: %d, suitableFirstBlock.Height: %d", suitableLastBlock.Height, suitableFirstBlock.Height)
	d.logger.Debugf("suitableLastBlock.Time: %d, suitableFirstBlock.Time: %d", suitableLastBlock.Time, suitableFirstBlock.Time)

	duration := int64(suitableLastBlock.Time - suitableFirstBlock.Time)
	if duration > 288*int64(d.chainParams.TargetTimePerBlock.Seconds()) {
		d.logger.Debugf("duration %d is greater than 288 * target time per block %d - setting to 288 * target time per block", duration, d.chainParams.TargetTimePerBlock.Seconds())
		duration = 288 * int64(d.chainParams.TargetTimePerBlock.Seconds())
	} else if duration < 72*int64(d.chainParams.TargetTimePerBlock.Seconds()) {
		d.logger.Debugf("duration %d is less than 72 * target time per block %d - setting to 72 * target time per block", duration, d.chainParams.TargetTimePerBlock.Seconds())
		duration = 72 * int64(d.chainParams.TargetTimePerBlock.Seconds())
	}

	// Calculate the projected work by multiplying the current work by the target time per block (in seconds).
	projectedWork := new(big.Int).Mul(work, big.NewInt(int64(d.chainParams.TargetTimePerBlock.Seconds())))
	// Divide the projected work by the actual time duration between the blocks to get the adjusted work.
	// check if duration is zero
	if duration == 0 {
		d.logger.Debugf("duration is zero - returning %v", lastSuitableBits)
		return lastSuitableBits, nil
	}

	pw := new(big.Int).Div(projectedWork, big.NewInt(duration))
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

	// clip again if above minimum target (too easy)
	if newTarget.Cmp(d.chainParams.PowLimit) > 0 {
		d.logger.Debugf("new target would be above pow limit, set to pow limit")
		newTarget.Set(d.chainParams.PowLimit)
	}

	nBitsUint := BigToCompact(newTarget)
	nb, _ := model.NewNBitFromSlice(uint32ToBytes(nBitsUint))

	return nb, nil
}

// BigToCompact converts a whole number N to a compact representation using
// an unsigned 32-bit number.  The compact representation only provides 23 bits
// of precision, so values larger than (2^23 - 1) only encode the most
// significant digits of the number.  Seepadding, padding)ToBig for details.
func BigToCompact(n *big.Int) uint32 {
	// No need to do any work if it's zero.
	if n.Sign() == 0 {
		return 0
	}

	// Since the base for the exponent is 256, the exponent can be treated
	// as the number of bytes.  So, shift the number right or left
	// accordingly.  This is equivalent to:
	// mantissa = mantissa / 256^(exponent-3)
	var mantissa uint32

	exponent := uint(len(n.Bytes()))
	if exponent <= 3 {
		//nolint:gosec // Ignore G115: integer overflow conversion
		mantissa = uint32(n.Bits()[0])
		mantissa <<= 8 * (3 - exponent)
	} else {
		// Use a copy to avoid modifying the caller's original number.
		//nolint:gosec // Ignore G115: integer overflow conversion
		tn := new(big.Int).Set(n)
		//nolint:gosec // Ignore G115: integer overflow conversion
		mantissa = uint32(tn.Rsh(tn, 8*(exponent-3)).Bits()[0])
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
	//nolint:gosec // Ignore G115: integer overflow conversion
	compact := uint32(exponent<<24) | mantissa
	if n.Sign() < 0 {
		compact |= 0x00800000
	}

	return compact
}

func uint32ToBytes(value uint32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, value)

	return bytes
}

// CalcWork calculates a work value from difficulty bits.  Bitcoin increases
// the difficulty for generating a block by decreasing the value which the
// generated hash must be less than.  This difficulty target is stored in each
// block header using a compact representation as described in the documentation
// for CompactToBig.  The main chain is selected by choosing the chain that has
// the most proof of work (highest difficulty).  Since a lower target difficulty
// value equates to higher actual difficulty, the work value which will be
// accumulated must be the inverse of the difficulty.  Also, in order to avoid
// potential division by zero and really small floating point numbers, the
// result adds 1 to the denominator and multiplies the numerator by 2^256.
func CalcWork(bits uint32) *big.Int {
	// Return a work value of zero if the passed difficulty bits represent
	// a negative number. Note this should not happen in practice with valid
	// blocks, but an invalid block could trigger it.
	difficultyNum := CompactToBig(bits)
	if difficultyNum.Sign() <= 0 {
		return big.NewInt(0)
	}

	// (1 << 256) / (difficultyNum + 1)
	denominator := new(big.Int).Add(difficultyNum, bigOne)

	return new(big.Int).Div(oneLsh256, denominator)
}

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
func (d *Difficulty) validateBlockHeaderDifficulty(ctx context.Context, newBlock, previousBlock *model.Block) error {
	// Calculate the expected difficulty for the new block
	expectedNBits, err := d.CalcNextWorkRequired(ctx, previousBlock.Header, previousBlock.Height)
	if err != nil {
		return errors.NewError("failed to calculate expected difficulty: %v", err)
	}

	// Compare the expected difficulty with the difficulty in the block header
	if newBlock.Header.Bits != *expectedNBits {
		return errors.NewError("block header difficulty is incorrect: expected %v, got %v", expectedNBits, newBlock.Header.Bits)
	}

	return nil
}
