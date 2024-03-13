package blockchain

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"

	"time"

	"github.com/bitcoin-sv/ubsv/model"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

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
	difficultyAdjustment       bool
	difficultyAdjustmentWindow int
	nBitsString                string
	powLimit                   *big.Int
	lastSlowBlockHash          *chainhash.Hash
	logger                     ulogger.Logger
	store                      blockchain_store.Store
}

func NewDifficulty(store blockchain_store.Store, logger ulogger.Logger, difficultyAdjustmentWindow int) (*Difficulty, error) {
	d := &Difficulty{}
	d.difficultyAdjustment = gocore.Config().GetBool("difficulty_adjustment", false)
	d.difficultyAdjustmentWindow = difficultyAdjustmentWindow

	d.nBitsString, _ = gocore.Config().Get("mining_n_bits", "2000ffff") // TEMP By default, we want hashes with 2 leading zeros. genesis was 1d00ffff

	// powLimit is the highest proof of work value a Bitcoin block
	// can have for the test network. It is the value 2^224 - 1.eee3e
	// d.powLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 224), bigOne)
	// make it easier
	powLimitExp, _ := gocore.Config().GetInt("difficulty_pow_limit", 224)

	d.powLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, uint(powLimitExp)), bigOne)

	d.logger = logger
	d.store = store
	return d, nil
}

func (d *Difficulty) GetNextWorkRequired(ctx context.Context, bestBlockHeader *model.BlockHeader, bestBlockHeight uint32) (*model.NBit, error) {

	initialBlockCount, _ := gocore.Config().GetInt("mine_initial_blocks_count", 200)

	if bestBlockHeight < uint32(d.difficultyAdjustmentWindow)+4 {
		d.logger.Debugf("not enough blocks to calculate difficulty adjustment")
		// not enough blocks to calculate difficulty adjustment
		// set to start difficulty
		nBits := model.NewNBitFromString(d.nBitsString)
		return &nBits, nil
	}

	// Special difficulty rule for testnet:
	// If the new block's timestamp is more than 2* 10 minutes then allow
	// mining of a min-difficulty block.
	now := time.Now()
	targetTimePerBlock, _ := gocore.Config().GetInt("difficulty_target_time_per_block", 600)

	thresholdSeconds := 2 * uint32(targetTimePerBlock)
	randomOffset := rand.Int31n(21) - 10

	timeDifference := uint32(now.Unix()) - bestBlockHeader.Timestamp

	// if uint32(now.Unix())-bestBlockHeader.Timestamp > 2*uint32(d.targetTimePerBlock) {
	d.logger.Debugf("timeDifference: %d", timeDifference)
	d.logger.Debugf("bestBlockHeader.Hash().String(): %s", bestBlockHeader.Hash().String())
	if d.lastSlowBlockHash != nil {
		d.logger.Debugf("d.lastSlowBlockHash: %s", d.lastSlowBlockHash.String())
	}
	if timeDifference > thresholdSeconds+uint32(randomOffset) && (d.lastSlowBlockHash != bestBlockHeader.Hash()) {
		d.logger.Debugf("applying special difficulty rule")
		d.lastSlowBlockHash = bestBlockHeader.Hash()
		// set to start difficulty
		nBits := model.NewNBitFromString(d.nBitsString)
		return &nBits, nil
	}

	if gocore.Config().GetBool("mine_initial_blocks", false) && bestBlockHeight < uint32(initialBlockCount) {
		// set to start difficulty
		d.logger.Debugf("mining initial blocks")

		nBits := model.NewNBitFromString(d.nBitsString)
		return &nBits, nil
	}

	lastSuitableBlock, err := d.store.GetSuitableBlock(ctx, bestBlockHeader.Hash())
	if err != nil {
		return nil, fmt.Errorf("error getting suitable block: %v", err)
	}
	ancestorHash, err := d.store.GetHashOfAncestorBlock(ctx, bestBlockHeader.Hash(), d.difficultyAdjustmentWindow)
	if err != nil {
		// could be that we don't have a long enough chain to get the ancestor
		d.logger.Debugf("error getting ancestor block: %v", err)
		ancestorHash = bestBlockHeader.Hash()
	}
	firstSuitableBlock, err := d.store.GetSuitableBlock(ctx, ancestorHash)
	if err != nil {
		return nil, fmt.Errorf("error getting suitable block: %v", err)
	}

	// d.logger.Debugf("firstSuitableBlock: %+v", firstSuitableBlock)
	// d.logger.Debugf("lastSuitableBlock: %+v", lastSuitableBlock)
	nBits, err := d.ComputeTarget(&model.BlockHeader{
		Bits:      model.NewNBitFromSlice(firstSuitableBlock.NBits),
		Timestamp: firstSuitableBlock.Time,
	}, &model.BlockHeader{
		Bits:      model.NewNBitFromSlice(lastSuitableBlock.NBits),
		Timestamp: lastSuitableBlock.Time,
	})
	if err != nil {
		return nil, fmt.Errorf("error calculating next required difficulty: %v", err)
	}
	d.logger.Debugf("nBits: %+v", nBits)
	return nBits, nil
}
func (d *Difficulty) ComputeTarget(firstBlock *model.BlockHeader, lastBlock *model.BlockHeader) (*model.NBit, error) {

	// If no difficulty adjustment is required, return the previous nbits
	if !d.difficultyAdjustment {
		d.logger.Debugf("no difficulty adjustment")
		return &lastBlock.Bits, nil
	}

	targetTimePerBlock, _ := gocore.Config().GetInt("difficulty_target_time_per_block", 600)

	// Calculate the actual duration (in seconds) taken to mine the blocks between the first and last blocks
	duration := int64(lastBlock.Timestamp - firstBlock.Timestamp)
	// In order to avoid difficulty cliffs, we bound the amplitude of the
	// adjustement we are going to do.
	if duration > int64(2*d.difficultyAdjustmentWindow)*int64(targetTimePerBlock) {
		duration = int64(2*d.difficultyAdjustmentWindow) * int64(targetTimePerBlock)
	} else if duration < int64(d.difficultyAdjustmentWindow/2)*int64(targetTimePerBlock) {
		duration = int64(d.difficultyAdjustmentWindow/2) * int64(targetTimePerBlock)
	}

	// Calculate the new target based on the ratio of actual to expected duration
	actualDuration := big.NewInt(duration)
	// Calculate the expected duration for mining the blocks in the difficulty adjustment window
	expectedDuration := big.NewInt(int64(targetTimePerBlock * d.difficultyAdjustmentWindow))

	// Calculate the new target by multiplying the last block's target with the ratio of actual to expected duration
	newTarget := new(big.Int).Mul(lastBlock.Bits.CalculateTarget(), actualDuration)
	newTarget.Div(newTarget, expectedDuration)

	if newTarget.Cmp(d.powLimit) > 0 {
		d.logger.Debugf("difficulty would be above pow limit, set to pow limit")
		newTarget.Set(d.powLimit)
	}

	nBitsUint := BigToCompact(newTarget)
	nb := model.NewNBitFromSlice(uint32ToBytes(nBitsUint))
	return &nb, nil
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
		mantissa = uint32(n.Bits()[0])
		mantissa <<= 8 * (3 - exponent)
	} else {
		// Use a copy to avoid modifying the caller's original number.
		tn := new(big.Int).Set(n)
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
