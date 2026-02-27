package model

import (
	"encoding/binary"
	"math/big"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
)

type NBit [4]byte // nBits is 4 bytes array held internal in little endian format

func NewNBitFromSlice(nBits []byte) (*NBit, error) {
	if len(nBits) != 4 {
		return nil, errors.NewInvalidArgumentError("nBits should be 4 bytes long")
	}

	var nBit NBit

	copy(nBit[:], nBits)

	return &nBit, nil
}

func NewNBitFromString(nBitStr string) (*NBit, error) {
	nBits, err := util.DecodeAndReverseHexString(nBitStr)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("error decoding nBitStr ", err)
	}

	return NewNBitFromSlice(nBits)
}

func (b NBit) String() string {
	return util.ReverseAndHexEncodeSlice(b[:])
}

func (b NBit) MarshalJSON() ([]byte, error) {
	return []byte(`"` + b.String() + `"`), nil
}

func (b NBit) UnmarshalJSON(data []byte) error {
	nBits, err := util.DecodeAndReverseHexString(string(data))
	if err != nil {
		return err
	}

	copy(b[:], nBits)

	return nil
}

func (b NBit) CloneBytes() []byte {
	return b[:]
}

// CalculateTarget from nBits returns the target as a big.Int
func (b NBit) CalculateTarget() *big.Int {
	nb := binary.LittleEndian.Uint32(b[:])

	exponent := nb >> 24
	mantissa := nb & 0x007FFFFF

	// Invalid nBits
	if exponent <= 3 {
		mantissa >>= 8 * (3 - exponent)
		return big.NewInt(int64(mantissa))
	}

	target := big.NewInt(int64(mantissa))
	target.Lsh(target, uint(8*(exponent-3)))

	return target
}

// CalculateDifficulty from nBits using the standard Bitcoin algorithm
func (b NBit) CalculateDifficulty() *big.Float {
	// This implementation follows the standard Bitcoin difficulty calculation
	// as used in both Bitcoin Core and Bitcoin SV:
	// https://github.com/bitcoin/bitcoin/blob/master/src/rpc/blockchain.cpp
	// https://github.com/bitcoin-sv/bitcoin-sv/blob/master/src/rpc/blockchain.cpp
	//
	// The algorithm is identical in both implementations:
	// double GetDifficulty(const CBlockIndex& blockindex) {
	//     int nShift = (blockindex.nBits >> 24) & 0xff;
	//     double dDiff = (double)0x0000ffff / (double)(blockindex.nBits & 0x00ffffff);
	//     while (nShift < 29) { dDiff *= 256.0; nShift++; }
	//     while (nShift > 29) { dDiff /= 256.0; nShift--; }
	//     return dDiff;
	// }

	nb := binary.LittleEndian.Uint32(b[:])

	// Extract nShift (exponent) and coefficient
	nShift := (nb >> 24) & 0xff
	coefficient := nb & 0x00ffffff

	// Calculate initial difficulty
	dDiff := float64(0x0000ffff) / float64(coefficient)

	// Normalize to exponent 29
	for nShift < 29 {
		dDiff *= 256.0
		nShift++
	}
	for nShift > 29 {
		dDiff /= 256.0
		nShift--
	}

	return big.NewFloat(dDiff)
}
