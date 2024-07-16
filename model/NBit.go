package model

import (
	"encoding/binary"
	"math/big"

	"github.com/ordishs/go-utils"
)

type NBit [4]byte // nBits is 4 bytes array held internal in little endian format

func NewNBitFromSlice(nBits []byte) NBit {
	if len(nBits) != 4 {
		panic("nBits should be 4 bytes long")
	}

	var nBit NBit
	copy(nBit[:], nBits)

	return nBit
}

func NewNBitFromString(nBitStr string) NBit {
	nBits, err := utils.DecodeAndReverseHexString(nBitStr)
	if err != nil {
		panic("error decoding nBitStr: " + err.Error())
	}

	return NewNBitFromSlice(nBits)
}

func (b NBit) String() string {
	return utils.ReverseAndHexEncodeSlice(b[:])
}

func (b NBit) MarshalJSON() ([]byte, error) {
	return []byte(`"` + b.String() + `"`), nil
}

func (b NBit) UnmarshalJSON(data []byte) error {
	nBits, err := utils.DecodeAndReverseHexString(string(data))
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

// CalculateDifficulty from nBits
func (b NBit) CalculateDifficulty() *big.Float {
	// Difficulty 1 target
	difficulty1Target, _ := new(big.Int).SetString("00000000FFFF0000000000000000000000000000000000000000000000000000", 16)

	// Current target from nBits
	currentTarget := b.CalculateTarget()

	// Calculate difficulty
	difficulty := new(big.Float).Quo(new(big.Float).SetInt(difficulty1Target), new(big.Float).SetInt(currentTarget))

	return difficulty
}
