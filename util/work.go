package util

import (
	"encoding/binary"
	"math/big"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

func CalculateWork(prevWork *chainhash.Hash, nBits []byte) (*chainhash.Hash, error) {
	target := CalculateTarget(nBits)

	// Work done is proportional to 1/difficulty
	work := new(big.Int).Div(new(big.Int).Lsh(big.NewInt(1), 256), target)

	// Add to previous work
	newWork := new(big.Int).Add(new(big.Int).SetBytes(bt.ReverseBytes(prevWork.CloneBytes())), work)

	b := bt.ReverseBytes(newWork.Bytes())
	hash := &chainhash.Hash{}
	copy(hash[:], b[:])

	return hash, nil
}

func CalculateTarget(nBits []byte) *big.Int {
	nb := binary.BigEndian.Uint32(nBits)
	// nBits in Bitcoin protocol is in little-endian, so reverse if necessary
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
