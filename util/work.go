package util

import (
	"math/big"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

func CalculateWork(prevWork *chainhash.Hash, nBits uint32) (*chainhash.Hash, error) {
	target, err := CalculateTarget(nBits)
	if err != nil {
		return nil, err
	}

	// Work done is proportional to 1/difficulty
	work := new(big.Int).Div(new(big.Int).Lsh(big.NewInt(1), 256), target)

	// Add to previous work
	newWork := new(big.Int).Add(new(big.Int).SetBytes(bt.ReverseBytes(prevWork.CloneBytes())), work)

	b := bt.ReverseBytes(newWork.Bytes())
	hash := &chainhash.Hash{}
	copy(hash[:], b[:])

	return hash, nil
}

func CalculateTarget(nBits uint32) (*big.Int, error) {
	// nBits in Bitcoin protocol is in little-endian, so reverse if necessary
	exponent := nBits >> 24
	mantissa := nBits & 0x007FFFFF

	// Invalid nBits
	if exponent <= 3 {
		mantissa >>= 8 * (3 - exponent)
		return big.NewInt(int64(mantissa)), nil
	}

	target := big.NewInt(int64(mantissa))
	target.Lsh(target, uint(8*(exponent-3)))

	return target, nil
}
