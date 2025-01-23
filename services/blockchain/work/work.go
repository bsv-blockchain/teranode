package work

import (
	"math/big"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

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
