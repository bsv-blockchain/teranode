package model

import "github.com/libsv/go-bt/v2/chainhash"

var CoinbasePlaceholder [32]byte
var CoinbasePlaceholderHash *chainhash.Hash

func init() {
	for i := 0; i < len(CoinbasePlaceholder); i++ {
		CoinbasePlaceholder[i] = 0xFF
	}
	CoinbasePlaceholderHash, _ = chainhash.NewHash(CoinbasePlaceholder[:])
}
