package model

import "github.com/libsv/go-p2p/chaincfg/chainhash"

var CoinbasePlaceholder [32]byte
var CoinbasePlaceholderHash *chainhash.Hash

func init() {
	for i := 0; i < len(CoinbasePlaceholder); i++ {
		CoinbasePlaceholder[i] = 0xFF
	}
	CoinbasePlaceholderHash, _ = chainhash.NewHash(CoinbasePlaceholder[:])
}
