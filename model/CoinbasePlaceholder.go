package model

import (
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

var CoinbasePlaceholder [32]byte
var (
	CoinbasePlaceholderHash   *chainhash.Hash
	CoinbasePlaceholderTx     *bt.Tx
	coinbasePlaceholderTxHash *chainhash.Hash
)

func init() {
	for i := 0; i < len(CoinbasePlaceholder); i++ {
		CoinbasePlaceholder[i] = 0xFF
	}
	CoinbasePlaceholderHash, _ = chainhash.NewHash(CoinbasePlaceholder[:])

	CoinbasePlaceholderTx = bt.NewTx()
	CoinbasePlaceholderTx.Version = 0xFFFFFFFF
	CoinbasePlaceholderTx.LockTime = 0xFFFFFFFF

	coinbasePlaceholderTxHash = CoinbasePlaceholderTx.TxIDChainHash()
}

func IsCoinbasePlaceHolderTx(tx *bt.Tx) bool {
	return tx.TxIDChainHash().IsEqual(coinbasePlaceholderTxHash)
}
