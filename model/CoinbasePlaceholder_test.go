package model

import (
	"testing"

	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/assert"
)

func TestCoinbasePlaceholderTx(t *testing.T) {
	assert.True(t, IsCoinbasePlaceHolderTx(CoinbasePlaceholderTx))
	assert.Equal(t, CoinbasePlaceholderTx.Version, uint32(0xFFFFFFFF))
	assert.Equal(t, CoinbasePlaceholderTx.LockTime, uint32(0xFFFFFFFF))
	assert.Equal(t, CoinbasePlaceholderTx.TxIDChainHash(), coinbasePlaceholderTxHash)
	assert.False(t, IsCoinbasePlaceHolderTx(bt.NewTx()))
	assert.Equal(t, "a8502e9c08b3c851201a71d25bf29fd38a664baedb777318b12d19242f0e46ab", CoinbasePlaceholderTx.TxIDChainHash().String())
}
