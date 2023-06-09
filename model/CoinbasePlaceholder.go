package model

var CoinbasePlaceholder [32]byte

func init() {
	for i := 0; i < len(CoinbasePlaceholder); i++ {
		CoinbasePlaceholder[i] = 0xFF
	}
}
