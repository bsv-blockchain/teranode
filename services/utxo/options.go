package utxo

import (
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
)

type Options func(utxostore.Interface)

func WithDeleteSpends(deleteSpends bool) Options {
	return func(u utxostore.Interface) {
		u.DeleteSpends(deleteSpends)
	}
}
