package utxo

import (
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
)

type Options func(utxostore.UTXOStore)

func WithDeleteSpends(deleteSpends bool) Options {
	return func(u utxostore.UTXOStore) {
		u.DeleteSpends(deleteSpends)
	}
}
