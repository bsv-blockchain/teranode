package utxo

import "github.com/TAAL-GmbH/ubsv/services/utxo/store"

type Options func(store.UTXOStore)

func WithDeleteSpends(deleteSpends bool) Options {
	return func(u store.UTXOStore) {
		u.DeleteSpends(deleteSpends)
	}
}
