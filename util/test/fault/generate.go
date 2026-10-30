package fault

import "github.com/bsv-blockchain/teranode/stores/utxo"

//go:generate go run ./internal/faultgen/cmd/faultgen

// Unwrap returns the wrapped store, so utxo.As can see through the decorator to a capability
// the wrapped store implements.
func (w *UTXOStore) Unwrap() utxo.Store {
	return w.inner
}

var _ utxo.Unwrapper = (*UTXOStore)(nil)
