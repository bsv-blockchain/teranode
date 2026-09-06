package utxoset

import utxostore "github.com/bsv-blockchain/teranode/stores/utxo"

var _ utxostore.Store = (*Store)(nil)
