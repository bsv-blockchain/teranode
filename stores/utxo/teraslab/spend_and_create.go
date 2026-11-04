package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// SpendAndCreate implements utxo.Store. It delegates to the shared sequential
// implementation (spend the inputs, create the outputs, roll the spends back on
// a create failure other than ErrTxExists); an atomic TeraSlab-native
// implementation is a followup.
func (s *Store) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	return utxo.SequentialSpendAndCreate(ctx, s.logger, s, tx, blockHeight, opts...)
}

// SpendAndCreateMulti implements utxo.Store for an ordered list of transactions.
// It delegates to the shared default implementation (which drives SpendAndCreate
// per transaction with the configured wave widths), matching the SQL/Aerospike
// backends; a TeraSlab-native atomic implementation is a followup.
func (s *Store) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	return utxo.DefaultSpendAndCreateMulti(ctx, s, utxo.SpendAndCreateMultiWidthsFor(s.settings), txs, blockHeight, opts...)
}
