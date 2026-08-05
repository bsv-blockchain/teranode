package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// SpendAndCreate implements utxo.Store. It delegates to the shared sequential
// implementation; an atomic implementation using a database transaction is a
// followup.
func (s *Store) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	// SQL does not support create-first (SupportsCreateFirst()==false), so this is always
	// spend-first; the flag is honoured uniformly so a capable backend can opt in.
	createFirst := s.SupportsCreateFirst() && s.settings.UtxoStore.UseCreateFirstOrder
	return utxo.SequentialSpendAndCreate(ctx, s.logger, s, tx, blockHeight, createFirst, opts...)
}
