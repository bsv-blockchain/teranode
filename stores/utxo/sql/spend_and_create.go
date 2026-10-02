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
	return utxo.SequentialSpendAndCreate(ctx, s.logger, s, tx, blockHeight, opts...)
}

// SpendAndCreateMulti implements utxo.Store through the shared
// DefaultSpendAndCreateMulti. Each dependency level's transactions are written
// concurrently, as wide as subtree validation writes a level today, on SQLite
// as on Postgres. SQLite has one writer, and two spend transactions that
// overlap can fail one another with a table-lock error, but isDeadlock
// recognises that error and sendSpendBatch retries it. Writing a level one
// transaction at a time instead made every spend wait out the batcher timer on
// its own: 300 independent transactions took 3.1s against 56ms concurrently.
func (s *Store) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	return utxo.DefaultSpendAndCreateMulti(ctx, s, utxo.SpendAndCreateMultiConcurrency(s.settings), txs, blockHeight, opts...)
}
