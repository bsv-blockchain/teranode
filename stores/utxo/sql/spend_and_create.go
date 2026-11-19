package sql

import (
	"context"
	"time"

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
// A WithBelowCheckpoint list goes in two waves, as wide as quick validation's
// create and spend phases were; see utxo.DefaultSpendAndCreateMulti.
func (s *Store) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	return utxo.DefaultSpendAndCreateMulti(ctx, s, utxo.SpendAndCreateMultiWidthsFor(s.settings), txs, blockHeight, opts...)
}

// SpendRollbackTimeout bounds the rollback utxo.SequentialSpendAndCreate runs
// after a failed create: utxostore_spendRollbackTimeout, the bound this store's
// own partial-spend rollback uses. Zero leaves the shared default in force.
func (s *Store) SpendRollbackTimeout() time.Duration {
	if s.settings == nil {
		return 0
	}

	return s.settings.UtxoStore.SpendRollbackTimeout
}
