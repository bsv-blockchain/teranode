package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// runCreateBatch drives the batch callback directly, so the assertions are about what one
// flush does rather than about when the batcher decides to flush.
func runCreateBatch(t *testing.T, s *Store, height uint32, txs ...*bt.Tx) []createResult {
	t.Helper()

	items := make([]*createItem, 0, len(txs))

	for _, tx := range txs {
		items = append(items, &createItem{
			tx:          tx,
			blockHeight: height,
			options:     &utxo.CreateOptions{},
			done:        make(chan createResult, 1),
		})
	}

	s.sendCreateBatch(items)

	out := make([]createResult, 0, len(items))
	for _, it := range items {
		out = append(out, <-it.done)
	}

	return out
}

// countRows is the row count of one table, read straight from the pool.
func countRows(t *testing.T, s *Store, ctx context.Context, table string) int {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n))

	return n
}

// TestCreateBatchClaimsARepeatedTransactionOnce covers the case a batch built around array
// parameters can silently get wrong: the same transaction offered twice in ONE flush.
//
// Only the first offer may store anything. The second must come back as ErrTxExists, exactly
// as it would had the two arrived in separate batches, and it must not write a second body
// row or a second set of coins.
func TestCreateBatchClaimsARepeatedTransactionOnce(t *testing.T) {
	s, ctx := newTestStore(t)

	first := mkTx(t, 2, 5000)
	second := mkTx(t, 3, 6000)

	res := runCreateBatch(t, s, 100, first, second, first)

	require.NoError(t, res[0].err, "the first offer stores the transaction")
	require.NoError(t, res[1].err, "an unrelated transaction in the same batch is unaffected")
	require.True(t, errors.Is(res[2].err, errors.ErrTxExists),
		"the repeat must report ErrTxExists, got %v", res[2].err)

	require.Equal(t, 2, countRows(t, s, ctx, "tx_ident"))
	require.Equal(t, 2, countRows(t, s, ctx, "tx_body"))
	require.Equal(t, 5, countRows(t, s, ctx, "utxo"), "2 outputs plus 3, each stored once")
}

// TestCreateBatchReportsATransactionTheStoreAlreadyHolds is the same rule across flushes:
// one item already stored must not spoil the rest of its batch, and must not duplicate its
// own coins.
func TestCreateBatchReportsATransactionTheStoreAlreadyHolds(t *testing.T) {
	s, ctx := newTestStore(t)

	held := mkTx(t, 2, 5000)
	fresh := mkTx(t, 4, 7000)

	_, err := s.Create(ctx, held, 100)
	require.NoError(t, err)

	res := runCreateBatch(t, s, 100, held, fresh)

	require.True(t, errors.Is(res[0].err, errors.ErrTxExists),
		"the held transaction must report ErrTxExists, got %v", res[0].err)
	require.NoError(t, res[1].err, "the fresh transaction in the same batch must still store")

	require.Equal(t, 2, countRows(t, s, ctx, "tx_ident"))
	require.Equal(t, 2, countRows(t, s, ctx, "tx_body"))
	require.Equal(t, 6, countRows(t, s, ctx, "utxo"), "2 outputs plus 4, each stored once")
}
