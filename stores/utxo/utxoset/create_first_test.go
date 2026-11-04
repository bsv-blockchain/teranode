package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// TestBeginCreateFirstRollbackReportsFalseAndDeletesNothing pins the no-tentative-state
// contract: every record is final, so the marker call reports false and leaves the record.
func TestBeginCreateFirstRollbackReportsFalseAndDeletesNothing(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	began, err := s.BeginCreateFirstRollback(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.False(t, began)

	require.NoError(t, s.GetMeta(ctx, tx.TxIDChainHash(), &meta.Data{}), "a final record must survive")
}
