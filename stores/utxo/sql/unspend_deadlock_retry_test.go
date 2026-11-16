package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	utxo2 "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// SQLite reports a lock cycle as a table lock, which isDeadlock treats as a deadlock. A trigger on the
// outputs table makes every Unspend attempt fail that way. The error that comes back after the retries
// must name the last cause and must not be retried again.
func TestUnspendReturnsTheLastCauseWhenDeadlockRetriesAreExhausted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	spendTx := utxo2.GetSpendingTx(tx, 0)
	_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
	require.NoError(t, err)

	utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	spend := &utxo.Spend{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash,
		SpendingData: spendpkg.NewSpendingData(spendTx.TxIDChainHash(), 0),
	}

	_, err = store.db.ExecContext(ctx, `
		CREATE TRIGGER fail_output_update BEFORE UPDATE ON outputs
		BEGIN SELECT RAISE(ABORT, 'database table is locked: database is deadlocked'); END`)
	require.NoError(t, err)

	unspendErr := store.Unspend(ctx, []*utxo.Spend{spend})
	require.Error(t, unspendErr)

	require.Contains(t, unspendErr.Error(), "deadlock persisted after 3 retries")
	require.Contains(t, unspendErr.Error(), "database table is locked", "the last cause must be in the error")
	require.True(t, errors.Is(unspendErr, errors.ErrStorageError))
	require.False(t, isDeadlock(unspendErr), "rollbackSpendChunks must not retry an exhausted error")
}
