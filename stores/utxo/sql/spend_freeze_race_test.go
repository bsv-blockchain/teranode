package sql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	utxo2 "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSpendRechecksFreezeStateUnderTheWrite pins that a spend's verdict is drawn from the
// output as it is at the WRITE, not at the read that preceded it (issue #1422). The SQL
// spend is a SELECT, a verdict computed in Go, and an UPDATE; the alert system can freeze,
// unfreeze or (another spender) spend the output in between. The UPDATE is therefore
// pinned to the state the SELECT observed, and a row it does not touch is re-read and
// judged again. Aerospike's atomic UDF has no such gap; this is SQL's equivalent.
//
// The change is applied inside the spend's own transaction, through the store's test
// hook, so the race is deterministic on both engines. A concurrent commit is seen the
// same way: the conditional UPDATE misses, and the re-read sees the committed row.
func TestSpendRechecksFreezeStateUnderTheWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	assertSpendRechecksFreezeStateUnderTheWrite(t, ctx, store, tx)
}

// TestSpendRechecksFreezeStateUnderTheWritePostgresBulk is the same contract on the bulk
// spend path, which only Postgres with batched operations exercises.
func TestSpendRechecksFreezeStateUnderTheWritePostgresBulk(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	store.settings.UtxoStore.BatchSQLOperations = true

	require.NoError(t, store.Delete(ctx, tests.TXHash))

	assertSpendRechecksFreezeStateUnderTheWrite(t, ctx, store, tests.Tx)
}

func assertSpendRechecksFreezeStateUnderTheWrite(t *testing.T, ctx context.Context, store *Store, tx *bt.Tx) {
	t.Helper()

	require.GreaterOrEqual(t, len(tx.Outputs), 2, "the fixture needs two outputs")

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	const blockHeight = 100

	// changeOutput returns a hook that applies set to output vout of tx inside the spend's
	// transaction. The hook runs on the batcher's goroutine, so it records rather than
	// asserts; hookErr is checked after the spend returns.
	var hookErr error

	changeOutput := func(vout uint32, set string, args ...interface{}) func(context.Context, *sql.Tx) {
		return func(ctx context.Context, txn *sql.Tx) {
			q := `UPDATE outputs SET ` + set + ` WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1) AND idx = $2`
			_, hookErr = txn.ExecContext(ctx, q, append([]interface{}{tx.TxIDChainHash()[:], vout}, args...)...)
		}
	}

	spend0 := utxo2.GetSpendingTx(tx, 0)
	spends0, err := utxo.GetSpends(spend0)
	require.NoError(t, err)

	t.Run("a freeze landing under the write is enforced, and nothing is written", func(t *testing.T) {
		// The legacy always-window record: frozen, from 0, no end.
		store.testBeforeSpendWrite = changeOutput(0, `frozen = TRUE, freezeFrom = 0`)
		defer func() { store.testBeforeSpendWrite = nil }()

		_, err := store.Spend(ctx, spend0, blockHeight)
		require.NoError(t, hookErr)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrUtxoConsensusFrozen), "the verdict must come from the row at the write, got: %v", err)

		// The rejected attempt must not have spent the output: the next cases spend it.
		store.testBeforeSpendWrite = nil
		require.NoError(t, store.UnFreezeUTXOs(ctx, []*utxo.Spend{spends0[0]}, store.settings))
	})

	// The change the hook makes is committed with the spend's transaction whether or not
	// the item it hit was rejected, so each case below starts from a clean row: the hook
	// must CHANGE the row for the write to miss, and a hook re-applying the state the read
	// already saw is, correctly, no change at all.
	windowNotCoveringHeight := `freezeFrom = 500, freezeUntil = 600`

	t.Run("a window landing under the write holds the policy tier", func(t *testing.T) {
		// A window that does not cover blockHeight lands after the read. The consensus
		// tier is inactive, but the policy tier now holds the coin: a mempool spend is
		// frozen, judged against the record as it is at the write.
		store.testBeforeSpendWrite = changeOutput(0, windowNotCoveringHeight)
		defer func() { store.testBeforeSpendWrite = nil }()

		_, err := store.Spend(ctx, spend0, blockHeight)
		require.NoError(t, hookErr)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrFrozen), "the policy tier of the record that landed must hold, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrUtxoConsensusFrozen), "the window does not cover the height, so this is not the consensus verdict")

		store.testBeforeSpendWrite = nil
		require.NoError(t, store.UnFreezeUTXOs(ctx, []*utxo.Spend{spends0[0]}, store.settings))
	})

	t.Run("an admissible change under the write is re-read, not committed on stale state", func(t *testing.T) {
		// The same window lands under a block-context spend (policy tier bypassed). The
		// spend is still admissible below the window — yet its verdict was computed from
		// a row that is gone, so it is reported for a retry rather than committed.
		store.testBeforeSpendWrite = changeOutput(0, windowNotCoveringHeight)
		defer func() { store.testBeforeSpendWrite = nil }()

		_, err := store.Spend(ctx, spend0, blockHeight, utxo.IgnoreFlags{IgnorePolicyFreeze: true})
		require.NoError(t, hookErr)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "an admissible spend judged on stale state must be retried, not committed, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrFrozen))

		// The retry reads the current record and admits the spend below the window.
		store.testBeforeSpendWrite = nil
		_, err = store.Spend(ctx, spend0, blockHeight, utxo.IgnoreFlags{IgnorePolicyFreeze: true})
		require.NoError(t, err)
	})

	t.Run("a competing spend landing under the write is reported with the actual spender", func(t *testing.T) {
		spend1 := utxo2.GetSpendingTx(tx, 1)
		spends1, err := utxo.GetSpends(spend1)
		require.NoError(t, err)

		other := chainhash.HashH([]byte("the spender that got there first"))
		otherSpender := spendpkg.NewSpendingData(&other, 0)

		store.testBeforeSpendWrite = changeOutput(1, `spending_data = $3`, otherSpender.Bytes())
		defer func() { store.testBeforeSpendWrite = nil }()

		_, err = store.Spend(ctx, spend1, blockHeight)
		require.NoError(t, hookErr)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrSpent), "got: %v", err)
		require.Contains(t, err.Error(), other.String(), "the error must name the transaction that actually spent the output")

		// Hand the output back for the next cases.
		store.testBeforeSpendWrite = nil
		reset := *spends1[0]
		reset.SpendingData = otherSpender
		require.NoError(t, store.Unspend(ctx, []*utxo.Spend{&reset}))
	})

	// The consensus record is a property of the outpoint, not of its spent-state, so a
	// freeze that lands together with a spend still decides an in-window spend — in the
	// order the initial validation uses, before any spent-state result.
	alwaysWindow := `frozen = TRUE, freezeFrom = 0`

	t.Run("a freeze landing with this transaction's own spend is the consensus verdict, not an idempotent success", func(t *testing.T) {
		spend1 := utxo2.GetSpendingTx(tx, 1)
		spends1, err := utxo.GetSpends(spend1)
		require.NoError(t, err)

		store.testBeforeSpendWrite = changeOutput(1, alwaysWindow+`, spending_data = $3`, spends1[0].SpendingData.Bytes())
		defer func() { store.testBeforeSpendWrite = nil }()

		_, err = store.Spend(ctx, spend1, blockHeight)
		require.NoError(t, hookErr)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrUtxoConsensusFrozen), "got: %v", err)

		store.testBeforeSpendWrite = nil
		require.NoError(t, store.Unspend(ctx, []*utxo.Spend{spends1[0]}))
		require.NoError(t, store.UnFreezeUTXOs(ctx, []*utxo.Spend{spends1[0]}, store.settings))
	})

	t.Run("a freeze landing with a competing spend is the consensus verdict, not ErrSpent", func(t *testing.T) {
		spend1 := utxo2.GetSpendingTx(tx, 1)
		other := chainhash.HashH([]byte("a competitor that also got frozen"))
		otherSpender := spendpkg.NewSpendingData(&other, 0)

		store.testBeforeSpendWrite = changeOutput(1, alwaysWindow+`, spending_data = $3`, otherSpender.Bytes())
		defer func() { store.testBeforeSpendWrite = nil }()

		_, err := store.Spend(ctx, spend1, blockHeight)
		require.NoError(t, hookErr)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrUtxoConsensusFrozen), "got: %v", err)
		require.False(t, errors.Is(err, errors.ErrSpent))
	})
}
