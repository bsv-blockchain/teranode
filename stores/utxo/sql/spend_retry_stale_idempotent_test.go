package sql

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// spendRetryHook is a store logger that runs a callback once, synchronously, at
// the point sendSpendBatch logs that it is about to retry a batch. That log line
// sits between the failed attempt's rollback and the next attempt, on the
// batcher worker goroutine, so anything the callback does to the store is what a
// concurrent writer could do inside the retry window.
type spendRetryHook struct {
	ulogger.TestLogger
	once    sync.Once
	onRetry func()
}

func (h *spendRetryHook) Warnf(format string, args ...interface{}) {
	if strings.Contains(format, "[Spend] deadlock or lifted freeze detected") {
		h.once.Do(h.onRetry)
	}

	h.TestLogger.Warnf(format, args...)
}

// TestSpendRetryRollsBackSpendWrittenOnRetry: C spends P:0 and Q:0. P:0 already
// records C's spend (history); Q:0 is unspent and frozen. Attempt 1 of the spend
// batch classifies P:0 as an idempotent match (nothing written) and then fails
// with a retryable error. Inside the retry window a concurrent writer releases
// P:0 (the rollback of another attempt at C, unwindShed, a reorg unspend).
// Attempt 2 writes a fresh spend of P:0, and the call then fails on Q:0, a
// rollback-class error. The spend of P:0 was written by this call for a
// transaction that is never created, so the rollback must reverse it: the
// correct end state is P:0 unspent.
//
// The retryable failure is injected deterministically: a trigger on transactions
// raises 'database is locked' from setDAH, which runs after every item has been
// classified and before the batch commits, and isDeadlock matches that text on
// both engines. The hook drops the trigger and performs the Unspend between the
// two attempts.
func TestSpendRetryRollsBackSpendWrittenOnRetry(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		newParent := func(seed string) *bt.Tx {
			parent := bt.NewTx()
			require.NoError(t, parent.From(strings.Repeat("1", 63)+seed, 0, "51", 30000))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			_, err := store.Create(ctx, parent, 1000)
			require.NoError(t, err)

			return parent
		}

		p, q := newParent("1"), newParent("2")

		child := bt.NewTx()
		for _, parent := range []*bt.Tx{p, q} {
			require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
		}

		for i := range child.Inputs {
			child.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		}

		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

		// History: C spent both outputs and both parents were mined.
		spends, err := store.Spend(ctx, child, 1000)
		require.NoError(t, err)
		require.Len(t, spends, 2)
		p0, q0 := spends[0], spends[1]

		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{p.TxIDChainHash(), q.TxIDChainHash()},
			utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
		require.NoError(t, err)

		// Q:0 is released and frozen, so a replay of C fails on it with a
		// rollback-class error. P:0 still records C.
		require.NoError(t, store.Unspend(ctx, []*utxo.Spend{q0}))
		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: q0.TxID, Vout: q0.Vout, UTXOHash: q0.UTXOHash}}, test.CreateBaseTestSettings(t)))
		require.Equal(t, p0.SpendingData.Bytes(), outputSpendingData(t, ctx, store, p, 0), "fixture: P:0 records C")
		require.Empty(t, outputSpendingData(t, ctx, store, q, 0), "fixture: Q:0 is unspent")

		// A higher tip makes setDAH recompute P's delete_at_height on the
		// replay, which gives the trigger below an UPDATE to intercept.
		require.NoError(t, store.SetBlockHeight(1100))

		createTrigger := "CREATE TRIGGER retry_once BEFORE UPDATE ON transactions BEGIN SELECT RAISE(ABORT, 'database is locked'); END"
		dropTrigger := "DROP TRIGGER retry_once"

		if store.engine == "postgres" {
			_, err = store.db.ExecContext(ctx, `CREATE FUNCTION raise_database_is_locked() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'database is locked'; END $$`)
			require.NoError(t, err)

			createTrigger = "CREATE TRIGGER retry_once BEFORE UPDATE ON transactions FOR EACH ROW EXECUTE FUNCTION raise_database_is_locked()"
			dropTrigger = "DROP TRIGGER retry_once ON transactions"
		}

		_, err = store.db.ExecContext(ctx, createTrigger)
		require.NoError(t, err)

		var (
			mu             sync.Mutex
			hookFired      bool
			hookErr        error
			p0AfterUnspend []byte
		)

		store.logger = &spendRetryHook{onRetry: func() {
			mu.Lock()
			defer mu.Unlock()

			hookFired = true

			if _, hookErr = store.db.ExecContext(ctx, dropTrigger); hookErr != nil {
				return
			}

			// The concurrent writer: C's spend of P:0 is reversed inside the
			// retry window.
			if hookErr = store.Unspend(ctx, []*utxo.Spend{p0}); hookErr != nil {
				return
			}

			hookErr = store.db.QueryRowContext(ctx,
				"SELECT spending_data FROM outputs WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
				p.TxIDChainHash()[:]).Scan(&p0AfterUnspend)
		}}

		replay, err := store.Spend(ctx, child, 1200)
		require.ErrorIs(t, err, errors.ErrFrozen, "the replay fails on frozen Q:0")
		require.Len(t, replay, 2)
		require.NoError(t, replay[0].Err, "P:0 was accepted on the retry")

		mu.Lock()
		defer mu.Unlock()

		require.True(t, hookFired, "attempt 1 must fail retryably after classifying P:0 as idempotent")
		require.NoError(t, hookErr)
		require.Empty(t, p0AfterUnspend, "the concurrent Unspend released P:0 inside the retry window")

		require.Empty(t, outputSpendingData(t, ctx, store, q, 0), "Q:0 stays unspent")
		require.Empty(t, outputSpendingData(t, ctx, store, p, 0),
			"the retry wrote a fresh spend of P:0 for a call that then failed on Q:0; the rollback must reverse it rather than leave P:0 spent by a transaction that was never created")
	})
}

// TestSpendRollbackReversesOrphanWhenASiblingParentIsMissing: C spends P:0, M:0
// and Q:0, and M is not in the store. An earlier attempt at C wrote P:0 and Q:0
// and never created C, so P:0 is an orphan spend of a transaction this store
// does not hold. Q:0 has since gone to D. On the retry P:0 is an idempotent
// match, M:0 is missing and Q:0 is spent by another transaction, a
// rollback-class error. A missing parent is not a replay answer, so the
// rollback must reverse P:0: leaving it spent would refuse P:0's next
// legitimate spender with a txid this node never stored. The marker case, where
// the match is a confirmed spend and must stay, is pinned by
// TestPrunedReplayRollbackPreservesHistoricalSpend.
func TestSpendRollbackReversesOrphanWhenASiblingParentIsMissing(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		newParent := func(seed string, create bool) *bt.Tx {
			parent := bt.NewTx()
			require.NoError(t, parent.From(strings.Repeat("2", 63)+seed, 0, "51", 30000))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

			if create {
				_, err := store.Create(ctx, parent, 1000)
				require.NoError(t, err)
			}

			return parent
		}

		p, m, q := newParent("1", true), newParent("2", false), newParent("3", true)

		spender := func(parents ...*bt.Tx) *bt.Tx {
			tx := bt.NewTx()
			for _, parent := range parents {
				require.NoError(t, tx.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			}

			for i := range tx.Inputs {
				tx.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			}

			require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			return tx
		}

		child := spender(p, m, q)

		// The earlier attempt's leftover: P:0 records C, and C has no record.
		// The store reverses such an attempt itself when it can, so the state is
		// seeded directly, as a crash or a failed rollback would leave it.
		cSpends, err := utxo.GetSpends(child)
		require.NoError(t, err)

		_, err = store.db.ExecContext(ctx,
			"UPDATE outputs SET spending_data = $1 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $2)",
			cSpends[0].SpendingData.Bytes(), p.TxIDChainHash()[:])
		require.NoError(t, err)
		require.Equal(t, cSpends[0].SpendingData.Bytes(), outputSpendingData(t, ctx, store, p, 0), "fixture: P:0 is an orphan spend by C")

		// Q:0 goes to D.
		_, err = store.Spend(ctx, spender(q), 1000)
		require.NoError(t, err)

		// The retry: P:0 idempotent, M:0 missing, Q:0 spent by D.
		_, err = store.Spend(ctx, child, 1000)
		require.ErrorIs(t, err, errors.ErrSpent, "Q:0 is spent by D")

		require.Empty(t, outputSpendingData(t, ctx, store, p, 0),
			"the orphan spend of P:0 is reversed: a missing sibling parent is not a pruned-replay answer")
	})
}
