package utxoset

import (
	"context"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// newSmallPoolStore opens the store with a deliberately tiny connection pool, so the
// concurrency needed to expose a nested acquire is four goroutines rather than the three
// hundred the mainnet box would need.
func newSmallPoolStore(t *testing.T, maxConns int) (*Store, context.Context) {
	t.Helper()

	ctx := context.Background()
	dsn := testDSN(t)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err, "opening the test pool")
	require.NoError(t, pool.Ping(ctx), "reaching the test postgres")

	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS utxo CASCADE;
	                       DROP TABLE IF EXISTS spend_journal CASCADE;
	                       DROP TABLE IF EXISTS applied_block CASCADE;
	                       DROP TABLE IF EXISTS applied_chunk CASCADE;`)
	pool.Close()

	u, err := url.Parse(dsn)
	require.NoError(t, err)

	q := u.Query()
	q.Set("pool_max_conns", strconv.Itoa(maxConns))
	u.RawQuery = q.Encode()

	s, err := New(ctx, ulogger.TestLogger{}, settings.NewSettings(), u)
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close(ctx) })

	return s, ctx
}

// TestConcurrentSpendsDoNotDeadlockThePool pins a deadlock that is sitting in front of
// the running mainnet sync.
//
// SpendAndCreate takes a pool connection and holds it for the whole call
// (spend_and_create.go, s.pool.Begin). Inside that held transaction, spendIn calls
// ensureSpendJournalPartition, which issues its DDL on s.pool -- a SECOND acquire from
// the same pool. Once the number of concurrent spenders reaches pool_max_conns, every
// connection is held by a transaction whose owner is blocked waiting for a connection
// that cannot be released until it makes progress. Nothing times out on its own.
//
// It has not fired on mainnet only because early blocks carry one or two transactions.
// The partition cache hides it for 47 of every 48 heights and resets to zero on every
// restart, so the trigger is the first large block after a restart, or any large block on
// a 48-height boundary. Block validation runs the spend phase at
// spendBatcherSize x spendBatcherConcurrency x 2 goroutines against one shared pool.
func TestConcurrentSpendsDoNotDeadlockThePool(t *testing.T) {
	s, ctx := newSmallPoolStore(t, 4)

	const n = 16

	parents := make([]*bt.Tx, n)
	children := make([]*bt.Tx, n)

	for i := range n {
		p := mkTx(t, 1, uint64(10_000+i))
		_, err := s.Create(ctx, p, 100)
		require.NoError(t, err)

		c := bt.NewTx()
		require.NoError(t, c.FromUTXOs(&bt.UTXO{
			TxIDHash: p.TxIDChainHash(), Vout: 0,
			LockingScript: p.Outputs[0].LockingScript, Satoshis: p.Outputs[0].Satoshis,
		}))

		parents[i], children[i] = p, c
	}

	// Force the partition cache to miss, which is the state after every restart.
	s.journalLeaf.Store(0)

	done := make(chan struct{})

	go func() {
		defer close(done)

		var wg sync.WaitGroup

		for i := range n {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				_, _, _ = s.SpendAndCreate(ctx, children[i], 200)
			}(i)
		}

		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlocked: every pool connection is held by a transaction waiting for a pool connection")
	}
}
