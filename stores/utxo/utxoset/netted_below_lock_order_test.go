package utxoset

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// A chunk of the below-checkpoint write takes its transaction locks before the fence, as every
// other create does. While it waits on a transaction lock that another writer holds, it holds no
// fence lock, so the stamp can take the fence exclusive. In the reverse order the chunk holds
// the fence shared while it waits. The writer that holds the transaction lock then queues for the
// fence behind the stamp's exclusive request, and Postgres aborts one of them as a deadlock.
func TestNettedBelowTakesTransactionLocksBeforeTheFence(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 975

	w := tests.BuildMultiWorkload(t, 0x7d, 1, 1)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)
	id := w.Txs[0].TxIDChainHash()

	// Another writer holds the lock of the list's first transaction.
	writer, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = writer.Rollback(ctx) }()

	key := int64(binary.BigEndian.Uint64(id[:8])) //nolint:gosec // a hash prefix as a lock key
	_, err = writer.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, key)
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() {
		_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
		done <- err
	}()

	// Wait until the chunk waits on that lock.
	require.Eventually(t, func() bool {
		var n int
		require.NoError(t, s.pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`).Scan(&n))

		return n > 0
	}, 10*time.Second, 20*time.Millisecond)

	// The stamp's exclusive request is granted at once: the waiting chunk holds no fence lock.
	stamp, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	var got bool
	require.NoError(t, stamp.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1, $2)`,
		int32(fenceLockKey1), int32(fenceLockKey2)).Scan(&got))
	require.NoError(t, stamp.Rollback(ctx))
	require.True(t, got, "the chunk holds the fence while it waits on a transaction lock")

	require.NoError(t, writer.Rollback(ctx))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the chunk did not finish after the transaction lock was released")
	}
}
