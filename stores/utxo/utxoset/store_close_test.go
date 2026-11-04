package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestCloseDrainsAQueuedGetBeforeClosingThePool covers the batcher Close skipped: a Get
// queued on getBatcher but not yet flushed when Close is called.
//
// Close must stop getBatcher the same way it stops createBatcher, and wait for the
// callback to actually finish, before it closes the pool underneath it. Without that, the
// queued read either panics reaching a closed pool or simply never gets an answer, because
// nothing forced the batcher to flush or waited for it to.
func TestCloseDrainsAQueuedGetBeforeClosingThePool(t *testing.T) {
	s, ctx := newTestStoreWith(t, func(st *settings.Settings) {
		st.UtxoStore.GetBatcherSize = 8
		st.UtxoStore.GetBatcherDurationMillis = 60_000 // long enough it never fires on its own
	})
	require.NotNil(t, s.getBatcher, "test setup: the batcher must actually be built")

	tx := mkTx(t, 2, 5000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	done := make(chan getResult, 1)
	s.getBatcher.PutCtx(ctx, &getItem{hash: *tx.TxIDChainHash(), done: done})

	require.NotPanics(t, func() {
		require.NoError(t, s.Close(context.Background()))
	})

	select {
	case res := <-done:
		require.NoError(t, res.err)
		require.NotNil(t, res.data)
	default:
		t.Fatal("Close returned before the queued get was drained and answered")
	}
}

// TestCloseDrainsAQueuedLockBeforeClosingThePool is the same fact for lockBatcher, the
// other batcher Close left running.
func TestCloseDrainsAQueuedLockBeforeClosingThePool(t *testing.T) {
	s, ctx := newTestStoreWith(t, func(st *settings.Settings) {
		st.UtxoStore.LockedBatcherSize = 8
		st.UtxoStore.LockedBatcherDurationMillis = 60_000
	})
	require.NotNil(t, s.lockBatcher, "test setup: the batcher must actually be built")

	tx := mkTx(t, 2, 5000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	s.lockBatcher.PutCtx(ctx, &lockItem{hash: *tx.TxIDChainHash(), value: true, errCh: errCh})

	require.NotPanics(t, func() {
		require.NoError(t, s.Close(context.Background()))
	})

	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
		t.Fatal("Close returned before the queued lock change was drained and answered")
	}
}

// TestACallAfterCloseIsRefusedNotPanicked covers the four entry points that hand an item to a
// batcher: Create, Get, SetLocked and SpendAndCreate. Close closes each batcher's channel, and a
// send on a closed channel panics, so a caller still running when the daemon stops the store
// would take the process down part-way through Close. Each must instead take its direct path
// and get the closed pool's error.
func TestACallAfterCloseIsRefusedNotPanicked(t *testing.T) {
	s, ctx := newTestStoreWith(t, func(st *settings.Settings) {
		st.UtxoStore.StoreBatcherSize = 8
		st.UtxoStore.GetBatcherSize = 8
		st.UtxoStore.LockedBatcherSize = 8
	})
	require.NotNil(t, s.createBatcher, "test setup: the create batcher must be built")
	require.NotNil(t, s.spendAndCreateBatcher, "test setup: the spend-and-create batcher must be built")
	require.NotNil(t, s.getBatcher, "test setup: the get batcher must be built")
	require.NotNil(t, s.lockBatcher, "test setup: the lock batcher must be built")

	held := mkTx(t, 2, 5000)
	_, err := s.Create(ctx, held, 100)
	require.NoError(t, err)

	require.NoError(t, s.Close(context.Background()))

	require.NotPanics(t, func() {
		_, err := s.Create(ctx, mkTx(t, 2, 6000), 100)
		require.Error(t, err, "Create after Close")
	})
	require.NotPanics(t, func() {
		_, err := s.Get(ctx, held.TxIDChainHash())
		require.Error(t, err, "Get after Close")
	})
	require.NotPanics(t, func() {
		require.Error(t, s.SetLocked(ctx, []chainhash.Hash{*held.TxIDChainHash()}, true), "SetLocked after Close")
	})
	require.NotPanics(t, func() {
		_, _, err := s.SpendAndCreate(ctx, mkTx(t, 2, 7000), 100)
		require.Error(t, err, "SpendAndCreate after Close")
	})
}
