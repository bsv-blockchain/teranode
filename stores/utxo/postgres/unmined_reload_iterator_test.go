package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// collectUnminedReload drains GetUnminedTxIterator into the set of returned
// (non-skip) tx hashes.
func collectUnminedReload(t *testing.T, st *Store) map[chainhash.Hash]struct{} {
	t.Helper()
	ctx := context.Background()

	iter, err := st.GetUnminedTxIterator()
	require.NoError(t, err)

	got := make(map[chainhash.Hash]struct{})

	for {
		batch, batchErr := iter.Next(ctx)
		require.NoError(t, batchErr)

		if len(batch) == 0 {
			break
		}

		for _, utx := range batch {
			if utx.Skip || utx.Node == nil {
				continue
			}

			got[utx.Node.Hash] = struct{}{}
		}
	}

	require.NoError(t, iter.Close())

	return got
}

// TestGetUnminedTxIterator_ReloadReturnsOnlyStillUnminedNonConflicting is the
// reload-path (block-assembly restart) correctness test. It builds a
// pending_unmined side-table that is a SUPERSET of the genuinely-unmined set —
// containing rows for a tx that has since been mined (stale row lingers, the
// lever-1 hot-path DELETE was removed) and a tx that has been marked conflicting
// — and asserts GetUnminedTxIterator returns EXACTLY the still-unmined,
// non-conflicting txs. This proves the JOIN's re-filter
// (t.unmined_since IS NOT NULL AND t.conflicting = false) excludes the stale
// superset rows even though their pending_unmined rows remain.
func TestGetUnminedTxIterator_ReloadReturnsOnlyStillUnminedNonConflicting(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()
	cleanPendingUnmined(t, st)
	require.NoError(t, st.SetBlockHeight(100))

	// Tx A: genuinely unmined and non-conflicting → must be returned.
	txA := testExtendedTx(t)
	txA.LockTime = 900
	_, err := st.Create(ctx, txA, 100)
	require.NoError(t, err)
	hA := txA.TxIDChainHash()

	// Tx B: unmined, then mined on the longest chain. SetMinedMulti clears
	// txs.unmined_since but (lever-1) leaves the pending_unmined row lingering →
	// a STALE superset row the re-filter must exclude. Project before mining so
	// the row actually lands (the projector's flush is mined-aware).
	txB := testExtendedTx(t)
	txB.LockTime = 901
	_, err = st.Create(ctx, txB, 100)
	require.NoError(t, err)
	hB := txB.TxIDChainHash()
	require.NoError(t, st.flushPendingUnmined(ctx))
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{hB}, utxo.MinedBlockInfo{
		BlockID: 42, BlockHeight: 100, SubtreeIdx: 0, OnLongestChain: true,
	})
	require.NoError(t, err)

	// Tx C: unmined but marked conflicting → must be excluded.
	txC := testExtendedTx(t)
	txC.LockTime = 902
	_, err = st.Create(ctx, txC, 100)
	require.NoError(t, err)
	hC := txC.TxIDChainHash()
	require.NoError(t, st.flushPendingUnmined(ctx))
	_, _, err = st.SetConflicting(ctx, []chainhash.Hash{*hC}, true)
	require.NoError(t, err)

	// Drain the projector so pending_unmined has rows for A (and lingering B).
	require.NoError(t, st.flushPendingUnmined(ctx))

	// Preconditions: pending_unmined is a superset — it holds rows for the mined
	// B and (typically) the conflicting C in addition to the live A. The stale-B
	// row is the row the re-filter must reject.
	var bRowExists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, hB[:]).Scan(&bRowExists))
	require.True(t, bRowExists, "precondition: mined tx B's pending_unmined row must linger (stale superset)")

	got := collectUnminedReload(t, st)

	require.Contains(t, got, *hA, "genuinely-unmined non-conflicting tx A must be returned")
	require.NotContains(t, got, *hB, "mined tx B must be excluded by re-filter t.unmined_since IS NOT NULL")
	require.NotContains(t, got, *hC, "conflicting tx C must be excluded by re-filter t.conflicting = false")
	require.Len(t, got, 1, "reload must return EXACTLY the still-unmined non-conflicting set")
}

// TestGetUnminedTxIterator_ReloadDrivesFromPendingUnmined proves the reload
// reads from pending_unmined and NOT directly from txs: a tx that is unmined in
// txs but ABSENT from pending_unmined must NOT be returned. This FAILS against
// the old txs-direct implementation (SELECT ... FROM txs WHERE unmined_since IS
// NOT NULL), which would return it, and PASSES against the pending_unmined JOIN.
func TestGetUnminedTxIterator_ReloadDrivesFromPendingUnmined(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()
	cleanPendingUnmined(t, st)
	require.NoError(t, st.SetBlockHeight(100))

	// Create an unmined tx, then remove its pending_unmined row so txs still
	// says unmined but the side-table does not.
	tx := testExtendedTx(t)
	tx.LockTime = 910
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	require.NoError(t, st.flushPendingUnmined(ctx))
	_, err = st.pool.Exec(ctx, `DELETE FROM pending_unmined WHERE hash = $1`, h[:])
	require.NoError(t, err)

	// Sanity: txs still marks it unmined, but pending_unmined is empty.
	var txsUnmined int
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT count(*) FROM txs WHERE hash=$1 AND unmined_since IS NOT NULL`, h[:]).Scan(&txsUnmined))
	require.Equal(t, 1, txsUnmined, "precondition: tx must be unmined in txs")

	var puCount int
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_unmined`).Scan(&puCount))
	require.Equal(t, 0, puCount, "precondition: pending_unmined must be empty")

	got := collectUnminedReload(t, st)
	require.NotContains(t, got, *h, "tx absent from pending_unmined must NOT be returned (reload drives from pending_unmined, not txs)")
	require.Empty(t, got, "reload must return nothing when pending_unmined is empty")
}

// TestGetUnminedTxIterator_ReloadEmptyFastPath verifies the bare-EXISTS fast
// path: when pending_unmined is empty (the common IBD case), the iterator
// returns an empty result and never opens the JOIN query rows. We assert the
// empty result; the iterator's done=true short-circuit (no rows opened) is the
// mechanism, exercised by Next returning nothing immediately.
func TestGetUnminedTxIterator_ReloadEmptyFastPath(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()
	cleanPendingUnmined(t, st)

	var puCount int
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_unmined`).Scan(&puCount))
	require.Equal(t, 0, puCount, "precondition: pending_unmined empty")

	iter, err := st.GetUnminedTxIterator()
	require.NoError(t, err)

	// Fast path: done=true, no rows handle opened.
	it, ok := iter.(*unminedTxIterator)
	require.True(t, ok, "iterator is the postgres unminedTxIterator")
	require.True(t, it.done, "empty pending_unmined must take the done=true fast path")
	require.Nil(t, it.rows, "fast path must not open a rows handle (no txs scan)")

	batch, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Empty(t, batch, "empty pending_unmined yields an empty iterator")

	require.NoError(t, iter.Close())
}

// TestGetUnminedTxIterator_ReloadSeesUnflushedProjectorBuffer pins the
// read-your-writes barrier: GetUnminedTxIterator must itself drain the
// write-behind projector, so a reload issued immediately after Create returned
// sees the tx even though the projector's ticker (puFlushInterval) has not
// fired yet. No explicit flushPendingUnmined call here — that is the point.
// This FAILS against an implementation without the barrier (the EXISTS fast
// path short-circuits to an empty iterator) and is the in-package twin of
// stores/utxo/tests TestUnminedTxIteratorPostgres/mixed_mined-unmined, which
// cannot reach the unexported flush hook.
func TestGetUnminedTxIterator_ReloadSeesUnflushedProjectorBuffer(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()
	cleanPendingUnmined(t, st)
	require.NoError(t, st.SetBlockHeight(100))

	tx := testExtendedTx(t)
	tx.LockTime = 920
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)

	got := collectUnminedReload(t, st)
	require.Contains(t, got, *tx.TxIDChainHash(), "reload must see a create that returned before the projector ticker fired")
}
