package utxoset

import (
	"context"
	"strconv"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

// TestTxBodyIsRangePartitionedOnCreatedHeight pins the shape that makes the body window
// droppable.
//
// The body is the only part of a transaction whose life is bounded by a horizon rather than
// by its coins. Everything else on tx_ident is pinned for as long as any output is unspent,
// at any age, because the validator reads the parent's block ids and heights for every input.
// Keeping the body there too would make the transaction archive permanent for the whole
// pinned population, measured at 136 GB of out-of-line storage today against roughly 95 GB of
// free space on the mainnet box, and projecting past the disk at the tip.
//
// Range partitioning on created_height is what turns reclaim into dropping a file rather than
// deleting rows: a drop returned 883 MB to the operating system for 228 KB of crash-recovery
// journal, where the equivalent delete plus vacuum returned 128 KB for 1.75 GB.
func TestTxBodyIsRangePartitionedOnCreatedHeight(t *testing.T) {
	s, ctx := newTestStore(t)

	var strategy string
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT partstrat::text FROM pg_partitioned_table WHERE partrelid = 'tx_body'::regclass`).Scan(&strategy))
	require.Equal(t, "r", strategy, "range partitioned, so a whole window can be dropped at once")
}

// TestTxBodyHasASentinelPartitionBelowEveryLiveWindow covers the case that takes transaction
// intake down completely if it is missing.
//
// A deep invalidation drops the tip below the lowest live partition, and a mempool create
// files at tip plus one. With no partition covering that height the insert is refused
// outright, so the node stops accepting transactions. The catch-all must be an explicit range
// from MINVALUE, never a DEFAULT partition: on 18.6 a default partition makes
// DETACH PARTITION ... CONCURRENTLY impossible on every other partition of the table, and the
// concurrent form is what keeps the drop from blocking readers.
func TestTxBodyHasASentinelPartitionBelowEveryLiveWindow(t *testing.T) {
	s, ctx := newTestStore(t)

	var isDefault bool
	require.NoError(t, s.pool.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM pg_class c
              JOIN pg_inherits i ON i.inhrelid = c.oid
             WHERE i.inhparent = 'tx_body'::regclass
               AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT')`).Scan(&isDefault))
	require.False(t, isDefault, "a DEFAULT partition blocks concurrent detach on every sibling")

	// A create far below any live window must still land somewhere.
	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 1)
	require.NoError(t, err, "a create below the lowest live window must not be refused")

	h := tx.TxIDChainHash()

	var body []byte
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT raw_tx FROM tx_body WHERE txid = $1`, h[:]).Scan(&body))
	require.Equal(t, tx.Bytes(), body)
}

// TestCreateWritesTheBody is the round trip. The body is the one thing that cannot be
// reconstructed: unlocking scripts exist only in the transaction, and the node keeps almost
// no blocks.
func TestCreateWritesTheBody(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 4_242)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	var body []byte
	var filedAt int32
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT created_height, raw_tx FROM tx_body WHERE txid = $1`, h[:]).Scan(&filedAt, &body))

	require.Equal(t, tx.Bytes(), body)
	require.Equal(t, int32(700_000), filedAt,
		"the body is filed by the height tx_ident records, so the two agree on where to look")
}

// TestCreateDoesNotWriteASecondBodyForADuplicate: the claim gates the body too, or a
// re-applied block writes the bytes again for every transaction in it.
func TestCreateDoesNotWriteASecondBodyForADuplicate(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_100)
	require.Error(t, err)

	h := tx.TxIDChainHash()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_body WHERE txid = $1`, h[:]).Scan(&n))
	require.Equal(t, 1, n, "a duplicate must not file a second body, at a second height")
}

// TestCreateIsAtomicAcrossItsThreeWrites pins that a Create either lands completely or not
// at all.
//
// Create writes three things: the identity row, the serialized bytes, and one coin row per
// spendable output. Run them on the connection pool rather than inside one transaction and
// each commits separately, so a failure partway leaves the earlier writes standing.
//
// The damaging case is an identity row with no bytes. A read cannot tell that apart from a
// transaction whose bytes have aged out of their window, because both are a row with no body
// row beside it. And a retry is refused, because the identity claim reports the transaction
// as already present, so the bytes are never written. The transaction stays body-less
// permanently and nothing reports it.
//
// The forced failure here is a body window that does not exist. Detaching one leaves the
// range unrouted, so the body insert fails while the identity claim has already succeeded.
func TestCreateIsAtomicAcrossItsThreeWrites(t *testing.T) {
	s, ctx := newTestStore(t)

	// Make the window for height 700_000 exist, then take it away, so the body insert fails
	// after the claim has been made.
	require.NoError(t, s.ensureTxBodyPartition(ctx, 700_000))

	window := 700_000 / TxBodyPartitionBlocks
	_, err := s.pool.Exec(ctx,
		`ALTER TABLE tx_body DETACH PARTITION tx_body_w`+itoa(window))
	require.NoError(t, err)

	// Drop the detached table, or it outlives this test as an orphan and breaks the next one.
	//
	// DROP TABLE tx_body CASCADE removes the parent and its ATTACHED partitions. A detached
	// one is no longer a partition, so it survives, and the next test's
	// CREATE TABLE IF NOT EXISTS then finds the name taken, skips silently, and never
	// attaches it. Every create at that height afterwards fails with "no partition found",
	// in a test that did nothing wrong. This is the same orphan hazard the spend journal's
	// reclaim has to handle, arriving here through a test rather than a crash.
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DROP TABLE IF EXISTS tx_body_w`+itoa(window))
	})

	tx := mkTx(t, 2, 1_000)
	_, err = s.Create(ctx, tx, 700_000)
	require.Error(t, err, "the body cannot be stored, so the create must fail")

	h := tx.TxIDChainHash()

	var idents, coins int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_ident WHERE txid = $1`, h[:]).Scan(&idents))
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM utxo WHERE txid = $1`, h[:]).Scan(&coins))

	require.Zero(t, idents,
		"a failed create must leave no identity row: one without its bytes reads as aged-out forever, and the retry is refused as a duplicate")
	require.Zero(t, coins, "and no coins")
}

// itoa keeps the partition name readable at the call site.
func itoa(i int) string { return strconv.Itoa(i) }

// tableExists says whether a table of this name is present in the current schema, whether or
// not it is still attached to a parent.
func tableExists(t *testing.T, s *Store, ctx context.Context, name string) bool {
	t.Helper()

	var found bool
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = $1 AND n.nspname = current_schema())`, name).Scan(&found))

	return found
}

// TestTxBodyReclaimRecoversAnOrphanedWindow covers the crash window that the listing query used
// to be blind to.
//
// Discarding a window is two statements: DETACH PARTITION ... CONCURRENTLY, then DROP TABLE. A
// crash between them leaves a fully standalone table, and PostgreSQL removes its pg_inherits row
// when it detaches. The listing used to INNER JOIN pg_inherits, so from that moment the window
// was invisible to every future session and its disk was never returned. Nothing reported it
// either, because the only symptom is free disk shrinking.
//
// The journal's own listing has handled this since it was written. This is the same handling,
// ported.
func TestTxBodyReclaimRecoversAnOrphanedWindow(t *testing.T) {
	s, ctx := newTestStore(t)

	s.bodyRetention = 96

	require.NoError(t, s.ensureTxBodyPartition(ctx, 100)) // window 2
	require.NoError(t, s.ensureTxBodyPartition(ctx, 500)) // window 10

	// Simulate the crash: detach window 2 and stop, exactly as a kill between the two
	// statements would leave it.
	_, err := s.pool.Exec(ctx, `ALTER TABLE tx_body DETACH PARTITION tx_body_w2 CONCURRENTLY`)
	require.NoError(t, err)

	var isPartition bool
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT relispartition FROM pg_class WHERE oid = 'tx_body_w2'::regclass`).Scan(&isPartition))
	require.False(t, isPartition, "the orphan is no longer a partition, which is why pg_inherits cannot find it")

	dropped, err := s.dropTxBodyWindowsBelow(ctx, 500+96)
	require.NoError(t, err)
	require.Positive(t, dropped)

	require.False(t, tableExists(t, s, ctx, "tx_body_w2"),
		"an orphaned window must be reclaimed by the next session, or its disk is lost for good")
}

// TestTxBodyReclaimRecoversAWindowStuckMidDetach is the worse of the two crash states, because
// it does not leak one window, it stops the entire cleanup.
//
// A crash DURING a concurrent detach leaves the window marked detach-pending. PostgreSQL then
// refuses every further attach and detach on tx_body. So the detach in this loop fails on every
// call, the call returns an error, and Prune returns before it reaches the journal loop at all.
// Discarding transaction bytes AND discarding undo history both stop. Creating a new window at
// the next 48-block rollover would fail too.
//
// Reaching this state on purpose needs a catalog write, because the only natural route is an
// interrupted DETACH ... CONCURRENTLY and that cannot be timed reliably. If the test role cannot
// write the catalog the test SKIPS rather than passing, which matters: an earlier version of it
// tried to reach the state with a rolled-back transaction, that form cannot produce it at all,
// and the test then asserted "nothing is pending" against a database where nothing ever was.
func TestTxBodyReclaimRecoversAWindowStuckMidDetach(t *testing.T) {
	s, ctx := newTestStore(t)

	s.bodyRetention = 96

	require.NoError(t, s.ensureTxBodyPartition(ctx, 100)) // window 2
	require.NoError(t, s.ensureTxBodyPartition(ctx, 200)) // window 4
	require.NoError(t, s.ensureTxBodyPartition(ctx, 500)) // window 10

	if _, err := s.pool.Exec(ctx, `UPDATE pg_inherits SET inhdetachpending = true
         WHERE inhrelid = 'tx_body_w2'::regclass AND inhparent = 'tx_body'::regclass`); err != nil {
		t.Skipf("cannot reach the detach-pending state here, which needs a catalog write: %v", err)
	}

	var pending bool
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT COALESCE(bool_or(i.inhdetachpending), false) FROM pg_inherits i
          WHERE i.inhparent = 'tx_body'::regclass`).Scan(&pending))
	require.True(t, pending, "the precondition must actually hold, or this test proves nothing")

	dropped, err := s.dropTxBodyWindowsBelow(ctx, 500+96)
	require.NoError(t, err,
		"a window stuck mid-detach must be finalised, not returned as an error that stops the whole prune")
	require.Positive(t, dropped)

	var stuck bool
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT COALESCE(bool_or(i.inhdetachpending), false) FROM pg_inherits i
          WHERE i.inhparent = 'tx_body'::regclass`).Scan(&stuck))
	require.False(t, stuck, "leaving it pending stops every later detach and every new window")
}

// TestTxBodyReclaimTakesTheOldestWindowFirst.
//
// The listing query has no ORDER BY, so without an explicit sort the catalog hands windows back
// in whatever order it scanned them. With one window retiring every 48 blocks and nothing behind,
// that does not matter. With a backlog, and the mainnet box reached 601 of these holding 14 GB,
// it decides which disk comes back first and whether the oldest surviving window is a usable
// measure of progress at all.
func TestTxBodyReclaimTakesTheOldestWindowFirst(t *testing.T) {
	s, ctx := newTestStore(t)

	s.bodyRetention = 96

	// Created out of order on purpose: ascending creation would pass against unsorted code by
	// luck, because the catalog would most likely return them in creation order.
	for _, h := range []uint32{500, 100, 350, 200} {
		require.NoError(t, s.ensureTxBodyPartition(ctx, h))
	}

	// A cutoff that retires windows 2 and 4 but not 7 and 10.
	dropped, err := s.dropTxBodyWindowsBelow(ctx, 250+96)
	require.NoError(t, err)
	require.Equal(t, 2, dropped)

	require.False(t, tableExists(t, s, ctx, "tx_body_w2"), "window 2 is the oldest and must go first")
	require.False(t, tableExists(t, s, ctx, "tx_body_w4"))
	require.True(t, tableExists(t, s, ctx, "tx_body_w7"), "still inside the horizon")
	require.True(t, tableExists(t, s, ctx, "tx_body_w10"))
}

func bodyExists(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) bool {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_body WHERE txid = $1`, hashBytes(tx)).Scan(&n))

	return n > 0
}

// TestBodyWindowsBelowRetentionAreDropped. The transaction bytes are the one part with a
// horizon rather than a dependency, so their windows go wholesale.
func TestBodyWindowsBelowRetentionAreDropped(t *testing.T) {
	s, ctx := newTestStore(t)

	old := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, old, 100)
	require.NoError(t, err)

	recent := mkTx(t, 1, 2_000)
	_, err = s.Create(ctx, recent, 900)
	require.NoError(t, err)

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	// Tip 1,000, so anything filed below 712 is past the 288-block horizon.
	_, err = svc.Prune(ctx, 1_000, "deadbeef")
	require.NoError(t, err)

	require.False(t, bodyExists(t, s, ctx, old), "filed at 100, far past the horizon")
	require.True(t, bodyExists(t, s, ctx, recent), "filed at 900, still inside it")

	require.True(t, identExists(t, s, ctx, old),
		"and the identity row survives its body: it is still needed while its coin is unspent")
}
