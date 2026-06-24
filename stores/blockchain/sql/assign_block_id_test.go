package sql

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestAssignBlockID_IdempotentPerHash(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	h := chainhash.HashH([]byte("block-A"))

	id1, err := s.AssignBlockID(ctx, &h)
	require.NoError(t, err)
	require.NotZero(t, id1)

	id2, err := s.AssignBlockID(ctx, &h)
	require.NoError(t, err)
	require.Equal(t, id1, id2, "same hash must return the same reserved id")

	h2 := chainhash.HashH([]byte("block-B"))
	id3, err := s.AssignBlockID(ctx, &h2)
	require.NoError(t, err)
	require.NotEqual(t, id1, id3)
}

func TestAssignBlockID_ConcurrentCallersConverge(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	h := chainhash.HashH([]byte("block-race"))

	const n = 16
	ids := make([]uint64, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id, err := s.AssignBlockID(ctx, &h)
			require.NoError(t, err)
			ids[i] = id
		}(i)
	}
	wg.Wait()

	for i := 1; i < n; i++ {
		require.Equal(t, ids[0], ids[i], "all concurrent callers for one hash must get one id")
	}
}

func TestAssignBlockID_ClearedOnCommit(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	reserved, err := s.AssignBlockID(ctx, block1.Hash())
	require.NoError(t, err)
	require.NotZero(t, reserved)

	storedID, _, err := s.StoreBlock(ctx, block1, "test", options.WithID(reserved))
	require.NoError(t, err)
	require.Equal(t, reserved, storedID)

	require.Nil(t, s.blockIDReservations.Get(*block1.Hash()), "reservation must be cleared on commit")

	again, err := s.AssignBlockID(ctx, block1.Hash())
	require.NoError(t, err)
	require.Equal(t, reserved, again, "AssignBlockID must return the committed id after reservation is cleared")
}

// Simulates the legacy + blockvalidation race over the SAME block: both call
// AssignBlockID concurrently, then one commits. The committed block id MUST
// equal the id every caller saw — i.e. no phantom id can exist.
func TestAssignBlockID_TwoPathRace_NoPhantom(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	var legacyID, catchupID uint64
	var legacyErr, catchupErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		legacyID, legacyErr = s.AssignBlockID(ctx, block1.Hash())
	}()
	go func() {
		defer wg.Done()
		catchupID, catchupErr = s.AssignBlockID(ctx, block1.Hash())
	}()
	wg.Wait()
	require.NoError(t, legacyErr)
	require.NoError(t, catchupErr)

	require.Equal(t, legacyID, catchupID, "both ingestion paths must get the same id")

	storedID, _, err := s.StoreBlock(ctx, block1, "test", options.WithID(legacyID))
	require.NoError(t, err)
	require.Equal(t, legacyID, storedID)

	// The id every path used IS a committed, on-chain block — never a phantom.
	got, ok, err := s.blockIDByHash(ctx, block1.Hash())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, storedID, got)
}

// TestReserveDurableBlockID_CommittedDuringReserveReturnsCommittedID is the #1056
// review regression for the cross-process "we won the INSERT" race: an instance
// can pass AssignBlockID's under-lock committed-check, then another instance
// commits the block (under a different id) and StoreBlock deletes its reservation
// row, so this instance's own nextval wins the INSERT and the re-read returns it —
// a divergent (phantom) id for an already-committed hash. reserveDurableBlockID's
// final committed-recheck must catch this and return the committed id instead.
//
// Deterministic stand-in: commit the block (clears its reservation), then call
// reserveDurableBlockID directly for that committed hash — its INSERT succeeds
// (no row) but the committed-recheck must still return the committed id.
func TestReserveDurableBlockID_CommittedDuringReserveReturnsCommittedID(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	reserved, err := s.AssignBlockID(ctx, block1.Hash())
	require.NoError(t, err)
	committedID, _, err := s.StoreBlock(ctx, block1, "test", options.WithID(reserved))
	require.NoError(t, err)
	require.Equal(t, reserved, committedID)

	got, err := s.reserveDurableBlockID(ctx, block1.Hash())
	require.NoError(t, err)
	require.Equal(t, committedID, got, "must return the committed id, not a divergent fresh reservation")
}

// TestReservationSweep_RunsWithInMemoryChainCheckDisabled is the #1056 review
// regression: block_id_reservations is written by AssignBlockID regardless of
// blockchain_use_in_memory_chain_check, so the age-based sweep must run regardless
// too. The sweep loop used to be started only inside the useInMemory branch
// (backgroundRefreshLoop), so a default (toggle-off) node never reclaimed
// reservations for blocks that got an id but never committed — unbounded growth.
// Here the toggle is off (the default) and the dedicated sweeper must still reclaim
// a stale row.
func TestReservationSweep_RunsWithInMemoryChainCheckDisabled(t *testing.T) {
	oldInterval := reservationSweepInterval
	reservationSweepInterval = 50 * time.Millisecond
	defer func() { reservationSweepInterval = oldInterval }()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.UseInMemoryChainCheck = false // the default — sweep must still run

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// A stale reservation, older than staleReservationSweepAge.
	staleHash := chainhash.HashH([]byte("toggle-off-stale"))
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO block_id_reservations (hash, block_id, reserved_at) VALUES ($1, $2, datetime('now','-2 hours'))`,
		staleHash[:], uint64(7777))
	require.NoError(t, err)

	// The dedicated sweeper (started independent of the toggle) must reclaim it.
	require.Eventually(t, func() bool {
		_, ok, e := s.durableReservationID(ctx, &staleHash)
		return e == nil && !ok
	}, 5*time.Second, 50*time.Millisecond, "stale reservation must be swept even with in-memory chain check disabled")
}

// TestReserveDurableBlockID_NextValError covers reserveDurableBlockID's allocation
// error path: if GetNextBlockID fails it must surface the error, not persist or
// return a bogus reservation. Calling it on a closed store makes the nextval fail.
func TestReserveDurableBlockID_NextValError(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	require.NoError(t, s.Close()) // closed DB → GetNextBlockID fails

	h := chainhash.HashH([]byte("block-nextval-error"))
	_, err = s.reserveDurableBlockID(context.Background(), &h)
	require.Error(t, err, "reserveDurableBlockID must surface a nextval allocation failure")
}

// TestAssignBlockID_DurableLookupError covers the L2 (durable-table) lookup error
// path: if the SELECT against block_id_reservations fails for a reason other than
// "no row", AssignBlockID must surface that storage error rather than silently
// minting a fresh id (which could diverge from a reservation it failed to read).
// Dropping the table makes the SELECT fail with a real DB error.
func TestAssignBlockID_DurableLookupError(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// Drop the durable table so the L2 lookup (durableReservationID) errors. The
	// blocks table is untouched, so the committed-id checks still succeed (no row),
	// and the flow reaches the durable lookup.
	_, err = s.db.ExecContext(ctx, `DROP TABLE block_id_reservations`)
	require.NoError(t, err)

	h := chainhash.HashH([]byte("block-l2-error"))
	_, err = s.AssignBlockID(ctx, &h)
	require.Error(t, err, "a durable-lookup storage error must propagate, not be swallowed")
	require.NotContains(t, err.Error(), "no row")
}

// TestAssignBlockID_SurvivesTTLExpiry is the #1056 core regression: a reservation
// must survive the in-memory ttlcache TTL window. If block processing outlasts
// blockIDReservationTTL (a multi-GB block on slow hardware during IBD), the cache
// entry is evicted; a second caller for the same still-uncommitted hash must get
// the SAME id from the durable reservation table, not burn a fresh nextval — which
// would re-create the phantom-id divergence #1043 closed (UTXO mined-info under
// id1, committed block row under id2).
//
// This is the same code path that protects a process RESTART and a SECOND
// blockchain instance (replicas>1): all three reach AssignBlockID with the hash
// uncommitted and absent from the in-memory L1 cache, and must recover the id from
// the durable L2 table rather than allocate a new one.
func TestAssignBlockID_SurvivesTTLExpiry(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	h := chainhash.HashH([]byte("block-ttl-expiry"))

	id1, err := s.AssignBlockID(ctx, &h)
	require.NoError(t, err)
	require.NotZero(t, id1)

	// Simulate the TTL firing mid-processing: evict the in-memory reservation while
	// the block is still uncommitted.
	s.blockIDReservations.Delete(h)
	require.Nil(t, s.blockIDReservations.Get(h), "in-memory reservation evicted")

	id2, err := s.AssignBlockID(ctx, &h)
	require.NoError(t, err)
	require.Equal(t, id1, id2, "reservation must survive ttlcache eviction via the durable table")
}

// TestAssignBlockID_DurableReservationClearedOnCommit verifies the best-effort
// DELETE on StoreBlock commit removes the durable reservation row, so the table
// does not grow unboundedly once a block is committed.
func TestAssignBlockID_DurableReservationClearedOnCommit(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	reserved, err := s.AssignBlockID(ctx, block1.Hash())
	require.NoError(t, err)
	require.NotZero(t, reserved)

	_, ok, err := s.durableReservationID(ctx, block1.Hash())
	require.NoError(t, err)
	require.True(t, ok, "durable reservation row should exist before commit")

	storedID, _, err := s.StoreBlock(ctx, block1, "test", options.WithID(reserved))
	require.NoError(t, err)
	require.Equal(t, reserved, storedID)

	_, ok, err = s.durableReservationID(ctx, block1.Hash())
	require.NoError(t, err)
	require.False(t, ok, "durable reservation row must be cleared on commit")
}

// TestSweepStaleReservations verifies the age-based sweep reclaims durable
// reservations for blocks that were fetched but never committed (bounding table
// growth), while leaving in-flight reservations untouched.
func TestSweepStaleReservations(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// A stale reservation: inserted directly with a reserved_at well past the sweep
	// age (> staleReservationSweepAge).
	staleHash := chainhash.HashH([]byte("block-stale"))
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO block_id_reservations (hash, block_id, reserved_at) VALUES ($1, $2, datetime('now','-2 hours'))`,
		staleHash[:], uint64(99999))
	require.NoError(t, err)

	// A fresh reservation via the normal path (reserved_at defaults to now).
	freshHash := chainhash.HashH([]byte("block-fresh"))
	freshID, err := s.AssignBlockID(ctx, &freshHash)
	require.NoError(t, err)

	s.sweepStaleReservations(ctx)

	_, ok, err := s.durableReservationID(ctx, &staleHash)
	require.NoError(t, err)
	require.False(t, ok, "stale reservation (older than sweep age) must be reclaimed")

	id, ok, err := s.durableReservationID(ctx, &freshHash)
	require.NoError(t, err)
	require.True(t, ok, "fresh reservation must survive the sweep")
	require.Equal(t, freshID, id)
}

// TestAssignBlockID_DBError covers the storage-error path: when the underlying
// DB is unavailable the committed-id lookup fails, and AssignBlockID must
// surface that error rather than silently minting a fresh id (which could
// re-introduce the divergence this method exists to prevent).
func TestAssignBlockID_DBError(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	// Close the store (and its DB) so the committed-id lookup SELECT errors.
	require.NoError(t, s.Close())

	h := chainhash.HashH([]byte("block-db-error"))
	_, err = s.AssignBlockID(context.Background(), &h)
	require.Error(t, err, "AssignBlockID must surface a storage error when the DB is closed, not mint an id")
}
