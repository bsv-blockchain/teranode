package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/require"
)

// bestBlockIDQueryText is enough of getBestBlockID's statement to pick it out and no other.
// InvalidateBlock's SELECT starts with the same two columns but continues
// "b.id, b.hash, b.previous_hash", so the trailing newline is what separates them.
const bestBlockIDQueryText = "SELECT b.id, b.hash\n"

// queryFault makes a chosen window of occurrences of one SQL statement fail, so a test can
// break a single query inside a call while the rest of that call runs for real.
//
// It is a database/sql driver wrapper rather than a seam on getBestBlockID. StoreBlock's
// post-insert getBestBlockID has no injection point, and a function field on *SQL whose
// only purpose is to be overwritten by a test would put test-only machinery on the
// consensus-critical write path. Wrapping the driver leaves production code untouched.
type queryFault struct {
	mu    sync.Mutex
	match string
	skip  int
	limit int
	seen  int
	fired int
	armed bool
}

// arm makes the next `limit` occurrences of a statement containing `match` fail, after
// letting `skip` of them through first.
func (f *queryFault) arm(match string, skip, limit int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.match, f.skip, f.limit = match, skip, limit
	f.seen, f.fired, f.armed = 0, 0, true
}

func (f *queryFault) disarm() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.armed = false
}

func (f *queryFault) fireCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.fired
}

func (f *queryFault) fires(query string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.armed || !strings.Contains(query, f.match) {
		return false
	}

	f.seen++

	if f.seen <= f.skip || f.seen > f.skip+f.limit {
		return false
	}

	f.fired++

	return true
}

// faultingDriver delegates to the real sqlite driver and hands out connections that consult
// a queryFault before preparing a statement.
type faultingDriver struct {
	inner driver.Driver
	fault *queryFault
}

func (d *faultingDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}

	return &faultingConn{Conn: conn, fault: d.fault}, nil
}

// faultingConn deliberately does not forward Queryer/Execer. database/sql prefers those
// when a connection offers them and would then never reach Prepare, so withholding them is
// what makes the interception below total.
type faultingConn struct {
	driver.Conn

	fault *queryFault
}

func (c *faultingConn) Prepare(query string) (driver.Stmt, error) {
	if c.fault.fires(query) {
		return nil, errors.NewStorageError("injected fault preparing query")
	}

	return c.Conn.Prepare(query)
}

func (c *faultingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.fault.fires(query) {
		return nil, errors.NewStorageError("injected fault preparing query")
	}

	if pc, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return pc.PrepareContext(ctx, query)
	}

	return c.Conn.Prepare(query)
}

func (c *faultingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if bt, ok := c.Conn.(driver.ConnBeginTx); ok {
		return bt.BeginTx(ctx, opts)
	}

	return c.Conn.Begin()
}

// faultingDriverSeq keeps each registered driver name unique; database/sql panics on a
// duplicate registration and offers no way to unregister.
var faultingDriverSeq atomic.Int64

// newFaultingStore builds a real file-backed sqlite store with the forked-set route on,
// then re-points it at the same database file through the faulting driver.
//
// It uses a file rather than the package's usual sqlitememory because a second handle has
// to reach the same database, and an sqlitememory store is named with a random
// shared-cache name the test cannot recover. The store is otherwise the real one: no
// mocks, and every query still runs against sqlite.
func newFaultingStore(t *testing.T) (*SQL, *queryFault) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.UseInMemoryChainCheck = true
	tSettings.BlockChain.ChainCheckShadowCompare = false

	storeURL, err := url.Parse("sqlite:///faultinjection")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	waitForStartupRebuild(t, s)

	// Recover the real sqlite driver without importing it, so the wrapper stays a wrapper.
	probe, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	inner := probe.Driver()
	require.NoError(t, probe.Close())

	fault := &queryFault{}
	driverName := fmt.Sprintf("faulting-sqlite-%d", faultingDriverSeq.Add(1))

	sql.Register(driverName, &faultingDriver{inner: inner, fault: fault})

	// The DSN util.InitSQLiteDB builds for a file-backed sqlite store. cache=shared is what
	// makes this second handle see the same database as the store's own.
	dbPath, err := filepath.Abs(filepath.Join(tSettings.DataFolder, "faultinjection.db"))
	require.NoError(t, err)

	faulting, err := sql.Open(driverName, dbPath+"?cache=shared&_pragma=busy_timeout=5000&_pragma=journal_mode=WAL&_pragma=foreign_keys=on")
	require.NoError(t, err)

	faulting.SetMaxOpenConns(5)

	// Prove the second handle reached the store's database and not an empty one, so a DSN
	// that drifts from InitSQLiteDB fails here rather than silently testing nothing.
	var blocks int

	require.NoError(t, faulting.QueryRow(`SELECT COUNT(*) FROM blocks`).Scan(&blocks))
	require.Positive(t, blocks, "the faulting handle opened a different database from the store's")

	original := s.db
	s.db = usql.WrapDB(faulting)

	t.Cleanup(func() {
		s.db = original

		_ = faulting.Close()
		_ = s.Close(context.Background())
	})

	return s, fault
}

// TestStoreBlockWhoseBestBlockLookupFailsLeavesTheChainCheckAnswerable covers StoreBlock's
// bestErr branch: when the post-insert getBestBlockID errors, that branch used to log and
// fall off the end of the if/else chain, so none of Cases 1-3 ran for a block that is
// already committed.
//
// Both subtests fail the pre-insert and post-insert getBestBlockID calls and let the
// rebuild's own call through. Failing the pre-insert call is what makes the post-insert
// one reachable at all: with preBestHash available the block is a common extend, the fast
// path wins the maxBlockID CAS and returns before the slow path ever runs.
func TestStoreBlockWhoseBestBlockLookupFailsLeavesTheChainCheckAnswerable(t *testing.T) {
	// The committed block is on the main chain. Without updateMaxBlockID the bound stays
	// below its id, CheckBlockIsInCurrentChain drops it as allocated-but-uncommitted, and
	// checkOldBlockIDs escalates that false into a permanent block invalidation.
	t.Run("an on-chain block is not dropped as above the bound", func(t *testing.T) {
		ctx := context.Background()
		s, fault := newFaultingStore(t)

		storeBlocks(t, s, block1, block2)
		require.NoError(t, s.rebuildOffChainSet(ctx), "precondition: the forked set is built and trusted")

		before := s.maxBlockID.Load()

		// The fast path primes the getBestBlockID cache with its own identity, so without
		// this the pre-insert lookup is a cache hit, never reaches the driver, and the
		// occurrence counting below would be off by one.
		s.ResetResponseCache()
		fault.arm(bestBlockIDQueryText, 0, 2)

		id, _, err := s.StoreBlock(ctx, block3, "peer")

		fault.disarm()
		require.NoError(t, err, "the INSERT itself must still commit")
		require.Equal(t, 2, fault.fireCount(), "both the pre-insert and post-insert lookups must have failed")
		require.Greater(t, uint64(id), before, "precondition: block3 committed above the old bound")

		onChain, err := s.CheckBlockIsInCurrentChain(ctx, []uint32{uint32(id)})
		require.NoError(t, err)
		require.True(t, onChain, "a committed block on the main chain was reported off-chain")

		require.GreaterOrEqual(t, s.maxBlockID.Load(), uint64(id),
			"maxBlockID was left below a committed id, so the in-memory route drops it as uncommitted")
	})

	// The committed block is a fork block. Advancing maxBlockID on its own would be worse
	// than the false negative it fixes: the installed forked set was read before this
	// INSERT and cannot contain the new id, and absence from that set is read as positive
	// proof of main-chain membership. Only the after-write rebuild closes that, which is
	// why the branch needs more than the one line.
	t.Run("a fork block does not become a false positive", func(t *testing.T) {
		ctx := context.Background()
		s, fault := newFaultingStore(t)

		storeBlocks(t, s, block1, block2)
		require.NoError(t, s.rebuildOffChainSet(ctx), "precondition: the forked set is built and trusted")

		// See the note in the sibling subtest: the cache must be cold for the pre-insert
		// lookup to reach the driver.
		s.ResetResponseCache()
		fault.arm(bestBlockIDQueryText, 0, 2)

		forkID, _, err := s.StoreBlock(ctx, blockAlternative2, "peer")

		fault.disarm()
		require.NoError(t, err)
		require.Equal(t, 2, fault.fireCount(), "both the pre-insert and post-insert lookups must have failed")

		onChain, err := s.CheckBlockIsInCurrentChain(ctx, []uint32{uint32(forkID)})
		require.NoError(t, err)
		require.False(t, onChain, "a fork block committed under a failed best-block lookup was reported on-chain")
	})
}
