package usql

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/util/test"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newPostgresTestDB opens a fresh database on the PostgreSQL server shared by this test
// binary (test/utils/postgres) and returns a *DB on it. Advisory locks are scoped to a
// database, so tests on the shared server cannot see each other's locks. The database is
// dropped when the test ends, after the *DB closes.
func newPostgresTestDB(t *testing.T) *DB {
	t.Helper()

	connStr, teardown, err := tpostgres.SetupTestPostgresContainer()
	test.SkipIfContainerUnavailable(t, err)
	t.Cleanup(func() { assert.NoError(t, teardown()) })

	db, err := Open("postgres", connStr)
	require.NoError(t, err)
	// Small pool so we can deterministically exercise conn pinning.
	db.DB.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// advisoryLockHolderCount returns how many postgres backends currently hold the
// given single-key advisory lock id in the test's own database. Uses pg_locks
// (system catalog, AccessShare) so the probe cannot block on user-table activity.
// pg_locks covers the whole server, and tests on other databases of the shared
// server take the same lock id, so the probe filters on the database.
//
// Encoding reference: pg_advisory_lock(int8 key) stores
//
//	classid  = (key >> 32) & 0xFFFFFFFF
//	objid    =  key        & 0xFFFFFFFF
//	objsubid = 1
//
// (The two-key form pg_advisory_lock(int4, int4) uses objsubid = 2.)
func advisoryLockHolderCount(t *testing.T, db *DB, lockID int64) int {
	t.Helper()
	var count int
	err := db.QueryRow(`
        SELECT count(*)
        FROM pg_locks
        WHERE locktype = 'advisory'
          AND objsubid = 1
          AND ((classid::bigint << 32) | (objid::bigint & x'FFFFFFFF'::bigint)) = $1
          AND granted = true
          AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
    `, lockID).Scan(&count)
	require.NoError(t, err)
	return count
}

const testLockID int64 = 0xabc_def

func TestWithAdvisoryLock_AcquireReleaseRoundTrip(t *testing.T) {
	t.Parallel() // a database of its own on the shared server

	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}
	db := newPostgresTestDB(t)
	ctx := context.Background()

	// Sanity: nobody holds the lock.
	require.Equal(t, 0, advisoryLockHolderCount(t, db, testLockID))

	var insideFn int
	err := WithAdvisoryLock(ctx, db, testLockID, func() error {
		// Inside fn, exactly one backend must hold the lock.
		insideFn = advisoryLockHolderCount(t, db, testLockID)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, insideFn, "lock should be held while fn executes")

	// After return, no backend should hold the lock — conn-pinning + defer
	// unlock must release it on the same session that acquired it.
	require.Eventually(t, func() bool {
		return advisoryLockHolderCount(t, db, testLockID) == 0
	}, 2*time.Second, 50*time.Millisecond, "lock must be released after fn returns")
}

func TestWithAdvisoryLock_Serializes(t *testing.T) {
	t.Parallel() // a database of its own on the shared server

	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}
	db := newPostgresTestDB(t)
	ctx := context.Background()

	const goroutines = 5
	var (
		wg       sync.WaitGroup
		inside   int32 // how many goroutines are currently inside fn
		maxSeen  int32
		observed = make([]int32, goroutines)
	)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			err := WithAdvisoryLock(ctx, db, testLockID, func() error {
				n := atomic.AddInt32(&inside, 1)
				observed[i] = n
				for {
					cur := atomic.LoadInt32(&maxSeen)
					if n <= cur || atomic.CompareAndSwapInt32(&maxSeen, cur, n) {
						break
					}
				}
				time.Sleep(50 * time.Millisecond)
				atomic.AddInt32(&inside, -1)
				return nil
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	require.EqualValues(t, 1, maxSeen, "advisory lock must permit at most one fn at a time (observed=%v)", observed)
	// Final state — lock released.
	require.Eventually(t, func() bool {
		return advisoryLockHolderCount(t, db, testLockID) == 0
	}, 2*time.Second, 50*time.Millisecond)
}

func TestWithAdvisoryLock_ReleasesOnFnError(t *testing.T) {
	t.Parallel() // a database of its own on the shared server

	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}
	db := newPostgresTestDB(t)
	ctx := context.Background()

	wantErr := errorString("boom")
	err := WithAdvisoryLock(ctx, db, testLockID, func() error { return wantErr })
	require.ErrorIs(t, err, wantErr)

	require.Eventually(t, func() bool {
		return advisoryLockHolderCount(t, db, testLockID) == 0
	}, 2*time.Second, 50*time.Millisecond, "lock must be released even when fn returns an error")
}

func TestWithAdvisoryLock_ReleasesOnFnPanic(t *testing.T) {
	t.Parallel() // a database of its own on the shared server

	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}
	db := newPostgresTestDB(t)
	ctx := context.Background()

	func() {
		defer func() {
			r := recover()
			require.NotNil(t, r, "fn must have panicked")
			require.True(t, strings.Contains(toString(r), "panic-in-fn"))
		}()
		_ = WithAdvisoryLock(ctx, db, testLockID, func() error {
			panic("panic-in-fn")
		})
	}()

	require.Eventually(t, func() bool {
		return advisoryLockHolderCount(t, db, testLockID) == 0
	}, 2*time.Second, 50*time.Millisecond, "lock must be released via defer when fn panics")
}

// errorString is a package-local error type to avoid pulling errors.New into
// tests just for identity comparisons.
type errorString string

func (e errorString) Error() string { return string(e) }

func toString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case error:
		return x.Error()
	default:
		return ""
	}
}
