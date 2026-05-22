package usql

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// newPostgresTestDB spins up a real postgres container and returns a *DB plus
// a cleanup func. Kept local to this file so advisory-lock behaviour is tested
// against the same server mechanics the code targets in production.
func newPostgresTestDB(t *testing.T) *DB {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pgContainer, err := postgres.Run(ctx,
		"postgres:13",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Minute),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgContainer.Terminate(context.Background()) })

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// pg driver wants a postgres://... DSN; ConnectionString already returns one.
	_, err = url.Parse(connStr)
	require.NoError(t, err)

	db, err := Open("postgres", connStr)
	require.NoError(t, err)
	// Small pool so we can deterministically exercise conn pinning.
	db.DB.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// advisoryLockHolderCount returns how many postgres backends currently hold the
// given single-key advisory lock id. Uses pg_locks (system catalog, AccessShare)
// so the probe cannot block on user-table activity.
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
    `, lockID).Scan(&count)
	require.NoError(t, err)
	return count
}

const testLockID int64 = 0xabc_def

func TestWithAdvisoryLock_AcquireReleaseRoundTrip(t *testing.T) {
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
