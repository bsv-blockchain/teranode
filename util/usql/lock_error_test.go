package usql

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// TestIsSQLiteLockError checks that IsSQLiteLockError recognises an EXTENDED
// SQLite result code. A real SQLITE_BUSY_SNAPSHOT (517) is SQLITE_BUSY (5) with
// a detail in the high bits; a bare `== SQLITE_BUSY` check (the bug in #1876)
// misses it. It also must see through a wrapped error.
func TestIsSQLiteLockError(t *testing.T) {
	err := busySnapshotError(t)

	var sqliteErr *sqlite.Error
	require.True(t, errors.As(err, &sqliteErr), "fixture should be a *sqlite.Error")
	require.Equal(t, sqlite3.SQLITE_BUSY_SNAPSHOT, sqliteErr.Code(), "driver should report the extended code 517")

	require.True(t, IsSQLiteLockError(err), "extended BUSY code (517) must be recognised as a lock error")
	require.True(t, IsSQLiteLockError(lockTestWrap{err}), "a wrapped lock error must still match (errors.As, not a type assertion)")
	require.False(t, IsSQLiteLockError(nil), "nil is not a lock error")
	require.False(t, IsSQLiteLockError(errors.NewProcessingError("some unrelated error")), "a non-SQLite error is not a lock error")
}

// lockTestWrap wraps an error behind an Unwrap boundary, as a caller that adds
// context with %w would, so the test exercises errors.As rather than a bare
// type assertion.
type lockTestWrap struct{ err error }

func (w lockTestWrap) Error() string { return "wrapped: " + w.err.Error() }
func (w lockTestWrap) Unwrap() error { return w.err }
