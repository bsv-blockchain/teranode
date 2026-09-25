package conflictwal_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/conflictwal"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// openSQLite opens a sqlite WAL DB at path and creates the conflict_intents
// table. Shared by the round-trip and reopen-durability assertions.
func openSQLite(t *testing.T, path string) *usql.DB {
	t.Helper()
	db, err := usql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	require.NoError(t, err)
	require.NoError(t, conflictwal.CreateTable(db, "sqlite"))
	return db
}

// TestConflictWAL exercises the shared conflict-WAL logic over SQLite, mirroring
// the cross-backend contract in stores/utxo/tests.ConflictWAL plus a reopen step
// that asserts on-disk durability the single-handle suite cannot.
func TestConflictWAL(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "conflict_wal.db")

	db := openSQLite(t, path)

	h1 := chainhash.HashH([]byte("wal-tx-1"))
	h2 := chainhash.HashH([]byte("wal-tx-2"))

	forward := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentForward,
		BlockHeight: 4242,
		BlockHash:   chainhash.HashH([]byte("block-fwd")),
		TxHashes:    []chainhash.Hash{h1, h2},
		StartedAt:   1_700_000_000_000_000_000,
	}

	// Clean WAL starts empty.
	pending, err := conflictwal.Pending(ctx, db)
	require.NoError(t, err)
	require.Empty(t, pending)

	// Begin → pending, fields round-trip.
	require.NoError(t, conflictwal.Begin(ctx, db, "sqlite", forward))

	pending, err = conflictwal.Pending(ctx, db)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, utxo.ConflictIntentForward, pending[0].Kind)
	require.Equal(t, uint32(4242), pending[0].BlockHeight)
	require.Equal(t, forward.BlockHash, pending[0].BlockHash)
	require.Equal(t, int64(1_700_000_000_000_000_000), pending[0].StartedAt)
	require.ElementsMatch(t, []chainhash.Hash{h1, h2}, pending[0].TxHashes)
	require.Equal(t, forward.IntentID(), pending[0].IntentID())

	// Idempotent on the deterministic id (hash order must not matter).
	reordered := forward
	reordered.TxHashes = []chainhash.Hash{h2, h1}
	require.NoError(t, conflictwal.Begin(ctx, db, "sqlite", reordered))

	pending, err = conflictwal.Pending(ctx, db)
	require.NoError(t, err)
	require.Len(t, pending, 1, "re-begin of the same intent must not duplicate")

	// A distinct reverse intent coexists.
	reverse := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentReverse,
		BlockHeight: 99,
		BlockHash:   chainhash.HashH([]byte("block-rev")),
		TxHashes:    []chainhash.Hash{chainhash.HashH([]byte("wal-tx-3"))},
		StartedAt:   1_700_000_000_000_000_001,
	}
	require.NoError(t, conflictwal.Begin(ctx, db, "sqlite", reverse))

	pending, err = conflictwal.Pending(ctx, db)
	require.NoError(t, err)
	require.Len(t, pending, 2)

	// Durability: a fresh handle on the same file must see both intents.
	require.NoError(t, db.Close())
	db2 := openSQLite(t, path)

	pending, err = conflictwal.Pending(ctx, db2)
	require.NoError(t, err)
	require.Len(t, pending, 2, "intents must survive a reopen (crash durability)")

	// Complete the forward intent → only reverse remains.
	require.NoError(t, conflictwal.Complete(ctx, db2, forward.IntentID()))

	pending, err = conflictwal.Pending(ctx, db2)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, reverse.IntentID(), pending[0].IntentID())

	// Completing an already-absent intent is a no-op.
	require.NoError(t, conflictwal.Complete(ctx, db2, forward.IntentID()))

	// Complete the reverse intent → clean again.
	require.NoError(t, conflictwal.Complete(ctx, db2, reverse.IntentID()))

	pending, err = conflictwal.Pending(ctx, db2)
	require.NoError(t, err)
	require.Empty(t, pending)

	require.NoError(t, db2.Close())
}
