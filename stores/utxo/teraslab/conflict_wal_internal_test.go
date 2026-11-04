package teraslab

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestCreateConflictWALTableAddsRecoveryColumn verifies the recovery-column
// migration. A conflict_intents table created by a pre-recovery build of this
// branch (the old conflictwal.CreateTable had no recovery column) must gain the
// column on startup — CREATE TABLE IF NOT EXISTS alone skips an existing table,
// so BeginConflictIntent's insert into recovery would fail and abort every
// ProcessConflicting after an upgrade.
func TestCreateConflictWALTableAddsRecoveryColumn(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	walURL, err := url.Parse("sqlitememory:///teraslab_wal_migration_test")
	require.NoError(t, err)

	db, err := util.InitSQLDB(logger, walURL, tSettings, tSettings.UtxoStore.PostgresPool)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// A table created by the old conflictwal.CreateTable: no recovery column.
	_, err = db.Exec(`CREATE TABLE conflict_intents (
		 intent_id    BLOB PRIMARY KEY
		,kind         TEXT NOT NULL
		,block_height BIGINT NOT NULL
		,block_hash   BLOB NOT NULL
		,tx_hashes    BLOB NOT NULL
		,started_at   BIGINT NOT NULL
	)`)
	require.NoError(t, err)

	require.NoError(t, createConflictWALTable(db, walURL.Scheme))

	var recoveryColumns int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM pragma_table_info('conflict_intents') WHERE name = 'recovery'`).Scan(&recoveryColumns))
	require.Equal(t, 1, recoveryColumns, "createConflictWALTable must add the recovery column to a pre-existing table")
}
