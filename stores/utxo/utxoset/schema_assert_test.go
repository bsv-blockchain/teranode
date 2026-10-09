package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestSchemaAssertionRefusesADatabaseFromThePreviousShape is the failure an operator will
// actually hit: they redeploy over an existing volume.
//
// Fresh-sync-only is by design and there is no migration. But CreateSchema is CREATE TABLE IF
// NOT EXISTS throughout, so against a database written by the previous schema it is a complete
// no-op on utxo -- the table exists, without mined_height and block_id -- creates the new
// tables beside it, and returns success. New then returns a healthy store, and the first
// statement to run fails with `column "mined_height" of relation "utxo" does not exist`, per
// batch, forever, drowning the log. The columns the reshape removed are all nullable or
// defaulted, so nothing fails earlier or louder.
//
// One assertion after the DDL converts that flood into a single startup refusal that names
// what is missing.
func TestSchemaAssertionRefusesADatabaseFromThePreviousShape(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, assertSchemaShape(ctx, s.pool), "a schema this binary just created passes")

	_, err := s.pool.Exec(ctx, `ALTER TABLE utxo DROP COLUMN mined_height`)
	require.NoError(t, err)

	err = assertSchemaShape(ctx, s.pool)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrConfiguration))
	require.Contains(t, err.Error(), "utxo.mined_height")
	require.Contains(t, err.Error(), "fresh database")

	// And the assertion is part of CreateSchema rather than beside it, so no caller can
	// install the schema and skip the check. Before this, the whole of CreateSchema was a
	// no-op against this database and it returned success.
	require.Error(t, CreateSchema(ctx, s.pool),
		"CreateSchema refuses the database it cannot repair")
}

// TestSchemaAssertionNamesAMissingTable: the previous schema had no tx_mined at all, so a
// database old enough to be missing the table has to be refused by name rather than by column.
func TestSchemaAssertionNamesAMissingTable(t *testing.T) {
	s, ctx := newTestStore(t)

	_, err := s.pool.Exec(ctx, `DROP TABLE tx_mined CASCADE`)
	require.NoError(t, err)

	err = assertSchemaShape(ctx, s.pool)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrConfiguration))
	require.Contains(t, err.Error(), "tx_mined")
}

// TestSchemaAssertionNamesTheJournalColumns: the journal's block facts are the fifth read
// step's only source, and a journal without them answers not-found for every fully-spent old
// parent rather than failing loudly. That is the quietest of the three, so it is asserted too.
func TestSchemaAssertionNamesTheJournalColumns(t *testing.T) {
	s, ctx := newTestStore(t)

	_, err := s.pool.Exec(ctx, `ALTER TABLE spend_journal DROP COLUMN mined_height`)
	require.NoError(t, err)

	err = assertSchemaShape(ctx, s.pool)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrConfiguration))
	require.Contains(t, err.Error(), "spend_journal.mined_height")
}

// TestSchemaAssertionRefusesADatabaseThatStillHasTheDeletedColumns is the gate for the
// containment change, whose distinguishing edits are DELETIONS. A database written by the
// schema before it has every column this binary requires, so the required-columns loop passes
// it, and the first by-txid read then fails on a column that is gone. The inverse list catches
// it at startup instead, with a message that says the column is still present rather than
// missing, because "tx_mined.seq is missing" about a column that is there would send an
// operator the wrong way.
func TestSchemaAssertionRefusesADatabaseThatStillHasTheDeletedColumns(t *testing.T) {
	s, ctx := newTestStore(t)

	for _, tc := range []struct {
		name, table, column, typ string
	}{
		{"the insertion counter on tx_mined", "tx_mined", "seq", "BIGINT"},
		{"the packed block list on tx_ident", "tx_ident", "membership", "BYTEA"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.pool.Exec(ctx, `ALTER TABLE `+tc.table+` ADD COLUMN `+tc.column+` `+tc.typ)
			require.NoError(t, err)

			err = assertSchemaShape(ctx, s.pool)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrConfiguration))
			require.Contains(t, err.Error(), tc.table+"."+tc.column+" is still present")
			require.Contains(t, err.Error(), "fresh database")

			require.Error(t, CreateSchema(ctx, s.pool), "CreateSchema refuses the database it cannot repair")

			_, err = s.pool.Exec(ctx, `ALTER TABLE `+tc.table+` DROP COLUMN `+tc.column)
			require.NoError(t, err)
		})
	}
}

// TestSchemaAssertionNamesTheNewFloorColumnsAndTheStampedTable: the containment change also
// adds two floor columns and one table, unused until the deep stamp of build step 5 exists, so
// the gate's required list changes once rather than twice.
func TestSchemaAssertionNamesTheNewFloorColumnsAndTheStampedTable(t *testing.T) {
	s, ctx := newTestStore(t)

	_, err := s.pool.Exec(ctx, `ALTER TABLE tx_mined_floor DROP CONSTRAINT tx_mined_floor_order,
	                             DROP COLUMN stamp_fence`)
	require.NoError(t, err)

	err = assertSchemaShape(ctx, s.pool)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrConfiguration))
	require.Contains(t, err.Error(), "tx_mined_floor.stamp_fence")

	_, err = s.pool.Exec(ctx, `ALTER TABLE tx_mined_floor ADD COLUMN stamp_fence INTEGER NOT NULL DEFAULT 0`)
	require.NoError(t, err)

	_, err = s.pool.Exec(ctx, `DROP TABLE tx_mined_stamped`)
	require.NoError(t, err)

	err = assertSchemaShape(ctx, s.pool)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tx_mined_stamped")
}
