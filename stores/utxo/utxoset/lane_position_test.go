package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// TestLanePositionContractUtxoset runs the shared lane position contract. The multi contract's
// "below the checkpoint" case and the mined create need a store whose checkpoint is below the test
// heights to take the block-path route, and its "mined above the checkpoint" case needs one with
// none, so the contract runs on both kinds of store.
func TestLanePositionContractUtxoset(t *testing.T) {
	t.Run("no checkpoints", func(t *testing.T) {
		s, _ := newUncheckpointedStore(t)
		tests.LanePositionContract(t, s)
	})

	t.Run("checkpointed", func(t *testing.T) {
		s, _ := newTestStore(t)
		tests.LanePositionMinedCreate(t, s)
		tests.LanePositionMulti(t, s)
		tests.LanePositionAbsentWithoutOption(t, s)
	})
}

// storedLane reads the raw lane_position column of the named table for one transaction. ok is
// false when the table holds no row for it, and null is true when the column is NULL.
func storedLane(t *testing.T, s *Store, ctx context.Context, table string, tx *bt.Tx) (value int64, null, ok bool) {
	t.Helper()

	var v *int64

	rows, err := s.pool.Query(ctx, `SELECT lane_position FROM `+table+` WHERE txid = $1`, hashBytes(tx))
	require.NoError(t, err)

	defer rows.Close()

	if !rows.Next() {
		require.NoError(t, rows.Err())

		return 0, false, false
	}

	require.NoError(t, rows.Scan(&v))

	if v == nil {
		return 0, true, true
	}

	return *v, false, true
}

func requireGetLane(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx, want uint64, msg string) {
	t.Helper()

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err, msg)
	require.Equal(t, want, got.LanePosition, msg)
}

// TestLanePositionSurvivesEveryCopyOfTheRow follows one transaction's lane position down the whole
// life of its row: written to the identity row by the create, copied to its containment row when
// a block is recorded, still there after the stamp deletes the identity row, and copied again to
// preserved_parent, which is what answers once the containment partition is gone.
func TestLanePositionSurvivesEveryCopyOfTheRow(t *testing.T) {
	s, ctx := newTestStore(t)

	// Lane 200 sets the top bit, so the column holds a negative number.
	want := lane.Pack(200, 4_242)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithLanePosition(want))
	require.NoError(t, err)

	v, null, ok := storedLane(t, s, ctx, "tx_ident", parent)
	require.True(t, ok, "the unmined create writes an identity row")
	require.False(t, null)
	require.Equal(t, lane.ToSigned(want), v)
	require.Less(t, v, int64(0), "lane 200 is stored as a negative BIGINT by design")

	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	v, null, ok = storedLane(t, s, ctx, "tx_mined", parent)
	require.True(t, ok, "recording the block writes a containment row")
	require.False(t, null, "the containment row copies the identity row's lane position")
	require.Equal(t, lane.ToSigned(want), v)

	tip := stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})
	require.False(t, identExists(t, s, ctx, parent), "the stamp deletes the identity row")
	require.Less(t, tip, uint32(900))

	// The only output is spent after the stamp, so its undo copy carries the stamped pair, and at a
	// height past the stamp so that copy does not hold the partition drop back.
	spendOneOutputInBlock(t, s, ctx, parent, 0, 900, 9)
	requireGetLane(t, s, ctx, parent, want, "answered from tx_mined alone")

	require.NoError(t, s.PreserveTransactions(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, 5_000))

	v, null, ok = storedLane(t, s, ctx, "preserved_parent", parent)
	require.True(t, ok)
	require.False(t, null, "the preserved copy takes the containment row's lane position")
	require.Equal(t, lane.ToSigned(want), v)

	require.Equal(t, 1, dropStamped(t, s, ctx, tip), "the partition has to be gone for this to mean anything")

	_, _, ok = storedLane(t, s, ctx, "tx_mined", parent)
	require.False(t, ok, "tx_mined no longer holds the transaction")

	// The spend's journal row answers a read before the preserved copy does, and carries only the
	// block facts, so it goes too.
	_, err = s.dropSpendJournalPartitionsBelow(ctx, 2_000)
	require.NoError(t, err)

	requireGetLane(t, s, ctx, parent, want, "answered from preserved_parent alone")
}

// TestLanePositionAbsentIsNullInEveryTable pins that a create made without a lane position,
// which is every create with the feature off, writes NULL on the identity and containment rows
// and reads back as lane.None.
func TestLanePositionAbsentIsNullInEveryTable(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	_, null, ok := storedLane(t, s, ctx, "tx_ident", tx)
	require.True(t, ok)
	require.True(t, null)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	_, null, ok = storedLane(t, s, ctx, "tx_mined", tx)
	require.True(t, ok)
	require.True(t, null)

	requireGetLane(t, s, ctx, tx, lane.None, "no lane position was written")

	// Not named, not returned: a read that does not ask leaves the field zero even when the row
	// holds one.
	withLane := mkTx(t, 2, 6_000)
	_, err = s.Create(ctx, withLane, 100, utxo.WithLanePosition(lane.Pack(1, 5)))
	require.NoError(t, err)

	got, err := s.Get(ctx, withLane.TxIDChainHash(), fields.Fee)
	require.NoError(t, err)
	require.Equal(t, lane.None, got.LanePosition)
}

// TestLanePositionColumnsAreAddedToAnExistingSchema is the migration: a database made before the
// column existed gains it on the tables and partitions that already exist, with no value, and a
// second pass changes nothing.
func TestLanePositionColumnsAreAddedToAnExistingSchema(t *testing.T) {
	s, ctx := newTestStore(t)

	// Put one containment partition in place first, so there is an attached tx_mined partition
	// for the migration to reach as well as the parents.
	old := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, old, 100)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(old), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	// Make the schema the previous build made. Dropping the column on a partitioned parent drops
	// it on every partition.
	for _, table := range []string{"tx_ident", "tx_mined", "preserved_parent"} {
		_, err = s.pool.Exec(ctx, `ALTER TABLE `+table+` DROP COLUMN lane_position`)
		require.NoError(t, err)
	}

	for _, table := range []string{"tx_ident", "tx_ident_l0", "tx_mined", "preserved_parent"} {
		require.False(t, hasLaneColumn(t, s, ctx, table), "%s is the old shape before the migration", table)
	}

	require.NoError(t, CreateSchema(ctx, s.pool))

	var partitions []string

	rows, err := s.pool.Query(ctx, `
		SELECT c.relname FROM pg_class c
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = current_schema() AND c.relispartition
		   AND c.relname ~ '^(tx_ident_l|tx_mined_w)[0-9]+$'`)
	require.NoError(t, err)

	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))

		partitions = append(partitions, name)
	}

	rows.Close()
	require.NoError(t, rows.Err())
	require.NotEmpty(t, partitions)

	for _, table := range append([]string{"tx_ident", "tx_mined", "preserved_parent"}, partitions...) {
		require.True(t, hasLaneColumn(t, s, ctx, table), "%s gains lane_position", table)
	}

	// No backfill: the transaction written before the column existed reads back with none, and a
	// create after the migration keeps its value.
	requireGetLane(t, s, ctx, old, lane.None, "a row from before the column")

	fresh := mkTx(t, 1, 7_000)
	want := lane.Pack(2, 77)
	_, err = s.Create(ctx, fresh, 101, utxo.WithLanePosition(want))
	require.NoError(t, err)
	requireGetLane(t, s, ctx, fresh, want, "a create after the migration")

	// Running the schema again is a no-op.
	require.NoError(t, CreateSchema(ctx, s.pool))
	requireGetLane(t, s, ctx, fresh, want, "after a second schema pass")
}

func hasLaneColumn(t *testing.T, s *Store, ctx context.Context, table string) bool {
	t.Helper()

	var found bool

	require.NoError(t, s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM information_schema.columns
		                WHERE table_schema = current_schema() AND table_name = $1
		                  AND column_name = 'lane_position' AND data_type = 'bigint'
		                  AND is_nullable = 'YES' AND column_default IS NULL)`, table).Scan(&found))

	return found
}
