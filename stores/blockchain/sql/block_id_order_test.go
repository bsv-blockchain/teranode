package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/stretchr/testify/require"
)

// TestStoreBlock_RefusesIDNotAboveParent builds the shape the offline rewind tool
// can leave behind: a child holds a reservation older than its parent's id. The
// child is reserved first (id N), the parent second (id N+1), and the parent is
// stored under its own reservation. Storing the child under N would put it below
// its parent, and the ancestry queries bound their parent_id walk by the gap between
// two ids, so they would answer "not an ancestor" for a true one.
//
// Both ids are the hash's own reservation, so checkCallerSuppliedBlockID passes
// them and the refusal can only come from the id-order check.
func TestStoreBlock_RefusesIDNotAboveParent(t *testing.T) {
	run := func(t *testing.T, s *SQL) {
		ctx := context.Background()

		childReserved, err := s.AssignBlockID(ctx, block2.Hash())
		require.NoError(t, err)

		parentReserved, err := s.AssignBlockID(ctx, block1.Hash())
		require.NoError(t, err)
		require.Greater(t, parentReserved, childReserved, "ids come from one sequence that only grows")

		parentID, _, err := s.StoreBlock(ctx, block1, "", options.WithID(parentReserved))
		require.NoError(t, err)
		require.Equal(t, parentReserved, parentID)

		_, _, err = s.StoreBlock(ctx, block2, "", options.WithID(childReserved))
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockIDOrder), "want the id-order refusal, got %v", err)

		_, stored, err := s.blockIDByHash(ctx, block2.Hash())
		require.NoError(t, err)
		require.False(t, stored, "a refused child must leave no blocks row")

		// storeInvalidBlock in blockvalidation stores a failed block with its
		// reserved id. The ancestry walks follow parent_id whatever the invalid
		// flag says, so the check must refuse an invalid child too.
		_, _, err = s.StoreBlock(ctx, block2, "", options.WithID(childReserved), options.WithInvalid(true))
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockIDOrder), "want the id-order refusal for an invalid child, got %v", err)

		_, stored, err = s.blockIDByHash(ctx, block2.Hash())
		require.NoError(t, err)
		require.False(t, stored, "a refused invalid child must leave no blocks row")
	}

	t.Run("sqlite", func(t *testing.T) {
		run(t, newOnMainChainTestStore(t))
	})

	t.Run("postgres", func(t *testing.T) {
		if testing.Short() {
			t.Skip("postgres container case")
		}

		s, cleanup := setupPostgresTestStore(t)
		t.Cleanup(cleanup)
		t.Cleanup(func() { _ = s.Close(context.Background()) })

		run(t, s)
	})
}

// TestStoreBlock_RefusesAutoIDNotAboveParent_Postgres reaches the check on an id the
// INSERT allocates. The sequence only grows and every caller-supplied id came from
// it, so an allocated id below the parent needs the sequence moved back by hand;
// setval does that here. SQLite cannot reach this branch: AUTOINCREMENT allocates
// above the largest rowid in the table whatever sqlite_sequence says.
//
// The child extends the best tip, so it takes the extend path, which is a single
// auto-committed INSERT. The row is therefore already committed when the check
// fails. The test records that so a later change that wraps the extend path in a
// transaction has to flip the assertion on purpose.
func TestStoreBlock_RefusesAutoIDNotAboveParent_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("postgres container case")
	}

	ctx := context.Background()

	s, cleanup := setupPostgresTestStore(t)
	t.Cleanup(cleanup)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	childReserved, err := s.AssignBlockID(ctx, block2.Hash())
	require.NoError(t, err)

	parentReserved, err := s.AssignBlockID(ctx, block1.Hash())
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(parentReserved))
	require.NoError(t, err)

	// Drop the child's reservation and move the sequence back so the next
	// allocated id is childReserved, below the parent.
	_, err = s.db.ExecContext(ctx, `DELETE FROM block_id_reservations WHERE hash = $1`, block2.Hash()[:])
	require.NoError(t, err)

	_, err = s.db.ExecContext(ctx, `SELECT setval(pg_get_serial_sequence('blocks', 'id'), $1, false)`, childReserved)
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockIDOrder), "want the id-order refusal, got %v", err)

	id, stored, err := s.blockIDByHash(ctx, block2.Hash())
	require.NoError(t, err)
	require.True(t, stored, "the extend path auto-commits its INSERT, so the row is already written when the check fails")
	require.Equal(t, childReserved, id)
}

// TestStoreBlock_ChildAboveParentIsStored guards the common case: a child reserved
// after its parent is stored, whether its id is supplied or allocated.
func TestStoreBlock_ChildAboveParentIsStored(t *testing.T) {
	ctx := context.Background()

	s := newOnMainChainTestStore(t)

	parentReserved, err := s.AssignBlockID(ctx, block1.Hash())
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(parentReserved))
	require.NoError(t, err)

	childReserved, err := s.AssignBlockID(ctx, block2.Hash())
	require.NoError(t, err)

	childID, _, err := s.StoreBlock(ctx, block2, "", options.WithID(childReserved))
	require.NoError(t, err)
	require.Greater(t, childID, parentReserved)

	thirdID, _, err := s.StoreBlock(ctx, block3, "")
	require.NoError(t, err)
	require.Greater(t, thirdID, childID)
}
