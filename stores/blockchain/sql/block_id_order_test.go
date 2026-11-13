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

		// The refusal dropped the stale reservation, so the next AssignBlockID
		// takes a fresh id above the parent and the retry stores the child.
		freshID, err := s.AssignBlockID(ctx, block2.Hash())
		require.NoError(t, err)
		require.Greater(t, freshID, parentReserved, "the retry must get an id above the parent, not the refused one")

		childID, _, err := s.StoreBlock(ctx, block2, "", options.WithID(freshID))
		require.NoError(t, err)
		require.Equal(t, freshID, childID)
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

// TestStoreBlock_ChildAboveParentIsStored guards the common case: a child reserved
// after its parent is stored, whether its id is supplied or allocated. An allocated
// id is not checked, because it comes from the sequence after the parent's.
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
