package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func newReservedIDTestStore(t *testing.T) *SQL {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	return s
}

func requireNoBlockRow(t *testing.T, s *SQL, what string) {
	t.Helper()

	_, ok, err := s.blockIDByHash(context.Background(), block1.Hash())
	require.NoError(t, err)
	require.False(t, ok, "%s: a refused id must not leave a blocks row behind", what)
}

// A caller-supplied block id is what ties a block's transactions, already stamped
// in the UTXO store, to its blocks row. These cases pin which ids StoreBlock
// accepts: the id reserved for the hash, or (when the reservation has been swept)
// an id the sequence really issued that no other block holds. Anything else is
// refused and writes nothing.
func TestStoreBlock_CallerSuppliedIDMustMatchReservation(t *testing.T) {
	ctx := context.Background()

	t.Run("the reserved id stores", func(t *testing.T) {
		s := newReservedIDTestStore(t)

		reserved, err := s.AssignBlockID(ctx, block1.Hash())
		require.NoError(t, err)

		storedID, _, err := s.StoreBlock(ctx, block1, "", options.WithID(reserved))
		require.NoError(t, err)
		require.Equal(t, reserved, storedID)

		got, ok, err := s.blockIDByHash(ctx, block1.Hash())
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, reserved, got, "the blocks row must carry the reserved id")
	})

	t.Run("an id other than the block's own reservation is refused", func(t *testing.T) {
		s := newReservedIDTestStore(t)

		reserved, err := s.AssignBlockID(ctx, block1.Hash())
		require.NoError(t, err)

		// An id the sequence issued but nobody reserved, so the only thing wrong
		// with it is that block1 reserved a different one.
		burned, err := s.GetNextBlockID(ctx)
		require.NoError(t, err)
		require.NotEqual(t, reserved, burned)

		_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(burned))
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "got %v", err)
		requireNoBlockRow(t, s, "mismatched id")

		// The honest id still stores afterwards.
		storedID, _, err := s.StoreBlock(ctx, block1, "", options.WithID(reserved))
		require.NoError(t, err)
		require.Equal(t, reserved, storedID)
	})

	t.Run("an id reserved for another block is refused and that block keeps it", func(t *testing.T) {
		s := newReservedIDTestStore(t)

		_, err := s.AssignBlockID(ctx, block1.Hash())
		require.NoError(t, err)

		otherReserved, err := s.AssignBlockID(ctx, block2.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(otherReserved))
		require.Error(t, err)
		requireNoBlockRow(t, s, "stolen id")

		// With block1's own reservation gone the check falls through to the
		// by-value lookup, which must still protect block2's id.
		_, err = s.db.ExecContext(ctx, `DELETE FROM block_id_reservations WHERE hash = $1`, block1.Hash()[:])
		require.NoError(t, err)

		_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(otherReserved))
		require.Error(t, err)
		requireNoBlockRow(t, s, "stolen id after own reservation swept")

		id, ok, err := s.durableReservationID(ctx, block2.Hash())
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, otherReserved, id, "block2's reservation must be untouched")
	})

	t.Run("an id the sequence never issued is refused", func(t *testing.T) {
		s := newReservedIDTestStore(t)

		highest, err := highestIssuedForTest(ctx, s)
		require.NoError(t, err)

		forged := highest + 1000

		_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(forged))
		require.Error(t, err)
		requireNoBlockRow(t, s, "never-issued id")

		// A later honest reservation is unaffected by the refused attempt.
		reserved, err := s.AssignBlockID(ctx, block1.Hash())
		require.NoError(t, err)

		storedID, _, err := s.StoreBlock(ctx, block1, "", options.WithID(reserved))
		require.NoError(t, err)
		require.Equal(t, reserved, storedID)
	})

	t.Run("an id another block is already stored under is refused, not reported as a duplicate", func(t *testing.T) {
		s := newReservedIDTestStore(t)

		reserved1, err := s.AssignBlockID(ctx, block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(reserved1))
		require.NoError(t, err)

		// Without the check the INSERT hits the primary key and parseSQLError
		// calls it "block already exists", which legacy sync treats as success.
		_, _, err = s.StoreBlock(ctx, block2, "", options.WithID(reserved1))
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrBlockExists), "a different block must not be told it already exists: %v", err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "got %v", err)

		_, ok, err := s.blockIDByHash(ctx, block2.Hash())
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("a retry of a committed block still reports that the block exists", func(t *testing.T) {
		s := newReservedIDTestStore(t)

		reserved, err := s.AssignBlockID(ctx, block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(reserved))
		require.NoError(t, err)

		// StoreBlock deleted the reservation on commit. The retry must get the
		// duplicate error callers already handle, whatever id it carries.
		_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(reserved))
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockExists), "got %v", err)

		_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(reserved+1000))
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockExists), "got %v", err)
	})

	t.Run("a block whose reservation was swept still stores under the id its transactions carry", func(t *testing.T) {
		s := newReservedIDTestStore(t)

		reserved, err := s.AssignBlockID(ctx, block1.Hash())
		require.NoError(t, err)

		// sweepStaleReservations deletes rows older than an hour. A retry after a
		// long outage reads the id back from the UTXO store and arrives here with
		// no reservation row. Refusing it would wedge that block for good.
		_, err = s.db.ExecContext(ctx, `DELETE FROM block_id_reservations`)
		require.NoError(t, err)

		storedID, _, err := s.StoreBlock(ctx, block1, "", options.WithID(reserved))
		require.NoError(t, err)
		require.Equal(t, reserved, storedID)
	})
}
