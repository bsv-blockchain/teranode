package sql

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/stretchr/testify/require"
)

// The happy paths and refusals of checkCallerSuppliedBlockID are pinned in
// StoreBlock_ReservedID_test.go. These cases cover what happens when a lookup the
// check depends on fails: every one must refuse with a storage error rather than
// fall through to the INSERT, since a failed read proves nothing about the id.

func TestCheckCallerSuppliedBlockID_ClosedDB(t *testing.T) {
	s := newReservedIDTestStore(t)
	require.NoError(t, s.db.Close()) // the cleanup still closes the store once

	err := s.checkCallerSuppliedBlockID(context.Background(), block1.Hash(), 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "got %v", err)
}

func TestCheckCallerSuppliedBlockID_ReservationLookupFails(t *testing.T) {
	ctx := context.Background()
	s := newReservedIDTestStore(t)

	// The blocks table is intact, so both blocks lookups find nothing and the
	// check reaches the reservation lookup, which then fails on the missing table.
	_, err := s.db.ExecContext(ctx, `DROP TABLE block_id_reservations`)
	require.NoError(t, err)

	err = s.checkCallerSuppliedBlockID(ctx, block1.Hash(), 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "got %v", err)
	require.Contains(t, err.Error(), "failed to look up what holds caller-supplied block id 1")

	_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(1))
	require.Error(t, err)
	requireNoBlockRow(t, s, "reservation lookup failed")
}

// A real database will not fail a single statement on demand for every cause, so
// this one scripts the driver: whatever the lookup error, the check refuses.
func TestCheckCallerSuppliedBlockID_InjectedLookupFailure(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)

	h := chainhash.HashH([]byte("check-caller-supplied-id"))

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT`)).WillReturnError(errors.NewProcessingError("injected lookup failure"))

	err = s.checkCallerSuppliedBlockID(context.Background(), &h, 7)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "got %v", err)
	require.Contains(t, err.Error(), "failed to look up what holds caller-supplied block id 7")
	require.NoError(t, mock.ExpectationsWereMet())
}

// The check reads everything in one statement. Each rule's refusal is pinned in
// StoreBlock_ReservedID_test.go; this pins that it really is one round trip, which
// is the point of reading them together (slowPathMu serialises every block insert).
func TestCheckCallerSuppliedBlockID_OneRoundTrip(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)

	h := chainhash.HashH([]byte("check-caller-supplied-id"))

	rows := sqlmock.NewRows([]string{"committed", "owner", "reserved", "holder", "highest"}).
		AddRow(false, nil, nil, nil, int64(9))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT`)).WillReturnRows(rows)

	require.NoError(t, s.checkCallerSuppliedBlockID(context.Background(), &h, 7))
	require.NoError(t, mock.ExpectationsWereMet(), "the check must issue exactly one statement")
}

// The highest issued id must read as zero, never as an error, when the sequence
// has nothing to report: then every caller-supplied id without a reservation is
// refused as never issued, which is the safe direction.
func TestCallerSuppliedBlockIDFacts_NothingIssued(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		stmt string
	}{
		{"no sequence row", `DELETE FROM sqlite_sequence WHERE name = 'blocks'`},
		{"NULL sequence value", `UPDATE sqlite_sequence SET seq = NULL WHERE name = 'blocks'`},
		{"negative sequence value", `UPDATE sqlite_sequence SET seq = -1 WHERE name = 'blocks'`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newReservedIDTestStore(t)

			_, err := s.db.ExecContext(ctx, tc.stmt)
			require.NoError(t, err)

			highest, err := highestIssuedForTest(ctx, s)
			require.NoError(t, err)
			require.Zero(t, highest)

			_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(1))
			require.Error(t, err)
			require.Contains(t, err.Error(), "has only issued up to 0")
			requireNoBlockRow(t, s, tc.name)
		})
	}
}

// highestIssuedForTest reads the id sequence the way checkCallerSuppliedBlockID
// does. The hash and id are ones no test stores, so only the sequence matters.
func highestIssuedForTest(ctx context.Context, s *SQL) (uint64, error) {
	h := chainhash.HashH([]byte("highest-issued-probe"))

	f, err := s.callerSuppliedBlockIDFacts(ctx, &h, 0)

	return f.highestIssued, err
}

func TestHashString(t *testing.T) {
	h := chainhash.HashH([]byte("hash-string"))
	require.Equal(t, h.String(), hashString(h[:]))

	// A column that is not 32 bytes cannot be a hash, so it is shown as raw hex.
	require.Equal(t, "0a0b0c", hashString([]byte{0x0a, 0x0b, 0x0c}))
}
