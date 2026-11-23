package sql

import (
	"context"
	"database/sql"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestGetUnbatchedSurfacesInputsIterationError covers the inputs row loop in
// getUnbatched, which feeds data.TxInpoints for fields.TxInpoints and so for
// utxo.MetaFields and GetMeta. The driver fails on the second input row. Without
// a rows.Err() check the loop ends quietly after one input and the caller is told
// the transaction has one parent when it has two.
//
// The end state asserted is the returned value: an error and no data. A test
// that only checked the call did not panic would pass against the bug.
func TestGetUnbatchedSurfacesInputsIterationError(t *testing.T) {
	store, sqlMock := CreateMockStore(ulogger.TestLogger{})

	hashes := CreateTestHashes(3)
	hash := hashes[0]

	sqlMock.ExpectQuery(regexp.QuoteMeta("FROM transactions")).
		WithArgs(hash[:]).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "version", "lock_time", "fee", "size_in_bytes", "coinbase",
			"frozen", "conflicting", "locked", "unmined_since", "inserted_at",
		}).AddRow(7, 1, 0, 100, 250, false, false, false, false, nil, "2026-09-15 00:00:00"))

	iterationErr := sql.ErrConnDone

	sqlMock.ExpectQuery(regexp.QuoteMeta("SELECT previous_transaction_hash,previous_tx_idx FROM inputs WHERE transaction_id = $1 ORDER BY idx")).
		WithArgs(7).
		WillReturnRows(sqlmock.NewRows([]string{"previous_transaction_hash", "previous_tx_idx"}).
			AddRow(hashes[1][:], 0).
			AddRow(hashes[2][:], 1).
			RowError(1, iterationErr))

	data, err := store.getUnbatched(context.Background(), hash, []fields.FieldName{fields.TxInpoints})

	require.ErrorIs(t, err, iterationErr)
	require.Nil(t, data, "a truncated inputs read must not hand back a short TxInpoints")
	require.NoError(t, sqlMock.ExpectationsWereMet())
}
