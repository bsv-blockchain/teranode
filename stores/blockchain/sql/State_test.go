package sql

import (
	"context"
	"database/sql"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestSQLGetState(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("state 0", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// A key never written is not found, as a code a caller can match with errors.Is,
		// over gRPC too; the store error's text is kept for callers that match on it.
		_, err = s.GetState(context.Background(), "test")
		require.ErrorIs(t, err, errors.ErrNotFound)
		require.ErrorIs(t, err, sql.ErrNoRows, "callers matching the raw error still match")

		err = s.SetState(context.Background(), "test", []byte("test data"))
		require.NoError(t, err)

		state, err := s.GetState(context.Background(), "test")
		require.NoError(t, err)
		require.Equal(t, []byte("test data"), state)

		err = s.Close(context.Background())
		require.NoError(t, err)
	})
}
