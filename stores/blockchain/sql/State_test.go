package sql

import (
	"context"
	"database/sql"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/require"
)

func TestSQL_GetState(t *testing.T) {
	t.Run("state 0", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
		require.NoError(t, err)

		_, err = s.GetState(context.Background(), "test")
		require.ErrorIs(t, err, sql.ErrNoRows)

		err = s.SetState(context.Background(), "test", []byte("test data"))
		require.NoError(t, err)

		state, err := s.GetState(context.Background(), "test")
		require.NoError(t, err)
		require.Equal(t, []byte("test data"), state)

		err = s.Close()
		require.NoError(t, err)
	})
}
