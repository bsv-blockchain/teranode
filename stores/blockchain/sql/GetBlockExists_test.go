package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetBlockExists(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("check non-existent block", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		exists, err := s.GetBlockExists(context.Background(), block1.Hash())
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("check existing block", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store block1
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		exists, err := s.GetBlockExists(context.Background(), block1.Hash())
		require.NoError(t, err)
		assert.True(t, exists)
	})
}
