package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetBlockGraphData(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("get graph data with empty chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		data, err := s.GetBlockGraphData(context.Background(), uint64(time.Hour.Milliseconds())) // nolint:gosec
		require.NoError(t, err)
		assert.Empty(t, data.DataPoints)
	})

	t.Run("get graph data with blocks", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks 1, 2, and 3
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		data, err := s.GetBlockGraphData(context.Background(), uint64(time.Hour.Milliseconds())) // nolint:gosec
		require.NoError(t, err)
		assert.NotEmpty(t, data.DataPoints)
		assert.Len(t, data.DataPoints, 3)
	})
}
