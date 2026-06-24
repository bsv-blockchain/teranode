package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetBlockGraphData(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

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

	t.Run("get graph data with periodMillis=0 returns all blocks", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		// periodMillis=0 means block_time >= 0, which matches every block.
		data, err := s.GetBlockGraphData(context.Background(), 0)
		require.NoError(t, err)
		assert.Len(t, data.DataPoints, 3)
	})

	t.Run("get graph data via mainChainRebuilding CTE branch", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		// Simulate an in-progress rebuild so GetBlockGraphData uses the CTE branch.
		s.mainChainRebuilding.Add(1)
		defer s.mainChainRebuilding.Add(-1)

		data, err := s.GetBlockGraphData(context.Background(), 0)
		require.NoError(t, err)
		assert.NotEmpty(t, data.DataPoints)
	})
}
