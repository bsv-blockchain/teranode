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

func TestSQLGetBlockStats(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("get stats with empty chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings.ChainCfgParams)
		require.NoError(t, err)

		stats, err := s.GetBlockStats(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint64(1), stats.BlockCount)
		assert.Equal(t, uint64(0), stats.TxCount)
	})

	t.Run("get stats with blocks", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings.ChainCfgParams)
		require.NoError(t, err)

		// Store blocks 1, 2, and 3
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		stats, err := s.GetBlockStats(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint64(3), stats.BlockCount) // There are 3 blocks
	})
}
