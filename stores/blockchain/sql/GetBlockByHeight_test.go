package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_GetBlockByHeight(t *testing.T) {
	t.Run("block 0 - genesis block", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		block, err := s.GetBlockByHeight(context.Background(), 0)
		require.NoError(t, err)

		assertGenesis(t, block.Header)

		// block
		assert.Equal(t, uint64(1), block.TransactionCount)
		assert.Len(t, block.Subtrees, 0)
	})

	t.Run("blocks ", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		_, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		block, err := s.GetBlockByHeight(context.Background(), 1)
		require.NoError(t, err)

		assert.Equal(t, block1.String(), block.String())

		block, err = s.GetBlockByHeight(context.Background(), 2)
		require.NoError(t, err)

		assert.Equal(t, block2.String(), block.String())
	})
}
