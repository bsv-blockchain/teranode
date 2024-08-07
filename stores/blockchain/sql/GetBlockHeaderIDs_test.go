package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_GetBlockHeaderIDs(t *testing.T) {
	t.Run("empty - no error", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		headerIDs, err := s.GetBlockHeaderIDs(context.Background(), &chainhash.Hash{}, 2)
		require.NoError(t, err)
		assert.Equal(t, 0, len(headerIDs))
	})

	t.Run("", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "")
		require.NoError(t, err)

		headerIDs, err := s.GetBlockHeaderIDs(context.Background(), &chainhash.Hash{}, 10)
		require.NoError(t, err)
		assert.Equal(t, 0, len(headerIDs))

		headerIDs, err = s.GetBlockHeaderIDs(context.Background(), block1.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headerIDs))
		require.Equal(t, headerIDs, []uint32{1, 0})

		headerIDs, err = s.GetBlockHeaderIDs(context.Background(), block2.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 3, len(headerIDs))
		require.Equal(t, headerIDs, []uint32{2, 1, 0})

		headerIDs, err = s.GetBlockHeaderIDs(context.Background(), blockAlternative2.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 3, len(headerIDs))
		require.Equal(t, headerIDs, []uint32{3, 1, 0})

		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		headerIDs, err = s.GetBlockHeaderIDs(context.Background(), block3.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 4, len(headerIDs))
		require.Equal(t, headerIDs, []uint32{4, 2, 1, 0})
	})
}
