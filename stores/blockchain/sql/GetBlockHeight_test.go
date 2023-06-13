package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_GetBlockHeight(t *testing.T) {
	t.Run("block 0 - genesis block", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(storeUrl)
		require.NoError(t, err)

		headerHash, err := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		require.NoError(t, err)

		height, err := s.GetBlockHeight(context.Background(), headerHash)
		require.NoError(t, err)

		assert.Equal(t, uint64(0), height)
	})

	t.Run("block 2", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(storeUrl)
		require.NoError(t, err)

		err = s.StoreBlock(context.Background(), block1)
		require.NoError(t, err)

		err = s.StoreBlock(context.Background(), block2)
		require.NoError(t, err)

		height, err := s.GetBlockHeight(context.Background(), block2.Hash())
		require.NoError(t, err)

		assert.Equal(t, uint64(2), height)
	})
}
