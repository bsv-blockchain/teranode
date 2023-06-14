package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_GetBlock(t *testing.T) {
	t.Run("block 0 - genesis block", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(storeUrl)
		require.NoError(t, err)

		headerHash, err := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		require.NoError(t, err)

		block, height, err := s.GetBlock(context.Background(), headerHash)
		require.NoError(t, err)

		// header
		assert.Equal(t, uint64(0), height)
		assertGenesis(t, block.Header)

		// block
		assert.Equal(t, uint64(1), block.TransactionCount)
		assert.Len(t, block.Subtrees, 1)
		txHash, err := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")
		require.NoError(t, err)

		assert.Equal(t, txHash, block.Subtrees[0])
	})
}
