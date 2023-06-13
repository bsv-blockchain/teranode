package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_GetBlockHeaders(t *testing.T) {
	t.Run("", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(storeUrl)
		require.NoError(t, err)

		err = s.StoreBlock(context.Background(), block1)
		require.NoError(t, err)

		err = s.StoreBlock(context.Background(), block2)
		require.NoError(t, err)

		headers, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 2)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers))
		assert.Equal(t, block2.Header.Hash(), headers[0].Hash())
		assert.Equal(t, block1.Header.Hash(), headers[1].Hash())

		headers, err = s.GetBlockHeaders(context.Background(), block1.Hash(), 2)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers))
		assert.Equal(t, block1.Header.Hash(), headers[0].Hash())
		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", headers[1].Hash().String())

		headers, err = s.GetBlockHeaders(context.Background(), block1.Hash(), 1)
		require.NoError(t, err)
		assert.Equal(t, 1, len(headers))
		assert.Equal(t, block1.Header.Hash(), headers[0].Hash())

		headers, err = s.GetBlockHeaders(context.Background(), block1.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers)) // there are only 2 headers in the chain
		assert.Equal(t, block1.Header.Hash(), headers[0].Hash())
		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", headers[1].Hash().String())
	})
}
