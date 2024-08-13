package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_GetBlockHeaders(t *testing.T) {
	t.Run("empty - no error", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		headers, heights, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 2)
		require.NoError(t, err)
		assert.Equal(t, 0, len(headers))
		assert.Equal(t, 0, len(heights))
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

		block2_alt := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1231469744,
				Nonce:          1639830026,
				HashPrevBlock:  block2PrevBlockHash,
				HashMerkleRoot: block2MerkleRootHash,
				Bits:           bits,
			},
			CoinbaseTx:       coinbaseTx2,
			TransactionCount: 1,
			Subtrees: []*chainhash.Hash{
				subtree,
			},
		}

		_, _, err = s.StoreBlock(context.Background(), block2_alt, "")
		require.NoError(t, err)

		headers, metas, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 2)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers))
		assert.Equal(t, block2.Header.Hash(), headers[0].Hash())
		assert.Equal(t, uint32(2), metas[0].Height)
		assert.Equal(t, block1.Header.Hash(), headers[1].Hash())
		assert.Equal(t, uint32(1), metas[1].Height)

		headers, metas, err = s.GetBlockHeaders(context.Background(), block1.Hash(), 2)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers))
		assert.Equal(t, block1.Header.Hash(), headers[0].Hash())
		assert.Equal(t, uint32(1), metas[0].Height)
		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", headers[1].Hash().String())

		headers, metas, err = s.GetBlockHeaders(context.Background(), block1.Hash(), 1)
		require.NoError(t, err)
		assert.Equal(t, 1, len(headers))
		assert.Equal(t, block1.Header.Hash(), headers[0].Hash())
		assert.Equal(t, uint32(1), metas[0].Height)

		headers, metas, err = s.GetBlockHeaders(context.Background(), block1.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers)) // there are only 2 headers in the chain
		assert.Equal(t, block1.Header.Hash(), headers[0].Hash())
		assert.Equal(t, uint32(1), metas[0].Height)
		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", headers[1].Hash().String())

		headers, metas, err = s.GetBlockHeaders(context.Background(), block2_alt.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 3, len(headers)) // there should be 3 headers in the chain
		assert.Equal(t, block2_alt.Header.Hash(), headers[0].Hash())
		assert.Equal(t, uint32(2), metas[0].Height)
		assert.Equal(t, block1.Header.Hash(), headers[1].Hash())
		assert.Equal(t, uint32(1), metas[1].Height)
		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", headers[2].Hash().String())
	})
}
