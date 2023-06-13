package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_GetHeader(t *testing.T) {
	t.Run("block 0 - genesis block header", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(storeUrl)
		require.NoError(t, err)

		headerHash, err := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		require.NoError(t, err)

		header, err := s.GetHeader(context.Background(), headerHash)
		require.NoError(t, err)

		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", header.Hash().String())
		assert.Equal(t, uint32(1), header.Version)
		assert.Equal(t, &chainhash.Hash{}, header.HashPrevBlock)
		assert.Equal(t, "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b", header.HashMerkleRoot.String())
		assert.Equal(t, uint32(1231006505), header.Timestamp)
		assert.Equal(t, []byte{0x1d, 0x0, 0xff, 0xff}, header.Bits)
		assert.Equal(t, uint32(2083236893), header.Nonce)
	})
}
