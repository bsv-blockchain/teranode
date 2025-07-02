package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetBlockHeight(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("block 0 - genesis block", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		headerHash, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
		require.NoError(t, err)

		height, err := s.GetBlockHeight(context.Background(), headerHash)
		require.NoError(t, err)

		assert.Equal(t, uint32(0), height)
	})

	t.Run("block 2", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		height, err := s.GetBlockHeight(context.Background(), block2.Hash())
		require.NoError(t, err)

		assert.Equal(t, uint32(2), height)
	})
}
