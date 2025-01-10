package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("block 0 - genesis block", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		headerHash, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
		require.NoError(t, err)

		block, height, err := s.GetBlock(context.Background(), headerHash)
		require.NoError(t, err)

		// header
		assert.Equal(t, uint32(0), height)
		assertRegtestGenesis(t, block.Header)

		// block
		assert.Equal(t, uint64(1), block.TransactionCount)
		assert.Len(t, block.Subtrees, 0)
	})
}
