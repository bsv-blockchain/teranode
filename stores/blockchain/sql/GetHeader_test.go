package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestSQLGetHeader(t *testing.T) {
	t.Run("block 0 - genesis block header", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings(t)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		headerHash, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
		require.NoError(t, err)

		header, err := s.GetHeader(context.Background(), headerHash)
		require.NoError(t, err)

		assertRegtestGenesis(t, header)
	})
}
