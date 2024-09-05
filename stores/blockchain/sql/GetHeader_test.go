package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestSQL_GetHeader(t *testing.T) {
	t.Run("block 0 - genesis block header", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
		require.NoError(t, err)

		headerHash, err := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		require.NoError(t, err)

		header, err := s.GetHeader(context.Background(), headerHash)
		require.NoError(t, err)

		assertGenesis(t, header)
	})
}
