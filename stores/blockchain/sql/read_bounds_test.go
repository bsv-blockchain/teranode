package sql

import (
	"context"
	"math"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestSQLReadBounds(t *testing.T) {
	ctx := context.Background()
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	s, err := New(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)
	defer s.Close(ctx)
	headers := generateBlockHeaders(t, s, 25)
	// Reversed public hashes must not underflow the preallocation or SQL span.
	got, metas, err := s.GetBlockHeadersFromTill(ctx, headers[15].Hash(), headers[10].Hash())
	require.NoError(t, err)
	require.Empty(t, got)
	require.Empty(t, metas)
	blocks, err := s.GetBlocks(ctx, headers[24].Hash(), math.MaxUint32)
	require.NoError(t, err)
	require.NotEmpty(t, blocks)
	require.Less(t, cap(blocks), 20000)
	// The upper height must be calculated in uint64; height+limit wraps otherwise.
	got, metas, err = s.GetBlockHeadersFromHeight(ctx, 1, math.MaxUint32)
	require.NoError(t, err)
	require.Len(t, got, 25)
	require.Len(t, metas, 25)
	require.Less(t, cap(got), 20000)
}
