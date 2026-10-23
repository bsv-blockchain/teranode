package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// The pruner's stamp reads the best tip between every store call of a drain, and that read
// must never come from the response cache: every block store clears the cache only after its
// own commit, so a cached read can be one block stale. Here the cache is primed with a stale
// answer by hand. The cached call returns it; the uncached call reads the row.
func TestGetBestBlockHeaderUncachedIgnoresThePrimedCache(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	id1, _, err := s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	staleHeader := &model.BlockHeader{}
	staleMeta := &model.BlockHeaderMeta{Height: 999}
	cacheOp := s.responseCache.NewOp(chainhash.HashH([]byte("GetBestBlockHeader")))
	require.True(t, cacheOp.Set([2]interface{}{staleHeader, staleMeta}, time.Minute))

	_, cachedMeta, err := s.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(999), cachedMeta.Height, "the cached call answers from the cache")

	header, meta, err := s.GetBestBlockHeaderUncached(ctx)
	require.NoError(t, err)
	require.Equal(t, block1.Hash(), header.Hash())
	require.Equal(t, uint32(1), meta.Height)
	require.Equal(t, uint32(id1), meta.ID, "the block id is carried, because the freshness check needs it") //nolint:gosec // small

	_, cachedMeta, err = s.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(999), cachedMeta.Height, "and the uncached call fills nothing")
}
