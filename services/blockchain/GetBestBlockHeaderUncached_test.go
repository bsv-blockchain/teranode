package blockchain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// The uncached best-header call carries the block id, which the cached call leaves at zero.
// The pruner's stamp asks CheckBlockIsAncestorOfBlock about its drain's anchor by id, so an
// answer without the id would leave the freshness check nothing to ask about.
func TestGetBestBlockHeaderUncachedCarriesTheBlockID(t *testing.T) {
	ctx := setup(t)

	blk := mockBlock(ctx, t)
	blockID, _, err := ctx.server.store.StoreBlock(context.Background(), blk, "peer1")
	require.NoError(t, err)

	resp, err := ctx.server.GetBestBlockHeaderUncached(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, uint32(blockID), resp.Id) //nolint:gosec // a test id is small
	require.Equal(t, blk.Height, resp.Height)
	require.Equal(t, blk.Header.Bytes(), resp.BlockHeader)
}
