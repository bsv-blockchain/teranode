package blockchain

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

func TestLaneMapRowsRoundTripOverAddBlock(t *testing.T) {
	rows := []lane.MapRow{
		{Lane: 0, First: 1, Last: 90_000, Tag: lane.RowTagRange},
		{Lane: 255, First: 5, Last: 9, FirstHole: 6, Tag: lane.RowTagRoaring64, HoleData: []byte{1, 2, 3}},
	}

	back, err := laneMapRowsFromProto(laneMapRowsToProto(rows))
	require.NoError(t, err)
	require.Equal(t, rows, back)

	none, err := laneMapRowsFromProto(laneMapRowsToProto(nil))
	require.NoError(t, err)
	require.Nil(t, none)
}

func TestLaneMapRowsFromProtoRefusesFieldsThatDoNotFit(t *testing.T) {
	_, err := laneMapRowsFromProto([]*blockchain_api.LaneMapRow{{Lane: 256, First: 1, Last: 1}})
	require.Error(t, err)

	_, err = laneMapRowsFromProto([]*blockchain_api.LaneMapRow{{Lane: 1, First: 1, Last: 1, Tag: 256}})
	require.Error(t, err)
}

// The server hands the rows AddBlock carries to the store, which commits them with the block.
func TestAddBlockStoresTheLaneMapRowsItCarries(t *testing.T) {
	tc := setup(t)
	blk := mockBlock(tc, t)

	rows := []lane.MapRow{{Lane: 7, First: 1, Last: 90_000, Tag: lane.RowTagRange}}

	subtreeHashes := make([][]byte, len(blk.Subtrees))
	for i, hash := range blk.Subtrees {
		subtreeHashes[i] = hash[:]
	}

	_, err := tc.server.AddBlock(context.Background(), &blockchain_api.AddBlockRequest{
		Header:           blk.Header.Bytes(),
		CoinbaseTx:       blk.CoinbaseTx.Bytes(),
		SubtreeHashes:    subtreeHashes,
		TransactionCount: blk.TransactionCount,
		SizeInBytes:      blk.SizeInBytes,
		PeerId:           "test-peer",
		LaneMapRows:      laneMapRowsToProto(rows),
	})
	require.NoError(t, err)

	_, meta, err := tc.server.store.GetBlockHeader(context.Background(), blk.Hash())
	require.NoError(t, err)

	got, err := tc.server.store.LaneMapRows(context.Background(), meta.ID)
	require.NoError(t, err)
	require.Equal(t, rows, got)
}
