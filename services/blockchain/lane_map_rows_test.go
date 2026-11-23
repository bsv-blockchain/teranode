package blockchain

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
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

// Lane 255 and tag 255 fit; only past a byte is refused.
func TestLaneMapRowsFromProtoTakesTheWidestFields(t *testing.T) {
	got, err := laneMapRowsFromProto([]*blockchain_api.LaneMapRow{{Lane: 255, First: 1, Last: 1, Tag: 255}})
	require.NoError(t, err)
	require.Equal(t, uint8(255), got[0].Lane)
	require.Equal(t, lane.RowTag(255), got[0].Tag)
}

// addBlockCapture records the AddBlock request a Client sends.
type addBlockCapture struct {
	blockchain_api.BlockchainAPIClient
	req *blockchain_api.AddBlockRequest
}

func (c *addBlockCapture) AddBlock(_ context.Context, in *blockchain_api.AddBlockRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	c.req = in
	return &emptypb.Empty{}, nil
}

// The client puts the rows WithLaneMapRows names on the request, and none without it.
func TestClientAddBlockSendsTheLaneMapRows(t *testing.T) {
	tc := setup(t)
	blk := mockBlock(tc, t)

	rows := []lane.MapRow{{Lane: 7, First: 1, Last: 9, Tag: lane.RowTagRange}}

	capture := &addBlockCapture{}
	c := &Client{client: capture, logger: tc.logger}

	require.NoError(t, c.AddBlock(context.Background(), blk, "peer", options.WithLaneMapRows(rows)))

	got, err := laneMapRowsFromProto(capture.req.GetLaneMapRows())
	require.NoError(t, err)
	require.Equal(t, rows, got)

	require.NoError(t, c.AddBlock(context.Background(), blk, "peer"))
	require.Empty(t, capture.req.GetLaneMapRows())
}

// Rows that do not parse are dropped and counted under the block they came with, our own or a
// peer's, and the block is stored without them.
func TestAddBlockStoresTheBlockWhenItsLaneMapRowsDoNotParse(t *testing.T) {
	for _, own := range []bool{false, true} {
		t.Run(fmt.Sprintf("own=%t", own), func(t *testing.T) {
			tc := setup(t)
			blk := mockBlock(tc, t)

			subtreeHashes := make([][]byte, len(blk.Subtrees))
			for i, hash := range blk.Subtrees {
				subtreeHashes[i] = hash[:]
			}

			counted, other := lane.BlockPeer, lane.BlockOwn
			if own {
				counted, other = lane.BlockOwn, lane.BlockPeer
			}

			failures := laneSeries(t, "teranode_lane_row_insert_failures_total", counted, "")
			otherFailures := laneSeries(t, "teranode_lane_row_insert_failures_total", other, "")

			_, err := tc.server.AddBlock(context.Background(), &blockchain_api.AddBlockRequest{
				Header:              blk.Header.Bytes(),
				CoinbaseTx:          blk.CoinbaseTx.Bytes(),
				SubtreeHashes:       subtreeHashes,
				TransactionCount:    blk.TransactionCount,
				SizeInBytes:         blk.SizeInBytes,
				PeerId:              "test-peer",
				LaneMapRows:         []*blockchain_api.LaneMapRow{{Lane: 256, First: 1, Last: 1}},
				LaneMapRowsOwnBlock: own,
			})
			require.NoError(t, err, "the block is stored")
			require.Equal(t, failures+1, laneSeries(t, "teranode_lane_row_insert_failures_total", counted, ""))
			require.Equal(t, otherFailures, laneSeries(t, "teranode_lane_row_insert_failures_total", other, ""))

			_, meta, err := tc.server.store.GetBlockHeader(context.Background(), blk.Hash())
			require.NoError(t, err)

			got, err := tc.server.store.LaneMapRows(context.Background(), meta.ID)
			require.NoError(t, err)
			require.Empty(t, got)
		})
	}
}

func laneInsertFailures(t *testing.T) float64 {
	t.Helper()

	return laneSeries(t, "teranode_lane_row_insert_failures_total", lane.BlockPeer, "")
}

// laneSeries is the counter value, or the histogram's observation count, of metric's series for
// block and, when not empty, stage.
func laneSeries(t *testing.T, metric, block, stage string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var sum float64

	for _, f := range families {
		if f.GetName() != metric {
			continue
		}

		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			if labels["block"] != block || (stage != "" && labels["stage"] != stage) {
				continue
			}

			if m.GetCounter() != nil {
				sum += m.GetCounter().GetValue()
			}

			if m.GetHistogram() != nil {
				sum += float64(m.GetHistogram().GetSampleCount())
			}
		}
	}

	return sum
}

// The client marks rows given WithOwnBlockLaneMapRows as our own block's, and sends the mark only
// with rows, so a block stored without lanes is the same on the wire.
func TestClientAddBlockMarksOurOwnBlocksLaneMapRows(t *testing.T) {
	tc := setup(t)
	blk := mockBlock(tc, t)

	rows := []lane.MapRow{{Lane: 7, First: 1, Last: 9, Tag: lane.RowTagRange}}

	capture := &addBlockCapture{}
	c := &Client{client: capture, logger: tc.logger}

	require.NoError(t, c.AddBlock(context.Background(), blk, "", options.WithOwnBlockLaneMapRows(rows)))
	require.True(t, capture.req.GetLaneMapRowsOwnBlock())
	require.Len(t, capture.req.GetLaneMapRows(), 1)

	require.NoError(t, c.AddBlock(context.Background(), blk, "peer", options.WithLaneMapRows(rows)))
	require.False(t, capture.req.GetLaneMapRowsOwnBlock(), "a peer's block")

	require.NoError(t, c.AddBlock(context.Background(), blk, "", options.WithOwnBlockLaneMapRows(nil)))
	require.False(t, capture.req.GetLaneMapRowsOwnBlock(), "no mark without rows")
}

// The server stores our own block's rows and the store times their insert under block="own".
func TestAddBlockCountsOurOwnBlocksLaneMapRowsAsOwn(t *testing.T) {
	tc := setup(t)
	blk := mockBlock(tc, t)

	subtreeHashes := make([][]byte, len(blk.Subtrees))
	for i, hash := range blk.Subtrees {
		subtreeHashes[i] = hash[:]
	}

	own := laneSeries(t, "teranode_lane_gate_seconds", lane.BlockOwn, lane.GateStageInsert)
	peer := laneSeries(t, "teranode_lane_gate_seconds", lane.BlockPeer, lane.GateStageInsert)

	_, err := tc.server.AddBlock(context.Background(), &blockchain_api.AddBlockRequest{
		Header:              blk.Header.Bytes(),
		CoinbaseTx:          blk.CoinbaseTx.Bytes(),
		SubtreeHashes:       subtreeHashes,
		TransactionCount:    blk.TransactionCount,
		SizeInBytes:         blk.SizeInBytes,
		LaneMapRows:         []*blockchain_api.LaneMapRow{{Lane: 7, First: 1, Last: 9, Tag: uint32(lane.RowTagRange)}},
		LaneMapRowsOwnBlock: true,
	})
	require.NoError(t, err)

	require.Equal(t, own+1, laneSeries(t, "teranode_lane_gate_seconds", lane.BlockOwn, lane.GateStageInsert))
	require.Equal(t, peer, laneSeries(t, "teranode_lane_gate_seconds", lane.BlockPeer, lane.GateStageInsert))

	_, meta, err := tc.server.store.GetBlockHeader(context.Background(), blk.Hash())
	require.NoError(t, err)

	got, err := tc.server.store.LaneMapRows(context.Background(), meta.ID)
	require.NoError(t, err)
	require.Len(t, got, 1)
}
