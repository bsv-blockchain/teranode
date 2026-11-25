package blockchain

import (
	"context"
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

// Rows that do not parse are dropped and counted, and the block is stored without them.
func TestAddBlockStoresTheBlockWhenItsLaneMapRowsDoNotParse(t *testing.T) {
	tc := setup(t)
	blk := mockBlock(tc, t)

	subtreeHashes := make([][]byte, len(blk.Subtrees))
	for i, hash := range blk.Subtrees {
		subtreeHashes[i] = hash[:]
	}

	failures := laneInsertFailures(t)

	_, err := tc.server.AddBlock(context.Background(), &blockchain_api.AddBlockRequest{
		Header:           blk.Header.Bytes(),
		CoinbaseTx:       blk.CoinbaseTx.Bytes(),
		SubtreeHashes:    subtreeHashes,
		TransactionCount: blk.TransactionCount,
		SizeInBytes:      blk.SizeInBytes,
		PeerId:           "test-peer",
		LaneMapRows:      []*blockchain_api.LaneMapRow{{Lane: 256, First: 1, Last: 1}},
	})
	require.NoError(t, err, "the block is stored")
	require.Equal(t, failures+1, laneInsertFailures(t))

	_, meta, err := tc.server.store.GetBlockHeader(context.Background(), blk.Hash())
	require.NoError(t, err)

	got, err := tc.server.store.LaneMapRows(context.Background(), meta.ID)
	require.NoError(t, err)
	require.Empty(t, got)
}

func laneInsertFailures(t *testing.T) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() == "teranode_lane_row_insert_failures_total" {
			return f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	return 0
}

// laneRowsServer answers a Client's GetLaneMapRows from a server, as gRPC would.
type laneRowsServer struct {
	blockchain_api.BlockchainAPIClient
	server *Blockchain
}

func (c *laneRowsServer) GetLaneMapRows(ctx context.Context, in *blockchain_api.GetLaneMapRowsRequest, _ ...grpc.CallOption) (*blockchain_api.GetLaneMapRowsResponse, error) {
	return c.server.GetLaneMapRows(ctx, in)
}

// A reader reads back the rows AddBlock stored, through the server and a client, and nothing for a
// block without rows.
func TestGetLaneMapRowsReturnsTheRowsStoredWithTheBlock(t *testing.T) {
	tc := setup(t)
	blk := mockBlock(tc, t)

	rows := []lane.MapRow{
		{Lane: 3, First: 1, Last: 10, Tag: lane.RowTagRange},
		{Lane: 7, First: 1, Last: 90_000, Tag: lane.RowTagRange},
	}

	_, _, err := tc.server.store.StoreBlock(context.Background(), blk, "peer", options.WithLaneMapRows(rows))
	require.NoError(t, err)

	_, meta, err := tc.server.store.GetBlockHeader(context.Background(), blk.Hash())
	require.NoError(t, err)

	c := &Client{client: &laneRowsServer{server: tc.server}, logger: tc.logger}

	got, err := c.LaneMapRows(context.Background(), meta.ID)
	require.NoError(t, err)
	require.Equal(t, rows, got)

	got, err = c.LaneMapRows(context.Background(), meta.ID+1_000)
	require.NoError(t, err)
	require.Empty(t, got)
}
