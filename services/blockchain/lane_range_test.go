package blockchain

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestServerReserveLaneRange covers the handler: size comes from settings, a second ask for
// the same lane continues above the first range, and a lane above 255 is rejected.
func TestServerReserveLaneRange(t *testing.T) {
	tc := setup(t)
	ctx := context.Background()

	tc.server.settings.BlockChain.LaneRangeSize = 500

	before := testutil.ToFloat64(prometheusBlockchainLaneHandouts.WithLabelValues("0"))

	first, err := tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: "validator-1"})
	require.NoError(t, err)
	require.Equal(t, &blockchain_api.ReserveLaneRangeResponse{Lane: 0, First: 1, Last: 500}, first)

	second, err := tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: "validator-1", WantLane: first.Lane, HasLane: true})
	require.NoError(t, err)
	require.Equal(t, uint64(501), second.First)
	require.Equal(t, uint64(1000), second.Last)

	// A second holder naming no lane is assigned one that is free.
	other, err := tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: "validator-2"})
	require.NoError(t, err)
	require.NotEqual(t, first.Lane, other.Lane, "a lane with a recent hand-out is not free")

	require.Equal(t, before+2, testutil.ToFloat64(prometheusBlockchainLaneHandouts.WithLabelValues("0")))

	_, err = tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: "x", WantLane: 256, HasLane: true})
	require.Error(t, err)
}

// TestClientReserveLaneRangeOverGRPC sends the request through a real gRPC client and server.
func TestClientReserveLaneRangeOverGRPC(t *testing.T) {
	_, s, _ := startAuthenticatedBlockchain(t, false)
	s.BlockChain.LaneRangeSize = 100

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := NewClient(ctx, ulogger.NewErrorTestLogger(t), s, "lane-client")
	require.NoError(t, err)

	defer func() { require.NoError(t, client.(*Client).Close()) }()

	var src lane.RangeSource = client

	r1, err := src.ReserveLaneRange(ctx, "holder-a", 0, false)
	require.NoError(t, err)
	require.Equal(t, lane.Range{Lane: 0, First: 1, Last: 100}, r1)

	r2, err := src.ReserveLaneRange(ctx, "holder-a", r1.Lane, true)
	require.NoError(t, err)
	require.Equal(t, lane.Range{Lane: 0, First: 101, Last: 200}, r2)

	r3, err := src.ReserveLaneRange(ctx, "holder-b", 0, false)
	require.NoError(t, err)
	require.Equal(t, lane.Range{Lane: 1, First: 1, Last: 100}, r3, "the wire carries no lane as no lane, not as lane 0")
}
