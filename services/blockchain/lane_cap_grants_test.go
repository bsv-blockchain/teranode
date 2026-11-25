package blockchain

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// capGrantsStore answers LaneCapGrants with fixed grants, or err, and records the height it was
// asked for.
type capGrantsStore struct {
	blockchain_store.Store
	grants []lane.CapGrant
	err    error
	height uint32
}

func (s *capGrantsStore) LaneCapGrants(_ context.Context, height uint32) ([]lane.CapGrant, error) {
	s.height = height

	return s.grants, s.err
}

// rawCapGrants answers GetLaneCapGrants with a fixed response, as a server that sent it would.
type rawCapGrants struct {
	blockchain_api.BlockchainAPIClient
	resp *blockchain_api.GetLaneCapGrantsResponse
}

func (c *rawCapGrants) GetLaneCapGrants(context.Context, *blockchain_api.GetLaneCapGrantsRequest, ...grpc.CallOption) (*blockchain_api.GetLaneCapGrantsResponse, error) {
	return c.resp, nil
}

// capGrantsServer answers a Client's GetLaneCapGrants from a server, as gRPC would.
type capGrantsServer struct {
	blockchain_api.BlockchainAPIClient
	server *Blockchain
}

func (c *capGrantsServer) GetLaneCapGrants(ctx context.Context, in *blockchain_api.GetLaneCapGrantsRequest, _ ...grpc.CallOption) (*blockchain_api.GetLaneCapGrantsResponse, error) {
	return c.server.GetLaneCapGrants(ctx, in)
}

// Block assembly in a process of its own reads the grants through the server and a client: each
// field arrives as the store gave it, the hand-out time to the nanosecond, and a grant out of range
// is refused.
func TestGetLaneCapGrantsRoundTrip(t *testing.T) {
	tc := setup(t)

	handedAt := time.Unix(1_700_000_000, 123_456_789)
	store := &capGrantsStore{Store: tc.server.store, grants: []lane.CapGrant{
		{Lane: 3, First: 2_001, Height: 996, HandedAt: handedAt},
		{Lane: 255, First: lane.MaxPosition, Height: 1_000, HandedAt: handedAt.Add(time.Minute)},
	}}
	tc.server.store = store

	c := &Client{client: &capGrantsServer{server: tc.server}, logger: tc.logger}

	got, err := c.LaneCapGrants(context.Background(), 995)
	require.NoError(t, err)
	require.Equal(t, uint32(995), store.height)
	require.Len(t, got, 2)

	for i, want := range store.grants {
		require.Equal(t, want.Lane, got[i].Lane)
		require.Equal(t, want.First, got[i].First)
		require.Equal(t, want.Height, got[i].Height)
		require.True(t, want.HandedAt.Equal(got[i].HandedAt), "%s, want %s", got[i].HandedAt, want.HandedAt)
	}

	store.grants = []lane.CapGrant{{Lane: 1, First: 0, Height: 996, HandedAt: handedAt}}

	_, err = c.LaneCapGrants(context.Background(), 995)
	require.Error(t, err, "a grant with no first position")

	store.grants, store.err = nil, context.DeadlineExceeded

	_, err = c.LaneCapGrants(context.Background(), 995)
	require.Error(t, err, "the store's error reaches the client")

	// A client refuses a grant whose lane or first position does not fit, whatever sent it.
	for name, g := range map[string]*blockchain_api.LaneCapGrant{
		"lane above 255":       {Lane: lane.MaxLane + 1, First: 1},
		"first above the lane": {Lane: 1, First: lane.MaxPosition + 1},
		"no first position":    {Lane: 1, First: 0},
	} {
		raw := &Client{client: &rawCapGrants{resp: &blockchain_api.GetLaneCapGrantsResponse{Grants: []*blockchain_api.LaneCapGrant{g}}}, logger: tc.logger}

		_, err = raw.LaneCapGrants(context.Background(), 995)
		require.Error(t, err, name)
	}
}

// A block assembly in the blockchain service's process reads the grants from the store directly,
// at the height it asks for.
func TestLocalClientLaneCapGrants(t *testing.T) {
	tc := setup(t)

	store := &capGrantsStore{Store: tc.server.store, grants: []lane.CapGrant{{Lane: 3, First: 9, Height: 7}}}
	c := &LocalClient{store: store}

	got, err := c.LaneCapGrants(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, store.grants, got)
	require.Equal(t, uint32(7), store.height)
}
