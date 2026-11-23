package collect

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/stretchr/testify/require"
)

type fakePeers struct {
	p2p.ClientI

	mu    sync.Mutex
	peers []*p2p.PeerInfo
	err   error
	hang  bool
}

func (f *fakePeers) set(peers []*p2p.PeerInfo, err error, hang bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.peers, f.err, f.hang = peers, err, hang
}

func (f *fakePeers) GetPeers(ctx context.Context) ([]*p2p.PeerInfo, error) {
	f.mu.Lock()
	peers, err, hang := f.peers, f.err, f.hang
	f.mu.Unlock()

	if hang {
		<-ctx.Done()

		return nil, ctx.Err()
	}

	if err != nil {
		return nil, err
	}

	return peers, nil
}

func collectPeers(t *testing.T, p *Peers) (snapshot.Snapshot, error) {
	t.Helper()

	patch, err := p.Collect(context.Background())
	if err != nil {
		require.Nil(t, patch)

		return snapshot.Snapshot{}, err
	}

	require.NotNil(t, patch)

	var s snapshot.Snapshot

	patch(&s)

	return s, nil
}

func TestPeersReportsLowerMedianOfCappedHeights(t *testing.T) {
	p := NewPeers(&fakePeers{peers: []*p2p.PeerInfo{
		{Height: 10, AdvertisedHeight: 50},
		{Height: 70},
		nil,
		{Height: 20, AdvertisedHeight: 500_000_000},
	}})

	s, err := collectPeers(t, p)
	require.NoError(t, err)

	require.Equal(t, "p2p", p.Name())
	require.Equal(t, uint32(20), s.PeerTip)
	require.Equal(t, snapshot.Reading{Value: 20, Status: snapshot.StatusOK}, s.Chain.PeerHeight)
}

func TestOneLyingPeerCannotRaiseThePeerHeight(t *testing.T) {
	honest := []*p2p.PeerInfo{{Height: 1000}, {Height: 1001}, {Height: 1000}}
	liar := &p2p.PeerInfo{Height: 11_000, AdvertisedHeight: 500_000_000}

	for _, peers := range [][]*p2p.PeerInfo{
		{honest[0], liar},
		{honest[0], honest[1], liar},
		{honest[0], honest[1], honest[2], liar},
	} {
		s, err := collectPeers(t, NewPeers(&fakePeers{peers: peers}))
		require.NoError(t, err)
		require.LessOrEqual(t, s.PeerTip, uint32(1001), "peers %d", len(peers))
	}
}

func TestPeersMedianWithDivergentHeights(t *testing.T) {
	low := &p2p.PeerInfo{Height: 1000}
	high := &p2p.PeerInfo{Height: 11_000}
	secondHigh := &p2p.PeerInfo{Height: 11_000}

	for _, tc := range []struct {
		peers []*p2p.PeerInfo
		want  uint32
	}{
		{[]*p2p.PeerInfo{low, high}, 1000},
		{[]*p2p.PeerInfo{low, high, secondHigh}, 11_000},
	} {
		s, err := collectPeers(t, NewPeers(&fakePeers{peers: tc.peers}))
		require.NoError(t, err)
		require.Equal(t, tc.want, s.PeerTip)
		require.Equal(t, float64(tc.want), s.Chain.PeerHeight.Value)
	}
}

func TestPeersWithoutAHeightAreSkipped(t *testing.T) {
	s, err := collectPeers(t, NewPeers(&fakePeers{peers: []*p2p.PeerInfo{{Height: 0}, {Height: 0}, {Height: 500}}}))
	require.NoError(t, err)
	require.Equal(t, uint32(500), s.PeerTip)
}

func TestPeersIgnoresBannedPeers(t *testing.T) {
	p := NewPeers(&fakePeers{peers: []*p2p.PeerInfo{
		{Height: 100},
		{Height: 200},
		{Height: 300, IsBanned: true},
	}})

	s, err := collectPeers(t, p)
	require.NoError(t, err)

	require.Equal(t, uint32(100), s.PeerTip, "counting the banned peer would move the lower median to 200")
	require.Equal(t, snapshot.Reading{Value: 100, Status: snapshot.StatusOK}, s.Chain.PeerHeight)
}

func TestPeersWithNoUsableHeightSayWhy(t *testing.T) {
	for name, peers := range map[string][]*p2p.PeerInfo{
		"all banned":   {{Height: 500, IsBanned: true}, {Height: 600, IsBanned: true}},
		"no heights":   {{Height: 0}, {Height: 0}},
		"banned, zero": {{Height: 500, IsBanned: true}, {Height: 0}},
	} {
		t.Run(name, func(t *testing.T) {
			s, err := collectPeers(t, NewPeers(&fakePeers{peers: peers}))
			require.NoError(t, err)

			require.Equal(t, uint32(0), s.PeerTip)
			require.Equal(t, snapshot.Reading{Value: 0, Status: snapshot.StatusOK, Note: "no usable peer height"}, s.Chain.PeerHeight)
		})
	}
}

func TestPeersWithNoPeersIsLiveZero(t *testing.T) {
	s, err := collectPeers(t, NewPeers(&fakePeers{}))
	require.NoError(t, err)

	require.Equal(t, uint32(0), s.PeerTip)
	require.Equal(t, snapshot.Reading{Value: 0, Status: snapshot.StatusOK, Note: "no peers"}, s.Chain.PeerHeight)
}

func TestPeersFailureReturnsError(t *testing.T) {
	_, err := collectPeers(t, NewPeers(&fakePeers{err: errors.NewServiceError("p2p unavailable")}))
	require.Error(t, err)
}

func TestHungPeersCallHonoursContext(t *testing.T) {
	p := NewPeers(&fakePeers{hang: true})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	patch, err := p.Collect(ctx)

	require.Error(t, err)
	require.Nil(t, patch)
	require.Less(t, time.Since(start), time.Second)
}

func TestPeersOwnsOnlyPeerFields(t *testing.T) {
	s, err := collectPeers(t, NewPeers(&fakePeers{peers: []*p2p.PeerInfo{{Height: 5}}}))
	require.NoError(t, err)

	require.Empty(t, s.FSM)
	require.Zero(t, s.Tip)
	require.Nil(t, s.Chain.BlocksWaiting)
}
