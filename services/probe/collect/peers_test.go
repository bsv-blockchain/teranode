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

func TestPeersReportsMaxPeerHeight(t *testing.T) {
	p := NewPeers(&fakePeers{peers: []*p2p.PeerInfo{
		{Height: 10, AdvertisedHeight: 50},
		{Height: 70},
		nil,
		{Height: 20, AdvertisedHeight: 30},
	}})

	s, err := collectPeers(t, p)
	require.NoError(t, err)

	require.Equal(t, "p2p", p.Name())
	require.Equal(t, uint32(70), s.PeerTip)
	require.Equal(t, snapshot.Reading{Value: 70, Status: snapshot.StatusOK}, s.Chain.PeerHeight)
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
