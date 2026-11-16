package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// startListenerTestServer wires a Server whose blockchain client is mockBlockchain,
// gives it a sync coordinator with peer-a as the current sync peer, and starts
// blockchainSubscriptionListener on a fresh subscription channel.
func startListenerTestServer(t *testing.T, mockBlockchain *blockchain.Mock) (*Server, chan *blockchain.Notification) {
	t.Helper()

	s, _ := newAnnounceTestServer(t, mockBlockchain)

	sc, _ := newTestSyncCoordinator(t)
	sc.mu.Lock()
	sc.currentSyncPeer = "peer-a"
	sc.mu.Unlock()
	s.syncCoordinator = sc

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	subscription := make(chan *blockchain.Notification, 16)
	go s.blockchainSubscriptionListener(ctx, subscription)

	return s, subscription
}

func blockNotification(hash chainhash.Hash) *blockchain.Notification {
	return &blockchain.Notification{Type: model.NotificationType_Block, Hash: hash[:]}
}

func catchupPeerFailureNotification() *blockchain.Notification {
	return &blockchain.Notification{
		Type: model.NotificationType_PeerFailure,
		Hash: (&chainhash.Hash{})[:],
		Metadata: &blockchain_api.NotificationMetadata{Metadata: map[string]string{
			"peer_id":      "peer-a",
			"failure_type": "catchup",
			"reason":       "test catchup failure",
		}},
	}
}

func currentSyncPeer(s *Server) string {
	s.syncCoordinator.mu.RLock()
	defer s.syncCoordinator.mu.RUnlock()

	return s.syncCoordinator.currentSyncPeer
}

// A Block notification whose GetBlockHeader never returns, not even when its
// context ends, must not hold back a catchup PeerFailure queued behind it. The
// peer-failure signal is what drops the failed sync peer, so if it waits behind
// the parked block announcement the node keeps syncing from a peer that has
// already failed it.
func TestBlockchainSubscriptionListener_PeerFailureNotHeldBehindParkedBlockAnnouncement(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}).Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100, SubtreesSet: true}, nil)

	s, subscription := startListenerTestServer(t, mockBlockchain)

	subscription <- blockNotification(chainhash.HashH([]byte("parked block")))

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("block announcement never reached GetBlockHeader")
	}

	subscription <- catchupPeerFailureNotification()

	require.Eventually(t, func() bool { return currentSyncPeer(s) == "" }, 2*time.Second, 10*time.Millisecond,
		"the catchup PeerFailure must clear the failed sync peer while a block announcement is parked")
}

// A Block notification whose GetBlockHeader honours its context must give up
// once notificationProcessTimeout passes, so the next announcement is processed
// instead of the worker waiting on the first one forever.
func TestBlockchainSubscriptionListener_StuckAnnouncementTimesOut(t *testing.T) {
	previous := notificationProcessTimeout
	notificationProcessTimeout = 100 * time.Millisecond
	t.Cleanup(func() { notificationProcessTimeout = previous })

	calls := make(chan chainhash.Hash, 4)

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		calls <- *args.Get(1).(*chainhash.Hash)
		<-args.Get(0).(context.Context).Done()
	}).Return(nil, nil, context.DeadlineExceeded)

	_, subscription := startListenerTestServer(t, mockBlockchain)

	first := chainhash.HashH([]byte("first block"))
	second := chainhash.HashH([]byte("second block"))
	subscription <- blockNotification(first)
	subscription <- blockNotification(second)

	for _, want := range []chainhash.Hash{first, second} {
		select {
		case got := <-calls:
			require.Equal(t, want, got)
		case <-time.After(5 * time.Second):
			t.Fatalf("announcement for %s was never started: the worker is still waiting on the one before it", want)
		}
	}
}
