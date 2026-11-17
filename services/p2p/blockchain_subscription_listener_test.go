package p2p

import (
	"context"
	"sync"
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
// instead of the worker waiting on the first one forever. The second call must
// start only after the first call's context hit its deadline and the first call
// returned: that pins both the serial worker and the per-notification bound.
func TestBlockchainSubscriptionListener_StuckAnnouncementTimesOut(t *testing.T) {
	previous := notificationProcessTimeout
	notificationProcessTimeout = 100 * time.Millisecond
	t.Cleanup(func() { notificationProcessTimeout = previous })

	type headerCall struct {
		hash                 chainhash.Hash
		previousCtxErr       error
		previousCallReturned bool
	}

	calls := make(chan headerCall, 4)

	var (
		mu               sync.Mutex
		previousCtx      context.Context
		previousReturned bool
	)

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		callCtx := args.Get(0).(context.Context)

		mu.Lock()
		call := headerCall{hash: *args.Get(1).(*chainhash.Hash), previousCallReturned: previousReturned}
		if previousCtx != nil {
			call.previousCtxErr = previousCtx.Err()
		}
		previousCtx = callCtx
		previousReturned = false
		mu.Unlock()

		calls <- call

		<-callCtx.Done()

		mu.Lock()
		previousReturned = true
		mu.Unlock()
	}).Return(nil, nil, context.DeadlineExceeded)

	_, subscription := startListenerTestServer(t, mockBlockchain)

	first := chainhash.HashH([]byte("first block"))
	second := chainhash.HashH([]byte("second block"))
	subscription <- blockNotification(first)
	subscription <- blockNotification(second)

	var got []headerCall

	for _, want := range []chainhash.Hash{first, second} {
		select {
		case call := <-calls:
			require.Equal(t, want, call.hash)
			got = append(got, call)
		case <-time.After(5 * time.Second):
			t.Fatalf("announcement for %s was never started: the worker is still waiting on the one before it", want)
		}
	}

	require.ErrorIs(t, got[1].previousCtxErr, context.DeadlineExceeded,
		"the second announcement started before the first one's notificationProcessTimeout expired")
	require.True(t, got[1].previousCallReturned,
		"the second announcement started while the first GetBlockHeader was still running")
}

// When the announcement queue is full the listener must drop the new
// notification rather than block on it. A blocking send would park the
// listener behind the stuck announcement worker, and the PeerFailure sent
// after the flood would never reach its worker.
func TestBlockchainSubscriptionListener_FullAnnouncementQueueDropsInsteadOfBlocking(t *testing.T) {
	previous := announcementQueueSize
	announcementQueueSize = 2
	t.Cleanup(func() { announcementQueueSize = previous })

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		<-release
	}).Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100, SubtreesSet: true}, nil)

	s, subscription := startListenerTestServer(t, mockBlockchain)

	// One parked in the worker, two filling the queue, at least one more with
	// nowhere to go.
	for i := 0; i < announcementQueueSize+2; i++ {
		subscription <- blockNotification(chainhash.HashH([]byte{byte(i)}))
	}

	subscription <- catchupPeerFailureNotification()

	require.Eventually(t, func() bool { return currentSyncPeer(s) == "" }, 2*time.Second, 10*time.Millisecond,
		"the catchup PeerFailure must get through while the announcement queue is full")
}
