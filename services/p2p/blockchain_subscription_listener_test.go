package p2p

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// startListenerTestServer wires a Server whose blockchain client is mockBlockchain,
// gives it a sync coordinator with peer-a as the current sync peer, and starts
// blockchainSubscriptionListener on a fresh subscription channel.
func startListenerTestServer(t *testing.T, mockBlockchain *blockchain.Mock) (*Server, chan *blockchain.Notification) {
	t.Helper()

	s, _, subscription := startListenerTestServerWithP2PClient(t, mockBlockchain)

	return s, subscription
}

func startListenerTestServerWithP2PClient(t *testing.T, mockBlockchain *blockchain.Mock) (*Server, *MockServerP2PClient, chan *blockchain.Notification) {
	t.Helper()

	s, p2pClient := newAnnounceTestServer(t, mockBlockchain)

	sc, _ := newTestSyncCoordinator(t)
	sc.mu.Lock()
	sc.currentSyncPeer = "peer-a"
	sc.mu.Unlock()
	s.syncCoordinator = sc

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	subscription := make(chan *blockchain.Notification, 16)
	go s.blockchainSubscriptionListener(ctx, subscription)

	return s, p2pClient, subscription
}

// droppedNotifications reads the drop counter by label name, so a test fails if
// the label names change.
func droppedNotifications(queue string, notificationType model.NotificationType) prometheus.Counter {
	initPrometheusMetrics()

	return prometheusP2PBlockchainNotificationsDropped.With(prometheus.Labels{"queue": queue, "type": notificationType.String()})
}

// A Block notification sent through the listener at the default
// notificationProcessTimeout must be announced on the block topic. The header
// read honours its context, as the gRPC client does, so a timeout that is
// already over when the work starts would stop the announcement.
func TestBlockchainSubscriptionListener_AnnouncesBlockAtDefaultTimeout(t *testing.T) {
	announced := chainhash.HashH([]byte("announced block"))
	barrier := chainhash.HashH([]byte("barrier block"))

	barrierReached := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	ready := &model.BlockHeaderMeta{Height: 100, SubtreesSet: true}

	mockBlockchain := &blockchain.Mock{}
	// A context that has already ended fails the read, as it would over gRPC.
	mockBlockchain.On("GetBlockHeader", mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() != nil }), mock.Anything).
		Return(nil, nil, context.DeadlineExceeded).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.MatchedBy(func(hash *chainhash.Hash) bool { return *hash == barrier })).
		Run(func(mock.Arguments) {
			barrierReached <- struct{}{}
			<-release
		}).Return(model.GenesisBlockHeader, ready, nil)
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Return(model.GenesisBlockHeader, ready, nil)

	s, p2pClient, subscription := startListenerTestServerWithP2PClient(t, mockBlockchain)

	subscription <- blockNotification(announced)
	// The worker is serial: once the barrier block reaches GetBlockHeader, the
	// first block's handling, publish included, has finished, and the parked
	// barrier keeps the worker from publishing anything while the test reads.
	subscription <- blockNotification(barrier)

	select {
	case <-barrierReached:
	case <-time.After(5 * time.Second):
		t.Fatal("the barrier block never reached GetBlockHeader")
	}

	published := publishedBlockMessages(p2pClient, s.blockTopicName)
	require.Len(t, published, 1, "the block sent through the listener must be announced once")
	require.Equal(t, announced.String(), published[0].Hash)
}

func blockNotification(hash chainhash.Hash) *blockchain.Notification {
	return &blockchain.Notification{Type: model.NotificationType_Block, Hash: hash[:]}
}

func catchupPeerFailureNotification() *blockchain.Notification {
	return peerFailureNotification("catchup")
}

func peerFailureNotification(failureType string) *blockchain.Notification {
	return &blockchain.Notification{
		Type: model.NotificationType_PeerFailure,
		Hash: (&chainhash.Hash{})[:],
		Metadata: &blockchain_api.NotificationMetadata{Metadata: map[string]string{
			"peer_id":      "peer-a",
			"failure_type": failureType,
			"reason":       "test " + failureType + " failure",
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

// A Block notification whose FSM skip check never returns must not hold back a
// catchup PeerFailure either. The skip check runs on the announcement worker,
// not on the listener, so the listener stays free to route the PeerFailure.
func TestBlockchainSubscriptionListener_PeerFailureNotHeldBehindParkedFSMCheck(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	running := blockchain_api.FSMStateType_RUNNING

	var parked atomic.Bool

	mockBlockchain := &blockchain.Mock{}
	// Registered before newAnnounceTestServer's RUNNING expectation, so testify
	// matches this one first.
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Run(func(mock.Arguments) {
		// Park only the first call, the Block's skip check. A later call (the
		// same Block's publish gate after release) returns at once. The
		// PeerFailure path makes no FSM call.
		if parked.CompareAndSwap(false, true) {
			entered <- struct{}{}
			<-release
		}
	}).Return(&running, nil)
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100, SubtreesSet: true}, nil).Maybe()

	s, subscription := startListenerTestServer(t, mockBlockchain)

	subscription <- blockNotification(chainhash.HashH([]byte("block with parked FSM check")))

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("block announcement never reached the FSM skip check")
	}

	subscription <- catchupPeerFailureNotification()

	require.Eventually(t, func() bool { return currentSyncPeer(s) == "" }, 2*time.Second, 10*time.Millisecond,
		"the catchup PeerFailure must clear the failed sync peer while a block's FSM skip check is parked")
}

// While the node is catching up, a Block notification must be skipped before
// GetBlockHeader is called for it. This does not pin where the skip check
// runs; PeerFailureNotHeldBehindParkedFSMCheck does that.
func TestBlockchainSubscriptionListener_SkipsBlockAnnouncementWhileCatchingUp(t *testing.T) {
	catchingBlocks := blockchain_api.FSMStateType_CATCHINGBLOCKS

	var fsmCalls, headerCalls atomic.Int32

	mockBlockchain := &blockchain.Mock{}
	// Registered before newAnnounceTestServer's RUNNING expectation, so testify
	// matches this one first.
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Run(func(mock.Arguments) {
		fsmCalls.Add(1)
	}).Return(&catchingBlocks, nil)
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		headerCalls.Add(1)
	}).Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100, SubtreesSet: true}, nil).Maybe()

	_, subscription := startListenerTestServer(t, mockBlockchain)

	// The worker is serial, so the second block's FSM check starts only after
	// the first block's handling has finished.
	subscription <- blockNotification(chainhash.HashH([]byte("first block while catching up")))
	subscription <- blockNotification(chainhash.HashH([]byte("second block while catching up")))

	require.Eventually(t, func() bool { return fsmCalls.Load() >= 2 }, 2*time.Second, 10*time.Millisecond,
		"the announcement worker never ran the FSM skip check for both blocks")
	require.Zero(t, headerCalls.Load(), "a Block notification must be skipped before GetBlockHeader while catching up")
}

// A Block notification whose GetBlockHeader honours its context must give up
// once notificationProcessTimeout passes, so the next announcement is processed
// instead of the worker waiting on the first one forever. Each call must start
// with a live context of its own, and the second must start only after the
// first call's context hit its deadline and the first call returned: that pins
// the serial worker and the per-notification bound.
func TestBlockchainSubscriptionListener_StuckAnnouncementTimesOut(t *testing.T) {
	previous := notificationProcessTimeout
	notificationProcessTimeout = 100 * time.Millisecond
	t.Cleanup(func() { notificationProcessTimeout = previous })

	type headerCall struct {
		hash                 chainhash.Hash
		ctxErrOnEntry        error
		deadline             time.Time
		hasDeadline          bool
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
		call := headerCall{
			hash:                 *args.Get(1).(*chainhash.Hash),
			ctxErrOnEntry:        callCtx.Err(),
			previousCallReturned: previousReturned,
		}
		call.deadline, call.hasDeadline = callCtx.Deadline()
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
		case <-time.After(2 * time.Second):
			t.Fatalf("announcement for %s was never started: the worker is still waiting on the one before it", want)
		}
	}

	for i, call := range got {
		require.NoError(t, call.ctxErrOnEntry, "announcement %d started with a context that had already ended", i)
		require.True(t, call.hasDeadline, "announcement %d ran without a deadline", i)
	}

	require.True(t, got[1].deadline.After(got[0].deadline),
		"the second announcement must get a fresh notificationProcessTimeout, not share the first one's deadline")

	require.ErrorIs(t, got[1].previousCtxErr, context.DeadlineExceeded,
		"the second announcement started before the first one's notificationProcessTimeout expired")
	require.True(t, got[1].previousCallReturned,
		"the second announcement started while the first GetBlockHeader was still running")
}

// When the announcement queue is full the listener must drop the new
// notification rather than block on it, and count the drop. A blocking send
// would park the listener behind the stuck announcement worker, and the
// PeerFailure sent after the flood would never reach its worker.
func TestBlockchainSubscriptionListener_FullAnnouncementQueueDropsInsteadOfBlocking(t *testing.T) {
	previous := announcementQueueSize
	announcementQueueSize = 2
	t.Cleanup(func() { announcementQueueSize = previous })

	dropped := droppedNotifications("announcement", model.NotificationType_Block)
	droppedBefore := testutil.ToFloat64(dropped)

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

	// Park the worker on the first block before sending the rest, so the queue
	// is empty when they arrive: two fill it and exactly one is dropped.
	subscription <- blockNotification(chainhash.HashH([]byte{0}))

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first block announcement never reached GetBlockHeader")
	}

	for i := 1; i <= announcementQueueSize+1; i++ {
		subscription <- blockNotification(chainhash.HashH([]byte{byte(i)}))
	}

	subscription <- catchupPeerFailureNotification()

	require.Eventually(t, func() bool { return currentSyncPeer(s) == "" }, 2*time.Second, 10*time.Millisecond,
		"the catchup PeerFailure must get through while the announcement queue is full")

	// The listener routes in order, so the drop was counted before the
	// PeerFailure was routed.
	require.Equal(t, 1.0, testutil.ToFloat64(dropped)-droppedBefore,
		"exactly the one Block notification sent to the full announcement queue must be counted as dropped")
}

// When the peer-failure queue is full, a PeerFailure is dropped and counted
// under the peer-failure queue, so a lost peer-failure signal is visible apart
// from lost announcements.
func TestBlockchainSubscriptionListener_FullPeerFailureQueueCountsDrop(t *testing.T) {
	previous := peerFailureQueueSize
	peerFailureQueueSize = 1
	t.Cleanup(func() { peerFailureQueueSize = previous })

	droppedPeerFailure := droppedNotifications("peer-failure", model.NotificationType_PeerFailure)
	droppedAsAnnouncement := droppedNotifications("announcement", model.NotificationType_PeerFailure)
	peerFailureBefore := testutil.ToFloat64(droppedPeerFailure)
	announcementBefore := testutil.ToFloat64(droppedAsAnnouncement)

	s, subscription := startListenerTestServer(t, &blockchain.Mock{})

	// HandleCatchupFailure takes decisionMu, so holding it parks the
	// peer-failure worker on whichever failure it takes first.
	s.syncCoordinator.decisionMu.Lock()
	t.Cleanup(s.syncCoordinator.decisionMu.Unlock)

	for i := 0; i < 3; i++ {
		subscription <- catchupPeerFailureNotification()
	}

	// One or two drops: one failure may still be in the queue rather than
	// parked in the worker when the third arrives.
	require.Eventually(t, func() bool { return testutil.ToFloat64(droppedPeerFailure)-peerFailureBefore >= 1 },
		2*time.Second, 10*time.Millisecond, "a PeerFailure sent to a full peer-failure queue must be counted as dropped")
	require.LessOrEqual(t, testutil.ToFloat64(droppedPeerFailure)-peerFailureBefore, 2.0)
	require.Zero(t, testutil.ToFloat64(droppedAsAnnouncement)-announcementBefore,
		"a dropped PeerFailure must not be counted under the announcement queue")
}

func TestIsCatchupPeerFailure(t *testing.T) {
	require.True(t, isCatchupPeerFailure(peerFailureNotification("catchup")))
	require.False(t, isCatchupPeerFailure(peerFailureNotification("block")))
	require.False(t, isCatchupPeerFailure(peerFailureNotification("subtree")))
	require.False(t, isCatchupPeerFailure(&blockchain.Notification{Type: model.NotificationType_PeerFailure}))
	require.False(t, isCatchupPeerFailure(blockNotification(chainhash.HashH([]byte("block")))))

	blockWithCatchupMetadata := peerFailureNotification("catchup")
	blockWithCatchupMetadata.Type = model.NotificationType_Block
	require.False(t, isCatchupPeerFailure(blockWithCatchupMetadata), "only a PeerFailure notification is a catchup failure")
}

// PeerFailure reports that do not rotate the sync peer (block, subtree) must
// not take room in the peer-failure queue, or a burst of them would push out
// the catchup failure that does.
func TestBlockchainSubscriptionListener_NonCatchupPeerFailuresBypassPeerFailureQueue(t *testing.T) {
	previous := peerFailureQueueSize
	peerFailureQueueSize = 1
	t.Cleanup(func() { peerFailureQueueSize = previous })

	dropped := droppedNotifications("peer-failure", model.NotificationType_PeerFailure)
	droppedBefore := testutil.ToFloat64(dropped)

	blockReached := make(chan struct{}, 1)

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		select {
		case blockReached <- struct{}{}:
		default:
		}
	}).Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100, SubtreesSet: true}, nil)

	s, subscription := startListenerTestServer(t, mockBlockchain)

	// Hold the peer-failure worker on the first catchup failure, so the
	// one-slot queue has no room to spare.
	s.syncCoordinator.decisionMu.Lock()
	t.Cleanup(s.syncCoordinator.decisionMu.Unlock)

	subscription <- catchupPeerFailureNotification()

	for i := 0; i < 3; i++ {
		subscription <- peerFailureNotification("block")
	}

	// The listener routes in order, so once this Block reaches GetBlockHeader
	// every failure before it has been routed.
	subscription <- blockNotification(chainhash.HashH([]byte("barrier block")))

	select {
	case <-blockReached:
	case <-time.After(5 * time.Second):
		t.Fatal("the barrier block never reached GetBlockHeader")
	}

	require.Zero(t, testutil.ToFloat64(dropped)-droppedBefore,
		"block PeerFailure reports must not be dropped from, or fill, the peer-failure queue")
}
