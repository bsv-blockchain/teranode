package p2p

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
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
func droppedNotifications(queue string, notificationType model.NotificationType, reason string) prometheus.Counter {
	initPrometheusMetrics()

	return prometheusP2PBlockchainNotificationsDropped.With(prometheus.Labels{"queue": queue, "type": notificationType.String(), "reason": reason})
}

// The tests shrink these values for themselves, so pin the shipped defaults
// here: a short timeout or a zero queue would break the fix in production.
func TestBlockchainSubscriptionListener_Defaults(t *testing.T) {
	require.Equal(t, 30*time.Second, notificationProcessTimeout)
	require.Equal(t, 1_000, announcementQueueSize)
	require.Equal(t, 64, peerFailureQueueSize)
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
		// Park only the first call, the Block's skip check. Any later call
		// returns at once.
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

	timedOut := droppedNotifications("announcement", model.NotificationType_Block, "timeout")
	timedOutBefore := testutil.ToFloat64(timedOut)

	type headerCall struct {
		hash                 chainhash.Hash
		ctxErrOnEntry        error
		deadline             time.Time
		hasDeadline          bool
		previousCtxErr       error
		previousCallReturned bool
	}

	calls := make(chan headerCall, 4)

	first := chainhash.HashH([]byte("first block"))
	second := chainhash.HashH([]byte("second block"))

	// The second call parks until cleanup instead of timing out, so the
	// timeout count read below covers the first announcement only.
	releaseSecond := make(chan struct{})
	t.Cleanup(func() { close(releaseSecond) })

	var (
		mu               sync.Mutex
		previousCtx      context.Context
		previousReturned bool
	)

	var (
		fsmDeadlines       []time.Time
		fsmWithoutDeadline int
	)

	running := blockchain_api.FSMStateType_RUNNING

	mockBlockchain := &blockchain.Mock{}
	// Registered before newAnnounceTestServer's RUNNING expectation, so testify
	// matches this one first. It records the deadline the FSM skip check runs
	// under, which must come from the notification's bound.
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Run(func(args mock.Arguments) {
		deadline, ok := args.Get(0).(context.Context).Deadline()

		mu.Lock()
		fsmDeadlines = append(fsmDeadlines, deadline)
		if !ok {
			fsmWithoutDeadline++
		}
		mu.Unlock()
	}).Return(&running, nil)
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

		if call.hash == second {
			<-releaseSecond
		} else {
			<-callCtx.Done()
		}

		mu.Lock()
		previousReturned = true
		mu.Unlock()
		// An error that is not a context error, as a gRPC call returns, so only
		// a check on the notification's own deadline counts it as a timeout.
	}).Return(nil, nil, errors.NewServiceError("rpc call ran out of time"))

	_, subscription := startListenerTestServer(t, mockBlockchain)

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

	// currentFSMState adds its own 5 s bound, so a skip check on the service
	// context would get a deadline seconds past the notification's.
	mu.Lock()
	require.NotEmpty(t, fsmDeadlines, "the FSM skip check never ran")
	require.Zero(t, fsmWithoutDeadline, "the FSM skip check ran without a deadline")
	for _, deadline := range fsmDeadlines {
		require.False(t, deadline.After(got[1].deadline),
			"the FSM skip check must run under the notification's notificationProcessTimeout")
	}
	mu.Unlock()

	// The worker counts the first announcement's timeout before it takes the
	// second, and the second is parked.
	require.Equal(t, 1.0, testutil.ToFloat64(timedOut)-timedOutBefore,
		"the announcement abandoned at notificationProcessTimeout must be counted as a timeout")
}

// When the announcement queue is full the listener must drop the new
// notification rather than block on it, and count the drop. A blocking send
// would park the listener behind the stuck announcement worker, and the
// PeerFailure sent after the flood would never reach its worker.
func TestBlockchainSubscriptionListener_FullAnnouncementQueueDropsInsteadOfBlocking(t *testing.T) {
	previous := announcementQueueSize
	announcementQueueSize = 2
	t.Cleanup(func() { announcementQueueSize = previous })

	dropped := droppedNotifications("announcement", model.NotificationType_Block, "queue_full")
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

	droppedPeerFailure := droppedNotifications("peer-failure", model.NotificationType_PeerFailure, "queue_full")
	droppedAsAnnouncement := droppedNotifications("announcement", model.NotificationType_PeerFailure, "queue_full")
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

	dropped := droppedNotifications("peer-failure", model.NotificationType_PeerFailure, "queue_full")
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

// An announcement that fails with an ordinary error, well inside its bound,
// must not be counted as a timeout.
func TestBlockchainSubscriptionListener_NonTimeoutErrorIsNotCountedAsTimeout(t *testing.T) {
	timedOut := droppedNotifications("announcement", model.NotificationType_Block, "timeout")
	timedOutBefore := testutil.ToFloat64(timedOut)

	failing := chainhash.HashH([]byte("block whose header read fails"))
	barrier := chainhash.HashH([]byte("barrier block"))

	barrierReached := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.MatchedBy(func(hash *chainhash.Hash) bool { return *hash == failing })).
		Return(nil, nil, errors.NewProcessingError("block header not found"))
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.MatchedBy(func(hash *chainhash.Hash) bool { return *hash == barrier })).
		Run(func(mock.Arguments) {
			barrierReached <- struct{}{}
			<-release
		}).Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100, SubtreesSet: true}, nil)

	_, subscription := startListenerTestServer(t, mockBlockchain)

	subscription <- blockNotification(failing)
	// The worker is serial: once the barrier reaches GetBlockHeader, the failed
	// announcement has been fully handled.
	subscription <- blockNotification(barrier)

	select {
	case <-barrierReached:
	case <-time.After(5 * time.Second):
		t.Fatal("the barrier block never reached GetBlockHeader")
	}

	require.Zero(t, testutil.ToFloat64(timedOut)-timedOutBefore,
		"an announcement that failed for another reason must not be counted as a timeout")
}

// Cancelling the service context must end an announcement in flight at once,
// not leave it running until its notificationProcessTimeout.
func TestBlockchainSubscriptionListener_ShutdownCancelsInFlightAnnouncement(t *testing.T) {
	timedOut := droppedNotifications("announcement", model.NotificationType_Block, "timeout")
	timedOutBefore := testutil.ToFloat64(timedOut)

	entered := make(chan struct{}, 1)
	ended := make(chan error, 1)

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		callCtx := args.Get(0).(context.Context)
		entered <- struct{}{}
		<-callCtx.Done()
		ended <- callCtx.Err()
	}).Return(nil, nil, context.Canceled)

	s, _ := newAnnounceTestServer(t, mockBlockchain)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	subscription := make(chan *blockchain.Notification, 1)
	go s.blockchainSubscriptionListener(ctx, subscription)

	subscription <- blockNotification(chainhash.HashH([]byte("block in flight at shutdown")))

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("block announcement never reached GetBlockHeader")
	}

	cancel()

	select {
	case err := <-ended:
		require.ErrorIs(t, err, context.Canceled, "the announcement must end because the service stopped, not because its bound passed")
	case <-time.After(2 * time.Second):
		t.Fatal("the announcement in flight kept running after the service context was cancelled")
	}

	require.Never(t, func() bool { return testutil.ToFloat64(timedOut)-timedOutBefore != 0 }, 200*time.Millisecond, 10*time.Millisecond,
		"an announcement cancelled by shutdown must not be counted as a timeout")
}

// Only an announcement that fails after its own deadline is a timeout. An inner
// call that reports DeadlineExceeded while the bound still has time left, and
// an announcement that succeeds after its deadline, are not counted.
func TestBlockchainSubscriptionListener_OnlyFailuresPastTheDeadlineCountAsTimeouts(t *testing.T) {
	previous := notificationProcessTimeout
	notificationProcessTimeout = 50 * time.Millisecond
	t.Cleanup(func() { notificationProcessTimeout = previous })

	timedOut := droppedNotifications("announcement", model.NotificationType_Block, "timeout")
	timedOutBefore := testutil.ToFloat64(timedOut)

	innerDeadline := chainhash.HashH([]byte("inner call's own deadline"))
	lateSuccess := chainhash.HashH([]byte("succeeds after the bound"))
	barrier := chainhash.HashH([]byte("barrier block"))

	isHash := func(want chainhash.Hash) interface{} {
		return mock.MatchedBy(func(hash *chainhash.Hash) bool { return *hash == want })
	}

	ready := &model.BlockHeaderMeta{Height: 100, SubtreesSet: true}
	barrierReached := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, isHash(innerDeadline)).
		Return(nil, nil, context.DeadlineExceeded)
	mockBlockchain.On("GetBlockHeader", mock.Anything, isHash(lateSuccess)).Run(func(args mock.Arguments) {
		<-args.Get(0).(context.Context).Done()
	}).Return(model.GenesisBlockHeader, ready, nil)
	mockBlockchain.On("GetBlockHeader", mock.Anything, isHash(barrier)).Run(func(mock.Arguments) {
		barrierReached <- struct{}{}
		<-release
	}).Return(model.GenesisBlockHeader, ready, nil)

	_, subscription := startListenerTestServer(t, mockBlockchain)

	subscription <- blockNotification(innerDeadline)
	subscription <- blockNotification(lateSuccess)
	// The worker is serial: once the barrier reaches GetBlockHeader, both
	// announcements before it have been fully handled.
	subscription <- blockNotification(barrier)

	select {
	case <-barrierReached:
	case <-time.After(5 * time.Second):
		t.Fatal("the barrier block never reached GetBlockHeader")
	}

	require.Zero(t, testutil.ToFloat64(timedOut)-timedOutBefore,
		"neither an inner DeadlineExceeded inside the bound nor a late success is a timeout")
}
