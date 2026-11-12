package subtreevalidation

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// The scale-3 incident of 2026-10-26: a 6-minute CATCHINGBLOCKS window dropped every peer subtree
// announced meanwhile, so the next block arrived with 286 subtrees unvalidated and checking it
// meant 300M txmeta reads from the UTXO store. Subtrees skipped while not RUNNING are now kept and
// validated once the node is RUNNING again.

// fsmState is the state the test blockchain client reports. Replays read it from their own
// goroutines while tests change it, so it is locked.
type fsmState struct {
	mu    sync.Mutex
	state blockchain.FSMStateType
}

func (f *fsmState) set(state blockchain.FSMStateType) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = state
}

func (f *fsmState) get() blockchain.FSMStateType {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state
}

// fsmClient reports the state in fsmState. With waitForRunning, WaitForFSMtoTransitionToGivenState
// behaves like the real client: it returns once the state is RUNNING. Otherwise the mock answers,
// and the RUNNING watcher gives up at once.
type fsmClient struct {
	*blockchain.Mock
	state          *fsmState
	waitForRunning bool
}

func (c *fsmClient) GetFSMCurrentState(context.Context) (*blockchain.FSMStateType, error) {
	state := c.state.get()
	return &state, nil
}

func (c *fsmClient) WaitForFSMtoTransitionToGivenState(ctx context.Context, target blockchain.FSMStateType) error {
	if !c.waitForRunning {
		return c.Mock.WaitForFSMtoTransitionToGivenState(ctx, target)
	}

	for c.state.get() != target {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}

	return nil
}

type replayedSubtree struct {
	hash    chainhash.Hash
	baseURL string
	peerID  string
}

func newSkippedSubtreeServer(t *testing.T, state *fsmState, max int) (*Server, chan replayedSubtree) {
	t.Helper()
	InitPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.BlocksOnly = false
	tSettings.SubtreeValidation.SkippedSubtreeBackfillMax = max

	blockchainMock := &blockchain.Mock{}
	// The RUNNING watcher gives up at once unless a test wires a real wait.
	blockchainMock.On("WaitForFSMtoTransitionToGivenState", mock.Anything, blockchain.FSMStateRUNNING).Return(errors.NewContextCanceledError("no watcher in this test")).Maybe()
	blockchainClient := &fsmClient{Mock: blockchainMock, state: state}

	replayed := make(chan replayedSubtree, 16)
	server := &Server{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		blockchainClient: blockchainClient,
	}
	server.replaySubtreeFn = func(_ context.Context, hash *chainhash.Hash, baseURL *url.URL, peerID string) error {
		replayed <- replayedSubtree{hash: *hash, baseURL: baseURL.String(), peerID: peerID}
		return nil
	}

	return server, replayed
}

func subtreeMessage(t *testing.T, hash string, peerID string) *kafka.KafkaMessage {
	t.Helper()

	value, err := proto.Marshal(&kafkamessage.KafkaSubtreeTopicMessage{Hash: hash, URL: "http://peer-" + peerID + ":8000", PeerId: peerID})
	require.NoError(t, err)

	return &kafka.KafkaMessage{Value: value}
}

const (
	skippedHashA = "d580e67e847f65c73496a9f1adafacc5f73b4ca9d44fbd0749d6d926914bdcaf"
	skippedHashB = "0000000000000000000000000000000000000000000000000000000000000001"
	skippedHashC = "0000000000000000000000000000000000000000000000000000000000000002"
)

func TestSubtreeMessageHandler_CatchingBlocks_RecordsSubtree(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, replayed := newSkippedSubtreeServer(t, fsm, 16)

	recorded := testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("recorded"))
	require.NoError(t, server.subtreeMessageHandler(context.Background())(subtreeMessage(t, skippedHashA, "p1")))

	require.Equal(t, recorded+1, testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("recorded")))
	require.Equal(t, 1, server.skippedSubtrees.len())
	require.Empty(t, replayed, "nothing is validated while not RUNNING")
}

// TestSubtreeMessageHandler_Idle_DoesNotRecord: IDLE (an operator STOP) can last days. By then
// the announcing peers may have pruned the subtrees, and a replay that cannot fetch one reports
// the peer, so subtrees announced while IDLE are dropped as before.
func TestSubtreeMessageHandler_Idle_DoesNotRecord(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateIDLE}
	server, _ := newSkippedSubtreeServer(t, fsm, 16)

	require.NoError(t, server.subtreeMessageHandler(context.Background())(subtreeMessage(t, skippedHashA, "p1")))
	require.Zero(t, server.skippedSubtrees.len())
}

func TestSubtreeMessageHandler_Running_ReplaysSkippedSubtrees(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, replayed := newSkippedSubtreeServer(t, fsm, 16)
	handler := server.subtreeMessageHandler(context.Background())

	require.NoError(t, handler(subtreeMessage(t, skippedHashA, "p1")))
	require.NoError(t, handler(subtreeMessage(t, skippedHashB, "p2")))

	fsm.set(blockchain.FSMStateRUNNING)

	// The first message handled in RUNNING starts the replay. Its own subtree goes the live way
	// (subtreesHandler), which this test does not observe.
	require.NoError(t, handler(subtreeMessage(t, skippedHashC, "p3")))

	got := map[string]replayedSubtree{}
	for len(got) < 2 {
		select {
		case r := <-replayed:
			got[r.hash.String()] = r
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of 2 skipped subtrees were replayed", len(got))
		}
	}

	require.Equal(t, "http://peer-p1:8000", got[skippedHashA].baseURL)
	require.Equal(t, "p1", got[skippedHashA].peerID)
	require.Equal(t, "p2", got[skippedHashB].peerID)
	require.Eventually(t, func() bool { return server.skippedSubtrees.len() == 0 }, time.Second, 10*time.Millisecond)
}

func TestSubtreeMessageHandler_SkippedSubtreeCapIsEnforced(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, _ := newSkippedSubtreeServer(t, fsm, 2)
	handler := server.subtreeMessageHandler(context.Background())

	evicted := testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("evicted"))
	recorded := testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("recorded"))

	require.NoError(t, handler(subtreeMessage(t, skippedHashA, "p1")))
	require.NoError(t, handler(subtreeMessage(t, skippedHashB, "p1")))
	require.NoError(t, handler(subtreeMessage(t, skippedHashB, "p2")), "a duplicate is not a new entry")
	require.NoError(t, handler(subtreeMessage(t, skippedHashC, "p1")))

	// The newest announcements are the ones the next block needs, so the oldest goes.
	require.Equal(t, []string{skippedHashB, skippedHashC}, skippedHashes(server.skippedSubtrees.take()))
	require.Equal(t, evicted+1, testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("evicted")))
	require.Equal(t, recorded+3, testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("recorded")))
}

// TestSkippedSubtrees_TakeKeepsAnnouncementOrder: parents are announced before the subtrees that
// spend from them, so the replay starts them in announcement order.
func TestSkippedSubtrees_TakeKeepsAnnouncementOrder(t *testing.T) {
	var set skippedSubtrees

	for _, hash := range []string{skippedHashC, skippedHashA, skippedHashB} {
		h, err := chainhash.NewHashFromStr(hash)
		require.NoError(t, err)
		set.record(skippedSubtree{hash: *h}, 16)
	}

	require.Equal(t, []string{skippedHashC, skippedHashA, skippedHashB}, skippedHashes(set.take()))
	require.Zero(t, set.len())
}

func skippedHashes(entries []skippedSubtree) []string {
	hashes := make([]string, 0, len(entries))
	for _, entry := range entries {
		hashes = append(hashes, entry.hash.String())
	}

	return hashes
}

// TestReplaySkippedSubtreesIfRunning: the block notification also starts the replay, so a block
// arriving before any peer subtree message still finds the skipped subtrees being validated.
func TestReplaySkippedSubtreesIfRunning(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, replayed := newSkippedSubtreeServer(t, fsm, 16)

	require.NoError(t, server.subtreeMessageHandler(context.Background())(subtreeMessage(t, skippedHashA, "p1")))

	server.replaySkippedSubtreesIfRunning(context.Background())
	require.Equal(t, 1, server.skippedSubtrees.len(), "no replay while still catching up")

	fsm.set(blockchain.FSMStateRUNNING)
	server.replaySkippedSubtreesIfRunning(context.Background())

	select {
	case r := <-replayed:
		require.Equal(t, skippedHashA, r.hash.String())
	case <-time.After(2 * time.Second):
		t.Fatal("the skipped subtree was not replayed")
	}
}

// TestReplaySkippedSubtrees_CancelledContextLaunchesNothing: on shutdown the replay stops
// instead of starting every remaining subtree.
func TestReplaySkippedSubtrees_CancelledContextLaunchesNothing(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, replayed := newSkippedSubtreeServer(t, fsm, 16)
	handler := server.subtreeMessageHandler(context.Background())

	require.NoError(t, handler(subtreeMessage(t, skippedHashA, "p1")))
	require.NoError(t, handler(subtreeMessage(t, skippedHashB, "p1")))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fsm.set(blockchain.FSMStateRUNNING)
	server.replaySkippedSubtrees(ctx)

	require.Eventually(t, func() bool { return !server.skippedSubtrees.replaying.Load() }, time.Second, 10*time.Millisecond)
	require.Empty(t, replayed)
}

// TestReplayLimit_LeavesSlotsForLiveSubtrees: the replay takes at most half the peer-subtree
// admission slots, so live subtrees keep the rest.
func TestReplayLimit_LeavesSlotsForLiveSubtrees(t *testing.T) {
	require.Equal(t, 16, replayLimit(32))
	require.Equal(t, 1, replayLimit(1))
	require.Equal(t, 1, replayLimit(2))
	require.Positive(t, replayLimit(0), "an unlimited admission gate still gets a bounded replay")
}

func TestSubtreeMessageHandler_SkippedSubtreeBackfillDisabled(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, _ := newSkippedSubtreeServer(t, fsm, 0)

	require.NoError(t, server.subtreeMessageHandler(context.Background())(subtreeMessage(t, skippedHashA, "p1")))
	require.Zero(t, server.skippedSubtrees.len())
}

func TestSubtreeMessageHandler_BlocksOnly_DoesNotRecord(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, _ := newSkippedSubtreeServer(t, fsm, 16)
	server.settings.SubtreeValidation.BlocksOnly = true

	require.NoError(t, server.subtreeMessageHandler(context.Background())(subtreeMessage(t, skippedHashA, "p1")))
	require.Zero(t, server.skippedSubtrees.len(), "a blocks-only node never validates peer subtrees, so it keeps none")
}

func TestSubtreeMessageHandler_NotRunning_MalformedIsNotRecordedOrCounted(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, _ := newSkippedSubtreeServer(t, fsm, 16)

	badHash := prometheusSubtreeKafkaMalformed.WithLabelValues("bad_hash")
	before := testutil.ToFloat64(badHash)

	require.NoError(t, server.subtreeMessageHandler(context.Background())(subtreeMessage(t, "not-a-hex-hash", "p1")))
	require.Zero(t, server.skippedSubtrees.len())
	require.Equal(t, before, testutil.ToFloat64(badHash), "the malformed metric stays a RUNNING-only signal")
}

// TestReplaySkippedSubtrees_FailureIsLoggedNotFatal: a subtree that fails on replay is dropped
// like a live one would be; the others are still replayed.
func TestReplaySkippedSubtrees_FailureIsLoggedNotFatal(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, _ := newSkippedSubtreeServer(t, fsm, 16)
	handler := server.subtreeMessageHandler(context.Background())

	require.NoError(t, handler(subtreeMessage(t, skippedHashA, "p1")))
	require.NoError(t, handler(subtreeMessage(t, skippedHashB, "p2")))

	var mu sync.Mutex
	calls := 0
	server.replaySubtreeFn = func(context.Context, *chainhash.Hash, *url.URL, string) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return errors.NewProcessingError("peer gone")
	}

	failed := testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("replay_failed"))

	fsm.set(blockchain.FSMStateRUNNING)
	server.replaySkippedSubtrees(context.Background())

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls == 2
	}, 2*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("replay_failed")) == failed+2
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return !server.skippedSubtrees.replaying.Load() }, time.Second, 10*time.Millisecond)
}

// TestSubtreesHandler_LeftRunningWhileQueued_RecordsSubtree: a subtree that waited for an
// admission slot while the node left RUNNING is kept too, not dropped.
func TestSubtreesHandler_LeftRunningWhileQueued_RecordsSubtree(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, _ := newSkippedSubtreeServer(t, fsm, 16)

	hash, err := chainhash.NewHashFromStr(skippedHashA)
	require.NoError(t, err)
	baseURL, err := url.Parse("http://peer-p1:8000")
	require.NoError(t, err)

	require.NoError(t, server.subtreesHandler(context.Background(), time.Now(), hash, baseURL, "p1"))
	require.Equal(t, 1, server.skippedSubtrees.len())
}

// TestSkippedSubtrees_TakeDropsStaleEntries: a catchup can be followed by an operator STOP that
// lasts days. By then the peers may have pruned the subtrees, and a replay that cannot fetch one
// reports the peer, so entries older than skippedSubtreeMaxAge are not replayed.
func TestSkippedSubtrees_TakeDropsStaleEntries(t *testing.T) {
	var set skippedSubtrees

	stale, err := chainhash.NewHashFromStr(skippedHashA)
	require.NoError(t, err)
	fresh, err := chainhash.NewHashFromStr(skippedHashB)
	require.NoError(t, err)

	set.record(skippedSubtree{hash: *stale}, 16)
	set.record(skippedSubtree{hash: *fresh}, 16)

	entry := set.entries[*stale]
	entry.recordedAt = time.Now().Add(-skippedSubtreeMaxAge - time.Minute)
	set.entries[*stale] = entry

	require.Equal(t, []string{skippedHashB}, skippedHashes(set.take()))
}

// TestSkippedSubtrees_ReplayStartsWhenNodeReachesRunning: the first kept subtree starts a watcher
// that replays once the node is RUNNING, with no peer subtree message or block needed. Catchup
// sends RUN only after its last block, and a quiet network may announce nothing before the next
// block, which would then be validated with the kept subtrees still unvalidated.
func TestSkippedSubtrees_ReplayStartsWhenNodeReachesRunning(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, replayed := newSkippedSubtreeServer(t, fsm, 16)

	running := make(chan struct{})
	blockchainClient := server.blockchainClient.(*fsmClient).Mock
	blockchainClient.ExpectedCalls = nil
	blockchainClient.On("WaitForFSMtoTransitionToGivenState", mock.Anything, blockchain.FSMStateRUNNING).
		Run(func(mock.Arguments) { <-running }).Return(nil).Once()

	handler := server.subtreeMessageHandler(context.Background())
	require.NoError(t, handler(subtreeMessage(t, skippedHashA, "p1")))
	require.NoError(t, handler(subtreeMessage(t, skippedHashB, "p1")), "one watcher for the whole catchup")

	select {
	case <-replayed:
		t.Fatal("replayed while still catching up")
	case <-time.After(100 * time.Millisecond):
	}

	fsm.set(blockchain.FSMStateRUNNING)
	close(running)

	got := 0
	for got < 2 {
		select {
		case <-replayed:
			got++
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of 2 kept subtrees were replayed after RUNNING", got)
		}
	}

	require.Eventually(t, func() bool { return !server.skippedSubtrees.watching.Load() }, time.Second, 10*time.Millisecond)
}

const skippedHashD = "0000000000000000000000000000000000000000000000000000000000000003"

func skippedEntry(t *testing.T, hash string, recordedAt time.Time) skippedSubtree {
	t.Helper()

	h, err := chainhash.NewHashFromStr(hash)
	require.NoError(t, err)

	baseURL, err := url.Parse("http://peer-p1:8000")
	require.NoError(t, err)

	return skippedSubtree{hash: *h, baseURL: baseURL, peerID: "p1", recordedAt: recordedAt}
}

// TestReplaySkippedSubtrees_KeptDuringReplayAreReplayedAfter: a catchup that starts and ends
// while a replay runs keeps subtrees and starts a watcher, which finds the replay running and
// gives up. The running replay must pick them up when it finishes, or they wait for the next
// peer subtree or block.
func TestReplaySkippedSubtrees_KeptDuringReplayAreReplayedAfter(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, replayed := newSkippedSubtreeServer(t, fsm, 16)
	server.blockchainClient.(*fsmClient).waitForRunning = true

	blocked := make(chan struct{})
	release := make(chan struct{})
	server.replaySubtreeFn = func(_ context.Context, hash *chainhash.Hash, baseURL *url.URL, peerID string) error {
		if hash.String() == skippedHashA {
			close(blocked)
			<-release
		}
		replayed <- replayedSubtree{hash: *hash, baseURL: baseURL.String(), peerID: peerID}
		return nil
	}

	handler := server.subtreeMessageHandler(context.Background())
	require.NoError(t, handler(subtreeMessage(t, skippedHashA, "p1")))

	fsm.set(blockchain.FSMStateRUNNING)
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("the replay of the first catchup did not start")
	}

	// A second catchup while that replay runs.
	fsm.set(blockchain.FSMStateCATCHINGBLOCKS)
	require.NoError(t, handler(subtreeMessage(t, skippedHashB, "p2")))
	fsm.set(blockchain.FSMStateRUNNING)
	require.Eventually(t, func() bool { return !server.skippedSubtrees.watching.Load() }, 2*time.Second, time.Millisecond, "the second watcher found the replay running")

	close(release)

	got := map[string]bool{}
	for len(got) < 2 {
		select {
		case r := <-replayed:
			got[r.hash.String()] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("replayed %v; the subtree kept during the replay was left queued", got)
		}
	}

	require.Eventually(t, func() bool { return server.skippedSubtrees.len() == 0 }, time.Second, time.Millisecond)
}

// TestReplaySkippedSubtrees_ExpiresWhileWaitingForAdmission: an entry can wait for an admission
// slot past skippedSubtreeMaxAge. It must not be fetched then: the peer may have pruned it, and a
// fetch that fails reports the peer.
func TestReplaySkippedSubtrees_ExpiresWhileWaitingForAdmission(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateRUNNING}
	server, replayed := newSkippedSubtreeServer(t, fsm, 16)
	server.settings.SubtreeValidation.PeerSubtreeConcurrency = 1

	// Take the only admission slot, as a live peer subtree would.
	releaseSlot, err := server.peerSubtreeAdmission.acquire(context.Background(), 1)
	require.NoError(t, err)

	server.skippedSubtrees.record(skippedEntry(t, skippedHashA, time.Now().Add(-skippedSubtreeMaxAge+300*time.Millisecond)), 16)

	expired := testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("expired"))
	server.replaySkippedSubtrees(context.Background())

	time.Sleep(600 * time.Millisecond)
	releaseSlot()

	require.Eventually(t, func() bool { return !server.skippedSubtrees.replaying.Load() }, 2*time.Second, time.Millisecond)
	require.Empty(t, replayed, "an entry past skippedSubtreeMaxAge is not fetched")
	require.Equal(t, expired+1, testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("expired")))
}

// TestReplaySkippedSubtrees_KeptAgainIsNotReplayed: a subtree that meets another catchup during
// its replay is kept again with its original announcement time, so the age limit still counts
// from the announcement, and it is not counted as replayed.
func TestReplaySkippedSubtrees_KeptAgainIsNotReplayed(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, replayed := newSkippedSubtreeServer(t, fsm, 16)

	announced := time.Now().Add(-10 * time.Minute)
	entry := skippedEntry(t, skippedHashA, announced)
	server.skippedSubtrees.record(entry, 16)

	replayedCount := testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("replayed"))
	requeued := testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("requeued"))

	server.replaySkippedSubtrees(context.Background())

	require.Eventually(t, func() bool { return !server.skippedSubtrees.replaying.Load() }, 2*time.Second, time.Millisecond)
	require.Empty(t, replayed, "nothing is validated while catching up")
	require.Equal(t, replayedCount, testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("replayed")))
	require.Equal(t, requeued+1, testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("requeued")))

	kept := server.skippedSubtrees.take()
	require.Len(t, kept, 1)
	require.True(t, kept[0].recordedAt.Equal(announced), "kept again with the original announcement time")
}

// TestReplaySkippedSubtrees_HeldLockIsNotReplayed: a quorum lock held elsewhere with no subtree
// stored yet means another validation is running, which may still fail. Only a stored subtree
// counts as replayed.
func TestReplaySkippedSubtrees_HeldLockIsNotReplayed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stored   bool
		replayed float64
	}{
		{name: "lock held, nothing stored", stored: false, replayed: 0},
		{name: "subtree stored", stored: true, replayed: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fsm := &fsmState{state: blockchain.FSMStateRUNNING}
			server, _ := newSkippedSubtreeServer(t, fsm, 16)
			server.replaySubtreeFn = nil

			quorumPath := t.TempDir()
			subtreeStore := memory.New()
			server.subtreeStore = subtreeStore

			var err error
			server.quorum, err = NewQuorum(ulogger.TestLogger{}, subtreeStore, quorumPath)
			require.NoError(t, err)

			blockIDsMap := make(map[uint32]bool)
			server.currentBlockIDsMap.Store(&blockIDsMap)
			server.bestBlockHeaderMeta.Store(&model.BlockHeaderMeta{Height: 100})

			entry := skippedEntry(t, skippedHashA, time.Now())
			if tc.stored {
				require.NoError(t, subtreeStore.Set(context.Background(), entry.hash.CloneBytes(), fileformat.FileTypeSubtree, []byte("existing")))
			} else {
				require.NoError(t, os.WriteFile(filepath.Join(quorumPath, entry.hash.String()+".lock"), nil, 0o600))
			}

			server.skippedSubtrees.record(entry, 16)

			replayedCount := testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("replayed"))
			failed := testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("replay_failed"))

			server.replaySkippedSubtrees(context.Background())

			require.Eventually(t, func() bool { return !server.skippedSubtrees.replaying.Load() }, 2*time.Second, time.Millisecond)
			require.Equal(t, replayedCount+tc.replayed, testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("replayed")))
			require.Equal(t, failed, testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("replay_failed")))
		})
	}
}

// TestSkippedSubtrees_KeptAgainKeepsAnnouncementOrder: entries kept again after a replay met
// another catchup go back by announcement time, not to the back. With the queue full, they are
// the oldest and go first, so newer announcements stay.
func TestSkippedSubtrees_KeptAgainKeepsAnnouncementOrder(t *testing.T) {
	now := time.Now()

	t.Run("full queue drops the older entries kept again", func(t *testing.T) {
		var set skippedSubtrees

		set.record(skippedEntry(t, skippedHashC, now.Add(-time.Second)), 2)
		set.record(skippedEntry(t, skippedHashD, now), 2)

		added, evicted := set.record(skippedEntry(t, skippedHashA, now.Add(-2*time.Minute)), 2)
		require.False(t, added)
		require.True(t, evicted)

		added, evicted = set.record(skippedEntry(t, skippedHashB, now.Add(-time.Minute)), 2)
		require.False(t, added)
		require.True(t, evicted)

		require.Equal(t, []string{skippedHashC, skippedHashD}, skippedHashes(set.take()))
	})

	t.Run("room left inserts by announcement time", func(t *testing.T) {
		var set skippedSubtrees

		set.record(skippedEntry(t, skippedHashC, now.Add(-time.Second)), 4)
		set.record(skippedEntry(t, skippedHashD, now), 4)
		set.record(skippedEntry(t, skippedHashB, now.Add(-time.Minute)), 4)
		set.record(skippedEntry(t, skippedHashA, now.Add(-2*time.Minute)), 4)

		require.Equal(t, []string{skippedHashA, skippedHashB, skippedHashC, skippedHashD}, skippedHashes(set.take()))
	})
}

// TestReplaySkippedSubtrees_WatcherHandoffInNarrowWindow: the second watcher finds the replay
// running and gives up, but the replay ends before that watcher has returned. The replay's new
// watcher must still start, so the watcher clears its flag before it calls the replay, not after.
func TestReplaySkippedSubtrees_WatcherHandoffInNarrowWindow(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, replayed := newSkippedSubtreeServer(t, fsm, 16)
	server.blockchainClient.(*fsmClient).waitForRunning = true

	blocked := make(chan struct{})
	release := make(chan struct{})
	server.replaySubtreeFn = func(_ context.Context, hash *chainhash.Hash, baseURL *url.URL, peerID string) error {
		if hash.String() == skippedHashA {
			close(blocked)
			<-release
		}
		replayed <- replayedSubtree{hash: *hash, baseURL: baseURL.String(), peerID: peerID}
		return nil
	}

	// Hold the second watcher right after its replay call, which finds the first replay running.
	var watchers atomic.Int32
	secondHeld := make(chan struct{})
	releaseSecond := make(chan struct{})
	server.watcherReplayCalledFn = func() {
		if watchers.Add(1) == 2 {
			close(secondHeld)
			<-releaseSecond
		}
	}
	defer close(releaseSecond)

	handler := server.subtreeMessageHandler(context.Background())
	require.NoError(t, handler(subtreeMessage(t, skippedHashA, "p1")))

	fsm.set(blockchain.FSMStateRUNNING)
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("the replay of the first catchup did not start")
	}

	// A second catchup while that replay runs.
	fsm.set(blockchain.FSMStateCATCHINGBLOCKS)
	require.NoError(t, handler(subtreeMessage(t, skippedHashB, "p2")))
	fsm.set(blockchain.FSMStateRUNNING)

	select {
	case <-secondHeld:
	case <-time.After(2 * time.Second):
		t.Fatal("the second watcher did not reach the replay")
	}

	// The first replay ends while the second watcher has not returned.
	close(release)

	got := map[string]bool{}
	for len(got) < 2 {
		select {
		case r := <-replayed:
			got[r.hash.String()] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("replayed %v; the replay's new watcher lost its flag to the second watcher", got)
		}
	}
}

// TestSubtreesHandler_AnnouncementTimeIsBeforeAdmissionWait: a live subtree kept because the node
// started catching up while it waited for an admission slot is aged from its arrival, not from
// the end of the wait.
func TestSubtreesHandler_AnnouncementTimeIsBeforeAdmissionWait(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateCATCHINGBLOCKS}
	server, _ := newSkippedSubtreeServer(t, fsm, 16)
	server.settings.SubtreeValidation.PeerSubtreeConcurrency = 1

	releaseSlot, err := server.peerSubtreeAdmission.acquire(context.Background(), 1)
	require.NoError(t, err)

	entry := skippedEntry(t, skippedHashA, time.Time{})
	arrived := time.Now()

	done := make(chan error, 1)
	go func() {
		done <- server.subtreesHandler(context.Background(), arrived, &entry.hash, entry.baseURL, entry.peerID)
	}()

	time.Sleep(300 * time.Millisecond)
	releaseSlot()
	require.NoError(t, <-done)

	kept := server.skippedSubtrees.take()
	require.Len(t, kept, 1)
	require.WithinDuration(t, arrived, kept[0].recordedAt, 100*time.Millisecond, "aged from arrival, not from the end of the admission wait")
}

// TestReplaySkippedSubtrees_IdleIsCountedAsDropped: a replayed subtree that finds the node IDLE
// (an operator STOP, or the blockchain client's cached IDLE after a broken notification stream)
// is not kept, and is counted so the outcomes still add up.
func TestReplaySkippedSubtrees_IdleIsCountedAsDropped(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateIDLE}
	server, replayed := newSkippedSubtreeServer(t, fsm, 16)

	server.skippedSubtrees.record(skippedEntry(t, skippedHashA, time.Now()), 16)

	dropped := testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("dropped"))
	server.replaySkippedSubtrees(context.Background())

	require.Eventually(t, func() bool { return !server.skippedSubtrees.replaying.Load() }, 2*time.Second, time.Millisecond)
	require.Empty(t, replayed)
	require.Zero(t, server.skippedSubtrees.len())
	require.Equal(t, dropped+1, testutil.ToFloat64(prometheusSkippedSubtrees.WithLabelValues("dropped")))
}

// TestSubtreeMessageHandler_AnnouncementTimeIsBeforeHandlerGroupWait: a subtree received while
// RUNNING can wait for a slot in the handler's goroutine group before subtreesHandler starts. If
// the node is catching up by then, it is kept aged from its receipt, not from the end of that wait.
func TestSubtreeMessageHandler_AnnouncementTimeIsBeforeHandlerGroupWait(t *testing.T) {
	fsm := &fsmState{state: blockchain.FSMStateRUNNING}
	server, _ := newSkippedSubtreeServer(t, fsm, 1024)
	server.settings.SubtreeValidation.PeerSubtreeConcurrency = 1

	// Hold the only admission slot, so every subtreesHandler blocks and fills the handler group.
	releaseSlot, err := server.peerSubtreeAdmission.acquire(context.Background(), 1)
	require.NoError(t, err)

	handler := server.subtreeMessageHandler(context.Background())

	groupLimit := max(4, runtime.NumCPU())
	for i := 0; i < groupLimit; i++ {
		require.NoError(t, handler(subtreeMessage(t, chainhash.HashH([]byte{byte(i), 1}).String(), "p1")))
	}

	waiting := chainhash.HashH([]byte("waits for the handler group"))
	received := time.Now()

	done := make(chan error, 1)
	go func() { done <- handler(subtreeMessage(t, waiting.String(), "p2")) }()

	time.Sleep(300 * time.Millisecond)
	fsm.set(blockchain.FSMStateCATCHINGBLOCKS)
	releaseSlot()
	require.NoError(t, <-done)

	require.Eventually(t, func() bool { return server.skippedSubtrees.len() == groupLimit+1 }, 5*time.Second, time.Millisecond)

	server.skippedSubtrees.mu.Lock()
	entry, ok := server.skippedSubtrees.entries[waiting]
	server.skippedSubtrees.mu.Unlock()

	require.True(t, ok)
	require.WithinDuration(t, received, entry.recordedAt, 100*time.Millisecond, "aged from receipt, not from the end of the handler group wait")
}
