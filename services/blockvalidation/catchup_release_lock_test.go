package blockvalidation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/require"
)

// blockingP2PClient is a minimal P2PClientI used to simulate a slow or hung P2P
// service. It blocks inside RecordCatchupMalicious until released, letting tests
// assert that releaseCatchupLock makes reputation gRPC calls *outside* the
// activeCatchupCtxMu lock. Only the methods exercised by releaseCatchupLock are
// overridden; the embedded interface is nil and will panic if any other method is
// called (which would itself be a useful signal that the test assumptions changed).
type blockingP2PClient struct {
	P2PClientI

	maliciousStarted chan struct{} // closed when RecordCatchupMalicious is entered
	release          chan struct{} // unblocks RecordCatchupMalicious when closed

	mu              sync.Mutex
	maliciousCalled bool
	maliciousPeer   string
	maliciousCtx    context.Context
	errorCalled     bool
	errorPeer       string
	errorMsg        string
}

func (b *blockingP2PClient) RecordCatchupMalicious(ctx context.Context, peerID string) error {
	b.mu.Lock()
	b.maliciousCalled = true
	b.maliciousPeer = peerID
	b.maliciousCtx = ctx
	b.mu.Unlock()

	close(b.maliciousStarted)
	<-b.release // simulate a hung P2P service

	return nil
}

func (b *blockingP2PClient) UpdateCatchupError(_ context.Context, peerID string, errorMsg string) error {
	b.mu.Lock()
	b.errorCalled = true
	b.errorPeer = peerID
	b.errorMsg = errorMsg
	b.mu.Unlock()

	return nil
}

// TestReleaseCatchupLock_DoesNotHoldMutexDuringReputationRPC verifies that a slow
// or hung P2P reputation gRPC call cannot hold activeCatchupCtxMu, which would
// otherwise block GetCatchupStatus and prevent the active catchup context from
// being cleared.
func TestReleaseCatchupLock_DoesNotHoldMutexDuringReputationRPC(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocker := &blockingP2PClient{
		maliciousStarted: make(chan struct{}),
		release:          make(chan struct{}),
	}
	server.p2pClient = blocker

	header := testhelpers.CreateTestHeaders(t, 1)[0]
	ctx := &CatchupContext{
		blockUpTo: &model.Block{Header: header, Height: 1000},
		baseURL:   "http://peer1",
		peerID:    "peer-123",
		startTime: time.Now(),
	}

	require.NoError(t, server.acquireCatchupLock(ctx))

	// A validation failure triggers the (blocking) reportCatchupMalicious RPC.
	relErr := error(errors.NewBlockInvalidError("bad block"))

	releaseDone := make(chan struct{})
	go func() {
		server.releaseCatchupLock(ctx, &relErr)
		close(releaseDone)
	}()

	// Wait for the hung RPC to begin. Reaching this point proves releaseCatchupLock
	// already passed the unlock (the RPC is made after activeCatchupCtxMu is released).
	select {
	case <-blocker.maliciousStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("reportCatchupMalicious was never invoked")
	}

	// While the RPC is hung, GetCatchupStatus must NOT block on activeCatchupCtxMu.
	statusDone := make(chan *CatchupStatus, 1)
	go func() {
		statusDone <- server.getCatchupStatusInternal()
	}()

	select {
	case status := <-statusDone:
		require.NotNil(t, status)
		// The active context was cleared under the lock before the RPC ran.
		require.False(t, status.IsCatchingUp)
		require.NotNil(t, status.PreviousAttempt)
		require.Equal(t, "validation_failure", status.PreviousAttempt.ErrorType)
	case <-time.After(2 * time.Second):
		t.Fatal("GetCatchupStatus blocked while P2P reputation RPC was hung — mutex held during gRPC")
	}

	// releaseCatchupLock should still be in flight, blocked on the RPC.
	select {
	case <-releaseDone:
		t.Fatal("releaseCatchupLock returned before the RPC was unblocked")
	default:
	}

	// Unblock the RPC and confirm release completes promptly.
	close(blocker.release)
	select {
	case <-releaseDone:
	case <-time.After(2 * time.Second):
		t.Fatal("releaseCatchupLock did not complete after the RPC unblocked")
	}

	// The reputation RPC must run with a bounded context, detached from neither
	// shutdown nor a timeout.
	blocker.mu.Lock()
	require.True(t, blocker.maliciousCalled, "malicious report should have been made")
	require.Equal(t, "peer-123", blocker.maliciousPeer)
	require.True(t, blocker.errorCalled, "peer error report should have been made")
	require.Equal(t, "peer-123", blocker.errorPeer)
	_, hasDeadline := blocker.maliciousCtx.Deadline()
	blocker.mu.Unlock()
	require.True(t, hasDeadline, "reputation RPC should use a bounded (timeout) context")
}

// TestReleaseCatchupLock_LocalErrorSkipsReputationRPC ensures that local system
// errors (not attributable to the peer) do not trigger reputation gRPC calls.
func TestReleaseCatchupLock_LocalErrorSkipsReputationRPC(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	blocker := &blockingP2PClient{
		maliciousStarted: make(chan struct{}),
		release:          make(chan struct{}),
	}
	server.p2pClient = blocker

	header := testhelpers.CreateTestHeaders(t, 1)[0]
	ctx := &CatchupContext{
		blockUpTo: &model.Block{Header: header, Height: 1000},
		baseURL:   "http://peer1",
		peerID:    "peer-123",
		startTime: time.Now(),
	}

	require.NoError(t, server.acquireCatchupLock(ctx))

	// ErrServiceUnavailable is a local system error — must not affect peer reputation.
	relErr := error(errors.NewServiceUnavailableError("block assembly unavailable"))
	server.releaseCatchupLock(ctx, &relErr)

	blocker.mu.Lock()
	defer blocker.mu.Unlock()
	require.False(t, blocker.maliciousCalled, "local error must not report malicious")
	require.False(t, blocker.errorCalled, "local error must not report peer error")
}

// incompleteBlockP2PClient records whether the peer-attributable incomplete-block penalty
// path (RecordCatchupFailureWithKind) fires. Only that method is overridden; any other call
// hits the nil embedded interface and panics, which would flag an unexpected penalty path.
type incompleteBlockP2PClient struct {
	P2PClientI

	mu            sync.Mutex
	failureCalled bool
	failureKind   string
}

func (c *incompleteBlockP2PClient) RecordCatchupFailureWithKind(_ context.Context, _ string, failureKind, _ string) error {
	c.mu.Lock()
	c.failureCalled = true
	c.failureKind = failureKind
	c.mu.Unlock()
	return nil
}

// TestReleaseCatchupLock_IncompleteBlockPenaltyAttribution verifies that a transient-local
// incomplete block (an unabsorbed-parent ordering gap, not the peer's fault) does NOT open a
// full-storage penalty, while a genuinely peer-attributable incomplete block DOES.
func TestReleaseCatchupLock_IncompleteBlockPenaltyAttribution(t *testing.T) {
	header := testhelpers.CreateTestHeaders(t, 1)[0]

	newCtx := func() *CatchupContext {
		return &CatchupContext{
			blockUpTo:           &model.Block{Header: header, Height: 1000},
			baseURL:             "http://peer1",
			peerID:              "peer-123",
			startTime:           time.Now(),
			incompleteBlockHash: "0000000000000000000000000000000000000000000000000000000000000001",
		}
	}

	t.Run("transient-local incomplete does not penalize", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		rec := &incompleteBlockP2PClient{}
		server.p2pClient = rec

		ctx := newCtx()
		require.NoError(t, server.acquireCatchupLock(ctx))

		relErr := error(errors.NewBlockIncompleteTransientError("transient missing-data state"))
		server.releaseCatchupLock(ctx, &relErr)

		rec.mu.Lock()
		defer rec.mu.Unlock()
		require.False(t, rec.failureCalled,
			"transient-local incomplete must not report a catchup failure / open a penalty window")
		require.NotNil(t, server.previousCatchupAttempt)
		require.Equal(t, "block_incomplete_transient", server.previousCatchupAttempt.ErrorType)
	})

	t.Run("peer-attributable incomplete penalizes", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		rec := &incompleteBlockP2PClient{}
		server.p2pClient = rec

		ctx := newCtx()
		require.NoError(t, server.acquireCatchupLock(ctx))

		relErr := error(errors.NewBlockIncompleteError("no coinbase from seeded peer"))
		server.releaseCatchupLock(ctx, &relErr)

		rec.mu.Lock()
		defer rec.mu.Unlock()
		require.True(t, rec.failureCalled,
			"peer-attributable incomplete must report a catchup failure (penalty)")
		require.Equal(t, catchupFailureKindBlockIncomplete, rec.failureKind)
		require.NotNil(t, server.previousCatchupAttempt)
		require.Equal(t, "block_incomplete", server.previousCatchupAttempt.ErrorType)
	})
}
