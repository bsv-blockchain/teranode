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

// peerFailureRecordingP2PClient records the reputation calls releaseCatchupLock makes, so a
// test can assert WHICH peer was charged for a failure.
type peerFailureRecordingP2PClient struct {
	P2PClientI

	mu              sync.Mutex
	failuresByPeer  map[string]int
	kindsByPeer     map[string][]string // every failureKind recorded per peer, in call order
	errorMsgsByPeer map[string]string
}

func newPeerFailureRecordingP2PClient() *peerFailureRecordingP2PClient {
	return &peerFailureRecordingP2PClient{
		failuresByPeer:  make(map[string]int),
		kindsByPeer:     make(map[string][]string),
		errorMsgsByPeer: make(map[string]string),
	}
}

func (r *peerFailureRecordingP2PClient) RecordCatchupFailureWithKind(_ context.Context, peerID, failureKind, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failuresByPeer[peerID]++
	r.kindsByPeer[peerID] = append(r.kindsByPeer[peerID], failureKind)

	return nil
}

func (r *peerFailureRecordingP2PClient) UpdateCatchupError(_ context.Context, peerID string, errorMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errorMsgsByPeer[peerID] = errorMsg

	return nil
}

// TestReleaseCatchupLock_ExternalErrorChargesOnlyTheFailingPeers covers issue-1368
// Defect B: an all-peers-failed subtree error used to fall through to
// "unknown_error" with isPeerError=true, charging the catchup primary for another
// peer's failure. The primary must be charged only if it actually failed, each
// failing peer must get its own error text, and the classification must be explicit.
func TestReleaseCatchupLock_ExternalErrorChargesOnlyTheFailingPeers(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	recorder := newPeerFailureRecordingP2PClient()
	server.p2pClient = recorder

	header := testhelpers.CreateTestHeaders(t, 1)[0]
	ctx := &CatchupContext{
		blockUpTo: &model.Block{Header: header, Height: 1000},
		baseURL:   "http://healthy-primary:8000",
		peerID:    "healthy-primary",
		startTime: time.Now(),
	}

	require.NoError(t, server.acquireCatchupLock(ctx))

	// Only this peer actually failed — twice, from two concurrent subtree fetches.
	server.recordCatchupPeerFailure("broken-peer", errors.NewNotFoundError("status code [404] subtree not found"))
	server.recordCatchupPeerFailure("broken-peer", errors.NewNotFoundError("status code [404] subtree not found"))

	relErr := error(errors.NewExternalError("all 2 peer attempts failed to fetch subtree abc [primary healthy-primary (http://healthy-primary:8000)=empty body; alternative broken-peer (http://broken:8000)=404]"))
	server.releaseCatchupLock(ctx, &relErr)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	require.Equal(t, 0, recorder.failuresByPeer["healthy-primary"], "the primary must not be charged for another peer's failure")
	require.Equal(t, 1, recorder.failuresByPeer["broken-peer"], "each failing peer is charged exactly once per cycle")
	require.Contains(t, recorder.errorMsgsByPeer["broken-peer"], "404", "each peer stores its own error, not another peer's")
	require.Empty(t, recorder.errorMsgsByPeer["healthy-primary"])

	status := server.getCatchupStatusInternal()
	require.NotNil(t, status.PreviousAttempt)
	require.Equal(t, "peer_data_unavailable", status.PreviousAttempt.ErrorType)
	require.Contains(t, status.PreviousAttempt.ErrorMessage, "broken-peer", "the dashboard keeps the full per-peer summary")
}

// TestReleaseCatchupLock_DrainChargesPrimaryEvenOnMixedCycle covers issue-1368
// review round 2, change 1: the drain loop is authoritative for every peer in
// failedPeers, including the primary, with no assumption about which branch
// Server.go's processCatchupChItem will take for the cycle's terminal error.
//
// An earlier version tried to skip the primary in the drain whenever the
// terminal error was a generic peer error (reportPeerErr), on the theory that
// Server.go would charge it once instead. That assumption does not hold for
// every terminal-error shape: a genuinely local failure surfacing as
// ErrServiceError falls to this switch's unknown_error default (isPeerError
// stays true, so reportPeerErr is true) while Server.go's ErrServiceError
// branch returns early WITHOUT ever reporting a failure — so the skip could
// leave a real, already-recorded subtree failure charged zero times. Charging
// the primary here unconditionally means a peer that both failed a subtree
// fetch mid-cycle AND caused the cycle's generic terminal error is charged
// twice for one attempt — an accepted tradeoff: both increments feed
// InteractionAttempts/InteractionFailures consistently (reputation math is
// unaffected), so this is telemetry precision, not a correctness break, and
// far preferable to silently dropping a genuine failure.
func TestReleaseCatchupLock_DrainChargesPrimaryEvenOnMixedCycle(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	recorder := newPeerFailureRecordingP2PClient()
	server.p2pClient = recorder

	header := testhelpers.CreateTestHeaders(t, 1)[0]
	ctx := &CatchupContext{
		blockUpTo: &model.Block{Header: header, Height: 1000},
		baseURL:   "http://primary:8000",
		peerID:    "primary-peer",
		startTime: time.Now(),
	}
	require.NoError(t, server.acquireCatchupLock(ctx))

	// The primary itself failed one subtree fetch earlier in this cycle...
	server.recordCatchupPeerFailure("primary-peer", errors.NewNotFoundError("status code [404] subtree not found"))

	// ...and the cycle's terminal error is unrelated: a generic peer error, not
	// ErrExternal/ErrBlockInvalid/ErrBlockIncomplete/local.
	relErr := error(errors.NewProcessingError("terminal generic peer error"))
	server.releaseCatchupLock(ctx, &relErr)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	require.Equal(t, 1, recorder.failuresByPeer["primary-peer"],
		"the drain charges the primary for its genuine subtree failure regardless of the cycle's terminal error")
	require.Contains(t, recorder.errorMsgsByPeer["primary-peer"], "terminal generic peer error",
		"the reportPeerErr branch overwrites the stored message with the terminal error after the drain runs")
}

// TestReleaseCatchupLock_LocalTerminalErrorStillChargesFailedPrimary covers
// issue-1368 review round 2, change 1's under-charge regression: the primary
// genuinely failed a subtree fetch mid-cycle (recorded in failedPeers), and the
// cycle's terminal error is the exact ErrServiceError-coded shape
// fetchAndStoreSubtreeAndSubtreeData/fetchSubtreeDataForBlock produce for a
// genuinely LOCAL failure (get_blocks.go's "Local error fetching subtree ...
// not retrying with other peers", re-wrapped by fetchSubtreeDataForBlock).
// releaseCatchupLock's switch has no specific case for generic ErrServiceError
// (only the narrower ErrServiceUnavailable), so this falls to the
// unknown_error default with isPeerError left true — and Server.go's
// ErrServiceError branch returns early WITHOUT ever calling
// reportCatchupFailureForError. The drain loop is therefore the ONLY place
// this cycle's genuine primary failure can be recorded; it must not be
// skipped down to zero.
func TestReleaseCatchupLock_LocalTerminalErrorStillChargesFailedPrimary(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	recorder := newPeerFailureRecordingP2PClient()
	server.p2pClient = recorder

	header := testhelpers.CreateTestHeaders(t, 1)[0]
	ctx := &CatchupContext{
		blockUpTo: &model.Block{Header: header, Height: 1000},
		baseURL:   "http://primary:8000",
		peerID:    "primary-peer",
		startTime: time.Now(),
	}
	require.NoError(t, server.acquireCatchupLock(ctx))

	// The primary genuinely failed a subtree fetch earlier in this cycle.
	server.recordCatchupPeerFailure("primary-peer", errors.NewNotFoundError("status code [404] subtree not found"))

	// The cycle's terminal error mirrors the production local-error wrap chain:
	// fetchAndStoreSubtreeAndSubtreeData's local-error ServiceError, re-wrapped by
	// fetchSubtreeDataForBlock's ServiceError.
	localErr := errors.NewStorageError("disk full")
	innerErr := errors.NewServiceError("[catchup:fetchAndStoreSubtreeAndSubtreeData] Local error fetching subtree abc (not retrying with other peers)", localErr)
	relErr := error(errors.NewServiceError("[catchup:fetchSubtreeDataForBlock] Failed to fetch subtree data for block bb", innerErr))

	require.True(t, errors.Is(relErr, errors.ErrServiceError), "precondition: this must be the ErrServiceError-coded shape Server.go treats as local infra")

	server.releaseCatchupLock(ctx, &relErr)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	require.Equal(t, 1, recorder.failuresByPeer["primary-peer"],
		"the primary must be charged exactly once for its genuine subtree failure, even though the terminal error is local-infra-coded and Server.go's ErrServiceError branch never reports it")
}

// TestReleaseCatchupLock_IncompleteBlockDoesNotDoubleChargeFailedPrimary covers
// issue-1368 review round 2, change 2: the primary genuinely failed a subtree
// fetch mid-cycle (recorded in failedPeers), and the cycle's terminal error is a
// peer-attributable incomplete block from that SAME primary. Unlike the
// reportPeerErr case, this guard is safe: both the generic drain charge and the
// kind-specific incomplete-block charge live in this same function, so there is
// no cross-file assumption about what Server.go will do. The primary must be
// charged exactly once, with the specific catchupFailureKindBlockIncomplete
// kind (which drives the documented incomplete-block penalty window), not
// twice; its subtree-level error message must still be stored even though the
// generic counter call is skipped.
func TestReleaseCatchupLock_IncompleteBlockDoesNotDoubleChargeFailedPrimary(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	recorder := newPeerFailureRecordingP2PClient()
	server.p2pClient = recorder

	header := testhelpers.CreateTestHeaders(t, 1)[0]
	ctx := &CatchupContext{
		blockUpTo:           &model.Block{Header: header, Height: 1000},
		baseURL:             "http://primary:8000",
		peerID:              "primary-peer",
		startTime:           time.Now(),
		incompleteBlockHash: "0000000000000000000000000000000000000000000000000000000000000001",
	}
	require.NoError(t, server.acquireCatchupLock(ctx))

	// The primary genuinely failed a subtree fetch earlier in this cycle.
	server.recordCatchupPeerFailure("primary-peer", errors.NewNotFoundError("status code [404] subtree not found"))

	relErr := error(errors.NewBlockIncompleteError("no coinbase from seeded peer"))
	server.releaseCatchupLock(ctx, &relErr)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	require.Equal(t, 1, recorder.failuresByPeer["primary-peer"],
		"the primary must be charged exactly once, not twice, for the same cycle")
	require.Equal(t, []string{catchupFailureKindBlockIncomplete}, recorder.kindsByPeer["primary-peer"],
		"the one charge must be the kind-specific incomplete-block penalty, not the drain's generic kind")
	require.Contains(t, recorder.errorMsgsByPeer["primary-peer"], "404",
		"the primary's subtree-level error message must still be stored even though its generic counter charge is skipped")
}

// TestRecordCatchupPeerFailure_SkipsLocalErrors covers review finding 1 on
// issue-1368 Defect B: a local failure — a context cancellation (catchup abort /
// peer switch / shutdown landing mid-retry) or a local storage error — is ours, not
// the peer's, and must never land an innocent peer in failedPeers. The guard lives
// in recordCatchupPeerFailure itself so both call sites in tryPeerForSubtree
// (the non-retryable branch and the cache-bypass-retry tail) get it for free.
func TestRecordCatchupPeerFailure_SkipsLocalErrors(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	header := testhelpers.CreateTestHeaders(t, 1)[0]
	ctx := &CatchupContext{
		blockUpTo: &model.Block{Header: header, Height: 1000},
		baseURL:   "http://peer1",
		peerID:    "peer-123",
		startTime: time.Now(),
	}
	require.NoError(t, server.acquireCatchupLock(ctx))
	defer func() {
		var relErr error
		server.releaseCatchupLock(ctx, &relErr)
	}()

	recorded := func(peerID string) bool {
		ctx.failedPeersMu.Lock()
		defer ctx.failedPeersMu.Unlock()
		_, ok := ctx.failedPeers[peerID]
		return ok
	}

	t.Run("context-cancelled error is not recorded", func(t *testing.T) {
		server.recordCatchupPeerFailure("innocent-peer-ctx", context.Canceled)
		require.False(t, recorded("innocent-peer-ctx"), "a context cancellation is our failure, not the peer's")
	})

	t.Run("storage error is not recorded", func(t *testing.T) {
		server.recordCatchupPeerFailure("innocent-peer-storage", errors.NewStorageError("disk full"))
		require.False(t, recorded("innocent-peer-storage"), "a local storage failure is ours, not the peer's")
	})

	t.Run("genuine peer error is still recorded", func(t *testing.T) {
		server.recordCatchupPeerFailure("broken-peer-genuine", errors.NewNotFoundError("status code [404]"))
		require.True(t, recorded("broken-peer-genuine"), "a genuine peer failure must still be recorded")
	})
}
