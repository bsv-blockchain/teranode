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

// TestReleaseCatchupLock_StorageErrorIsLocalNotPeer covers the peer-attribution
// hole left open by issue 1439's reclassification: a torn, stale or mis-keyed
// external transaction blob is now correctly an ErrStorageError, but the switch
// in releaseCatchupLock had no case for it, so it reached the unknown_error
// default with isPeerError left true and charged the primary for this node's own
// disk fault.
//
// The truncated-blob variant was worse than a bare default. Its wrap chain
// contains "unexpected EOF", and errors.IsNetworkError falls back to substring
// matching that includes "eof", so it was actively labelled a network error
// against the peer — which is why the new case has to sit above IsNetworkError.
func TestReleaseCatchupLock_StorageErrorIsLocalNotPeer(t *testing.T) {
	header := testhelpers.CreateTestHeaders(t, 1)[0]

	cases := []struct {
		name string
		err  error
	}{
		{
			name: "mis-keyed blob",
			err:  errors.NewStorageError("[GetTxFromExternalStore][abc] external tx does not hash to the key it was stored under (got def) — stale, rotted or mis-keyed blob"),
		},
		{
			// Must not be classified network_error by the IsNetworkError substring
			// fallback, which matches "eof".
			name: "truncated blob surfacing as unexpected EOF",
			err:  errors.NewStorageError("[GetTxFromExternalStore][abc] could not read tx from stream", errors.NewProcessingError("unexpected EOF")),
		},
		{
			// The laundering shape: a storage fault wrapped in a consensus code, which
			// is what the aerospike store produced for its own bins' read failures.
			// errors.Is walks the whole chain, so both codes match — and the switch
			// used to test the consensus case first, scoring this a validation_failure
			// and flagging an honest peer for our own disk. Pins the case ordering.
			name: "storage fault wrapped in a consensus code",
			err:  errors.NewTxInvalidError("could not process utxos", errors.NewStorageError("failed to get extra record")),
		},
		{
			// Same shape from the other direction: the block-level consensus code.
			name: "storage fault wrapped in a block-invalid code",
			err:  errors.NewBlockInvalidError("block failed validation", errors.NewStorageError("failed to get block ID")),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server, _, _, cleanup := setupTestCatchupServer(t)
			defer cleanup()

			recorder := newPeerFailureRecordingP2PClient()
			server.p2pClient = recorder

			ctx := &CatchupContext{
				blockUpTo: &model.Block{Header: header, Height: 1000},
				baseURL:   "http://honest-primary:8000",
				peerID:    "honest-primary",
				startTime: time.Now(),
			}
			require.NoError(t, server.acquireCatchupLock(ctx))

			relErr := tc.err
			server.releaseCatchupLock(ctx, &relErr)

			recorder.mu.Lock()
			defer recorder.mu.Unlock()

			// The malicious report is the assertion that actually distinguishes the
			// fix from its absence. Revert the storage case and the two
			// wrapped-consensus rows below fall through to the consensus case,
			// which sets reportMalicious and flags an honest peer for our own
			// disk. failuresByPeer, by contrast, only moves when ctx.failedPeers is
			// non-empty, which it is not here, so on its own it cannot fail —
			// review caught that; it is kept as a second, weaker guard.
			require.Zero(t, recorder.maliciousByPeer["honest-primary"],
				"an honest peer must never be flagged malicious for a local storage fault")
			require.Equal(t, 0, recorder.failuresByPeer["honest-primary"],
				"a local storage fault must never be charged to the serving peer")
			require.Empty(t, recorder.errorMsgsByPeer["honest-primary"])

			require.NotNil(t, server.previousCatchupAttempt)
			require.Equal(t, "local_storage_fault", server.previousCatchupAttempt.ErrorType,
				"must be classified as a local fault, not network_error or unknown_error")
		})
	}
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
	maliciousByPeer map[string]int
}

func newPeerFailureRecordingP2PClient() *peerFailureRecordingP2PClient {
	return &peerFailureRecordingP2PClient{
		failuresByPeer:  make(map[string]int),
		kindsByPeer:     make(map[string][]string),
		errorMsgsByPeer: make(map[string]string),
		maliciousByPeer: make(map[string]int),
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

// RecordCatchupMalicious must be implemented rather than left to the embedded nil
// P2PClientI: the storage-fault cases below assert that no malicious report is
// made, and a nil-interface panic would "fail" those tests for the wrong reason
// while telling a reader nothing about which peer was flagged.
func (r *peerFailureRecordingP2PClient) RecordCatchupMalicious(_ context.Context, peerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maliciousByPeer[peerID]++

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
// (only the narrower ErrServiceUnavailable). IsLocalError admits two shapes —
// a context error and an ErrStorageError — and this fixture uses the storage
// one, which is what most of get_blocks.go's own subtree store reads and writes
// are coded as (fetchAndStoreSubtreeData's Exists check is the exception, still
// ErrProcessing). Since issue 1439 that lands the terminal error in the
// local_storage_fault case rather than the unknown_error default, and leaves
// isPeerError false rather than true.
//
// Either way nothing outside the drain CHARGES the primary. Post-1439
// reportPeerErr is not set at all; pre-1439 it was, but its only effect is
// reportCatchupError, which stores the error text for the dashboard and does
// not touch the failure counters. And Server.go's local-infra branch returns
// early WITHOUT ever calling reportCatchupFailureForError (it now matches this
// fixture on ErrStorageError as well as ErrServiceError). The drain loop is
// therefore the ONLY place this cycle's genuine primary failure can be
// recorded; it must not be skipped down to zero.
//
// (The unknown_error + isPeerError=true combination this test originally
// exercised is still covered by DrainChargesPrimaryEvenOnMixedCycle above,
// whose terminal error carries no storage link.)
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

// TestReleaseCatchupLock_HeaderContextErrorIsNotChargedToPeer pins the classification that keeps
// an honest peer out of the demotion loop of issue 1368.
//
// When the parent-header run OUR OWN store returned cannot carry the median-time-past window
// (issue 1467), the peer that served the block had no part in producing it. The switch in
// releaseCatchupLock therefore classifies it local and leaves isPeerError false, and 9bd669e78
// moved that case ABOVE errors.IsNetworkError to keep it there. Nothing pinned either fact.
//
// The ordering is the fragile half. errors.IsNetworkError matches a bare substring — "http",
// "eof", "timeout" — anywhere in the whole chain, so if this switch is ever re-sorted, or if any
// wrapper puts a peer baseURL into the message, a purely local failure is reclassified
// network_error, isPeerError flips true, and the serving peer is charged for our own state.
// The second subtest is the one that catches that: its message carries a peer URL, so it passes
// only while the header-context case precedes IsNetworkError.
func TestReleaseCatchupLock_HeaderContextErrorIsNotChargedToPeer(t *testing.T) {
	tests := []struct {
		name string
		err  func() error
	}{
		{
			name: "bare header-context error",
			err: func() error {
				return errors.NewBlockHeaderContextError("window is not anchored at the block's parent")
			},
		},
		{
			// The re-sort canary: "http" in the message text is all IsNetworkError needs.
			name: "wrapped in a message carrying a peer URL",
			err: func() error {
				return errors.NewProcessingError(
					"[catchup] failed validating block from http://peer1:8000",
					errors.NewBlockHeaderContextError("window is not anchored at the block's parent"),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _, _, cleanup := setupTestCatchupServer(t)
			defer cleanup()

			blocker := &blockingP2PClient{
				maliciousStarted: make(chan struct{}),
				release:          make(chan struct{}),
			}
			server.p2pClient = blocker

			header := testhelpers.CreateTestHeaders(t, 1)[0]
			catchupCtx := &CatchupContext{
				blockUpTo: &model.Block{Header: header, Height: 1000},
				baseURL:   "http://peer1:8000",
				peerID:    "peer-123",
				startTime: time.Now(),
			}

			require.NoError(t, server.acquireCatchupLock(catchupCtx))

			relErr := tt.err()
			server.releaseCatchupLock(catchupCtx, &relErr)

			status := server.getCatchupStatusInternal()
			require.NotNil(t, status.PreviousAttempt)
			require.Equal(t, "local_header_context_error", status.PreviousAttempt.ErrorType,
				"a run our own store produced must never be classified as the peer's fault")

			blocker.mu.Lock()
			defer blocker.mu.Unlock()
			require.False(t, blocker.maliciousCalled, "a local header-context failure must not report malicious")
			require.False(t, blocker.errorCalled, "a local header-context failure must not charge the peer")
		})
	}
}

// TestReleaseCatchupLock_ContextErrorIsNotChargedToPeer pins that a shutdown or
// a catchup-context deadline is never the serving peer's fault.
//
// There was no case for a context error in the classification switch at all, so
// it fell past every branch to "unknown_error" with isPeerError left true, and
// the honest primary was charged for our own shutdown. recordCatchupPeerFailure
// has always exempted it (via IsLocalError); this makes the terminal path agree.
func TestReleaseCatchupLock_ContextErrorIsNotChargedToPeer(t *testing.T) {
	header := testhelpers.CreateTestHeaders(t, 1)[0]

	cases := []struct {
		name string
		err  error
	}{
		{name: "cancelled", err: errors.NewContextCanceledError("catchup context cancelled")},
		{
			// Deadline text contains no network token, but must be exempt on class
			// rather than by luck.
			name: "deadline exceeded",
			err:  errors.NewContextCanceledError("context deadline exceeded while catching up"),
		},
		{
			// No context code anywhere in the chain, only the stdlib sentinel and
			// its rendered text. This is what the text-matched case below ErrExternal
			// exists to catch; the code-matched case at the top of the switch cannot.
			name: "uncoded stdlib deadline",
			err:  errors.NewProcessingError("catchup stalled", context.DeadlineExceeded),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server, _, _, cleanup := setupTestCatchupServer(t)
			defer cleanup()

			recorder := newPeerFailureRecordingP2PClient()
			server.p2pClient = recorder

			ctx := &CatchupContext{
				blockUpTo: &model.Block{Header: header, Height: 1000},
				baseURL:   "http://honest-primary:8000",
				peerID:    "honest-primary",
				startTime: time.Now(),
			}
			require.NoError(t, server.acquireCatchupLock(ctx))

			relErr := tc.err
			server.releaseCatchupLock(ctx, &relErr)

			recorder.mu.Lock()
			defer recorder.mu.Unlock()

			require.Equal(t, 0, recorder.failuresByPeer["honest-primary"],
				"our own context cancellation must never be charged to the serving peer")

			require.NotNil(t, server.previousCatchupAttempt)
			require.Equal(t, "local_context_cancelled", server.previousCatchupAttempt.ErrorType,
				"must not fall through to unknown_error, which leaves isPeerError true")
		})
	}
}

// TestReleaseCatchupLock_PeerFetchDeadlineKeepsPeerLabel pins the ordering fix for
// the context case: it must not take a label that a code-matched case below it owns.
//
// errors.IsContextError falls back to a substring match over the rendered chain, so
// while it sat at the top of the switch it swallowed anything whose text merely
// CONTAINED "context deadline exceeded". fetchSubtreeFromPeer wraps a failed peer
// fetch as a ServiceError naming the peer URL, and the all-peers-failed roll-up
// wraps those in ErrExternal, so a stalled peer was reported as this node's own
// shutdown and issue 1368's peer_data_unavailable label went missing from the
// dashboard exactly when it was needed.
//
// The impact is telemetry, not reputation: both cases set isPeerError false. The
// labels are the product.
func TestReleaseCatchupLock_PeerFetchDeadlineKeepsPeerLabel(t *testing.T) {
	header := testhelpers.CreateTestHeaders(t, 1)[0]

	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	recorder := newPeerFailureRecordingP2PClient()
	server.p2pClient = recorder

	ctx := &CatchupContext{
		blockUpTo: &model.Block{Header: header, Height: 1000},
		baseURL:   "http://honest-primary:8000",
		peerID:    "honest-primary",
		startTime: time.Now(),
	}
	require.NoError(t, server.acquireCatchupLock(ctx))

	// The shape fetchSubtreeFromPeer produces, rolled up the way the all-peers-failed
	// path rolls it up. The context text is real and unavoidable: it is what an HTTP
	// client returns when the request deadline fires against a slow peer.
	relErr := error(errors.NewExternalError("all peers failed to supply subtree",
		errors.NewServiceError("failed to fetch subtree from http://slow-peer:8000",
			context.DeadlineExceeded)))
	server.releaseCatchupLock(ctx, &relErr)

	require.NotNil(t, server.previousCatchupAttempt)
	require.Equal(t, "peer_data_unavailable", server.previousCatchupAttempt.ErrorType,
		"a peer's fetch deadline must keep the peer label, not be relabelled as our own cancellation")

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	require.Equal(t, 0, recorder.failuresByPeer["honest-primary"],
		"peer_data_unavailable must still not charge the catchup primary for another peer's failure")
}

// TestShouldReportConsensusMalicious pins the predicate that validateBlocksOnChannel
// uses to decide whether a failed block validation is chargeable to the serving
// peer. The cases that matter are the both-codes chains: errors.Is walks the whole
// wrap chain, so a consensus code sitting outside a storage fault must not be read
// as proof about the peer. releaseCatchupLock scores those same chains
// local_storage_fault, and the two must not disagree.
func TestShouldReportConsensusMalicious(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "bare block-invalid is the peer's fault",
			err:  errors.NewBlockInvalidError("block does not meet target difficulty"),
			want: true,
		},
		{
			name: "bare tx-invalid is the peer's fault",
			err:  errors.NewTxInvalidError("previous tx has no output at index 3"),
			want: true,
		},
		{
			name: "block-invalid wrapping a storage fault is ours, not the peer's",
			err: errors.NewBlockInvalidError("block is not valid",
				errors.NewStorageError("external blob does not hash to its key")),
			want: false,
		},
		{
			name: "tx-invalid wrapping a storage fault is ours, not the peer's",
			err: errors.NewTxInvalidError("invalid tx",
				errors.NewStorageError("could not read input")),
			want: false,
		},
		{
			name: "storage fault nested two deep is still ours",
			err: errors.NewBlockInvalidError("block contains invalid transactions",
				errors.NewTxInvalidError("invalid tx",
					errors.NewStorageError("invalid version"))),
			want: false,
		},
		{
			name: "bare storage fault carries no consensus code at all",
			err:  errors.NewStorageError("external blob does not hash to its key"),
			want: false,
		},
		{
			name: "an incomplete block is handled elsewhere and is not chargeable here",
			err:  errors.NewBlockIncompleteError("no coinbase from seeded peer"),
			want: false,
		},
		{
			// The codes below are why this delegates to isLocalCatchupFault rather
			// than naming ErrStorageError. Both sibling classifiers exempt them, so
			// screening only storage here would leave one code on which this file
			// still reaches two opposite verdicts, on the reputation call.
			name: "block-invalid wrapping a service-unavailable is ours, not the peer's",
			err: errors.NewBlockInvalidError("block is not valid",
				errors.NewServiceUnavailableError("aerospike batch timed out")),
			want: false,
		},
		{
			name: "tx-invalid wrapping a storage-unavailable is ours, not the peer's",
			err: errors.NewTxInvalidError("invalid tx",
				errors.NewStorageUnavailableError("blob store is unhealthy")),
			want: false,
		},
		{
			name: "block-invalid wrapping our own context cancellation is ours",
			err: errors.NewBlockInvalidError("block is not valid",
				errors.NewContextCanceledError("catchup context cancelled on shutdown")),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldReportConsensusMalicious(tc.err))
		})
	}
}

// TestIsLocalCatchupFault pins the union predicate processCatchupChItem uses to
// decide that a terminal catchup error is this node's own fault. The union is the
// point: errors.IsLocalError misses the *Unavailable codes and
// errors.IsTransientLocalError misses context errors, so either helper alone leaves
// a class of purely local failure charged to an honest primary.
func TestIsLocalCatchupFault(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "storage fault - the mis-keyed external blob this branch reclassified",
			err:  errors.NewStorageError("external tx does not hash to the key it was stored under"),
			want: true,
		},
		{
			name: "service error - the pre-existing 1057 case",
			err:  errors.NewServiceError("blockchain service unavailable"),
			want: true,
		},
		{
			name: "service unavailable - aerospike batch timeout, missed by IsLocalError",
			err:  errors.NewServiceUnavailableError("aerospike batch read timed out"),
			want: true,
		},
		{
			name: "storage unavailable - missed by IsLocalError",
			err:  errors.NewStorageUnavailableError("blob store health check failed"),
			want: true,
		},
		{
			name: "context cancelled - shutdown, missed by IsTransientLocalError",
			err:  errors.NewContextCanceledError("shutting down"),
			want: true,
		},
		{
			name: "wrapped context deadline - catchup context timing out",
			err:  errors.NewProcessingError("catchup failed", context.DeadlineExceeded),
			want: true,
		},
		{
			name: "storage fault nested under a consensus code is still ours",
			err: errors.NewBlockInvalidError("block is not valid",
				errors.NewStorageError("could not read tx from stream")),
			want: true,
		},
		{
			name: "bare block-invalid is the peer's, not ours",
			err:  errors.NewBlockInvalidError("block does not meet target difficulty"),
			want: false,
		},
		{
			name: "bare tx-invalid is the peer's, not ours",
			err:  errors.NewTxInvalidError("previous tx has no output at index 3"),
			want: false,
		},
		{
			name: "an incomplete block is a peer-supplied shortfall, handled elsewhere",
			err:  errors.NewBlockIncompleteError("no coinbase from seeded peer"),
			want: false,
		},
		{
			// IsContextError alone fails this one. It resolves the chain with
			// errors.As, which stops at the outermost *Error and so reads
			// ERR_BLOCK_INVALID, then falls back to matching the rendered text
			// against the exact string "context canceled". This message says
			// "cancelled", so only the code walk catches it. The wording is the
			// point of the fixture: a predicate that depends on how a caller spelt
			// its message is not a predicate.
			name: "consensus code wrapping our own cancellation, message spelt otherwise",
			err: errors.NewBlockInvalidError("block is not valid",
				errors.NewContextCanceledError("catchup context cancelled on shutdown")),
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isLocalCatchupFault(tc.err))
		})
	}
}
