package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/require"
)

// TestReleaseCatchupLock_QuickValidationMarkersAreLocal pins the terminal-error verdict for the two
// markers tryQuickValidation aborts on (bitcoin-sv/teranode#4978, bitcoin-sv/teranode#4987). Both are
// this node's own failure: a .subtree write that did not land, or an AddBlock this node could not
// confirm. isLocalCatchupFault already calls them local, so processCatchupChItem does not charge the
// primary, but releaseCatchupLock had no case for them. A block_committed error is a ServiceError,
// which no code case matched, so it fell to the text cases: a flattened gRPC deadline was labelled
// local_context_cancelled only by luck of its text, and a plain connection failure to our own
// blockchain service was labelled network_error and charged to the primary.
//
// Each error is built the way its producer in quick_validate.go builds it, with the inner error a
// flattened gRPC failure (ERR_ERROR, as UnwrapGRPC returns for a status error) or a context error.
func TestReleaseCatchupLock_QuickValidationMarkersAreLocal(t *testing.T) {
	const caller = "quickValidateBlockAsync"

	hash := "0000000000000000000000000000000000000000000000000000000000000001"
	deadlineErr := errors.NewError("rpc error: code = DeadlineExceeded desc = context deadline exceeded")
	connErr := errors.NewError("rpc error: code = Unavailable desc = connection error: desc = \"transport: Error while dialing: dial tcp 127.0.0.1:8087: connect: connection refused\"")

	probeFailed := func(addErr, probeErr error) error {
		return markBlockCommitted(errors.NewServiceError("[%s][%s] AddBlock failed and the block may be stored (probe failed: %v)", caller, hash, probeErr, addErr))
	}

	cases := []struct {
		name      string
		err       error
		wantLabel string
	}{
		{
			name:      "block_committed probe failed, flattened gRPC deadline",
			err:       probeFailed(deadlineErr, deadlineErr),
			wantLabel: "local_block_maybe_committed",
		},
		{
			name:      "block_committed probe failed, blockchain service down",
			err:       probeFailed(connErr, connErr),
			wantLabel: "local_block_maybe_committed",
		},
		{
			name:      "block_committed not stored yet but ctx ended",
			err:       markBlockCommitted(errors.NewServiceError("[%s][%s] AddBlock failed and the block may be stored (not stored yet, but ctx ended)", caller, hash, deadlineErr)),
			wantLabel: "local_block_maybe_committed",
		},
		{
			name:      "block_committed unlock failed after commit",
			err:       markBlockCommitted(errors.NewServiceError("[%s][%s] block is committed but its UTXOs are still locked", caller, hash, connErr)),
			wantLabel: "local_block_maybe_committed",
		},
		{
			name:      "subtree_writes_not_landed context ended",
			err:       markSubtreeWritesNotLanded(errors.NewStorageError("[quickValidateBlockAsync][%s] context ended before the block's .subtree writes settled", hash, context.DeadlineExceeded)),
			wantLabel: "local_subtree_writes_not_landed",
		},
		{
			name:      "subtree_writes_not_landed write failed",
			err:       markSubtreeWritesNotLanded(errors.NewStorageError("[quickValidateBlockAsync][%s] a .subtree write for this block did not land, not storing it with subtrees_set", hash)),
			wantLabel: "local_subtree_writes_not_landed",
		},
		{
			// The run's error leaves validateBlocksOnChannel through errgroup and any wrapper
			// above it; the marker is found anywhere in the chain.
			name:      "block_committed wrapped by an outer processing error",
			err:       errors.NewProcessingError("[catchup] failed to validate blocks", probeFailed(connErr, connErr)),
			wantLabel: "local_block_maybe_committed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, isLocalCatchupFault(tc.err), "precondition: the sibling classifier calls this error local")

			server, _, _, cleanup := setupTestCatchupServer(t)
			defer cleanup()

			recorder := newPeerFailureRecordingP2PClient()
			server.p2pClient = recorder

			header := testhelpers.CreateTestHeaders(t, 1)[0]
			catchupCtx := &CatchupContext{
				blockUpTo: &model.Block{Header: header, Height: 1000},
				baseURL:   "http://honest-primary:8000",
				peerID:    "honest-primary",
				startTime: time.Now(),
			}
			require.NoError(t, server.acquireCatchupLock(catchupCtx))

			relErr := tc.err
			server.releaseCatchupLock(catchupCtx, &relErr)

			recorder.mu.Lock()
			defer recorder.mu.Unlock()

			require.Empty(t, recorder.failuresByPeer, "a local quick-validation abort must not charge any peer")
			require.Empty(t, recorder.errorMsgsByPeer, "a local quick-validation abort must not be stored as a peer error")
			require.Empty(t, recorder.maliciousByPeer)

			status := server.getCatchupStatusInternal()
			require.NotNil(t, status.PreviousAttempt)
			require.Equal(t, tc.wantLabel, status.PreviousAttempt.ErrorType)
		})
	}
}
