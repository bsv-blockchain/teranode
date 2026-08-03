package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// failureCountingP2PClient counts recorded catchup failures; every other
// P2PClientI method is inherited as a no-op from maliciousAbortP2PClient.
// reportCatchupFailure routes through RecordCatchupFailureWithKind, so both
// entry points are counted.
type failureCountingP2PClient struct {
	maliciousAbortP2PClient
	failures int
}

func (f *failureCountingP2PClient) RecordCatchupFailure(_ context.Context, _ string) error {
	f.failures++
	return nil
}

func (f *failureCountingP2PClient) RecordCatchupFailureWithKind(_ context.Context, _, _, _ string) error {
	f.failures++
	return nil
}

// TestReportCatchupFailureForError_SkipsAlreadyReported guards the
// one-failure-per-attempt invariant: when a lower layer (the header-fetch
// stage) has already recorded the failure, the top-level handler must not
// record a second one for the same propagated error.
func TestReportCatchupFailureForError_SkipsAlreadyReported(t *testing.T) {
	newServer := func() (*Server, *failureCountingP2PClient) {
		client := &failureCountingP2PClient{}
		return &Server{p2pClient: client, logger: ulogger.TestLogger{}}, client
	}

	t.Run("unmarked error is reported", func(t *testing.T) {
		u, client := newServer()
		u.reportCatchupFailureForError(context.Background(), "peer-1", errors.NewNetworkTimeoutError("peer timed out"))
		require.Equal(t, 1, client.failures)
	})

	t.Run("marked error is skipped", func(t *testing.T) {
		u, client := newServer()
		err := markCatchupFailureReported(errors.NewNetworkTimeoutError("peer timed out"))
		u.reportCatchupFailureForError(context.Background(), "peer-1", err)
		require.Equal(t, 0, client.failures)
	})

	t.Run("marker survives teranode error wrapping", func(t *testing.T) {
		u, client := newServer()
		// Same shape fetchHeaders produces: ProcessingError wrapping the marked error.
		err := errors.NewProcessingError("failed to get block headers",
			markCatchupFailureReported(errors.NewServiceError("http request returned status code [429]")))
		u.reportCatchupFailureForError(context.Background(), "peer-1", err)
		require.Equal(t, 0, client.failures)
	})

	t.Run("block incomplete is still skipped", func(t *testing.T) {
		u, client := newServer()
		u.reportCatchupFailureForError(context.Background(), "peer-1", errors.ErrBlockIncomplete)
		require.Equal(t, 0, client.failures)
	})
}

// TestMarkCatchupFailureReported_TransparentToErrorCodes proves the marker does
// not break teranode error-code matching, which the top-level catchup dispatch
// in Server.go relies on (ErrServiceError, malicious classification, etc.).
func TestMarkCatchupFailureReported_TransparentToErrorCodes(t *testing.T) {
	require.Nil(t, markCatchupFailureReported(nil))

	// ProcessingError → marker → ServiceError: the ServiceError code must still
	// match through the foreign marker link (the top-level ErrServiceError
	// branch routes on this).
	err := errors.NewProcessingError("wrap",
		markCatchupFailureReported(errors.NewServiceError("http 429")))
	require.True(t, errors.Is(err, errors.ErrServiceError))
	require.True(t, catchupFailureAlreadyReported(err))

	// Malicious classification survives the marker too.
	malicious := errors.NewProcessingError("wrap",
		markCatchupFailureReported(errors.NewNetworkPeerMaliciousError("bad headers")))
	require.True(t, errors.Is(malicious, errors.ErrNetworkPeerMalicious))

	// An unmarked error must not read as already-reported.
	require.False(t, catchupFailureAlreadyReported(errors.NewServiceError("http 429")))
}

// TestMarkedExternalError_SurvivesProductionWrapChain guards issue-1368 review
// finding 3: catchupFailureAlreadyReported bails at the first non-native link, so
// this pins that the exact production wrap chain — ErrExternal marked at the
// source (fetchAndStoreSubtreeAndSubtreeData), then re-wrapped as ServiceError
// (fetchSubtreeDataForBlock) and ProcessingError (orderedDelivery), both native
// *errors.Error links — still carries the marker AND still matches ErrExternal by
// the time it reaches releaseCatchupLock's switch and Server.go's dispatch. A
// future refactor swapping in a foreign (non-native) wrapper anywhere in this
// chain would silently break both and restore the primary-charging bug.
func TestMarkedExternalError_SurvivesProductionWrapChain(t *testing.T) {
	marked := markCatchupFailureReported(errors.NewExternalError("all peer attempts failed to fetch subtree abc"))
	svcWrap := errors.NewServiceError("failed to fetch subtree data for block bb", marked)
	chain := errors.NewProcessingError("worker failed for block bb", svcWrap)

	require.True(t, errors.Is(chain, errors.ErrExternal), "ErrExternal classification must survive the production wrap chain")
	require.True(t, catchupFailureAlreadyReported(chain), "the already-reported marker must survive the production wrap chain")
}
