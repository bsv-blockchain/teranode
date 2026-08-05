package blockvalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestReleaseCatchupLock_ClassifiesCorruptBodyWithoutPeerPenalty covers the corrupt-body
// classification switch in releaseCatchupLock (bitcoin-sv/teranode#4692): a corrupt terminal error is
// recorded for the dashboard as "corrupt_block_body" and, crucially, is NOT a peer error — the
// serving peer was already struck at the corrupt site, so releaseCatchupLock must not open a generic
// peer-error window (that would double-charge a possibly-sole-source peer). The behaviour is
// asserted via the recorded PreviousAttempt.ErrorType and the fact that, with a nil p2p client and
// no failed peers, no reputation report is attempted (the reporting block is skipped for a
// non-peer error, so the nil client is never dereferenced).
func TestReleaseCatchupLock_ClassifiesCorruptBodyWithoutPeerPenalty(t *testing.T) {
	blockUpTo := testhelpers.CreateTestBlocks(t, 1)[0]

	newCtx := func() *CatchupContext {
		return &CatchupContext{
			blockUpTo:   blockUpTo,
			baseURL:     "http://peer",
			peerID:      "peer-1",
			startTime:   time.Now(),
			failedPeers: map[string]string{}, // empty: no per-peer failure reports
		}
	}

	t.Run("corrupt body -> corrupt_block_body, not a peer error", func(t *testing.T) {
		u := &Server{logger: ulogger.TestLogger{}} // p2pClient nil: proves no report is attempted
		cctx := newCtx()
		err := error(errors.NewBlockCorruptError("corrupt block body"))

		require.NotPanics(t, func() { u.releaseCatchupLock(cctx, &err) },
			"a corrupt terminal error must not attempt a peer report (no nil-client deref)")

		require.NotNil(t, u.previousCatchupAttempt)
		require.Equal(t, "corrupt_block_body", u.previousCatchupAttempt.ErrorType,
			"a corrupt body must be classified corrupt_block_body")
		require.False(t, u.isCatchingUp.Load(), "releaseCatchupLock clears the catching-up flag")
		require.Nil(t, u.activeCatchupCtx, "releaseCatchupLock clears the active context")
	})

	// Contrast: a transient local incomplete state is also a non-peer error but a DIFFERENT type,
	// proving the corrupt case is a distinct, deliberate classification and not a catch-all.
	t.Run("transient incomplete -> block_incomplete_transient, not a peer error", func(t *testing.T) {
		u := &Server{logger: ulogger.TestLogger{}}
		cctx := newCtx()
		err := error(errors.NewBlockIncompleteTransientError("unabsorbed parent"))

		require.NotPanics(t, func() { u.releaseCatchupLock(cctx, &err) })

		require.NotNil(t, u.previousCatchupAttempt)
		require.Equal(t, "block_incomplete_transient", u.previousCatchupAttempt.ErrorType)
	})
}
