package blockvalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestReleaseCatchupLock_ClassifiesLocalPolicyDeclineWithoutPeerPenalty pins the ErrBlockError case
// in releaseCatchupLock's classification switch (bitcoin-sv/teranode#4692). On the catchup path a
// bare ERR_BLOCK_ERROR is a LOCAL block-level decision — an oversized-block policy decline
// (excessiveblocksize) or the "given up waiting on previous blocks" ordering timeout — not the
// serving peer's fault. It must classify as "local_block_policy_or_wait" AND set isPeerError=false
// so no generic peer-error report is issued (a possibly-sole-source peer must not be charged for our
// own config).
//
// isPeerError=false is pinned DIRECTLY with a recording P2P client: the ErrBlockError case must issue
// NO UpdateCatchupError for the peer. The positive control — a generic terminal error that falls to
// the peer-attributable default — DOES issue one, proving the recorder observes a report when one is
// made, so the "no report" assertion is meaningful.
//
// Mutation proof: flip this case's isPeerError back to true (or delete the case so it falls to the
// default) and releaseCatchupLock issues a generic peer-error report, so errorMsgsByPeer becomes
// non-empty and the "no report" assertion reddens.
func TestReleaseCatchupLock_ClassifiesLocalPolicyDeclineWithoutPeerPenalty(t *testing.T) {
	blockUpTo := testhelpers.CreateTestBlocks(t, 1)[0]

	newCtx := func() *CatchupContext {
		return &CatchupContext{
			blockUpTo:   blockUpTo,
			baseURL:     "http://peer",
			peerID:      "peer-1",
			startTime:   time.Now(),
			failedPeers: map[string]string{}, // empty: no per-peer failure reports from the drain loop
		}
	}

	t.Run("policy decline -> local_block_policy_or_wait, NO generic peer-error report", func(t *testing.T) {
		recorder := newPeerFailureRecordingP2PClient()
		u := &Server{logger: ulogger.TestLogger{}, p2pClient: recorder}
		cctx := newCtx()
		err := error(errors.NewBlockError("block size exceeds policy: excessiveblocksize; block not stored, not invalid"))

		u.releaseCatchupLock(cctx, &err)

		require.NotNil(t, u.previousCatchupAttempt)
		require.Equal(t, "local_block_policy_or_wait", u.previousCatchupAttempt.ErrorType,
			"a bare ERR_BLOCK_ERROR on catchup is a local decision, not a peer error")

		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		require.Empty(t, recorder.errorMsgsByPeer["peer-1"],
			"isPeerError must be false: a local policy decline must NOT issue a generic peer-error report")
		require.Equal(t, 0, recorder.failuresByPeer["peer-1"],
			"a local policy decline must not charge the serving peer a catchup failure")
	})

	// Positive control: a generic terminal error is peer-attributable — it falls to the default case
	// with isPeerError=true and DOES issue a generic peer-error report. This proves the recorder
	// captures a report when one is made, so the "no report" assertion above is load-bearing.
	t.Run("generic peer error -> unknown_error, DOES issue a peer-error report", func(t *testing.T) {
		recorder := newPeerFailureRecordingP2PClient()
		u := &Server{logger: ulogger.TestLogger{}, p2pClient: recorder}
		cctx := newCtx()
		err := error(errors.NewProcessingError("some generic terminal error"))

		u.releaseCatchupLock(cctx, &err)

		require.NotNil(t, u.previousCatchupAttempt)
		require.Equal(t, "unknown_error", u.previousCatchupAttempt.ErrorType)

		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		require.NotEmpty(t, recorder.errorMsgsByPeer["peer-1"],
			"a peer-attributable error must issue a generic peer-error report (proves the recorder observes reports)")
	})
}
