package blockvalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestReleaseCatchupLock_ClassifiesCorruptBodyWithoutPeerPenalty covers the corrupt-body
// classification switch in releaseCatchupLock (bitcoin-sv/teranode#4692): a corrupt terminal error is
// recorded for the dashboard as "corrupt_block_body" and, crucially, is NOT a generic peer error —
// the serving peer was already struck via AddBanScore at the corrupt site, so releaseCatchupLock
// must not ALSO open the isPeerError/reportPeerErr window (that would double-charge a
// possibly-sole-source peer). What it does carry is exactly one generic catch-up failure charge,
// which feeds the reputation counters — the only selection input this route takes — with no
// penalty window, plus a display-only diagnostic naming the corrupt block hash.
func TestReleaseCatchupLock_ClassifiesCorruptBodyWithoutPeerPenalty(t *testing.T) {
	blockUpTo := testhelpers.CreateTestBlocks(t, 1)[0]

	newCtx := func() *CatchupContext {
		return &CatchupContext{
			blockUpTo:        blockUpTo,
			baseURL:          "http://peer",
			peerID:           "peer-1",
			startTime:        time.Now(),
			failedPeers:      map[string]string{}, // empty: no per-peer failure reports
			corruptBlockHash: blockUpTo.Hash().String(),
		}
	}

	t.Run("corrupt body -> corrupt_block_body, one generic charge plus a diagnostic", func(t *testing.T) {
		// peerFailureRecordingP2PClient rather than incompleteBlockP2PClient: this path now also
		// writes the display-only diagnostic through UpdateCatchupError, which the narrower fake
		// does not implement.
		rec := newPeerFailureRecordingP2PClient()
		u := &Server{logger: ulogger.TestLogger{}, p2pClient: rec}
		cctx := newCtx()
		err := error(errors.NewBlockCorruptError("corrupt block body"))

		require.NotPanics(t, func() { u.releaseCatchupLock(cctx, &err) },
			"a corrupt terminal error must never report the peer malicious")

		require.NotNil(t, u.previousCatchupAttempt)
		require.Equal(t, "corrupt_block_body", u.previousCatchupAttempt.ErrorType,
			"a corrupt body must be classified corrupt_block_body")
		require.False(t, u.isCatchingUp.Load(), "releaseCatchupLock clears the catching-up flag")
		require.Nil(t, u.activeCatchupCtx, "releaseCatchupLock clears the active context")

		rec.mu.Lock()
		defer rec.mu.Unlock()
		require.Equal(t, 1, rec.failuresByPeer["peer-1"],
			"a corrupt body must be charged exactly one catch-up failure")
		require.Equal(t, []string{catchupFailureKindGeneric}, rec.kindsByPeer["peer-1"],
			"the charge is the generic kind: no penalty window is opened for a corrupt body")
		require.Contains(t, rec.errorMsgsByPeer["peer-1"], blockUpTo.Hash().String(),
			"the peer-visible diagnostic names the corrupt block hash")
		require.Equal(t, 0, rec.maliciousByPeer["peer-1"],
			"a corrupt body must NOT flag the serving peer malicious")
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

// TestReleaseCatchupLock_CorruptChargesPrimaryOnce pins that a corrupt cycle charges the catch-up
// primary exactly one catch-up failure, whether or not the primary also failed a subtree fetch
// mid-cycle (bitcoin-sv/teranode#4692). The drain loop is authoritative for every peer in
// failedPeers, so without the corrupt arm of its primary-skip guard a primary that both failed a
// subtree fetch and produced the corrupt terminal verdict is charged twice for one attempt,
// re-creating the CatchupFailures > CatchupAttempts skew. The subtree-level error text is still
// recorded, then overwritten by the terminal corrupt message, which is the later write.
func TestReleaseCatchupLock_CorruptChargesPrimaryOnce(t *testing.T) {
	header := testhelpers.CreateTestHeaders(t, 1)[0]
	block := &model.Block{Header: header, Height: 1000}

	t.Run("primary also in failedPeers is charged once", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		rec := newPeerFailureRecordingP2PClient()
		server.p2pClient = rec

		cctx := &CatchupContext{
			blockUpTo: block,
			baseURL:   "http://primary:8000",
			peerID:    "primary",
			startTime: time.Now(),
		}
		require.NoError(t, server.acquireCatchupLock(cctx))

		// The primary also failed a subtree fetch mid-cycle, so it lands in failedPeers.
		server.recordCatchupPeerFailure("primary", errors.NewNotFoundError("status code [404] subtree not found"))

		cctx.corruptBlockHash = block.Hash().String()
		relErr := error(errors.NewBlockCorruptError("[BLOCK] body is corrupt"))
		server.releaseCatchupLock(cctx, &relErr)

		rec.mu.Lock()
		defer rec.mu.Unlock()
		require.Equal(t, 1, rec.failuresByPeer["primary"],
			"a corrupt cycle must charge the primary exactly once, not once per source")
		require.Equal(t, []string{catchupFailureKindGeneric}, rec.kindsByPeer["primary"])
		require.Contains(t, rec.errorMsgsByPeer["primary"], block.Hash().String(),
			"the terminal corrupt message is the last write and wins over the subtree text")
		require.Equal(t, 0, rec.maliciousByPeer["primary"])
		require.NotNil(t, server.previousCatchupAttempt)
		require.Equal(t, "corrupt_block_body", server.previousCatchupAttempt.ErrorType)
	})

	t.Run("primary not in failedPeers is still charged once", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		rec := newPeerFailureRecordingP2PClient()
		server.p2pClient = rec

		cctx := &CatchupContext{
			blockUpTo:        block,
			baseURL:          "http://primary:8000",
			peerID:           "primary",
			startTime:        time.Now(),
			corruptBlockHash: block.Hash().String(),
		}
		require.NoError(t, server.acquireCatchupLock(cctx))

		relErr := error(errors.NewBlockCorruptError("[BLOCK] body is corrupt"))
		server.releaseCatchupLock(cctx, &relErr)

		rec.mu.Lock()
		defer rec.mu.Unlock()
		require.Equal(t, 1, rec.failuresByPeer["primary"],
			"the skip must not remove the only charge when the primary never entered failedPeers")
		require.Equal(t, []string{catchupFailureKindGeneric}, rec.kindsByPeer["primary"])
	})
}
