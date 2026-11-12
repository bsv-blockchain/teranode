package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestPipelineRefill_NoAcceptanceBookkeeping guards the property upstream extracted
// refillHeaderBlockPipeline to get (bitcoin-sv/teranode#4692): the corrupt branch tops the download
// pipeline up WITHOUT running any accepted-block bookkeeping — no rejected-tx clear, no peer-height
// update, no FSM RUN, no fee-filter reset.
//
// That helper does not exist here, because there is no separate pipeline to top up: the wanted-range
// pass IS the download pipeline, it is recomputed from the committed tip on every call, and the
// corrupt branch reaches it through the same fetchHeaderBlocks every other caller uses. The property
// is still worth pinning, and it is now a property of that pass: calling it must have no accepted-
// block side effects, so the corrupt branch can call it on a FAILED delivery.
//
// The assertion is narrow and says so: the pass runs without panicking and leaves rejectedTxns —
// which the acceptance footer would clear — untouched. With no eligible peer the assigner is nil and
// nothing is actually requested; what is proven is the ABSENCE of acceptance side effects, not that
// the pipeline grew. That the pipeline does grow on the corrupt branch is pinned separately, on the
// wire, by TestHandleBlockMsg_CorruptBody_HeadersFirst_ReRequestsBlock.
func TestPipelineRefill_NoAcceptanceBookkeeping(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		chainParams:      &chaincfg.MainNetParams,
		blockSizeTracker: newBlockSizeTracker(10),
		rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](),
		peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		blockDownloads:   newBlockDownloadTracker(blockRequestAssignmentTTL),
		headerCache:      newHeaderCache(),
	}
	sm.headersFirstMode.Store(true)

	// A rejected tx that the acceptance footer WOULD clear; the pass must not.
	rejected := chainhash.Hash{0xAB}
	sm.rejectedTxns.Set(rejected, struct{}{})

	// A header cache naming two heights above a mocked committed tip, so the pass has
	// something to consider rather than returning at its first check.
	tipHash := mockCommittedTip(t, sm, 100, 0x55)
	headers, _ := linkedRun(tipHash, 2)
	require.True(t, sm.headerCache.Fill(tipHash, 101, headers))

	require.NotPanics(t, sm.fetchHeaderBlocks, "the pass must be safe to call on a failed delivery")

	_, stillRejected := sm.rejectedTxns.Get(rejected)
	require.True(t, stillRejected,
		"the pass must NOT clear rejectedTxns — that is accepted-block bookkeeping the corrupt path must skip")

	require.Zero(t, sm.currentFeeFilter.Load(), "the pass must not reset the fee filter")
}
