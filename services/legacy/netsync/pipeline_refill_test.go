package netsync

import (
	"container/list"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestRefillHeaderBlockPipeline_NoAcceptanceBookkeeping guards that the headers-first pipeline
// refill (bitcoin-sv/teranode#4692), extracted from the accepted-block footer so the corrupt-body
// drop can refill the download pipeline WITHOUT running any accepted-block bookkeeping (rejected-tx
// clear, peer-height update, FSM RUN, fee-filter reset), carries none of that bookkeeping. What this
// test actually asserts is narrow: the helper runs without error and leaves rejectedTxns — which the
// acceptance footer would clear — untouched. With no sync peer the fetchHeaderBlocks branch is a
// safe no-op, so the pipeline is not actually topped up here; the point proven is the ABSENCE of
// acceptance side effects, not that the pipeline grew.
func TestRefillHeaderBlockPipeline_NoAcceptanceBookkeeping(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		blockSizeTracker: newBlockSizeTracker(10),
		rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](),
	}

	// A rejected tx that the acceptance footer WOULD clear; the refill must not.
	rejected := chainhash.Hash{0xAB}
	sm.rejectedTxns.Set(rejected, struct{}{})

	// startHeader set + an empty requestedBlocks (Len 0 < dynamicMax) selects the fetchHeaderBlocks
	// branch, which safely no-ops without a sync peer — proving the refill runs without error and
	// without any accepted-block side effects.
	sm.startHeader = list.New().PushBack(nil)
	state := &peerSyncState{requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute)}
	t.Cleanup(func() { state.requestedBlocks.Stop() })

	err := sm.refillHeaderBlockPipeline(&peer.Peer{}, state)
	require.NoError(t, err, "pipeline refill must not error")

	_, stillRejected := sm.rejectedTxns.Get(rejected)
	require.True(t, stillRejected,
		"refill must NOT clear rejectedTxns — that is accepted-block bookkeeping the corrupt path must skip")
}
