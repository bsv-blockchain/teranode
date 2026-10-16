package netsync

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/require"
)

// The other two rows that drop a record without writing the block off ask again above the last
// checkpoint too. A blob that will not read back (BlobUnusable) and a corrupt verdict on a
// record this node wrote (RecordCorrupt) used to drop the record with no re-ask, and the chain
// then waited for the next announcement, the same stall as files gone at 970,502.
func TestSyncManager_EveryDroppedRecordIsAskedForAgainAboveTheCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name string
		arm  func(t *testing.T, h *parkWiringHarness, child chainhash.Hash)
	}{
		{
			name: "blob_unusable",
			arm: func(t *testing.T, h *parkWiringHarness, child chainhash.Hash) {
				require.NoError(t, os.Remove(filepath.Join(h.parkDir, child.String()+".block")))
			},
		},
		{
			name: "record_corrupt",
			arm: func(_ *testing.T, h *parkWiringHarness, child chainhash.Hash) {
				h.validation.failOnce(child, errors.NewBlockCorruptError("[processBlockFound][%s] a subtree file this node wrote does not hash to its key", child.String()))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING, withTransactions(1))
			h.sm.headersFirstMode.Store(false)

			child := h.blocks[1].MsgBlock().BlockHash()

			require.NoError(t, h.deliver(t, 1))
			require.Equal(t, 1, h.sm.blockPark.Len())

			tc.arm(t, h, child)

			h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

			before := h.rec.getDataCount()

			require.NoError(t, h.deliver(t, 0))
			h.drainOneParkCommit(t)

			require.Zero(t, h.sm.blockPark.Len(), "the record is dropped")
			require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 5*time.Second),
				"above the checkpoint a dropped record must be asked for again at once")
		})
	}
}

// The re-ask is a column of the table, so which rows carry it is pinned here: every row that
// drops the record without writing the block off and still wants it back.
func TestParkDispositions_ReAskIsSetOnTheDropRowsThatWantTheBlockBack(t *testing.T) {
	want := map[string]bool{
		parkDispositionBlobUnusable.name:  true,
		parkDispositionRecordCorrupt.name: true,
		parkDispositionFilesGone.name:     true,
	}

	for _, d := range parkDispositionRows {
		require.Equal(t, want[d.name], d.reAsk, "row %s", d.name)

		if d.reAsk {
			require.Equal(t, parkBlobDrop, d.blob, "row %s re-asks for a block it keeps", d.name)
			require.False(t, d.markFailed, "row %s re-asks for a block it writes off", d.name)
		}
	}
}
