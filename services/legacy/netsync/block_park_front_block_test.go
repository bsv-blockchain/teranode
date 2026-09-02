package netsync

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// The two drain paths that give a block up — an unusable blob and a block that
// will not commit — both rewind the download walk, and both were only ever
// driven by tests that delivered a block which was NOT the front of the header
// list. That is the easy branch: the header is still indexed, so the rewind
// finds it by lookup and never needs the header node the park carried with the
// block. Passing nil for that node at either site changed nothing any test could
// see.
//
// A block that arrives as the FRONT is the case the machinery exists for. Its
// header is removed from the list AND unindexed before the park ever sees the
// block, so unless the node travelled with it there is nothing to rewind onto,
// and the block leaves the download walk for good. Both tests here drive that.

// parkFrontBlock delivers the FRONT block of the header list, which parks
// because its parent is not stored, and returns its hash. Delivering the front
// is what makes the removedFront threading matter: the header is removed from
// the list AND unindexed before the park ever sees the block, so any path that
// later gives the block up has nothing to look it up by.
func (h *parkWiringHarness) parkFrontBlock(t *testing.T) chainhash.Hash {
	t.Helper()

	front := h.blocks[0].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 0))
	require.Equal(t, 1, h.sm.blockPark.Len(), "the front block parks like any other orphan")

	h.sm.headerMu.Lock()
	_, stillIndexed := h.sm.headerIndex[front]
	startHeader := h.sm.startHeader
	h.sm.headerMu.Unlock()

	require.False(t, stillIndexed, "an arriving front block's header is removed and unindexed before the park sees it")
	require.Nil(t, startHeader, "nothing has rewound yet")

	return front
}

// requireBackInTheWalk asserts the end state a given-up block must reach: its
// header is back in the list, indexed, at the front, with the download cursor on
// it, and the peer is asked for the block again.
func (h *parkWiringHarness) requireBackInTheWalk(t *testing.T, hash chainhash.Hash, getDataBefore int) {
	t.Helper()

	h.sm.headerMu.Lock()
	indexed := h.sm.headerIndex[hash]
	front := h.sm.headerList.Front()
	startHeader := h.sm.startHeader
	h.sm.headerMu.Unlock()

	require.NotNil(t, indexed, "a block given up on must be back in the header index")
	require.NotNil(t, front)
	require.Equal(t, hash.String(), front.Value.(*headerNode).hash.String(),
		"a block that was the front when it arrived must go back on the front")
	require.NotNil(t, startHeader, "the download cursor must be back on it")
	require.Equal(t, hash.String(), startHeader.Value.(*headerNode).hash.String())

	h.sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(getDataBefore, hash) }, 5*time.Second),
		"a block given up on must be asked for again")
}

// TestSyncManager_AParkedBlobThatIsNotTheBlockItClaimsIsGivenUp is the other
// half of the same decision, and it must keep working: a blob that will not
// decode, or that decodes into some other block, is evidence about the blob, so
// it is deleted and the block is put back into the download walk. It is not
// evidence about the peer — we wrote that file, so nobody else is to blame.
//
// The block used here is the FRONT of the header list, which is the case the
// rewind machinery exists for: its header was removed AND unindexed on arrival,
// so unless the park carried the header node with the block there is nothing to
// rewind onto.
func TestSyncManager_AParkedBlobThatIsNotTheBlockItClaimsIsGivenUp(t *testing.T) {
	for _, tc := range []struct {
		name    string
		corrupt func(t *testing.T, h *parkWiringHarness, path string)
	}{
		{
			name: "the blob will not decode",
			corrupt: func(t *testing.T, _ *parkWiringHarness, path string) {
				// Leave the store's own 8-byte header intact and cut the block
				// off part way through, which is what a torn write looks like.
				require.NoError(t, os.Truncate(path, int64(fileformat.Header{}.Size())+16))
			},
		},
		{
			name: "the blob is a different block",
			corrupt: func(t *testing.T, h *parkWiringHarness, path string) {
				// Keep the store header and put another block's bytes behind it,
				// so it decodes perfectly and hashes to the wrong thing.
				original, err := os.ReadFile(path)
				require.NoError(t, err)

				var other bytes.Buffer
				require.NoError(t, h.blocks[1].MsgBlock().Serialize(&other))

				body := append(original[:fileformat.Header{}.Size()], other.Bytes()...)

				require.NoError(t, os.WriteFile(path, body, 0o600))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newParkWiringHarness(t, true)

			prev := h.blocks[0].MsgBlock().Header.PrevBlock

			// The parked block's own parent is in the chain, so the sweep picks
			// it up and tries to commit it.
			h.client.On("GetBlockExists", mock.Anything, &prev).Return(true, nil)
			h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

			front := h.parkFrontBlock(t)

			tc.corrupt(t, h, filepath.Join(h.parkDir, front.String()+".msgBlock"))

			getDataBefore := h.rec.getDataCount()

			h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

			require.Zero(t, h.sm.blockPark.Len(), "an unusable blob must not stay in the index")
			require.Zero(t, h.sm.blockPark.Bytes(), "giving a block up must give its budget back")
			require.NotContains(t, parkDirEntries(t, h.parkDir), front.String()+".msgBlock",
				"an unusable blob must be deleted, not left to hold disk and budget")

			require.False(t, h.rec.wasRejected(front),
				"we wrote the blob, so a bad blob is our fault and must not be blamed on the peer")

			h.requireBackInTheWalk(t, front, getDataBefore)
		})
	}
}

// TestSyncManager_AParkedFrontBlockThatFailsValidationIsGivenUpAndRejected is
// the third row of the table, on the block that makes the rewind hard. A block
// that will not validate IS evidence about the block and about whoever sent it,
// so the blob goes, the walk is put back on it, and the delivering peer is told.
//
// Told only once the node is RUNNING, which is why this one does not use the
// default harness: while catching blocks no reject is sent at all, on the wire
// path or off the disk. That suppression is pinned in both directions by
// AParkedBlockThatWillNotCommitIsGivenUpAndRejected, so what is under test here
// is who hears about it, not whether anybody does.
func TestSyncManager_AParkedFrontBlockThatFailsValidationIsGivenUpAndRejected(t *testing.T) {
	h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING)

	front := h.blocks[0].MsgBlock().BlockHash()
	prev := h.blocks[0].MsgBlock().Header.PrevBlock

	// First lookup parks the block; the second, on the drain, is a judgement on
	// the block itself rather than a local fault, so it earns a reject.
	h.client.On("GetBlockExists", mock.Anything, &front).Return(false, nil).Once()
	h.client.On("GetBlockExists", mock.Anything, &front).
		Return(false, errors.NewBlockInvalidError("this block is not one we can take")).Once()
	h.client.On("GetBlockExists", mock.Anything, &prev).Return(true, nil)
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	h.parkFrontBlock(t)

	getDataBefore := h.rec.getDataCount()

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	require.Zero(t, h.sm.blockPark.Len(), "a block that will not commit must not stay parked")
	require.NotContains(t, parkDirEntries(t, h.parkDir), front.String()+".msgBlock",
		"a block given up on must not leave its blob behind")

	require.True(t, WaitUntil(func() bool { return h.rec.wasRejected(front) }, 5*time.Second),
		"the peer that sent the block must be told it was rejected")

	_, failed := h.sm.recentlyFailedBlocks.Get(front)
	require.True(t, failed, "a block written off must be remembered so its descendants are short-circuited")

	h.requireBackInTheWalk(t, front, getDataBefore)
}
