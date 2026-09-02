package netsync

import (
	"context"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// recordingDeletionScheduler stands in for the blockchain client the file store
// hands scheduled deletions to. Counting the calls is the only way to see a DAH
// being stamped: the file store no longer writes a .dah sidecar, it books the
// deletion with this interface instead.
type recordingDeletionScheduler struct {
	mu        sync.Mutex
	scheduled int
}

func (s *recordingDeletionScheduler) ScheduleBlobDeletion(_ context.Context, _ []byte, _ string, _ storetypes.BlobStoreType, _ uint32) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.scheduled++

	return int64(s.scheduled), true, nil
}

func (s *recordingDeletionScheduler) CancelBlobDeletion(_ context.Context, _ []byte, _ string, _ storetypes.BlobStoreType) (bool, error) {
	return false, nil
}

func (s *recordingDeletionScheduler) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.scheduled
}

// TestBlockPark_NeverSchedulesAParkedBlobForDeletion closes the park's one
// dependence on a setting nobody has to keep.
//
// The park owns the lifetime of every blob it writes: it deletes one when the
// block commits, when the block is given up on, or when the restart scan finds
// it unusable. Nothing else may. But options.MergeOptions copies the STORE's
// BlockHeightRetention into every operation before any file option is applied,
// and the park had no file option that could clear it — so the day anyone gives
// the legacy temp store a retention, every parked blob is booked for deletion
// at a height, and the deletion scheduler starts removing blocks out from under
// a live park. The block is then gone from disk while the index still says it is
// there, and the drain gives it up and downloads it again.
//
// The temp store has no retention today. This test is what makes that a choice
// rather than a load-bearing accident: it configures one and proves the park
// still books nothing.
func TestBlockPark_NeverSchedulesAParkedBlobForDeletion(t *testing.T) {
	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	scheduler := &recordingDeletionScheduler{}

	store, err := file.New(ulogger.TestLogger{}, storeURL,
		// The hazard, made real: a temp store somebody has given a retention.
		options.WithDefaultBlockHeightRetention(144),
		options.WithBlobDeletionScheduler(scheduler),
		options.WithStoreType(storetypes.TEMPSTORE),
	)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.TempStore = storeURL

	park := newBlockPark(ulogger.TestLogger{}, tSettings, store)
	require.NotNil(t, park)

	blocks := minedBlocks(t, 1)
	msgBlock := blocks[0].MsgBlock()
	hash := msgBlock.BlockHash()

	require.Equal(t, parkAccepted,
		park.Park(context.Background(), parkedBlock{hash: hash, prevBlock: msgBlock.Header.PrevBlock}, msgBlock))

	require.Zero(t, scheduler.count(),
		"a parked block's blob must never be booked for deletion at a height; the park deletes it when the block commits or is given up, and nothing else may")

	// And it is still there to be read back, which is the consequence that
	// matters to sync.
	readBack, err := park.Read(context.Background(), hash)
	require.NoError(t, err)

	readBackHash := readBack.BlockHash()
	require.True(t, readBackHash.IsEqual(&hash))
}
