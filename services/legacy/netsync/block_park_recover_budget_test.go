package netsync

import (
	"context"
	"crypto/rand"
	"io"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// parkStallingStore is a store whose reads never answer: they wait for the
// caller's deadline and then report what a store with no read permit free
// reports. That is what a busy file store looks like from the park's side, and
// it is the shape that makes a restart scan expensive — every file costs the
// full per-operation deadline.
type parkStallingStore struct {
	blob.Store

	stalling atomic.Bool
}

func (s *parkStallingStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType,
	opts ...options.FileOption) (io.ReadCloser, error) {
	if s.stalling.Load() {
		<-ctx.Done()

		return nil, errors.NewServiceUnavailableError("no read permit available")
	}

	return s.Store.GetIoReader(ctx, key, fileType, opts...)
}

// TestBlockPark_RecoveryGivesUpRatherThanHoldingUpTheStart is about how long a
// node takes to start.
//
// Recover runs before the block handler goroutine is started, so every second it
// spends is a second the node is not syncing, not answering and not visibly
// doing anything. Each file it looks at costs one store read, and each read is
// bounded — but the number of files is not: it is whatever a previous run left
// behind, and that run may have had a much larger legacy_parkMaxBytes. Files
// times the per-operation deadline is not a bound anybody chose.
//
// The end state asserted here is the one an operator cares about: the node
// starts. What recovery did not reach is left on disk for the next start.
func TestBlockPark_RecoveryGivesUpRatherThanHoldingUpTheStart(t *testing.T) {
	const files = 20

	root := t.TempDir()

	storeURL, err := url.Parse("file://" + root)
	require.NoError(t, err)

	realStore, err := blob.NewStore(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	store := &parkStallingStore{Store: realStore}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.TempStore = storeURL
	// The floor, so the whole scan's budget is three seconds and the difference
	// between bounded and unbounded is twenty seconds against three.
	tSettings.Legacy.ParkStoreTimeout = time.Second

	park := newBlockPark(ulogger.TestLogger{}, tSettings, store)
	require.NotNil(t, park)

	// What a previous run left behind. The contents do not matter: recovery
	// never gets far enough to read them.
	ctx := context.Background()

	for i := 0; i < files; i++ {
		var hash chainhash.Hash

		_, err = rand.Read(hash[:])
		require.NoError(t, err)

		body := make([]byte, 128)
		_, err = rand.Read(body)
		require.NoError(t, err)

		require.NoError(t, store.Set(ctx, hash[:], fileformat.FileTypeMsgBlock, body, parkOpts...))
	}

	require.GreaterOrEqual(t, len(parkDirEntries(t, park.dir)), files, "the previous run's files should all be there")

	store.stalling.Store(true)

	start := time.Now()

	park.Recover(ctx)

	elapsed := time.Since(start)

	require.Less(t, elapsed, 10*time.Second,
		"recovery must give up on its budget rather than pay the per-file deadline %d times over", files)
	require.Zero(t, park.Len(), "nothing could be read, so nothing was adopted")
	// Not adopted is not the same as thrown away. Giving up on the budget must
	// leave the previous run's downloads where they are, or "recovery is
	// bounded" would mean "recovery destroys whatever it ran out of time for".
	require.GreaterOrEqual(t, len(parkDirEntries(t, park.dir)), files,
		"the files recovery did not adopt must still be on disk for the next start")
}

// TestBlockPark_RecoveryKeepsABlockItCouldNotRead is the other half of the
// budget, and it is the half that destroys data when it is missing.
//
// Recovery reads the 80-byte header off every parked blob to learn which parent
// the block is waiting for. That read can fail for two completely different
// reasons: the blob really is not the block it claims, or the store simply had
// no read permit free inside the deadline — and, once the scan carries a budget
// of its own, because the budget ran out part way through a read. Only the first
// says anything about the block. Treating them alike deletes fully downloaded
// blocks, up to 150 MB each, for being unreadable at the one moment the store is
// busiest: a restart under load.
//
// The end state asserted here is the operator-visible one — the files are still
// on disk afterwards — rather than a count of what recovery decided.
func TestBlockPark_RecoveryKeepsABlockItCouldNotRead(t *testing.T) {
	const files = 20

	root := t.TempDir()

	storeURL, err := url.Parse("file://" + root)
	require.NoError(t, err)

	realStore, err := blob.NewStore(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	store := &parkStallingStore{Store: realStore}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.TempStore = storeURL
	tSettings.Legacy.ParkStoreTimeout = time.Second

	park := newBlockPark(ulogger.TestLogger{}, tSettings, store)
	require.NotNil(t, park)

	ctx := context.Background()

	for i := 0; i < files; i++ {
		var hash chainhash.Hash

		_, err = rand.Read(hash[:])
		require.NoError(t, err)

		body := make([]byte, 128)
		_, err = rand.Read(body)
		require.NoError(t, err)

		require.NoError(t, store.Set(ctx, hash[:], fileformat.FileTypeMsgBlock, body, parkOpts...))
	}

	before := parkDirEntries(t, park.dir)
	require.GreaterOrEqual(t, len(before), files, "the previous run's files should all be there")

	// Every read now behaves the way a file store with no read permit free
	// behaves: it waits for the caller's deadline and then reports the store is
	// unavailable. Nothing about the blobs themselves has changed.
	store.stalling.Store(true)

	park.Recover(ctx)

	require.ElementsMatch(t, before, parkDirEntries(t, park.dir),
		"a read that could not get a permit says nothing about the block, so every file must still be there")
}
