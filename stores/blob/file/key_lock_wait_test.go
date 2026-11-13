package file_test

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// hangingScheduler is the real SQL scheduler with ScheduleBlobDeletion made to hang, the first
// time only, until its context ends or the test lets it go: a blockchain service that has stopped
// answering.
type hangingScheduler struct {
	sqlDeletionScheduler

	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *hangingScheduler) ScheduleBlobDeletion(ctx context.Context, key []byte, fileType string, storeType storetypes.BlobStoreType, dah uint32) (int64, bool, error) {
	first := false

	s.once.Do(func() {
		first = true

		close(s.entered)
	})

	if first {
		select {
		case <-ctx.Done():
			return 0, false, ctx.Err()
		case <-s.release:
		}
	}

	return s.sqlDeletionScheduler.ScheduleBlobDeletion(ctx, key, fileType, storeType, dah)
}

func newHangingStore(t *testing.T) (*file.File, *hangingScheduler) {
	t.Helper()

	_, bc := exclusiveDAHStores(t)

	sched := &hangingScheduler{sqlDeletionScheduler: sqlDeletionScheduler{store: bc}, entered: make(chan struct{}), release: make(chan struct{})}

	u, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	f, err := file.New(ulogger.TestLogger{}, u, options.WithBlobDeletionScheduler(sched))
	require.NoError(t, err)

	return f, sched
}

// startExclusivePublish starts an exclusive publish of key with a delete-at-height, which
// schedules under the blob's key lock, and returns its result channel once the schedule hangs.
func startExclusivePublish(t *testing.T, f *file.File, sched *hangingScheduler, key []byte) chan error {
	t.Helper()

	published := make(chan error, 1)

	go func() {
		published <- f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, io.NopCloser(bytes.NewReader([]byte("blob"))),
			options.WithExclusivePublish(), options.WithDeleteAt(1000))
	}()

	select {
	case <-sched.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the publish never reached the scheduler")
	}

	return published
}

// SetDAH waited on a blob's key lock with no context while it held a write permit, so a publish
// whose scheduler call hung pinned every SetDAH of that blob, and with enough of them the permit
// pool (review of 76be891d6). The wait now ends with the caller's context.
func TestSetDAH_GivesUpWaitingForTheKeyLockWhenItsContextEnds(t *testing.T) {
	f, sched := newHangingStore(t)
	key := []byte("subtree-root-key-lock-wait-00001")

	published := startExclusivePublish(t, f, sched, key)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)

	go func() { done <- f.SetDAH(ctx, key, fileformat.FileTypeTesting, 0) }()

	select {
	case err := <-done:
		require.Error(t, err, "a SetDAH that could not take the key lock reports it")
	case <-time.After(5 * time.Second):
		close(sched.release)
		t.Fatal("SetDAH must stop waiting for the key lock once its context ends")
	}

	close(sched.release)
	require.NoError(t, <-published)

	require.NoError(t, f.SetDAH(context.Background(), key, fileformat.FileTypeTesting, 0), "with the lock free the promotion goes through")
}

// The schedule an exclusive publish makes under the key lock ran with context.Background(), so a
// scheduler call that hung held the key for good (review of 76be891d6). It is bounded now: the
// publish fails, nothing is published, and the key is free for the next writer.
func TestExclusivePublish_AHungSchedulerReleasesTheKeyLock(t *testing.T) {
	restore := file.SetKeyLockedSchedulerTimeout(200 * time.Millisecond)
	t.Cleanup(restore)

	f, sched := newHangingStore(t)
	key := []byte("subtree-root-key-lock-wait-00002")

	published := startExclusivePublish(t, f, sched, key)

	select {
	case err := <-published:
		require.Error(t, err, "a publish whose schedule did not complete fails")
	case <-time.After(5 * time.Second):
		close(sched.release)
		t.Fatal("a hung scheduler call under the key lock must be bounded")
	}

	exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.False(t, exists, "the blob is scheduled before it is published, so a failed schedule publishes nothing")

	require.NoError(t, f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, io.NopCloser(bytes.NewReader([]byte("blob"))),
		options.WithExclusivePublish(), options.WithDeleteAt(1000)), "the key is free for the next writer")
}
