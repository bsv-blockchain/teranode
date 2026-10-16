package file_test

import (
	"bytes"
	"context"
	"fmt"
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

// promotingScheduler is the real SQL scheduler with a hook on ScheduleBlobDeletion: before the
// schedule is written, it starts a promotion of the same blob (SetDAH to zero, the block
// persister keeping it) and gives that promotion promoteWait to finish. A promotion that has to
// wait for the publish is left running, and the test waits for it at the end.
type promotingScheduler struct {
	sqlDeletionScheduler

	f        *file.File
	fileType fileformat.FileType

	once     sync.Once
	promoted chan error
}

func (s *promotingScheduler) ScheduleBlobDeletion(ctx context.Context, key []byte, fileType string, storeType storetypes.BlobStoreType, dah uint32) (int64, bool, error) {
	s.once.Do(func() {
		go func() { s.promoted <- s.f.SetDAH(context.Background(), key, s.fileType, 0) }()

		select {
		case err := <-s.promoted:
			s.promoted <- err
		case <-time.After(promoteWait):
		}
	})

	return s.sqlDeletionScheduler.ScheduleBlobDeletion(ctx, key, fileType, storeType, dah)
}

const promoteWait = 200 * time.Millisecond

// External review of e4698b219, P2. An exclusive publish made the blob visible and only then
// scheduled its delete-at-height. A promotion to permanent retention (SetDAH 0) that landed in
// between found nothing to cancel, and the publish then wrote its deletion over it, so a blob
// the block persister meant to keep was deleted at height. The promotion must win whichever
// way the two interleave.
func TestAPromotionDuringAnExclusivePublishIsKept(t *testing.T) {
	ctx := context.Background()

	for _, name := range []string{"pending file commit", "set from reader"} {
		t.Run(name, func(t *testing.T) {
			_, bc := exclusiveDAHStores(t)

			sched := &promotingScheduler{sqlDeletionScheduler: sqlDeletionScheduler{store: bc}, fileType: fileformat.FileTypeTesting, promoted: make(chan error, 1)}

			u, err := url.Parse("file://" + t.TempDir())
			require.NoError(t, err)

			f, err := file.New(ulogger.TestLogger{}, u, options.WithBlobDeletionScheduler(sched))
			require.NoError(t, err)

			sched.f = f

			key := []byte("subtree-root-promoted-0000000001")

			switch name {
			case "pending file commit":
				p, err := f.NewPendingFile(ctx, fileformat.FileTypeTesting)
				require.NoError(t, err)

				_, err = p.Write([]byte("kept"))
				require.NoError(t, err)

				require.NoError(t, p.Commit(ctx, key, options.WithDeleteAt(100), options.WithExclusivePublish()))
			default:
				require.NoError(t, f.SetFromReader(ctx, key, fileformat.FileTypeTesting, io.NopCloser(bytes.NewReader([]byte("kept"))), options.WithDeleteAt(100), options.WithExclusivePublish()))
			}

			require.NoError(t, <-sched.promoted)

			require.Empty(t, scheduledFor(t, bc, key), "the promotion must not be undone by the publish it overlapped")

			got, err := f.Get(ctx, key, fileformat.FileTypeTesting)
			require.NoError(t, err)
			require.Equal(t, []byte("kept"), got)
		})
	}
}

// Many blobs published exclusively while a promoter keeps each one the moment it can see it.
// Whatever the interleaving, no kept blob may end with a deletion scheduled. Run with -race
// -count=300 -cpu 1,2,8.
func TestEveryBlobPromotedOnceVisibleEndsKept(t *testing.T) {
	ctx := context.Background()
	f, bc := exclusiveDAHStores(t)

	const blobs = 16

	var wg sync.WaitGroup

	for i := 0; i < blobs; i++ {
		key := []byte(fmt.Sprintf("promoted-blob-%018d", i))

		wg.Add(2)

		go func() {
			defer wg.Done()

			err := f.SetFromReader(ctx, key, fileformat.FileTypeTesting, io.NopCloser(bytes.NewReader([]byte("kept"))), options.WithDeleteAt(100), options.WithExclusivePublish())
			if err != nil {
				t.Errorf("publish: %v", err)
			}
		}()

		go func() {
			defer wg.Done()

			for {
				exists, err := f.Exists(ctx, key, fileformat.FileTypeTesting)
				if err != nil {
					t.Errorf("exists: %v", err)

					return
				}

				if exists {
					if err := f.SetDAH(ctx, key, fileformat.FileTypeTesting, 0); err != nil {
						t.Errorf("promote: %v", err)
					}

					return
				}
			}
		}()
	}

	wg.Wait()

	for i := 0; i < blobs; i++ {
		key := []byte(fmt.Sprintf("promoted-blob-%018d", i))
		require.Empty(t, scheduledFor(t, bc, key), "blob %d was promoted once visible and must end kept", i)
	}
}
