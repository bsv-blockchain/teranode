package blockassembly

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/stretchr/testify/require"
)

// The store step of a fork reorg (issue 2269, plan PR W): a forward block re-stores every subtree of
// the pending set, each a meta build from the parent tx map and two blob writes. These time that step
// alone, on the old worker path (a few workers each building and writing) and on the staged path
// behind blockassembly_storedPrefixTemplate (a CPU-wide meta stage and a bounded writer stage).

// storeStageFixture is a set of subtrees with their parent inpoints in one disk-backed tx map.
type storeStageFixture struct {
	subtrees []*subtreepkg.Subtree
	parents  *subtreeprocessor.DiskTxMap
	leaves   int
}

func newStoreStageFixture(tb testing.TB, subtrees, leaves int, mapDir string) *storeStageFixture {
	tb.Helper()

	parents, err := subtreeprocessor.NewDiskTxMap(subtreeprocessor.DiskTxMapOptions{BasePaths: []string{mapDir}})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = parents.Close() })

	fx := &storeStageFixture{parents: parents, leaves: leaves}

	var n, previous uint64

	for range subtrees {
		subtree, err := subtreepkg.NewTreeByLeafCount(leaves)
		require.NoError(tb, err)

		for range leaves {
			n++

			var hash, parent chainhash.Hash

			binary.LittleEndian.PutUint64(hash[:], n)
			binary.LittleEndian.PutUint64(parent[:], previous)
			previous = n

			in := &bt.Input{PreviousTxOutIndex: 0}
			require.NoError(tb, in.PreviousTxIDAdd(&parent))

			inpoints, err := subtreepkg.NewTxInpointsFromInputs([]*bt.Input{in})
			require.NoError(tb, err)

			parents.Set(hash, &inpoints)
			require.NoError(tb, subtree.AddNode(hash, 1, 250))
		}

		fx.subtrees = append(fx.subtrees, subtree)
	}

	require.NoError(tb, parents.Flush())

	return fx
}

// noDeletionScheduler accepts every scheduled deletion: a blob with a delete-at height needs a scheduler,
// and the real one is a gRPC call to the blockchain service that these runs leave out.
type noDeletionScheduler struct{}

func (noDeletionScheduler) ScheduleBlobDeletion(context.Context, []byte, string, storetypes.BlobStoreType, uint32) (int64, bool, error) {
	return 1, true, nil
}

func (noDeletionScheduler) CancelBlobDeletion(context.Context, []byte, string, storetypes.BlobStoreType) (bool, error) {
	return true, nil
}

// storeStagePath is one way to store the fixture's subtrees.
type storeStagePath struct {
	staged  bool
	workers int // blockassembly_subtreeStorageWorkers
	writers int // blockassembly_subtreeStorageWriters, staged only
}

// runner makes the server of a path once and returns a function that stores every subtree into a fresh
// file blob store under the directory it is given, and returns the time from the first request to the
// last settled result. The server is made outside the timer, and once per path, not per run.
func (fx *storeStageFixture) runner(t *testing.T, path storeStagePath) func(blobDir string) time.Duration {
	t.Helper()

	server, _ := setupServerWithSettings(t, func(s *settings.Settings) {
		s.BlockAssembly.StoredPrefixTemplate = path.staged
		s.BlockAssembly.SubtreeStorageWorkers = path.workers
		s.BlockAssembly.SubtreeStorageWriters = path.writers
	})

	var push func(subtreeprocessor.NewSubtreeRequest)

	if path.staged {
		push = func(req subtreeprocessor.NewSubtreeRequest) { server.storeQueue.Push(req) }
	} else {
		// the old path has no queue: a listener of our own reads the requests from a channel
		in := make(chan subtreeprocessor.NewSubtreeRequest, len(fx.subtrees))
		retry := make(chan *subtreeRetrySend, 100)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup

		wg.Add(2)

		go func() { defer wg.Done(); server.runSubtreeRetryProcessor(ctx, retry) }()
		go func() { defer wg.Done(); server.runNewSubtreeListener(ctx, in, retry) }()

		t.Cleanup(func() {
			cancel()
			wg.Wait()
		})

		push = func(r subtreeprocessor.NewSubtreeRequest) { in <- r }
	}

	return func(blobDir string) time.Duration {
		storeURL, err := url.Parse("file://" + blobDir)
		require.NoError(t, err)

		store, err := blob.NewStore(server.logger, storeURL, options.WithBlobDeletionScheduler(noDeletionScheduler{}))
		require.NoError(t, err)

		server.subtreeStore = store

		results := make([]chan subtreeprocessor.StoreResult, len(fx.subtrees))

		start := time.Now()

		for i, subtree := range fx.subtrees {
			results[i] = make(chan subtreeprocessor.StoreResult, 1)
			push(subtreeprocessor.NewSubtreeRequest{
				Subtree:          subtree,
				ParentTxMap:      fx.parents,
				SkipNotification: true,
				ErrChan:          make(chan error, 1),
				Result:           results[i],
			})
		}

		for i, r := range results {
			res := <-r
			require.True(t, res.OK(), fmt.Sprintf("subtree %d: %+v", i, res))
		}

		return time.Since(start)
	}
}

// BenchmarkReorgStoreStage compares 4 workers on the old path with the staged path (CPU-wide meta
// stage, bounded writers), on subtrees of 64K leaves with a disk map and a file blob store, and
// reports subtrees/s. Under -short: 8 subtrees of 8K leaves.
func BenchmarkReorgStoreStage(b *testing.B) {
	subtrees, leaves := 64, 64<<10
	if testing.Short() {
		subtrees, leaves = 8, 8<<10
	}

	fx := newStoreStageFixture(b, subtrees, leaves, b.TempDir())

	for _, c := range []struct {
		name string
		path storeStagePath
	}{
		{"old/workers=4", storeStagePath{workers: 4}},
		{"staged/writers=8", storeStagePath{staged: true, workers: 4, writers: 8}},
		{"staged/writers=32", storeStagePath{staged: true, workers: 4, writers: 32}},
	} {
		b.Run(c.name, func(b *testing.B) {
			var total time.Duration

			// setupServerWithSettings wants a *testing.T; the cleanups of this one never run, as in
			// BenchmarkAddTxBatchColumnar_Validation, so the server is made once per sub-benchmark
			run := fx.runner(&testing.T{}, c.path)

			for range b.N {
				total += run(b.TempDir())
			}

			b.ReportMetric(float64(subtrees*b.N)/total.Seconds(), "subtrees/s")
		})
	}
}
