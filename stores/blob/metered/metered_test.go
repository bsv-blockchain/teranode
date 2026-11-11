package metered

import (
	"context"
	"io"
	"net/url"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobfile "github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

type failingStore struct {
	blob.Store
	err error
}

func (f *failingStore) Set(context.Context, []byte, fileformat.FileType, []byte, ...options.FileOption) error {
	return f.err
}

func (f *failingStore) Get(context.Context, []byte, fileformat.FileType, ...options.FileOption) ([]byte, error) {
	return nil, f.err
}

type counts struct {
	reads, writes, failures   float64
	readSamples, writeSamples uint64
}

func snapshotOf(t *testing.T, name string) counts {
	t.Helper()

	initPrometheusMetrics()

	return counts{
		reads:        testutil.ToFloat64(prometheusBlobReads.WithLabelValues(name)),
		writes:       testutil.ToFloat64(prometheusBlobWrites.WithLabelValues(name)),
		failures:     testutil.ToFloat64(prometheusBlobErrors.WithLabelValues(name)),
		readSamples:  sampleCount(t, prometheusBlobReadDuration.WithLabelValues(name)),
		writeSamples: sampleCount(t, prometheusBlobWriteDuration.WithLabelValues(name)),
	}
}

func sampleCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()

	metric, ok := observer.(prometheus.Metric)
	require.True(t, ok)

	var out dto.Metric

	require.NoError(t, metric.Write(&out))

	return out.GetHistogram().GetSampleCount()
}

func TestMeteredCountsReadsAndWritesPerStore(t *testing.T) {
	ctx := context.Background()
	key := []byte("0123456789abcdef0123456789abcdef")

	subtree := New("test_counts_subtree", memory.New())
	New("test_counts_block", memory.New())

	require.NoError(t, subtree.Set(ctx, key, fileformat.FileTypeSubtree, []byte("data")))

	value, err := subtree.Get(ctx, key, fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.Equal(t, []byte("data"), value)

	exists, err := subtree.Exists(ctx, key, fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists)

	reader, err := subtree.GetIoReader(ctx, key, fileformat.FileTypeSubtree)
	require.NoError(t, err)

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, []byte("data"), body)

	require.NoError(t, subtree.SetFromReader(ctx, key, fileformat.FileTypeSubtreeData, io.NopCloser(strings.NewReader("more"))))
	require.NoError(t, subtree.Del(ctx, key, fileformat.FileTypeSubtreeData))

	got := snapshotOf(t, "test_counts_subtree")
	require.Equal(t, counts{reads: 3, writes: 3, readSamples: 3, writeSamples: 3}, got)

	require.Equal(t, counts{}, snapshotOf(t, "test_counts_block"))
}

func TestMeteredNotFoundIsNotAFailure(t *testing.T) {
	ctx := context.Background()
	store := New("test_not_found", memory.New())

	_, err := store.Get(ctx, []byte("0123456789abcdef0123456789abcdef"), fileformat.FileTypeSubtree)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrNotFound))

	got := snapshotOf(t, "test_not_found")
	require.Equal(t, float64(1), got.reads)
	require.Equal(t, float64(0), got.failures)
}

func TestMeteredCountsFailuresButNotCancellation(t *testing.T) {
	ctx := context.Background()
	key := []byte("0123456789abcdef0123456789abcdef")

	broken := New("test_failures_broken", &failingStore{Store: memory.New(), err: errors.NewStorageError("disk full")})

	require.Error(t, broken.Set(ctx, key, fileformat.FileTypeSubtree, []byte("data")))

	_, err := broken.Get(ctx, key, fileformat.FileTypeSubtree)
	require.Error(t, err)

	got := snapshotOf(t, "test_failures_broken")
	require.Equal(t, counts{reads: 1, writes: 1, failures: 2, readSamples: 1, writeSamples: 1}, got)

	cancelled := New("test_failures_cancelled", &failingStore{Store: memory.New(), err: context.Canceled})

	require.Error(t, cancelled.Set(ctx, key, fileformat.FileTypeSubtree, []byte("data")))

	got = snapshotOf(t, "test_failures_cancelled")
	require.Equal(t, float64(1), got.writes)
	require.Equal(t, float64(0), got.failures)
}

func TestMeteredStartsAtZero(t *testing.T) {
	New("test_idle", memory.New())

	require.Equal(t, counts{}, snapshotOf(t, "test_idle"))
}

func TestMeteredPassesThroughHealthAndBlockHeight(t *testing.T) {
	ctx := context.Background()
	store := New("test_passthrough", memory.New())

	status, _, err := store.Health(ctx, true)
	require.NoError(t, err)
	require.Equal(t, 200, status)

	store.SetCurrentBlockHeight(10)
	require.NoError(t, store.Close(ctx))
}

// legacyPendingFileStore is the assertion legacy sync's subtree writer makes on its store.
type legacyPendingFileStore interface {
	NewPendingFile(ctx context.Context, fileType fileformat.FileType, opts ...options.FileOption) (*blobfile.PendingFile, error)
}

func newFileStore(t *testing.T) *blobfile.File {
	t.Helper()

	u, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	f, err := blobfile.New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	return f
}

func TestMeteredKeepsPendingFilesOfAFileStore(t *testing.T) {
	store := New("test_pending_file_store", newFileStore(t))

	_, ok := store.(legacyPendingFileStore)
	require.True(t, ok)
}

func TestMeteredDoesNotAddPendingFilesToAStoreWithout(t *testing.T) {
	store := New("test_pending_memory_store", memory.New())

	_, ok := store.(legacyPendingFileStore)
	require.False(t, ok)
}

func TestMeteredCountsACommittedPendingFileAsOneWrite(t *testing.T) {
	ctx := context.Background()
	store := New("test_pending_commit", newFileStore(t))

	pending, ok := store.(legacyPendingFileStore)
	require.True(t, ok)

	file, err := pending.NewPendingFile(ctx, fileformat.FileTypeTesting)
	require.NoError(t, err)

	_, err = file.Write([]byte("streamed"))
	require.NoError(t, err)

	before := snapshotOf(t, "test_pending_commit")
	require.Equal(t, 0.0, before.writes)

	require.NoError(t, file.Commit(ctx, []byte("0123456789abcdef0123456789abcdef")))

	after := snapshotOf(t, "test_pending_commit")
	require.Equal(t, 1.0, after.writes)
	require.Equal(t, uint64(1), after.writeSamples)
	require.Equal(t, 0.0, after.failures)

	got, err := store.Get(ctx, []byte("0123456789abcdef0123456789abcdef"), fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.Equal(t, []byte("streamed"), got)
}

func TestMeteredCountsAFailedPendingCommitAsAFailure(t *testing.T) {
	ctx := context.Background()
	store := New("test_pending_commit_failure", newFileStore(t))

	pending, ok := store.(legacyPendingFileStore)
	require.True(t, ok)

	file, err := pending.NewPendingFile(ctx, fileformat.FileTypeTesting)
	require.NoError(t, err)

	require.Error(t, file.Commit(ctx, []byte("0123456789abcdef0123456789abcdef")))

	after := snapshotOf(t, "test_pending_commit_failure")
	require.Equal(t, 1.0, after.writes)
	require.Equal(t, 1.0, after.failures)
}

func TestMeteredBlobAlreadyExistsIsNotAFailure(t *testing.T) {
	ctx := context.Background()
	exists := New("test_exists", &failingStore{Store: memory.New(), err: errors.NewBlobAlreadyExistsError("key")})

	require.Error(t, exists.Set(ctx, []byte("k"), fileformat.FileTypeTesting, []byte("v")))

	got := snapshotOf(t, "test_exists")
	require.Equal(t, 1.0, got.writes)
	require.Equal(t, 0.0, got.failures)
}
