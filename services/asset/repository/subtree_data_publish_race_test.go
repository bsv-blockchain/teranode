package repository

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blobfile "github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// This test runs over the REAL file store (stores/blob/file) in a temporary directory,
// decorated with one hook so a second writer publishes the key as this writer's SetFromReader
// starts: after the caller's own existence check and before the store's pre-write check.
// Nothing about storage is faked. This writer does not ask for the file store's exclusive
// publish, so the pre-write check is where the store refuses it.

type noopDeletionScheduler struct{}

func (noopDeletionScheduler) ScheduleBlobDeletion(context.Context, []byte, string, storetypes.BlobStoreType, uint32) (int64, bool, error) {
	return 0, true, nil
}

func (noopDeletionScheduler) CancelBlobDeletion(context.Context, []byte, string, storetypes.BlobStoreType) (bool, error) {
	return true, nil
}

type publishRaceStore struct {
	*blobfile.File
	compete func(key []byte, fileType fileformat.FileType)
}

// SetFromReader lets the competitor publish first, then writes. The store's pre-write check
// then finds the competitor's file and refuses this write, which is the refusal a caller of
// the file store gets without options.WithExclusivePublish.
func (s *publishRaceStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	s.compete(key, fileType)

	return s.File.SetFromReader(ctx, key, fileType, reader, opts...)
}

// TestGetSubtreeDataReader_ALostPublishRaceIsASuccessForThePeer: block validation publishes the
// subtreeData as the on-demand generation starts streaming it to the peer and to the store. The
// store refuses this write at its pre-write check. Whether that refusal reaches the generator at
// a Write or only at Close depends on timing, and either way the peer must read the whole body to
// a clean end, the store must keep the other writer's file, and the event is counted as a
// success under its own label. TestGetSubtreeDataReader_ARefusalBeforeTheFirstWriteStillServesThePeer
// pins the Write ordering deterministically.
func TestGetSubtreeDataReader_ALostPublishRaceIsASuccessForThePeer(t *testing.T) {
	tracing.SetupMockTracer()
	resetQuorumForTests()

	tc, subtree, txs := setupSubtreeReaderTest(t)

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	plain, err := blobfile.New(ulogger.TestLogger{}, storeURL, options.WithBlobDeletionScheduler(noopDeletionScheduler{}))
	require.NoError(t, err)

	// The structure file makes the on-demand path run; the data file is what it generates.
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, plain.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	// What block validation would have written: the non-coinbase transactions in order.
	theirs := &bytes.Buffer{}
	for _, tx := range txs[1:] {
		theirs.Write(tx.Bytes())
	}

	// The hook runs on the file storer's goroutine, so it records and the test asserts.
	var competeErr error

	store := &publishRaceStore{File: plain}
	store.compete = func(key []byte, fileType fileformat.FileType) {
		competeErr = plain.Set(context.Background(), key, fileType, theirs.Bytes(), options.WithDeleteAt(100))
	}

	tc.repo.SubtreeStore = store

	initPrometheusMetrics()
	lostBefore := testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("success", "lost_publish_race"))
	closeFailedBefore := testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "close_failed"))

	r, err := tc.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
	require.NoError(t, err)

	// The peer reads the whole body and reaches a clean end of stream.
	checkSubtreeTransactions(t, r, false)
	require.NoError(t, r.Close())

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("success", "lost_publish_race")) > lostBefore
	}, 2*time.Second, 10*time.Millisecond, "the lost race is counted as a success")

	require.NoError(t, competeErr, "the other writer's publish must succeed")
	require.InDelta(t, closeFailedBefore, testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "close_failed")), 0,
		"a lost race is not a close failure")

	got, err := plain.Get(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.Equal(t, theirs.Bytes(), got, "the other writer's file stands")
}

// refusedFirstStore makes the mid-stream ordering of the lost publish race certain. Its
// SetFromReader lets the competitor publish, takes the store's refusal and fails the file
// storer's pipe with it before signalling. The generator's read of the subtree structure waits
// for that signal, so every byte it writes reaches a file storer that has already been refused.
type refusedFirstStore struct {
	*blobfile.File
	compete func(key []byte, fileType fileformat.FileType)
	refused chan struct{}
}

func (s *refusedFirstStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	s.compete(key, fileType)

	err := s.File.SetFromReader(ctx, key, fileType, reader, opts...)
	if pr, ok := reader.(interface{ CloseWithError(error) error }); ok && err != nil {
		_ = pr.CloseWithError(err)
	}

	close(s.refused)

	return err
}

func (s *refusedFirstStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	if fileType == fileformat.FileTypeSubtree {
		select {
		case <-s.refused:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return s.File.GetIoReader(ctx, key, fileType, opts...)
}

// TestGetSubtreeDataReader_ARefusalBeforeTheFirstWriteStillServesThePeer pins the usual ordering
// of the lost publish race: the store refuses this write before the generator has written
// anything, so the refusal comes back from the file storer's Write rather than from Close. With a
// 16-byte file storer buffer every write goes straight to the refused pipe. The peer must still
// read the whole body to a clean end, and the store must keep the other writer's file.
func TestGetSubtreeDataReader_ARefusalBeforeTheFirstWriteStillServesThePeer(t *testing.T) {
	tracing.SetupMockTracer()
	resetQuorumForTests()

	tc, subtree, txs := setupSubtreeReaderTest(t)
	tc.repo.settings.Block.UTXOPersisterBufferSize = "16B"

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	plain, err := blobfile.New(ulogger.TestLogger{}, storeURL, options.WithBlobDeletionScheduler(noopDeletionScheduler{}))
	require.NoError(t, err)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, plain.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	theirs := &bytes.Buffer{}
	for _, tx := range txs[1:] {
		theirs.Write(tx.Bytes())
	}

	var competeErr error

	store := &refusedFirstStore{File: plain, refused: make(chan struct{})}
	store.compete = func(key []byte, fileType fileformat.FileType) {
		competeErr = plain.Set(context.Background(), key, fileType, theirs.Bytes(), options.WithDeleteAt(100))
	}

	tc.repo.SubtreeStore = store

	initPrometheusMetrics()
	lostBefore := testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("success", "lost_publish_race"))
	writeFailedBefore := testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "write_failed"))

	r, err := tc.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
	require.NoError(t, err)

	checkSubtreeTransactions(t, r, false)
	require.NoError(t, r.Close())

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("success", "lost_publish_race")) > lostBefore
	}, 2*time.Second, 10*time.Millisecond, "the lost race is counted as a success")

	require.NoError(t, competeErr, "the other writer's publish must succeed")
	require.InDelta(t, writeFailedBefore, testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "write_failed")), 0,
		"a lost race is not a write failure")

	got, err := plain.Get(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.Equal(t, theirs.Bytes(), got, "the other writer's file stands")
}
