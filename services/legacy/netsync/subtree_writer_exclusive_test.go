package netsync

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	blobfile "github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// The tests in this file run over the REAL file store (stores/blob/file) in a temporary
// directory, decorated with one hook so a second writer can be made to publish a key at a
// chosen moment. Nothing about storage is faked: every byte lands on disk through the file
// store's own code, and every assertion reads the file store back.

func exclusiveFileStore(t *testing.T) *blobfile.File {
	t.Helper()

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	store, err := blobfile.New(ulogger.TestLogger{}, storeURL, options.WithBlobDeletionScheduler(&recordingDeletionScheduler{}))
	require.NoError(t, err)

	return store
}

// competingStore is the real file store with a hook on SetFromReader. The hook runs once per
// SetFromReader call: at the first read of the body when beforeBody is false, which is after
// the store's own pre-check and before its publish; or before the call is forwarded when
// beforeBody is true, so the pre-check itself meets the key. It embeds the concrete *File so
// the subtree writer's pendingFileStore assertion still holds and the data file keeps its
// streamed path.
type competingStore struct {
	*blobfile.File
	beforeBody bool
	compete    func(key []byte, fileType fileformat.FileType)
}

func (s *competingStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	if s.beforeBody {
		s.compete(key, fileType)

		return s.File.SetFromReader(ctx, key, fileType, reader, opts...)
	}

	return s.File.SetFromReader(ctx, key, fileType, &competeOnFirstRead{ReadCloser: reader, hook: func() { s.compete(key, fileType) }}, opts...)
}

type competeOnFirstRead struct {
	io.ReadCloser
	hook func()
	once sync.Once
}

func (r *competeOnFirstRead) Read(p []byte) (int, error) {
	r.once.Do(r.hook)

	return r.ReadCloser.Read(p)
}

// TestSubtreeWriter_AKeyTakenWhileStreamingIsNotCreated pins put's three not-created answers
// against the real store. The other writer's bytes differ from this writer's on purpose, so
// the test can tell whose publish stood; in production the two would be the same bytes under
// the same content-addressed key.
func TestSubtreeWriter_AKeyTakenWhileStreamingIsNotCreated(t *testing.T) {
	ctx := context.Background()

	const fileType = fileformat.FileTypeSubtreeMeta

	theirs := []byte("published by the other writer")
	mine := bytes.Repeat([]byte("m"), 1<<20) // above the storer's 256KB buffer, so a pipe error reaches Write

	for name, beforeBody := range map[string]bool{
		"taken after the pre-check, refused at the publish": false,
		"taken before the pre-check, refused mid-write":     true,
	} {
		t.Run(name, func(t *testing.T) {
			plain := exclusiveFileStore(t)
			store := &competingStore{File: plain, beforeBody: beforeBody}

			// The hook runs on the file storer's goroutine, so it records and the test asserts.
			var competeErr error

			store.compete = func(key []byte, ft fileformat.FileType) {
				competeErr = plain.Set(ctx, key, ft, theirs)
			}

			w := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)
			_, root := streamTx(t, 1)

			created, err := w.put(ctx, *root, fileType, writeBytes(mine))
			require.NoError(t, competeErr, "the other writer's publish must succeed")
			require.NoError(t, err, "losing the key to another writer is not a failure")
			require.False(t, created, "the writer that lost the key did not create the blob")
			require.Empty(t, w.written, "and must not record it as its own")

			got, err := plain.Get(ctx, root[:], fileType)
			require.NoError(t, err)
			require.Equal(t, theirs, got, "the other writer's publish stands")

			require.NoError(t, w.DeleteAll(ctx))

			exists, err := plain.Exists(ctx, root[:], fileType)
			require.NoError(t, err)
			require.True(t, exists, "DeleteAll leaves the file it did not create")
		})
	}
}

// TestSubtreeWriter_TwoWritersOfOneSubtreeOverAFileStore is the end state the finding is
// about. Two writers emit one subtree; writer A is made to publish each streamed artefact
// after writer B has passed the store's pre-check for it, which is the race the pre-check
// cannot decide. Exactly one writer records each file as created, so exactly one DeleteAll
// removes it: the loser's leaves it, the winner's takes it. Reverting renameTempFile's link
// to a rename makes B overwrite A's meta and structure files and record them too.
func TestSubtreeWriter_TwoWritersOfOneSubtreeOverAFileStore(t *testing.T) {
	ctx := context.Background()
	plain := exclusiveFileStore(t)

	st, data, meta := oneSubtree(t, 8)
	root := st.RootHash()

	metaBytes, err := meta.Serialize()
	require.NoError(t, err)

	structureBytes, err := st.Serialize()
	require.NoError(t, err)

	a := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), plain, 800000)

	// The hook runs on the file storer's goroutine, so it records and the test asserts.
	var (
		competeErr error
		unexpected []fileformat.FileType
	)

	store := &competingStore{File: plain}
	store.compete = func(key []byte, ft fileformat.FileType) {
		var payload []byte

		switch ft {
		case fileformat.FileTypeSubtreeMeta:
			payload = metaBytes
		case fileformat.FileTypeSubtreeToCheck:
			payload = structureBytes
		default:
			// The data file is streamed into a pending file and never comes through
			// SetFromReader; if it did this test would no longer be racing what it claims.
			unexpected = append(unexpected, ft)

			return
		}

		created, putErr := a.put(ctx, *root, ft, writeBytes(payload))
		if putErr != nil && competeErr == nil {
			competeErr = putErr
		}

		if created {
			a.written = append(a.written, writtenSubtree{Hash: *root, FileType: ft})
		}
	}

	b := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000)

	sink, err := b.OpenData(ctx)(0)
	require.NoError(t, err)
	require.NotNil(t, sink, "the file store takes the data file before its key is known")
	require.NoError(t, data.WriteTransactionsToWriter(sink, 0, st.Length()))

	require.NoError(t, b.Emit(ctx)(0, st, data, meta))
	require.NoError(t, competeErr, "writer A's publishes must succeed")
	require.Empty(t, unexpected, "only the two small artefacts are streamed through SetFromReader")

	types := []fileformat.FileType{fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta, fileformat.FileTypeSubtreeToCheck}

	countCreated := func(w *subtreeWriter, ft fileformat.FileType) int {
		n := 0

		for _, entry := range w.written {
			if entry.FileType == ft && entry.Hash == *root {
				n++
			}
		}

		return n
	}

	for _, ft := range types {
		require.Equal(t, 1, countCreated(a, ft)+countCreated(b, ft), "exactly one writer created %s", ft)
	}

	require.Equal(t, 1, countCreated(b, fileformat.FileTypeSubtreeData), "B streamed the data file before A ran")
	require.Equal(t, 0, countCreated(b, fileformat.FileTypeSubtreeMeta)+countCreated(b, fileformat.FileTypeSubtreeToCheck), "B lost both streamed artefacts at the publish")

	// The writer that did not create a file leaves it; the one that did removes it.
	require.NoError(t, b.DeleteAll(ctx))

	for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeMeta, fileformat.FileTypeSubtreeToCheck} {
		exists, err := plain.Exists(ctx, root[:], ft)
		require.NoError(t, err)
		require.True(t, exists, "B's DeleteAll must leave A's %s", ft)
	}

	exists, err := plain.Exists(ctx, root[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.False(t, exists, "B's DeleteAll removes the data file B created")

	require.NoError(t, a.DeleteAll(ctx))

	for _, ft := range types {
		exists, err := plain.Exists(ctx, root[:], ft)
		require.NoError(t, err)
		require.False(t, exists, "%s is gone once its creator has deleted it", ft)
	}
}
