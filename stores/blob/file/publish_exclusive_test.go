package file

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// With options.WithExclusivePublish a no-overwrite publish is exclusive: of two writers of one
// name, exactly one is told it created the blob. Without it the store behaves as it always
// has: the later of two writers past the pre-check replaces the earlier. The tests here drive
// the REAL file store in a temporary directory; nothing is faked. They are the detectors for
// the publish mechanism in renameTempFile, which the subtree writer's "records only files it
// created" rule rests on, and for the option leaving every other writer alone.

func exclusiveStore(t *testing.T) (*File, string) {
	t.Helper()

	dir := t.TempDir()
	u, err := url.Parse("file://" + dir)
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	return f, dir
}

// gatedReader is an io.ReadCloser whose first Read reports that it was reached, then waits to
// be released before yielding its body. SetFromReader only reads the body after errorOnOverwrite
// and createTempSibling, so a caller whose reader has started is past the pre-check.
type gatedReader struct {
	body      *bytes.Reader
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

func newGatedReader(body []byte) *gatedReader {
	return &gatedReader{body: bytes.NewReader(body), started: make(chan struct{}), release: make(chan struct{})}
}

func (r *gatedReader) Read(p []byte) (int, error) {
	r.startOnce.Do(func() { close(r.started) })
	<-r.release

	return r.body.Read(p)
}

func (r *gatedReader) Close() error { return nil }

func nlink(t *testing.T, path string) uint64 {
	t.Helper()

	info, err := os.Stat(path)
	require.NoError(t, err)

	st, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok, "expected a unix stat for %s", path)

	return uint64(st.Nlink) //nolint:unconvert // uint16 on darwin, wider elsewhere
}

func filesUnder(t *testing.T, dir string) []string {
	t.Helper()

	var found []string

	require.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			found = append(found, path)
		}

		return err
	}))

	return found
}

// TestSetFromReader_TwoWritersPastThePreCheckPublishExactlyOne is the gate for the exclusive
// publish. Both writers are held inside writeBody, so both have passed errorOnOverwrite's Stat
// and the pre-check cannot decide between them; only the publish can. Reverting renameTempFile's
// Link to Rename makes B's call return nil and Get return B's body.
func TestSetFromReader_TwoWritersPastThePreCheckPublishExactlyOne(t *testing.T) {
	ctx := context.Background()
	f, dir := exclusiveStore(t)

	key := []byte("two-writers-one-key")
	bodyA := bytes.Repeat([]byte("A"), 4096)
	bodyB := bytes.Repeat([]byte("B"), 4096)

	readerA := newGatedReader(bodyA)
	readerB := newGatedReader(bodyB)

	resultA := make(chan error, 1)
	resultB := make(chan error, 1)

	go func() {
		resultA <- f.SetFromReader(ctx, key, fileformat.FileTypeTesting, readerA, options.WithExclusivePublish())
	}()
	go func() {
		resultB <- f.SetFromReader(ctx, key, fileformat.FileTypeTesting, readerB, options.WithExclusivePublish())
	}()

	<-readerA.started
	<-readerB.started

	close(readerA.release)
	require.NoError(t, <-resultA, "the first writer to publish creates the blob")

	close(readerB.release)
	errB := <-resultB
	require.Error(t, errB, "the second writer must not be told it created the blob")
	require.True(t, errors.Is(errB, errors.ErrBlobAlreadyExists), "got %v", errB)

	got, err := f.Get(ctx, key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.Equal(t, bodyA, got, "the blob holds the winner's bytes")

	for _, path := range filesUnder(t, dir) {
		require.False(t, strings.HasSuffix(path, ".tmp"), "no temporary file may be left behind: %s", path)
	}

	filename, err := f.options.ConstructFilename(dir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.Equal(t, uint64(1), nlink(t, filename), "the published blob has one name")

	require.NoError(t, f.Del(ctx, key, fileformat.FileTypeTesting))
	require.Empty(t, filesUnder(t, dir), "Del removes the blob and leaves no second name behind")
}

// TestRenameTempFile_SecondNoOverwritePublishOfOneNameIsRefused drives the publish helper
// directly: the second temp file linked to a name that exists is refused at the link itself,
// the first file's content stands, and the refused temp file is left for its caller to remove.
func TestRenameTempFile_SecondNoOverwritePublishOfOneNameIsRefused(t *testing.T) {
	f, dir := exclusiveStore(t)

	final, err := f.options.ConstructFilename(dir, []byte("one-name"), fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(final), 0755))

	writeTemp := func(body string) string {
		file, tmp, err := f.createTempSibling(final, 0644)
		require.NoError(t, err)

		_, err = io.WriteString(file, body)
		require.NoError(t, err)
		require.NoError(t, f.syncAndCloseTempFile(file, tmp))

		return tmp
	}

	tmpA := writeTemp("A")
	tmpB := writeTemp("B")

	require.NoError(t, f.renameTempFile(tmpA, final, false, true))
	require.Equal(t, uint64(1), nlink(t, final), "the temporary name was unlinked after the publish")

	_, err = os.Stat(tmpA)
	require.True(t, os.IsNotExist(err), "the first temp name is gone")

	err = f.renameTempFile(tmpB, final, false, true)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlobAlreadyExists), "got %v", err)

	got, err := os.ReadFile(final)
	require.NoError(t, err)
	require.Equal(t, "A", string(got), "the first publish stands")

	_, err = os.Stat(tmpB)
	require.NoError(t, err, "the refused temp file is its caller's to remove")
}

// TestSetFromReader_WithoutExclusivePublishTheLaterWriterWins pins that the option is the only
// way in: a plain no-overwrite write keeps the store's behaviour from before the exclusive
// publish existed. Both writers are past the pre-check, both are told they wrote the blob, the
// later publish's bytes stand under one name, and no temporary file is left.
func TestSetFromReader_WithoutExclusivePublishTheLaterWriterWins(t *testing.T) {
	ctx := context.Background()
	f, dir := exclusiveStore(t)

	key := []byte("two-writers-no-option")
	bodyA := bytes.Repeat([]byte("A"), 4096)
	bodyB := bytes.Repeat([]byte("B"), 4096)

	readerA := newGatedReader(bodyA)
	readerB := newGatedReader(bodyB)

	resultA := make(chan error, 1)
	resultB := make(chan error, 1)

	go func() { resultA <- f.SetFromReader(ctx, key, fileformat.FileTypeTesting, readerA) }()
	go func() { resultB <- f.SetFromReader(ctx, key, fileformat.FileTypeTesting, readerB) }()

	<-readerA.started
	<-readerB.started

	close(readerA.release)
	require.NoError(t, <-resultA)

	close(readerB.release)
	require.NoError(t, <-resultB, "without the option a writer past the pre-check replaces the blob, as before")

	got, err := f.Get(ctx, key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.Equal(t, bodyB, got, "the later publish's bytes stand")

	for _, path := range filesUnder(t, dir) {
		require.False(t, strings.HasSuffix(path, ".tmp"), "no temporary file may be left behind: %s", path)
	}

	filename, err := f.options.ConstructFilename(dir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.Equal(t, uint64(1), nlink(t, filename), "the blob has one name")
}

// Without the option the publish helper renames, so a second publish of one name replaces the
// first and consumes its temporary name, exactly as before the exclusive publish existed.
func TestRenameTempFile_WithoutExclusivePublishASecondPublishReplaces(t *testing.T) {
	f, dir := exclusiveStore(t)

	final, err := f.options.ConstructFilename(dir, []byte("one-name-rename"), fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(final), 0755))

	for _, body := range []string{"A", "B"} {
		file, tmp, err := f.createTempSibling(final, 0644)
		require.NoError(t, err)

		_, err = io.WriteString(file, body)
		require.NoError(t, err)
		require.NoError(t, f.syncAndCloseTempFile(file, tmp))

		require.NoError(t, f.renameTempFile(tmp, final, false, false))

		_, err = os.Stat(tmp)
		require.True(t, os.IsNotExist(err), "the rename consumed the temporary name")
	}

	got, err := os.ReadFile(final)
	require.NoError(t, err)
	require.Equal(t, "B", string(got), "the second publish replaced the first")
	require.Equal(t, uint64(1), nlink(t, final))
}
