package replayrecovery

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A journal must never replace a prior recovery or allow two concurrent writers.
func TestJournalExclusiveAndResumable(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	path := filepath.Join(dir, "journal.db")
	j, err := openJournal(path, false, "manifest-a", "store-a")
	require.NoError(t, err)
	_, err = openJournal(path, false, "manifest-a", "store-a")
	require.Error(t, err)
	_, err = openJournal(path, true, "manifest-a", "store-a")
	require.Error(t, err)
	require.NoError(t, j.put("checkpoint", []byte("durable")))
	require.NoError(t, j.Close())
	_, err = openJournal(path, true, "manifest-b", "store-a")
	require.Error(t, err)
	j, err = openJournal(path, true, "manifest-a", "store-a")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, j.Close()) })
	b, err := j.get("checkpoint")
	require.NoError(t, err)
	require.Equal(t, "durable", string(b))
}

// Symlink and permissive-directory checks protect before-images from accidental
// overwrite/disclosure and prevent opening a different journal via a link.
func TestJournalRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	path := filepath.Join(dir, "target")
	require.NoError(t, os.WriteFile(path, []byte("untouched"), 0600))
	link := filepath.Join(dir, "journal")
	require.NoError(t, os.Symlink(path, link))
	_, err := openJournal(link, true, "a", "b")
	require.Error(t, err)
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "untouched", string(b))
}
