package replayrecovery

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJournalDeleteCommitsSyncDirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	path := filepath.Join(dir, "journal.sqlite")
	for _, resume := range []bool{false, true} {
		j, err := openJournal(path, resume, "manifest", "store")
		require.NoError(t, err)
		var mode string
		var synchronous int
		require.NoError(t, j.db.QueryRow("PRAGMA journal_mode").Scan(&mode))
		require.NoError(t, j.db.QueryRow("PRAGMA synchronous").Scan(&synchronous))
		require.Equal(t, "delete", mode)
		require.Equal(t, 3, synchronous, "EXTRA must sync rollback-journal deletion before a remote mutation")
		require.NoError(t, j.Close())
	}
}

func journalFixture(t *testing.T, statements string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	path := filepath.Join(dir, "journal.sqlite")
	require.NoError(t, os.WriteFile(path, nil, 0600))
	if statements != "" {
		db, err := sql.Open("sqlite", path)
		require.NoError(t, err)
		_, err = db.Exec(statements)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	}
	return path
}

func TestJournalResumeInterruptedInitialization(t *testing.T) {
	for name, statements := range map[string]string{
		"file created":               "",
		"empty database":             "VACUUM",
		"legacy schema committed":    "CREATE TABLE state (key TEXT PRIMARY KEY, value BLOB NOT NULL)",
		"initialization rolled back": "BEGIN; CREATE TABLE state (key TEXT PRIMARY KEY, value BLOB NOT NULL); INSERT INTO state VALUES ('header', '[\"replay-recovery-journal-v1\",\"manifest\",\"store\"]'); ROLLBACK",
	} {
		t.Run(name, func(t *testing.T) {
			path := journalFixture(t, statements)
			j, err := openJournal(path, true, "manifest", "store")
			require.NoError(t, err)
			header, err := j.get("header")
			require.NoError(t, err)
			require.JSONEq(t, `["replay-recovery-journal-v1","manifest","store"]`, string(header))
			require.NoError(t, j.put("step/delete/child", []byte(`{"kind":"delete","done":false}`)))
			require.NoError(t, j.Close())
			j, err = openJournal(path, true, "manifest", "store")
			require.NoError(t, err)
			intent, err := j.get("step/delete/child")
			require.NoError(t, err)
			require.JSONEq(t, `{"kind":"delete","done":false}`, string(intent))
			require.NoError(t, j.Close())
			_, err = openJournal(path, true, "other-manifest", "store")
			require.Error(t, err)
		})
	}
}

func TestJournalResumeRefusesUnsafeInitialization(t *testing.T) {
	const state = "CREATE TABLE state (key TEXT PRIMARY KEY, value BLOB NOT NULL); "
	for name, statements := range map[string]string{
		"intent without header":     state + "INSERT INTO state VALUES ('step/delete/child', '{\"kind\":\"delete\"}')",
		"outcome without header":    state + "INSERT INTO state VALUES ('latest/child', 'null')",
		"checkpoint without header": state + "INSERT INTO state VALUES ('validated', 'true')",
		"malformed header":          state + "INSERT INTO state VALUES ('header', '')",
		"foreign header":            state + "INSERT INTO state VALUES ('header', '[\"replay-recovery-journal-v1\",\"other\",\"store\"]')",
		"foreign table":             "CREATE TABLE unrelated (value TEXT)",
		"foreign table with state":  state + "CREATE TABLE unrelated (value TEXT)",
		"foreign index":             state + "CREATE INDEX unrelated ON state(value)",
		"foreign trigger":           state + "CREATE TRIGGER unrelated AFTER INSERT ON state BEGIN DELETE FROM state; END",
		"foreign state schema":      "CREATE TABLE state (key TEXT PRIMARY KEY, value BLOB)",
		"foreign application":       "PRAGMA application_id=42",
		"foreign version":           "PRAGMA user_version=1",
	} {
		t.Run(name, func(t *testing.T) {
			path := journalFixture(t, statements)
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			_, err = openJournal(path, true, "manifest", "store")
			require.Error(t, err)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after, "refusal must preserve existing journal contents")
		})
	}
	t.Run("corrupt database", func(t *testing.T) {
		path := journalFixture(t, "")
		require.NoError(t, os.WriteFile(path, []byte("not a SQLite database"), 0600))
		_, err := openJournal(path, true, "manifest", "store")
		require.Error(t, err)
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "not a SQLite database", string(b))
	})
}

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
