package seeder

import (
	"net/url"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// syncFilesystem must succeed on a real directory and report a missing one:
// on Linux it is syncfs(2), which surfaces writeback errors.
func TestSyncFilesystem(t *testing.T) {
	require.NoError(t, syncFilesystem(t.TempDir()))
	require.Error(t, syncFilesystem(filepath.Join(t.TempDir(), "does-not-exist")))
}

func TestSeedingExternalStoreURL(t *testing.T) {
	parse := func(s string) *url.URL {
		u, err := url.Parse(s)
		require.NoError(t, err)

		return u
	}

	externalOf := func(u *url.URL) string {
		return u.Query().Get("externalStore")
	}

	t.Run("file store without fsyncMode gets the seeding mode and needs a final sync of its directory", func(t *testing.T) {
		in := parse("aerospike://h:3000/test?set=utxo&externalStore=file:///data/external")

		out, syncPath, err := seedingExternalStoreURL(in, "data")
		require.NoError(t, err)
		require.Equal(t, "/data/external", syncPath)
		require.Equal(t, "file:///data/external?fsyncMode=data", externalOf(out))
		require.Equal(t, "utxo", out.Query().Get("set"), "other params are kept")
		require.Equal(t, "file:///data/external", externalOf(in), "input URL is not mutated")
	})

	t.Run("relative file store path is resolved like the file blob store does", func(t *testing.T) {
		_, syncPath, err := seedingExternalStoreURL(parse("aerospike://h/test?externalStore=file://./data/externalStore"), "data")
		require.NoError(t, err)
		require.Equal(t, "data/externalStore", syncPath)
	})

	t.Run("operator-set fsyncMode is kept; a relaxed one still needs a final sync", func(t *testing.T) {
		out, syncPath, err := seedingExternalStoreURL(parse("aerospike://h/test?externalStore=file%3A%2F%2F%2Fd%3FfsyncMode%3Dnone"), "data")
		require.NoError(t, err)
		require.Equal(t, "/d", syncPath)
		require.Equal(t, "file:///d?fsyncMode=none", externalOf(out))

		out, syncPath, err = seedingExternalStoreURL(parse("aerospike://h/test?externalStore=file%3A%2F%2F%2Fd%3FfsyncMode%3Dfull"), "data")
		require.NoError(t, err)
		require.Empty(t, syncPath)
		require.Equal(t, "file:///d?fsyncMode=full", externalOf(out))
	})

	t.Run("full or empty seeding mode leaves the URL alone", func(t *testing.T) {
		for _, mode := range []string{"", "full"} {
			out, syncPath, err := seedingExternalStoreURL(parse("aerospike://h/test?externalStore=file:///d"), mode)
			require.NoError(t, err)
			require.Empty(t, syncPath)
			require.Equal(t, "file:///d", externalOf(out))
		}
	})

	t.Run("non-file external stores are untouched", func(t *testing.T) {
		out, syncPath, err := seedingExternalStoreURL(parse("aerospike://h/test?externalStore=s3://bucket/x"), "data")
		require.NoError(t, err)
		require.Empty(t, syncPath)
		require.Equal(t, "s3://bucket/x", externalOf(out))
	})

	t.Run("no external store is untouched", func(t *testing.T) {
		out, syncPath, err := seedingExternalStoreURL(parse("sqlitememory:///utxo"), "data")
		require.NoError(t, err)
		require.Empty(t, syncPath)
		require.Equal(t, "sqlitememory:///utxo", out.String())
	})

	t.Run("modes are case-insensitive, like the file blob store", func(t *testing.T) {
		out, syncPath, err := seedingExternalStoreURL(parse("aerospike://h/test?externalStore=file:///d"), "Data")
		require.NoError(t, err)
		require.Equal(t, "/d", syncPath)
		require.Equal(t, "file:///d?fsyncMode=data", externalOf(out))

		_, syncPath, err = seedingExternalStoreURL(parse("aerospike://h/test?externalStore=file%3A%2F%2F%2Fd%3FfsyncMode%3DFULL"), "data")
		require.NoError(t, err)
		require.Empty(t, syncPath, "an operator-set FULL is full")
	})

	t.Run("an invalid seeding mode is rejected", func(t *testing.T) {
		_, _, err := seedingExternalStoreURL(parse("aerospike://h/test?externalStore=file:///d"), "sometimes")
		require.Error(t, err)
	})
}

// resumeUnsafeAfterHostCrash drives the startup warning: only a file:// external
// store whose effective fsyncMode is none can keep partial blobs across a host
// crash that a re-run would then accept.
func TestResumeUnsafeAfterHostCrash(t *testing.T) {
	for in, want := range map[string]bool{
		"aerospike://h/test?externalStore=file%3A%2F%2F%2Fd%3FfsyncMode%3Dnone": true,
		"aerospike://h/test?externalStore=file%3A%2F%2F%2Fd%3FfsyncMode%3DNONE": true, // file store parses case-insensitively
		"aerospike://h/test?externalStore=file%3A%2F%2F%2Fd%3FfsyncMode%3Ddata": false,
		"aerospike://h/test?externalStore=file:///d":                            false,
		"aerospike://h/test?externalStore=s3://bucket/x":                        false,
		"sqlitememory:///utxo": false,
	} {
		u, err := url.Parse(in)
		require.NoError(t, err)
		require.Equal(t, want, resumeUnsafeAfterHostCrash(u), in)
	}
}
