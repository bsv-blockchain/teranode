package teranodecli

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBuildPostgresConnString_QuotesValues covers #1877: a connection value
// containing a space, single quote or backslash must be quoted so it cannot
// truncate the keyword/value string or inject another keyword. Fails on the
// pre-fix code, which interpolated the raw values.
func TestBuildPostgresConnString_QuotesValues(t *testing.T) {
	// Password decodes to: p a's\s  (%20 space, %27 ' , %5C \)
	u, err := url.Parse("postgres://alice:p%20a%27s%5Cs@db.example:5433/mychain?sslmode=require")
	require.NoError(t, err)

	cs := buildPostgresConnString(u)

	// Each string value is single-quoted with ' -> \' and \ -> \\ (libpq rules);
	// port is an int and is not quoted.
	require.Contains(t, cs, `password='p a\'s\\s'`)
	require.Contains(t, cs, `host='db.example'`)
	require.Contains(t, cs, `dbname='mychain'`)
	require.Contains(t, cs, `user='alice'`)
	require.Contains(t, cs, `sslmode='require'`)
	require.Contains(t, cs, "port=5433")
}

// TestBuildPostgresConnString_NoPathDoesNotPanic covers #1877: a URL with no
// path must not panic. The pre-fix code did storeURL.Path[1:], which panics on
// an empty path ("postgres://host:5432").
func TestBuildPostgresConnString_NoPathDoesNotPanic(t *testing.T) {
	u, err := url.Parse("postgres://db.example:5432")
	require.NoError(t, err)

	require.NotPanics(t, func() {
		cs := buildPostgresConnString(u)
		require.Contains(t, cs, `dbname=''`)
	})
}
