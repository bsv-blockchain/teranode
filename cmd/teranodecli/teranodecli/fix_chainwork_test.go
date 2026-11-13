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

	cs, err := buildPostgresConnString(u)
	require.NoError(t, err)

	// Each string value is single-quoted with ' -> \' and \ -> \\ (libpq rules);
	// port is an int and is not quoted.
	require.Contains(t, cs, `password='p a\'s\\s'`)
	require.Contains(t, cs, `host='db.example'`)
	require.Contains(t, cs, `dbname='mychain'`)
	require.Contains(t, cs, `user='alice'`)
	require.Contains(t, cs, `sslmode='require'`)
	require.Contains(t, cs, "port=5433")
}

// TestBuildPostgresConnString_NoDatabaseNameIsAnError covers #1877 and the
// review of #1886: a URL with no database name must not panic (the pre-fix code
// did storeURL.Path[1:]) and must not build dbname=”, which Postgres resolves
// to the user name and which overrides PGDATABASE, so the command would write
// chainwork into a database the caller never named.
func TestBuildPostgresConnString_NoDatabaseNameIsAnError(t *testing.T) {
	for _, raw := range []string{
		"postgres://db.example:5432",
		"postgres://teranode:pw@db.example:5432",
		"postgres://teranode:pw@db.example:5432/",
	} {
		u, err := url.Parse(raw)
		require.NoError(t, err)

		require.NotPanics(t, func() {
			cs, err := buildPostgresConnString(u)
			require.Error(t, err, raw)
			require.Contains(t, err.Error(), "no database name")
			require.Empty(t, cs)
			// The message names the format, never the URL (it can carry a password).
			require.NotContains(t, err.Error(), "pw")
		}, raw)
	}
}
