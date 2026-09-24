package urlutil

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedact(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"user and password", "postgres://teranode:hunter2@db:5432/blockchain", "postgres://teranode:xxxxx@db:5432/blockchain"},
		{"password only, empty username", "http://:hunter2@blobserver:8080/", "http://:xxxxx@blobserver:8080/"},
		{"empty password is still userinfo", "http://teranode:@blobserver:8080/", "http://teranode:xxxxx@blobserver:8080/"},
		{"username only", "aerospike://teranode@host:3000/ns", "aerospike://teranode@host:3000/ns"},
		{"no userinfo", "aerospike://host:3000/ns", "aerospike://host:3000/ns"},
		{"query preserved", "http://u:p@blobserver:8080/blob?hashPrefix=2", "http://u:xxxxx@blobserver:8080/blob?hashPrefix=2"},
		{"percent-encoded password", "postgres://u:p%40ss%3Aword@db:5432/x", "postgres://u:xxxxx@db:5432/x"},
		{"empty string", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := url.Parse(tc.in)
			require.NoError(t, err, "test setup: url.Parse failed")

			require.Equal(t, tc.want, Redact(parsed))
		})
	}
}

func TestRedactNil(t *testing.T) {
	require.Equal(t, "<nil>", Redact(nil))
}

func TestRedactDoesNotMutateInput(t *testing.T) {
	u, err := url.Parse("postgres://teranode:hunter2@db:5432/blockchain")
	require.NoError(t, err)

	_ = Redact(u)

	require.NotNil(t, u.User)

	pwd, ok := u.User.Password()
	require.True(t, ok)
	require.Equal(t, "hunter2", pwd, "Redact must not mutate the caller's URL")
}

func TestRedactString(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"user and password", "kafka://teranode:hunter2@broker:9092/txs", "kafka://teranode:xxxxx@broker:9092/txs"},
		{"multi-host", "kafka://u:p@broker1:9092,broker2:9092/txs", "kafka://u:xxxxx@broker1:9092,broker2:9092/txs"},
		{"no userinfo", "kafka://broker:9092/txs", "kafka://broker:9092/txs"},
		{"empty string", "", ""},
		// A raw "@" inside the password used to split the authority at the
		// first "@" rather than the last, so "cret" escaped into the host and
		// was printed by the helper that exists to mask it.
		{
			"multi-host, raw @ in password",
			"aerospike://user:se@cret@h1:3000,h2:3000/ns?set=x",
			"aerospike://user:xxxxx@h1:3000,h2:3000/ns?set=x",
		},
		// A raw "," inside the password used to start the host list inside
		// the password, so the URL either failed to parse or parsed with the
		// credential dropped.
		{
			"multi-host, raw comma in password",
			"kafka://user:hun,ter2@h1:9092,h2:9092/t",
			"kafka://user:xxxxx@h1:9092,h2:9092/t",
		},
		// The authority ends at the first "/", "?" or "#". Preferring "/" over
		// "?" put the query string into the host here.
		{
			"multi-host, slash inside the query",
			"http://u:p@h1:8080,h2:8080?ids=a,b/c",
			"http://u:xxxxx@h1:8080,h2:8080?ids=a,b/c",
		},
		// net/url ends the authority at the first "/", "?" or "#" before it
		// looks for the "@". When the password segment before that character
		// is empty or all digits, the port check passes, url.Parse succeeds
		// with no userinfo, and the rest of the password lands in the path,
		// query or fragment. Redacted() then had nothing to mask and printed
		// the whole credential.
		{"raw slash in password, empty first segment", "postgres://user:/Qw7+Lm2=@db:5432/teranode", "<unparseable url>"},
		{"raw ? in password, numeric first segment", "postgres://user:2024?Qw7@db:5432/teranode", "<unparseable url>"},
		{"raw # in password, numeric first segment", "http://teranode:8080#s3cret@blob:8080/x", "<unparseable url>"},
		// The same misread through the comma path: the authority ends at the
		// "/" in the password, so ParseMultiHostURL sees no comma in the host
		// and hands the whole string to url.Parse.
		{"multi-host, raw slash in password", "kafka://user:2024/pw@h1:9092,h2:9092/topic", "<unparseable url>"},
		// With a raw "@" before the delimiter, url.Parse finds userinfo, but
		// it ends at that first "@", so Redacted() masked "p" and printed
		// "ss?word".
		{"raw @ then ? in password", "postgres://user:p@ss?word@db/x", "<unparseable url>"},
		// The price of the guard: a legitimate "@" after the authority is
		// masked too. That errs in the safe direction for a log line.
		{"@ in the query is over-redacted", "http://blob:8080/x?owner=a@b", "<unparseable url>"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, RedactString(tc.in))
		})
	}
}

// A string that will not parse as a URL must not be echoed back, because an
// unparseable string may still hold a credential.
func TestRedactStringUnparseable(t *testing.T) {
	require.Equal(t, "<unparseable url>", RedactString("://not a url\x7f"))
}

// TestParseErrorReasonQuotesNoInput feeds url.Parse the shapes whose own
// reason quotes part of the password and asserts that the reason
// ParseErrorReason returns carries none of it.
func TestParseErrorReasonQuotesNoInput(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		leaked string
		want   string
	}{
		{"raw slash in password", "postgres://user:canary-slash/rest@db:5432/teranode", "canary-slash", "malformed URL"},
		{"raw ? in password", "http://teranode:canary-query?rest@blob:8080/x", "canary-query", "malformed URL"},
		{"raw # in password", "http://teranode:canary-hash#rest@blob:8080/x", "canary-hash", "malformed URL"},
		{"stray percent in password", "postgres://user:canary%zzrest@host/db", "%zz", "malformed URL"},
		{"space in host", "http://teranode:canary-host@blob server:8080/x", `" "`, "invalid character in host name"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := url.Parse(tc.in)
			require.Error(t, err, "test setup: url.Parse accepted the input")

			var urlErr *url.Error
			require.ErrorAs(t, err, &urlErr)
			require.Contains(t, urlErr.Err.Error(), tc.leaked, "test setup: net/url no longer quotes this part of the input, so this case checks nothing")

			reason := ParseErrorReason(err).Error()
			require.NotContains(t, reason, tc.leaked)
			require.NotContains(t, reason, `"`, "the reason quotes part of its input")
			require.Contains(t, reason, tc.want)
		})
	}
}
