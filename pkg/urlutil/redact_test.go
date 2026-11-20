package urlutil

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
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
		// The same plain address percent-encoded is masked the same way, so
		// encoding is not a way round the guard.
		{"encoded @ in a plain query value is over-redacted", "http://blob:8080/x?owner=a%40b", "<unparseable url>"},
		// The guard reads the escaped path and fragment, so a correctly
		// percent-encoded "@" there is not mistaken for a misread authority.
		{"encoded @ in the path passes", "http://host/a%40b", "http://host/a%40b"},
		{"encoded @ in the fragment passes", "http://h/p#f%40g", "http://h/p#f%40g"},
		// An encoded "@" inside the password does not hide the real one, which
		// is raw by definition, in the path or the fragment.
		{"encoded @ before the real @, path", "postgres://user:2024/a%40b@db:5432/x", "<unparseable url>"},
		{"encoded @ before the real @, fragment", "http://teranode:8080#s3%40c@blob:8080/x", "<unparseable url>"},
		// externalStore carries a whole blob store URL, and an http blob store
		// sends its userinfo as a Basic header. Once the nested URL is
		// percent-encoded its "@" is no longer raw, so the guard above never
		// fires and only the nested redaction stands between it and the log.
		{
			"nested store URL with an encoded @",
			"aerospike://h:3000/ns?set=utxo&externalStore=http://user:hunter2%40blob:8080/x",
			"aerospike://h:3000/ns?set=utxo&externalStore=http%3A%2F%2Fuser%3Axxxxx%40blob%3A8080%2Fx",
		},
		{
			"nested store URL fully encoded",
			"aerospike://h:3000/ns?set=utxo&externalStore=http%3A%2F%2Fuser%3Ahunter2%40blob%3A8080%2Fx%3Fbatch%3Dtrue%26sizeInBytes%3D100",
			"aerospike://h:3000/ns?set=utxo&externalStore=http%3A%2F%2Fuser%3Axxxxx%40blob%3A8080%2Fx%3Fbatch%3Dtrue%26sizeInBytes%3D100",
		},
		{
			"outer and nested credentials",
			"aerospike://u:outer%2Fpw@h:3000/ns?externalStore=http%3A%2F%2Fuser%3Ahunter2%40blob%3A8080%2Fx",
			"aerospike://u:xxxxx@h:3000/ns?externalStore=http%3A%2F%2Fuser%3Axxxxx%40blob%3A8080%2Fx",
		},
		// A nested URL with a username and no password has nothing to mask, so
		// its bytes are left exactly as configured.
		{
			"nested store URL with username only",
			"aerospike://h:3000/ns?externalStore=http://user%40blob:8080/x",
			"aerospike://h:3000/ns?externalStore=http://user%40blob:8080/x",
		},
		// A "+" or "%20" decodes to a space, which ends the nested URL token
		// before the "@", so RedactText would leave the rest of the password
		// behind. The whole URL is masked instead.
		{"nested password with a space", "aerospike://h:3000/ns?externalStore=http://u:pa+ss%40blob/x", "<unparseable url>"},
		// url.Values drops a pair that will not decode, so its consumer never
		// sees it, but its raw bytes would still reach the log.
		{"malformed escape in the query", "aerospike://h:3000/ns?externalStore=http://u:pw%zz%40blob/x", "<unparseable url>"},
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

// TestRedactLeavesCommittedQueriesAlone runs the nested-URL query redaction
// over every URL in the committed settings.conf. None of them carries a
// credential in its query, so each query must come back byte for byte: the
// redaction must cost an operator nothing on a URL it has nothing to mask.
func TestRedactLeavesCommittedQueriesAlone(t *testing.T) {
	conf, err := os.ReadFile(filepath.Join("..", "..", "settings.conf"))
	require.NoError(t, err)

	checked := 0

	// The query is cut out of the raw token rather than taken from a parsed
	// URL, because many committed values hold ${...} placeholders that do not
	// parse until they are expanded, and the expanded value keeps the query.
	for _, token := range urlToken.FindAllString(string(conf), -1) {
		_, rawQuery, hasQuery := strings.Cut(token, "?")
		if !hasQuery {
			continue
		}

		rawQuery, _, _ = strings.Cut(rawQuery, "#")

		got, ok := redactQuery(rawQuery)
		require.True(t, ok, "query of %q was refused", token)
		require.Equal(t, rawQuery, got, "query of %q was rewritten", token)

		checked++
	}

	// The externalStore and kafka URLs alone are well over this, so a smaller
	// count means the scan stopped finding them rather than that they are fine.
	require.Greater(t, checked, 20, "too few committed URLs with a query were checked")
}
