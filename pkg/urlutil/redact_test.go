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
		// The authority ends at the first "/", "?" or "#". Preferring "/" over
		// "?" put the query string into the host here.
		{
			"multi-host, slash inside the query",
			"http://u:p@h1:8080,h2:8080?ids=a,b/c",
			"http://u:xxxxx@h1:8080,h2:8080?ids=a,b/c",
		},
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
