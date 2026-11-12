package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoinPeerURL_KeepsBasePath(t *testing.T) {
	cases := map[string]struct {
		base string
		elem []string
		want string
	}{
		"api prefix": {
			base: "https://peer.example/api/v1",
			elem: []string{"subtree", "abcd", "txs"},
			want: "https://peer.example/api/v1/subtree/abcd/txs",
		},
		"trailing slash on base": {
			base: "http://peer.example:8090/api/v1/",
			elem: []string{"block", "abcd"},
			want: "http://peer.example:8090/api/v1/block/abcd",
		},
		"no base path": {
			base: "http://10.0.0.5:8090",
			elem: []string{"subtree_data", "abcd"},
			want: "http://10.0.0.5:8090/subtree_data/abcd",
		},
		"uppercase scheme": {
			base: "HTTP://peer.example/api/v1",
			elem: []string{"blocks", "abcd"},
			want: "http://peer.example/api/v1/blocks/abcd",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := JoinPeerURL(tc.base, tc.elem...)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestJoinPeerURL_RejectsSmugglingBases covers issue 4843: a base carrying a query turned
// the appended protocol path into query data, so the request's effective path was the one
// the peer chose.
func TestJoinPeerURL_RejectsSmugglingBases(t *testing.T) {
	cases := map[string]struct {
		base   string
		reason string
	}{
		"query suffix from the audit": {"http://attacker.example:9644/v1/debug/bundle?x=", "query"},
		"bare question mark":          {"http://attacker.example:9644/v1/debug/bundle?", "query"},
		"fragment":                    {"http://attacker.example/v1/debug/bundle#", "fragment"},
		"fragment with value":         {"http://attacker.example/v1/debug/bundle#x", "fragment"},
		"userinfo":                    {"http://user:pass@peer.example/api/v1", "userinfo"},
		"non http scheme":             {"file:///etc/passwd", "scheme"},
		"legacy sentinel":             {"legacy", "scheme"},
		"empty":                       {"", "scheme"},
		"opaque":                      {"http:peer.example/api", "host"},
		"no host":                     {"http:///api/v1", "host"},
		"unparseable":                 {"http://peer.example/%zz", "invalid"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := JoinPeerURL(tc.base, "subtree", "abcd", "txs")
			require.Error(t, err)
			require.Empty(t, got)
			require.Contains(t, err.Error(), tc.reason)
			require.NoError(t, ValidatePeerBaseURL("https://peer.example/api/v1"))
			require.Error(t, ValidatePeerBaseURL(tc.base))
		})
	}
}

// TestJoinPeerURL_ErrorDoesNotEchoCredentials keeps a rejected base's userinfo out of the
// error, which callers log.
func TestJoinPeerURL_ErrorDoesNotEchoCredentials(t *testing.T) {
	_, err := JoinPeerURL("http://user:s3cret@peer.example/api/v1?x=", "block", "abcd")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "s3cret")
}
