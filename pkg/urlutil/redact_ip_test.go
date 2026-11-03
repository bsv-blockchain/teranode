package urlutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactIPLiterals(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "dial error with RFC1918 address and port",
			in:   `Get "http://kafka:9092/x": dial tcp 10.0.0.4:9092: connect: connection refused`,
			want: `Get "http://kafka:9092/x": dial tcp <redacted-ip>: connect: connection refused`,
		},
		{
			name: "resolver address in a lookup error",
			in:   "dial tcp: lookup blob on 127.0.0.11:53: no such host",
			want: "dial tcp: lookup blob on <redacted-ip>: no such host",
		},
		{
			name: "address glued to a preceding word",
			in:   "failed:192.168.1.20, retrying",
			want: "failed:<redacted-ip>, retrying",
		},
		{
			name: "address at the end of a sentence",
			in:   "refused by 172.16.5.4.",
			want: "refused by <redacted-ip>.",
		},
		{
			name: "bracketed IPv6 with port",
			in:   "dial tcp [fd00::1:2]:8080: i/o timeout",
			want: "dial tcp <redacted-ip>: i/o timeout",
		},
		{
			name: "bracketed IPv6 with zone",
			in:   "dial tcp [fe80::1%eth0]:80: connect",
			want: "dial tcp <redacted-ip>: connect",
		},
		{
			name: "bare IPv6",
			in:   "no route to fd12:3456:789a::5",
			want: "no route to <redacted-ip>",
		},
		{
			name: "IPv4-mapped IPv6",
			in:   "peer ::ffff:10.1.2.3 closed",
			want: "peer <redacted-ip> closed",
		},
		{
			name: "loopback",
			in:   "connect to 127.0.0.1:8090 failed",
			want: "connect to <redacted-ip> failed",
		},
		{
			name: "several addresses",
			in:   "10.0.0.1 then 10.0.0.2:1",
			want: "<redacted-ip> then <redacted-ip>",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, RedactIPLiterals(tc.in))
		})
	}
}

// TestRedactIPLiteralsLeavesOtherTextAlone pins that over-matched shapes which are not
// addresses survive: catchup diagnostics carry timestamps, hashes, heights and
// versions, and an operator needs them intact.
func TestRedactIPLiteralsLeavesOtherTextAlone(t *testing.T) {
	for _, s := range []string{
		"",
		"http request [http://peer.example:8090/block/abc] returned status code [500]",
		"at 12:34:56 the header 000000000000000003a2f7c1b5e9d8f6a4c2e0b8d6f4a2c0e8b6d4f2a0c8e6d4 was invalid",
		"version 1.2.3 at height 875123",
		"ratio 3.5:1 over 10.5s",
		"dial tcp kafka:9092: connect: connection refused",
	} {
		require.Equal(t, s, RedactIPLiterals(s))
	}
}
