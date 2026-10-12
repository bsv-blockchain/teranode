package util

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/stretchr/testify/require"
)

func TestRedactURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"user+password", "aerospike://user:secret@host:3000/ns", "aerospike://user:REDACTED@host:3000/ns"},
		{"user only", "aerospike://user@host:3000", "aerospike://user@host:3000"},
		{"no userinfo", "aerospike://host:3000", "aerospike://host:3000"},
		{"empty", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := url.Parse(tc.in)
			require.NoError(t, err, "test setup: url.Parse failed")

			got := redactURL(parsed)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRedactURLNil(t *testing.T) {
	require.Equal(t, "<nil>", redactURL(nil))
}

func TestRedactURLDoesNotMutateInput(t *testing.T) {
	u, err := url.Parse("aerospike://user:secret@host:3000/ns")
	require.NoError(t, err)

	_ = redactURL(u)

	require.NotNil(t, u.User)
	pwd, ok := u.User.Password()
	require.True(t, ok)
	require.Equal(t, "secret", pwd, "redactURL must not mutate caller's URL")
}

func TestAerospikePolicySummary(t *testing.T) {
	p := aerospike.NewClientPolicy()
	p.User = "u"
	p.Password = "hunter2"
	p.ConnectionQueueSize = 50
	p.Timeout = 5 * time.Second

	out := aerospikePolicySummary(p)

	require.NotContains(t, out, "hunter2")
	require.Contains(t, out, `User:"u"`)
	require.Contains(t, out, "ConnectionQueueSize:50")
	require.True(t, strings.Contains(out, "Password:***"), "expected Password:*** placeholder, got %q", out)
}

func TestAerospikePolicySummaryNil(t *testing.T) {
	require.Equal(t, "<nil>", aerospikePolicySummary(nil))
}
