package util

import (
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/stretchr/testify/require"
)

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
