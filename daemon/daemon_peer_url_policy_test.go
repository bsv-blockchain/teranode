package daemon

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// capturingT collects what a ulogger writes, so a test can assert on the line itself
// rather than on the fact that a call was made.
type capturingT struct {
	mu    sync.Mutex
	lines []string
}

func (c *capturingT) Logf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lines = append(c.lines, fmt.Sprintf(format, args...))
}

func (c *capturingT) Errorf(format string, args ...interface{}) { c.Logf(format, args...) }

func (c *capturingT) FailNow() {}

func (c *capturingT) output() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return strings.Join(c.lines, "\n")
}

// TestApplyPeerURLPolicy_FollowsAllowPrivateIPs pins the wiring: p2p_allow_private_ips is
// what lets a peer-supplied hostname resolve to a private-network address (issue 4843).
func TestApplyPeerURLPolicy_FollowsAllowPrivateIPs(t *testing.T) {
	orig := util.SSRFAllowPrivateNetworks()
	defer util.SetSSRFAllowPrivateNetworks(orig)

	for _, allowed := range []bool{true, false, true} {
		applyPeerURLPolicy(ulogger.TestLogger{}, &settings.Settings{P2P: settings.P2PSettings{AllowPrivateIPs: allowed}})
		require.Equal(t, allowed, util.SSRFAllowPrivateNetworks())
	}
}

// TestApplyPeerURLPolicy_WarnsWhenPrivateNetworksRefused asserts the operator-visible signal
// exists, because the failure it explains is otherwise silent above debug level: peers
// reachable only over a private network fail their availability probe and drop out of
// selection, and the node simply stops catching up.
func TestApplyPeerURLPolicy_WarnsWhenPrivateNetworksRefused(t *testing.T) {
	orig := util.SSRFAllowPrivateNetworks()
	defer util.SetSSRFAllowPrivateNetworks(orig)

	refusing := &capturingT{}
	applyPeerURLPolicy(
		ulogger.NewUnifiedTestLogger(refusing, t.Name(), "daemon"),
		&settings.Settings{P2P: settings.P2PSettings{AllowPrivateIPs: false}},
	)

	require.Contains(t, refusing.output(), "p2p_allow_private_ips")
	require.Contains(t, refusing.output(), "WARN")

	allowing := &capturingT{}
	applyPeerURLPolicy(
		ulogger.NewUnifiedTestLogger(allowing, t.Name(), "daemon"),
		&settings.Settings{P2P: settings.P2PSettings{AllowPrivateIPs: true}},
	)

	require.NotContains(t, allowing.output(), "p2p_allow_private_ips")
}
