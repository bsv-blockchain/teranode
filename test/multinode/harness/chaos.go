//go:build network_chaos

package harness

import (
	"fmt"
	"testing"
)

// Isolate adds iptables DROP rules that block node from all other teranode
// containers. RPC from the host is unaffected. Requires passwordless sudo.
func (s *Stack) Isolate(t *testing.T, node int) {
	t.Helper()
	s.MustRun(t, "chaos", "isolate", fmt.Sprintf("%d", node))
}

// Heal clears iptables DROP rules on node. Passing node=0 heals every node.
func (s *Stack) Heal(t *testing.T, node int) {
	t.Helper()
	if node == 0 {
		s.MustRun(t, "chaos", "heal")
		return
	}
	s.MustRun(t, "chaos", "heal", fmt.Sprintf("%d", node))
}

// HealAll is a convenient alias for Heal(t, 0). Call from a defer/cleanup to
// guarantee no iptables rules survive a scenario.
func (s *Stack) HealAll(t *testing.T) { s.Heal(t, 0) }

// Kill stops node's container (docker stop). RPC becomes unreachable.
func (s *Stack) Kill(t *testing.T, node int) {
	t.Helper()
	s.MustRun(t, "chaos", "kill", fmt.Sprintf("%d", node))
}

// Start restarts a previously-killed container.
func (s *Stack) Start(t *testing.T, node int) {
	t.Helper()
	s.MustRun(t, "chaos", "start", fmt.Sprintf("%d", node))
}

// Pause freezes a running node via docker pause.
func (s *Stack) Pause(t *testing.T, node int) {
	t.Helper()
	s.MustRun(t, "chaos", "pause", fmt.Sprintf("%d", node))
}

// Unpause resumes a paused node.
func (s *Stack) Unpause(t *testing.T, node int) {
	t.Helper()
	s.MustRun(t, "chaos", "unpause", fmt.Sprintf("%d", node))
}

// Slow installs a tc netem qdisc adding the given latency (milliseconds) to
// node's egress traffic. Requires passwordless sudo.
func (s *Stack) Slow(t *testing.T, node, ms int) {
	t.Helper()
	s.MustRun(t, "chaos", "slow", fmt.Sprintf("%d", node), fmt.Sprintf("%d", ms))
}

// Unslow removes the tc netem qdisc installed by Slow.
func (s *Stack) Unslow(t *testing.T, node int) {
	t.Helper()
	s.MustRun(t, "chaos", "unslow", fmt.Sprintf("%d", node))
}
