//go:build network_chaos

package multinode

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
	"github.com/stretchr/testify/require"
)

// TestSplitBrainAndHeal exercises the canonical network-partition invariant:
// a partitioned node mines its own minority chain, and once the partition
// heals, every node converges on the longer (majority) chain. This is the
// highest-value assertion in the suite because it's impossible to make with
// the in-process TestDaemon tests (those share memory and can't be
// iptables'd apart).
//
// Shape:
//   - Reset(t) puts all 5 nodes at a converged baseline.
//   - `chaos isolate 3` cuts node 3 off from the peer mesh (RPC still works).
//   - Majority side (via node 1) mines 5 more blocks.
//   - Minority side (node 3, alone in its partition) mines 2 blocks.
//   - After heal, all 5 nodes must agree on the majority tip.
func TestSplitBrainAndHeal(t *testing.T) {
	s := stack()
	s.RequireSudo(t)
	s.Reset(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	node1 := s.Node(1)
	node3 := s.Node(3)
	all := s.Nodes()

	// Baseline: whatever tip/height everyone agrees on right now.
	info, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	baselineHeight := info.Blocks
	t.Logf("baseline: height=%d tip=%s", baselineHeight, short(info.BestBlockHash))

	// Partition node 3 off the mesh. RPC still answers so we can still mine
	// on it; what's blocked is peer-to-peer gossip.
	t.Log("isolating teranode3...")
	s.Isolate(t, 3)

	// Majority mines 5 -> expected height baselineHeight + 5.
	t.Log("mining 5 on majority (teranode1)...")
	_, err = node1.Generate(ctx, 5)
	require.NoError(t, err, "majority generate on node 1")

	// Minority mines 2 -> expected height baselineHeight + 2, different tip.
	t.Log("mining 2 on isolated minority (teranode3)...")
	_, err = node3.Generate(ctx, 2)
	require.NoError(t, err, "minority generate on node 3")

	// Wait for majority nodes to see the 5 extra blocks.
	majority := []*harness.RPCClient{s.Node(1), s.Node(2), s.Node(4), s.Node(5)}
	harness.WaitForHeightOn(t, majority, baselineHeight+5, 2*time.Minute)

	majorityInfo, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	majorityTip := majorityInfo.BestBlockHash
	require.Equal(t, baselineHeight+5, majorityInfo.Blocks, "majority tip height")

	minorityInfo, err := node3.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, baselineHeight+2, minorityInfo.Blocks, "minority tip height")
	require.NotEqual(t, majorityTip, minorityInfo.BestBlockHash, "partitioned chains must diverge")

	// Heal the partition and assert reorg.
	t.Log("healing teranode3...")
	s.Heal(t, 3)

	converged := harness.WaitForConverged(t, all, 2*time.Minute)
	require.Equal(t, majorityTip, converged, "all nodes should converge on majority tip")
	t.Logf("converged at %s", short(converged))
}

// short is a tiny helper to keep log lines readable. We redefine it per-file
// rather than exporting the harness one to avoid dragging test-only symbols
// into the public API.
func short(s string) string {
	if len(s) < 12 {
		return s
	}
	return s[:8] + "..."
}
