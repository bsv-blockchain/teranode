//go:build network_chaos

package multinodesplit

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
	"github.com/stretchr/testify/require"
)

// TestNetworkPartitionReorg is the suite's first scenario to use the
// network-layer chaos primitives (iptables-based Isolate / Heal) rather
// than container-level kill / pause. It validates the most important
// consensus-layer property the per-service scenarios do NOT touch: that a
// minority partition correctly reorgs onto the majority chain when the
// network heals.
//
// Shape:
//
//  1. Reset to a converged 3-node baseline at height H, tip T.
//  2. Isolate node 3 (iptables DROP rules block node 3 from talking to
//     nodes 1 and 2; host RPC remains reachable, so we can still drive
//     Generate on node 3 from the test).
//  3. Mine 5 blocks on node 1. Nodes 1 and 2 (still meshed) reach H+5
//     on the "majority" chain.
//  4. Mine 3 blocks on the now-solo node 3. Node 3 reaches H+3 on a
//     divergent "minority" chain (different coinbase, different timestamps,
//     different block hashes from the majority chain).
//  5. Sanity-check: node 1 tip and node 3 tip differ, and both are
//     non-baseline. This rules out "the partition didn't actually take
//     effect" before we assert anything about reorg.
//  6. Heal node 3 (clear iptables). Node 3 rejoins the mesh and learns
//     about the majority chain.
//  7. Wait for node 3 to reorg: its tip should switch from its
//     H+3 minority tip to node 1's H+5 majority tip, because the
//     majority chain has more cumulative proof-of-work. Node 3's three
//     locally-mined blocks become orphans.
//  8. All three nodes converge on the majority tip.
//
// Requires passwordless sudo (iptables); skipped otherwise via RequireSudo.
// A defer s.HealAll guarantees iptables rules are torn down even if an
// assertion fails — leaving rules behind would silently break every
// subsequent scenario.
func TestNetworkPartitionReorg(t *testing.T) {
	s := stack()
	s.RequireSudo(t)
	s.Reset(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	node1 := s.Node(1)
	node2 := s.Node(2)
	node3 := s.Node(3)
	all := s.Nodes()

	info, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	baselineHeight := info.Blocks
	baselineTip := info.BestBlockHash
	t.Logf("baseline: height=%d tip=%s", baselineHeight, short(baselineTip))

	t.Log("isolating teranode3 from teranode1+teranode2 (iptables)...")
	s.Isolate(t, 3)
	// HealAll ensures no iptables rules survive this scenario, even if a
	// require below fails. Rules left in place would silently break every
	// subsequent test that depends on a connected mesh.
	defer s.HealAll(t)

	// Mine on the majority side first so nodes 1 and 2 both clearly
	// commit blocks while node 3 is genuinely cut off.
	t.Log("mining 5 blocks on teranode1 (majority side)...")
	_, err = node1.Generate(ctx, 5)
	require.NoError(t, err, "node 1 generate")

	harness.WaitForHeight(t, node1, baselineHeight+5, 1*time.Minute)
	harness.WaitForHeight(t, node2, baselineHeight+5, 1*time.Minute)
	majorityInfo, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	majorityTip := majorityInfo.BestBlockHash
	t.Logf("majority side at height=%d tip=%s", majorityInfo.Blocks, short(majorityTip))

	// Mine on the isolated minority side. Generate is local; node 3
	// produces blocks without needing peers.
	t.Log("mining 3 blocks on teranode3 (minority side, isolated)...")
	_, err = node3.Generate(ctx, 3)
	require.NoError(t, err, "node 3 generate while isolated")

	harness.WaitForHeight(t, node3, baselineHeight+3, 1*time.Minute)
	minorityInfo, err := node3.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	minorityTip := minorityInfo.BestBlockHash
	t.Logf("minority side at height=%d tip=%s", minorityInfo.Blocks, short(minorityTip))

	// Sanity: partition must have actually severed the mesh. If majority
	// and minority tips matched, either Isolate is a no-op or the
	// scenario is misconfigured — either way nothing below is meaningful.
	require.NotEqual(t, majorityTip, minorityTip,
		"majority and minority tips should diverge while node 3 is isolated; otherwise the partition didn't take effect")
	require.NotEqual(t, baselineTip, majorityTip, "majority should have advanced past baseline")
	require.NotEqual(t, baselineTip, minorityTip, "minority should have advanced past baseline")

	t.Log("healing teranode3 (clearing iptables)...")
	s.Heal(t, 3)

	// The majority chain has more cumulative work (5 blocks vs 3), so on
	// rejoin node 3 must abandon its minority fork and adopt the majority
	// tip. WaitForConverged polls every node until they all report the
	// same best-block hash.
	t.Log("waiting for teranode3 to reorg onto the majority chain...")
	converged := harness.WaitForConverged(t, all, 2*time.Minute)
	require.Equal(t, majorityTip, converged,
		"all 3 nodes should converge on the majority tip after node 3 reorgs (more work wins)")
	t.Logf("all 3 nodes converged at majority tip %s after reorg", short(converged))

	// Final assertion on heights: convergence at the majority tip implies
	// node 3's three minority blocks are now orphans and node 3 sits at
	// majorityHeight, not minorityHeight.
	finalInfo, err := node3.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, majorityInfo.Blocks, finalInfo.Blocks,
		"teranode3 height should match the majority chain after reorg")
}
