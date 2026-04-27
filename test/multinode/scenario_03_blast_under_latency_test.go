//go:build network_chaos

package multinode

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
	"github.com/stretchr/testify/require"
)

// TestSlowPeerPropagation adds 300ms egress latency to one node and
// interleaves mining on two others. Assertions:
//
//  1. All three participating nodes converge on a single active tip.
//  2. getchaintips on the non-slow nodes has no "valid-fork" entries,
//     which would indicate the slow node published a competing chain.
//
// Uses nodes 1, 2, 3 of the shared 5-node stack. Requires passwordless sudo
// (chaos slow uses nsenter + tc).
func TestSlowPeerPropagation(t *testing.T) {
	s := stack()
	s.RequireSudo(t)
	s.Reset(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	node1 := s.Node(1)
	node3 := s.Node(3)
	participants := []*harness.RPCClient{s.Node(1), s.Node(2), s.Node(3)}

	// Baseline.
	info, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	baselineHeight := info.Blocks
	t.Logf("baseline height=%d", baselineHeight)

	// Apply latency before mining so the slow peer fights the delay on
	// every block it receives or forwards.
	t.Log("slowing teranode2 by 300ms...")
	s.Slow(t, 2, 300)

	// Interleave 3 mines on node1 and 3 on node3. Net +6 blocks.
	for i := 0; i < 3; i++ {
		_, err := node1.Generate(ctx, 1)
		require.NoError(t, err, "node1 generate iter %d", i)
		_, err = node3.Generate(ctx, 1)
		require.NoError(t, err, "node3 generate iter %d", i)
	}

	// Remove latency before the convergence check so node 2 isn't
	// handicapped during catch-up.
	t.Log("removing latency from teranode2...")
	s.Unslow(t, 2)

	converged := harness.WaitForConverged(t, participants, 2*time.Minute)
	t.Logf("converged at %s", short(converged))

	// Non-slow nodes should not retain a valid-fork branch. Note:
	// getchaintips is cached for 5 minutes server-side, so this is a
	// one-shot check per node (first call this test makes for that RPC).
	for _, c := range []*harness.RPCClient{node1, node3} {
		tips, err := c.GetChainTips(ctx)
		require.NoError(t, err)
		for _, tip := range tips {
			require.NotEqual(t, "valid-fork", tip.Status,
				"teranode%d should have no valid-fork tip after convergence: %+v",
				c.NodeIndex, tip)
		}
	}

	// Height sanity: baseline + 6.
	for _, c := range participants {
		info, err := c.GetBlockchainInfo(ctx)
		require.NoError(t, err)
		require.Equal(t, baselineHeight+6, info.Blocks, "teranode%d final height", c.NodeIndex)
	}
}
