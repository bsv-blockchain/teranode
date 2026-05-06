//go:build network_chaos

package multinode

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
	"github.com/stretchr/testify/require"
)

// TestSlowPeerPropagation adds 300ms egress latency to one node and verifies
// that newly mined blocks still reach it via gossip. All blocks are mined on
// a single non-slow node (no fork race); the slow node's job is simply to
// keep up. Assertions:
//
//  1. Every participating node reaches the new height (baseline+N).
//  2. They converge on the same tip.
//  3. getchaintips on the non-slow nodes has no "valid-fork" entries.
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

	// Apply latency before mining so the slow peer has to fight the delay
	// on every gossiped block.
	t.Log("slowing teranode2 by 300ms...")
	s.Slow(t, 2, 300)

	// Mine sequentially on a single source. Mining on multiple nodes while
	// one peer is slow creates fork races that aren't what this scenario is
	// trying to test: the question is "does the slow peer keep up", not
	// "does the network resolve forks under latency".
	const blocksToMine = 6
	for i := 0; i < blocksToMine; i++ {
		_, err := node1.Generate(ctx, 1)
		require.NoError(t, err, "node1 generate iter %d", i)
	}

	// Remove latency before the convergence wait so node 2 isn't
	// handicapped during catch-up.
	t.Log("removing latency from teranode2...")
	s.Unslow(t, 2)

	// Every participant must reach baseline+6 within a generous window.
	// node 2 carried 300ms latency through the mining phase so it may still
	// be processing the queue.
	target := baselineHeight + blocksToMine
	harness.WaitForHeightOn(t, participants, target, 2*time.Minute)

	converged := harness.WaitForConverged(t, participants, 60*time.Second)
	t.Logf("converged at %s (height %d)", short(converged), target)

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
}
