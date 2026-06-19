//go:build network_chaos

package multinodesplit

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
	"github.com/stretchr/testify/require"
)

// TestAssetIsolation is a control / null-hypothesis scenario: killing the
// asset service on node 3 must NOT stall block sync. Asset is the HTTP
// server that exposes this node's subtrees and block data to external
// queries and to peers fetching for their own subtreevalidation. The
// node-3 receive path for peer blocks is:
//
//	node1 p2p inv -> node3 p2p -> node3 blockvalidation
//	  -> node3 subtreevalidation (pulls subtree blobs from node1's asset)
//	  -> node3 validator (tx validation)
//	  -> node3 blockchain (commit)
//
// node3-asset only serves outbound queries from peers and external clients.
// It is not on node3's own block-acceptance path. Killing it should make
// node3 invisible to *peers* fetching from it, but not affect node3 itself.
//
// Without a control like this, the suite cannot distinguish "block-assembly /
// validator / subtreevalidation / p2p kills stalled node 3 because they're
// on the consensus path" from "any KillService stalls node 3 because the
// harness or compose graph is broken." This scenario rules out the latter.
//
// If this test surprisingly fails — i.e. node 3 *does* stall after asset is
// killed — then asset is silently on node 3's own block-acceptance path
// (most likely subtreevalidation reaches into localhost asset rather than
// the originating peer's asset), and that coupling is a finding worth
// documenting and fixing.
//
// Shape:
//
//  1. Reset to a converged 3-node baseline.
//  2. Kill teranode3-asset. All other node-3 services stay up.
//  3. Mine 5 blocks on node 1.
//  4. Assert node 3 catches up — proving asset is off node 3's
//     receive-side consensus path.
//  5. Restart asset. Assert 3-node convergence.
func TestAssetIsolation(t *testing.T) {
	s := stack()
	s.Reset(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	node1 := s.Node(1)
	node3 := s.Node(3)
	all := s.Nodes()

	info, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	baselineHeight := info.Blocks
	t.Logf("baseline: height=%d tip=%s", baselineHeight, short(info.BestBlockHash))

	t.Log("killing teranode3-asset...")
	s.KillService(t, 3, "asset")

	t.Log("mining 5 blocks on teranode1...")
	_, err = node1.Generate(ctx, 5)
	require.NoError(t, err, "node 1 generate")

	harness.WaitForHeight(t, node1, baselineHeight+5, 1*time.Minute)
	survivorInfo, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	survivorTip := survivorInfo.BestBlockHash

	// Asymmetric expectation: node 3 must catch up despite its own asset
	// being dead. If subtreevalidation actually reaches into localhost
	// asset rather than the originating peer's asset, this wait fails
	// and we have a finding.
	t.Log("waiting for teranode3 to catch up (its own asset is dead, subtreevalidation should pull from node1's)...")
	harness.WaitForHeight(t, node3, baselineHeight+5, 2*time.Minute)
	caughtUp, err := node3.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, baselineHeight+5, caughtUp.Blocks,
		"teranode3 should catch up to node 1 even with its own asset dead (asset is outbound-only on the receive path)")
	t.Logf("teranode3 reached height=%d with its asset still down", caughtUp.Blocks)

	t.Log("restarting teranode3-asset...")
	s.StartService(t, 3, "asset")

	converged := harness.WaitForConverged(t, all, 1*time.Minute)
	require.Equal(t, survivorTip, converged, "all 3 nodes should converge on the survivors' tip")
	t.Logf("all 3 nodes converged at %s", short(converged))
}
