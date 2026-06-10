//go:build network_chaos

package multinodesplit

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
	"github.com/stretchr/testify/require"
)

// TestP2PIsolation kills the p2p service on node 3 and asserts the node can
// no longer receive blocks from peers until p2p is restored.
//
// p2p is the inter-node block- and tx-announcement control plane. With it
// down, node 3 loses both directions: it cannot receive block/tx INVs from
// nodes 1 and 2, and it cannot pull headers/blocks from them. The blockchain,
// blockvalidation, subtreevalidation, validator, propagation, asset, and core
// containers all stay up on node 3, so node 3's RPC keeps answering: it is
// alive but blind to the rest of the mesh.
//
// Note: the multinode docker stack relays blocks between teranode nodes only
// via the native p2p service, not the legacy SV wire protocol. Legacy lives
// in the core sidecar but its peering is configured for bridging to actual
// Bitcoin SV nodes, not for inter-teranode block relay. So killing
// teranodeN-p2p genuinely severs node N from the test mesh.
//
// Shape mirrors TestBlockAssemblyIsolation: kill on node 3, mine on node 1,
// assert node 3 stalls, restart, assert catch-up and convergence.
func TestP2PIsolation(t *testing.T) {
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
	baselineTip := info.BestBlockHash
	t.Logf("baseline: height=%d tip=%s", baselineHeight, short(baselineTip))

	t.Log("killing teranode3-p2p...")
	s.KillService(t, 3, "p2p")

	t.Log("mining 5 blocks on teranode1...")
	_, err = node1.Generate(ctx, 5)
	require.NoError(t, err, "node 1 generate")

	harness.WaitForHeight(t, node1, baselineHeight+5, 1*time.Minute)
	survivorInfo, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	survivorTip := survivorInfo.BestBlockHash

	// Settle window: if block relay had any path not routed through p2p,
	// catch-up would happen here.
	t.Log("waiting 15s to confirm teranode3 stays stalled (no inbound block announcements without p2p)...")
	time.Sleep(15 * time.Second)

	stalledInfo, err := node3.GetBlockchainInfo(ctx)
	require.NoError(t, err, "node 3 RPC must still answer (only p2p is down, not core)")
	require.Equal(t, baselineHeight, stalledInfo.Blocks,
		"teranode3 should be stuck at baseline height while p2p is down (no path for peer blocks to arrive)")
	require.Equal(t, baselineTip, stalledInfo.BestBlockHash, "teranode3 tip should still be the baseline")
	t.Logf("teranode3 correctly stalled at height=%d while node 1 is at %d", stalledInfo.Blocks, baselineHeight+5)

	t.Log("restarting teranode3-p2p...")
	s.StartService(t, 3, "p2p")

	t.Log("waiting for teranode3 to catch up to node 1...")
	harness.WaitForHeight(t, node3, baselineHeight+5, 2*time.Minute)
	t.Logf("teranode3 caught up to height %d", baselineHeight+5)

	converged := harness.WaitForConverged(t, all, 1*time.Minute)
	require.Equal(t, survivorTip, converged, "all 3 nodes should converge on the survivors' tip after node 3 catches up")
	t.Logf("all 3 nodes converged at %s", short(converged))
}
