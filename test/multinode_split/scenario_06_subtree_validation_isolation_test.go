//go:build network_chaos

package multinodesplit

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
	"github.com/stretchr/testify/require"
)

// TestSubtreeValidationIsolation PAUSES (does not kill) the subtreevalidation
// service on node 3 and asserts the node cannot commit inbound blocks until it
// is resumed.
//
// blockvalidation calls subtreeValidationClient.CheckBlockSubtrees
// (BlockValidation.go) on every inbound block, so subtreevalidation is squarely
// on the block-acceptance path. This is the most direct dependency in the
// split graph, which makes it the cleanest stall to assert.
//
// We use PauseService rather than KillService deliberately: docker pause
// SIGSTOPs the container, so the gRPC call from blockvalidation HANGS (no
// connection-refused, no responder) — the "frozen / unresponsive dependency"
// failure mode, distinct from a process that has exited. Either way node 3
// cannot make progress; UnpauseService thaws it and validation resumes. This is
// the first scenario to exercise the pause/unpause verbs.
//
// Shape mirrors TestBlockAssemblyIsolation otherwise.
func TestSubtreeValidationIsolation(t *testing.T) {
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

	t.Log("pausing teranode3-subtreevalidation (docker pause - gRPC calls will hang)...")
	s.PauseService(t, 3, "subtreevalidation")
	// Ensure the service is thawed even if an assertion below fails, so the
	// shared stack is left healthy for the next scenario's Reset.
	defer s.UnpauseService(t, 3, "subtreevalidation")

	t.Log("mining 5 blocks on teranode1...")
	_, err = node1.Generate(ctx, 5)
	require.NoError(t, err, "node 1 generate")

	harness.WaitForHeight(t, node1, baselineHeight+5, 1*time.Minute)
	survivorInfo, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	survivorTip := survivorInfo.BestBlockHash

	t.Log("waiting 15s to confirm teranode3 stays stalled (CheckBlockSubtrees hangs on the frozen service)...")
	time.Sleep(15 * time.Second)

	stalledInfo, err := node3.GetBlockchainInfo(ctx)
	require.NoError(t, err, "node 3 RPC must still answer (only subtreevalidation is frozen, not core)")
	require.Equal(t, baselineHeight, stalledInfo.Blocks,
		"teranode3 should be stuck at baseline height while subtreevalidation is paused (cannot validate block subtrees)")
	require.Equal(t, baselineTip, stalledInfo.BestBlockHash, "teranode3 tip should still be the baseline")
	t.Logf("teranode3 correctly stalled at height=%d while node 1 is at %d", stalledInfo.Blocks, baselineHeight+5)

	t.Log("unpausing teranode3-subtreevalidation...")
	s.UnpauseService(t, 3, "subtreevalidation")

	t.Log("waiting for teranode3 to catch up to node 1...")
	harness.WaitForHeight(t, node3, baselineHeight+5, 2*time.Minute)
	t.Logf("teranode3 caught up to height %d", baselineHeight+5)

	converged := harness.WaitForConverged(t, all, 1*time.Minute)
	require.Equal(t, survivorTip, converged, "all 3 nodes should converge on the survivors' tip after node 3 catches up")
	t.Logf("all 3 nodes converged at %s", short(converged))
}
