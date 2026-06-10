//go:build network_chaos

package multinodesplit

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
	"github.com/stretchr/testify/require"
)

// TestValidatorIsolation kills the validator service on node 3 and asserts the
// node can no longer commit inbound blocks until validator is restored.
//
// The coupling: validating a peer's block walks
//
//	blockvalidation.ProcessBlock
//	  -> subtreeValidationClient.CheckBlockSubtrees   (BlockValidation.go)
//	    -> validatorClient.ValidateWithOptions         (SubtreeValidation.go)
//
// i.e. the per-transaction validation of a block's subtrees goes through the
// validator service. With validator down, node 3 cannot validate the txs in
// the blocks node 1 mines and should stay pinned at the baseline height.
//
// Important config dependency: this only holds when subtreevalidation calls
// out to the standalone validator container over gRPC, NOT when it embeds an
// in-process validator. settings.conf ships useLocalValidator=true (the right
// default for all-in-one), so a vanilla docker.teranode{N}.test context
// would build an in-process validator and ignore the validator container
// entirely — making this scenario a no-op. The split-mode overlay generated
// by compose/cmd/gennodes/templates/settings.conf.tmpl now flips
// useLocalValidator to false per node, which is what makes the kill
// observable here. If a future edit drops that override, this test fails
// loudly and points at it.
//
// Shape mirrors TestBlockAssemblyIsolation: kill on node 3, mine on node 1,
// assert node 3 stalls, restart, assert catch-up and convergence.
func TestValidatorIsolation(t *testing.T) {
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

	t.Log("killing teranode3-validator...")
	s.KillService(t, 3, "validator")

	t.Log("mining 5 blocks on teranode1...")
	_, err = node1.Generate(ctx, 5)
	require.NoError(t, err, "node 1 generate")

	// Sanity: node 1 actually advanced (its validator is healthy).
	harness.WaitForHeight(t, node1, baselineHeight+5, 1*time.Minute)
	survivorInfo, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	survivorTip := survivorInfo.BestBlockHash

	// Settle window: catch-up would happen here if block validation did not
	// depend on the validator service.
	t.Log("waiting 15s to confirm teranode3 stays stalled (block validation needs the validator)...")
	time.Sleep(15 * time.Second)

	stalledInfo, err := node3.GetBlockchainInfo(ctx)
	require.NoError(t, err, "node 3 RPC must still answer (only validator is down, not core)")
	require.Equal(t, baselineHeight, stalledInfo.Blocks,
		"teranode3 should be stuck at baseline height while validator is down (cannot validate block txs)")
	require.Equal(t, baselineTip, stalledInfo.BestBlockHash, "teranode3 tip should still be the baseline")
	t.Logf("teranode3 correctly stalled at height=%d while node 1 is at %d", stalledInfo.Blocks, baselineHeight+5)

	t.Log("restarting teranode3-validator...")
	s.StartService(t, 3, "validator")

	t.Log("waiting for teranode3 to catch up to node 1...")
	harness.WaitForHeight(t, node3, baselineHeight+5, 2*time.Minute)
	t.Logf("teranode3 caught up to height %d", baselineHeight+5)

	converged := harness.WaitForConverged(t, all, 1*time.Minute)
	require.Equal(t, survivorTip, converged, "all 3 nodes should converge on the survivors' tip after node 3 catches up")
	t.Logf("all 3 nodes converged at %s", short(converged))
}
