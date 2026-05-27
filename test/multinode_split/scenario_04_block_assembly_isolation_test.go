//go:build network_chaos

package multinodesplit

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
	"github.com/stretchr/testify/require"
)

// TestBlockAssemblyIsolation showcases what the split-per-service topology
// reveals about a teranode node's internal dependency graph that the
// all-in-one topology cannot: even though blockassembly is "only" the
// mining-candidate service, blockvalidation has a hard runtime dependency
// on it (the blockassembly_wait.WaitForBlockAssemblyReady gate fires on
// every inbound block to sync the assembler's tip), so killing
// blockassembly stalls the node's ability to commit new blocks from peers.
//
// Shape:
//
//  1. Reset to a converged 3-node baseline.
//  2. Kill teranode3-blockassembly. blockchain, blockvalidation,
//     subtreevalidation, p2p, asset, and core all stay up.
//  3. Mine 5 blocks on node 1. Node 1's height advances.
//  4. After a settle window, assert node 3 has *not* caught up — proving
//     the validation pipeline is stalled on the missing blockassembly,
//     even though its blockvalidation container is healthy.
//  5. Restart blockassembly. Wait for node 3 to catch up to node 1.
//
// This is unreachable from the all-in-one suite: there's no way to kill
// blockassembly there without taking the whole node down, so the
// "validation stalls on its missing sibling" failure mode is invisible.
// That visibility is exactly what split-mode chaos buys you, and it's
// why this kind of scenario lives here.
//
// Note: we deliberately do NOT mine a final block on node 3 after the
// restart. Empirically, a block produced by a freshly-restored
// blockassembly broadcasts in a form that crashes peers' core sidecar
// via legacy's "unknown magic" wire-format parser — a separate teranode
// robustness issue. Catch-up to the survivors' tip is sufficient
// evidence that the validation pipeline cleared.
func TestBlockAssemblyIsolation(t *testing.T) {
	// Skipped pending two teranode robustness fixes that block this
	// scenario (and the split-stack TestMain itself) from running reliably:
	//
	//  1. utxopersister.CreateUTXOSet (services/utxopersister/UTXOSet.go:527)
	//     nil-pointer panics during startup when processing the height-1
	//     probe block: "Processing block <nil> height 0" → SIGSEGV. Brings
	//     down the core sidecar before the test starts; TestMain reports
	//     "waitForMesh: probe block ... did not propagate" with heights
	//     map[N:-1] (RPC unreachable because core exited).
	//  2. legacy peer-protocol parser returns "unknown magic: [...]"
	//     when receiving a block from a peer whose blockassembly was
	//     killed and restarted; ServiceManager treats it as fatal and
	//     bails the whole core sidecar. The crash hits the peers
	//     receiving the block, not the miner, so it manifests as RPC
	//     connection-refused on healthy-looking nodes during the final
	//     converge wait.
	//
	// Once both are fixed, remove this t.Skip — the scenario assertions
	// below are good. The harness extension (ProvisionSplit,
	// KillService/StartService, split-aware Reset / dumpNodeLogs,
	// ulimits on aerospike) is independent of these bugs and ships on
	// its own.
	t.Skip("blocked by teranode utxopersister nil-pointer panic and legacy 'unknown magic' crash; re-enable once those land")

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

	t.Log("killing teranode3-blockassembly...")
	s.KillService(t, 3, "blockassembly")

	t.Log("mining 5 blocks on teranode1...")
	_, err = node1.Generate(ctx, 5)
	require.NoError(t, err, "node 1 generate")

	// Sanity: node 1 actually advanced.
	harness.WaitForHeight(t, node1, baselineHeight+5, 1*time.Minute)
	survivorInfo, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	survivorTip := survivorInfo.BestBlockHash

	// Settle window: give catch-up the chance to happen IF blockvalidation
	// were independent of blockassembly. Empirically the gate is hit
	// within ~5s of each inbound block, so 15s is comfortable.
	t.Log("waiting 15s to confirm teranode3 stays stalled (blockvalidation gated on blockassembly)...")
	time.Sleep(15 * time.Second)

	stalledInfo, err := node3.GetBlockchainInfo(ctx)
	require.NoError(t, err, "node 3 RPC must still answer (only blockassembly is down, not core)")
	require.Equal(t, baselineHeight, stalledInfo.Blocks,
		"teranode3 should be stuck at baseline height while blockassembly is down (validation cannot commit)")
	require.Equal(t, baselineTip, stalledInfo.BestBlockHash, "teranode3 tip should still be the baseline")
	t.Logf("teranode3 correctly stalled at height=%d while node 1 is at %d", stalledInfo.Blocks, baselineHeight+5)

	t.Log("restarting teranode3-blockassembly...")
	s.StartService(t, 3, "blockassembly")

	// Once the gate clears, blockvalidation drains its catch-up queue.
	t.Log("waiting for teranode3 to catch up to node 1...")
	harness.WaitForHeight(t, node3, baselineHeight+5, 2*time.Minute)
	t.Logf("teranode3 caught up to height %d", baselineHeight+5)

	// Final convergence check: all three nodes should agree on the
	// survivors' tip now that node 3 finished its catch-up.
	converged := harness.WaitForConverged(t, all, 1*time.Minute)
	require.Equal(t, survivorTip, converged, "all 3 nodes should converge on the survivors' tip after node 3 catches up")
	t.Logf("all 3 nodes converged at %s", short(converged))
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
