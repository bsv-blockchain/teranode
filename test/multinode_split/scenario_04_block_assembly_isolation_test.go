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
// Note: we stop at catch-up rather than mining a final block on node 3
// after the restart. The "unknown magic" crash that previously made that
// step flaky was not a legacy wire-format parser bug as first assumed; it
// was the utxopersister double-reading the 8-byte fileformat magic when
// processing a previous UTXO set (the bytes that looked like an unknown
// magic were chainhash prefixes), fixed in #971/#979. Catch-up to the
// survivors' tip is sufficient evidence that the validation pipeline
// cleared, so the conservative shape is kept regardless.
func TestBlockAssemblyIsolation(t *testing.T) {
	// Previously skipped pending teranode robustness fixes that blocked
	// this scenario (and the split-stack TestMain itself) from running
	// reliably. All have since landed:
	//
	//  1. utxopersister.CreateUTXOSet nil-pointer panic at startup when
	//     processing the height-1 probe block, which took down the core
	//     sidecar before the test began — fixed in #969.
	//  2. the "unknown magic" crash when a peer persisted a block, which was
	//     utxopersister double-reading the fileformat magic of a previous
	//     UTXO set (not a legacy wire-format bug as first assumed) — fixed
	//     in #971/#979.
	//  3. utxopersister.CreateUTXOSet crashing the core sidecar on the
	//     16-byte footer (txCount + utxoCount) of a previous UTXO set during
	//     consolidation — hit here when node 3 persists peer blocks while
	//     its blockassembly is down. This was the last blocker, surfaced by
	//     this very scenario and fixed in #985.
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
