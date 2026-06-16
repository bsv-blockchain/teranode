//go:build network_chaos

package multinodesplit

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
	"github.com/stretchr/testify/require"
)

// TestPropagationFreeze is the suite's first *asymmetric* scenario: pausing
// the propagation service on node 2 must NOT stall block sync, because
// propagation is the tx-ingress path (RPC sendrawtransaction -> propagation
// -> validator -> blockassembly), not the block-relay path. Inbound peer
// blocks reach node 2 via p2p -> blockvalidation -> subtreevalidation; they
// never touch propagation.
//
// We use PauseService (docker pause / SIGSTOP) rather than KillService so any
// accidental gRPC call into propagation from a block-path code site would
// hang the caller (frozen-dependency failure mode), making the coupling
// visible. If block sync were silently depending on propagation, a paused
// container would manifest as a stall here, not as a connection-refused
// error.
//
// Shape:
//
//  1. Reset to a converged 3-node baseline.
//  2. Pause teranode2-propagation. blockchain, blockvalidation,
//     subtreevalidation, validator, p2p, asset, and core all stay healthy
//     on node 2.
//  3. Mine 5 blocks on node 1.
//  4. After a settle window, assert node 2 *has* caught up — proving the
//     block-acceptance path is independent of propagation.
//  5. Unpause propagation. Assert all 3 nodes converge.
//
// A `defer s.UnpauseService` guarantees the container is thawed even if a
// require assertion fails, so the shared stack stays healthy for the next
// scenario's Reset.
func TestPropagationFreeze(t *testing.T) {
	s := stack()
	s.Reset(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	node1 := s.Node(1)
	node2 := s.Node(2)
	all := s.Nodes()

	info, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	baselineHeight := info.Blocks
	t.Logf("baseline: height=%d tip=%s", baselineHeight, short(info.BestBlockHash))

	t.Log("pausing teranode2-propagation (docker pause - any gRPC call from a block-path site would hang)...")
	s.PauseService(t, 2, "propagation")
	defer s.UnpauseService(t, 2, "propagation")

	t.Log("mining 5 blocks on teranode1...")
	_, err = node1.Generate(ctx, 5)
	require.NoError(t, err, "node 1 generate")

	harness.WaitForHeight(t, node1, baselineHeight+5, 1*time.Minute)
	survivorInfo, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	survivorTip := survivorInfo.BestBlockHash

	// Asymmetric expectation: node 2 must catch up despite propagation
	// being frozen. WaitForHeight enforces this with a real timeout — if
	// block sync silently depends on propagation, the wait fails and we
	// learn the coupling exists.
	t.Log("waiting for teranode2 to catch up via p2p+blockvalidation (propagation still frozen)...")
	harness.WaitForHeight(t, node2, baselineHeight+5, 2*time.Minute)
	caughtUp, err := node2.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, baselineHeight+5, caughtUp.Blocks,
		"teranode2 should catch up to node 1 even with propagation frozen (block sync goes via p2p, not propagation)")
	t.Logf("teranode2 reached height=%d with propagation still paused", caughtUp.Blocks)

	t.Log("unpausing teranode2-propagation...")
	s.UnpauseService(t, 2, "propagation")

	converged := harness.WaitForConverged(t, all, 1*time.Minute)
	require.Equal(t, survivorTip, converged, "all 3 nodes should converge on the survivors' tip")
	t.Logf("all 3 nodes converged at %s", short(converged))
}
