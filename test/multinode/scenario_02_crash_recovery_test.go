//go:build network_chaos

package multinode

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
	"github.com/stretchr/testify/require"
)

// TestCrashRecovery kills a node, mines more blocks on the survivors, then
// restarts the killed node and asserts it catches up to tip. This exercises
// the catchup/syncer path end-to-end, which is unreachable from in-process
// TestDaemon tests because those share memory.
//
// Uses nodes 1, 2, 3 of the shared 5-node stack. Nodes 4 and 5 just sit
// there; the scenario asserts only about the three it operates on.
//
// No sudo required: docker kill/start only.
func TestCrashRecovery(t *testing.T) {
	s := stack()
	s.Reset(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	node1 := s.Node(1)
	node2 := s.Node(2)
	node3 := s.Node(3)
	participants := []*harness.RPCClient{node1, node2, node3}

	// Baseline: current converged state.
	info, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	baselineHeight := info.Blocks
	baselineTip := info.BestBlockHash
	t.Logf("baseline: height=%d tip=%s", baselineHeight, short(baselineTip))

	// Crash node 3. The others must keep operating.
	t.Log("killing teranode3...")
	s.Kill(t, 3)

	// Mine 5 more blocks on node 1 while node 3 is dead.
	t.Log("mining 5 blocks on teranode1 while teranode3 is down...")
	_, err = node1.Generate(ctx, 5)
	require.NoError(t, err)

	survivors := []*harness.RPCClient{node1, node2}
	harness.WaitForHeightOn(t, survivors, baselineHeight+5, 2*time.Minute)
	divergedInfo, err := node1.GetBlockchainInfo(ctx)
	require.NoError(t, err)
	divergedTip := divergedInfo.BestBlockHash
	require.NotEqual(t, baselineTip, divergedTip, "survivors should be past baseline")
	t.Logf("survivors at height=%d tip=%s", divergedInfo.Blocks, short(divergedTip))

	// Confirm node 3's RPC is really down.
	{
		readyCtx, rcancel := context.WithTimeout(ctx, 2*time.Second)
		err := node3.Ready(readyCtx)
		rcancel()
		require.Error(t, err, "teranode3 RPC should be unreachable while killed")
	}

	// Restart and wait for RPC.
	t.Log("restarting teranode3...")
	s.Start(t, 3)

	harness.WaitForCondition(t, "teranode3 RPC ready after restart", 2*time.Minute, 2*time.Second,
		func(ctx context.Context) (bool, error) {
			rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
			defer rcancel()
			return node3.Ready(rctx) == nil, nil
		})

	// Core assertion: node 3 must catch up to the survivors.
	converged := harness.WaitForConverged(t, participants, 3*time.Minute)
	require.Equal(t, divergedTip, converged, "all 3 nodes should converge on the survivors' tip")
	t.Logf("node 3 caught up, all 3 converged at %s", short(converged))
}
