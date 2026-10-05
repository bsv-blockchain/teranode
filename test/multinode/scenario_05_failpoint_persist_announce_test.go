//go:build network_chaos && failpoints

package multinode

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/multinode/harness"
	"github.com/bsv-blockchain/teranode/util/failpoint"
	"github.com/stretchr/testify/require"
)

// TestFailpointPersistBeforeAddBlock crashes teranode3 in block assembly's
// submitMiningSolution after the block's subtrees and coinbase have been
// persisted and before the block is added to the blockchain store and
// announced, then restarts it and checks:
//
//  1. no peer saw the half-built block: every tip is unchanged;
//  2. the restarted node is still on its pre-crash tip;
//  3. the pending tx is mined in a later block that every peer accepts, and
//     each subtree of that block is retrievable from the subtree stores.
//
// Requires a teranode:latest image built with the "failpoints" tag; run via
// make network-chaos-failpoint-test.
func TestFailpointPersistBeforeAddBlock(t *testing.T) {
	const crashNode = 3

	s := stack()
	s.Reset(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	node1 := s.Node(1)
	node2 := s.Node(2)
	node3 := s.Node(crashNode)
	participants := []*harness.RPCClient{node1, node2, node3}

	// Give the block a real subtree: a pending tx in node 3's block assembly.
	parent := s.MatureCoinbaseOutput(ctx, t, node3)
	tx, err := harness.BuildSpend(ctx, parent)
	require.NoError(t, err)
	txid := tx.TxID()
	_, err = node3.SendRawTransaction(ctx, harness.TxHex(tx))
	require.NoError(t, err)
	harness.WaitForCondition(t, "tx "+short(txid)+" known to teranode3", time.Minute, time.Second,
		func(ctx context.Context) (bool, error) {
			_, status, err := harness.GetTxMeta(ctx, crashNode, txid)
			return status == http.StatusOK, err
		})

	baseline := harness.WaitForConverged(t, participants, time.Minute)
	t.Logf("baseline tip %s, pending tx %s", short(baseline), short(txid))

	armed := true
	s.RecreateNode(t, crashNode, failpoint.BlockAssemblyPersistBeforeAddBlock)
	t.Cleanup(func() {
		if armed {
			s.RecreateNode(t, crashNode, "")
		}
	})

	// The node dies inside generate; the RPC result is irrelevant.
	_, err = node3.Generate(ctx, 1)
	t.Logf("generate on armed teranode%d returned err=%v", crashNode, err)

	require.Equal(t, 1, s.WaitForExit(t, crashNode, 2*time.Minute), "teranode%d should exit with code 1 at the seam", crashNode)
	s.RequireFailpointHit(t, crashNode, failpoint.BlockAssemblyPersistBeforeAddBlock)

	// Nothing was announced: the survivors are still on the baseline tip.
	for _, c := range []*harness.RPCClient{node1, node2} {
		tip, err := c.BestTip(ctx)
		require.NoError(t, err)
		require.Equal(t, baseline, tip.Hash, "teranode%d should not have seen the crashed block", c.NodeIndex)
	}

	s.RecreateNode(t, crashNode, "")
	armed = false

	tip, err := node3.BestTip(ctx)
	require.NoError(t, err)
	require.Equal(t, baseline, tip.Hash, "restarted teranode%d should be on its pre-crash tip", crashNode)

	minedIn := mineUntilMined(ctx, t, node3, crashNode, txid)
	waitForTipOn(t, participants, minedIn)

	// The block that mined the pending tx is the announced tip; every one of its
	// subtrees must be retrievable from each node's subtree store.
	for _, n := range []int{crashNode, 1} {
		block, err := harness.GetBlock(ctx, n, minedIn)
		require.NoError(t, err)
		require.NotEmpty(t, block.Subtrees)
		for _, st := range block.Subtrees {
			status, _, err := harness.AssetGet(ctx, n, "/subtree/"+st)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, status, "teranode%d: subtree %s of block %s should be retrievable", n, short(st), short(minedIn))
		}
	}
}
