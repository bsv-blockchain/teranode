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

// TestFailpointSpendBeforeCreate crashes teranode3 inside
// utxo.SequentialSpendAndCreate, after the parent output has been spent and
// before the child tx record is created, then restarts it and checks the UTXO
// store:
//
//  1. the crash left a half-applied spend: the parent is spent by a tx the
//     store has no record of;
//  2. resubmitting the same tx heals it: the idempotent same-spender spend
//     succeeds, the record is created, and the tx is mined and seen by peers.
//
// Requires a teranode:latest image built with the "failpoints" tag; run via
// make network-chaos-failpoint-test.
func TestFailpointSpendBeforeCreate(t *testing.T) {
	const crashNode = 3

	s := stack()
	s.Reset(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	node1 := s.Node(1)
	node3 := s.Node(crashNode)

	parent := s.MatureCoinbaseOutput(ctx, t, node3)
	tx, err := harness.BuildSpend(ctx, parent)
	require.NoError(t, err)
	txid := tx.TxID()
	t.Logf("spend tx %s of %s:%d", txid, parent.TxID, parent.Vout)

	armed := true
	s.RecreateNode(t, crashNode, failpoint.UTXOSpendBeforeCreate)
	t.Cleanup(func() {
		if armed {
			s.RecreateNode(t, crashNode, "")
		}
	})

	// The node dies while handling the submission; the RPC result is irrelevant.
	_, err = node3.SendRawTransaction(ctx, harness.TxHex(tx))
	t.Logf("sendrawtransaction on armed teranode%d returned err=%v", crashNode, err)

	require.Equal(t, 1, s.WaitForExit(t, crashNode, 2*time.Minute), "teranode%d should exit with code 1 at the seam", crashNode)
	s.RequireFailpointHit(t, crashNode, failpoint.UTXOSpendBeforeCreate)

	s.RecreateNode(t, crashNode, "")
	armed = false

	// The crash landed between the two writes: the parent output is spent by
	// txid, but txid itself was never created.
	items, status, err := harness.UTXOs(ctx, crashNode, parent.TxID)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Greater(t, len(items), int(parent.Vout))
	spent := items[parent.Vout]
	require.Equal(t, "SPENT", spent.Status, "parent output should be spent by the crashed tx")
	require.NotNil(t, spent.SpendingData)
	require.Equal(t, txid, spent.SpendingData.TxID)

	_, status, err = harness.GetTxMeta(ctx, crashNode, txid)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, status, "the child tx record should not exist after a crash before Create")
	t.Logf("half-applied spend after restart: %s:%d spent by %s, which has no record", parent.TxID, parent.Vout, txid)

	// Heal by resubmitting the same tx.
	_, err = node3.SendRawTransaction(ctx, harness.TxHex(tx))
	require.NoError(t, err, "resubmitting the crashed tx should succeed")

	minedIn := mineUntilMined(ctx, t, node3, crashNode, txid)
	waitForTipOn(t, []*harness.RPCClient{node1, s.Node(2), node3}, minedIn)

	for _, n := range []int{crashNode, 1} {
		meta, status, err := harness.GetTxMeta(ctx, n, txid)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status, "teranode%d should have the healed tx", n)
		require.NotEmpty(t, meta.BlockIDs, "teranode%d should see the healed tx as mined", n)

		items, status, err := harness.UTXOs(ctx, n, parent.TxID)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "SPENT", items[parent.Vout].Status)
		require.NotNil(t, items[parent.Vout].SpendingData)
		require.Equal(t, txid, items[parent.Vout].SpendingData.TxID, "teranode%d parent should be spent by the healed tx", n)
	}
}

// mineUntilMined waits for txid to be known to node n, then mines blocks on c
// one at a time until node n reports txid as mined. It returns the hash of the
// block that mined it, which is c's tip.
func mineUntilMined(ctx context.Context, t *testing.T, c *harness.RPCClient, node int, txid string) string {
	t.Helper()

	harness.WaitForCondition(t, "tx "+short(txid)+" known to teranode", time.Minute, time.Second,
		func(ctx context.Context) (bool, error) {
			_, status, err := harness.GetTxMeta(ctx, node, txid)
			return status == http.StatusOK, err
		})

	var minedIn string
	harness.WaitForCondition(t, "tx "+short(txid)+" mined", 3*time.Minute, 3*time.Second,
		func(ctx context.Context) (bool, error) {
			hashes, err := c.Generate(ctx, 1)
			if err != nil || len(hashes) != 1 {
				return false, err
			}
			meta, status, err := harness.GetTxMeta(ctx, node, txid)
			if err != nil || status != http.StatusOK {
				return false, err
			}
			minedIn = hashes[0]
			return len(meta.BlockIDs) > 0, nil
		})

	t.Logf("tx %s mined in %s", short(txid), short(minedIn))
	return minedIn
}

// waitForTipOn waits until every client's best block is want. Unlike
// WaitForConverged it cannot pass on a stale (cached) shared tip.
func waitForTipOn(t *testing.T, clients []*harness.RPCClient, want string) {
	t.Helper()

	for _, c := range clients {
		harness.WaitForTip(t, c, want, 3*time.Minute)
	}
}
