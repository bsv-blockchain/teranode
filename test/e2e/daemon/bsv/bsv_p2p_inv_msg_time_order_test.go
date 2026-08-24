package bsv

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// Port of bitcoin-sv's bsv-p2p_inv_msg_time_order.py.
//
// THE NAME OVERSTATES WHAT THE SCRIPT ASSERTS, and the port follows the script
// rather than the name. Upstream sends a batch of transactions from one peer and
// waits for each to be announced by inv to a SECOND peer, then asserts the counts
// match. It does not assert ordering - its own comment says so: "Due to
// asynchronous validation we can not expect that an order of receiving
// transactions is the same as order of sending." So the real subject is
// COMPLETENESS of transaction relay: every transaction accepted from one peer is
// announced to another, exactly once.
//
// Asserting the order the name implies would be asserting something upstream
// deliberately does not, and would be flaky for the reason upstream gives.
//
// WHAT THE FUNDING LOOKS LIKE HERE. Upstream calls listunspent and makes one
// transaction per entry, with an increasing fee per transaction. Teranode
// declines wallet RPCs, so the port uses outputPool - see funding_test.go - which
// is the same idea without a wallet: fund a set of outputs once, then spend them
// one at a time. The fees still increase across the batch, because a batch of
// identical-fee transactions would not exercise whatever ordering the node
// applies internally, even though the port does not assert that ordering.
//
// The COUNT differs and is incidental upstream: listunspent returns however many
// outputs the wallet happens to hold, and upstream makes that many transactions.
// Nothing asserts the number. The port fixes it at a size that exercises batching
// without dominating runtime.
//
// THE RELAY DELAY IS REPRODUCED, not waived. Upstream runs the node with
// -broadcastdelay=500 -txnpropagationfreq=500, which slows and batches inventory
// relay so the assertion is made under load rather than against an idle fast path.
// Teranode has the same control: TrickleInterval, "Minimum time between attempts
// to send new inventory to a connected peer" (services/legacy/config.go), which
// defaults to 50ms. The port raises it to 500ms so relay is batched exactly as
// upstream intends. Without it the test would still pass, but against a faster and
// therefore weaker condition than the script describes.
const (
	// invOrderBatch is how many transactions the batch carries.
	invOrderBatch = 20

	// invOrderBaseFee and invOrderFeeStep mirror upstream's
	// range(100000, 500000, 1000) - an increasing fee per transaction.
	invOrderBaseFee = 1000
	invOrderFeeStep = 100

	// invOrderTrickle matches upstream's -broadcastdelay/-txnpropagationfreq of
	// 500ms.
	invOrderTrickle = "500ms"
)

func TestBSVP2PInvMsgTimeOrder(t *testing.T) {
	setLegacyConfigForTest(t, "legacy_config_TrickleInterval", invOrderTrickle)

	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	pool := newOutputPool(t, td, invOrderBatch)

	// Upstream uses two connections: one sends, the other observes the invs. That
	// separation is the point - it asserts relay to a DIFFERENT peer, not an echo
	// back to the sender.
	sender := wirepeer.Connect(t, td)
	defer sender.Close()

	observer := wirepeer.Connect(t, td)
	defer observer.Close()

	// Build the batch before sending any of it, as upstream does, so that send
	// timing is not interleaved with construction.
	sent := make([]*bt.Tx, 0, invOrderBatch)

	for i := range invOrderBatch {
		fundingTx, vout := pool.take(t)
		fee := uint64(invOrderBaseFee + i*invOrderFeeStep)
		sent = append(sent, spendAnyoneCanSpend(t, fundingTx, vout, 1, fee))
	}

	require.Equal(t, 0, pool.remaining(), "the batch should have consumed the pool")

	sendStart := time.Now()

	for _, tx := range sent {
		sender.Send(t, asWireTx(t, tx))
	}

	// Upstream waits for every sent txid to appear among the observer's tx invs.
	// Collect once at the end rather than polling per transaction: the assertion
	// is over the whole set, and a per-transaction wait would report the first
	// missing one while hiding how many others were also missing.
	require.Eventually(t, func() bool {
		return len(txInvHashes(observer)) >= invOrderBatch
	}, 60*time.Second, 10*time.Millisecond,
		"every transaction sent by one peer should be announced by inv to the other; "+
			"saw %d of %d", len(txInvHashes(observer)), invOrderBatch)

	relayTook := time.Since(sendStart)

	announced := txInvHashes(observer)

	// Upstream: assert_equal(len(transaction_list_by_time), len(txinvs)).
	for _, tx := range sent {
		require.Contains(t, announced, tx.TxID(),
			"transaction %s should have been announced to the observing peer", tx.TxID())
	}

	require.Len(t, announced, invOrderBatch,
		"the observer should see exactly one inv per transaction sent - no duplicates, "+
			"none missing")

	// CONTROL on the TrickleInterval override, not an upstream assertion.
	//
	// The legacy config is built inside services/legacy and is not reachable from
	// td.Settings, so the override cannot be asserted directly. Its timing can, and
	// this was measured both ways rather than assumed:
	//
	//   default trickle (50ms):  first announcement at ~100ms, 2 inv messages
	//   override (500ms):        first announcement at ~500ms, 1 inv message
	//
	// So a floor of 400ms distinguishes the two with margin. If this fails, the
	// override stopped reaching the daemon and everything above is passing against
	// default fast relay - weaker than the condition the script describes.
	//
	// An earlier version of this control counted inv messages instead. That was
	// worthless: both 50ms and 500ms batch 20 transactions into fewer than 20 invs,
	// so it could not tell them apart.
	require.GreaterOrEqual(t, relayTook, 400*time.Millisecond,
		"with a 500ms trickle interval the first announcement should not arrive inside "+
			"400ms; %s suggests the override did not reach the daemon and relay ran at "+
			"the 50ms default", relayTook)
}

// txInvHashes collects every transaction hash the peer has been told about,
// deduplicated.
//
// Deduplicated because the count assertion is about which transactions were
// announced, and Teranode is free to batch them into inv messages however it
// likes - upstream flattens the same way, appending every vector from every inv
// into one list.
func txInvHashes(p *wirepeer.Peer) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)

	for _, m := range p.Received(wire.CmdInv) {
		inv, ok := m.(*wire.MsgInv)
		if !ok {
			continue
		}

		for _, v := range inv.InvList {
			if v.Type != wire.InvTypeTx {
				continue
			}

			h := v.Hash.String()
			if _, dup := seen[h]; dup {
				continue
			}

			seen[h] = struct{}{}
			out = append(out, h)
		}
	}

	return out
}
