package bsv

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// Port of bitcoin-sv's bsv-p2p_inv_msg_time_order2.py.
//
// THE SIBLING SCRIPT'S OPPOSITE, and the pair is deliberate.
// bsv-p2p_inv_msg_time_order.py explicitly declines to assert ordering, because
// it sends a batch and lets asynchronous validation reorder it. This one asserts
// ordering, and earns the right to by removing the two sources of ambiguity:
//
//  1. -broadcastdelay=10000 holds inventory for ten seconds, so ONE inv carries
//     every transaction instead of a stream of separate announcements. Teranode's
//     equivalent is TrickleInterval; the sibling port measured that it is honoured
//     (50ms default gives ~100ms and two invs, 500ms gives ~500ms and one).
//  2. The script waits for each transaction to be processed before sending the
//     next, so the order transactions were VALIDATED in is the order they were
//     sent in. Without that, validation order is a race and the assertion would be
//     meaningless.
//
// WHAT UPSTREAM'S CHECK ACTUALLY IS. invsOrderedbyTime walks the received hashes
// and fails if any of them was sent EARLIER than its received position - that is,
// received[i] must not have index < i in the sent list. Since not every
// transaction is guaranteed to be accepted, the received list may be a
// subsequence of the sent list, and this is the order-preservation test for a
// subsequence. It is not "received == sent".
//
// THE FEES ARE NON-MONOTONIC ON PURPOSE. Upstream uses fee0, 2x, 3x, 2x across
// four groups. A node that sorted its inventory by fee would produce a different
// order from the send order, so the varied fees are what stop the assertion
// passing for the wrong reason. The port keeps that shape.
const (
	// invOrder2Groups and invOrder2PerGroup mirror upstream's four fee groups of
	// self.num_txns each.
	invOrder2PerGroup = 3

	// invOrder2Trickle is upstream's -broadcastdelay=10000. Long enough that every
	// transaction is still queued when the single inv finally goes out.
	invOrder2Trickle = "10s"
)

// invOrder2FeeMultipliers is upstream's [fee0, fee0*2, fee0*3, fee0*2].
var invOrder2FeeMultipliers = []uint64{1, 2, 3, 2}

func TestBSVP2PInvMsgTimeOrder2(t *testing.T) {
	setLegacyConfigForTest(t, "legacy_config_TrickleInterval", invOrder2Trickle)

	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	total := len(invOrder2FeeMultipliers) * invOrder2PerGroup
	pool := newOutputPool(t, td, total)

	sender := wirepeer.Connect(t, td)
	defer sender.Close()

	observer := wirepeer.Connect(t, td)
	defer observer.Close()

	// Send in order, waiting for each to be validated before sending the next -
	// upstream's sync_with_ping() after every send. Waiting on block assembly is
	// the stronger version of that: a ping only proves the node drained its input
	// queue, while this proves the transaction finished validating and was
	// accepted, which is what the ordering claim rests on.
	sent := make([]string, 0, total)

	for _, mult := range invOrder2FeeMultipliers {
		for range invOrder2PerGroup {
			fundingTx, vout := pool.take(t)
			tx := spendAnyoneCanSpend(t, fundingTx, vout, 1, invOrderBaseFee*mult)

			sender.Send(t, asWireTx(t, tx))
			td.WaitForBlockAssemblyToProcessTx(t, tx.TxID())

			sent = append(sent, tx.TxID())
		}
	}

	require.Len(t, sent, total, "every pool output should have produced a transaction")

	// Upstream: wait_until(lambda: len(txinvs) > 0, timeout=60). With a ten-second
	// trickle the single inv is not due until the interval elapses.
	require.Eventually(t, func() bool {
		return len(txInvHashes(observer)) > 0
	}, 60*time.Second, 100*time.Millisecond,
		"the observing peer should eventually be told about the transactions")

	// Let the full batch land rather than asserting on the first inv to arrive.
	require.Eventually(t, func() bool {
		return len(txInvHashes(observer)) >= total
	}, 60*time.Second, 100*time.Millisecond,
		"all %d transactions should be announced; saw %d", total, len(txInvHashes(observer)))

	announced := txInvHashes(observer)

	// Upstream: assert(invsOrderedbyTime(ids, txinvs)).
	requireOrderPreserved(t, sent, announced)

	// CONTROL on the trickle override, mirroring the sibling port. Upstream's whole
	// method depends on one inv carrying everything; if the override stopped
	// working, the transactions would be announced individually and the ordering
	// assertion would be trivially satisfied by twelve single-vector invs.
	require.Less(t, observer.Count(wire.CmdInv), total,
		"a ten-second trickle should batch %d transactions into far fewer than %d inv "+
			"messages; one per transaction means the override did not reach the daemon "+
			"and the ordering assertion above proves nothing", total, total)
}

// requireOrderPreserved is upstream's invsOrderedbyTime.
//
// For each announced hash, its position in the sent list must not be earlier than
// its own position among the announcements. Announcements may be a subsequence of
// what was sent - not every transaction is guaranteed to be accepted - so this
// checks relative order rather than equality.
func requireOrderPreserved(t *testing.T, sent, announced []string) {
	t.Helper()

	pos := make(map[string]int, len(sent))
	for i, h := range sent {
		pos[h] = i
	}

	for i, h := range announced {
		sentAt, ok := pos[h]
		require.True(t, ok,
			"announced transaction %s was never sent", h)
		require.GreaterOrEqual(t, sentAt, i,
			"announcement order does not preserve send order: %s was announced at "+
				"position %d but sent at position %d", h, i, sentAt)
	}
}
