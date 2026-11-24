package protocol

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// connectTx is relayTestFixture.connect's tx-relay counterpart: it needs two
// things connect (block-relay only) has no way to set — DisableRelayTx
// (negotiated in the version message itself) and a feefilter (its own
// message, sent after the handshake, barriered by the same ping/pong trick
// connect uses for sendheaders).
func (f *relayTestFixture) connectTx(t *testing.T, disableRelayTx bool, feeFilter int64) *scriptedPeer {
	t.Helper()

	before := f.peersSnapshot()

	far := dialScripted(t, f.m.ListenAddrs()[0])

	version := remoteVersion(uint64(time.Now().UnixNano())) //nolint:gosec // test-only nonce
	version.Services = wire.SFNodeNetwork
	version.DisableRelayTx = disableRelayTx
	far.completeOutboundHandshakeAs(t, version)

	_, ok := far.readUntil(t, wire.CmdGetHeaders).(*wire.MsgGetHeaders)
	require.True(t, ok)
	far.write(t, wire.NewMsgHeaders())

	if feeFilter > 0 {
		far.write(t, wire.NewMsgFeeFilter(feeFilter))
	}

	nonce := uint64(time.Now().UnixNano()) //nolint:gosec // test-only nonce
	far.write(t, wire.NewMsgPing(nonce))

	pong, ok := far.readUntil(t, wire.CmdPong).(*wire.MsgPong)
	require.True(t, ok)
	require.Equal(t, nonce, pong.Nonce)

	require.Eventually(t, func() bool {
		f.m.mu.Lock()
		defer f.m.mu.Unlock()

		for p := range f.m.peers {
			if _, existed := before[p]; !existed {
				return true
			}
		}

		return false
	}, 2*time.Second, 10*time.Millisecond, "the manager never registered the new connection")

	return far
}

// TestRelayTxsAnnouncesToPlainPeer is the wire-level pin on the basic path:
// a peer with no feefilter and relay enabled gets an inv for a relayed tx.
func TestRelayTxsAnnouncesToPlainPeer(t *testing.T) {
	f := newRelayTestFixture(t)

	peer := f.connectTx(t, false, 0)

	tx := TxHashAndFee{TxHash: chainhash.Hash{0xAA}, Fee: 1000, Size: 1000}
	f.m.RelayTxs([]TxHashAndFee{tx})

	invMsg, ok := peer.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Len(t, invMsg.InvList, 1)
	require.Equal(t, wire.InvTypeTx, invMsg.InvList[0].Type)
	require.Equal(t, tx.TxHash, invMsg.InvList[0].Hash)
}

// TestRelayTxsBatchesMultipleTxsIntoOneInv is net_processing.cpp
// SendTxnInventory's own "batched tx invs per peer" shape: two txs relayed
// in the same RelayTxs call reach the peer as ONE inv message with two
// entries, not two separate inv messages.
func TestRelayTxsBatchesMultipleTxsIntoOneInv(t *testing.T) {
	f := newRelayTestFixture(t)

	peer := f.connectTx(t, false, 0)

	tx1 := TxHashAndFee{TxHash: chainhash.Hash{0x01}, Fee: 1000, Size: 1000}
	tx2 := TxHashAndFee{TxHash: chainhash.Hash{0x02}, Fee: 1000, Size: 1000}
	f.m.RelayTxs([]TxHashAndFee{tx1, tx2})

	invMsg, ok := peer.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Len(t, invMsg.InvList, 2)

	hashes := []chainhash.Hash{invMsg.InvList[0].Hash, invMsg.InvList[1].Hash}
	require.ElementsMatch(t, []chainhash.Hash{tx1.TxHash, tx2.TxHash}, hashes)
}

// TestRelayTxsSuppressesDisabledPeerAndDuplicate pairs both negatives this
// plan's addenda warn about (E5) with a positive arrival on another peer in
// the SAME RelayTxs call, and a second call for the same hash: a peer that
// negotiated fRelayTxes=false never gets anything, and a peer that already
// received a hash does not get it again, while a plain peer (both times)
// does — proving "received nothing" here really is "correctly suppressed",
// not "sent but slow".
func TestRelayTxsSuppressesDisabledPeerAndDuplicate(t *testing.T) {
	f := newRelayTestFixture(t)

	disabledPeer := f.connectTx(t, true, 0)
	plainPeer := f.connectTx(t, false, 0)

	tx := TxHashAndFee{TxHash: chainhash.Hash{0xBB}, Fee: 1000, Size: 1000}
	f.m.RelayTxs([]TxHashAndFee{tx})

	invMsg, ok := plainPeer.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Equal(t, tx.TxHash, invMsg.InvList[0].Hash)

	// A second relay of the SAME hash: the plain peer already knows it
	// (knownTxs), so a barrier tx proves whether the second relay actually
	// reached the wire for it.
	barrier := TxHashAndFee{TxHash: chainhash.Hash{0xCC}, Fee: 1000, Size: 1000}
	f.m.RelayTxs([]TxHashAndFee{tx, barrier})

	secondInv, ok := plainPeer.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Len(t, secondInv.InvList, 1, "the already-relayed tx must not be re-announced")
	require.Equal(t, barrier.TxHash, secondInv.InvList[0].Hash)

	// The disabled peer never received anything at all across either call.
	// Read the very NEXT message on the wire directly (not readUntil, which
	// SKIPS non-matching messages and would silently swallow a spurious inv
	// while scanning for the pong — exactly the trap this has to avoid): it
	// must be the pong itself, with no inv in front of it.
	nonce := uint64(42)
	disabledPeer.write(t, wire.NewMsgPing(nonce))

	next := disabledPeer.read(t)
	pong, ok := next.(*wire.MsgPong)
	require.True(t, ok, "expected the pong as the very next message, got %T (a relay-disabled peer must receive no inv at all)", next)
	require.Equal(t, nonce, pong.Nonce)
}

// TestRelayTxsFeeFilterSuppressesLowFeeButNotHighFee pairs a suppressed
// low-fee peer with an announced high-fee peer in the same call (E5).
func TestRelayTxsFeeFilterSuppressesLowFeeButNotHighFee(t *testing.T) {
	f := newRelayTestFixture(t)

	pickyPeer := f.connectTx(t, false, 2000) // feefilter 2000 sat/kB
	relaxedPeer := f.connectTx(t, false, 0)

	// 1000 sat / 1000 bytes = 1000 sat/kB: below pickyPeer's filter.
	tx := TxHashAndFee{TxHash: chainhash.Hash{0xDD}, Fee: 1000, Size: 1000}
	f.m.RelayTxs([]TxHashAndFee{tx})

	invMsg, ok := relaxedPeer.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Equal(t, tx.TxHash, invMsg.InvList[0].Hash)

	nonce := uint64(43)
	pickyPeer.write(t, wire.NewMsgPing(nonce))

	next := pickyPeer.read(t)
	pong, ok := next.(*wire.MsgPong)
	require.True(t, ok, "expected the pong as the very next message, got %T (a below-filter peer must receive no inv at all)", next)
	require.Equal(t, nonce, pong.Nonce)
}
