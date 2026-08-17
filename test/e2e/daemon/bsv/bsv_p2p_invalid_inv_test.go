package bsv

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// TestBSVP2PInvalidInv ports bsv-p2p-invalid-inv.py, whose docstring is "Test
// disconnection from invalid inventory type".
//
// Upstream announces a block, then a transaction, checking after each that the
// node logged that it saw the announcement and did not hang up. It then announces
// an inventory vector of type ERROR (0) and asserts the node logs "Got invalid
// inv" and disconnects. No honest peer sends type 0, so bitcoin-sv treats it as a
// protocol violation.
//
// The first half ports, and better than upstream states it. Upstream can only
// read its log for "got block inv"; a wire peer can watch the node act, because
// Teranode answers each announcement with a getdata for exactly the hash
// announced. That is measured, and it is strictly stronger evidence that the
// announcement was processed than the log line is.
//
// The second half does not port: Teranode ignores unsupported inventory types
// rather than disconnecting. SyncManager.processInvMsg
// (services/legacy/netsync/manager.go) switches on the vector type and its
// default branch is a bare return, with the caller commenting "Ignore unsupported
// inventory types" - so the choice is deliberate, not an oversight. What follows
// from it is not: nothing charges the peer for sending them. See the
// unsupported-inv-type-ignored-not-disconnected gap, and the second subtest,
// which is that gap's impact made reproducible.
func TestBSVP2PInvalidInv(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	// Upstream is setup_clean_chain. A block is mined because requireStillServing
	// at the end needs a chain longer than genesis to be meaningful.
	td.MineBlocks(t, 1)

	p := wirepeer.Connect(t, td)
	defer p.Close()

	t.Run("valid inventory types are processed and cost the peer nothing", func(t *testing.T) {
		// Upstream: msg_inv([CInv(CInv.BLOCK, 0)]), then wait for "got block inv".
		blockHash := invHash(0)
		sendInv(t, p, wire.InvTypeBlock, blockHash)

		requireGetDataFor(t, p, wire.InvTypeBlock, blockHash)

		// Precondition, not an assertion, and it has no upstream counterpart
		// because upstream does not need one. See the comment on the helper: for a
		// window after startup Teranode drops transaction announcements while
		// handling block announcements normally, so upstream's tx assertion below
		// would fail on timing rather than on substance.
		requireTxInvProcessingLive(t, p)

		// Upstream: msg_inv([CInv(CInv.TX, 1)]), then wait for "got txn inv".
		txHash := invHash(1)
		sendInv(t, p, wire.InvTypeTx, txHash)

		requireGetDataFor(t, p, wire.InvTypeTx, txHash)

		// Upstream: time.sleep(2); assert conn.transport.cb.connected.
		p.AssertStillConnected(t, 2*time.Second,
			"announcing a block and a transaction should not cost the peer its connection")
	})

	t.Run("unsupported inventory types are ignored rather than punished", func(t *testing.T) {
		// Upstream: msg_inv([CInv(CInv.ERROR, 2)]) -> "Got invalid inv" and a
		// disconnect. Teranode does neither.
		sendInv(t, p, wire.InvTypeError, invHash(2))

		p.AssertStillConnected(t, 2*time.Second,
			"TRIPWIRE: Teranode now disconnects a peer that sends an unsupported inventory type, as "+
				"bitcoin-sv does. Revisit the unsupported-inv-type-ignored-not-disconnected gap and "+
				"assert upstream's disconnect instead of this tolerance")

		require.Empty(t, p.Received(wire.CmdReject),
			"an unsupported inventory type draws no reject either")

		// No upstream counterpart, and here because it is what the tolerance above
		// actually costs. A single message may carry wire.MaxInvPerMsg vectors, and
		// handleInvMsg spawns a goroutine per non-block vector before the type
		// switch that discards it - so this is 50,000 goroutines whose entire body
		// is a return, bought for one ~1.8MB message, chargeable to nobody. Sent
		// once here rather than in a loop: the point is that the peer survives it
		// with no ban score, not to stress the node.
		flood := wire.NewMsgInvSizeHint(wire.MaxInvPerMsg)

		for i := range wire.MaxInvPerMsg {
			var h chainhash.Hash
			h[0] = byte(i)
			h[1] = byte(i >> 8)

			require.NoError(t, flood.AddInvVect(wire.NewInvVect(wire.InvTypeError, &h)),
				"build a full-size inv of unsupported vectors")
		}

		p.Send(t, flood)

		p.AssertStillConnected(t, 2*time.Second,
			"TRIPWIRE: a full-size inv of unsupported types now costs the peer its connection. That is "+
				"bitcoin-sv's behaviour and an improvement; update the "+
				"unsupported-inv-type-ignored-not-disconnected gap")
	})

	// Upstream's framework fails the test if the node fell over.
	requireStillServing(t, td)
}

// invHash builds the hash upstream's CInv(type, n) carries: n as a 256-bit
// little-endian integer, so n=1 is a hash whose first byte is 1.
func invHash(n byte) *chainhash.Hash {
	var h chainhash.Hash
	h[0] = n

	return &h
}

// sendInv announces a single inventory vector, as upstream's
// msg_inv([CInv(type, n)]) does.
func sendInv(t *testing.T, p *wirepeer.Peer, typ wire.InvType, hash *chainhash.Hash) {
	t.Helper()

	msg := wire.NewMsgInv()
	require.NoError(t, msg.AddInvVect(wire.NewInvVect(typ, hash)), "build inv of type %v", typ)

	p.Send(t, msg)
}

// txInvProbeAttempts and txInvProbeInterval bound requireTxInvProcessingLive.
// Measured, the window it waits out is on the order of a second or two after
// daemon start, so this allows roughly ten times that before giving up.
const (
	txInvProbeAttempts = 20
	txInvProbeInterval = 500 * time.Millisecond
)

// requireTxInvProcessingLive announces throwaway transaction hashes until the
// node asks for one, establishing that transaction announcements are being acted
// on before the port asserts on the specific one upstream sends.
//
// This exists because of a measured Teranode behaviour with no upstream analogue:
// for a window after startup, transaction announcements are silently discarded
// while block announcements from the same peer on the same connection are handled
// normally. The only code path that distinguishes the two is the processInvs gate
// in SyncManager.handleInvMsg, which is set from a blockchain-client FSM lookup
// whose failure is indistinguishable from a genuine not-yet-RUNNING state - both
// leave it false and both drop the vector without requeueing it. See the
// tx-inv-dropped-during-startup-window gap.
//
// Each attempt uses a fresh hash, because a hash already requested sits in
// requestedTxns and would not be requested again - so reusing one would make a
// live node look dead. Deliberately not require.Eventually: the condition has to
// send, and Send asserts, which must not happen inside a testify polling
// goroutine (see tryBestBlockHash).
func requireTxInvProcessingLive(t *testing.T, p *wirepeer.Peer) {
	t.Helper()

	for attempt := range txInvProbeAttempts {
		var h chainhash.Hash

		// A prefix no other hash in this port uses, so a probe can never be
		// mistaken for one of upstream's announcements.
		h[0] = 0xF0
		h[1] = byte(attempt)

		sendInv(t, p, wire.InvTypeTx, &h)

		deadline := time.Now().Add(txInvProbeInterval)
		for time.Now().Before(deadline) {
			if getDataSeen(p, wire.InvTypeTx, &h) {
				return
			}

			time.Sleep(20 * time.Millisecond)
		}
	}

	t.Fatalf("the node did not request any of %d announced transactions in %s; transaction inv "+
		"processing never came up, received: %s",
		txInvProbeAttempts, time.Duration(txInvProbeAttempts)*txInvProbeInterval, p.Summary())
}

// getDataSeen reports whether the node has already asked for the given item. It
// asserts nothing, so it is safe to call from a polling loop.
func getDataSeen(p *wirepeer.Peer, typ wire.InvType, hash *chainhash.Hash) bool {
	for _, msg := range p.Received(wire.CmdGetData) {
		gd, ok := msg.(*wire.MsgGetData)
		if !ok {
			continue
		}

		for _, iv := range gd.InvList {
			if iv.Type == typ && iv.Hash.IsEqual(hash) {
				return true
			}
		}
	}

	return false
}

// requireGetDataFor waits for the node to request the announced item, which is
// what stands in for upstream's "got block inv" / "got txn inv" log assertions.
//
// It matches on type as well as hash. Matching the hash alone would let a getdata
// for the right hash but the wrong kind of object pass, and the two announcements
// in this port differ only in type.
func requireGetDataFor(t *testing.T, p *wirepeer.Peer, typ wire.InvType, hash *chainhash.Hash) {
	t.Helper()

	p.Wait(t, 10*time.Second, "getdata for "+typ.String()+" "+hash.String(), func(msg wire.Message) bool {
		gd, ok := msg.(*wire.MsgGetData)
		if !ok {
			return false
		}

		for _, iv := range gd.InvList {
			if iv.Type == typ && iv.Hash.IsEqual(hash) {
				return true
			}
		}

		return false
	})
}
