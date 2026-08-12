package bsv

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

const (
	// legacyMaxProtocolPayloadLength is upstream's
	// LEGACY_MAX_PROTOCOL_PAYLOAD_LENGTH: the pre-protoconf 1 MiB message cap,
	// and the smallest value bitcoin-sv allows -maxprotocolrecvpayloadlength to
	// take. The port declares this as its own limit, exactly as upstream's
	// mininode does.
	legacyMaxProtocolPayloadLength = 1024 * 1024

	// protoconfAnnounce is how many transactions the port announces in one inv.
	//
	// Upstream's splitting assertion lives in its second case, which announces
	// estimateMaxInvElements(3 MiB) = 87381 items with the node started at
	// -maxprotocolrecvpayloadlength=500MiB. Teranode can reach neither half of
	// that: it has no such setting, and 87381 is above go-wire's MaxInvPerMsg of
	// 50000 so the inv cannot even be encoded. So the same rule is tested at an
	// input Teranode can actually take - 40000 items is 1,440,009 bytes, which a
	// node honouring the 1 MiB declared above would have to split into two
	// getdata messages of 29126 and 10874.
	protoconfAnnounce = 40000
)

// estimateMaxInvElements is upstream's CInv.estimateMaxInvElements: how many
// inventory vectors fit in a payload of the given size, given a 9-byte varint
// count header and 36 bytes per vector.
func estimateMaxInvElements(payloadLength int) int {
	return (payloadLength - 9) / 36
}

// invPayloadSize is the inverse: the wire size of an inv or getdata carrying n
// vectors.
func invPayloadSize(n int) int {
	return 9 + n*36
}

// TestBSVProtoconf ports bsv-protoconf.py, which tests that protoconf's
// negotiated maximum receive payload length is honoured in both directions.
//
// Teranode advertises the value and then ignores it, both ways, and the two
// tripwires below are what pin that. The advertised number itself is right:
// peer.go sends wire.NewMsgProtoconf(0, ...), which resolves to go-wire's
// DefaultMaxRecvPayloadLength of 2 MiB - the same default upstream expects when
// -maxprotocolrecvpayloadlength is unset. So the first subtest is a genuine
// reproduction, not a coincidence.
//
// What does not port:
//
//   - Upstream runs its whole suite twice, once with
//     -maxprotocolrecvpayloadlength=500MiB. Teranode hardcodes the argument to
//     NewMsgProtoconf and exposes no setting, so only the default case exists.
//   - Upstream's run_ban_test asserts the node BANS a peer whose inv exceeds the
//     node's advertised limit. Teranode neither bans nor rejects, and the
//     threshold upstream probes - 58253 items for a 2 MiB advertisement - cannot
//     be encoded by go-wire at all.
//   - Upstream's run_recvinvqueuefactor_test needs -recvinvqueuefactor, a
//     bitcoind-only knob governing how many announced invs the node remembers.
//     Teranode has no equivalent to set or to observe.
//
// See the protoconf-payload-limit-not-honoured gap for all of it.
func TestBSVProtoconf(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	// Upstream's self.nodes[0].generate(1), commented there as "Prepare initial
	// block. Needed so that GETDATA can be send back."
	td.MineBlocks(t, 1)

	p := wirepeer.Connect(t, td)
	defer p.Close()

	var advertised uint32

	t.Run("the node advertises the default maximum receive payload length", func(t *testing.T) {
		// Upstream: test_node.wait_for_protoconf(), then read
		// protoconf.max_recv_payload_length.
		require.Eventually(t, func() bool { return p.Count(wire.CmdProtoconf) > 0 },
			10*time.Second, 100*time.Millisecond,
			"the node should send a protoconf, received: %s", p.Summary())

		msg, ok := p.Received(wire.CmdProtoconf)[0].(*wire.MsgProtoconf)
		require.True(t, ok, "first protoconf should decode as MsgProtoconf")

		advertised = msg.MaxRecvPayloadLength

		// Upstream: assert_equal(max_recv_payload_length,
		// 2 * LEGACY_MAX_PROTOCOL_PAYLOAD_LENGTH) when the option is unset.
		require.Equal(t, uint32(2*legacyMaxProtocolPayloadLength), advertised,
			"the node should advertise bitcoin-sv's default of 2 MiB")

		t.Logf("node advertises %d bytes = %d inv elements; we declare %d bytes = %d elements",
			advertised, estimateMaxInvElements(int(advertised)),
			legacyMaxProtocolPayloadLength, estimateMaxInvElements(legacyMaxProtocolPayloadLength))
	})

	t.Run("every announced transaction is requested, and announcing many does not ban", func(t *testing.T) {
		// Upstream's mininode declares LEGACY_MAX_PROTOCOL_PAYLOAD_LENGTH as its
		// own limit before announcing anything.
		p.Send(t, wire.NewMsgProtoconf(legacyMaxProtocolPayloadLength, true))

		// See the tx-inv-dropped-during-startup-window gap: transaction
		// announcements are dropped for a window after startup.
		requireTxInvProcessingLive(t, p)

		announced := announceTransactions(t, p, protoconfAnnounce)

		// Upstream: wait_for_getdata, then check every announced element was asked
		// for. Polled rather than waited-once because the node may answer across
		// several messages - which is exactly what the next subtest is about.
		tracker := newInvRequestTracker(announced)

		require.Eventually(t, func() bool { return tracker.progress(p) == len(announced) },
			30*time.Second, 200*time.Millisecond,
			"the node should request every announced transaction; got %d of %d",
			tracker.count(), len(announced))

		// Upstream: assert_equal(len(listbanned()), 0) - an inv within the limits
		// costs the peer nothing.
		require.Empty(t, wirepeer.TryListBanned(td), "an inv within the advertised limit must not ban the peer")
		p.AssertStillConnected(t, time.Second, "an inv within the advertised limit must not cost the connection")
	})

	t.Run("getdata is not split to respect the limit we declared", func(t *testing.T) {
		// Upstream asserts the node splits its getdata so no single message exceeds
		// the limit the peer declared: all but the last carry
		// estimateMaxInvElements(our limit) items, and the last carries the
		// remainder. Teranode sends one message however large it needs to be -
		// netsync caps requests per inv message at wire.MaxInvPerMsg (50000), which
		// is a count, not a byte budget, and the peer's declared limit is read
		// nowhere.
		var largest int

		for _, msg := range p.Received(wire.CmdGetData) {
			gd, ok := msg.(*wire.MsgGetData)
			if !ok {
				continue
			}

			if size := invPayloadSize(len(gd.InvList)); size > largest {
				largest = size
			}
		}

		require.Greater(t, largest, legacyMaxProtocolPayloadLength,
			"TRIPWIRE: the node's largest getdata (%d bytes) now fits inside the %d byte limit this peer "+
				"declared. If Teranode has learned to honour a peer's protoconf, revisit the "+
				"protoconf-payload-limit-not-honoured gap and assert upstream's split instead",
			largest, legacyMaxProtocolPayloadLength)

		t.Logf("largest getdata: %d bytes against a declared limit of %d; a compliant node would have "+
			"split %d items into %d messages",
			largest, legacyMaxProtocolPayloadLength, protoconfAnnounce,
			(protoconfAnnounce+estimateMaxInvElements(legacyMaxProtocolPayloadLength)-1)/
				estimateMaxInvElements(legacyMaxProtocolPayloadLength))
	})

	t.Run("an inv inside the advertised limit but over MaxInvPerMsg is dropped in silence", func(t *testing.T) {
		// The other direction of the same mismatch. The node advertises 2 MiB but
		// go-wire refuses an inv above MaxInvPerMsg, so the largest inv it will
		// actually decode is 1,800,009 bytes - less than it advertises. A peer that
		// believes the advertisement and sends 52000 items gets no getdata, no
		// reject and no ban.
		//
		// Upstream's run_ban_test probes the boundary at its advertised limit and
		// expects a ban one item over. Both the boundary and the ban are out of
		// reach here, so this asserts what Teranode does at the boundary it does
		// have - exactly one vector past MaxInvPerMsg, which keeps upstream's
		// "one over the line" shape.
		const entries = wire.MaxInvPerMsg + 1

		require.Greater(t, entries, wire.MaxInvPerMsg, "the point is to exceed MaxInvPerMsg")
		require.Less(t, uint32(invPayloadSize(entries)), advertised,
			"the point is to stay inside the advertised limit")

		before := p.Count(wire.CmdGetData)

		// Hand-framed: go-wire's AddInvVect refuses to build this, which is the
		// asymmetry under test.
		p.SendRawFrame(t, wire.CmdInv, oversizedInvPayload(entries))

		p.AssertStillConnected(t, 3*time.Second,
			"TRIPWIRE: the node now acts on an inv that exceeds MaxInvPerMsg. Whatever it does now - "+
				"reject, disconnect or ban - revisit the protoconf-payload-limit-not-honoured gap")

		require.Equal(t, before, p.Count(wire.CmdGetData),
			"no getdata should follow an inv the node cannot decode")
		require.Empty(t, p.Received(wire.CmdReject), "and no reject either")
		require.Empty(t, wirepeer.TryListBanned(td), "and nothing banned")
	})
}

// announceTransactions announces n transactions in a single inv and returns the
// hashes, as upstream's msg_inv([CInv(CInv.TX, i) for i in range(0, n)]) does.
// The hashes are distinct and carry a marker byte so they cannot collide with the
// probe hashes requireTxInvProcessingLive sends.
func announceTransactions(t *testing.T, p *wirepeer.Peer, n int) []chainhash.Hash {
	t.Helper()

	hashes := make([]chainhash.Hash, n)
	msg := wire.NewMsgInvSizeHint(uint(n))

	for i := range n {
		hashes[i][0] = byte(i)
		hashes[i][1] = byte(i >> 8)
		hashes[i][2] = 0xAA

		require.NoError(t, msg.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &hashes[i])),
			"build an inv of %d transactions", n)
	}

	p.Send(t, msg)

	return hashes
}

// invRequestTracker counts how many announced transactions the node has asked
// for, across however many getdata messages it uses.
//
// It exists because the obvious version - rebuild both sets and rescan every
// getdata on each poll - is O(announced) per tick, and with 40000 announced
// transactions polled five times a second that cost is the test's own, not the
// node's. It measurably dominated this port's runtime before being fixed. The
// tracker keeps the wanted set and the running total, and remembers how many
// getdata messages it has already consumed so each poll only looks at new ones.
type invRequestTracker struct {
	wanted   map[chainhash.Hash]bool
	seen     map[chainhash.Hash]bool
	consumed int
}

func newInvRequestTracker(hashes []chainhash.Hash) *invRequestTracker {
	wanted := make(map[chainhash.Hash]bool, len(hashes))
	for _, h := range hashes {
		wanted[h] = true
	}

	return &invRequestTracker{
		wanted: wanted,
		seen:   make(map[chainhash.Hash]bool, len(hashes)),
	}
}

// progress consumes any getdata messages that have arrived since the last call
// and returns the running total. It asserts nothing, so it is safe inside a
// polling condition.
func (t *invRequestTracker) progress(p *wirepeer.Peer) int {
	msgs := p.Received(wire.CmdGetData)

	for _, msg := range msgs[t.consumed:] {
		gd, ok := msg.(*wire.MsgGetData)
		if !ok {
			continue
		}

		for _, iv := range gd.InvList {
			if iv.Type == wire.InvTypeTx && t.wanted[iv.Hash] {
				t.seen[iv.Hash] = true
			}
		}
	}

	t.consumed = len(msgs)

	return len(t.seen)
}

// count returns the total without consuming anything, for failure messages.
func (t *invRequestTracker) count() int {
	return len(t.seen)
}

// oversizedInvPayload serialises an inv message body carrying n vectors, bypassing
// go-wire's MaxInvPerMsg ceiling. Only the count prefix and the vectors matter, so
// this is hand-rolled rather than encoded: n above 50000 is precisely what go-wire
// will not produce.
func oversizedInvPayload(n int) []byte {
	var buf bytes.Buffer

	// A varint count: 0xFD followed by a little-endian uint16 covers up to 65535.
	buf.WriteByte(0xFD)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(n))

	for i := range n {
		_ = binary.Write(&buf, binary.LittleEndian, uint32(wire.InvTypeTx))

		var h chainhash.Hash
		h[0] = byte(i)
		h[1] = byte(i >> 8)
		h[2] = 0xBB

		buf.Write(h[:])
	}

	return buf.Bytes()
}
