package bsv

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

const (
	// floodBatchSize and floodRounds set how hard the port floods. Upstream floods
	// until a log line appears and so has no fixed count; this needs a number, and
	// the number is kept deliberately small.
	//
	// Every tolerated read error is logged, so the flood costs one log line per
	// message in the suite's output - 200,000 of them at the first attempt, which
	// is both unusable and the likely source of most of the allocation it appeared
	// to provoke. Volume buys nothing here anyway: with no reply generated there is
	// no send buffer to fill, so the only thing more messages demonstrate is that
	// the node keeps going, which 2,000 shows as well as 200,000.
	floodBatchSize = 1_000
	floodRounds    = 2

	// floodHeapGrowthCeiling bounds how much more live memory the process holds
	// after the flood than before it.
	//
	// Both samples are taken after a forced collection, which is what makes this a
	// retention measurement rather than a garbage-collector-scheduling one. The
	// first version of this assertion sampled HeapInuse without forcing a
	// collection, passed standalone, and failed in a full suite run reporting
	// 2727 MB of "retention" - a figure identical to the total allocated during the
	// window, because in a suite process whose heap target is already ~10 GB the
	// collector simply does not run during a three-second flood. It was measuring
	// when the collector last ran.
	floodHeapGrowthCeiling = 256 << 20
)

// TestBSVPeerFlood ports bsv-peer-flood.py, whose docstring is "Test node handling
// for a peer that floods it with small msgs while not reading our responses".
//
// Upstream builds that situation out of four mechanisms, and Teranode has none of
// them:
//
//   - The flood message is a truncated `blocktxn`. go-wire implements no blocktxn,
//     getblocktxn or cmpctblock at all - only sendcmpct - so to Teranode this is
//     simply an unrecognised command.
//   - Upstream's node answers each parse failure with a reject, which is what fills
//     its send buffer. Teranode answers nothing: an undecodable message is a
//     *wire.MessageError, which peer.isAllowedReadError tolerates on regtest from a
//     loopback peer (see the unknown-command-disconnects-off-regtest gap).
//   - The buffers are bounded by -maxreceivebuffer and -maxsendbuffer. Teranode has
//     no equivalent settings; peer.queueHandler holds pending output in an
//     unbounded list.
//   - The recovery is observed through getpeerinfo's sendsize and recvsize.
//     Teranode's getpeerinfo reports neither.
//
// What survives is the property the four mechanisms exist to protect, and it is
// worth asserting on its own: a peer can flood a Teranode node with junk it will
// not read the answers to, and the node neither falls over nor retains the flood.
//
// On the unbounded list, recorded because it looked like a finding and turned out
// not to be one: a flood of a million pings - which the node does answer, one pong
// each, so the pending-output list is genuinely exercised - left HeapInuse
// oscillating between about 540 MB and 910 MB with the collector keeping pace, and
// showed no upward trend. The list is unbounded by construction; that a flood
// drives it to unbounded growth was not demonstrated, so no gap is raised for it.
// The heap ceiling asserted below is the tripwire that would catch it if that ever
// changed.
func TestBSVPeerFlood(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	// Upstream's generate(1), commented there as "Get out of IBD", and needed here
	// so requireStillServing has a chain to answer from.
	td.MineBlocks(t, 1)

	// A RawConn is what makes this port possible: wirepeer.Peer runs a read loop,
	// and the whole point is a peer that does not read. Nothing below ever reads
	// from this connection, so the node's replies back up in the socket buffers and
	// then in its own pending-output list.
	flooder := wirepeer.DialRaw(t, td)
	defer flooder.Close()

	// Send a version so the node treats this as a peer worth answering. Upstream's
	// P2PHandler completes a full handshake; a version alone is enough here,
	// because Teranode does not wait for a verack - see the
	// pre-handshake-message-leak gap.
	flooder.WriteFrame(t, wire.CmdVersion, floodVersionPayload(t))

	// Upstream's msg_badblocktxn: a 32-byte block hash of zeroes, then a
	// transaction-list length of 0x3a followed by only four bytes. Truncated on
	// purpose, and kept byte-for-byte even though Teranode rejects the command
	// before ever reaching the truncation, so the two files still line up.
	// The command is a literal because there is no wire.CmdBlockTxn to reference:
	// go-wire does not define the message, which is the first of the four
	// divergences above and is demonstrated by this line failing to compile any
	// other way.
	batch := repeatedFrames(td, "blocktxn", badBlockTxnPayload(), floodBatchSize)

	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	start := time.Now()

	for range floodRounds {
		flooder.WriteBytes(t, batch)
	}

	sent := floodBatchSize * floodRounds

	// Give the node time to work through the backlog it cannot reply to, then force
	// a collection so what remains is what is actually retained. See
	// floodHeapGrowthCeiling for why this matters.
	time.Sleep(2 * time.Second)
	runtime.GC()
	runtime.ReadMemStats(&after)

	// HeapInuse can fall as well as rise across the flood, since the collector runs
	// throughout, so growth is clamped at zero rather than subtracted blindly.
	var heapGrowth uint64
	if after.HeapInuse > before.HeapInuse {
		heapGrowth = after.HeapInuse - before.HeapInuse
	}

	t.Logf("flooded %d malformed messages (%.1f MB) in %s; HeapInuse %.1f -> %.1f MB "+
		"(growth %.1f MB), TotalAlloc +%.1f MB",
		sent, float64(len(batch)*floodRounds)/(1<<20), time.Since(start).Round(time.Millisecond),
		float64(before.HeapInuse)/(1<<20), float64(after.HeapInuse)/(1<<20),
		float64(heapGrowth)/(1<<20), float64(after.TotalAlloc-before.TotalAlloc)/(1<<20))

	// The tripwire on retention. See floodHeapGrowthCeiling for why it is loose.
	require.Less(t, heapGrowth, uint64(floodHeapGrowthCeiling),
		"TRIPWIRE: the node retained %.1f MB more after a flood of %.1f MB it could not answer. "+
			"peer.queueHandler holds pending output in an unbounded list; if that has started to grow "+
			"without bound this is where it shows",
		float64(heapGrowth)/(1<<20), float64(len(batch)*floodRounds)/(1<<20))

	// Upstream's premise is that each malformed message draws a reject, and that is
	// what fills its send buffer. Asserted here as an absence.
	//
	// What the flooder HAS been sent is the node's handshake - version, verack and
	// protoconf, queued in reply to the version above and still sitting unread
	// because this peer never reads. Asserting that it arrived matters: it proves
	// the node was willing and able to write to this peer, so the absence of
	// rejects is a decision about the malformed messages rather than a dead
	// connection.
	drained := drainAvailable(t, flooder)

	require.Contains(t, string(drained), wire.CmdVersion,
		"the node's handshake reply should be waiting unread, proving it does write to this peer")
	require.NotContains(t, string(drained), wire.CmdReject,
		"Teranode answers an unrecognised command with nothing, so no reject should appear among the "+
			"%d bytes it did send", len(drained))

	// Upstream's recovery check, in the only form available: the node is still
	// accepting connections and still answering from the blockchain store.
	requireStillServing(t, td)
}

// drainAvailable reads everything the node has queued for this connection, up to a
// cap, and returns it.
//
// It loops because the point is to see everything sent: a single read would return
// one bufferful, and "no reject arrived" is only meaningful if the whole backlog
// was examined. The cap bounds the loop if the node turns out to be sending far
// more than expected, which would itself be the interesting result - so the amount
// read is reported by the caller rather than silently discarded.
func drainAvailable(t *testing.T, conn *wirepeer.RawConn) []byte {
	t.Helper()

	const (
		cap        = 8 << 20
		chunk      = 64 << 10
		quietAfter = 500 * time.Millisecond
	)

	var out []byte

	for len(out) < cap {
		got := conn.ReadSome(t, chunk, quietAfter)
		if len(got) == 0 {
			break
		}

		out = append(out, got...)
	}

	return out
}

// floodVersionPayload builds a version message good enough to be accepted.
func floodVersionPayload(t *testing.T) []byte {
	t.Helper()

	me := wire.NewNetAddress(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, wire.SFNodeNetwork)
	you := wire.NewNetAddress(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}, wire.SFNodeNetwork)

	msg := wire.NewMsgVersion(me, you, 0x99, 0)

	// A BSV user agent, or the peer is banned for the agent rather than tested for
	// the flood. See the bsv-ban-useragents.py port.
	msg.UserAgent = ""
	require.NoError(t, msg.AddUserAgent("Bitcoin SV", "1.2.2", "teranode-wirepeer"),
		"set the version user agent")

	var buf bytes.Buffer
	require.NoError(t, msg.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding),
		"encode the version message")

	return buf.Bytes()
}

// badBlockTxnPayload is upstream's msg_badblocktxn body: a zero block hash, a
// declared transaction count of 0x3a, and four bytes where 58 transactions should
// be.
func badBlockTxnPayload() []byte {
	payload := make([]byte, 32)

	return append(payload, 0x3a, 0x00, 0x00, 0x00, 0x00)
}

// repeatedFrames builds n identical wire frames back to back, so the flood can be
// written with a handful of syscalls rather than one per message. Framing is done
// here rather than through RawConn.WriteFrame because that writes each frame
// separately, which at these counts dominates the runtime.
func repeatedFrames(td *daemon.TestDaemon, command string, payload []byte, n int) []byte {
	header := make([]byte, 24)
	binary.LittleEndian.PutUint32(header[0:4], uint32(td.Settings.ChainCfgParams.Net))
	copy(header[4:16], command)
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(payload)))

	first := sha256.Sum256(payload)
	second := sha256.Sum256(first[:])
	copy(header[20:24], second[:4])

	frame := append(header, payload...)

	out := make([]byte, 0, len(frame)*n)
	for range n {
		out = append(out, frame...)
	}

	return out
}
