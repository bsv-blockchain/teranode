package bsv

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

const (
	// overLongTxCount is upstream's declared transaction count: the nine bytes
	// 0xff ff ff ff ff ff ff ff 01 read as a varint. Comfortably above
	// maxTxPerBlock, so go-wire's guard rejects it before allocating anything.
	// It is also canonically encoded - above 0x100000000 - so it reaches the guard
	// rather than being turned away by ReadVarInt first, which matters because a
	// non-canonical value would test nothing.
	overLongTxCount = uint64(0x01FFFFFFFFFFFFFF)

	// permittedTxCount is a declared count the guard allows, kept deliberately
	// small. maxTxPerBlock is (MaxBlockPayload / minTxPayload) + 1, and Teranode
	// sets MaxBlockPayload to 4e9 via wire.SetLimits (services/legacy/Server.go:255),
	// so the true ceiling is 400,000,001 - which at the measured cost per declared
	// transaction is about 27 GiB. That is NOT probed here, by choice; see the
	// block-txcount-preallocation gap for the arithmetic.
	permittedTxCount = uint32(200_000)

	// minAllocationForPermittedCount is the floor this port asserts for the
	// allocation the permitted count provokes. Measured at 13.7 MB for 200,000
	// declared transactions - a clean 72.0 bytes each, linear across 200k, 600k and
	// 1.2M - against a noise floor of about 0.4 MB on an idle daemon. Set well
	// below the signal and well above the noise, because the point is the order of
	// magnitude, not the exact figure.
	minAllocationForPermittedCount = 4 << 20

	// maxAllocationForOverLongCount is the ceiling asserted for upstream's count,
	// which the guard should reject before allocating. Measured at 0.1 MB.
	maxAllocationForOverLongCount = 2 << 20

	// blockDecodeSettle bounds how long the port waits for the node to finish with
	// a malformed block frame. One of the two cases here is tolerated and keeps its
	// connection, so this cannot wait for a disconnect that may never come. The
	// allocation it needs to cover completes in milliseconds at these sizes.
	blockDecodeSettle = 3 * time.Second
)

// TestBSVBlockBadCount ports bsv-block-bad-count.py, which sends an 89-byte block
// message - 80 bytes of header followed by a huge declared transaction count - and
// checks the node rejects it as over-long rather than trying to allocate for it.
//
// Upstream's assertion is reproduced: go-wire guards the count before allocating
// (msg_block.go, "Prevent more transactions than could possibly fit into a block")
// and the node survives. Rather than assert the log line upstream greps for, this
// port measures the thing the guard exists to prevent, which is possible because a
// TestDaemon shares the process with the test: runtime.ReadMemStats sees the
// node's allocations directly.
//
// That measurement then shows the guard is set too high to be much protection. A
// count the guard permits is allocated for in full, from the same 89-byte message,
// before a single transaction is read - 72.0 bytes per declared transaction,
// measured and linear. The second subtest pins that at a small, safe scale. See the
// block-txcount-preallocation gap.
func TestBSVBlockBadCount(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	// Upstream mines a block first. Kept because the node's state should not be
	// what makes this work or not.
	td.MineBlocks(t, 1)

	t.Run("an over-long transaction count is rejected before anything is allocated", func(t *testing.T) {
		// Upstream: send MsgBlockBadCount, then wait for "reason: Over-long".
		allocated, disconnected := sendBlockHeaderWithTxCount(t, td, overLongVarInt())

		require.Less(t, allocated, uint64(maxAllocationForOverLongCount),
			"an over-long count should be refused before allocating; %d bytes were allocated", allocated)

		// The peer is NOT dropped, and the contrast with the next subtest is the
		// interesting part. The guard returns a *wire.MessageError, which
		// peer.isAllowedReadError tolerates on regtest from a loopback address, so
		// the connection survives; the permitted count below fails later with an
		// EOF-shaped error instead and costs the peer its connection. Two malformed
		// block messages, opposite outcomes, neither of them a reject. See the
		// unknown-command-disconnects-off-regtest gap for the tolerance, and
		// short-payload-read-as-peer-eof for the EOF misreading.
		require.False(t, disconnected,
			"on regtest a MessageError from a loopback peer is tolerated; if this peer is now dropped, "+
				"the isAllowedReadError tolerance has changed and unknown-command-disconnects-off-regtest "+
				"should be revisited")

		// Not upstream's assertion, but the reason its assertion matters: the node
		// is still there afterwards.
		requireStillServing(t, td)
	})

	t.Run("a permitted transaction count is allocated for in full from the same 89 bytes", func(t *testing.T) {
		allocated, disconnected := sendBlockHeaderWithTxCount(t, td, permittedVarInt())

		require.Greater(t, allocated, uint64(minAllocationForPermittedCount),
			"TRIPWIRE: the node no longer allocates in proportion to a declared transaction count it "+
				"has not read. If a payload-relative bound has been added, revisit the "+
				"block-txcount-preallocation gap and drop this subtest. Allocated %d bytes for %d "+
				"declared transactions", allocated, permittedTxCount)

		// The cost is paid before the peer is dropped, which is what makes it worth
		// anything to an attacker: the disconnect is not a defence, it is the
		// aftermath.
		require.True(t, disconnected, "the peer is dropped once the truncated payload runs out")

		t.Logf("%d declared transactions in an 89-byte frame allocated %.1f MB (%.1f bytes each); "+
			"maxTxPerBlock is 400000001",
			permittedTxCount, float64(allocated)/(1<<20), float64(allocated)/float64(permittedTxCount))

		requireStillServing(t, td)
	})
}

// sendBlockHeaderWithTxCount sends a block message consisting of 80 header bytes
// and the given encoded transaction count, and reports how many bytes the process
// allocated while handling it and whether the peer was dropped.
//
// The allocation figure is process-wide, which is only meaningful because the
// daemon runs in this process and is otherwise idle - measured drift on an idle
// daemon is under half a megabyte, against signals of tens of megabytes. TotalAlloc
// is cumulative and never decreases, so a garbage-collected allocation still counts,
// which is what makes the transient slice visible at all.
func sendBlockHeaderWithTxCount(t *testing.T, td *daemon.TestDaemon, encodedCount []byte) (uint64, bool) {
	t.Helper()

	// A fresh peer each time: the failed decode costs the sender its connection.
	p := wirepeer.Connect(t, td)
	defer p.Close()

	payload := append(bytes.Repeat([]byte{0x2a}, blockHeaderLen), encodedCount...)

	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	p.SendRawFrame(t, wire.CmdBlock, payload)

	// Wait for the node to finish with the frame. A disconnect is the prompt signal
	// where one happens - measured at ~50ms - but one of the two cases here is
	// tolerated and stays connected, so the wait is capped rather than open-ended.
	// The cap only has to cover the allocation, which for the sizes used here is
	// milliseconds.
	deadline := time.Now().Add(blockDecodeSettle)
	for time.Now().Before(deadline) && p.Connected() {
		time.Sleep(20 * time.Millisecond)
	}

	runtime.ReadMemStats(&after)

	return after.TotalAlloc - before.TotalAlloc, !p.Connected()
}

// blockHeaderLen is the fixed size of a Bitcoin block header: version, previous
// block, merkle root, timestamp, bits and nonce. Upstream fills the same 80 bytes
// with 0x2a, since nothing about the header needs to be valid for this test.
const blockHeaderLen = 80

// overLongVarInt encodes upstream's transaction count.
func overLongVarInt() []byte {
	buf := make([]byte, 9)
	buf[0] = 0xFF
	binary.LittleEndian.PutUint64(buf[1:], overLongTxCount)

	return buf
}

// permittedVarInt encodes a count the guard allows.
//
// It uses the 0xFE form deliberately: ReadVarInt rejects a non-canonical encoding,
// and any value the guard could permit is below 0xFFFFFFFF and so must be written
// in four bytes rather than eight. Encoding it the other way sends a frame that is
// turned away by the varint reader before the count is ever compared, which looks
// exactly like the guard working.
func permittedVarInt() []byte {
	buf := make([]byte, 5)
	buf[0] = 0xFE
	binary.LittleEndian.PutUint32(buf[1:], permittedTxCount)

	return buf
}
