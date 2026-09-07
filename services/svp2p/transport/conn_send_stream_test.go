package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// openerFor returns a BlockSendRequest opener over a fixed payload, plus a
// counter of how many times it was called: the two-pass contract is only
// honoured if the payload is opened twice.
func openerFor(payload []byte, calls *int) func(context.Context) (io.ReadCloser, error) {
	return func(context.Context) (io.ReadCloser, error) {
		*calls++

		return io.NopCloser(bytes.NewReader(payload)), nil
	}
}

// readFramesWithGoWire reads n messages off r with go-wire's OWN framing
// reader. That reader verifies the payload checksum against the header
// (message.go readMessageHeader plus the checksum compare), so a frame this
// helper accepts is a frame a real SVNode peer accepts. Nothing in this
// helper shares code with the send path under test.
func readFramesWithGoWire(t *testing.T, r io.Reader, n int) []wire.Message {
	t.Helper()

	out := make([]wire.Message, 0, n)

	for i := 0; i < n; i++ {
		_, msg, _, err := wire.ReadMessageWithEncodingN(r, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
		require.NoError(t, err, "frame %d did not decode", i)

		out = append(out, msg)
	}

	return out
}

// TestSendBlockFrameIsReadableByGoWire is the framing test that matters: the
// far side is go-wire's own reader, which recomputes the double-SHA256 of the
// payload and compares it with the header checksum. A wrong checksum, a wrong
// declared length, or a byte out of place all surface here as a decode error
// rather than as a difference this test's own encoder would reproduce.
func TestSendBlockFrameIsReadableByGoWire(t *testing.T) {
	a, b := net.Pipe()
	ca := New(a, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)

	blk := testMsgBlock(t, 4)
	payload := blockPayload(t, blk)

	var calls int

	errCh := make(chan error, 1)

	go func() {
		errCh <- ca.SendBlock(ctx, BlockSendRequest{
			Length: uint64(len(payload)),
			Open:   openerFor(payload, &calls),
		})
	}()

	msgs := readFramesWithGoWire(t, b, 1)

	require.NoError(t, <-errCh)

	got, ok := msgs[0].(*wire.MsgBlock)
	require.True(t, ok, "expected a block message, got %T", msgs[0])
	require.Equal(t, blk.BlockHash(), got.BlockHash())
	require.Len(t, got.Transactions, len(blk.Transactions))
	require.Equal(t, payload, blockPayload(t, got))

	// The two-pass ruling: one pass hashes, one pass writes.
	require.Equal(t, 2, calls, "the payload must be opened exactly twice")
}

// TestSendBlockChecksumIsPayloadDoubleSHA256 pins the checksum bytes
// themselves, computed here from the payload with chainhash rather than with
// the send path's own hasher, and asserts they are not the zeroed checksum
// that protocol.cpp:220-237 reserves for the extmsg path.
func TestSendBlockChecksumIsPayloadDoubleSHA256(t *testing.T) {
	a, b := net.Pipe()
	ca := New(a, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)

	blk := testMsgBlock(t, 3)
	payload := blockPayload(t, blk)

	var calls int

	errCh := make(chan error, 1)

	go func() {
		errCh <- ca.SendBlock(ctx, BlockSendRequest{
			Length: uint64(len(payload)),
			Open:   openerFor(payload, &calls),
		})
	}()

	hdr := make([]byte, wire.MessageHeaderSize)
	_, err := io.ReadFull(b, hdr)
	require.NoError(t, err)

	require.Equal(t, uint32(wire.MainNet), binary.LittleEndian.Uint32(hdr[0:4]))
	require.Equal(t, wire.CmdBlock, string(bytes.TrimRight(hdr[4:4+wire.CommandSize], "\x00")))
	require.Equal(t, uint32(len(payload)), binary.LittleEndian.Uint32(hdr[16:20])) //nolint:gosec // test payload is small

	want := chainhash.DoubleHashB(payload)[0:4]
	require.Equal(t, want, hdr[20:24], "header checksum is not the payload's double-SHA256 prefix")
	require.NotEqual(t, []byte{0, 0, 0, 0}, hdr[20:24], "a non-extended message must not carry a zeroed checksum")

	body := make([]byte, len(payload))
	_, err = io.ReadFull(b, body)
	require.NoError(t, err)
	require.Equal(t, payload, body)

	require.NoError(t, <-errCh)
}

// TestSendBlockInterleavedWithQueuedMessagesHoldsFraming is the socket
// alignment test. Three frames go out through three different lanes, and all
// three must decode in order through go-wire: a streamed block that writes the
// socket directly and a queued message that go-wire frames cannot be allowed
// to interleave (the writeLoop single-writer rule).
func TestSendBlockInterleavedWithQueuedMessagesHoldsFraming(t *testing.T) {
	a, b := net.Pipe()
	ca := New(a, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)

	blk := testMsgBlock(t, 6)
	payload := blockPayload(t, blk)

	var calls int

	require.NoError(t, ca.Send(wire.NewMsgPing(11)))

	errCh := make(chan error, 1)

	go func() {
		errCh <- ca.SendBlock(ctx, BlockSendRequest{
			Length: uint64(len(payload)),
			Open:   openerFor(payload, &calls),
		})

		errCh <- ca.Send(wire.NewMsgPing(22))
	}()

	msgs := readFramesWithGoWire(t, b, 3)

	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)

	first, ok := msgs[0].(*wire.MsgPing)
	require.True(t, ok, "expected the queued ping first, got %T", msgs[0])
	require.Equal(t, uint64(11), first.Nonce)

	got, ok := msgs[1].(*wire.MsgBlock)
	require.True(t, ok, "expected the block second, got %T", msgs[1])
	require.Equal(t, blk.BlockHash(), got.BlockHash())

	last, ok := msgs[2].(*wire.MsgPing)
	require.True(t, ok, "expected the trailing ping third, got %T", msgs[2])
	require.Equal(t, uint64(22), last.Nonce)
}

// TestSendBlockRefusesAboveTheFramingLimit is the >4 GiB refusal for a peer
// that has not negotiated ExtendedPayloadVersion: the header's length field
// is a uint32, so a longer payload cannot be framed at all without the extmsg
// path. Nothing may be opened, let alone written. A 70016 peer instead gets
// the extended header — see TestSendBlock_RoundTripsAcrossTheExtendedBoundary.
func TestSendBlockRefusesAboveTheFramingLimit(t *testing.T) {
	a, b := net.Pipe()

	cfg := testConfig()
	cfg.ProtocolVersion = ExtendedPayloadVersion - 1
	ca := New(a, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)

	var calls int

	// The boundary itself: 0xffffffff is the extended-message MARKER, so a
	// basic header must never declare it. go-wire's writer switches to extmsg
	// at `>= math.MaxUint32` (go-wire WriteMessageWithEncodingN,
	// message.go:391, v1.2.10); its reader takes the extended branch only for
	// command "extmsg" (readMessageHeader, message.go:270) yet applies
	// `if length == 0xffffffff { length = hdr.extLength }` unconditionally
	// (ReadMessageWithEncodingN, message.go:483-485), so a "block" frame
	// declaring it reads a zero-byte payload and desynchronises the socket.
	// That makes the exact value the hostile case, not the one above it.
	require.Equal(t, uint64(math.MaxUint32)-1, MaxBlockFrameBytes,
		"0xffffffff must be excluded, not served")

	for _, length := range []uint64{
		MaxBlockFrameBytes + 1, // exactly 0xffffffff, the reserved marker
		math.MaxUint32 + 1,     // genuinely beyond a uint32
	} {
		calls = 0

		err := ca.SendBlock(ctx, BlockSendRequest{
			Length: length,
			Open:   openerFor([]byte("never read"), &calls),
		})

		require.ErrorIs(t, err, ErrBlockTooLargeToFrame, "length %d must be refused", length)
		require.Equal(t, 0, calls, "an unframeable block must not be fetched at all")
	}

	// And the largest length that IS servable must not be refused. It is not
	// sent here — nobody would stream 4 GB in a unit test — so the assertion is
	// that the refusal did not fire: the failure comes from the opener, having
	// been called, not from the boundary check.
	calls = 0
	err := ca.SendBlock(ctx, BlockSendRequest{
		Length: MaxBlockFrameBytes,
		Open: func(context.Context) (io.ReadCloser, error) {
			calls++

			return nil, errors.New(errors.ERR_ERROR, "opener refused, boundary was passed")
		},
	})

	require.NotErrorIs(t, err, ErrBlockTooLargeToFrame, "the largest servable length must not be refused as unframeable")
	require.Equal(t, 1, calls, "the boundary check must have let it through to the opener")

	// Nothing was written, so the connection is still usable.
	require.NoError(t, ca.Send(wire.NewMsgPing(7)))

	msgs := readFramesWithGoWire(t, b, 1)
	ping, ok := msgs[0].(*wire.MsgPing)
	require.True(t, ok)
	require.Equal(t, uint64(7), ping.Nonce)
}

// TestSendBlockAbortsWhenFirstPassLengthDiffers is the between-pass guard. A
// payload whose real byte count does not match the declared length can only
// produce a frame whose checksum or length is a lie, so the send must abort
// BEFORE the header goes out, and the connection must survive it.
func TestSendBlockAbortsWhenFirstPassLengthDiffers(t *testing.T) {
	a, b := net.Pipe()
	ca := New(a, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)

	payload := blockPayload(t, testMsgBlock(t, 2))

	var calls int

	err := ca.SendBlock(ctx, BlockSendRequest{
		Length: uint64(len(payload)) + 1,
		Open:   openerFor(payload, &calls),
	})

	require.ErrorIs(t, err, ErrBlockLengthMismatch)
	require.Equal(t, 1, calls, "the second pass must not run after the first disagreed")

	require.NoError(t, ca.Send(wire.NewMsgPing(9)))

	msgs := readFramesWithGoWire(t, b, 1)
	ping, ok := msgs[0].(*wire.MsgPing)
	require.True(t, ok, "the connection must survive an aborted send, got %T", msgs[0])
	require.Equal(t, uint64(9), ping.Nonce)
}

// TestSendBlockReportsSecondPassOpenFailure covers the block that is reorged
// out, or whose body becomes unavailable, between the two passes. Nothing has
// been written, so the caller gets the underlying error back — it must NOT be
// flattened into a send failure, because the caller decides notfound versus a
// real fault from its type — and the connection stays intact.
func TestSendBlockReportsSecondPassOpenFailure(t *testing.T) {
	a, b := net.Pipe()
	ca := New(a, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)

	payload := blockPayload(t, testMsgBlock(t, 2))
	gone := errors.NewBlockNotFoundError("block went away between passes")

	var calls int

	err := ca.SendBlock(ctx, BlockSendRequest{
		Length: uint64(len(payload)),
		Open: func(context.Context) (io.ReadCloser, error) {
			calls++

			if calls == 1 {
				return io.NopCloser(bytes.NewReader(payload)), nil
			}

			return nil, gone
		},
	})

	require.ErrorIs(t, err, errors.ErrBlockNotFound)
	require.Equal(t, 2, calls)

	require.NoError(t, ca.Send(wire.NewMsgPing(13)))

	msgs := readFramesWithGoWire(t, b, 1)
	ping, ok := msgs[0].(*wire.MsgPing)
	require.True(t, ok, "the connection must survive a failed second open, got %T", msgs[0])
	require.Equal(t, uint64(13), ping.Nonce)
}

// TestSendBlockShortSecondPassFailsTheConnection is the one unrecoverable
// case: the header is already on the wire, so a body that ends early leaves
// the peer waiting for bytes that will never come and every later frame
// misaligned. The only honest answer is to drop the connection.
func TestSendBlockShortSecondPassFailsTheConnection(t *testing.T) {
	a, b := net.Pipe()
	ca := New(a, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)

	payload := blockPayload(t, testMsgBlock(t, 2))

	var calls int

	errCh := make(chan error, 1)

	go func() {
		errCh <- ca.SendBlock(ctx, BlockSendRequest{
			Length: uint64(len(payload)),
			Open: func(context.Context) (io.ReadCloser, error) {
				calls++

				if calls == 1 {
					return io.NopCloser(bytes.NewReader(payload)), nil
				}

				return io.NopCloser(bytes.NewReader(payload[:len(payload)-5])), nil
			},
		})
	}()

	// Drain whatever reached the socket so the writer is never blocked, and
	// stop when the far side sees the connection go away.
	go func() { _, _ = io.Copy(io.Discard, b) }()

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the short send to fail")
	}

	select {
	case <-ca.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a misaligned socket must fail the connection")
	}

	require.Error(t, ca.Err())
}

// TestSendBlockIsNotChargedAgainstTheSendBudget pins the accounting decision:
// the byte budget bounds what the send QUEUE holds in memory, and a streamed
// block holds none of it. A block far larger than the whole budget must still
// go out, and must leave the budget as it found it, so the queued lane keeps
// working afterwards.
func TestSendBlockIsNotChargedAgainstTheSendBudget(t *testing.T) {
	a, b := net.Pipe()
	cfg := testConfig()
	cfg.SendBudgetBytes = 64
	ca := New(a, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)

	blk := testMsgBlock(t, 8)
	payload := blockPayload(t, blk)
	require.Greater(t, len(payload), cfg.SendBudgetBytes, "the test block must exceed the whole budget")

	var calls int

	errCh := make(chan error, 1)

	go func() {
		errCh <- ca.SendBlock(ctx, BlockSendRequest{
			Length: uint64(len(payload)),
			Open:   openerFor(payload, &calls),
		})
	}()

	msgs := readFramesWithGoWire(t, b, 1)
	require.NoError(t, <-errCh)

	got, ok := msgs[0].(*wire.MsgBlock)
	require.True(t, ok, "expected a block message, got %T", msgs[0])
	require.Equal(t, blk.BlockHash(), got.BlockHash())

	// Read directly: the byte budget is transport-internal, and this task
	// adds no public accessor for it (the send-queue accessor the getheaders
	// flood limit needs is a separate booked residual).
	require.Equal(t, int64(0), ca.pending.Load(), "a streamed block must leave the byte budget untouched")

	// Header plus payload, exactly what a peer read off the socket.
	require.Equal(t, uint64(wire.MessageHeaderSize)+uint64(len(payload)), ca.BytesSent())
}

// TestSendChargesEncodedSizeForInventoryMessages fixes the byte-budget
// overcharge that made the droppable lane drop messages it had room for.
//
// go-wire's MaxPayloadLength is a worst case, and for the inventory-carrying
// messages it is a constant: MsgInv, MsgNotFound and MsgGetData all report
// MaxVarIntPayload + MaxInvPerMsg*maxInvVectPayload = 1,800,009 bytes
// (go-wire MaxPayloadLength, msg_inv.go:116-119, v1.2.10) no matter what they
// actually hold. A one-entry notfound
// of about 40 bytes was therefore charged 1.8 MB, so five of them exhausted the
// whole 10 MB production budget and the sixth was refused — and a dropped
// notfound makes a peer wait out its own request timeout for something we
// already knew we did not have.
//
// Nothing reads the far side, so the writer blocks on the first message and
// every later one stays queued, which is what puts them all in the budget at
// once.
func TestSendChargesEncodedSizeForInventoryMessages(t *testing.T) {
	a, _ := net.Pipe()

	cfg := testConfig()
	cfg.SendBudgetBytes = 64 * 1024
	ca := New(a, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)

	nf := wire.NewMsgNotFound()
	require.NoError(t, nf.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &chainhash.Hash{0x01})))

	// The premise: the worst-case bound alone exceeds the entire budget, so
	// under the old accounting at most two of these could ever be in flight.
	require.Greater(t, nf.MaxPayloadLength(wire.ProtocolVersion), uint64(cfg.SendBudgetBytes),
		"the test needs a message whose worst-case bound dwarfs the budget")

	const queued = 16

	for i := 0; i < queued; i++ {
		require.NoError(t, ca.Send(nf), "notfound %d was refused with the budget nowhere near full", i)
	}

	// 1 varint byte + 36 bytes per entry, times what is still queued. The
	// message the writer already dequeued is no longer charged, so this is an
	// upper bound, and the point is that it is a few hundred bytes and not
	// megabytes.
	require.LessOrEqual(t, ca.pending.Load(), int64(queued*(1+invVectPayloadBytes)))
	require.Positive(t, ca.pending.Load())
}

// invMsgOf builds an inventory message of n entries, for the cost assertions.
func notFoundOf(t *testing.T, n int) *wire.MsgNotFound {
	t.Helper()

	msg := wire.NewMsgNotFound()

	for i := 0; i < n; i++ {
		require.NoError(t, msg.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &chainhash.Hash{byte(i), byte(i >> 8)}))) //nolint:gosec // small loop
	}

	return msg
}

// TestSendCostIsTheEncodedInventorySize pins the numbers themselves, so an
// upstream change to MaxPayloadLength cannot silently restore the overcharge
// this replaced. It calls sendCost directly: no goroutines, no timing.
//
// productionBudget is the real sendBudgetBytes (protocol/manager.go:37),
// hardcoded here because transport must not import protocol. If the two ever
// diverge the arithmetic below stops describing production, which is the only
// thing that makes the "five used to be the ceiling" claim meaningful.
func TestSendCostIsTheEncodedInventorySize(t *testing.T) {
	const productionBudget = 10 * 1024 * 1024

	pver := wire.ProtocolVersion

	one := notFoundOf(t, 1)

	// The upstream worst case, pinned: a constant that ignores the contents.
	require.EqualValues(t, 1800009, one.MaxPayloadLength(pver),
		"go-wire's bound changed; re-check whether sendCost is still needed and still correct")

	// What it actually costs: a one byte count varint plus one 36 byte vector.
	require.Equal(t, 1+invVectPayloadBytes, sendCost(one, pver, productionBudget))
	require.Equal(t, 37, sendCost(one, pver, productionBudget))

	// The defect in one line: five of these used to exhaust the whole budget.
	require.Equal(t, 5, productionBudget/int(one.MaxPayloadLength(pver)),
		"the old ceiling was five messages")
	require.Greater(t, productionBudget/sendCost(one, pver, productionBudget), 100000,
		"the new ceiling must be a number no peer can reach with legal traffic")

	// The count varint grows at 253 entries (the `val < 0xfd` test in go-wire
	// VarIntSerializeSize, common.go:644, v1.2.10), so the formula is not just
	// 36*n.
	require.Equal(t, 1+252*invVectPayloadBytes, sendCost(notFoundOf(t, 252), pver, productionBudget))
	require.Equal(t, 3+253*invVectPayloadBytes, sendCost(notFoundOf(t, 253), pver, productionBudget))

	// All three inventory-carrying types, not just notfound.
	inv := wire.NewMsgInv()
	require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &chainhash.Hash{0x01})))
	require.Equal(t, 37, sendCost(inv, pver, productionBudget))

	gd := wire.NewMsgGetData()
	require.NoError(t, gd.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &chainhash.Hash{0x01})))
	require.Equal(t, 37, sendCost(gd, pver, productionBudget))

	// Everything else keeps go-wire's bound, which for the fixed-size messages
	// IS the encoded size.
	require.Equal(t, 8, sendCost(wire.NewMsgPing(1), pver, productionBudget))

	// The clamp survives on both paths, so a message bigger than the whole
	// budget is still admitted alone rather than refused for ever.
	//
	// MsgTx is the bound-path case here deliberately. This assertion used to
	// use MsgHeaders, which made it read as documentation of a 162 KB overcharge
	// rather than a clamp test — headers is now costed exactly, so the clamp
	// needs a type that genuinely still takes the bound path. MsgTx does
	// (32 MB), and legitimately: a transaction's real size is not derivable
	// from a slice length, and rawTxMsg — the only tx this service sends —
	// reports its exact payload from MaxPayloadLength anyway.
	require.Equal(t, 1000, sendCost(notFoundOf(t, 50), pver, 1000), "accurate path clamps")
	require.Equal(t, 100, sendCost(wire.NewMsgTx(1), pver, 100), "bound path clamps")
	require.Greater(t, wire.NewMsgTx(1).MaxPayloadLength(pver), uint64(100), "the bound-path case must actually exceed the budget")
}

// TestSendAdmitsFarMoreInventoryMessagesThanTheOldCeiling is the same fix seen
// through the lane, at the real budget: five one-entry notfounds used to fill
// it, and a serving pass emits one of these per pass.
func TestSendAdmitsFarMoreInventoryMessagesThanTheOldCeiling(t *testing.T) {
	const productionBudget = 10 * 1024 * 1024

	a, _ := net.Pipe() // nobody reads, so everything after the first stays queued

	cfg := testConfig()
	cfg.SendBudgetBytes = productionBudget
	ca := New(a, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)

	nf := notFoundOf(t, 1)

	oldCeiling := productionBudget / int(nf.MaxPayloadLength(wire.ProtocolVersion))
	require.Equal(t, 5, oldCeiling)

	// Bounded by the sendCh depth, not by the budget: the byte budget is one of
	// TWO bounds on this lane, and with the cost fixed the channel's 64 slots
	// are now the binding one for messages this small. Send BLOCKS on a full
	// channel rather than refusing, so a test must stay under it.
	const queued = 8 * 5

	require.Less(t, queued, 64, "must stay under the sendCh depth, which Send blocks on")

	for i := 0; i < queued; i++ {
		require.NoError(t, ca.Send(nf), "notfound %d refused, %d past the old ceiling of %d", i, i-oldCeiling, oldCeiling)
	}
}

// TestSendBlockSameLengthDifferentContentFailsTheConnection is the content
// verification, as distinct from the length check. A store that returns a body
// of the RIGHT length but the WRONG bytes between the two passes would put pass
// 1's checksum on the wire over pass 2's content, and SVNode ban-scores a
// checksum mismatch (net_processing.cpp:5005-5015). Length equality cannot
// detect it; hashing pass 2 as it copies can.
//
// The frame is already sent by the time this is known, so the only honest
// response is to drop the connection — one lost peer instead of a ban score
// from every peer it happens with.
func TestSendBlockSameLengthDifferentContentFailsTheConnection(t *testing.T) {
	a, b := net.Pipe()
	ca := New(a, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)

	first := blockPayload(t, testMsgBlock(t, 3))

	// Same length, one byte different: exactly what a length check cannot see.
	second := make([]byte, len(first))
	copy(second, first)
	second[len(second)-1] ^= 0xff

	require.Len(t, second, len(first))
	require.NotEqual(t, first, second)

	var calls int

	errCh := make(chan error, 1)

	go func() {
		errCh <- ca.SendBlock(ctx, BlockSendRequest{
			Length: uint64(len(first)),
			Open: func(context.Context) (io.ReadCloser, error) {
				calls++

				if calls == 1 {
					return io.NopCloser(bytes.NewReader(first)), nil
				}

				return io.NopCloser(bytes.NewReader(second)), nil
			},
		})
	}()

	// Drain so the writer is never blocked on the socket.
	go func() { _, _ = io.Copy(io.Discard, b) }()

	select {
	case err := <-errCh:
		require.Error(t, err, "a body that changed between passes must be reported")
		require.Contains(t, err.Error(), "changed between the hashing pass and the write pass")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the content mismatch to be reported")
	}

	select {
	case <-ca.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a frame whose checksum cannot match its payload must fail the connection")
	}

	require.Error(t, ca.Err())
}

// TestSendCostChargesHeadersByEntry closes the last of the three
// variable-length overcharges. MsgHeaders.MaxPayloadLength is
// MaxVarIntPayload + (MaxBlockHeaderPayload+1)*MaxBlockHeadersPerMsg = 162,009
// bytes whatever the message holds, so an EMPTY headers reply was charged
// 162 KB.
//
// The per-entry size is 81 and the test derives it from go-wire's own exported
// constant rather than the literal, because an off-by-one here is exactly the
// kind of error a hardcoded 81 would hide: BsvEncode writes writeBlockHeader
// (MaxBlockHeaderPayload = 16 + 2*32 = 80) then WriteVarInt(0), one byte.
func TestSendCostChargesHeadersByEntry(t *testing.T) {
	const productionBudget = 10 * 1024 * 1024

	pver := wire.ProtocolVersion

	require.Equal(t, 81, headerEntryPayloadBytes, "80 byte header plus the always-zero tx count varint")
	require.Equal(t, 80, wire.MaxBlockHeaderPayload)

	empty := wire.NewMsgHeaders()

	// The bound, pinned: a constant, and NOT the 64 MB maxMessagePayload.
	require.EqualValues(t, 162009, empty.MaxPayloadLength(pver),
		"go-wire's headers bound changed; re-check this cost function")

	// An empty headers message costs its one count varint.
	require.Equal(t, 1, sendCost(empty, pver, productionBudget))

	hdr := wire.NewBlockHeader(1, &chainhash.Hash{0x01}, &chainhash.Hash{0x02}, 0x1d00ffff, 0)

	one := wire.NewMsgHeaders()
	require.NoError(t, one.AddBlockHeader(hdr))
	require.Equal(t, 1+headerEntryPayloadBytes, sendCost(one, pver, productionBudget))

	// The count varint boundary at 253, same as the inventory formula.
	many := wire.NewMsgHeaders()
	for i := 0; i < 253; i++ {
		require.NoError(t, many.AddBlockHeader(hdr))
	}

	require.Equal(t, 3+253*headerEntryPayloadBytes, sendCost(many, pver, productionBudget))

	// A full 2000-header reply is the real worst case a serving node sends, and
	// it must be a fraction of the budget rather than all of it.
	full := wire.NewMsgHeaders()
	for i := 0; i < wire.MaxBlockHeadersPerMsg; i++ {
		require.NoError(t, full.AddBlockHeader(hdr))
	}

	cost := sendCost(full, pver, productionBudget)
	require.Equal(t, 3+wire.MaxBlockHeadersPerMsg*headerEntryPayloadBytes, cost)
	require.Less(t, cost, productionBudget/50, "a full headers reply must not dominate the budget")
}
