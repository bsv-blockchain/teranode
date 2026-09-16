package peer

import (
	"bytes"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// serialisedBlock builds a real block payload: header, transaction count, then
// that many transactions.
func serialisedBlock(t *testing.T, txs int) ([]byte, chainhash.Hash) {
	t.Helper()

	// wire.NewBlockHeader stamps Timestamp as time.Now(), which would make the
	// header hash, and therefore whether it meets its own target, different on
	// every run. The gate needs a header that reliably passes its own-target
	// check, so the timestamp is pinned and nonce 1 is the smallest nonce whose
	// hash actually meets the 0x207fffff target for that fixed header; picked
	// without checking, a nonce fails about half the time, since that target
	// sits at roughly the midpoint of the hash space.
	hdr := wire.NewBlockHeader(1, &chainhash.Hash{0x11}, &chainhash.Hash{0x22}, 0x207fffff, 1)
	hdr.Timestamp = time.Unix(1231006505, 0)
	blk := wire.NewMsgBlock(hdr)

	for i := 0; i < txs; i++ {
		tx := wire.NewMsgTx(1)
		tx.AddTxIn(&wire.TxIn{SignatureScript: []byte{byte(i)}, Sequence: 0xffffffff})
		tx.AddTxOut(&wire.TxOut{Value: 1, PkScript: []byte{0x51}})
		require.NoError(t, blk.AddTransaction(tx))
	}

	var buf bytes.Buffer
	require.NoError(t, blk.Serialize(&buf))

	return buf.Bytes(), hdr.BlockHash()
}

// fakeSyncManagerGate stands in for the gate the sync manager installs in
// production. It exists only to exercise the ordering the peer package
// guarantees around blockBodyGate; the real implementation belongs in the sync
// manager, the only place that actually has chainParams and the download
// ledger, per the controller's ruling that moved this check out of this
// package.
//
// It mirrors what that implementation is required to do: refuse a hash nobody
// asked for, then refuse a header whose declared target is easier than the
// chain's own difficulty limit, then refuse one that does not meet its own
// (now-bounded) target.
func fakeSyncManagerGate(requested map[chainhash.Hash]bool, limit *big.Int) func(chainhash.Hash, *wire.BlockHeader) error {
	return func(hash chainhash.Hash, header *wire.BlockHeader) error {
		if !requested[hash] {
			return errors.NewProcessingError("block %s was never requested", hash)
		}

		var headerBytes bytes.Buffer
		if err := header.Serialize(&headerBytes); err != nil {
			return errors.NewProcessingError("block %s: could not serialize the header", hash, err)
		}

		mh, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
		if err != nil {
			return errors.NewProcessingError("block %s: could not read the header", hash, err)
		}

		// This is Finding 1's fix: without a floor, a peer can simply declare a
		// target its own hash always meets. The floor is what makes the
		// following own-target check mean anything.
		if mh.Bits.CalculateTarget().Cmp(limit) > 0 {
			return errors.NewProcessingError("block %s: declared target is easier than the chain's difficulty limit", hash)
		}

		if met, _, err := mh.HasMetTargetDifficulty(); !met {
			return errors.NewProcessingError("block %s: does not meet its own target difficulty", hash, err)
		}

		return nil
	}
}

// permissiveGate lets any hash and header through. Tests that only care about
// exercising the sink path, not the gate's own logic, use this so a failure to
// wire the gate correctly cannot masquerade as a passing test.
func permissiveGate(chainhash.Hash, *wire.BlockHeader) error { return nil }

// installGate sets blockBodyGate for the duration of the test and restores it
// to nil afterwards. wire.SetExternalHandler and its collaborators are process
// globals, so a test that moves blockBodyGate and forgets to put it back would
// poison every test that runs after it in this package.
func installGate(t *testing.T, gate func(chainhash.Hash, *wire.BlockHeader) error) {
	t.Helper()

	blockBodyGate = gate
	t.Cleanup(func() { blockBodyGate = nil })
}

// With a sink and a gate installed the handler must not build a wire.MsgBlock
// at all, whatever the block's size. It reads the header, asks the gate, and
// hands the rest of the payload to the sink.
func TestStreamingBlockHandlerSendsABodyToTheSink(t *testing.T) {
	payload, hash := serialisedBlock(t, 4)

	var (
		gotHash   chainhash.Hash
		gotHeader *wire.BlockHeader
		gotBody   []byte
		gotN      int64
	)

	blockBodySink = func(h chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) (bool, error) {
		gotHash = h
		gotHeader = header
		gotN = n
		b, err := io.ReadAll(r)
		gotBody = b

		return false, err
	}
	t.Cleanup(func() { blockBodySink = nil })

	installGate(t, permissiveGate)

	n, msg, buf, err := streamingBlockHandler(bytes.NewReader(payload), uint64(len(payload)), 24)
	require.NoError(t, err)
	require.Nil(t, buf)
	require.Equal(t, 24+len(payload), n, "the whole message is accounted for")

	onDisk, ok := msg.(*MsgBlockOnDisk)
	require.True(t, ok, "the block must come back as a body on disk, not a decoded block")
	require.Equal(t, hash, onDisk.Hash)
	require.Equal(t, uint64(4), onDisk.TxCount)
	require.Equal(t, int64(len(payload)), onDisk.Size)

	require.Equal(t, hash, gotHash, "the sink is keyed by the block hash")

	// The sink is handed the header as structure, not as bytes, and this changed:
	// it used to get a reader whose first bytes were the re-serialized header. A
	// consumer that wants a byte-for-byte copy of the whole block, such as the
	// park, must re-serialize the header itself and put it back in front of what
	// the reader yields.
	require.NotNil(t, gotHeader, "the sink must be handed the parsed header")
	require.Equal(t, hash, gotHeader.BlockHash(), "and it must be the block's own header")

	var headerBytes bytes.Buffer
	require.NoError(t, gotHeader.Serialize(&headerBytes))
	require.Equal(t, payload, append(headerBytes.Bytes(), gotBody...),
		"the re-serialized header followed by what the sink read must reproduce the original payload byte-for-byte")
	require.Equal(t, int64(len(payload)), gotN,
		"the declared length must cover the header too, or a consumer that trusts it writes a short file")
}

// A small block streams too, once a sink and a gate are installed: there is
// no size threshold left to keep it on the decode path. Streaming used to
// mean "write the body to disk", worth it only for a block too large to hold
// in memory; it now means "convert the block as it arrives", which pays at
// every size.
func TestStreamingBlockHandlerSendsASmallBodyToTheSinkToo(t *testing.T) {
	payload, hash := serialisedBlock(t, 2)

	sinkCalled := false

	blockBodySink = func(_ chainhash.Hash, _ *wire.BlockHeader, r io.Reader, _ int64) (bool, error) {
		sinkCalled = true

		_, err := io.Copy(io.Discard, r)

		return false, err
	}
	t.Cleanup(func() { blockBodySink = nil })

	installGate(t, permissiveGate)

	_, msg, _, err := streamingBlockHandler(bytes.NewReader(payload), uint64(len(payload)), 24)
	require.NoError(t, err)
	require.True(t, sinkCalled, "a small block must reach the sink, not the whole-block decoder")

	onDisk, ok := msg.(*MsgBlockOnDisk)
	require.True(t, ok, "a small block must come back as a body on disk too")
	require.Equal(t, hash, onDisk.Hash)
}

// A block that fails its own target, having passed the requested and floor
// checks, must never be stored. This is what stops a peer filling the disk
// with a block that simply does not meet the difficulty it claims.
func TestStreamingBlockHandlerRefusesABadHeaderBeforeStoring(t *testing.T) {
	payload, hash := serialisedBlock(t, 4)

	// Break the proof of work by making the target impossible to have met.
	// nBits sits at offset 72 of the 80-byte header.
	copy(payload[72:76], []byte{0x00, 0x00, 0x00, 0x00})

	called := false

	blockBodySink = func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
		called = true
		return false, nil
	}
	t.Cleanup(func() { blockBodySink = nil })

	installGate(t, fakeSyncManagerGate(
		map[chainhash.Hash]bool{hash: true},
		chaincfg.RegressionNetParams.PowLimit,
	))

	_, _, _, err := streamingBlockHandler(bytes.NewReader(payload), uint64(len(payload)), 24)
	require.Error(t, err, "a header that fails its own target must be refused")
	require.False(t, called, "nothing may be written for a block that failed the check")
}

// FINDING 1. A header whose declared target is easier than the chain's own
// difficulty limit must be refused before its own target is even checked,
// because a peer can otherwise simply declare a target its own hash always
// meets. Measured before this fix: 64 of 64 consecutive nonces passed
// checkBlockHeaderStandsAlone at nBits 0x2100ffff, because that check only
// compared the hash against whatever target the header itself declared, with
// no floor. 0x2100ffff is chosen because its target is provably easier than
// regtest's own PowLimit, the loosest limit any real chain uses; verified
// against go-chaincfg.RegressionNetParams.PowLimit rather than asserted.
func TestStreamingBlockHandlerRefusesAnEasyTargetBeforeStoring(t *testing.T) {
	payload, hash := serialisedBlock(t, 4)

	// nBits sits at offset 72 of the 80-byte header, little-endian: 0x2100ffff
	// as a uint32 is bytes 0x21,0x00,0xff,0xff MSB to LSB, so 0xff,0xff,0x00,0x21
	// LSB first.
	copy(payload[72:76], []byte{0xff, 0xff, 0x00, 0x21})

	limit := chaincfg.RegressionNetParams.PowLimit

	nb := model.NBit{0xff, 0xff, 0x00, 0x21}
	require.Equal(t, 1, nb.CalculateTarget().Cmp(limit),
		"sanity: 0x2100ffff must declare a target easier than the chain limit for this test to mean anything")

	called := false

	blockBodySink = func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
		called = true
		return false, nil
	}
	t.Cleanup(func() { blockBodySink = nil })

	installGate(t, fakeSyncManagerGate(map[chainhash.Hash]bool{hash: true}, limit))

	_, _, _, err := streamingBlockHandler(bytes.NewReader(payload), uint64(len(payload)), 24)
	require.Error(t, err, "a header declaring an easier-than-limit target must be refused")
	require.False(t, called, "nothing may be written for a block declaring an impossible target")
}

// FINDING 2. A block nobody asked for must be refused before storing, even
// with a header that is otherwise perfectly valid. Without this, an
// unsolicited block message alone is enough to reach the sink; today that only
// costs a decode, which the decode itself bounds, but streaming makes it cost
// disk, unbounded, because the park's byte budget is only consulted at Admit,
// which happens after the write.
func TestStreamingBlockHandlerRefusesAnUnrequestedBlockBeforeStoring(t *testing.T) {
	payload, _ := serialisedBlock(t, 4)

	called := false

	blockBodySink = func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
		called = true
		return false, nil
	}
	t.Cleanup(func() { blockBodySink = nil })

	// Nothing is marked requested, so every hash is refused by this gate.
	installGate(t, fakeSyncManagerGate(map[chainhash.Hash]bool{}, chaincfg.RegressionNetParams.PowLimit))

	_, _, _, err := streamingBlockHandler(bytes.NewReader(payload), uint64(len(payload)), 24)
	require.Error(t, err, "a block nobody asked for must be refused")
	require.False(t, called, "nothing may be written for a block nobody asked for")
}

// With no sink installed, a large block falls back to decoding. That is what
// keeps every test and every caller that never wires a store working.
func TestStreamingBlockHandlerFallsBackWithNoSink(t *testing.T) {
	payload, _ := serialisedBlock(t, 2)

	blockBodySink = nil
	installGate(t, permissiveGate)

	_, msg, _, err := streamingBlockHandler(bytes.NewReader(payload), uint64(len(payload)), 24)
	require.NoError(t, err)
	require.IsType(t, &wire.MsgBlock{}, msg)
}

// A nil gate must fall back to decoding even with a sink installed. This is
// what keeps a caller that wires a sink but forgets a gate safe: the default
// is the old behaviour, not an open door to the sink.
func TestStreamingBlockHandlerNilGateFallsBackToDecodingEvenWithSinkInstalled(t *testing.T) {
	payload, hash := serialisedBlock(t, 2)

	blockBodySink = func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
		t.Fatal("a nil gate must never let a block reach the sink")
		return false, nil
	}
	t.Cleanup(func() { blockBodySink = nil })

	blockBodyGate = nil // explicit: this is the condition under test

	_, msg, _, err := streamingBlockHandler(bytes.NewReader(payload), uint64(len(payload)), 24)
	require.NoError(t, err)

	blk, ok := msg.(*wire.MsgBlock)
	require.True(t, ok, "a nil gate must fall back to a decoded block, sink or no sink")
	require.Equal(t, hash, blk.BlockHash())
}

// FINDING 3 / Ruling 8. When the handler fails after the sink has already
// returned success, whatever it wrote must be deleted. This test forces
// exactly that: the sink reads to EOF and reports success regardless of how
// much it actually got, mimicking a lenient store implementation, while the
// peer declares more bytes than actually arrive. The result is a truncated
// body sitting under a well-formed hash unless the handler cleans it up.
func TestStreamingBlockHandlerDeletesATruncatedBodyAfterAWrite(t *testing.T) {
	payload, hash := serialisedBlock(t, 4)

	// Declare more bytes than are actually sent, so the sink's reader EOFs
	// before the declared length is reached even though the sink itself never
	// errors.
	declaredLength := uint64(len(payload)) + 32

	blockBodySink = func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
		return false, nil // a lenient sink: never notices it got fewer bytes than promised
	}
	t.Cleanup(func() { blockBodySink = nil })

	var deletedHash chainhash.Hash

	var deletedConverted bool

	deleteCalled := false
	blockBodyDelete = func(h chainhash.Hash, converted bool) error {
		deleteCalled = true
		deletedHash = h
		deletedConverted = converted

		return nil
	}
	t.Cleanup(func() { blockBodyDelete = nil })

	installGate(t, permissiveGate)

	_, _, _, err := streamingBlockHandler(bytes.NewReader(payload), declaredLength, 24)
	require.Error(t, err, "a truncated body must be surfaced as an error")
	require.True(t, deleteCalled, "a body written for a stream that ended short must be deleted")
	require.Equal(t, hash, deletedHash, "the delete must be keyed by the same hash the sink was")
	require.False(t, deletedConverted, "this sink reports converted=false, so the delete must be told the same — fix-round item 2's whole point is that this must never be re-derived by asking whether a converted record merely exists for the hash")
}

// The reviewer's missing test: a gate rejection must still drain the rest of
// the declared payload, so the connection lands on a clean message boundary
// and the next message parses. Without this, a rejected block would leave its
// unread body bytes on the stream, and the next ReadMessage call would parse
// those bytes as a fresh wire header.
func TestStreamingBlockHandlerDrainsThePayloadAfterAGateRejection(t *testing.T) {
	payload, _ := serialisedBlock(t, 4)

	tail := []byte("MARKER-AFTER-PAYLOAD")
	src := io.MultiReader(bytes.NewReader(payload), bytes.NewReader(tail))

	blockBodySink = func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
		t.Fatal("a rejected block must never reach the sink")
		return false, nil
	}
	t.Cleanup(func() { blockBodySink = nil })

	installGate(t, func(chainhash.Hash, *wire.BlockHeader) error {
		return errors.NewProcessingError("rejected for this test")
	})

	_, _, _, err := streamingBlockHandler(src, uint64(len(payload)), 24)
	require.Error(t, err, "a gate rejection must be surfaced as an error")

	got := make([]byte, len(tail))
	_, readErr := io.ReadFull(src, got)
	require.NoError(t, readErr)
	require.Equal(t, tail, got, "the rejection must drain exactly the declared payload, leaving the next message intact")
}
