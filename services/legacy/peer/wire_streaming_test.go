package peer

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// makeTestBlock builds a wire.MsgBlock with synthetic transactions whose
// scripts have a fixed size, useful for confirming the streaming decoder
// reconstructs the same structure that the buffered path produces.
func makeTestBlock(t *testing.T, numTxs, scriptLen int) *wire.MsgBlock {
	t.Helper()

	prev := chainhash.Hash{}
	merkle := chainhash.Hash{}
	header := wire.NewBlockHeader(1, &prev, &merkle, 0x1d00ffff, 0)

	block := wire.NewMsgBlock(header)
	for i := 0; i < numTxs; i++ {
		tx := wire.NewMsgTx(1)

		script := make([]byte, scriptLen)
		_, err := rand.Read(script)
		require.NoError(t, err)

		tx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: prev, Index: uint32(i)},
			SignatureScript:  script,
			Sequence:         0xffffffff,
		})
		tx.AddTxOut(&wire.TxOut{Value: 1, PkScript: script})

		block.AddTransaction(tx)
	}

	return block
}

func TestStreamingBlockHandler_RoundTrip(t *testing.T) {
	wire.SetLimits(4000000000)

	want := makeTestBlock(t, 8, 256)

	var wire1 bytes.Buffer
	_, err := wire.WriteMessageN(&wire1, want, wire.ProtocolVersion, wire.MainNet)
	require.NoError(t, err)

	// Skip the 24-byte header so the handler receives the payload reader
	// it would get inside ReadMessageWithEncodingN.
	payloadLen := uint64(wire1.Len() - 24)
	_ = wire1.Next(24)

	n, msg, raw, err := streamingBlockHandler(&wire1, payloadLen, 24)
	require.NoError(t, err)
	require.Equal(t, int(payloadLen)+24, n)
	require.Nil(t, raw, "streaming handler must not retain the payload bytes")

	got, ok := msg.(*wire.MsgBlock)
	require.True(t, ok, "expected *wire.MsgBlock, got %T", msg)
	require.Equal(t, want.BlockHash(), got.BlockHash())
	require.Equal(t, len(want.Transactions), len(got.Transactions))

	for i := range want.Transactions {
		require.Equal(t,
			want.Transactions[i].TxHash(),
			got.Transactions[i].TxHash(),
			"tx %d hash mismatch", i)
	}
}

// TestStreamingBlockHandler_DrainsOnError verifies that a corrupted payload
// does not leave unread bytes on the underlying reader — otherwise the next
// ReadMessage call would parse those bytes as a fresh wire header and
// desync the stream.
func TestStreamingBlockHandler_DrainsOnError(t *testing.T) {
	const declared = 1024
	const truncated = 32

	// declared length 1024 but only 32 bytes of garbage followed by an
	// identifiable tail so we can detect whether the handler over-reads.
	payload := bytes.NewBuffer(make([]byte, truncated))
	tail := []byte("MARKER-AFTER-PAYLOAD")
	src := io.MultiReader(payload, bytes.NewReader(make([]byte, declared-truncated)), bytes.NewReader(tail))

	_, _, _, _ = streamingBlockHandler(src, declared, 0)

	got := make([]byte, len(tail))
	_, err := io.ReadFull(src, got)
	require.NoError(t, err)
	require.Equal(t, tail, got, "handler must drain exactly the declared payload, leaving the next message intact")
}

// TestStreamingBlockHandler_ShortStreamReturnsError verifies that when the
// underlying reader EOFs before the declared payload length is fully
// consumed, the handler returns an error. Without this check, a successful
// Bsvdecode followed by an undersized stream would silently succeed (io.Copy
// reports EOF as nil) and the next ReadMessage would desync.
func TestStreamingBlockHandler_ShortStreamReturnsError(t *testing.T) {
	const declared = 1024
	// 100 zero bytes is enough for Bsvdecode to parse an empty block (80
	// byte header + 1 byte tx-count varint = 81 bytes), with 19 bytes left
	// in the stream that the drain will consume before hitting EOF — well
	// short of the 1024 byte declared length.
	src := bytes.NewReader(make([]byte, 100))

	_, _, _, err := streamingBlockHandler(src, declared, 0)
	require.Error(t, err, "handler must surface short-stream as an error")
	require.Contains(t, err.Error(), "stream ended with", "error must identify the short-stream condition")
}

// TestRegisterStreamingBlockHandler_DispatchesViaWire verifies that after
// registration, wire.ReadMessageWithEncodingN takes the streaming code
// path for the "block" command: the returned payload slice must be nil
// (the streaming handler does not retain bytes) and the decoded block
// must match the original. This proves the handler is installed for the
// correct command, not just that calls do not panic.
func TestRegisterStreamingBlockHandler_DispatchesViaWire(t *testing.T) {
	wire.SetLimits(4000000000)
	RegisterStreamingBlockHandler()

	want := makeTestBlock(t, 4, 64)

	var buf bytes.Buffer
	_, err := wire.WriteMessageN(&buf, want, wire.ProtocolVersion, wire.MainNet)
	require.NoError(t, err)

	_, msg, raw, err := wire.ReadMessageWithEncodingN(&buf, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.NoError(t, err)
	require.Nil(t, raw, "streaming handler must not return the payload slice")

	got, ok := msg.(*wire.MsgBlock)
	require.True(t, ok, "expected *wire.MsgBlock, got %T", msg)
	require.Equal(t, want.BlockHash(), got.BlockHash())
	require.Equal(t, len(want.Transactions), len(got.Transactions))

	// Re-registering must not displace the installed handler — verify the
	// streaming path still wins after a second Register call.
	RegisterStreamingBlockHandler()
	RegisterStreamingBlockHandler()

	buf.Reset()
	_, err = wire.WriteMessageN(&buf, want, wire.ProtocolVersion, wire.MainNet)
	require.NoError(t, err)

	_, _, raw, err = wire.ReadMessageWithEncodingN(&buf, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.NoError(t, err)
	require.Nil(t, raw, "streaming handler must remain installed after repeated Register calls")
}
