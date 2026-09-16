package netsync

import (
	"bytes"
	"context"
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// TestStreamedBodyRoundTrips is the test the whole streaming path rests on.
//
// A streamed body and a parked one must be the same file. The park reads a body
// back with one deserializer, and nothing downstream knows or cares which path
// put the bytes there, so if the two writers disagree by a single byte then
// every streamed block is unreadable and the failure surfaces as blocks that
// were downloaded, stored, and then silently given up on.
//
// This is not hypothetical. The sink's original contract handed over only the
// bytes after the header, which would have produced exactly that: a file the
// park could never deserialize, under a hash that looks perfectly legitimate.
func TestStreamedBodyRoundTrips(t *testing.T) {
	h := newParkWiringHarness(t, true)
	park := h.sm.blockPark

	block := h.blocks[0].MsgBlock()
	hash := block.BlockHash()

	var whole bytes.Buffer
	require.NoError(t, block.Serialize(&whole))

	t.Run("what the sink writes is what the park reads", func(t *testing.T) {
		require.NoError(t, park.WriteStreamedBody(context.Background(), hash, bytes.NewReader(whole.Bytes()), int64(whole.Len())))

		got, err := park.Read(context.Background(), hash)
		require.NoError(t, err, "a streamed body must come back through the park's own reader, unchanged")

		var back bytes.Buffer
		require.NoError(t, got.Serialize(&back))
		require.Equal(t, whole.Bytes(), back.Bytes(),
			"round-tripping must be byte-exact, or the merkle root rebuilt downstream is rebuilt from different bytes")
	})

	t.Run("the header has to be there", func(t *testing.T) {
		// The failure mode the old sink contract would have produced, asserted so
		// nobody restores it: a body with its header missing is not a block.
		second := h.blocks[1].MsgBlock()
		secondHash := second.BlockHash()

		var full bytes.Buffer
		require.NoError(t, second.Serialize(&full))

		headerless := full.Bytes()[wire.MaxBlockHeaderPayload:]

		require.NoError(t, park.WriteStreamedBody(context.Background(), secondHash,
			bytes.NewReader(headerless), int64(len(headerless))))

		_, err := park.Read(context.Background(), secondHash)
		require.Error(t, err,
			"a file without its header must fail loudly on read, not be mistaken for a block whose first transaction is the header bytes")
	})
}
