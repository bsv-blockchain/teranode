package subtreevalidation

import (
	"bytes"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// widenVarInt rewrites the 1-byte CompactSize at off as the 3-byte form. The
// txid is unchanged by this: it is computed over the canonical re-serialization.
func widenVarInt(t *testing.T, raw []byte, off int) []byte {
	t.Helper()
	require.Less(t, raw[off], byte(0xfd), "fixture must have a 1-byte CompactSize at the offset")

	mangled := make([]byte, 0, len(raw)+2)
	mangled = append(mangled, raw[:off]...)
	mangled = append(mangled, 0xfd, raw[off], 0x00)
	mangled = append(mangled, raw[off+1:]...)

	return mangled
}

// TestReadTransactionsFromSubtreeDataStream_NonMinimalVarInt pins issue 1421 on
// the subtree-data stream a peer serves for block validation. go-bt (v2.7.0+,
// go-bt PR 174) rejects a non-minimal CompactSize at parse; this test notices if
// a go-bt bump ever relaxes that, since Teranode carries no detector of its own.
// The error must not carry the TxInvalid code: non-minimality is a fault in how
// the peer delivered the bytes, and the same subtree from an honest peer is valid.
//
// Production subtree data carries extended transactions, so the extended form is
// covered too, including the previous locking-script length that only the
// extended form has.
func TestReadTransactionsFromSubtreeDataStream_NonMinimalVarInt(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	require.NoError(t, err)

	tx2, err := createTestTransaction("6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4")
	require.NoError(t, err)
	require.Len(t, tx2.Inputs, 1)

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))
	require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), 1, 2))

	// Standard form: version (4) then the input count.
	raw := tx2.Bytes()

	// Extended form: version (4), marker (6), input count (1), then the input:
	// previous txid (32), vout (4), unlocking script with its length, sequence
	// (4), previous satoshis (8), and the previous locking-script length.
	tx2.Inputs[0].PreviousTxSatoshis = 5000000000
	tx2.Inputs[0].PreviousTxScript = bscript.NewFromBytes(bytes.Repeat([]byte{0x51}, 25))
	ext := tx2.ExtendedBytes()
	unlockingLen := len(*tx2.Inputs[0].UnlockingScript)
	require.Less(t, unlockingLen, 0xfd)
	prevScriptLenOff := 4 + 6 + 1 + 32 + 4 + 1 + unlockingLen + 4 + 8
	require.Equal(t, byte(25), ext[prevScriptLenOff], "offset must land on the previous locking-script length")

	cases := []struct {
		name string
		tx   []byte
	}{
		{"standard input count", widenVarInt(t, raw, 4)},
		{"extended input count", widenVarInt(t, ext, 4+6)},
		{"extended previous locking-script length", widenVarInt(t, ext, prevScriptLenOff)},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, arena := range []*bt.Arena{nil, bt.NewArena(4096)} {
				var stream bytes.Buffer
				stream.Write(tx1.Bytes())
				stream.Write(c.tx)

				var txs []*bt.Tx

				_, err := server.readTransactionsFromSubtreeDataStream(subtree, &stream, &txs, arena)
				require.Error(t, err)
				require.ErrorIs(t, err, bt.ErrNonMinimalVarInt, "go-bt must be what rejects it")
				require.True(t, errors.Is(err, errors.ErrProcessing))
				require.False(t, errors.Is(err, errors.ErrTxInvalid), "a delivery fault must not condemn the transaction")
				require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a delivery fault must not condemn the block")
				require.False(t, errors.IsLocalError(err), "a delivery fault must not be charged to this node")
			}
		})

		// Peer bytes reach the parser through processSubtreeDataStream, which tees
		// the stream into the subtree store. The store sees the parser abort the
		// pipe and fails too; that echo must not turn the peer's fault into a
		// local storage error, which catch-up would neither charge to the peer
		// nor retry on another.
		t.Run(c.name+" via processSubtreeDataStream", func(t *testing.T) {
			var stream bytes.Buffer
			stream.Write(tx1.Bytes())
			stream.Write(c.tx)

			var txs []*bt.Tx

			err := server.processSubtreeDataStream(t.Context(), subtree, io.NopCloser(&stream), &txs, 0, nil)
			require.Error(t, err)
			require.ErrorIs(t, err, bt.ErrNonMinimalVarInt, "go-bt must be what rejects it")
			require.False(t, errors.Is(err, errors.ErrTxInvalid), "a delivery fault must not condemn the transaction")
			require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a delivery fault must not condemn the block")
			require.False(t, errors.IsLocalError(err), "a delivery fault must not be charged to this node")
		})
	}

	// The unmangled extended stream parses, so each reject above is the prefix
	// and not a broken fixture.
	t.Run("canonical extended stream is accepted", func(t *testing.T) {
		var stream bytes.Buffer
		stream.Write(tx1.Bytes())
		stream.Write(ext)

		var txs []*bt.Tx

		n, err := server.readTransactionsFromSubtreeDataStream(subtree, &stream, &txs, nil)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Len(t, txs, 2)
		require.True(t, txs[1].IsExtended())
	})
}
