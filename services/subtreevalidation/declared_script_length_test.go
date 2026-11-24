package subtreevalidation

import (
	"bytes"
	"io"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// declaredScriptLengthBound is far above what decoding a short stream allocates
// and far below the gigabyte the declaration below asks for.
const declaredScriptLengthBound = 64 << 20

// declaredHugeScriptTx is a 46-byte transaction whose only input declares an
// unlocking script just under go-bt's 1 GiB cap and then ends.
func declaredHugeScriptTx() []byte {
	tx := []byte{0x01, 0x00, 0x00, 0x00, 0x01}    // version, one input
	tx = append(tx, make([]byte, 36)...)          // previous outpoint
	tx = append(tx, 0xfe, 0xff, 0xff, 0xff, 0x3f) // script length (1<<30)-1, then nothing

	return tx
}

func allocatedDuring(fn func()) uint64 {
	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	fn()

	runtime.ReadMemStats(&after)

	return after.TotalAlloc - before.TotalAlloc
}

// TestSubtreeDataStream_DeclaredScriptLengthIsNotAllocated pins that a peer's
// subtree data cannot buy a large allocation with a script length it never
// sends. Block validation decodes that data into a pooled arena, up to
// check_block_subtrees_concurrency subtrees at a time; go-bt's arena decode
// used to allocate the declared length before reading, so 46 bytes grew the
// arena to 1 GiB. The failure must also stay the peer's, not this node's.
func TestSubtreeDataStream_DeclaredScriptLengthIsNotAllocated(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))
	require.NoError(t, subtree.AddNode(chainhash.Hash{0x02}, 1, 2))

	stream := func() *bytes.Buffer {
		var b bytes.Buffer
		b.Write(tx1.Bytes())
		b.Write(declaredHugeScriptTx())

		return &b
	}

	for _, tc := range []struct {
		name  string
		arena func() *bt.Arena
	}{
		{name: "no arena", arena: func() *bt.Arena { return nil }},
		{name: "pooled arena", arena: getSubtreeArena},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arena := tc.arena()
			if arena != nil {
				defer putSubtreeArena(arena)
			}

			r := stream()

			var (
				txs   []*bt.Tx
				count int
			)

			allocated := allocatedDuring(func() {
				count, _ = server.readTransactionsFromSubtreeDataStream(subtree, r, &txs, arena)
			})

			// go-bt reports a script that ends before its first byte as io.EOF, which
			// this reader takes for the end of the stream. The stream then comes up
			// short of the subtree, and the callers reject it on the count.
			require.Less(t, count, subtree.Length())
			require.Less(t, allocated, uint64(declaredScriptLengthBound),
				"a declared script length must not be allocated before its bytes arrive (got %d bytes)", allocated)
		})
	}

	// Peer bytes reach the parser through processSubtreeDataStream.
	t.Run("pooled arena via processSubtreeDataStream", func(t *testing.T) {
		arena := getSubtreeArena()
		defer putSubtreeArena(arena)

		body := io.NopCloser(stream())

		var txs []*bt.Tx

		allocated := allocatedDuring(func() {
			err = server.processSubtreeDataStream(t.Context(), subtree, body, &txs, 0, arena)
		})

		require.Error(t, err)
		require.False(t, errors.IsLocalError(err), "a truncated script is the peer's fault, not this node's")
		require.Less(t, allocated, uint64(declaredScriptLengthBound),
			"a declared script length must not be allocated before its bytes arrive (got %d bytes)", allocated)
	})
}
