package subtreevalidation

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// TestReadTransactionsFromSubtreeDataStream_MemoryBoundWithArena builds a
// synthetic 1000-tx stream where each tx carries a 100 KB OP_RETURN output
// (total ≈ 100 MiB on the wire) and decodes it via the arena path. Asserts
// that the post-decode HeapInuse delta is well under what a non-arena decode
// would produce (which would scale 1:1 with the stream size since every
// script slice would be heap-allocated and held by the returned []*bt.Tx).
//
// Skipped under -short because it allocates ~100 MiB during the build phase.
func TestReadTransactionsFromSubtreeDataStream_MemoryBoundWithArena(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	const (
		txCount    = 1000
		scriptSize = 100 * 1024
	)

	// Build a canonical large tx with a single OP_RETURN payload.
	bigScript := make([]byte, scriptSize)
	bigScript[0] = 0x6a // OP_RETURN

	template := bt.NewTx()
	template.AddOutput(&bt.Output{
		Satoshis:      0,
		LockingScript: bscript.NewFromBytes(bigScript),
	})
	raw := template.Bytes()

	stream := make([]byte, 0, len(raw)*txCount)
	for i := 0; i < txCount; i++ {
		stream = append(stream, raw...)
	}

	// Build a matching subtree skeleton. The hash-check inside the stream
	// reader compares decoded txid vs subtree.Nodes[i].Hash, so populate
	// each node with the template's txid.
	expectedHash := *template.TxIDChainHash()
	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(txCount)
	require.NoError(t, err)
	for i := 0; i < txCount; i++ {
		require.NoError(t, subtree.AddNode(expectedHash, 0, 0))
	}

	server := &Server{}

	// Warm the pool with a small decode so the first benchmark iteration
	// doesn't pay the pool's New cost.
	{
		warmArena := getSubtreeArena()
		putSubtreeArena(warmArena)
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	arena := getSubtreeArena()
	subtreeTxs := make([]*bt.Tx, 0, txCount)
	count, err := server.readTransactionsFromSubtreeDataStream(subtree, bytes.NewReader(stream), &subtreeTxs, arena)
	require.NoError(t, err)
	require.Equal(t, txCount, count)
	require.Len(t, subtreeTxs, txCount)

	runtime.GC()
	var afterDecode runtime.MemStats
	runtime.ReadMemStats(&afterDecode)

	decodeDelta := int64(afterDecode.HeapInuse) - int64(before.HeapInuse)
	streamSizeMiB := int64(len(stream)) >> 20
	t.Logf("stream size: %d MiB, post-decode HeapInuse delta: %d MiB",
		streamSizeMiB, decodeDelta>>20)

	// With per-script make() (non-arena), the decoded []*bt.Tx would retain
	// every locking-script payload independently — heap delta ~ stream size.
	// With the arena, every tx's LockingScript points into a single slab
	// (the arena's), so the holding cost is one slab + the *bt.Tx slice +
	// per-tx struct overhead. Cap the assertion well below the wire size:
	// allow up to 1.5x the slab cap as a safety margin for the *bt.Tx +
	// chainhash.Hash + slice overheads.
	maxAllowed := int64(subtreeArenaShrinkCap) + int64(80<<20)
	require.Less(t, decodeDelta, maxAllowed,
		"arena-backed decode of a %d MiB stream should hold ≤ %d MiB live, got %d MiB",
		streamSizeMiB, maxAllowed>>20, decodeDelta>>20)

	// Release the tx slice + arena so the test process doesn't keep the
	// pool warm with a large slab for downstream tests.
	subtreeTxs = nil //nolint:ineffassign // explicit GC hint
	putSubtreeArena(arena)
	_ = subtreeTxs
}
