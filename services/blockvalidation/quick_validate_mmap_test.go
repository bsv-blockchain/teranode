// Package blockvalidation tests the mmap-to-heap fallback path in readSubtree.
package blockvalidation

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestReadSubtree_MmapFallbackReReadsFromStart is a regression test for the bug
// where the mmap-deserialization fallback in readSubtree reset the buffered
// reader onto an already-consumed, non-seekable io.ReadCloser. After the mmap
// attempt drained the stream, the heap fallback read from mid/end of the stream
// and produced a corrupt subtree (or a misleading deserialization error).
//
// The fix re-opens a fresh reader from the store for the fallback so the heap
// path reads from the start. This test forces the mmap path to fail (by pointing
// mmapDir at a non-existent directory, which makes the temp-file creation in the
// mmap allocator fail) and asserts the subtree still deserializes correctly.
func TestReadSubtree_MmapFallbackReReadsFromStart(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	utxoStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	tSettings := test.CreateBaseTestSettings(t)

	// Coinbase tx — must be the first tx of the first subtree.
	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)
	coinbaseTx.Outputs = nil
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000))

	// Minimal subtree containing a single coinbase node.
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))

	// Persist subtree + subtree data so readSubtree can load them. FileTypeSubtree
	// holds the full subtree serialization (root hash + header + nodes), matching
	// what subtree validation writes in production.
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	// Force the mmap path: a non-empty mmapDir enables it, and pointing it at a
	// non-existent directory makes the mmap allocator's os.CreateTemp fail, which
	// triggers the heap fallback after the underlying stream was already consumed.
	bv.mmapDir = filepath.Join(t.TempDir(), "does-not-exist")

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: subtree.RootHash(),
			Timestamp:      1,
		},
		Height: 1,
	}

	result := bv.readSubtree(ctx, block, 0, subtree.RootHash())

	// Before the fix this errored ("failed to deserialize subtree") or returned a
	// corrupt subtree because the fallback read from the consumed stream.
	require.NoError(t, result.err)
	require.NotNil(t, result.subtree)
	require.Equal(t, subtree.RootHash(), result.subtree.RootHash(), "fallback must reproduce the same subtree")
	require.Equal(t, subtree.Length(), result.subtree.Length())

	// The fallback subtree must be heap-backed, proving the mmap path actually
	// failed and the heap fallback was exercised.
	require.False(t, result.subtree.IsMmapBacked(), "expected heap-backed subtree from fallback path")

	// Coinbase at index 0 of subtree 0 is set to nil by readSubtree.
	require.NotNil(t, result.subtreeData)
	require.Len(t, result.subtreeData.Txs, 1)
	require.Nil(t, result.subtreeData.Txs[0])
}

// TestReadSubtree_MmapEnabled_MatchesHeapResult confirms that block validation
// with a configured, writable mmap directory is behaviour-neutral: the loaded
// subtree and its data must be identical to the heap-allocated (mmap disabled)
// case, the only difference being where the subtree Nodes live in memory.
func TestReadSubtree_MmapEnabled_MatchesHeapResult(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)
	coinbaseTx.Outputs = nil
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000))

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: subtree.RootHash(),
			Timestamp:      1,
		},
		Height: 1,
	}

	runReadSubtree := func(mmapDir string) subtreeResult {
		utxoStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore, cleanup := setup(t)
		defer cleanup()

		require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))
		require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

		tSettings := test.CreateBaseTestSettings(t)

		bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
		bv.mmapDir = mmapDir

		result := bv.readSubtree(ctx, block, 0, subtree.RootHash())

		// Release any mmap-backed resources before the enclosing t.TempDir() (used
		// by the caller for mmapDir) is torn down, so cleanup never races an open
		// mapping or its backing file.
		t.Cleanup(func() {
			if result.subtree != nil {
				_ = result.subtree.Close()
			}
		})

		return result
	}

	heapResult := runReadSubtree("")
	require.NoError(t, heapResult.err)
	require.False(t, heapResult.subtree.IsMmapBacked())

	mmapResult := runReadSubtree(t.TempDir())
	require.NoError(t, mmapResult.err)
	require.True(t, mmapResult.subtree.IsMmapBacked(), "expected mmap-backed subtree when a writable mmapDir is configured")

	require.Equal(t, heapResult.subtree.RootHash(), mmapResult.subtree.RootHash())
	require.Equal(t, heapResult.subtree.Length(), mmapResult.subtree.Length())
	require.Equal(t, heapResult.subtree.Nodes, mmapResult.subtree.Nodes)
	require.Equal(t, heapResult.subtreeData.Txs, mmapResult.subtreeData.Txs)
	require.Equal(t, heapResult.fullSubtreeExists, mmapResult.fullSubtreeExists)
}
