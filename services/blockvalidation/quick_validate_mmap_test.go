// Package blockvalidation tests the mmap-to-heap fallback path in readSubtree.
package blockvalidation

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo"
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

	result := bv.readSubtree(ctx, block, 0, subtree.RootHash(), subtreeReadWithFullSubtree)

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

// The tests below pin INVARIANT BO — one owner per batch, released exactly once — on
// the only substrate where a lifetime mistake is observable. On the heap path a missed
// release is invisible (the GC collects it) and a premature release corrupts nothing,
// so an mmap-backed run is the only thing that can tell a correct implementation from
// either failure. go-subtree creates one temp file per mmap-backed subtree directly in
// the directory it is handed (os.CreateTemp(dir, "subtree-nodes-*")) and removes it in
// Close, so "the directory is empty" is exactly "every subtree was released".

// requireMmapDirEmpty is the leak assertion: every mmap-backed subtree that was ever
// created has been closed, because Close is what removes the backing file.
func requireMmapDirEmpty(t *testing.T, mmapDir string) {
	t.Helper()

	entries, err := os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.Empty(t, entries, "every mmap-backed subtree must have been released; leftovers: %v", entries)
}

// requireMmapEngaged proves the fixture really takes the mmap path before anything is
// concluded from the directory. Without it a change that quietly fell back to the heap
// would leave the directory empty for the wrong reason and every assertion below would
// be vacuous.
func requireMmapEngaged(t *testing.T, h *preBindHarness, block *model.Block, subtreeHash *chainhash.Hash) {
	t.Helper()

	structure, err := h.bv.readSubtreeStructure(h.ctx, block, subtreeHash, subtreeReadAnchorOnly)
	require.NoError(t, err)
	require.True(t, structure.subtree.IsMmapBacked(),
		"the fixture is not mmap-backed, so nothing below distinguishes a correct release from no release at all")

	releaseSubtreeStructure(structure.subtree)
	releaseSubtreeStructure(structure.fullSubtree)
}

// TestProcessSubtreeBatch_ReaderFailure_LeavesNoMmapFiles covers the COLLECTOR: a batch
// that fails partway through collection used to be abandoned with every subtree it had
// already copied still mapped.
//
// The missing blob is on the FINAL index, and that is load-bearing rather than
// arbitrary. The collector reads the channels in index order and copies every result
// that arrives before the first failure into the batch, so with the failure last,
// indices 0..n-2 are in the batch and only the deferred batch.Close can release them.
// A failure at index 0 exercises the other release, which the next test covers.
//
// Mutation target: removing the deferred batch.Close() from prefetchSubtreeBatch must
// leave three temp files behind. Removing readSubtree's release of structure.subtree
// leaves a fourth, for the subtree whose own data read failed.
func TestProcessSubtreeBatch_ReaderFailure_LeavesNoMmapFiles(t *testing.T) {
	h := newPreBindHarness(t, nil)

	mmapDir := t.TempDir()
	h.bv.mmapDir = mmapDir

	coinbase := preBindCoinbase(t, 0x20)

	groups, _ := h.multiBatchGroups(0xd0, []int{1, 2, 2, 2})

	_, roots, merkleRoot := h.multiSubtreeBody(coinbase, groups)
	block := h.newPreBindBlock(coinbase, roots, merkleRoot, 8)

	requireMmapEngaged(t, h, block, roots[0])
	requireMmapDirEmpty(t, mmapDir)

	// The last subtree's transaction data is gone, so its structure reads cleanly and
	// mmaps, and only the subsequent data read fails.
	require.NoError(t, h.subtreeStore.Del(h.ctx, roots[len(roots)-1][:], fileformat.FileTypeSubtreeData))

	batch, err := h.bv.processSubtreeBatch(h.ctx, block, 0, len(roots), make(map[chainhash.Hash]*bt.Tx), false)
	require.Error(t, err, "a batch with an unreadable subtree_data must fail")
	require.Nil(t, batch)

	requireMmapDirEmpty(t, mmapDir)
}

// TestPrefetchSubtreeBatch_FirstIndexFailure_ReleasesLaterResults covers the release on
// the AGGREGATED failure path. The collector no longer returns at the first failing
// index: it receives every channel so every forged blob in the batch is named. With
// the failure at index 0 nothing is ever copied into the batch, so every later result
// is released by the collector itself as it arrives, not by the batch's close and not
// by the defensive drain, which finds every channel already consumed.
//
// Mutation target: removing the collector's release of post-failure results must leave
// temp files behind.
func TestPrefetchSubtreeBatch_FirstIndexFailure_ReleasesLaterResults(t *testing.T) {
	h := newPreBindHarness(t, nil)

	mmapDir := t.TempDir()
	h.bv.mmapDir = mmapDir

	coinbase := preBindCoinbase(t, 0x23)

	groups, _ := h.multiBatchGroups(0xc0, []int{1, 2, 2, 2})

	_, roots, merkleRoot := h.multiSubtreeBody(coinbase, groups)
	block := h.newPreBindBlock(coinbase, roots, merkleRoot, 8)

	requireMmapEngaged(t, h, block, roots[1])
	requireMmapDirEmpty(t, mmapDir)

	require.NoError(t, h.subtreeStore.Del(h.ctx, roots[0][:], fileformat.FileTypeSubtreeData))

	batch, err := h.bv.prefetchSubtreeBatch(h.ctx, block, 0, len(roots), false)
	require.Error(t, err, "a batch with an unreadable subtree_data must fail")
	require.True(t, errors.Is(err, errors.ErrNotFound), "got %v", err)
	require.Nil(t, batch)

	requireMmapDirEmpty(t, mmapDir)
}

// firstCallFailingUtxoStore fails BatchPreviousOutputsDecorate once, which is the
// simplest deterministic way to fail the pipeline's stage 2 without failing a READ —
// a reader failure is the collector's case and is already covered above.
type firstCallFailingUtxoStore struct {
	utxo.Store

	mu     sync.Mutex
	failed bool
}

func (s *firstCallFailingUtxoStore) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	s.mu.Lock()
	first := !s.failed
	s.failed = true
	s.mu.Unlock()

	if first {
		return errors.NewProcessingError("simulated decorate failure")
	}

	return s.Store.BatchPreviousOutputsDecorate(ctx, txs)
}

// TestPipeline_CancelledDuringExtend_ReleasesInHandBatch covers the IN-HAND batch,
// which no channel drain would ever find: stage 2 is holding the pointer when it
// fails, so nothing is buffered anywhere and only that stage's own release can free it.
//
// The failure is injected in stage 2 rather than in a read precisely so it is not the
// collector's case. Stage 2 fails on the first batch, which cancels gCtx; stage 1's
// send then takes its Done branch still holding the batch it just built, and any batch
// already buffered is left for the deferred drain. All three shapes have to release for
// the directory to come back empty.
//
// Mutation target: dropping stage 2's conditional close, or turning the deferred
// channel drains back into bare discards, must leave temp files behind.
func TestPipeline_CancelledDuringExtend_ReleasesInHandBatch(t *testing.T) {
	h := newPreBindHarness(t, nil)
	h.bv.settings.BlockValidation.SubtreeBatchSize = 2

	mmapDir := t.TempDir()
	h.bv.mmapDir = mmapDir

	coinbase := preBindCoinbase(t, 0x21)

	groups, _ := h.multiBatchGroups(0xe0, []int{1, 2, 2, 2, 2})

	_, roots, merkleRoot := h.multiSubtreeBody(coinbase, groups)
	block := h.newPreBindBlock(coinbase, roots, merkleRoot, 10)

	requireMmapEngaged(t, h, block, roots[0])
	requireMmapDirEmpty(t, mmapDir)

	h.bv.utxoStore = &firstCallFailingUtxoStore{Store: h.utxoStore}

	err := h.bv.quickValidateBlock(h.ctx, block, "peer", "")
	require.Error(t, err, "a stage-2 failure must fail the block")

	requireMmapDirEmpty(t, mmapDir)
}

// TestPipeline_MultiBatch_MmapBacked_HonestBodyValidates is the COUNTER-test, and it
// catches the opposite and more dangerous mistake: a close that happens after the batch
// has already been handed to the next stage.
//
// A missed close leaks. A close after a completed handoff is a use-after-release — the
// next stage dereferences batch.subtrees[i].Nodes into an unmapped region — and no
// amount of Close idempotence helps, because the harm lands on the NEXT owner. On the
// heap path that mistake is completely invisible, which is why this test must be
// mmap-backed and why the engagement check above is not optional.
//
// Three batches and the default prefetch depth, so the stages genuinely overlap and a
// premature release in stage 1 or 2 is concurrent with stage 3's use of it.
//
// Mutation target: turning stage 1's or stage 2's conditional close into an
// unconditional `defer batch.Close()` must make this test fail or crash. If it still
// passes, the fixture is not mmap-backed and the test is worthless.
func TestPipeline_MultiBatch_MmapBacked_HonestBodyValidates(t *testing.T) {
	h := newPreBindHarness(t, nil)
	h.bv.settings.BlockValidation.SubtreeBatchSize = 2

	mmapDir := t.TempDir()
	h.bv.mmapDir = mmapDir

	coinbase := preBindCoinbase(t, 0x22)

	groups, parents := h.multiBatchGroups(0xf0, []int{1, 2, 2, 2, 2})

	_, roots, merkleRoot := h.multiSubtreeBody(coinbase, groups)
	block := h.newPreBindBlock(coinbase, roots, merkleRoot, 10)

	requireMmapEngaged(t, h, block, roots[0])
	requireMmapDirEmpty(t, mmapDir)

	require.NoError(t, h.bv.quickValidateBlock(h.ctx, block, "peer", ""),
		"an honest mmap-backed body spanning three batches must validate")

	for _, group := range groups {
		for _, tx := range group {
			created, getErr := h.utxoStore.Get(h.ctx, tx.TxIDChainHash())
			require.NoError(t, getErr, "every transaction of every batch must have been created")
			require.Equal(t, tx.TxIDChainHash().String(), created.Tx.TxID())
		}
	}

	for _, parent := range parents {
		h.requireParentSpent(parent)
	}

	requireMmapDirEmpty(t, mmapDir)
}
