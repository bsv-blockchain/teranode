// Package blockvalidation tests the mmap-to-heap fallback path in readSubtree.
package blockvalidation

import (
	"context"
	"encoding/binary"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
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

	result := bv.readSubtree(ctx, block, 0, subtree.RootHash(), subtreeReadWithFullSubtree, "batch")

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

	structure, err := h.bv.readSubtreeStructure(h.ctx, block, subtreeHash, subtreeReadAnchorOnly, "binding")
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

// wrappingSubtreeCount is a hostile node count whose count * nodeSize wraps, so before
// go-subtree bounded it the mmap loader mapped a tiny scratch file and then panicked
// in unsafe.Slice.
const wrappingSubtreeCount uint64 = 768614336404564651

// malformedSubtreeBytes returns a 56-byte persisted subtree that declares count nodes
// and carries none: rootHash(32) | fees(8) | sizeInBytes(8) | count(8).
func malformedSubtreeBytes(count uint64) []byte {
	b := make([]byte, 56)
	binary.LittleEndian.PutUint64(b[48:], count)

	return b
}

// countHeapSubtreeFromBytes swaps heapSubtreeFromBytes for a wrapper that counts calls,
// restoring it when the test ends. Tests using it must not run in parallel.
func countHeapSubtreeFromBytes(t *testing.T) *int {
	t.Helper()

	calls := 0
	orig := heapSubtreeFromBytes
	heapSubtreeFromBytes = func(b []byte) (*subtreepkg.Subtree, error) {
		calls++
		return orig(b)
	}

	t.Cleanup(func() { heapSubtreeFromBytes = orig })

	return &calls
}

// TestSubtreeFromBytesWithMmap_MalformedCount_ReturnsError pins that a persisted
// subtree whose declared count cannot be real comes back as an error carrying the
// go-subtree sentinel, with no panic, no scratch file left behind and no heap retry.
// The heap loader returns the same sentinel, so only the call count tells the paths
// apart.
func TestSubtreeFromBytesWithMmap_MalformedCount_ReturnsError(t *testing.T) {
	tests := []struct {
		name    string
		count   uint64
		wantErr error
	}{
		{name: "wrapping count", count: wrappingSubtreeCount, wantErr: subtreepkg.ErrNumLeavesOutOfRange},
		{name: "count exceeds input", count: 1 << 36, wantErr: subtreepkg.ErrNodeCountExceedsInput},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mmapDir := t.TempDir()

			var (
				st  *subtreepkg.Subtree
				err error
			)

			heapCalls := countHeapSubtreeFromBytes(t)

			require.NotPanics(t, func() {
				st, err = subtreeFromBytesWithMmap(malformedSubtreeBytes(tt.count), mmapDir)
			})

			require.Error(t, err)
			require.True(t, errors.Is(err, tt.wantErr), "want %v, got %v", tt.wantErr, err)
			require.Nil(t, st)
			requireMmapDirEmpty(t, mmapDir)
			require.Zero(t, *heapCalls, "malformed bytes must not be retried on the heap loader")
		})
	}
}

// TestSubtreeFromBytesWithMmap_InfraFailureFallsBackToHeap pins the other half of the
// gate: an mmap infrastructure failure (here a missing scratch directory, which makes
// os.CreateTemp fail) still falls back to the heap loader for well-formed bytes.
func TestSubtreeFromBytesWithMmap_InfraFailureFallsBackToHeap(t *testing.T) {
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	b, err := subtree.Serialize()
	require.NoError(t, err)

	heapCalls := countHeapSubtreeFromBytes(t)

	st, err := subtreeFromBytesWithMmap(b, filepath.Join(t.TempDir(), "does-not-exist"))
	require.NoError(t, err)
	require.Equal(t, 1, *heapCalls, "an mmap infrastructure failure falls back to the heap loader once")
	require.False(t, st.IsMmapBacked(), "expected heap-backed subtree from the fallback path")
	require.Equal(t, subtree.RootHash(), st.RootHash())
}

// TestReadSubtree_MmapMalformedCount_ReturnsError drives a malformed persisted subtree
// through the block-validation read path with the mmap backend enabled. The read must
// fail with an error rather than a panic, leave no scratch file behind, and not re-open
// the blob for a heap retry.
func TestReadSubtree_MmapMalformedCount_ReturnsError(t *testing.T) {
	tests := []struct {
		name    string
		count   uint64
		wantErr error
	}{
		// Rejected before any scratch file is mapped.
		{name: "wrapping count", count: wrappingSubtreeCount, wantErr: subtreepkg.ErrNumLeavesOutOfRange},
		// The store reader is not seekable, so this count passes every bound, maps a
		// small sparse scratch file, and fails reading the first node.
		{name: "short body", count: 1000, wantErr: io.EOF},
		// Over the deserialization ceiling: rejected before mapping.
		{name: "terabytes", count: 1 << 36, wantErr: subtreepkg.ErrNodeCountExceedsLimit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initPrometheusMetrics()

			ctx := context.Background()

			utxoStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore, cleanup := setup(t)
			defer cleanup()

			tSettings := test.CreateBaseTestSettings(t)

			subtreeHash := chainhash.Hash{0x48, 0x61, 0x10}
			require.NoError(t, subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, malformedSubtreeBytes(tt.count)))

			recStore := &recordingSubtreeStore{Store: subtreeStore}

			bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, recStore, txStore, utxoStore, nil, subtreeValidationClient)

			mmapDir := t.TempDir()
			bv.mmapDir = mmapDir

			block := &model.Block{
				Header: &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  &chainhash.Hash{},
					HashMerkleRoot: &subtreeHash,
					Timestamp:      1,
				},
				Height: 1,
			}

			var result subtreeResult

			require.NotPanics(t, func() {
				result = bv.readSubtree(ctx, block, 0, &subtreeHash, subtreeReadWithFullSubtree, "batch")
			})

			require.Error(t, result.err)
			require.True(t, errors.Is(result.err, tt.wantErr), "want %v, got %v", tt.wantErr, result.err)
			require.Nil(t, result.subtree)
			requireMmapDirEmpty(t, mmapDir)
			require.Equal(t, 1, recStore.readsOf(subtreeHash, fileformat.FileTypeSubtreeToCheck),
				"malformed bytes must not be re-read for a heap retry")
		})
	}
}

// wrappedErr wraps err with a message the way go-subtree's fmt.Errorf("...: %w") does,
// so TestIsMmapInfraError can build realistic chains.
type wrappedErr struct {
	msg string
	err error
}

func (w wrappedErr) Error() string { return w.msg + ": " + w.err.Error() }

func (w wrappedErr) Unwrap() error { return w.err }

// TestIsMmapInfraError pins the fallback classification against the wrapping go-subtree
// v1.6.4 produces: scratch-file and mapping failures fall back to the heap loader, count
// and short-body errors from the subtree bytes do not.
func TestIsMmapInfraError(t *testing.T) {
	allocFailed := func(err error) error {
		return wrappedErr{msg: "mmap allocation for 4 nodes failed", err: err}
	}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "CreateTemp failure", err: allocFailed(wrappedErr{msg: "failed to create temp file", err: &fs.PathError{Op: "open", Path: "/missing", Err: syscall.ENOENT}}), want: true},
		{name: "mmap refused", err: allocFailed(wrappedErr{msg: "mmap failed", err: syscall.ENOMEM}), want: true},
		{name: "mmap unsupported", err: allocFailed(wrappedErr{msg: "mmap failed", err: subtreepkg.ErrMmapUnsupported}), want: true},
		{name: "recovered mmap panic", err: wrappedErr{msg: "boom", err: subtreepkg.ErrMmapPanic}, want: true},
		{name: "count out of range", err: wrappedErr{msg: "768614336404564651", err: subtreepkg.ErrNumLeavesOutOfRange}, want: false},
		{name: "count exceeds input", err: wrappedErr{msg: "2 declared", err: subtreepkg.ErrNodeCountExceedsInput}, want: false},
		{name: "count exceeds limit", err: wrappedErr{msg: "68719476736 declared", err: subtreepkg.ErrNodeCountExceedsLimit}, want: false},
		{name: "short body", err: wrappedErr{msg: "unable to read node 0", err: io.EOF}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isMmapInfraError(tt.err))
		})
	}
}
