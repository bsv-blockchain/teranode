package repository

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	memory_blob "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// errgroup deliberately does not propagate panics from its children, so every
// fan-out goroutine on a request path needs its own recover or one bad record
// takes the whole asset process down. The handler-side fan-outs are guarded in
// httpimpl; these cover the repository ones, which are worse: GetLegacyBlockReader
// and GetSubtreeDataReader return a pipe reader and let their producer goroutine
// outlive the request, so echo's middleware.Recover cannot reach them even in
// principle.
//
// The panic is not hypothetical. getTxs reconstructs transactions from the utxo
// store, which allocates the full output slice and fills only the outputs it has
// (stores/utxo/aerospike/get.go), so a tx whose outputs are partially absent
// carries nil *bt.Output holes — and Tx.WriteTo in writeChunkToWriter dereferences
// every one of them.
//
// Without the guards these tests do not fail, they abort the whole test binary,
// which is exactly the production behaviour being asserted against.

const repoPanicMessage = "runtime error: invalid memory address or nil pointer dereference"

// panicOnGetIoReader panics instead of returning a reader for one file type,
// standing in for a store driver that faults inside the producer goroutine.
type panicOnGetIoReader struct {
	*memory_blob.Memory

	targetType fileformat.FileType
}

func (s *panicOnGetIoReader) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	if fileType == s.targetType {
		panic(repoPanicMessage)
	}

	return s.Memory.GetIoReader(ctx, key, fileType, opts...)
}

// panicOnNthRead delegates to the wrapped store but makes the returned reader
// panic on its nth Read, which lands the panic in whichever goroutine is pulling
// bytes out of the subtree stream rather than in the one that opened it.
type panicOnNthRead struct {
	*memory_blob.Memory

	targetType fileformat.FileType
	nth        int64
}

func (s *panicOnNthRead) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	rc, err := s.Memory.GetIoReader(ctx, key, fileType, opts...)
	if err != nil || fileType != s.targetType {
		return rc, err
	}

	return &panicReader{inner: rc, nth: s.nth}, nil
}

type panicReader struct {
	inner io.ReadCloser
	nth   int64
	reads atomic.Int64
}

func (r *panicReader) Read(p []byte) (int, error) {
	if r.reads.Add(1) >= r.nth {
		panic(repoPanicMessage)
	}

	return r.inner.Read(p)
}

func (r *panicReader) Close() error { return r.inner.Close() }

// panicOnBatchDecorate faults in the innermost fan-out: the getTxs batch workers,
// nested two errgroups below the streaming goroutine.
type panicOnBatchDecorate struct {
	utxo.Store
}

func (s *panicOnBatchDecorate) BatchDecorate(_ context.Context, _ []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	panic(repoPanicMessage)
}

// bigSubtree builds and stores a subtree with enough leaves that reading one
// chunk of hashes needs more than one Read off the underlying reader (the stream
// is buffered at subtreeStreamBufferSize), so a panic can be aimed at the chunk
// scheduler rather than at the goroutine that opened the stream.
func bigSubtree(t *testing.T, tc *testContext, leaves int) *subtreepkg.Subtree {
	t.Helper()

	subtree, err := subtreepkg.NewTreeByLeafCount(leaves)
	require.NoError(t, err)

	require.NoError(t, subtree.AddCoinbaseNode())

	for i := 1; i < leaves; i++ {
		hash := chainhash.Hash{}
		hash[0] = byte(i)
		hash[1] = byte(i >> 8)

		require.NoError(t, subtree.AddNode(hash, 100, 0))
	}

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, tc.repo.SubtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	require.Greater(t, len(subtreeBytes), subtreeStreamBufferSize,
		"fixture must exceed the stream buffer or the chunk read never touches the underlying reader")

	return subtree
}

// storeSubtree writes the subtree index file so the on-demand streaming path has
// something to read.
func storeSubtree(t *testing.T, tc *testContext, subtree *subtreepkg.Subtree) {
	t.Helper()

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, tc.repo.SubtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))
}

// GetLegacyBlock.go:70 — the detached legacy-block streamer. The consumer is
// blocked on the pipe, so recovering is not enough: the pipe has to be closed
// with the error or the request hangs until the client gives up.
func TestGetLegacyBlockReader_PanicInDetachedStreamerFailsTheStream(t *testing.T) {
	tracing.SetupMockTracer()

	tc := setup(t)
	block, subtree := newBlock(tc, t, params)

	blockchainClientMock := tc.repo.BlockchainClient.(*blockchain.Mock)
	blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

	storeSubtree(t, tc, subtree)

	tc.repo.SubtreeStore = &panicOnGetIoReader{
		Memory:     tc.repo.SubtreeStore.(*memory_blob.Memory),
		targetType: fileformat.FileTypeSubtree,
	}

	r, err := tc.repo.GetLegacyBlockReader(t.Context(), &chainhash.Hash{})
	require.NoError(t, err)

	defer func() {
		_ = r.Close()
	}()

	_, readErr := io.ReadAll(r)
	require.Error(t, readErr, "consumer must see a failed stream; if this hangs the pipe was not closed with the error")
	require.NotContains(t, readErr.Error(), repoPanicMessage, "the panic value belongs in the log, not in the stream error")
}

// panicOnReadCloser panics on first Read and records whether it was closed.
type panicOnReadCloser struct {
	closed atomic.Bool
}

func (p *panicOnReadCloser) Read([]byte) (int, error) { panic(repoPanicMessage) }

func (p *panicOnReadCloser) Close() error {
	p.closed.Store(true)

	return nil
}

// Recovering the panic is only half the job on this path: the subtree-data reader
// is a *semaphoreReadCloser, and its Close is the only thing that returns the file
// store's process-global read permit (768 of them; once exhausted every blob read
// waits 25s and then 503s). A recover that skips the Close trades a crash — which
// a restart resets — for a process that quietly bleeds read capacity for its
// lifetime, which is worse.
func TestGetLegacyBlockReader_PanicInSubtreeDataLoopStillClosesTheReader(t *testing.T) {
	tracing.SetupMockTracer()

	tc := setup(t)
	block, subtree := newBlock(tc, t, params)

	blockchainClientMock := tc.repo.BlockchainClient.(*blockchain.Mock)
	blockchainClientMock.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

	// Pre-write a non-empty SubtreeData so Exists() is true and the inline
	// streaming branch — the one that owns a reader — is taken.
	subtreeData := subtreepkg.NewSubtreeData(subtree)

	for i, tx := range params.txs {
		if i != 0 {
			require.NoError(t, subtreeData.AddTx(tx, i))
		}
	}

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, tc.repo.SubtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	// Panic where the PR says the hazard is: deserialising store-sourced bytes.
	panicking := &panicOnReadCloser{}
	tc.repo.SubtreeStore = &subtreeStoreFakeReader{
		Memory:     tc.repo.SubtreeStore.(*memory_blob.Memory),
		targetKey:  subtree.RootHash()[:],
		targetType: fileformat.FileTypeSubtreeData,
		reader:     panicking,
	}

	r, err := tc.repo.GetLegacyBlockReader(t.Context(), &chainhash.Hash{})
	require.NoError(t, err)

	defer func() {
		_ = r.Close()
	}()

	_, readErr := io.ReadAll(r)
	require.Error(t, readErr, "consumer must see a failed stream")
	require.True(t, panicking.closed.Load(),
		"the subtree-data reader must be closed on the panic path too, or the file-store read permit leaks for the life of the process")
}

// GetLegacyBlock.go:322 — the chunk scheduler, one level below the streamer.
// A recover on the outer goroutine cannot observe this one.
func TestWriteTransactionsViaSubtreeStoreStreaming_PanicInChunkSchedulerIsSurfaced(t *testing.T) {
	tracing.SetupMockTracer()

	tc := setup(t)
	subtree := bigSubtree(t, tc, 2048)

	// The scheduler is the only goroutine that reads chunk hashes off the stream.
	// Read 1 is the buffered header fill; the panic lands on the chunk read.
	tc.repo.SubtreeStore = &panicOnNthRead{
		Memory:     tc.repo.SubtreeStore.(*memory_blob.Memory),
		targetType: fileformat.FileTypeSubtree,
		nth:        2,
	}

	err := tc.repo.writeTransactionsViaSubtreeStoreStreaming(t.Context(), io.Discard, nil, subtree.RootHash())
	require.Error(t, err, "the scheduler's panic must come back as an error from g.Wait()")
	require.NotContains(t, err.Error(), repoPanicMessage)
}

// GetLegacyBlock.go:562 — the innermost fan-out (getTxs batch workers), nested
// two errgroups deep. Its error has to travel out through the chunk worker and
// the scheduler to reach the caller.
func TestWriteTransactionsViaSubtreeStoreStreaming_PanicInGetTxsWorkerIsSurfaced(t *testing.T) {
	tracing.SetupMockTracer()

	tc, subtree, _ := setupSubtreeReaderTest(t)
	storeSubtree(t, tc, subtree)

	tc.repo.UtxoStore = &panicOnBatchDecorate{Store: tc.repo.UtxoStore}

	err := tc.repo.writeTransactionsViaSubtreeStoreStreaming(t.Context(), io.Discard, nil, subtree.RootHash())
	require.Error(t, err, "a panic two errgroups down must still surface as an error")
	require.NotContains(t, err.Error(), repoPanicMessage)
}

// GetSubtreeData.go:261 — the detached subtree-data streamer. It writes to a
// FileStorer as well as the pipe, so on panic the temp blob must be discarded:
// finalising a partial subtreeData is what #1377 fixed, and Exists() would then
// serve it forever.
func TestGetSubtreeDataReader_PanicInDetachedStreamerAbortsTheBlob(t *testing.T) {
	tracing.SetupMockTracer()
	resetQuorumForTests()
	initPrometheusMetrics()

	tc, subtree, _ := setupSubtreeReaderTest(t)
	storeSubtree(t, tc, subtree)

	// Panic on the streamer's own frame, not in a nested fan-out: the subtree
	// stream is opened by this goroutine, so this is the guard being exercised.
	tc.repo.SubtreeStore = &panicOnGetIoReader{
		Memory:     tc.repo.SubtreeStore.(*memory_blob.Memory),
		targetType: fileformat.FileTypeSubtree,
	}

	writeFailedBefore := testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "write_failed"))

	r, err := tc.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
	require.NoError(t, err)

	defer func() {
		_ = r.Close()
	}()

	_, readErr := io.ReadAll(r)
	require.Error(t, readErr, "consumer must see a failed stream; if this hangs the pipe was not closed with the error")
	require.NotContains(t, readErr.Error(), repoPanicMessage)

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "write_failed")) > writeFailedBefore
	}, 2*time.Second, 10*time.Millisecond, "a panic is a server-side failure and must be counted, not silently dropped")

	exists, err := tc.repo.SubtreeStore.Exists(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.False(t, exists, "a panic mid-generation must abort the storer, never finalise a partial subtreeData")
}
