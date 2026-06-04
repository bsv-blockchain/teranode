package repository

import (
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestGetSubtreeDataWithReader tests subtree data retrieval from storage backends.
func TestGetSubtreeDataWithReader(t *testing.T) {
	tracing.SetupMockTracer()

	t.Run("get subtree from subtree store", func(t *testing.T) {
		ctx, subtree, txs := setupSubtreeReaderTest(t)

		// create the block-store .subtree file
		storer, err := filestorer.NewFileStorer(t.Context(), ctx.logger, ctx.settings, ctx.repo.SubtreeStore, subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)

		for _, tx := range txs {
			_, err = storer.Write(tx.Bytes())
			require.NoError(t, err)
		}

		require.NoError(t, storer.Close(t.Context()))

		// should be able to get the subtree from the block-store (should NOT be looking at subtree-store)
		r, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.NoError(t, err)

		checkSubtreeTransactions(t, r, true)

		// close the reader
		require.NoError(t, r.Close())
	})

	t.Run("get subtree from utxo store", func(t *testing.T) {
		ctx, subtree, _ := setupSubtreeReaderTest(t)

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		// write the subtree to the subtree store
		err = ctx.repo.SubtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
		require.NoError(t, err)

		// should be able to get the subtree from the block-store (should NOT be looking at subtree-store)
		r, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.NoError(t, err)

		checkSubtreeTransactions(t, r, false)

		// close the reader
		require.NoError(t, r.Close())
	})

	t.Run("returns NotFound when neither subtreeData nor subtree exists", func(t *testing.T) {
		// Regression: previously the on-demand fallback committed to 200 OK and then
		// failed inside a goroutine, leaving peers with a truncated body that parsed
		// as ErrSubtreeLengthMismatch. With no source data on disk the repository
		// must surface NotFound up front so the HTTP layer can emit 404.
		resetQuorumForTests()
		ctx, subtree, _ := setupSubtreeReaderTest(t)

		_, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound), "expected ErrNotFound, got: %v", err)
	})

	t.Run("client disconnect during stream is classified as client_gone, not write_failed", func(t *testing.T) {
		// Regression for the catchup avalanche: when the HTTP client/proxy disconnects
		// mid-stream, the producer goroutine sees io.ErrClosedPipe on its next write.
		// This is not a server fault, so it must be logged at debug and counted under
		// the "client_gone" reason — never "write_failed", which is reserved for genuine
		// server-side errors.
		resetQuorumForTests()
		ctx, subtree, _ := setupSubtreeReaderTest(t)

		// Make sure on-demand path runs (subtreeData absent, subtree present).
		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)
		require.NoError(t, ctx.repo.SubtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

		// Force metrics initialization (also done lazily on first dual-stream call).
		initPrometheusMetrics()
		clientGoneBefore := testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "client_gone"))
		writeFailedBefore := testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "write_failed"))

		r, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.NoError(t, err)

		// Emulate the client disconnecting before reading any bytes — closes the pipe
		// reader, the producer's next Write returns io.ErrClosedPipe.
		require.NoError(t, r.Close())

		require.Eventually(t, func() bool {
			return testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "client_gone")) > clientGoneBefore
		}, 2*time.Second, 10*time.Millisecond, "client_gone counter did not increment")

		require.InDelta(t,
			writeFailedBefore,
			testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "write_failed")),
			0, "write_failed must not increment for a client disconnect")
	})

	t.Run("genuine server-side write error still records write_failed", func(t *testing.T) {
		// Companion to the client_gone test above: when the producer goroutine fails
		// for a reason that is NOT a client disconnect (here: a referenced tx is missing
		// from the utxo store, so writeTransactionsViaSubtreeStoreStreaming returns a
		// ProcessingError unrelated to the pipe/ctx), the metric must record under
		// "write_failed", not "client_gone".
		resetQuorumForTests()
		ctx := setup(t)

		// Build a subtree referencing a tx that is NOT in the utxo store, then
		// register only the subtree (no subtreeData), forcing the on-demand path.
		_, subtree := newBlock(ctx, t, params)
		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)
		require.NoError(t, ctx.repo.SubtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

		// Intentionally DO NOT populate utxo store — so fetchSubtreeChunk will report
		// missing tx meta and return a server-side ProcessingError.

		initPrometheusMetrics()
		clientGoneBefore := testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "client_gone"))
		writeFailedBefore := testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "write_failed"))

		r, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.NoError(t, err)

		// Drain to let the goroutine produce an error and close the pipe-with-error.
		_, _ = io.Copy(io.Discard, r)
		_ = r.Close()

		require.Eventually(t, func() bool {
			return testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "write_failed")) > writeFailedBefore
		}, 2*time.Second, 10*time.Millisecond, "write_failed counter did not increment for a server-side error")

		require.InDelta(t,
			clientGoneBefore,
			testutil.ToFloat64(prometheusAssetSubtreeDataCreated.WithLabelValues("error", "client_gone")),
			0, "client_gone must not increment for a server-side fault")
	})

	t.Run("get subtree from utxo store and verify file creation", func(t *testing.T) {
		resetQuorumForTests() // Reset singleton for this test
		ctx, subtree, _ := setupSubtreeReaderTest(t)

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		// write the subtree to the subtree store
		err = ctx.repo.SubtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
		require.NoError(t, err)

		// Verify subtreeData file does NOT exist yet
		exists, err := ctx.repo.SubtreeStore.Exists(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		require.False(t, exists, "subtreeData file should not exist before first request")

		// First request - should trigger dual-streaming and file creation
		r, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.NoError(t, err)

		checkSubtreeTransactions(t, r, false)
		require.NoError(t, r.Close())

		// Verify subtreeData file NOW exists
		exists, err = ctx.repo.SubtreeStore.Exists(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		require.True(t, exists, "subtreeData file should exist after first request")

		// Verify the file is valid by reading it directly and parsing as subtreeData
		subtreeDataReader, err := ctx.repo.SubtreeStore.GetIoReader(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		subtreeData, err := subtreepkg.NewSubtreeDataFromReader(subtree, subtreeDataReader)
		require.NoError(t, err)
		require.NoError(t, subtreeDataReader.Close())
		require.NotNil(t, subtreeData, "subtreeData should be valid")

		// The subtreeData should match the subtree structure
		// Subtree has 2 nodes: coinbase placeholder + tx1
		// Since we skip coinbase during write (block=nil), subtreeData.Txs will have:
		// - Index 0: nil (coinbase placeholder, not in file)
		// - Index 1: tx1 (from file)
		// So len(subtreeData.Txs) == subtree.Length() == 2
		require.Equal(t, subtree.Length(), len(subtreeData.Txs), "subtreeData should have same length as subtree")

		// Verify non-nil transaction matches (skip index 0 which is coinbase placeholder)
		require.Nil(t, subtreeData.Txs[0], "coinbase placeholder should be nil")
		require.NotNil(t, subtreeData.Txs[1], "tx1 should not be nil")
		require.Equal(t, params.txs[1].TxID(), subtreeData.Txs[1].TxID(), "tx1 ID should match")

		// Second request - should read from the file we just created
		r2, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.NoError(t, err)

		checkSubtreeTransactions(t, r2, false)
		require.NoError(t, r2.Close())
	})
}

func setupSubtreeReaderTest(t *testing.T) (*testContext, *subtreepkg.Subtree, []*bt.Tx) {
	ctx := setup(t)
	ctx.logger.Debugf("test")

	_, subtree := newBlock(ctx, t, params)

	txs := make([]*bt.Tx, 0, len(params.txs))

	// Create the txs in the utxo store
	for i, tx := range params.txs {
		if i != 0 {
			_, err := ctx.repo.UtxoStore.Create(t.Context(), tx, params.height)
			require.NoError(t, err)
		}

		txs = append(txs, tx)
	}

	return ctx, subtree, txs
}

func checkSubtreeTransactions(t *testing.T, r io.ReadCloser, includeCoinbase bool) {
	// read the transactions from the subtree data
	txCount := 0

	offset := 1
	if !includeCoinbase {
		offset = 0
	}

	for {
		tx := &bt.Tx{}

		_, err := tx.ReadFrom(r)
		if err != nil {
			break
		}

		txCount++
		require.Equal(t, params.txs[txCount-offset].TxID(), tx.TxID())
	}

	if includeCoinbase {
		require.Equal(t, len(params.txs), txCount)
	} else {
		require.Equal(t, len(params.txs)-1, txCount)
	}
}

// TestGetSubtreeDataWithQuorum tests quorum-based distributed locking for subtreeData creation.
func TestGetSubtreeDataWithQuorum(t *testing.T) {
	tracing.SetupMockTracer()

	t.Run("create with quorum lock", func(t *testing.T) {
		resetQuorumForTests() // Reset singleton for this test
		ctx, subtree, _ := setupSubtreeReaderTest(t)

		// Configure quorum path in settings to enable distributed locking
		quorumPath := t.TempDir()
		ctx.settings.SubtreeValidation.QuorumPath = quorumPath

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		// Write the subtree to the subtree store
		err = ctx.repo.SubtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
		require.NoError(t, err)

		// Verify subtreeData file does NOT exist yet
		exists, err := ctx.repo.SubtreeStore.Exists(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		require.False(t, exists, "subtreeData file should not exist before first request")

		// First request with quorum - should trigger dual-streaming with lock
		r, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.NoError(t, err)

		checkSubtreeTransactions(t, r, false)
		require.NoError(t, r.Close())

		// Verify subtreeData file NOW exists
		exists, err = ctx.repo.SubtreeStore.Exists(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		require.True(t, exists, "subtreeData file should exist after first request")

		// Second request - should read from the file (no lock needed)
		r2, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.NoError(t, err)

		checkSubtreeTransactions(t, r2, false)
		require.NoError(t, r2.Close())
	})

	t.Run("fallback when quorum not configured", func(t *testing.T) {
		resetQuorumForTests() // Reset singleton for this test
		ctx, subtree, _ := setupSubtreeReaderTest(t)

		// Ensure quorum path is empty (no distributed locking)
		ctx.settings.SubtreeValidation.QuorumPath = ""

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		// Write the subtree to the subtree store
		err = ctx.repo.SubtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
		require.NoError(t, err)

		// Request without quorum - should use fallback behavior (no lock)
		r, err := ctx.repo.GetSubtreeDataReader(t.Context(), subtree.RootHash())
		require.NoError(t, err)

		checkSubtreeTransactions(t, r, false)
		require.NoError(t, r.Close())

		// Verify file was still created (just without distributed locking)
		exists, err := ctx.repo.SubtreeStore.Exists(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		require.True(t, exists, "subtreeData file should exist even without quorum")
	})
}
