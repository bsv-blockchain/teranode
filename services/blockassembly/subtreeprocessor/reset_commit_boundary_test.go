package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// reset has no rollback: by the time its postProcess callback runs, reset has
// already committed (header at the target tip, currentTxMap cleared). A disk
// tx map error surfacing during the reload inside postProcess must therefore
// be logged and counted, not fail reset: failing it would tell the caller
// nothing changed when the new state is already committed. This is what BlockAssembler.loadUnminedTransactions
// achieves in production by calling AddNodesDirectlyReportOnly/AddDirectlyReportOnly
// instead of AddNodesDirectly/AddDirectly when isReload is true; this test
// exercises the same contract directly against SubtreeProcessor.reset.
func TestReset_PostProcessReportOnlyMapErrorDoesNotFailReset(t *testing.T) {
	ctx := context.Background()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	blockchainClient := &blockchain.Mock{}

	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, ulogger.NewErrorTestLogger(t), test.CreateBaseTestSettings(t), blob_memory.New(),
		blockchainClient, utxoStore, newSubtreeChan, WithTxMapDirs([]string{t.TempDir()}))
	require.NoError(t, err)
	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	targetHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2200000001,
		Bits:           model.NBit{},
		Nonce:          9201,
	}

	boom := errors.NewStorageError("badger write failed")

	before := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("AddDirectlyReportOnly"))

	postProcess := func() error {
		// Simulates BlockAssembler.loadUnminedTransactions(ctx, isReload=true):
		// a pending map error observed by the report-only add must not fail
		// this callback (and so must not fail reset).
		stp.diskTxMap.recordErr(boom)

		node := &subtreepkg.Node{Hash: chainhash.HashH([]byte("reset-postprocess-reportonly-tx")), Fee: 1, SizeInBytes: 100}
		inp := &subtreepkg.TxInpoints{}

		return stp.AddDirectlyReportOnly(node, inp, true)
	}

	response := stp.Reset(targetHeader, nil, nil, false, postProcess)
	require.NoError(t, response.Err, "a report-only map error in postProcess must not fail reset")

	require.Equal(t, targetHeader.Hash(), stp.GetCurrentBlockHeader().Hash(), "reset must still land on the target header")

	after := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("AddDirectlyReportOnly"))
	require.Equal(t, before+1, after, "the map error must still be logged and counted")

	require.True(t, stp.TakeResetRequested(), "a post-commit map error surfacing during reset's own reload must request a further reset - reset has no rollback of its own, so this is the only way the phantom it may have left gets cured")
}

// A disk tx map error recorded before reset ever started - or Clear's own
// pre-rotation flushAllDisks - belongs to the generation Clear just rotated
// away. Reset's Clear gives every disk a fresh Badger generation, so nothing
// about that stale error is still true of the map's current content: it must
// not survive to request a (redundant) reset once reset itself succeeds.
// Without this, a stale pending error - or a prior reset's own unconsumed
// request - makes every subsequent clean reset immediately request another
// one, looping.
func TestReset_StaleErrorBeforeResetDoesNotRequestReset(t *testing.T) {
	stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir()})

	stp.diskTxMap.recordErr(errors.NewStorageError("old generation write failed"))

	response := stp.reset(&model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2200000002,
		Bits:           model.NBit{},
		Nonce:          9202,
	}, nil, nil, false, func() error { return nil })
	require.NoError(t, response, "a clean reset (no reload error) must succeed")

	require.False(t, stp.TakeResetRequested(), "a stale pre-reset error must not survive the rotation to request a redundant reset")
	require.NoError(t, stp.diskTxMapErr(), "the stale error must have been drained, not left pending")
}

// reset's currentTxMap.Clear() + Length()==0 check runs before
// closeChainedSubtrees and before currentSubtree is replaced: a failed
// rotation must leave STP fully intact - still on the pre-reset header,
// chainedSubtrees and currentSubtree - rather than committed to an empty
// template on top of a still-populated, stale map.
func TestReset_FailedTxMapClearLeavesSTPFullyIntact(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	blockchainClient := &blockchain.Mock{}

	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, ulogger.NewErrorTestLogger(t), test.CreateBaseTestSettings(t), blob_memory.New(),
		blockchainClient, utxoStore, newSubtreeChan, WithTxMapDirs([]string{dir}))
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, half := range []*DiskTxMap{stp.diskTxMap, stp.diskTxMapShadow} {
			if half != nil {
				_ = half.Close()
			}
		}
	})

	// Not started: reset is called directly (single-goroutine), so reading
	// STP's internal fields afterwards is race-free.
	node := &subtreepkg.Node{Hash: chainhash.HashH([]byte("pre-reset-tx")), Fee: 1, SizeInBytes: 100}
	require.NoError(t, stp.AddDirectly(node, &subtreepkg.TxInpoints{}, true))

	originalHeader := stp.currentBlockHeader.Load()
	originalCurrentSubtree := stp.currentSubtree.Load()
	originalChainedLen := len(stp.chainedSubtrees)

	sealDir(t, dir) // Clear can no longer rotate: its replacement generation can't be created.

	targetHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2300000001,
		Bits:           model.NBit{},
		Nonce:          9301,
	}

	before := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("reset_rotation_failed"))

	resp := stp.runReset(&resetBlocks{blockHeader: targetHeader})
	resetErr := resp.Err
	require.Error(t, resetErr, "a failed tx map rotation must fail reset")
	require.False(t, resp.Rotated)
	require.True(t, resp.StorageFailed, "a failed rotation is the reset's own storage failure")
	require.ErrorContains(t, resetErr, "tx map still holds")

	// A rotation failure dead-ends the usual post-commit escalation (reset
	// fails outright, so reportOrJoinDiskTxMapErr("reset") only joins the map
	// error into that failure instead of requesting a reset): it must still
	// be visible through its own distinct counter, since the generic
	// disk_tx_map_errors_total{where="reset"} label is never incremented on
	// this path.
	after := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("reset_rotation_failed"))
	require.Equal(t, before+1, after, "a rotation failure must be counted distinctly from the generic post-commit path")

	require.Same(t, originalCurrentSubtree, stp.currentSubtree.Load(), "currentSubtree must be untouched: reset must fail before replacing it")
	require.Equal(t, originalChainedLen, len(stp.chainedSubtrees), "chainedSubtrees must be untouched: closeChainedSubtrees must never run")
	require.Equal(t, originalHeader.Hash(), stp.currentBlockHeader.Load().Hash(), "header must be untouched")
}

func resetTestHeader(nonce uint32) *model.BlockHeader {
	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2200000003,
		Bits:           model.NBit{},
		Nonce:          nonce,
	}
}

// runReset reports, for the reset that just ran, whether it rotated the tx map
// and whether it hit a disk tx map storage error itself (its rotation failed,
// its reload raised a new reset request, or a map error was joined into its
// failure). BlockAssembler decides the degraded state and the pre-rotation
// retry from this response, never from state a previous reset left behind.
func TestRunReset_StorageOutcome(t *testing.T) {
	clean := func() error { return nil }

	t.Run("clean reset", func(t *testing.T) {
		stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir()})

		resp := stp.runReset(&resetBlocks{blockHeader: resetTestHeader(9301), postProcess: clean})
		require.NoError(t, resp.Err)
		require.True(t, resp.Rotated)
		require.False(t, resp.StorageFailed)
	})

	t.Run("reload raises a new request", func(t *testing.T) {
		stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir()})

		resp := stp.runReset(&resetBlocks{blockHeader: resetTestHeader(9302), postProcess: func() error {
			stp.requestReset("test_reload")
			return nil
		}})
		require.NoError(t, resp.Err)
		require.True(t, resp.StorageFailed)
		require.True(t, stp.TakeResetRequested(), "the reload's request stays pending for the heartbeat")

		// The next reset reports its own outcome, not the previous one's.
		resp = stp.runReset(&resetBlocks{blockHeader: resetTestHeader(9303), postProcess: clean})
		require.False(t, resp.StorageFailed)
	})

	t.Run("a request raised before the rotation does not count", func(t *testing.T) {
		stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir()})
		stp.requestReset("before_reset")

		resp := stp.runReset(&resetBlocks{blockHeader: resetTestHeader(9304), postProcess: clean})
		require.NoError(t, resp.Err)
		require.False(t, resp.StorageFailed)
	})

	t.Run("a map error joined into a failed reload counts", func(t *testing.T) {
		stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir()})

		resp := stp.runReset(&resetBlocks{blockHeader: resetTestHeader(9305), postProcess: func() error {
			stp.diskTxMap.recordErr(errors.NewStorageError("badger write failed"))
			return errors.NewProcessingError("utxo store unavailable")
		}})
		require.Error(t, resp.Err)
		require.True(t, resp.Rotated)
		require.True(t, resp.StorageFailed, "the reload's map error is joined into the failure, not requested, but it is still this reset's storage error")
	})

	t.Run("a failure before the rotation with a request pending", func(t *testing.T) {
		stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir()})

		// An assembly tx makes reset call markNotOnLongestChain, which fails
		// before the rotation.
		node := &subtreepkg.Node{Hash: chainhash.HashH([]byte("pre-rotation-tx")), Fee: 1, SizeInBytes: 100}
		require.NoError(t, stp.AddDirectly(node, &subtreepkg.TxInpoints{}, true))

		failing := &utxo.MockUtxostore{}
		failing.On("MarkTransactionsOnLongestChain", mock.Anything, mock.Anything, false).
			Return(errors.NewStorageError("utxo store unavailable"))
		stp.utxoStore = failing

		stp.requestReset("before_reset")

		resp := stp.runReset(&resetBlocks{blockHeader: resetTestHeader(9306), postProcess: clean})
		require.Error(t, resp.Err)
		require.False(t, resp.Rotated)
		require.False(t, resp.StorageFailed, "a UTXO store failure before the rotation is not a disk tx map storage failure")
		require.True(t, stp.TakeResetRequested(), "the rotation never ran, so the earlier request is still pending")
	})

	t.Run("a failure before the rotation with a map error pending", func(t *testing.T) {
		stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir()})

		node := &subtreepkg.Node{Hash: chainhash.HashH([]byte("pre-rotation-tx-2")), Fee: 1, SizeInBytes: 100}
		require.NoError(t, stp.AddDirectly(node, &subtreepkg.TxInpoints{}, true))

		failing := &utxo.MockUtxostore{}
		failing.On("MarkTransactionsOnLongestChain", mock.Anything, mock.Anything, false).
			Return(errors.NewStorageError("utxo store unavailable"))
		stp.utxoStore = failing

		// An earlier operation's post-commit error, not yet reported.
		stp.diskTxMap.recordErr(errors.NewStorageError("badger write failed"))

		resp := stp.runReset(&resetBlocks{blockHeader: resetTestHeader(9307), postProcess: clean})
		require.Error(t, resp.Err)
		require.False(t, resp.Rotated)
		require.False(t, resp.StorageFailed, "the map error is left over from before this reset")
		require.True(t, stp.TakeResetRequested(), "the map error is joined into the failure and drained, and the rotation that would cure its phantom never ran, so it must still request a reset")
	})
}
