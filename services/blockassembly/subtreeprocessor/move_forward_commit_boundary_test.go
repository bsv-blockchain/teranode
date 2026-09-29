package subtreeprocessor

import (
	"context"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newMoveForwardCommitBoundaryProcessor builds a DiskTxMap-backed SubtreeProcessor
// with the same fixture block/subtree used by TestMoveForwardBlock_LeftInQueue and
// TestMoveForwardBlockDrainLoss_BatchesLostOnPostDrainError, but not started (no
// dispatcher goroutine), so moveForwardBlock can be called directly and its
// in-memory state inspected synchronously without racing anything.
func newMoveForwardCommitBoundaryProcessor(t *testing.T) (*SubtreeProcessor, *model.Block) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blob_memory.New()
	subtreeHash, _ := chainhash.NewHashFromStr("fd61a79793c4fb02ba14b85df98f5f60f727be359089d8fa125c4ce37945106b")
	subtreeBytes, err := os.ReadFile("./testdata/fd61a79793c4fb02ba14b85df98f5f60f727be359089d8fa125c4ce37945106b.subtree")
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree, subtreeBytes))

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 32

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	newSubtreeChan := make(chan NewSubtreeRequest, 16)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, logger, tSettings, subtreeStore, blockchainClient, utxoStore, newSubtreeChan,
		WithTxMapDirs([]string{t.TempDir(), t.TempDir()}))
	require.NoError(t, err)

	t.Cleanup(func() {
		halves := make([]*DiskTxMap, 0, 3+len(stp.diskTxMapRetired))
		halves = append(halves, stp.diskTxMap, stp.diskTxMapShadow, stp.diskTxMapAnchor)
		halves = append(halves, stp.diskTxMapRetired...)
		closed := make(map[*DiskTxMap]struct{})
		for _, half := range halves {
			if half == nil {
				continue
			}
			if _, done := closed[half]; done {
				continue
			}
			closed[half] = struct{}{}
			_ = half.Close()
		}
	})

	stp.currentBlockHeader.Store(model.GenesisBlockHeader)

	blockBytes, err := hex.DecodeString("000000206a21d13c3d2656557493b4652f67a763f835b86bf90107a60f412c290000000083ba48026c405d5a4b4d5aa3f10cee9de605a012e9a25f72a19aa9fe123380c689505c67c874461cc6dda18002fde501016b104579e34c5c12fad8899035be27f7605f8ff95db814ba02fbc49397a761fd01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1903af32190000000000205f7c477c327c437c5f200001000000ffffffff01e50b5402000000001976a9147a112f6a373b80b4ebb2b02acef97f35aef7494488ac00000000feaf321900")
	require.NoError(t, err)
	block, err := model.NewBlockFromBytes(blockBytes)
	require.NoError(t, err)
	block.Header.HashPrevBlock = stp.currentBlockHeader.Load().Hash()

	return stp, block
}

// A disk tx map error recorded before moveForwardBlock's commit point
// (clearCurrentTxMapShadow) must fail the call and leave the processor's
// in-memory state exactly as it was, so a retry of the same block succeeds -
// map errors fail the operation only where the existing rollback path can
// still run. Goes through the public MoveForwardBlock
// entry point (started processor, dispatcher goroutine) so this exercises the
// real production rollback() closure in SubtreeProcessor.go's
// moveForwardBlockChan handler, not a hand-rolled stand-in for it: a
// regression that breaks or removes that rollback fails this test.
func TestMoveForwardBlock_DiskTxMapErrorBeforeCommitFailsAndRollsBack(t *testing.T) {
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	originalChainedSubtrees := stp.chainedSubtrees
	originalCurrentSubtree := stp.currentSubtree.Load()
	originalCurrentTxMap := stp.currentTxMap
	originalHeader := stp.currentBlockHeader.Load()

	boom := errors.NewStorageError("badger write failed")
	stp.diskTxMap.recordErr(boom)

	stp.Start(context.Background())

	err := stp.MoveForwardBlock(block)
	require.ErrorIs(t, err, boom, "the pending map error must fail this call")

	require.Same(t, originalCurrentSubtree, stp.currentSubtree.Load(), "currentSubtree must be unchanged")
	require.Same(t, originalHeader, stp.currentBlockHeader.Load(), "currentBlockHeader must be unchanged")
	require.Equal(t, originalChainedSubtrees, stp.chainedSubtrees, "chainedSubtrees must be unchanged")
	requireSameMap(t, originalCurrentTxMap, stp.currentTxMap, "currentTxMap must be unchanged")

	require.NoError(t, stp.diskTxMapErr(), "the error was consumed by the failed call, not left pending")
	require.True(t, stp.TakeResetRequested(), "a pre-commit failure still requests a reset: the drained error may be an earlier write that never reached disk, whose phantom the rollback keeps")

	// Retry: the same block, now with no map error pending, must succeed.
	retryErr := stp.MoveForwardBlock(block)
	require.NoError(t, retryErr, "a retry after the transient error clears must succeed")
}

// A disk tx map error that only surfaces at or after moveForwardBlock's
// commit point (simulated here as arriving after the pre-commit check has
// already run clean, e.g. from the Clear() rotation inside
// clearCurrentTxMapShadow) must not fail the call: the block is already
// applied, and failing it now would desync a caller that treats the error as
// "not applied" from the state that already reflects it. It is reported by
// the caller's own post-commit drain instead (see
// TestSubtreeProcessorDispatcher tests / reorgBlocks per-iteration drains for
// that half of the contract).
func TestMoveForwardBlock_DiskTxMapErrorAfterCommitDoesNotFailTheCall(t *testing.T) {
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	// Force clearCurrentTxMapShadow's own Clear() to fail. Both diskTxMap and
	// diskTxMapShadow are created from the same stp.txMapDirs list (only their
	// Badger prefix differs), so sealing basePaths[0] here blocks Clear's
	// rotation on EITHER half — including the one that resetSubtreeState is
	// about to swap into diskTxMapShadow and clearCurrentTxMapShadow then
	// tries to rotate. Clear cannot create a replacement generation, so it
	// records the rotation failure via recordErr instead of rotating.
	shadowDir := stp.diskTxMapShadow.basePaths[0]
	sealDir(t, shadowDir)

	preCallTxMap := stp.currentTxMap

	processedConflictingHashesMap := make(map[chainhash.Hash]struct{})
	_, _, err := stp.moveForwardBlock(context.Background(), block, false, processedConflictingHashesMap, false, true)
	require.NoError(t, err, "a post-commit map error must not fail an already-applied block")

	require.NotSame(t, preCallTxMap, stp.currentTxMap, "moveForwardBlock's own commit (the double-buffer swap) is unaffected by the post-commit error")

	mapErr := stp.diskTxMapErr()
	require.Error(t, mapErr, "the Clear rotation failure is still pending for the caller to log")
}

// dequeueDuringBlockMovement has already drained the queue by the time this
// final check runs: rollback() does not requeue what it drained (see #852),
// so a flush failure for that pass's own writes must not fail moveForwardBlock
// - doing so would lose the drained batches permanently. The dequeue drain is
// this block's own commit point: the error is logged, counted, and a reset is
// requested (to reload from the UTXO store and cure the resulting phantom),
// but the call still succeeds and the double-buffer swap still commits.
func TestMoveForwardBlock_DequeueOwnWriteFailure_LogsCountsRequestsResetButSucceeds(t *testing.T) {
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	// Prime the queue with a tx NOT in the block, so processRemainderTransactionsAndDequeue's
	// dequeue pass writes it into the new current map via addNode -> SetIfNotExists.
	// Same construction as TestMoveForwardBlockDrainLoss_BatchesLostOnPostDrainError.
	enqueueAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	drainAt := enqueueAt.Add(1 * time.Millisecond)
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: drainAt}

	queuedTxHash := chainhash.HashH([]byte("queued-tx-flush-before-check"))
	stp.queue.enqueueBatch(
		[]subtreepkg.Node{{Hash: queuedTxHash, Fee: 1, SizeInBytes: 220}},
		[]*subtreepkg.TxInpoints{{}},
	)
	require.Equal(t, int64(1), stp.queue.length(), "precondition: 1 batch enqueued")

	// resetSubtreeState swaps diskTxMap <-> diskTxMapShadow, so the pre-swap
	// shadow becomes the new current map that the dequeued write above lands
	// in. Installed on every disk shard, not just disk 0, so the test does
	// not depend on which disk queuedTxHash happens to shard to.
	for i := range stp.diskTxMapShadow.disks {
		stp.diskTxMapShadow.disks[i].batch = &failingBatch{flushErr: errors.NewStorageError("flush failed")}
	}

	originalCurrentTxMap := stp.currentTxMap

	before := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("moveForwardBlock_postDequeue"))

	processedConflictingHashesMap := make(map[chainhash.Hash]struct{})
	_, _, err := stp.moveForwardBlock(context.Background(), block, false, processedConflictingHashesMap, false, true)
	require.NoError(t, err, "a flush failure for the dequeue pass's own writes must not fail moveForwardBlock: the queue is already drained and rollback cannot requeue it")

	require.NotSame(t, originalCurrentTxMap, stp.currentTxMap, "moveForwardBlock must still commit: the double-buffer swap is not rolled back")
	require.Equal(t, int64(0), stp.queue.length(), "the queue was drained and must not be replayed")

	after := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("moveForwardBlock_postDequeue"))
	require.Equal(t, before+1, after, "the dequeue pass's own flush failure must be logged and counted")

	require.True(t, stp.TakeResetRequested(), "a post-commit storage error from the dequeue pass must request a reset to cure the resulting phantom")
}

// The own-block path (all of the block's subtrees are already in
// stp.chainedSubtrees) never dequeues - processOwnBlockNodes only reads the
// old half and SetIfNotExists's into the new one - so unlike the dequeue
// pass, a disk tx map error here is still pre-commit and rollback is fully
// safe. It must fail moveForwardBlock, not be logged as
// moveForwardBlock_postDequeue and committed with a phantom.
func TestMoveForwardBlock_OwnBlockPreCommitErrorFailsAndRollsBack(t *testing.T) {
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	for i := 0; i < 40; i++ {
		n := subtreepkg.Node{Hash: chainhash.HashH([]byte{byte(i), 7}), Fee: 1, SizeInBytes: 100}
		require.NoError(t, stp.AddDirectly(&n, &subtreepkg.TxInpoints{}, true))
	}

	require.NotEmpty(t, stp.chainedSubtrees, "precondition: enough txs to chain at least one subtree")

	// Make this block's Subtrees exactly our own chained subtree, so
	// processBlockSubtrees clears blockSubtreesMap entirely and moveForwardBlock
	// takes the own-block (else) branch, not the foreign-block dequeue path.
	root := stp.chainedSubtrees[0].RootHash()
	block.Subtrees = []*chainhash.Hash{root}

	stp.flushDiskTxMapWriters()
	require.NoError(t, stp.diskTxMapErr(), "precondition: no error pending before the call under test")

	originalCurrentTxMap := stp.currentTxMap

	boom := errors.NewStorageError("badger write failed")
	stp.diskTxMap.recordErr(boom)

	_, _, err := stp.moveForwardBlock(context.Background(), block, true, map[chainhash.Hash]struct{}{}, false, true)
	require.ErrorIs(t, err, boom, "a pre-commit map error on the own-block path (no dequeue) must fail moveForwardBlock")

	requireSameMap(t, originalCurrentTxMap, stp.currentTxMap, "the double-buffer swap must be rolled back: nothing here is post-commit")
	require.NoError(t, stp.diskTxMapErr(), "the error was consumed by the failed call, not left pending")
	require.True(t, stp.TakeResetRequested(), "a pre-commit failure still requests a reset: the drained error may be an earlier write that never reached disk, whose phantom the rollback keeps")
}

// Pins the actual P0 fix through the public entry point: a started processor
// (dispatcher goroutine running) that hits a post-commit disk tx map error
// must still report success to the caller and advance STP's header, with the
// error logged and counted via disk_tx_map_errors_total{where="moveForwardBlock_commit"} -
// the dispatcher-level drain added in SubtreeProcessor.go's moveForwardBlockChan
// case, not moveForwardBlock's own internal handling (already covered by
// TestMoveForwardBlock_DiskTxMapErrorAfterCommitDoesNotFailTheCall, which calls
// moveForwardBlock directly and so cannot see whether the dispatcher's own
// drain runs).
func TestMoveForwardBlock_Started_PostCommitMapErrorIsLoggedAndCountedButSucceeds(t *testing.T) {
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	// Force clearCurrentTxMapShadow's own Clear() to fail (see
	// TestMoveForwardBlock_DiskTxMapErrorAfterCommitDoesNotFailTheCall for why
	// sealing basePaths[0] hits whichever half Clear rotates).
	sealDir(t, stp.diskTxMapShadow.basePaths[0])

	stp.Start(context.Background())

	before := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("moveForwardBlock_commit"))

	err := stp.MoveForwardBlock(block)
	require.NoError(t, err, "a post-commit map error must not fail an already-applied block, even through the public dispatcher path")

	require.Equal(t, block.Header.Hash(), stp.GetCurrentBlockHeader().Hash(), "STP must have advanced to the new block")

	after := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("moveForwardBlock_commit"))
	require.Equal(t, before+1, after, "the post-commit error must be logged and counted by the dispatcher's own drain")
}

// The second, cheaper pre-commit check between processRemainderTxHashes and
// dequeueDuringBlockMovement narrows the #852 queue-drain exposure: a flush
// failure for a remainder tx's re-add (a pre-existing mempool tx not in this
// block, carried over into the new current map) must fail moveForwardBlock
// BEFORE dequeueDuringBlockMovement runs, so the queue is never drained in
// the first place - not drained-then-lost-on-rollback.
func TestMoveForwardBlock_RemainderCheckBeforeDequeue_QueueUntouchedOnFailure(t *testing.T) {
	stp, block := newMoveForwardCommitBoundaryProcessor(t)

	// Pre-existing mempool tx not in the block: becomes "remainder", carried
	// over into the new current map by processRemainderTxHashes.
	remainderNode := subtreepkg.Node{Hash: chainhash.HashH([]byte("remainder-tx")), Fee: 1, SizeInBytes: 100}
	require.NoError(t, stp.AddDirectly(&remainderNode, &subtreepkg.TxInpoints{}, true))

	// A separate queued tx, to observe whether dequeueDuringBlockMovement ran.
	enqueueAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	drainAt := enqueueAt.Add(1 * time.Millisecond)
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: drainAt}

	queuedTxHash := chainhash.HashH([]byte("queued-tx-remainder-check"))
	stp.queue.enqueueBatch(
		[]subtreepkg.Node{{Hash: queuedTxHash, Fee: 1, SizeInBytes: 220}},
		[]*subtreepkg.TxInpoints{{}},
	)
	require.Equal(t, int64(1), stp.queue.length(), "precondition: 1 batch enqueued")

	// resetSubtreeState swaps diskTxMap <-> diskTxMapShadow, so the pre-swap
	// shadow becomes the new current map that processRemainderTxHashes' write
	// (re-adding remainderNode) lands in. Installed on every disk shard,
	// since which one remainderNode's hash shards to is not under this test's
	// control.
	for i := range stp.diskTxMapShadow.disks {
		stp.diskTxMapShadow.disks[i].batch = &failingBatch{flushErr: errors.NewStorageError("flush failed")}
	}

	processedConflictingHashesMap := make(map[chainhash.Hash]struct{})
	_, _, err := stp.moveForwardBlock(context.Background(), block, false, processedConflictingHashesMap, false, true)
	require.Error(t, err, "a remainder-pass write flush failure must fail the call before dequeue runs")
	require.ErrorContains(t, err, "disk tx map storage error before dequeue", "must be this check's own error, not some unrelated failure")
	require.ErrorContains(t, err, "flush failed", "and must actually be the flush failure")

	require.Equal(t, int64(1), stp.queue.length(), "dequeueDuringBlockMovement must never have run: the queue is untouched")
}
