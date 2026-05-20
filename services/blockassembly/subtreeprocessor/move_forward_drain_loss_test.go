package subtreeprocessor

import (
	"context"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// errOnCreateUtxoStore wraps a real utxostore.Store and forces Create to
// return a sentinel error. All other methods delegate to the embedded
// store. Used to deterministically trigger a step-6 failure in
// moveForwardBlock (processCoinbaseUtxos -> utxoStore.Create) AFTER
// step-5 has drained the queue into the new subtree state.
type errOnCreateUtxoStore struct {
	utxostore.Store
	err error
}

func (e *errOnCreateUtxoStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxostore.CreateOption) (*meta.Data, error) {
	return nil, e.err
}

// TestMoveForwardBlockDrainLoss_BatchesLostOnPostDrainError demonstrates
// the rollback gap described in #852: when moveForwardBlock errors AFTER
// processRemainderTransactionsAndDequeue has drained the queue (e.g. at
// processCoinbaseUtxos -> Create), the caller's deferred rollback restores
// the four snapshotted in-memory fields but does NOT restore the queue.
// The drained batches end up nowhere — neither in the queue nor in any
// subtree — and are silently lost.
//
// Reproduction shape:
//
//  1. SubtreeProcessor with utxoStore wrapped to fail on Create.
//  2. Enqueue N batches via stp.queue.enqueueBatch with a clock that
//     makes them old enough to drain (DoubleSpendWindow=0 default).
//  3. Snapshot pre-call state: queue.length, chainedSubtrees,
//     currentSubtree, currentTxMap.
//  4. Call stp.moveForwardBlock directly (bypassing the event-loop chan
//     to avoid a race against a concurrent Start-loop drain).
//  5. Apply the exact production rollback inline (mirrors
//     SubtreeProcessor.go:711-717).
//  6. Assert:
//     - moveForwardBlock returned an error
//     - queue is empty (proves the drain happened)
//     - in-memory snapshot fields are restored
//     - the drained batch's tx hash is in neither the queue nor any
//     subtree -> the batch is lost.
//
// This test PINS the current buggy behaviour. When the underlying fix
// in #852 lands (e.g. reorder side effects, queue snapshot/restore, or
// two-phase dequeue), the post-rollback queue length assertion below
// will need to flip to require.Equal(t, preLen, postLen).
func TestMoveForwardBlockDrainLoss_BatchesLostOnPostDrainError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	// Real sqlitememory store, wrapped so Create errors with a sentinel.
	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	realUtxoStore, err := sql.New(ctx, logger, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	sentinelErr := errors.NewProcessingError("coinbase Create sabotaged by test wrapper")
	utxoStore := &errOnCreateUtxoStore{Store: realUtxoStore, err: sentinelErr}

	// Subtree store pre-populated with the same testdata fixture used by
	// TestMoveForwardBlock_LeftInQueue.
	subtreeStore := blob_memory.New()
	subtreeHash, _ := chainhash.NewHashFromStr("fd61a79793c4fb02ba14b85df98f5f60f727be359089d8fa125c4ce37945106b")
	subtreeBytes, err := os.ReadFile("./testdata/fd61a79793c4fb02ba14b85df98f5f60f727be359089d8fa125c4ce37945106b.subtree")
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree, subtreeBytes))

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 32
	// DoubleSpendWindow stays at the default (0) so the queue filter
	// admits same-millisecond batches at drain time — see #846 and #851.

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

	stp, err := NewSubtreeProcessor(ctx, logger, tSettings, subtreeStore, blockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	// We deliberately do NOT call stp.Start so the event-loop goroutine
	// cannot drain our batches concurrently with the test.

	// currentBlockHeader must equal block.Header.HashPrevBlock, otherwise
	// moveForwardBlock fails the precondition check at line 3583 and
	// returns before reaching the drain.
	stp.currentBlockHeader.Store(model.GenesisBlockHeader)

	// Drain clock 1ms ahead of the enqueue clock so the per-drain cutoff
	// (validFromMillis = drainClock at window=0) admits the enqueued
	// batch. Same construction as TestDequeueDuringBlockMovement_*.
	enqueueAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	drainAt := enqueueAt.Add(1 * time.Millisecond)
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: drainAt}

	// Enqueue a batch directly into the queue. Using a tx hash that is
	// NOT in the block we are about to move forward, so the dequeue path
	// at processRemainderTransactionsAndDequeue admits it (not filtered
	// out by transactionMap / losingTxHashesMap / conflictingHashes).
	queuedTxHash := chainhash.HashH([]byte("queued-tx-852-repro"))
	stp.queue.enqueueBatch(
		[]subtreepkg.Node{{Hash: queuedTxHash, Fee: 1, SizeInBytes: 220}},
		[]*subtreepkg.TxInpoints{{}},
	)
	require.Equal(t, int64(1), stp.queue.length(), "precondition: 1 batch enqueued")

	// Snapshot pre-call state. Mirrors lines 704-707 of the event-loop
	// handler before calling stp.moveForwardBlock.
	originalChainedSubtrees := stp.chainedSubtrees
	originalCurrentSubtree := stp.currentSubtree.Load()
	originalCurrentTxMap := stp.currentTxMap
	originalCurrentBlockHeader := stp.currentBlockHeader.Load()
	preLen := stp.queue.length()

	// Construct the block. Re-uses the hex from TestMoveForwardBlock_LeftInQueue
	// so we know the block parses and the subtree it references is in
	// subtreeStore.
	blockBytes, err := hex.DecodeString("000000206a21d13c3d2656557493b4652f67a763f835b86bf90107a60f412c290000000083ba48026c405d5a4b4d5aa3f10cee9de605a012e9a25f72a19aa9fe123380c689505c67c874461cc6dda18002fde501016b104579e34c5c12fad8899035be27f7605f8ff95db814ba02fbc49397a761fd01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1903af32190000000000205f7c477c327c437c5f200001000000ffffffff01e50b5402000000001976a9147a112f6a373b80b4ebb2b02acef97f35aef7494488ac00000000feaf321900")
	require.NoError(t, err)
	block, err := model.NewBlockFromBytes(blockBytes)
	require.NoError(t, err)
	block.Header.HashPrevBlock = stp.currentBlockHeader.Load().Hash()

	// Drive moveForwardBlock directly. The exported MoveForwardBlock would
	// go through moveForwardBlockChan and the event-loop goroutine, which
	// would race against our enqueue. Calling the unexported method here
	// keeps the test deterministic.
	processedConflictingHashesMap := make(map[chainhash.Hash]bool)
	_, _, mfbErr := stp.moveForwardBlock(ctx, block, false, processedConflictingHashesMap, false, true)
	require.Error(t, mfbErr, "moveForwardBlock must surface the Create failure injected by the wrapper")
	require.ErrorIs(t, mfbErr, sentinelErr, "the error chain must mention the sentinel from errOnCreateUtxoStore")

	// Apply the production rollback inline (mirrors SubtreeProcessor.go:711-717).
	stp.chainedSubtrees = originalChainedSubtrees
	stp.currentSubtree.Store(originalCurrentSubtree)
	stp.currentTxMap = originalCurrentTxMap
	stp.currentBlockHeader.Store(originalCurrentBlockHeader)
	stp.setTxCountFromSubtrees()

	// The in-memory snapshot fields are restored. Verify.
	require.Same(t, originalCurrentSubtree, stp.currentSubtree.Load(),
		"currentSubtree must be restored by the rollback")
	require.Same(t, originalCurrentBlockHeader, stp.currentBlockHeader.Load(),
		"currentBlockHeader must be restored by the rollback")

	// The bug: the queue is NOT restored. processRemainderTransactionsAndDequeue
	// drained the batches into the (now-discarded) new subtree state. They
	// are gone from the queue and nowhere to be found in the rolled-back
	// in-memory subtree state.
	require.Equal(t, int64(0), stp.queue.length(),
		"queue is empty: the drain happened despite moveForwardBlock failing later")
	require.NotEqual(t, preLen, stp.queue.length(),
		"queue length differs from pre-call snapshot: this is the lost-batch evidence (#852)")

	// And the batch is not in any subtree either — chainedSubtrees was
	// rolled back to the empty pre-call snapshot, currentSubtree likewise.
	require.NotContains(t, collectSubtreeHashes(stp), queuedTxHash,
		"the drained batch is not in any subtree after rollback — it is lost (#852)")

	t.Logf("[#852 repro] pre-call queue.length=%d, post-call queue.length=%d, "+
		"lost tx hash=%s — not in queue, not in any subtree after rollback",
		preLen, stp.queue.length(), queuedTxHash.String())
}
