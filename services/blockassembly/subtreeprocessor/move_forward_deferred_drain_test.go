package subtreeprocessor

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// deferredDrainBlockHex is a block with one subtree (fd61a797...), whose
// transactions include 6affcabb...; see TestMoveForwardBlock_LeftInQueue.
const deferredDrainBlockHex = "000000206a21d13c3d2656557493b4652f67a763f835b86bf90107a60f412c290000000083ba48026c405d5a4b4d5aa3f10cee9de605a012e9a25f72a19aa9fe123380c689505c67c874461cc6dda18002fde501016b104579e34c5c12fad8899035be27f7605f8ff95db814ba02fbc49397a761fd01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1903af32190000000000205f7c477c327c437c5f200001000000ffffffff01e50b5402000000001976a9147a112f6a373b80b4ebb2b02acef97f35aef7494488ac00000000feaf321900"

// newDeferredDrainProcessor returns a processor that has not been started, so
// nothing but handleMoveForwardRequest drains its queue, together with the
// foreign block it will be moved forward with and the unbuffered subtree
// announcement channel it reports completed subtrees on.
//
// wrap, when set, wraps the UTXO store the processor uses.
func newDeferredDrainProcessor(t *testing.T, wrap func(utxostore.Store) utxostore.Store, tweaks ...func(*settings.Settings)) (*SubtreeProcessor, *model.Block, chan NewSubtreeRequest) {
	t.Helper()

	return newDeferredDrainProcessorWith(t, wrap, nil, tweaks...)
}

// newDeferredDrainProcessorWith is newDeferredDrainProcessor with processor
// options, e.g. WithTxMapDirs for a disk-backed tx map.
func newDeferredDrainProcessorWith(t *testing.T, wrap func(utxostore.Store) utxostore.Store, opts []Options, tweaks ...func(*settings.Settings)) (*SubtreeProcessor, *model.Block, chan NewSubtreeRequest) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	subtreeStore := blob_memory.New()

	subtreeHash, err := chainhash.NewHashFromStr("fd61a79793c4fb02ba14b85df98f5f60f727be359089d8fa125c4ce37945106b")
	require.NoError(t, err)

	subtreeBytes, err := os.ReadFile("./testdata/fd61a79793c4fb02ba14b85df98f5f60f727be359089d8fa125c4ce37945106b.subtree")
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree, subtreeBytes))

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.DoubleSpendWindow = 0
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4
	tSettings.BlockAssembly.TxMapDirs = nil
	tSettings.BlockAssembly.SubtreeMmapDir = ""

	for _, tweak := range tweaks {
		tweak(tSettings)
	}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	sqlStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	var utxoStore utxostore.Store = sqlStore
	if wrap != nil {
		utxoStore = wrap(sqlStore)
	}

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	newSubtreeChan := make(chan NewSubtreeRequest)

	stp, err := NewSubtreeProcessor(ctx, logger, tSettings, subtreeStore, blockchainClient, utxoStore, newSubtreeChan, opts...)
	require.NoError(t, err)

	stp.currentBlockHeader.Store(model.GenesisBlockHeader)

	blockBytes, err := hex.DecodeString(deferredDrainBlockHex)
	require.NoError(t, err)

	block, err := model.NewBlockFromBytes(blockBytes)
	require.NoError(t, err)

	block.Header.HashPrevBlock = model.GenesisBlockHeader.Hash()

	return stp, block, newSubtreeChan
}

// subtreeHashes returns every tx hash in the processor's chained and current
// subtrees.
func subtreeHashes(stp *SubtreeProcessor) map[chainhash.Hash]struct{} {
	hashes := make(map[chainhash.Hash]struct{})

	for _, st := range stp.chainedSubtrees {
		for _, n := range st.Nodes {
			hashes[n.Hash] = struct{}{}
		}
	}

	for _, n := range stp.currentSubtree.Load().Nodes {
		hashes[n.Hash] = struct{}{}
	}

	return hashes
}

// TestHandleMoveForwardRequest_RespondsBeforeQueueDrain pins that the caller
// of MoveForwardBlock gets its result as soon as the block is applied, and the
// queue that built up while the block was being applied is drained afterwards.
// Block assembly only reports the new tip, and serves mining candidates with
// transactions again, once MoveForwardBlock returns, so the drain must not
// hold that up.
//
// The drain here completes a subtree, and the announcement of that subtree
// blocks on the unbuffered channel until the test reads it. The response must
// arrive while the drain is still blocked there.
func TestHandleMoveForwardRequest_RespondsBeforeQueueDrain(t *testing.T) {
	stp, block, newSubtreeChan := newDeferredDrainProcessor(t, nil)

	inBlock, err := chainhash.NewHashFromStr("6affcabb2013261e764a5d4286b463b11127f4fd1de05368351530ddb3f19942")
	require.NoError(t, err)

	// One tx that is in the block, and enough that are not to complete a
	// 4-leaf subtree (the coinbase placeholder takes the first leaf).
	nodes := []subtreepkg.Node{{Hash: *inBlock, Fee: 1, SizeInBytes: 250}}
	notInBlock := make([]chainhash.Hash, 0, 5)

	for i := 0; i < 5; i++ {
		h := chainhash.HashH([]byte{byte(i), 0xde, 0xfe, 0x22})
		notInBlock = append(notInBlock, h)
		nodes = append(nodes, subtreepkg.Node{Hash: h, Fee: 1, SizeInBytes: 250})
	}

	inpoints := make([]*subtreepkg.TxInpoints, len(nodes))
	for i := range inpoints {
		inpoints[i] = &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{chainhash.HashH([]byte{byte(i), 0x01})}}
	}

	stp.AddBatch(nodes, inpoints)

	// The drain only takes batches enqueued before it starts.
	time.Sleep(10 * time.Millisecond)

	errChan := make(chan error, 1)

	var handlerDone sync.WaitGroup

	handlerDone.Go(func() {
		stp.handleMoveForwardRequest(context.Background(), moveBlockRequest{block: block, errChan: errChan})
	})

	select {
	case err := <-errChan:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("moveForwardBlock result was held back by the queue drain")
	}

	// Block assembly now serves mining candidates on the new tip. With no
	// complete subtree it asks this goroutine for a snapshot of the
	// incomplete one; while the drain runs that must answer at once (empty,
	// as it did while the block was still being applied), not wait out its
	// 5s timeout behind the drain.
	snapshotStart := time.Now()
	require.Nil(t, stp.GetIncompleteSubtreeMiningData(context.Background()))
	require.Less(t, time.Since(snapshotStart), time.Second, "the snapshot request waited behind the deferred drain")

	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	handlerDone.Wait()

	require.False(t, stp.DrainingAfterBlock(), "the drain flag must be cleared once the deferred work is done")

	hashes := subtreeHashes(stp)

	_, found := hashes[*inBlock]
	require.False(t, found, "a queued tx that is in the block must not be re-added")

	for _, h := range notInBlock {
		_, found = hashes[h]
		require.True(t, found, "queued tx %s is not in the block and must be added", h)
	}

	require.Zero(t, stp.queue.length(), "the drain must empty the queue")
	require.Zero(t, stp.currentTxMapShadow.Length(), "the retired tx map half must be cleared after the drain")

	leaves := len(stp.currentSubtree.Load().Nodes)
	for _, st := range stp.chainedSubtrees {
		leaves += len(st.Nodes)
	}

	require.Equal(t, uint64(leaves), stp.TxCount(), "the tx count must include the txs the drain added") //nolint:gosec // small test count
}

// TestHandleMoveForwardRequest_FailedBlockLeavesQueueUntouched pins the #852
// fix on the dispatcher path: the queue is drained only after the block is
// applied, so a block that fails after the leftover pass (here: creating the
// coinbase UTXOs) leaves every queued batch in the queue, instead of having
// drained batches that the rollback then cannot put back.
func TestHandleMoveForwardRequest_FailedBlockLeavesQueueUntouched(t *testing.T) {
	boom := errors.NewStorageError("create failed")

	stp, block, _ := newDeferredDrainProcessor(t, func(s utxostore.Store) utxostore.Store {
		return &errOnCreateUtxoStore{Store: s, err: boom}
	})

	queued := chainhash.HashH([]byte("queued-before-failed-block"))
	stp.AddBatch([]subtreepkg.Node{{Hash: queued, Fee: 1, SizeInBytes: 250}}, []*subtreepkg.TxInpoints{{}})

	// The drain only takes batches enqueued before it starts.
	time.Sleep(10 * time.Millisecond)

	errChan := make(chan error, 1)
	stp.handleMoveForwardRequest(context.Background(), moveBlockRequest{block: block, errChan: errChan})

	require.ErrorIs(t, <-errChan, boom)
	require.Equal(t, int64(1), stp.queue.length(), "a failed block must not drain the queue")

	_, found := subtreeHashes(stp)[queued]
	require.False(t, found, "the queued tx must still be waiting in the queue, not in a subtree")
}

// drainOrder returns the processor's leaves in order and, for every leaf,
// the parent the tx map holds for it.
func drainOrder(t *testing.T, stp *SubtreeProcessor) ([]chainhash.Hash, map[chainhash.Hash]chainhash.Hash) {
	t.Helper()

	var leaves []chainhash.Hash

	for _, st := range stp.chainedSubtrees {
		for _, n := range st.Nodes {
			leaves = append(leaves, n.Hash)
		}
	}

	for _, n := range stp.currentSubtree.Load().Nodes {
		leaves = append(leaves, n.Hash)
	}

	parents := make(map[chainhash.Hash]chainhash.Hash, len(leaves))

	for _, h := range leaves {
		if h.Equal(*subtreepkg.CoinbasePlaceholderHash) {
			continue
		}

		ip, ok := stp.currentTxMap.Get(h)
		require.True(t, ok, "leaf %s has no tx map entry", h)
		require.Len(t, ip.ParentTxHashes, 1)

		parents[h] = ip.ParentTxHashes[0]
	}

	return leaves, parents
}

// TestDrainQueueAfterBlock_MatchesSequentialDrain pins that the parallel drain
// adds exactly what dequeueDuringBlockMovement adds, in the same order and with
// the same inpoints: txs in the block or on the losing side are skipped, and of
// a tx queued twice only the first copy is added, as SetIfNotExists does one
// tx at a time. A 4-leaf subtree size makes the drain cross several subtrees.
func TestDrainQueueAfterBlock_MatchesSequentialDrain(t *testing.T) {
	inBlock := make([]chainhash.Hash, 0, 8)
	for i := 0; i < 8; i++ {
		inBlock = append(inBlock, chainhash.HashH([]byte{byte(i), 'b'}))
	}

	losing := chainhash.HashH([]byte("losing"))

	type queued struct {
		hash   chainhash.Hash
		parent chainhash.Hash
	}

	var queue []queued

	for i := 0; i < 40; i++ {
		queue = append(queue, queued{hash: chainhash.HashH([]byte{byte(i), 'q'}), parent: chainhash.HashH([]byte{byte(i), 'p'})})

		switch i % 7 {
		case 2:
			queue = append(queue, queued{hash: inBlock[i%len(inBlock)], parent: chainhash.HashH([]byte{byte(i), 'x'})})
		case 4:
			// A second copy of an earlier tx, with different inpoints: the
			// first copy must win.
			queue = append(queue, queued{hash: chainhash.HashH([]byte{byte(i / 2), 'q'}), parent: chainhash.HashH([]byte{byte(i), 'd'})})
		case 6:
			queue = append(queue, queued{hash: losing, parent: chainhash.HashH([]byte{byte(i), 'l'})})
		}
	}

	transactionMap := NewSplitSwissMap(4, len(inBlock))
	for _, h := range inBlock {
		require.NoError(t, transactionMap.Put(h))
	}

	transactionMap.Freeze()

	losingMap := txmap.NewSplitSwissMap(4)
	require.NoError(t, losingMap.Put(losing, 0))

	run := func(t *testing.T, disk, parallel bool) ([]chainhash.Hash, map[chainhash.Hash]chainhash.Hash) {
		var opts []Options
		if disk {
			opts = append(opts, WithTxMapDirs([]string{t.TempDir()}))
		}

		stp, _, newSubtreeChan := newDeferredDrainProcessorWith(t, nil, opts)
		if disk {
			require.NotNil(t, stp.diskTxMap, "precondition: the processor uses a disk tx map")

			t.Cleanup(func() {
				for _, m := range []*DiskTxMap{stp.diskTxMap, stp.diskTxMapShadow} {
					if m != nil {
						_ = m.Close()
					}
				}
			})
		}

		go func() {
			for req := range newSubtreeChan {
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			}
		}()
		t.Cleanup(func() { close(newSubtreeChan) })

		// Several batches, as the queue holds them.
		for start := 0; start < len(queue); start += 5 {
			end := min(start+5, len(queue))

			nodes := make([]subtreepkg.Node, 0, end-start)
			inpoints := make([]*subtreepkg.TxInpoints, 0, end-start)

			for _, q := range queue[start:end] {
				nodes = append(nodes, subtreepkg.Node{Hash: q.hash, Fee: 1, SizeInBytes: 250})
				// One parent, vout 0: inpoints the disk tx map can serialize.
				ip := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{q.parent}, []uint32{1, 0})
				inpoints = append(inpoints, &ip)
			}

			stp.AddBatch(nodes, inpoints)
		}

		// The drain only takes batches enqueued before it starts.
		time.Sleep(10 * time.Millisecond)

		if parallel {
			require.NoError(t, stp.drainQueueAfterBlock(context.Background(), &deferredBlockDrain{
				drainQueue:        true,
				transactionMap:    transactionMap,
				losingTxHashesMap: losingMap,
			}))
		} else {
			require.NoError(t, stp.dequeueDuringBlockMovement(transactionMap, losingMap, nil, false))
		}

		require.Zero(t, stp.queue.length())

		return drainOrder(t, stp)
	}

	// Small chunks, so the parallel drain crosses several of them.
	defer func(n int) { drainChunkItems = n }(drainChunkItems)
	drainChunkItems = 7

	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("diskTxMap=%t", disk), func(t *testing.T) {
			wantLeaves, wantParents := run(t, disk, false)
			gotLeaves, gotParents := run(t, disk, true)

			require.Greater(t, len(wantLeaves), 8, "the drain must cross several 4-leaf subtrees")
			require.Equal(t, wantLeaves, gotLeaves, "same txs in the same order")
			require.Equal(t, wantParents, gotParents, "same inpoints for every tx (the first queued copy wins)")
		})
	}
}

// enqueueDrainTest queues one 1-tx batch per hash, each with the given
// parent, and waits until the drain will take them.
func enqueueDrainTest(stp *SubtreeProcessor, parent chainhash.Hash, hashes ...chainhash.Hash) {
	for _, h := range hashes {
		stp.AddBatch([]subtreepkg.Node{{Hash: h, Fee: 1, SizeInBytes: 250}}, []*subtreepkg.TxInpoints{{ParentTxHashes: []chainhash.Hash{parent}}})
	}

	time.Sleep(10 * time.Millisecond)
}

func emptyBlockTxMap() *SplitSwissMap {
	m := NewSplitSwissMap(4, 1)
	m.Freeze()

	return m
}

// TestForEachDrainChunk_DequeuesAsItGoes pins that the drain takes batches
// off the queue chunk by chunk, so the queue length (which backpressure
// reads) keeps counting what is still waiting.
func TestForEachDrainChunk_DequeuesAsItGoes(t *testing.T) {
	stp, _, _ := newDeferredDrainProcessor(t, nil)

	for i := 0; i < 6; i++ {
		enqueueDrainTest(stp, chainhash.Hash{}, chainhash.HashH([]byte{byte(i), 'c'}))
	}

	var seen []int64

	require.NoError(t, stp.forEachDrainChunk(2, func(batches []*TxBatch) error {
		seen = append(seen, stp.queue.length())
		return nil
	}))

	require.Equal(t, []int64{4, 2, 0}, seen, "each chunk must leave the rest in the queue")
}

// TestDrainQueueAfterBlock_ConflictingHashesFallBackToSequential pins the
// fallback: with conflicting hashes, a queued child of a conflicting parent
// is dropped, as dequeueDuringBlockMovement does.
func TestDrainQueueAfterBlock_ConflictingHashesFallBackToSequential(t *testing.T) {
	stp, _, newSubtreeChan := newDeferredDrainProcessor(t, nil)
	t.Cleanup(func() { close(newSubtreeChan) })

	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	conflicting := chainhash.HashH([]byte("conflicting-parent"))
	child := chainhash.HashH([]byte("child-of-conflicting"))
	other := chainhash.HashH([]byte("unrelated"))

	enqueueDrainTest(stp, conflicting, child)
	enqueueDrainTest(stp, chainhash.HashH([]byte("fine-parent")), other)

	require.NoError(t, stp.drainQueueAfterBlock(context.Background(), &deferredBlockDrain{
		drainQueue:        true,
		transactionMap:    emptyBlockTxMap(),
		conflictingHashes: map[chainhash.Hash]struct{}{conflicting: {}},
	}))

	hashes := subtreeHashes(stp)

	_, found := hashes[child]
	require.False(t, found, "a child of a conflicting parent must be dropped")

	_, found = hashes[other]
	require.True(t, found, "an unrelated tx must be added")
}

// TestDrainQueueAfterBlock_NilInpointsFallBackToSequential pins the other
// fallback: a queued tx without inpoints is added only if the tx map already
// holds it, as addNode requires, and the rest of the chunk is still added.
func TestDrainQueueAfterBlock_NilInpointsFallBackToSequential(t *testing.T) {
	stp, _, newSubtreeChan := newDeferredDrainProcessor(t, nil)
	t.Cleanup(func() { close(newSubtreeChan) })

	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	known := chainhash.HashH([]byte("known-nil-inpoints"))
	unknown := chainhash.HashH([]byte("unknown-nil-inpoints"))
	regular := chainhash.HashH([]byte("regular"))

	stp.currentTxMap.SetIfNotExists(known, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{{1}}})

	stp.AddBatch([]subtreepkg.Node{{Hash: known, Fee: 1, SizeInBytes: 250}, {Hash: unknown, Fee: 1, SizeInBytes: 250}}, []*subtreepkg.TxInpoints{nil, nil})
	enqueueDrainTest(stp, chainhash.Hash{2}, regular)

	require.NoError(t, stp.drainQueueAfterBlock(context.Background(), &deferredBlockDrain{
		drainQueue:     true,
		transactionMap: emptyBlockTxMap(),
	}))

	hashes := subtreeHashes(stp)

	for h, want := range map[chainhash.Hash]bool{known: true, unknown: false, regular: true} {
		_, found := hashes[h]
		require.Equal(t, want, found, "tx %s", h)
	}
}

// TestRunDeferredBlockDrain_FailureRequestsReset pins that a failure in the
// deferred drain, which has no rollback, asks block assembly for a reset and
// still clears the retired tx map half.
func TestRunDeferredBlockDrain_FailureRequestsReset(t *testing.T) {
	stp, block, _ := newDeferredDrainProcessor(t, nil)

	enqueueDrainTest(stp, chainhash.Hash{3}, chainhash.HashH([]byte("queued-before-failure")))

	stp.currentTxMapShadow.SetIfNotExists(chainhash.HashH([]byte("retired")), &subtreepkg.TxInpoints{})

	// A cancelled context (shutdown) makes parallelBuildRemainderSubtrees fail.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stp.runDeferredBlockDrain(ctx, block, &deferredBlockDrain{drainQueue: true, transactionMap: emptyBlockTxMap()})

	require.True(t, stp.TakeDrainResetRequested(), "a failed deferred drain must request a reset")
	require.False(t, stp.TakeResetRequested(), "a drain failure is not a disk tx map storage error")
	require.Zero(t, stp.currentTxMapShadow.Length(), "the retired half must still be cleared")
}

// TestHandleMoveForwardRequest_ServesLeftoverSnapshotDuringDrain pins what a
// mining candidate sees while the deferred drain runs: with no complete
// subtree, block assembly asks for the incomplete one, and that must answer
// at once with the txs the block left in it (not nil, which gave callers an
// empty block, and not after the drain, which made them wait). The copy is
// stored, and announced, only once someone asks for it.
func TestHandleMoveForwardRequest_ServesLeftoverSnapshotDuringDrain(t *testing.T) {
	stp, block, newSubtreeChan := newDeferredDrainProcessor(t, nil)

	// A tx already in block assembly and not in the block: it stays in the
	// open subtree after the leftover pass.
	leftover := subtreepkg.Node{Hash: chainhash.HashH([]byte("leftover")), Fee: 1, SizeInBytes: 250}
	require.NoError(t, stp.AddDirectly(&leftover, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{{9}}}, true))

	// Enough queued txs that the drain completes a subtree, whose
	// announcement then blocks until the gate opens.
	for i := 0; i < 5; i++ {
		enqueueDrainTest(stp, chainhash.Hash{8}, chainhash.HashH([]byte{byte(i), 'g'}))
	}

	gate := make(chan struct{})

	var incompleteStored atomic.Int32

	go func() {
		for req := range newSubtreeChan {
			if req.Subtree != nil && req.Subtree.IsComplete() {
				<-gate
			} else {
				incompleteStored.Add(1)
			}

			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	errChan := make(chan error, 1)

	var handlerDone sync.WaitGroup

	handlerDone.Go(func() {
		stp.handleMoveForwardRequest(context.Background(), moveBlockRequest{block: block, errChan: errChan})
	})

	select {
	case err := <-errChan:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("moveForwardBlock result was held back by the queue drain")
	}

	require.Zero(t, incompleteStored.Load(), "the snapshot must not be stored until a caller asks for it")

	start := time.Now()
	data := stp.GetIncompleteSubtreeMiningData(context.Background())
	require.Less(t, time.Since(start), time.Second, "the snapshot request waited behind the deferred drain")
	require.True(t, stp.DrainingAfterBlock(), "precondition: the drain is still running")

	require.NotNil(t, data, "the leftover txs must be served while the drain runs")
	require.Len(t, data.Subtrees, 1)
	require.True(t, data.Subtrees[0].HasNode(leftover.Hash), "the snapshot must hold the leftover tx")
	require.Equal(t, block.Header.Hash(), data.PreviousHeader.Hash(), "the snapshot must build on the new block")

	require.Same(t, data, stp.GetIncompleteSubtreeMiningData(context.Background()), "later callers get the same snapshot")
	require.Equal(t, int32(1), incompleteStored.Load(), "the snapshot is stored once, when first asked for")

	close(gate)
	handlerDone.Wait()
}

// TestStartLoop_NextRequestWaitsForDeferredDrain pins the ordering the
// deferral relies on, through the real Start loop: MoveForwardBlock returns
// before the queue drain, but the processing goroutine takes no other request
// until the drain is done, so nothing (a reorg, a reset, a snapshot) sees it
// half done.
//
// The loop is first held storing an on-demand snapshot, with the
// MoveForwardBlock request waiting behind it, which is when the test queues
// the txs; that keeps the loop's own dequeue from taking them. The drain then completes two subtrees. Announcing one does not wait
// for its acknowledgement, only for the receiver, so the reader holds the
// first and the drain blocks sending the second. A GetCurrentLength sent
// meanwhile must wait for it.
func TestStartLoop_NextRequestWaitsForDeferredDrain(t *testing.T) {
	// No periodic announcement: it would make the loop send on the held
	// channel for a reason that has nothing to do with the drain.
	stp, block, newSubtreeChan := newDeferredDrainProcessor(t, nil, func(s *settings.Settings) {
		s.BlockAssembly.SubtreeAnnouncementInterval = time.Hour
	})

	leftover := subtreepkg.Node{Hash: chainhash.HashH([]byte("leftover-start")), Fee: 1, SizeInBytes: 250}
	require.NoError(t, stp.AddDirectly(&leftover, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{{7}}}, true))

	snapshotSeen, snapshotGate := make(chan struct{}), make(chan struct{})
	drainSeen, drainGate := make(chan struct{}), make(chan struct{})

	var snapshotOnce, drainOnce sync.Once

	go func() {
		for req := range newSubtreeChan {
			if req.Subtree != nil && req.Subtree.IsComplete() {
				drainOnce.Do(func() { close(drainSeen) })
				<-drainGate
			} else {
				snapshotOnce.Do(func() { close(snapshotSeen) })
				<-snapshotGate
			}

			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stp.Start(ctx)

	go func() { _ = stp.GetIncompleteSubtreeMiningData(ctx) }()

	select {
	case <-snapshotSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop never started storing the on-demand snapshot")
	}

	moveDone := make(chan error, 1)
	go func() { moveDone <- stp.MoveForwardBlock(block) }()

	// Let the MoveForwardBlock request reach the loop's channel.
	time.Sleep(50 * time.Millisecond)

	// With the coinbase placeholder and the leftover, 9 txs fill two 4-leaf
	// subtrees and start a third.
	for i := 0; i < 9; i++ {
		enqueueDrainTest(stp, chainhash.Hash{6}, chainhash.HashH([]byte{byte(i), 's'}))
	}

	close(snapshotGate)

	select {
	case err := <-moveDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("MoveForwardBlock did not return before the drain")
	}

	select {
	case <-drainSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("the deferred drain never completed a subtree")
	}

	// It is the deferred drain, not the loop's own dequeue, that holds them.
	require.True(t, stp.DrainingAfterBlock(), "the queued txs must be drained by the deferred drain")

	lengthDone := make(chan struct{})
	go func() {
		_ = stp.GetCurrentLength()
		close(lengthDone)
	}()

	require.Never(t, func() bool {
		select {
		case <-lengthDone:
			return true
		default:
			return false
		}
	}, 200*time.Millisecond, 10*time.Millisecond, "a request was served while the deferred drain was still running")

	close(drainGate)

	require.Eventually(t, func() bool {
		select {
		case <-lengthDone:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond, "the request must be served once the drain is done")

	require.False(t, stp.DrainingAfterBlock())
	require.Zero(t, stp.queue.length(), "the drain took the queued txs")
}

// newAfterBlockSnapshotProcessor leaves a processor as handleMoveForwardRequest
// does while the drain runs: an after-block snapshot holding one leftover tx,
// not yet stored. Every store request is handed to answer, on its own
// goroutine.
func newAfterBlockSnapshotProcessor(t *testing.T, answer func(req NewSubtreeRequest)) (*SubtreeProcessor, *PrecomputedMiningData) {
	t.Helper()

	stp, _, newSubtreeChan := newDeferredDrainProcessor(t, nil)

	leftover := subtreepkg.Node{Hash: chainhash.HashH([]byte("leftover-retry")), Fee: 1, SizeInBytes: 250}
	require.NoError(t, stp.AddDirectly(&leftover, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{{5}}}, true))

	data := stp.incompleteSubtreeSnapshot()
	require.NotNil(t, data)

	stp.afterBlockMiningData.Store(&afterBlockSnapshot{data: data, parentTxMap: stp.currentTxMap})
	stp.drainingAfterBlock.Store(true)

	go func() {
		for req := range newSubtreeChan {
			go answer(req)
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	return stp, data
}

// TestGetIncompleteSubtreeMiningData_WaitsOnAbandonedSnapshotStore pins that
// a caller who gives up while the after-block snapshot is being stored does
// not leave later callers without it, and that a later caller waits on that
// same store instead of sending the subtree again (a second store of the
// same hash would be announced twice).
func TestGetIncompleteSubtreeMiningData_WaitsOnAbandonedSnapshotStore(t *testing.T) {
	storeGate := make(chan struct{})

	var stores atomic.Int32

	stp, data := newAfterBlockSnapshotProcessor(t, func(req NewSubtreeRequest) {
		stores.Add(1)
		<-storeGate
		req.ErrChan <- nil // buffered: never blocks, even for a caller that gave up
	})

	shortCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	require.Nil(t, stp.GetIncompleteSubtreeMiningData(shortCtx), "the store did not finish within the first caller's deadline")

	close(storeGate)

	require.Same(t, data, stp.GetIncompleteSubtreeMiningData(context.Background()), "a later caller gets the snapshot once that store finishes")
	require.Same(t, data, stp.GetIncompleteSubtreeMiningData(context.Background()))
	require.Equal(t, int32(1), stores.Load(), "the subtree is stored once; nobody sends it again")
}

// TestGetIncompleteSubtreeMiningData_RetriesFailedSnapshotStore pins that a
// store that fails is not taken as stored: the next caller stores again.
func TestGetIncompleteSubtreeMiningData_RetriesFailedSnapshotStore(t *testing.T) {
	var stores atomic.Int32

	stp, data := newAfterBlockSnapshotProcessor(t, func(req NewSubtreeRequest) {
		if stores.Add(1) == 1 {
			req.ErrChan <- errors.NewStorageError("blob store unavailable")
			return
		}

		req.ErrChan <- nil
	})

	require.Nil(t, stp.GetIncompleteSubtreeMiningData(context.Background()), "a failed store must not be served")
	require.Same(t, data, stp.GetIncompleteSubtreeMiningData(context.Background()), "the next caller stores again")
	require.Equal(t, int32(2), stores.Load())
}
