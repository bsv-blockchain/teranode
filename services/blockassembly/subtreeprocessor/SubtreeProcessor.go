package subtreeprocessor

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"log"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type Job struct {
	ID              *chainhash.Hash
	Subtrees        []*util.Subtree
	MiningCandidate *model.MiningCandidate
}

type NewSubtreeRequest struct {
	Subtree *util.Subtree
	ErrChan chan error
}

type moveBlockRequest struct {
	block   *model.Block
	errChan chan error
}
type reorgBlocksRequest struct {
	moveDownBlocks []*model.Block
	moveUpBlocks   []*model.Block
	errChan        chan error
}

type resetBlocks struct {
	blockHeader    *model.BlockHeader
	moveDownBlocks []*model.Block
	moveUpBlocks   []*model.Block
	responseCh     chan ResetResponse
	isLegacySync   bool
}

type ResetResponse struct {
	Err error
}

type SubtreeProcessor struct {
	currentItemsPerFile       int
	txChan                    chan *[]txIDAndFee
	getSubtreesChan           chan chan []*util.Subtree
	moveUpBlockChan           chan moveBlockRequest
	reorgBlockChan            chan reorgBlocksRequest
	deDuplicateTransactionsCh chan struct{}
	resetCh                   chan *resetBlocks
	removeTxCh                chan chainhash.Hash
	newSubtreeChan            chan NewSubtreeRequest // used to notify of a new subtree
	chainedSubtrees           []*util.Subtree        // TODO change this to use badger under the hood, so we can scale beyond RAM
	chainedSubtreeCount       atomic.Int32
	currentSubtree            *util.Subtree
	currentBlockHeader        *model.BlockHeader
	sync.Mutex
	txCount                   atomic.Uint64
	batcher                   *txIDAndFeeBatch
	queue                     *LockFreeQueue
	removeMap                 *util.SwissMap
	doubleSpendWindowDuration time.Duration
	subtreeStore              blob.Store
	utxoStore                 utxostore.Store
	logger                    ulogger.Logger
	stats                     *gocore.Stat
	currentRunningState       atomic.Value
}

var (
	ExpectedNumberOfSubtrees = 1024 // this is the number of subtrees we expect to be in a block, with a subtree create about every second
)

func NewSubtreeProcessor(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store, utxoStore utxostore.Store,
	newSubtreeChan chan NewSubtreeRequest, options ...Options) (*SubtreeProcessor, error) {
	initPrometheusMetrics()

	initialItemsPerFile, _ := gocore.Config().GetInt("initial_merkle_items_per_subtree", 1_048_576)

	firstSubtree, err := util.NewTreeByLeafCount(initialItemsPerFile)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("error creating first subtree", err)
	}
	// We add a placeholder for the coinbase tx because we know this is the first subtree in the chain
	if err := firstSubtree.AddCoinbaseNode(); err != nil {
		return nil, errors.NewInvalidArgumentError("error adding coinbase placeholder to first subtree", err)
	}

	txChanBufferSize := 100_000
	if settingsBufferSize, ok := gocore.Config().GetInt("tx_chan_buffer_size", 0); ok {
		txChanBufferSize = settingsBufferSize
	}

	batcherSize := 1000
	if settingsBufferSize, ok := gocore.Config().GetInt("blockassembly_subtreeProcessorBatcherSize", 1000); ok {
		batcherSize = settingsBufferSize
	}

	doubleSpendWindowMillis, _ := gocore.Config().GetInt("double_spend_window_millis", 2000)

	queue := NewLockFreeQueue()

	stp := &SubtreeProcessor{
		currentItemsPerFile:       initialItemsPerFile,
		txChan:                    make(chan *[]txIDAndFee, txChanBufferSize),
		getSubtreesChan:           make(chan chan []*util.Subtree),
		moveUpBlockChan:           make(chan moveBlockRequest),
		reorgBlockChan:            make(chan reorgBlocksRequest),
		deDuplicateTransactionsCh: make(chan struct{}),
		resetCh:                   make(chan *resetBlocks),
		removeTxCh:                make(chan chainhash.Hash),
		newSubtreeChan:            newSubtreeChan,
		chainedSubtrees:           make([]*util.Subtree, 0, ExpectedNumberOfSubtrees),
		chainedSubtreeCount:       atomic.Int32{},
		currentSubtree:            firstSubtree,
		batcher:                   newTxIDAndFeeBatch(batcherSize),
		queue:                     queue,
		removeMap:                 util.NewSwissMap(0),
		doubleSpendWindowDuration: time.Duration(doubleSpendWindowMillis) * time.Millisecond,
		subtreeStore:              subtreeStore,
		utxoStore:                 utxoStore,
		logger:                    logger,
		stats:                     gocore.NewStat("subtreeProcessor").NewStat("Add", false),
		currentRunningState:       atomic.Value{},
	}
	stp.currentRunningState.Store("starting")

	for _, opts := range options {
		opts(stp)
	}

	stp.currentRunningState.Store("running")

	go func() {
		var (
			txReq *txIDAndFee
			err   error
		)

		for {
			select {
			case getSubtreesChan := <-stp.getSubtreesChan:
				stp.currentRunningState.Store("getSubtrees")

				logger.Infof("[SubtreeProcessor] get current subtrees")

				chainedCount := stp.chainedSubtreeCount.Load()
				completeSubtrees := make([]*util.Subtree, 0, chainedCount)
				completeSubtrees = append(completeSubtrees, stp.chainedSubtrees...)

				// incomplete subtrees ?
				if chainedCount == 0 && stp.currentSubtree.Length() > 1 {
					incompleteSubtree, err := util.NewTreeByLeafCount(stp.currentItemsPerFile)
					if err != nil {
						logger.Errorf("[SubtreeProcessor] error creating incomplete subtree: %s", err.Error())
						getSubtreesChan <- nil

						continue
					}

					_ = incompleteSubtree.AddCoinbaseNode()
					for _, node := range stp.currentSubtree.Nodes[1:] {
						_ = incompleteSubtree.AddSubtreeNode(node)
					}

					incompleteSubtree.Fees = stp.currentSubtree.Fees
					completeSubtrees = append(completeSubtrees, incompleteSubtree)

					// store (and announce) new incomplete subtree to other miners
					send := NewSubtreeRequest{
						Subtree: incompleteSubtree,
						ErrChan: make(chan error),
					}
					newSubtreeChan <- send

					// wait for a response
					// if we don't then we can't mine initial blocks and run coinbase splitter together
					// this is because getMiningCandidate would create subtrees in the background and
					// submitMiningSolution would try to setTTL on something that might not yet exist
					<-send.ErrChan
				}

				getSubtreesChan <- completeSubtrees

				logger.Infof("[SubtreeProcessor] get current subtrees DONE")

				stp.currentRunningState.Store("running")

			case reorgReq := <-stp.reorgBlockChan:
				stp.currentRunningState.Store("reorg")
				logger.Infof("[SubtreeProcessor] reorgReq subtree processor: %d, %d", len(reorgReq.moveDownBlocks), len(reorgReq.moveUpBlocks))
				err = stp.reorgBlocks(ctx, reorgReq.moveDownBlocks, reorgReq.moveUpBlocks)
				reorgReq.errChan <- err
				logger.Infof("[SubtreeProcessor] reorgReq subtree processor DONE: %d, %d", len(reorgReq.moveDownBlocks), len(reorgReq.moveUpBlocks))
				stp.currentRunningState.Store("running")

			case moveUpReq := <-stp.moveUpBlockChan:
				stp.currentRunningState.Store("moveUpBlock")

				logger.Infof("[SubtreeProcessor][%s] moveUpBlock subtree processor", moveUpReq.block.String())

				err = stp.moveUpBlock(ctx, moveUpReq.block, false)
				if err == nil {
					stp.currentBlockHeader = moveUpReq.block.Header
				}
				moveUpReq.errChan <- err
				logger.Infof("[SubtreeProcessor][%s] moveUpBlock subtree processor DONE", moveUpReq.block.String())
				stp.currentRunningState.Store("running")

			case <-stp.deDuplicateTransactionsCh:
				stp.currentRunningState.Store("deDuplicateTransactions")
				stp.deDuplicateTransactions()
				stp.currentRunningState.Store("running")

			case resetBlocksMsg := <-stp.resetCh:
				stp.currentRunningState.Store("resetBlocks")
				err = stp.reset(resetBlocksMsg.blockHeader, resetBlocksMsg.moveDownBlocks, resetBlocksMsg.moveUpBlocks, resetBlocksMsg.isLegacySync)

				if resetBlocksMsg.responseCh != nil {
					resetBlocksMsg.responseCh <- ResetResponse{Err: err}
				}

				stp.currentRunningState.Store("running")

			case removeTxHash := <-stp.removeTxCh:
				// remove the given transaction from the subtrees
				stp.currentRunningState.Store("removingTx")

				if err = stp.removeTxFromSubtrees(ctx, removeTxHash); err != nil {
					stp.logger.Errorf("[SubtreeProcessor] error removing tx from subtrees: %s", err.Error())
				}

				stp.currentRunningState.Store("running")

			default:
				stp.currentRunningState.Store("dequeue")

				nrProcessed := 0
				mapLength := stp.removeMap.Length()
				// set the validFromMillis to the current time minus the double spend window - so in the past
				validFromMillis := time.Now().Add(-1 * stp.doubleSpendWindowDuration).UnixMilli()

				for {
					txReq = stp.queue.dequeue(validFromMillis)
					if txReq == nil {
						time.Sleep(1 * time.Millisecond)
						break
					}

					// check if the tx needs to be removed
					if mapLength > 0 && stp.removeMap.Exists(txReq.node.Hash) {
						// remove from the map
						if err = stp.removeMap.Delete(txReq.node.Hash); err != nil {
							stp.logger.Errorf("[SubtreeProcessor] error removing tx from remove map: %s", err.Error())
						}

						continue
					}

					if txReq.node.Hash.Equal(*util.CoinbasePlaceholderHash) {
						stp.logger.Errorf("[SubtreeProcessor] error adding node: skipping request to add coinbase tx placeholder")
						continue
					}

					err = stp.addNode(txReq.node, false)
					if err != nil {
						stp.logger.Errorf("[SubtreeProcessor] error adding node: %s", err.Error())
					}

					stp.txCount.Add(1)

					nrProcessed++
					if nrProcessed > batcherSize {
						break
					}
				}

				stp.currentRunningState.Store("running")
			}
		}
	}()

	return stp, nil
}

func (stp *SubtreeProcessor) GetCurrentRunningState() string {
	return stp.currentRunningState.Load().(string)
}

// Reset resets the subtree processor, removing all subtrees and transactions
// this will be called from the block assembler in a channel select, making sure no other operations are happening
// the queue will still be ingesting transactions
func (stp *SubtreeProcessor) Reset(blockHeader *model.BlockHeader, moveDownBlocks []*model.Block, moveUpBlocks []*model.Block, isLegacySync bool) ResetResponse {
	responseCh := make(chan ResetResponse)
	stp.resetCh <- &resetBlocks{
		blockHeader:    blockHeader,
		moveDownBlocks: moveDownBlocks,
		moveUpBlocks:   moveUpBlocks,
		responseCh:     responseCh,
		isLegacySync:   isLegacySync,
	}

	return <-responseCh
}

func (stp *SubtreeProcessor) reset(blockHeader *model.BlockHeader, moveDownBlocks []*model.Block, moveUpBlocks []*model.Block, isLegacySync bool) error {
	_, _, deferFn := tracing.StartTracing(context.Background(), "reset",
		tracing.WithParentStat(stp.stats),
		tracing.WithHistogram(prometheusSubtreeProcessorReset),
		tracing.WithLogMessage(stp.logger, "[SubtreeProcessor][reset][%s] Resetting subtree processor with %d moveDownBlocks and %d moveUpBlocks", len(moveDownBlocks), len(moveUpBlocks)),
	)

	defer deferFn()

	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)
	stp.chainedSubtreeCount.Store(0)

	stp.currentSubtree, _ = util.NewTreeByLeafCount(stp.currentItemsPerFile)
	stp.txCount.Store(0)

	// dequeue all transactions
	stp.logger.Warnf("[SubtreeProcessor][Reset] Dequeueing all transactions")

	validUntilMillis := time.Now().UnixMilli()

	for {
		txReq := stp.queue.dequeue(0)
		if txReq == nil || txReq.time > validUntilMillis {
			// we are done
			break
		}
	}

	for _, block := range moveDownBlocks {
		if err := stp.utxoStore.Delete(context.Background(), block.CoinbaseTx.TxIDChainHash()); err != nil {
			// no need to error out if the key doesn't exist anyway
			if !errors.Is(err, errors.ErrTxNotFound) {
				return errors.NewProcessingError("[SubtreeProcessor][Reset] error deleting utxos for tx %s", block.CoinbaseTx.String(), err)
			}
		}

		stp.currentBlockHeader = block.Header
	}

	// optimized version for legacy sync
	if isLegacySync {
		coinbaseTxsAdded := sync.Map{}

		g, gCtx := errgroup.WithContext(context.Background())

		for _, block := range moveUpBlocks {
			g.Go(func() error {
				if err := stp.processCoinbaseUtxos(gCtx, block); err != nil {
					return err
				}

				coinbaseTxsAdded.Store(block.Hash().String(), block)

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			coinbaseTxsAdded.Range(func(key, value interface{}) bool {
				// remove all the coinbase transactions we added
				block := value.(*model.Block)
				if delErr := stp.utxoStore.Delete(context.Background(), block.CoinbaseTx.TxIDChainHash()); err != nil {
					stp.logger.Errorf("[SubtreeProcessor][Reset] error deleting utxos for coinbase tx %s: %v", block.CoinbaseTx.String(), delErr)
				}

				return true
			})

			return errors.NewProcessingError("[SubtreeProcessor][Reset] error processing coinbase utxos", err)
		}

		stp.currentBlockHeader = blockHeader
	} else {
		for _, block := range moveUpBlocks {
			if err := stp.processCoinbaseUtxos(context.Background(), block); err != nil {
				return errors.NewProcessingError("[SubtreeProcessor][Reset] error processing coinbase utxos", err)
			}

			stp.currentBlockHeader = block.Header
		}
	}

	return nil
}

func (stp *SubtreeProcessor) SetCurrentBlockHeader(blockHeader *model.BlockHeader) {
	// TODO should this also be in the channel select ?
	stp.currentBlockHeader = blockHeader
}

func (stp *SubtreeProcessor) GetCurrentBlockHeader() *model.BlockHeader {
	return stp.currentBlockHeader
}

func (stp *SubtreeProcessor) TxCount() uint64 {
	return stp.txCount.Load()
}

func (stp *SubtreeProcessor) QueueLength() int64 {
	return stp.queue.length()
}

// SubtreeCount is used to gather prometheus statics
func (stp *SubtreeProcessor) SubtreeCount() int {
	// not using len(chainSubtrees) to avoid Race condition
	// should we be using locks around all chainSubtree operations instead?
	// the subtree count isn't mission-critical - it's just for statistics
	return int(stp.chainedSubtreeCount.Load()) + 01
}

func (stp *SubtreeProcessor) addNode(node util.SubtreeNode, skipNotification bool) (err error) {
	prometheusSubtreeProcessorAddTx.Inc()

	err = stp.currentSubtree.AddSubtreeNode(node)
	if err != nil {
		return errors.NewProcessingError("error adding node to subtree", err)
	}

	if stp.currentSubtree.IsComplete() {
		if !skipNotification {
			stp.logger.Infof("[%s] append subtree", stp.currentSubtree.RootHash().String())
		}

		// Add the subtree to the chain
		// this needs to happen here, so we can wait for the append action to complete
		stp.chainedSubtrees = append(stp.chainedSubtrees, stp.currentSubtree)
		stp.chainedSubtreeCount.Add(1)

		oldSubtree := stp.currentSubtree
		oldSubtreeHash := oldSubtree.RootHash()

		// create a new subtree with the same height as the previous subtree
		stp.currentSubtree, err = util.NewTree(stp.currentSubtree.Height)
		if err != nil {
			return errors.NewProcessingError("[%s] error creating new subtree", oldSubtreeHash.String(), err)
		}

		if !skipNotification {
			// Send the subtree to the newSubtreeChan
			stp.newSubtreeChan <- NewSubtreeRequest{Subtree: oldSubtree}
		}
	}

	return nil
}

// Add adds a tx hash to a channel
func (stp *SubtreeProcessor) Add(node util.SubtreeNode) {
	stp.queue.enqueue(&txIDAndFee{node: node})
}

// Remove prevents a tx to be processed from the queue into a subtree
// this needs to happen before the delay time in the queue has passed
func (stp *SubtreeProcessor) Remove(hash chainhash.Hash) error {
	// add to the removeMap to make sure it gets removed if processing
	// or if it comes in later after cleaning the subtrees
	if err := stp.removeMap.Put(hash); err != nil {
		return errors.NewProcessingError("error adding tx to remove map", err)
	}

	// send a remove request to the subtree processor, making sure it does not block
	go func() {
		stp.removeTxCh <- hash
	}()

	return nil
}

func (stp *SubtreeProcessor) removeTxFromSubtrees(ctx context.Context, hash chainhash.Hash) error {
	_, _, deferFn := tracing.StartTracing(ctx, "removeTxFromSubtrees",
		tracing.WithParentStat(stp.stats),
		tracing.WithHistogram(prometheusSubtreeProcessorRemoveTx),
		tracing.WithLogMessage(stp.logger, "[SubtreeProcessor][removeTxFromSubtrees][%s] removing transaction from subtrees", hash),
	)

	defer deferFn()

	// find the transaction in the current and all chained subtrees
	foundIndex := stp.currentSubtree.NodeIndex(hash)
	foundSubtreeIndex := -1

	if foundIndex == -1 {
		// not found in the current subtree, check chained subtrees
		for subtreeIndex, subtree := range stp.chainedSubtrees {
			idx := subtree.NodeIndex(hash)
			if idx >= 0 {
				foundSubtreeIndex = subtreeIndex
				foundIndex = idx
			}
		}
	}

	if foundIndex >= 0 {
		// we found the transaction in a subtree
		if foundSubtreeIndex == -1 {
			// it was found in the current tree, remove it from there
			// further processing is not needed, as the subtrees in the chainedSubtrees are older than the current subtree
			return stp.currentSubtree.RemoveNodeAtIndex(foundIndex)
		}

		// it was found in a chained subtree, remove it from there and chain the subtrees again from the point it was removed
		// this is a bit more complex, as we need to remove the transaction from the subtree it is in and then make sure
		// the subtrees are chained correctly again
		if err := stp.chainedSubtrees[foundSubtreeIndex].RemoveNodeAtIndex(foundIndex); err != nil {
			return errors.NewProcessingError("error removing node from subtree", err)
		}

		// all the chained subtrees should be complete, as we now have a hole in the one we just removed from
		// we need to fill them up all again, including the current subtree
		if err := stp.reChainSubtrees(foundSubtreeIndex); err != nil {
			return errors.NewProcessingError("error rechaining subtrees", err)
		}
	}

	return nil
}

// reChainSubtrees will cycle through all subtrees from the given index and create new subtrees from the nodes
// in the same order as they were before
func (stp *SubtreeProcessor) reChainSubtrees(fromIndex int) error {
	// copy the original subtrees from the given index into a new structure
	originalSubtrees := stp.chainedSubtrees[fromIndex:]
	originalSubtrees = append(originalSubtrees, stp.currentSubtree)

	// reset the chained subtrees and the current subtree
	stp.chainedSubtrees = stp.chainedSubtrees[:fromIndex]

	//nolint:gosec
	stp.chainedSubtreeCount.Store(int32(len(stp.chainedSubtrees)))

	stp.currentSubtree, _ = util.NewTreeByLeafCount(stp.currentItemsPerFile)

	// add the nodes from the original subtrees to the new subtrees
	for _, subtree := range originalSubtrees {
		for _, node := range subtree.Nodes {
			// this adds to the current subtree
			if err := stp.addNode(node, true); err != nil {
				return errors.NewProcessingError("error adding node to subtree", err)
			}
		}
	}

	return nil
}

func (stp *SubtreeProcessor) GetCompletedSubtreesForMiningCandidate() []*util.Subtree {
	stp.logger.Infof("GetCompletedSubtreesForMiningCandidate")

	var subtrees []*util.Subtree

	subtreesChan := make(chan []*util.Subtree)

	// get the subtrees from channel
	stp.getSubtreesChan <- subtreesChan

	subtrees = <-subtreesChan

	return subtrees
}

// MoveUpBlock the subtrees when a new block is found
func (stp *SubtreeProcessor) MoveUpBlock(block *model.Block) error {
	errChan := make(chan error)
	stp.moveUpBlockChan <- moveBlockRequest{
		block:   block,
		errChan: errChan,
	}

	return <-errChan
}

func (stp *SubtreeProcessor) Reorg(moveDownBlocks []*model.Block, modeUpBlocks []*model.Block) error {
	errChan := make(chan error)
	stp.reorgBlockChan <- reorgBlocksRequest{
		moveDownBlocks: moveDownBlocks,
		moveUpBlocks:   modeUpBlocks,
		errChan:        errChan,
	}

	return <-errChan
}

// reorgBlocks adds all transactions that are in the block given to the current subtrees
// TODO handle conflicting transactions
func (stp *SubtreeProcessor) reorgBlocks(ctx context.Context, moveDownBlocks []*model.Block, moveUpBlocks []*model.Block) error {
	if moveDownBlocks == nil {
		return errors.NewProcessingError("you must pass in blocks to move down the chain")
	}

	if moveUpBlocks == nil {
		return errors.NewProcessingError("you must pass in blocks to move up the chain")
	}

	stp.logger.Infof("reorgBlocks with %d moveDownBlocks and %d moveUpBlocks", len(moveDownBlocks), len(moveUpBlocks))

	for _, block := range moveDownBlocks {
		err := stp.moveDownBlock(ctx, block)
		if err != nil {
			return err
		}
	}

	for _, block := range moveUpBlocks {
		// we skip the notifications for now and do them all at the end
		err := stp.moveUpBlock(ctx, block, true)
		if err != nil {
			return err
		}
	}

	// announce all the subtrees to the network
	// this will also store it by the Server in the subtree store
	for _, subtree := range stp.chainedSubtrees {
		stp.newSubtreeChan <- NewSubtreeRequest{Subtree: subtree}
	}

	stp.setTxCountFromSubtrees()

	return nil
}

func (stp *SubtreeProcessor) setTxCountFromSubtrees() {
	stp.txCount.Store(0)

	for _, subtree := range stp.chainedSubtrees {
		stp.txCount.Add(uint64(subtree.Length()))
	}

	stp.txCount.Add(uint64(stp.currentSubtree.Length()))
	stp.txCount.Add(uint64(stp.queue.length()))
}

// moveDownBlock adds all transactions that are in the block given to the current subtrees
// TODO handle conflicting transactions
func (stp *SubtreeProcessor) moveDownBlock(ctx context.Context, block *model.Block) (err error) {
	if block == nil {
		return errors.NewProcessingError("[moveDownBlock] you must pass in a block to moveDownBlock")
	}

	startTime := time.Now()

	prometheusSubtreeProcessorMoveDownBlock.Inc()

	// add all the transactions from the block, excluding the coinbase, which needs to be reverted in the utxo store
	stp.logger.Infof("[moveDownBlock][%s] with %d subtrees", block.String(), len(block.Subtrees))

	defer func() {
		stp.logger.Infof("[moveDownBlock][%s] with %d subtrees DONE in %s", block.String(), len(block.Subtrees), time.Since(startTime).String())

		err := recover()
		if err != nil {
			stp.logger.Errorf("[moveDownBlock] with block %s: %s", block.String(), err)
		}
	}()

	lastIncompleteSubtree := stp.currentSubtree
	chainedSubtrees := stp.chainedSubtrees

	// TODO add check for the correct parent block

	// reset the subtree processor
	stp.currentSubtree, err = util.NewTreeByLeafCount(stp.currentItemsPerFile)
	if err != nil {
		return errors.NewProcessingError("[moveDownBlock][%s] error creating new subtree", block.String(), err)
	}

	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)
	stp.chainedSubtreeCount.Store(0)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddCoinbaseNode()

	g, gCtx := errgroup.WithContext(ctx)
	moveDownBlockConcurrency, _ := gocore.Config().GetInt("blockassembly_moveDownBlockConcurrency", 64)
	g.SetLimit(moveDownBlockConcurrency)

	// get all the subtrees in parallel
	stp.logger.Warnf("[moveDownBlock][%s] with %d subtrees: get subtrees", block.String(), len(block.Subtrees))
	subtreesNodes := make([][]util.SubtreeNode, len(block.Subtrees))

	for idx, subtreeHash := range block.Subtrees {
		idx := idx
		subtreeHash := subtreeHash

		g.Go(func() error {
			subtreeReader, err := stp.subtreeStore.GetIoReader(gCtx, subtreeHash[:], options.WithFileExtension("subtree"))
			if err != nil {
				return errors.NewServiceError("[moveDownBlock][%s] error getting subtree %s", block.String(), subtreeHash.String(), err)
			}

			defer func() {
				_ = subtreeReader.Close()
			}()

			subtree := &util.Subtree{}

			err = subtree.DeserializeFromReader(subtreeReader)
			if err != nil {
				return errors.NewProcessingError("[moveDownBlock][%s] error deserializing subtree", block.String(), err)
			}

			// TODO add metrics about how many txs we are reading per second
			subtreesNodes[idx] = subtree.Nodes

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("[moveDownBlock][%s] error getting subtrees", block.String(), err)
	}

	stp.logger.Warnf("[moveDownBlock][%s] with %d subtrees: get subtrees DONE", block.String(), len(block.Subtrees))

	stp.logger.Warnf("[moveDownBlock][%s] with %d subtrees: create new subtrees", block.String(), len(block.Subtrees))
	// run through the nodes of the subtrees in order and add to the new subtrees
	for idx, subtreeNode := range subtreesNodes {
		if idx == 0 {
			// process coinbase utxos
			if err = stp.utxoStore.Delete(ctx, block.CoinbaseTx.TxIDChainHash()); err != nil {
				return errors.NewServiceError("[moveDownBlock][%s] error deleting utxos for tx %s", block.String(), block.CoinbaseTx.String(), err)
			}

			// skip the first transaction of the first subtree (coinbase)
			for i := 1; i < len(subtreeNode); i++ {
				_ = stp.addNode(subtreeNode[i], true)
			}
		} else {
			for _, node := range subtreeNode {
				_ = stp.addNode(node, true)
			}
		}
	}

	stp.logger.Warnf("[moveDownBlock][%s] with %d subtrees: create new subtrees DONE", block.String(), len(block.Subtrees))

	stp.logger.Warnf("[moveDownBlock][%s] with %d subtrees: add previous nodes to subtrees", block.String(), len(block.Subtrees))
	// add all the transactions from the previous state
	for _, subtree := range chainedSubtrees {
		for _, node := range subtree.Nodes {
			if !node.Hash.Equal(*util.CoinbasePlaceholderHash) {
				_ = stp.addNode(node, true)
			}
		}
	}

	// add all the transactions from the last incomplete subtree
	for _, node := range lastIncompleteSubtree.Nodes {
		_ = stp.addNode(node, true)
	}

	stp.logger.Warnf("[moveDownBlock][%s] with %d subtrees: add previous nodes to subtrees DONE", block.String(), len(block.Subtrees))

	// we must set the current block header
	stp.currentBlockHeader = block.Header

	prometheusSubtreeProcessorMoveDownBlockDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	return nil
}

// moveDownBlocks adds all transactions that are in given blocks to current stp subtrees
func (stp *SubtreeProcessor) moveDownBlocks(ctx context.Context, blocks []*model.Block) (err error) {
	if len(blocks) == 0 || blocks[0] == nil {
		return errors.NewProcessingError("[moveDownBlocks] you must pass in a block to moveDownBlock")
	}

	startTime := time.Now()

	prometheusSubtreeProcessorMoveDownBlock.Inc()

	stp.logger.Infof("[moveDownBlocks] with %d blocks", len(blocks))

	lastIncompleteSubtree := stp.currentSubtree
	chainedSubtrees := stp.chainedSubtrees

	stp.currentSubtree, err = util.NewTreeByLeafCount(stp.currentItemsPerFile)
	if err != nil {
		return errors.NewProcessingError("[moveDownBlocks] error creating new subtree", err)
	}

	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)
	stp.chainedSubtreeCount.Store(0)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddCoinbaseNode()

	g, gCtx := errgroup.WithContext(ctx)
	moveDownBlockConcurrency, _ := gocore.Config().GetInt("blockassembly_moveDownBlockConcurrency", 64)
	g.SetLimit(moveDownBlockConcurrency)

	var block *model.Block

	for i := 0; i < len(blocks); i++ {
		block = blocks[i]
		// add all the transactions from the block, excluding the coinbase, which needs to be reverted in the utxo store
		stp.logger.Infof("[moveDownBlocks][%s], block %d (block hash: %v), with %d subtrees", block.String(), i, block.Hash(), len(block.Subtrees))

		defer func() {
			stp.logger.Infof("[moveDownBlocks][%s], block %d (block hash: %v), with %d subtrees DONE in %s", block.String(), i, block.Hash(), len(block.Subtrees), time.Since(startTime).String())

			err := recover()
			if err != nil {
				stp.logger.Errorf("[moveDownBlocks] with block %s: %s", block.String(), err)
			}
		}()

		// get all the subtrees in parallel
		stp.logger.Warnf("[moveDownBlocks][%s], block %d (block hash: %v)  with %d subtrees: get subtrees", block.String(), i, block.Hash(), len(block.Subtrees))
		subtreesNodes := make([][]util.SubtreeNode, len(block.Subtrees))

		for idx, subtreeHash := range block.Subtrees {
			idx := idx
			subtreeHash := subtreeHash

			g.Go(func() error {
				subtreeReader, err := stp.subtreeStore.GetIoReader(gCtx, subtreeHash[:], options.WithFileExtension("subtree"))
				if err != nil {
					return errors.NewServiceError("[moveDownBlocks][%s], block %d (block hash: %v), error getting subtree %s", block.String(), i, block.Hash(), subtreeHash.String(), err)
				}

				defer func() {
					_ = subtreeReader.Close()
				}()

				subtree := &util.Subtree{}

				err = subtree.DeserializeFromReader(subtreeReader)
				if err != nil {
					return errors.NewProcessingError("[moveDownBlocks][%s], block %d (block hash: %v), error deserializing subtree", block.String(), i, block.Hash(), err)
				}

				// TODO add metrics about how many txs we are reading per second
				subtreesNodes[idx] = subtree.Nodes

				return nil
			})
		}

		if err = g.Wait(); err != nil {
			return errors.NewProcessingError("[moveDownBlocks][%s], block %d (block hash: %v), error getting subtrees", block.String(), i, block.Hash(), err)
		}

		stp.logger.Warnf("[moveDownBlocks][%s], block %d (block hash: %v), with %d subtrees: get subtrees DONE", block.String(), i, block.Hash(), len(block.Subtrees))

		stp.logger.Warnf("[moveDownBlocks][%s] with %d subtrees: create new subtrees", block.String(), i, block.Hash(), len(block.Subtrees))
		// run through the nodes of the subtrees in order and add to the new subtrees
		for idx, subtreeNode := range subtreesNodes {
			if idx == 0 {
				// process coinbase utxos
				if err = stp.utxoStore.Delete(ctx, block.CoinbaseTx.TxIDChainHash()); err != nil {
					return errors.NewServiceError("[moveDownBlocks][%s], block %d (block hash: %v), error deleting utxos for tx %s", block.String(), i, block.Hash(), block.CoinbaseTx.String(), err)
				}

				// skip the first transaction of the first subtree (coinbase)
				for i := 1; i < len(subtreeNode); i++ {
					_ = stp.addNode(subtreeNode[i], true)
				}
			} else {
				for _, node := range subtreeNode {
					_ = stp.addNode(node, true)
				}
			}
		}

		stp.logger.Warnf("[moveDownBlocks][%s], block %d (block hash: %v), with %d subtrees: create new subtrees DONE", block.String(), i, block.Hash(), len(block.Subtrees))
	}

	stp.logger.Warnf("[moveDownBlocks] add previous nodes to subtrees")

	// add all the transactions from the previous state
	for _, subtree := range chainedSubtrees {
		for _, node := range subtree.Nodes {
			if !node.Hash.Equal(*util.CoinbasePlaceholderHash) {
				_ = stp.addNode(node, true)
			}
		}
	}

	// add all the transactions from the last incomplete subtree
	for _, node := range lastIncompleteSubtree.Nodes {
		_ = stp.addNode(node, true)
	}

	stp.logger.Warnf("[moveDownBlock][%s] with %d subtrees: add previous nodes to subtrees DONE", block.String(), len(block.Subtrees))

	// we must set the current block header to the last block header we have added
	stp.currentBlockHeader = block.Header

	prometheusSubtreeProcessorMoveDownBlockDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	return nil
}

// moveUpBlock cleans out all transactions that are in the current subtrees and also in the block
// given. It is akin moving up the blockchain to the next block.
// TODO handle conflicting transactions
func (stp *SubtreeProcessor) moveUpBlock(ctx context.Context, block *model.Block, skipNotification bool) error {
	if block == nil {
		return errors.NewProcessingError("[moveUpBlock] you must pass in a block to moveUpBlock")
	}

	_, _, deferFn := tracing.StartTracing(ctx, "moveUpBlock",
		tracing.WithParentStat(stp.stats),
		tracing.WithCounter(prometheusSubtreeProcessorMoveUpBlock),
		tracing.WithHistogram(prometheusSubtreeProcessorMoveUpBlockDuration),
		tracing.WithLogMessage(stp.logger, "[moveUpBlock][%s] with block", block.String()),
	)

	defer func() {
		deferFn()

		err := recover()
		if err != nil {
			// print the stack trace
			stp.logger.Errorf("%s", debug.Stack())
			stp.logger.Errorf("[moveUpBlock][%s] with block: %s", block.String(), err)
		}
	}()

	// TODO reactivate and test
	// if !block.Header.HashPrevBlock.IsEqual(stp.currentBlockHeader.Hash()) {
	//	return errors.NewProcessingError("the block passed in does not match the current block header: [%s] - [%s]", block.Header.StringDump(), stp.currentBlockHeader.StringDump())
	// }

	stp.logger.Debugf("[moveUpBlock][%s] resetting subtrees: %v", block.String(), block.Subtrees)

	err := stp.processCoinbaseUtxos(ctx, block)
	if err != nil {
		return errors.NewProcessingError("[moveUpBlock][%s] error processing coinbase utxos", block.String(), err)
	}

	// if there are no transactions in the subtree processor, we do not have to do anything
	// we will always have at least 1 single coinbase placeholder transaction
	if stp.txCount.Load() == 1 && stp.SubtreeCount() == 1 && stp.currentSubtree.Nodes[0].Hash.Equal(*util.CoinbasePlaceholderHash) {
		stp.logger.Debugf("[moveUpBlock][%s] no transactions in subtree processor, skipping cleanup", block.String())

		// set the current block header
		stp.currentBlockHeader = block.Header

		return nil
	}

	// create a reverse lookup map of all the subtrees in the block
	blockSubtreesMap := make(map[chainhash.Hash]int, len(block.Subtrees))
	for idx, subtree := range block.Subtrees {
		blockSubtreesMap[*subtree] = idx
	}

	// get all the subtrees that were not in the block
	// this should clear out all subtrees from our own blocks, giving an empty blockSubtreesMap as a result
	// and preventing processing of the map
	chainedSubtrees := make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)
	for _, subtree := range stp.chainedSubtrees {
		id := *subtree.RootHash()
		if _, ok := blockSubtreesMap[id]; !ok {
			// only add the subtrees that were not in the block
			chainedSubtrees = append(chainedSubtrees, subtree)
		} else {
			// remove the subtree from the block subtrees map, we had it in our list
			delete(blockSubtreesMap, id)
		}
	}

	// clear the transaction ids from all the subtrees of the block that are left over
	var transactionMap util.TxMap

	if len(blockSubtreesMap) > 0 {
		mapStartTime := time.Now()

		stp.logger.Debugf("[moveUpBlock][%s] processing subtrees into transaction map", block.String())

		if transactionMap, err = stp.createTransactionMap(ctx, blockSubtreesMap); err != nil {
			// TODO revert the created utxos
			return errors.NewProcessingError("[moveUpBlock][%s] error creating transaction map", block.String(), err)
		}

		stp.logger.Debugf("[moveUpBlock][%s] processing subtrees into transaction map DONE in %s: %d", block.String(), time.Since(mapStartTime).String(), transactionMap.Length())
	}

	// reset the current subtree
	currentSubtree := stp.currentSubtree

	stp.currentSubtree, err = util.NewTreeByLeafCount(stp.currentItemsPerFile)
	if err != nil {
		return errors.NewProcessingError("[moveUpBlock][%s] error creating new subtree", block.String(), err)
	}

	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)
	stp.chainedSubtreeCount.Store(0)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddCoinbaseNode()

	remainderStartTime := time.Now()

	stp.logger.Debugf("[moveUpBlock][%s] processing remainder tx hashes into subtrees", block.String())

	if transactionMap != nil && transactionMap.Length() > 0 {
		remainderSubtrees := make([]*util.Subtree, 0, len(chainedSubtrees)+1)

		remainderSubtrees = append(remainderSubtrees, chainedSubtrees...)
		remainderSubtrees = append(remainderSubtrees, currentSubtree)

		remainderTxHashesStartTime := time.Now()

		stp.logger.Debugf("[moveUpBlock][%s] processRemainderTxHashes with %d subtrees", block.String(), len(chainedSubtrees))

		if err = stp.processRemainderTxHashes(ctx, remainderSubtrees, transactionMap, skipNotification); err != nil {
			return errors.NewProcessingError("[moveUpBlock][%s] error getting remainder tx hashes", block.String(), err)
		}

		stp.logger.Debugf("[moveUpBlock][%s] processRemainderTxHashes with %d subtrees DONE in %s", block.String(), len(chainedSubtrees), time.Since(remainderTxHashesStartTime).String())

		// empty the queue to make sure we have all the transactions that could be in the block
		// we only have to do this when we have a transaction map, because otherwise we would be processing our own block
		dequeueStartTime := time.Now()

		stp.logger.Debugf("[moveUpBlock][%s] processing queue while moveUpBlock: %d", block.String(), stp.queue.length())

		err = stp.moveUpBlockDeQueue(transactionMap, skipNotification)
		if err != nil {
			return errors.NewProcessingError("[moveUpBlock][%s] error moving up block deQueue", block.String(), err)
		}

		stp.logger.Debugf("[moveUpBlock][%s] processing queue while moveUpBlock DONE in %s", block.String(), time.Since(dequeueStartTime).String())
	} else {
		// TODO find a way to this in parallel
		// there were no subtrees in the block, that were not in our block assembly
		// this was most likely our own block
		removeMapLength := stp.removeMap.Length()

		coinbaseID := block.CoinbaseTx.TxIDChainHash()

		for _, subtree := range chainedSubtrees {
			for _, node := range subtree.Nodes {
				// TODO is all this needed? This adds a lot to the processing time
				if !node.Hash.Equal(*util.CoinbasePlaceholderHash) {
					if !coinbaseID.Equal(node.Hash) {
						if removeMapLength > 0 && stp.removeMap.Exists(node.Hash) {
							if err = stp.removeMap.Delete(node.Hash); err != nil {
								stp.logger.Errorf("[moveUpBlock][%s] error removing tx from remove map: %s", block.String(), err.Error())
							}
						} else {
							_ = stp.addNode(node, skipNotification)
						}
					}
				}
			}
		}

		for _, node := range currentSubtree.Nodes {
			// TODO is all this needed? This adds a lot to the processing time
			if !node.Hash.Equal(*util.CoinbasePlaceholderHash) {
				if !coinbaseID.Equal(node.Hash) {
					if removeMapLength > 0 && stp.removeMap.Exists(node.Hash) {
						if err = stp.removeMap.Delete(node.Hash); err != nil {
							stp.logger.Errorf("[moveUpBlock][%s] error removing tx from remove map: %s", block.String(), err.Error())
						}
					} else {
						_ = stp.addNode(node, skipNotification)
					}
				}
			}
		}
	}

	stp.logger.Debugf("[moveUpBlock][%s] processing remainder tx hashes into subtrees DONE in %s", block.String(), time.Since(remainderStartTime).String())

	stp.setTxCountFromSubtrees()

	// set the current block header
	stp.currentBlockHeader = block.Header

	return nil
}

func (stp *SubtreeProcessor) moveUpBlockDeQueue(transactionMap util.TxMap, skipNotification bool) (err error) {
	queueLength := stp.queue.length()
	if queueLength > 0 {
		nrProcessed := int64(0)
		validFromMillis := time.Now().Add(-1 * stp.doubleSpendWindowDuration).UnixMilli()

		for {
			// TODO make sure to add the time delay here when activated
			item := stp.queue.dequeue(validFromMillis)
			if item == nil {
				break
			}

			if !transactionMap.Exists(item.node.Hash) {
				_ = stp.addNode(item.node, skipNotification)
			}

			nrProcessed++
			if nrProcessed > queueLength {
				break
			}
		}
	}

	return nil
}

func (stp *SubtreeProcessor) DeDuplicateTransactions() {
	stp.deDuplicateTransactionsCh <- struct{}{}
}

func (stp *SubtreeProcessor) deDuplicateTransactions() {
	var err error

	stp.logger.Infof("[DeDuplicateTransactions] de-duplicating transactions")

	currentSubtree := stp.currentSubtree
	chainedSubtrees := stp.chainedSubtrees

	// reset the current subtree
	stp.currentSubtree, err = util.NewTreeByLeafCount(stp.currentItemsPerFile)
	if err != nil {
		stp.logger.Errorf("[DeDuplicateTransactions] error creating new subtree in de-duplication: %s", err.Error())
		return
	}

	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)
	stp.chainedSubtreeCount.Store(0)

	deDuplicationMap := util.NewSplitSwissMapUint64(int(stp.txCount.Load())) // nolint:gosec
	removeMapLength := stp.removeMap.Length()

	for subtreeIdx, subtree := range chainedSubtrees {
		for nodeIdx, node := range subtree.Nodes {
			if removeMapLength > 0 && stp.removeMap.Exists(node.Hash) {
				if err = stp.removeMap.Delete(node.Hash); err != nil {
					stp.logger.Errorf("[DeDuplicateTransactions] error removing tx from remove map: %s", err.Error())
				}
			} else {
				if err = deDuplicationMap.Put(node.Hash, uint64(subtreeIdx*subtree.Size()+nodeIdx)); err != nil {
					stp.logger.Errorf("[DeDuplicateTransactions] found duplicate transaction in block assembly: %s - %v", node.Hash.String(), err)
				} else {
					_ = stp.addNode(node, false)
				}
			}
		}
	}

	for nodeIdx, node := range currentSubtree.Nodes {
		if removeMapLength > 0 && stp.removeMap.Exists(node.Hash) {
			if err = stp.removeMap.Delete(node.Hash); err != nil {
				stp.logger.Errorf("[DeDuplicateTransactions] error removing tx from remove map: %s", err.Error())
			}
		} else {
			if err = deDuplicationMap.Put(node.Hash, uint64(nodeIdx)); err != nil {
				stp.logger.Errorf("[DeDuplicateTransactions] found duplicate transaction in block assembly: %s - %v", node.Hash.String(), err)
			} else {
				_ = stp.addNode(node, false)
			}
		}
	}

	stp.logger.Infof("[DeDuplicateTransactions] de-duplicating transactions DONE")
}

func (stp *SubtreeProcessor) processCoinbaseUtxos(ctx context.Context, block *model.Block) error {
	startTime := time.Now()

	prometheusSubtreeProcessorProcessCoinbaseTx.Inc()

	if block == nil || block.CoinbaseTx == nil {
		log.Printf("********************************************* block or coinbase is nil")
		return nil
	}

	utxos, err := utxo.GetUtxoHashes(block.CoinbaseTx)
	if err != nil {
		return errors.NewProcessingError("[SubtreeProcessor][coinbase:%s] error extracting coinbase utxos", block.CoinbaseTx.TxIDChainHash(), err)
	}

	for _, u := range utxos {
		stp.logger.Debugf("[SubtreeProcessor][coinbase:%s] store utxo: %s", block.CoinbaseTx.TxIDChainHash(), u.String())
	}

	blockHeight := block.Height
	if blockHeight <= 0 {
		// lookup the block height for the current block from the blockchain service, we cannot rely on the block height
		// that is set in the utxo store, since that is the height of the current block in block assembly, which might
		// not be the same
		blockHeight = stp.utxoStore.GetBlockHeight()
		if blockHeight == 0 {
			return errors.NewServiceError("[SubtreeProcessor][coinbase:%s] error extracting coinbase height via utxo store", block.CoinbaseTx.TxIDChainHash())
		}
	}

	stp.logger.Debugf("[SubtreeProcessor][%s] height %d storeCoinbaseTx %s blockID %d", block.Header.Hash().String(), blockHeight, block.CoinbaseTx.TxIDChainHash().String(), block.ID)
	// we pass in the block height we are working on here, since the utxo store will recognize the tx as
	// a coinbase and add the correct spending height, which should be + 99
	if _, err = stp.utxoStore.Create(ctx, block.CoinbaseTx, blockHeight, utxo.WithBlockIDs(block.ID)); err != nil {
		if errors.Is(err, errors.ErrTxExists) {
			// This will also be called for the 2 coinbase transactions that are duplicated on the network
			// These transactions were created twice:
			//   e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468
			//   d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599
			stp.logger.Warnf("[SubtreeProcessor] coinbase utxos for %s already exist. Skipping", block.CoinbaseTx.TxIDChainHash())
		} else {
			stp.logger.Errorf("[SubtreeProcessor] error storing utxos: %v", err)
			return err
		}
	}

	prometheusSubtreeProcessorProcessCoinbaseTxDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	return nil
}

func (stp *SubtreeProcessor) processRemainderTxHashes(ctx context.Context, chainedSubtrees []*util.Subtree, transactionMap util.TxMap, skipNotification bool) error {
	var hashCount atomic.Int64

	// clean out the transactions from the old current subtree that were in the block
	// and add the remainderSubtreeNodes to the new current subtree
	g, _ := errgroup.WithContext(ctx)
	processRemainderTxHashesConcurrency, _ := gocore.Config().GetInt("blockassembly_processRemainderTxHashesConcurrency", 64)
	g.SetLimit(processRemainderTxHashesConcurrency)

	// we need to process this in order, so we first process all subtrees in parallel, but keeping the order
	remainderSubtrees := make([][]util.SubtreeNode, len(chainedSubtrees))

	for idx, subtree := range chainedSubtrees {
		idx := idx
		st := subtree

		g.Go(func() error {
			remainderSubtrees[idx] = make([]util.SubtreeNode, 0, len(st.Nodes)/10) // expect max 10% of the nodes to be different
			// don't use the util function, keep the memory local in this function, no jumping between heap and stack
			// err = st.Difference(transactionMap, &remainderSubtrees[idx])
			for _, node := range st.Nodes {
				if !transactionMap.Exists(node.Hash) {
					remainderSubtrees[idx] = append(remainderSubtrees[idx], node)
				}
			}

			hashCount.Add(int64(len(remainderSubtrees[idx])))

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("error getting remainder tx difference", err)
	}

	removeMapLength := stp.removeMap.Length()

	// TODO find a way to this in parallel
	// add all found tx hashes to the final list, in order
	for _, subtreeNodes := range remainderSubtrees {
		for _, node := range subtreeNodes {
			if !node.Hash.Equal(*util.CoinbasePlaceholderHash) {
				if removeMapLength > 0 && stp.removeMap.Exists(node.Hash) {
					if err := stp.removeMap.Delete(node.Hash); err != nil {
						stp.logger.Errorf("[SubtreeProcessor] error removing tx from remove map: %s", err.Error())
					}
				} else {
					_ = stp.addNode(node, skipNotification)
				}
			}
		}
	}

	return nil
}

func (stp *SubtreeProcessor) createTransactionMap(ctx context.Context, blockSubtreesMap map[chainhash.Hash]int) (util.TxMap, error) {
	startTime := time.Now()

	prometheusSubtreeProcessorCreateTransactionMap.Inc()

	concurrentSubtreeReads, _ := gocore.Config().GetInt("blockassembly_subtreeProcessorConcurrentReads", 4)

	// TODO this bit is slow !
	stp.logger.Infof("createTransactionMap with %d subtrees, concurrency %d", len(blockSubtreesMap), concurrentSubtreeReads)

	mapSize := len(blockSubtreesMap) * stp.currentItemsPerFile // TODO fix this assumption, should be gleaned from the block
	transactionMap := util.NewSplitSwissMap(mapSize)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrentSubtreeReads)

	// get all the subtrees from the block that we have not yet cleaned out
	for subtreeHash := range blockSubtreesMap {
		st := subtreeHash

		g.Go(func() error {
			stp.logger.Debugf("getting subtree: %s", st.String())

			subtreeReader, err := stp.subtreeStore.GetIoReader(ctx, st[:], options.WithFileExtension("subtree"))
			if err != nil {
				return errors.NewServiceError("error getting subtree: %s", st.String(), err)
			}

			// TODO add metrics about how many txs we are reading per second
			txHashBuckets, err := DeserializeHashesFromReaderIntoBuckets(subtreeReader, transactionMap.Buckets())
			if err != nil {
				return errors.NewProcessingError("error deserializing subtree: %s", st.String(), err)
			}

			bucketG := errgroup.Group{}

			for bucket, hashes := range txHashBuckets {
				bucket := bucket
				hashes := hashes
				// put the hashes into the transaction map in parallel, it has already been split into the correct buckets
				bucketG.Go(func() error {
					_ = transactionMap.PutMulti(bucket, hashes)
					return nil
				})
			}

			if err = bucketG.Wait(); err != nil {
				return errors.NewProcessingError("error putting hashes into transaction map", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, errors.NewProcessingError("error getting subtrees", err)
	}

	stp.logger.Infof("createTransactionMap with %d subtrees DONE", len(blockSubtreesMap))

	prometheusSubtreeProcessorCreateTransactionMapDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	return transactionMap, nil
}

func DeserializeHashesFromReaderIntoBuckets(reader io.Reader, nBuckets uint16) (hashes map[uint16][][32]byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in DeserializeHashesFromReaderIntoBuckets: %v", r)
		}
	}()

	buf := bufio.NewReaderSize(reader, 1024*1024*16) // 16MB buffer

	if _, err = buf.Discard(48); err != nil { // skip headers
		return nil, errors.NewProcessingError("unable to read header", err)
	}

	// read number of leaves
	bytes8 := make([]byte, 8)
	if _, err = io.ReadFull(buf, bytes8); err != nil {
		return nil, errors.NewProcessingError("unable to read number of leaves", err)
	}

	numLeaves := binary.LittleEndian.Uint64(bytes8)

	// read leaves
	hashes = make(map[uint16][][32]byte, nBuckets)
	for i := uint16(0); i < nBuckets; i++ {
		hashes[i] = make([][32]byte, 0, int(math.Ceil(float64(numLeaves/uint64(nBuckets))*1.1)))
	}

	var bucket uint16

	bytes48 := make([]byte, 48)

	for i := uint64(0); i < numLeaves; i++ {
		// read all the node data in 1 go
		if _, err = io.ReadFull(buf, bytes48); err != nil {
			return nil, errors.NewProcessingError("unable to read node", err)
		}

		bucket = util.Bytes2Uint16Buckets([32]byte(bytes48[:32]), nBuckets)
		hashes[bucket] = append(hashes[bucket], [32]byte(bytes48[:32]))
	}

	return hashes, nil
}
