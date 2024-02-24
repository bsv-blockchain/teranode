package subtreeprocessor

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
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

type SubtreeProcessor struct {
	currentItemsPerFile       int
	txChan                    chan *[]txIDAndFee
	getSubtreesChan           chan chan []*util.Subtree
	moveUpBlockChan           chan moveBlockRequest
	reorgBlockChan            chan reorgBlocksRequest
	deDuplicateTransactionsCh chan struct{}
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
	utxoStore                 utxostore.Interface
	logger                    ulogger.Logger
	stat                      *gocore.Stat
}

var (
	ExpectedNumberOfSubtrees = 1024 // this is the number of subtrees we expect to be in a block, with a subtree create about every second
)

func NewSubtreeProcessor(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store, utxoStore utxostore.Interface,
	newSubtreeChan chan NewSubtreeRequest, options ...Options) *SubtreeProcessor {

	initPrometheusMetrics()

	initialItemsPerFile, _ := gocore.Config().GetInt("initial_merkle_items_per_subtree", 1_048_576)

	firstSubtree, err := util.NewTreeByLeafCount(initialItemsPerFile)
	if err != nil {
		panic(err)
	}
	// We add a placeholder for the coinbase tx because we know this is the first subtree in the chain
	if err := firstSubtree.AddNode(model.CoinbasePlaceholder, 0, 0); err != nil {
		panic(err)
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
		newSubtreeChan:            newSubtreeChan,
		chainedSubtrees:           make([]*util.Subtree, 0, ExpectedNumberOfSubtrees),
		chainedSubtreeCount:       atomic.Int32{},
		currentSubtree:            firstSubtree,
		batcher:                   newTxIDAndFeeBatch(batcherSize),
		queue:                     queue,
		removeMap:                 util.NewSwissMap(0),
		doubleSpendWindowDuration: time.Duration(doubleSpendWindowMillis) * time.Millisecond,
		subtreeStore:              subtreeStore,
		utxoStore:                 utxoStore, // TODO should this be here? It is needed to remove the coinbase on moveDownBlock
		logger:                    logger,
		stat:                      gocore.NewStat("subtreeProcessor").NewStat("Add", false),
	}

	for _, opts := range options {
		opts(stp)
	}

	go func() {
		var txReq *txIDAndFee
		var err error
		for {
			select {
			case getSubtreesChan := <-stp.getSubtreesChan:
				logger.Infof("[SubtreeProcessor] get current subtrees")
				completeSubtrees := make([]*util.Subtree, 0, len(stp.chainedSubtrees))
				completeSubtrees = append(completeSubtrees, stp.chainedSubtrees...)

				// incomplete subtrees ?
				if len(stp.chainedSubtrees) == 0 && stp.currentSubtree.Length() > 1 {
					incompleteSubtree, err := util.NewTreeByLeafCount(stp.currentItemsPerFile)
					if err != nil {
						logger.Errorf("[SubtreeProcessor] error creating incomplete subtree: %s", err.Error())
						getSubtreesChan <- nil
						continue
					}
					for _, node := range stp.currentSubtree.Nodes {
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

			case reorgReq := <-stp.reorgBlockChan:
				logger.Infof("[SubtreeProcessor] reorgReq subtree processor: %d, %d", len(reorgReq.moveDownBlocks), len(reorgReq.moveUpBlocks))
				err = stp.reorgBlocks(ctx, reorgReq.moveDownBlocks, reorgReq.moveUpBlocks)
				reorgReq.errChan <- err
				logger.Infof("[SubtreeProcessor] reorgReq subtree processor DONE: %d, %d", len(reorgReq.moveDownBlocks), len(reorgReq.moveUpBlocks))

			case moveUpReq := <-stp.moveUpBlockChan:
				logger.Infof("[SubtreeProcessor][%s] moveUpBlock subtree processor", moveUpReq.block.String())
				err = stp.moveUpBlock(ctx, moveUpReq.block, false)
				if err == nil {
					stp.currentBlockHeader = moveUpReq.block.Header
				}
				moveUpReq.errChan <- err
				logger.Infof("[SubtreeProcessor][%s] moveUpBlock subtree processor DONE", moveUpReq.block.String())

			case <-stp.deDuplicateTransactionsCh:
				stp.deDuplicateTransactions()

			default:
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

				stp.chainedSubtreeCount.Store(int32(len(stp.chainedSubtrees)))
			}
		}
	}()

	return stp
}

func (stp *SubtreeProcessor) SetCurrentBlockHeader(blockHeader *model.BlockHeader) {
	// TODO should this also be in the channel select ?
	stp.currentBlockHeader = blockHeader
}

func (stp *SubtreeProcessor) TxCount() uint64 {
	return stp.txCount.Load()
}

func (stp *SubtreeProcessor) QueueLength() int64 {
	return stp.queue.length()
}

/* used to gather prometheus statics */
func (stp *SubtreeProcessor) SubtreeCount() int {
	// not using len(chainSubtrees) to avoid Race condition
	// should we be using locks around all chainSubtree operations instead?
	// the subtree count isn't mission critical - it's just for statistics
	return int(stp.chainedSubtreeCount.Load()) + 01
}

func (stp *SubtreeProcessor) addNode(node util.SubtreeNode, skipNotification bool) (err error) {
	prometheusSubtreeProcessorAddTx.Inc()

	err = stp.currentSubtree.AddSubtreeNode(node)
	if err != nil {
		return fmt.Errorf("error adding node to subtree: %s", err.Error())
	}

	if stp.currentSubtree.IsComplete() {
		// Add the subtree to the chain
		// this needs to happen here, so we can wait for the append action to complete
		stp.logger.Infof("[%s] append subtree", stp.currentSubtree.RootHash().String())
		stp.chainedSubtrees = append(stp.chainedSubtrees, stp.currentSubtree)

		oldSubtree := stp.currentSubtree
		oldSubtreeHash := oldSubtree.RootHash()

		// create a new subtree with the same height as the previous subtree
		stp.currentSubtree, err = util.NewTree(stp.currentSubtree.Height)
		if err != nil {
			return fmt.Errorf("[%s] error creating new subtree: %s", oldSubtreeHash.String(), err.Error())
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
	if err := stp.removeMap.Put(hash); err != nil {
		return fmt.Errorf("error adding tx to remove map: %s", err.Error())
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
		return errors.New("you must pass in blocks to move down the chain")
	}
	if moveUpBlocks == nil {
		return errors.New("you must pass in blocks to move up the chain")
	}

	stp.logger.Infof("reorgBlocks with %d moveDownBlocks and %d moveUpBlocks", len(moveDownBlocks), len(moveUpBlocks))

	for _, block := range moveDownBlocks {
		err := stp.moveDownBlock(ctx, block)
		if err != nil {
			return err
		}
	}

	for _, block := range moveUpBlocks {
		// we skip the notifications for and do them all at the end
		err := stp.moveUpBlock(ctx, block, false)
		if err != nil {
			return err
		}
	}

	// announce all the subtrees to the network
	// this will also store it by the Server in the subtree store
	for _, subtree := range stp.chainedSubtrees {
		stp.newSubtreeChan <- NewSubtreeRequest{Subtree: subtree}
	}

	stp.setTxCount()

	return nil
}

func (stp *SubtreeProcessor) setTxCount() {
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
		return errors.New("[moveDownBlock] you must pass in a block to moveDownBlock")
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
		return fmt.Errorf("[moveDownBlock][%s] error creating new subtree: %s", block.String(), err.Error())
	}
	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddNode(model.CoinbasePlaceholder, 0, 0)

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
			subtreeReader, err := stp.subtreeStore.GetIoReader(gCtx, subtreeHash[:])
			if err != nil {
				return fmt.Errorf("[moveDownBlock][%s] error getting subtree %s: %s", block.String(), subtreeHash.String(), err.Error())
			}
			defer func() {
				_ = subtreeReader.Close()
			}()

			subtree := &util.Subtree{}
			err = subtree.DeserializeFromReader(subtreeReader)
			if err != nil {
				return fmt.Errorf("[moveDownBlock][%s] error deserializing subtree: %s", block.String(), err.Error())
			}

			subtreesNodes[idx] = subtree.Nodes

			subtree = nil
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("[moveDownBlock][%s] error getting subtrees: %s", block.String(), err.Error())
	}
	stp.logger.Warnf("[moveDownBlock][%s] with %d subtrees: get subtrees DONE", block.String(), len(block.Subtrees))

	stp.logger.Warnf("[moveDownBlock][%s] with %d subtrees: create new subtrees", block.String(), len(block.Subtrees))
	// run through the nodes of the subtrees in order and add to the new subtrees
	for idx, subtreeNode := range subtreesNodes {
		if idx == 0 {
			// process coinbase utxos
			if err = stp.utxoStore.Delete(ctx, block.CoinbaseTx); err != nil {
				return fmt.Errorf("[moveDownBlock][%s] error deleting utxos for tx %s: %s", block.String(), block.CoinbaseTx.String(), err.Error())
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
			if !node.Hash.Equal(*model.CoinbasePlaceholderHash) {
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

// moveUpBlock cleans out all transactions that are in the current subtrees and also in the block
// given. It is akin moving up the blockchain to the next block.
// TODO handle conflicting transactions
func (stp *SubtreeProcessor) moveUpBlock(ctx context.Context, block *model.Block, skipNotification bool) error {
	if block == nil {
		return errors.New("[moveUpBlock] you must pass in a block to moveUpBlock")
	}

	startTime := time.Now()
	defer func() {
		prometheusSubtreeProcessorMoveUpBlockDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
		stp.logger.Infof("[moveUpBlock][%s] with block DONE in %s", block.String(), time.Since(startTime).String())

		err := recover()
		if err != nil {
			stp.logger.Errorf("[moveUpBlock][%s] with block: %s", block.String(), err)
		}
	}()

	prometheusSubtreeProcessorMoveUpBlock.Inc()

	// TODO reactivate and test
	//if !block.Header.HashPrevBlock.IsEqual(stp.currentBlockHeader.Hash()) {
	//	return fmt.Errorf("the block passed in does not match the current block header: [%s] - [%s]", block.Header.StringDump(), stp.currentBlockHeader.StringDump())
	//}

	stp.logger.Infof("[moveUpBlock][%s] with block", block.String())
	stp.logger.Debugf("[moveUpBlock][%s] resetting subtrees: %v", block.String(), block.Subtrees)

	coinbaseId := block.CoinbaseTx.TxIDChainHash()
	err := stp.processCoinbaseUtxos(ctx, block)
	if err != nil {
		return fmt.Errorf("[moveUpBlock][%s] error processing coinbase utxos: %s", block.String(), err.Error())
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
		stp.logger.Infof("[moveUpBlock][%s] processing subtrees into transaction map", block.String())
		if transactionMap, err = stp.createTransactionMap(ctx, blockSubtreesMap); err != nil {
			// TODO revert the created utxos
			return fmt.Errorf("[moveUpBlock][%s] error creating transaction map: %s", block.String(), err.Error())
		}
		stp.logger.Infof("[moveUpBlock][%s] processing subtrees into transaction map DONE in %s: %d", block.String(), time.Since(mapStartTime).String(), transactionMap.Length())
	}

	// reset the current subtree
	currentSubtree := stp.currentSubtree
	stp.currentSubtree, err = util.NewTreeByLeafCount(stp.currentItemsPerFile)
	if err != nil {
		return fmt.Errorf("[moveUpBlock][%s] error creating new subtree: %s", block.String(), err.Error())
	}
	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddNode(model.CoinbasePlaceholder, 0, 0)

	remainderStartTime := time.Now()
	stp.logger.Infof("[moveUpBlock][%s] processing remainder tx hashes into subtrees", block.String())

	if transactionMap != nil && transactionMap.Length() > 0 {
		remainderSubtrees := make([]*util.Subtree, 0, len(chainedSubtrees)+1)

		remainderSubtrees = append(remainderSubtrees, chainedSubtrees...)
		remainderSubtrees = append(remainderSubtrees, currentSubtree)

		remainderTxHashesStartTime := time.Now()
		stp.logger.Infof("[moveUpBlock][%s] processRemainderTxHashes with %d subtrees", block.String(), len(chainedSubtrees))
		if err = stp.processRemainderTxHashes(ctx, remainderSubtrees, transactionMap, skipNotification); err != nil {
			return fmt.Errorf("[moveUpBlock][%s] error getting remainder tx hashes: %s", block.String(), err.Error())
		}
		stp.logger.Infof("[moveUpBlock][%s] processRemainderTxHashes with %d subtrees DONE in %s", block.String(), len(chainedSubtrees), time.Since(remainderTxHashesStartTime).String())

		// empty the queue to make sure we have all the transactions that could be in the block
		// we only have to do this when we have a transaction map, because otherwise we would be processing our own block
		dequeueStartTime := time.Now()
		stp.logger.Infof("[moveUpBlock][%s] processing queue while moveUpBlock: %d", block.String(), stp.queue.length())
		err = stp.moveUpBlockDeQueue(transactionMap)
		if err != nil {
			return fmt.Errorf("[moveUpBlock][%s] error moving up block deQueue: %s", block.String(), err.Error())
		}
		stp.logger.Infof("[moveUpBlock][%s] processing queue while moveUpBlock DONE in %s", block.String(), time.Since(dequeueStartTime).String())
	} else {
		// there were no subtrees in the block, that were not in our block assembly
		// this was most likely our own block
		removeMapLength := stp.removeMap.Length()
		for _, subtree := range chainedSubtrees {
			for _, node := range subtree.Nodes {
				// TODO is all this needed? This adds a lot to the processing time
				if !node.Hash.Equal(*model.CoinbasePlaceholderHash) {
					if !coinbaseId.Equal(node.Hash) {
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
			if !node.Hash.Equal(*model.CoinbasePlaceholderHash) {
				if !coinbaseId.Equal(node.Hash) {
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

	stp.logger.Infof("[moveUpBlock][%s] processing remainder tx hashes into subtrees DONE in %s", block.String(), time.Since(remainderStartTime).String())

	stp.setTxCount()

	// set the current block header
	stp.currentBlockHeader = block.Header

	return nil
}

func (stp *SubtreeProcessor) moveUpBlockDeQueue(transactionMap util.TxMap) (err error) {
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
				_ = stp.addNode(item.node, false)
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

	deDuplicationMap := util.NewSplitSwissMapUint64(int(stp.txCount.Load()))
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

	// TODO this does not work for the early blocks in Bitcoin
	blockHeight, err := block.ExtractCoinbaseHeight()
	if err != nil {
		return fmt.Errorf("error extracting coinbase height: %v", err)
	}

	if err = stp.utxoStore.Store(ctx, block.CoinbaseTx, blockHeight+100); err != nil {
		// error will be handled below
		stp.logger.Errorf("[SubtreeProcessor] error storing utxos: %v", err)
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
			//err = st.Difference(transactionMap, &remainderSubtrees[idx])
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
		return fmt.Errorf("error getting remainder tx difference: %s", err.Error())
	}

	removeMapLength := stp.removeMap.Length()

	// add all found tx hashes to the final list, in order
	for _, subtreeNodes := range remainderSubtrees {
		for _, node := range subtreeNodes {
			if !node.Hash.Equal(*model.CoinbasePlaceholderHash) {
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

	// TODO this bit is slow !
	stp.logger.Infof("createTransactionMap with %d subtrees", len(blockSubtreesMap))

	mapSize := len(blockSubtreesMap) * 1024 * 1024 // TODO fix this assumption, should be gleaned from the block
	transactionMap := util.NewSplitSwissMap(mapSize)

	g, ctx := errgroup.WithContext(ctx)
	concurrentSubtreeReads, _ := gocore.Config().GetInt("blockassembly_subtreeProcessorConcurrentReads", 4)
	g.SetLimit(concurrentSubtreeReads)

	// get all the subtrees from the block that we have not yet cleaned out
	for subtreeHash := range blockSubtreesMap {
		st := subtreeHash
		g.Go(func() error {
			stp.logger.Debugf("getting subtree: %s", st.String())
			subtreeReader, err := stp.subtreeStore.GetIoReader(ctx, st[:])
			if err != nil {
				return errors.Join(fmt.Errorf("error getting subtree: %s", st.String()), err)
			}

			txHashBuckets, err := DeserializeHashesFromReaderIntoBuckets(subtreeReader, transactionMap.Buckets())
			if err != nil {
				return errors.Join(fmt.Errorf("error deserializing subtree: %s", st.String()), err)
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
				return fmt.Errorf("error putting hashes into transaction map: %s", err.Error())
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("error getting subtrees: %s", err.Error())
	}

	stp.logger.Infof("createTransactionMap with %d subtrees DONE", len(blockSubtreesMap))

	prometheusSubtreeProcessorCreateTransactionMapDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	return transactionMap, nil
}

func DeserializeHashesFromReaderIntoBuckets(reader io.Reader, nBuckets uint16) (hashes map[uint16][][32]byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered in DeserializeHashesFromReaderIntoBuckets: %v", r)
		}
	}()

	buf := bufio.NewReaderSize(reader, 1024*1024*4)
	if _, err = buf.Discard(48); err != nil { // skip headers
		return nil, fmt.Errorf("unable to read header: %v", err)
	}

	// read number of leaves
	bytes8 := make([]byte, 8)
	if _, err = io.ReadFull(buf, bytes8); err != nil {
		return nil, fmt.Errorf("unable to read number of leaves: %v", err)
	}
	numLeaves := binary.LittleEndian.Uint64(bytes8)

	// read leaves
	hashes = make(map[uint16][][32]byte, nBuckets)
	for i := uint16(0); i < nBuckets; i++ {
		hashes[i] = make([][32]byte, 0, int(math.Ceil(float64(numLeaves/uint64(nBuckets))*1.1)))
	}

	hash := chainhash.Hash{}
	var bucket uint16
	for i := uint64(0); i < numLeaves; i++ {
		if _, err = io.ReadFull(buf, hash[:]); err != nil {
			return nil, fmt.Errorf("unable to read node: %v", err)
		}
		bucket = util.Bytes2Uint16Buckets(hash, nBuckets)
		hashes[bucket] = append(hashes[bucket], hash)

		// read rest of the bytes
		if _, err = buf.Discard(16); err != nil { // skip headers
			return nil, fmt.Errorf("unable to read fees and size: %v", err)
		}
	}

	return hashes, nil
}
