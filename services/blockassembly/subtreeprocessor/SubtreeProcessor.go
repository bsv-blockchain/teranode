package subtreeprocessor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type Job struct {
	ID              *chainhash.Hash
	Subtrees        []*util.Subtree
	MiningCandidate *model.MiningCandidate
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
	currentItemsPerFile int
	txChan              chan *[]txIDAndFee
	getSubtreesChan     chan chan []*util.Subtree
	moveUpBlockChan     chan moveBlockRequest
	reorgBlockChan      chan reorgBlocksRequest
	newSubtreeChan      chan *util.Subtree // used to notify of a new subtree
	chainedSubtrees     []*util.Subtree
	currentSubtree      *util.Subtree
	currentBlockHeader  *model.BlockHeader
	sync.Mutex
	txCount      atomic.Uint64
	batcher      *txIDAndFeeBatch
	queue        *LockFreeQueue
	subtreeStore blob.Store
	utxoStore    utxostore.Interface
	logger       utils.Logger
}

var (
	ExpectedNumberOfSubtrees = 1024 // this is the number of subtrees we expect to be in a block, with a subtree create about every second
)

func NewSubtreeProcessor(ctx context.Context, logger utils.Logger, subtreeStore blob.Store, utxoStore utxostore.Interface,
	newSubtreeChan chan *util.Subtree, options ...Options) *SubtreeProcessor {

	initPrometheusMetrics()

	initialItemsPerFile, _ := gocore.Config().GetInt("initial_merkle_items_per_subtree", 1_048_576)

	firstSubtree := util.NewTreeByLeafCount(initialItemsPerFile)
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

	queue := NewLockFreeQueue(0)

	stp := &SubtreeProcessor{
		currentItemsPerFile: initialItemsPerFile,
		txChan:              make(chan *[]txIDAndFee, txChanBufferSize),
		getSubtreesChan:     make(chan chan []*util.Subtree),
		moveUpBlockChan:     make(chan moveBlockRequest),
		reorgBlockChan:      make(chan reorgBlocksRequest),
		newSubtreeChan:      newSubtreeChan,
		chainedSubtrees:     make([]*util.Subtree, 0, ExpectedNumberOfSubtrees),
		currentSubtree:      firstSubtree,
		batcher:             newTxIDAndFeeBatch(batcherSize),
		queue:               queue,
		subtreeStore:        subtreeStore,
		utxoStore:           utxoStore, // TODO should this be here? It is needed to remove the coinbase on moveDownBlock
		logger:              logger,
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
					incompleteSubtree := util.NewTreeByLeafCount(stp.currentItemsPerFile)
					for _, node := range stp.currentSubtree.Nodes {
						_ = incompleteSubtree.AddSubtreeNode(node)
					}
					incompleteSubtree.Fees = stp.currentSubtree.Fees
					completeSubtrees = append(completeSubtrees, incompleteSubtree)

					// store (and announce) new incomplete subtree to other miners
					newSubtreeChan <- incompleteSubtree
				}

				getSubtreesChan <- completeSubtrees

			case reorgReq := <-stp.reorgBlockChan:
				logger.Infof("[SubtreeProcessor] reorgReq subtree processor")
				err = stp.reorgBlocks(ctx, reorgReq.moveDownBlocks, reorgReq.moveUpBlocks)
				if err == nil {
					if len(reorgReq.moveUpBlocks) > 0 {
						stp.currentBlockHeader = reorgReq.moveUpBlocks[len(reorgReq.moveUpBlocks)-1].Header
					} else {
						stp.currentBlockHeader = reorgReq.moveDownBlocks[len(reorgReq.moveDownBlocks)-1].Header
					}
				}
				reorgReq.errChan <- err

			case moveUpReq := <-stp.moveUpBlockChan:
				logger.Infof("[SubtreeProcessor] moveUpBlock subtree processor: %s", moveUpReq.block.String())
				err = stp.moveUpBlock(ctx, moveUpReq.block, false)
				if err == nil {
					stp.currentBlockHeader = moveUpReq.block.Header
				}
				moveUpReq.errChan <- err

			default:
				nrProcessed := 0
				for {
					txReq = stp.queue.dequeue()
					if txReq == nil {
						time.Sleep(1 * time.Millisecond)
						break
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

func (stp *SubtreeProcessor) SubtreeCount() int {
	return len(stp.chainedSubtrees) + 1
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
		stp.logger.Infof("[SubtreeProcessor] append subtree: %s", stp.currentSubtree.RootHash().String())
		stp.chainedSubtrees = append(stp.chainedSubtrees, stp.currentSubtree)

		oldSubtree := stp.currentSubtree

		// create a new subtree with the same height as the previous subtree
		stp.currentSubtree = util.NewTree(stp.currentSubtree.Height)

		if !skipNotification {
			// Send the subtree to the newSubtreeChan
			stp.newSubtreeChan <- oldSubtree
		}
	}

	return nil
}

// Add adds a tx hash to a channel
func (stp *SubtreeProcessor) Add(node util.SubtreeNode) {
	stp.queue.enqueue(&txIDAndFee{node: node})
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
		return errors.New("you must pass in blocks to move down the chain")
	}

	// TODO make this more efficient by doing all the moveDownBlocks in 1 go into the subtrees
	for _, block := range moveDownBlocks {
		err := stp.moveDownBlock(ctx, block)
		if err != nil {
			return err
		}
	}

	for idx, block := range moveUpBlocks {
		// we skip the notifications for all but the last block
		lastBlock := idx == len(moveUpBlocks)-1
		err := stp.moveUpBlock(ctx, block, !lastBlock)
		if err != nil {
			return err
		}
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
func (stp *SubtreeProcessor) moveDownBlock(ctx context.Context, block *model.Block) error {
	if block == nil {
		return errors.New("you must pass in a block to moveDownBlock")
	}
	startTime := time.Now()
	prometheusSubtreeProcessorMoveDownBlock.Inc()

	lastIncompleteSubtree := stp.currentSubtree
	chainedSubtrees := stp.chainedSubtrees

	// TODO add check for the correct parent block

	// reset the subtree processor
	stp.currentSubtree = util.NewTreeByLeafCount(stp.currentItemsPerFile)
	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddNode(model.CoinbasePlaceholder, 0, 0)

	// add all the transactions from the block, excluding the coinbase, which needs to be reverted in the utxo store
	stp.logger.Warnf("moveDownBlock %s with %d subtrees", block.String(), len(block.Subtrees))
	for idx, subtreeHash := range block.Subtrees {
		subtreeBytes, err := stp.subtreeStore.Get(ctx, subtreeHash[:])
		if err != nil {
			return fmt.Errorf("error getting subtree %s: %s", subtreeHash.String(), err.Error())
		}

		subtree := &util.Subtree{}
		err = subtree.Deserialize(subtreeBytes)
		if err != nil {
			return fmt.Errorf("error deserializing subtree: %s", err.Error())
		}

		if idx == 0 {
			// process coinbase utxos
			if err = stp.utxoStore.Delete(ctx, block.CoinbaseTx); err != nil {
				return fmt.Errorf("error deleting utxos for tx %s: %s", block.CoinbaseTx.String(), err.Error())
			}

			// skip the first transaction of the first subtree (coinbase)
			for i := 1; i < len(subtree.Nodes); i++ {
				_ = stp.addNode(subtree.Nodes[i], true)
			}
		} else {
			for _, node := range subtree.Nodes {
				_ = stp.addNode(node, true)
			}
		}
	}

	// add all the transactions from the previous state
	for _, subtree := range chainedSubtrees {
		for _, node := range subtree.Nodes {
			_ = stp.addNode(node, true)
		}
	}

	// add all the transactions from the last incomplete subtree
	for _, node := range lastIncompleteSubtree.Nodes {
		_ = stp.addNode(node, true)
	}

	// we must set the current block header
	stp.currentBlockHeader = block.Header

	prometheusSubtreeProcessorMoveDownBlockDuration.Observe(time.Since(startTime).Seconds())

	return nil
}

// moveUpBlock cleans out all transactions that are in the current subtrees and also in the block
// given. It is akin moving up the blockchain to the next block.
// TODO handle conflicting transactions
func (stp *SubtreeProcessor) moveUpBlock(ctx context.Context, block *model.Block, skipNotification bool) error {
	defer func() {
		stp.logger.Infof("DONE moveUpBlock with block %s", block.String())
		err := recover()
		if err != nil {
			stp.logger.Errorf("moveUpBlock with block %s: %s", block.String(), err)
		}
	}()

	if block == nil {
		return errors.New("you must pass in a block to moveUpBlock")
	}
	startTime := time.Now()
	prometheusSubtreeProcessorMoveUpBlock.Inc()

	// TODO reactivate and test
	//if !block.Header.HashPrevBlock.IsEqual(stp.currentBlockHeader.Hash()) {
	//	return fmt.Errorf("the block passed in does not match the current block header: [%s] - [%s]", block.Header.StringDump(), stp.currentBlockHeader.StringDump())
	//}

	stp.logger.Infof("moveUpBlock with block %s", block.String())
	stp.logger.Debugf("resetting subtrees: %v", block.Subtrees)

	coinbaseId := block.CoinbaseTx.TxIDChainHash()
	err := stp.processCoinbaseUtxos(ctx, block)
	if err != nil {
		return err
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
	var transactionMap *util.SplitSwissMap
	if len(blockSubtreesMap) > 0 {
		if transactionMap, err = stp.createTransactionMap(ctx, blockSubtreesMap); err != nil {
			// TODO revert the created utxos
			return fmt.Errorf("error creating transaction map: %s", err.Error())
		}
	}

	var remainderTxHashes *[]util.SubtreeNode
	if transactionMap != nil && transactionMap.Length() > 0 {
		remainderSubtrees := make([]*util.Subtree, 0, len(chainedSubtrees)+1)

		remainderSubtrees = append(remainderSubtrees, chainedSubtrees...)
		remainderSubtrees = append(remainderSubtrees, stp.currentSubtree)

		// empty the queue to make sure we have all the transactions that could be in the block
		err = stp.moveUpBlockDeQueue(&remainderSubtrees)
		if err != nil {
			return fmt.Errorf("error moving up block deQueue: %s", err.Error())
		}

		if remainderTxHashes, err = stp.getRemainderTxHashes(ctx, remainderSubtrees, transactionMap); err != nil {
			return fmt.Errorf("error getting remainder tx hashes: %s", err.Error())
		}
	} else {
		// there were no subtrees in the block, that were not in our block assembly
		// this was most likely our own block
		chainedSubtreeSize := 0
		if len(chainedSubtrees) > 0 {
			// just use the first subtree, each subtree should be the same size
			chainedSubtreeSize = chainedSubtrees[0].Size()
		}
		r := make([]util.SubtreeNode, 0, (len(chainedSubtrees)*chainedSubtreeSize)+len(stp.currentSubtree.Nodes))
		remainderTxHashes = &r

		for _, subtree := range chainedSubtrees {
			*remainderTxHashes = append(*remainderTxHashes, subtree.Nodes...)
		}

		*remainderTxHashes = append(*remainderTxHashes, stp.currentSubtree.Nodes...)
	}

	// reset the current subtree
	stp.currentSubtree = util.NewTreeByLeafCount(stp.currentItemsPerFile)
	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddNode(model.CoinbasePlaceholder, 0, 0)

	// remainderTxHashes is from early trees, so they need to be added before the current subtree nodes
	if remainderTxHashes != nil {
		for idx, node := range *remainderTxHashes {
			if !node.Hash.Equal(*model.CoinbasePlaceholderHash) {
				if coinbaseId.Equal(node.Hash) {
					// this is the coinbase transaction, we need to skip it
					stp.logger.Warnf("skipping coinbase transaction: %s, %d", node.Hash.String(), idx)
					continue
				}
				_ = stp.addNode(node, skipNotification)
			}
		}
	}

	stp.setTxCount()

	// set the current block header
	stp.currentBlockHeader = block.Header

	prometheusSubtreeProcessorMoveUpBlockDuration.Observe(time.Since(startTime).Seconds())

	return nil
}

func (stp *SubtreeProcessor) moveUpBlockDeQueue(remainderSubtrees *[]*util.Subtree) (err error) {
	queueLength := stp.queue.length()
	var currentSubtree *util.Subtree
	if queueLength > 0 {
		stp.logger.Infof("processing queue while moveUpBlock: %d", queueLength)

		nrProcessed := int64(0)
		currentSubtree = util.NewTree(stp.currentSubtree.Height)
		for {
			item := stp.queue.dequeue()
			if item == nil {
				break
			}

			err = currentSubtree.AddSubtreeNode(item.node)
			if err != nil {
				return fmt.Errorf("error adding node to subtree: %s", err.Error())
			}

			if currentSubtree.IsComplete() {
				*remainderSubtrees = append(*remainderSubtrees, currentSubtree)
				// create a new subtree with the same height as the previous subtree
				currentSubtree = util.NewTree(currentSubtree.Height)
			}

			nrProcessed++
			if nrProcessed > queueLength {
				break
			}
		}
	}

	if currentSubtree != nil && currentSubtree.Length() > 0 {
		*remainderSubtrees = append(*remainderSubtrees, currentSubtree)
	}

	return nil
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

	prometheusSubtreeProcessorProcessCoinbaseTxDuration.Observe(time.Since(startTime).Seconds())

	return nil
}

func (stp *SubtreeProcessor) getRemainderTxHashes(ctx context.Context, chainedSubtrees []*util.Subtree, transactionMap *util.SplitSwissMap) (*[]util.SubtreeNode, error) {
	var hashCount atomic.Int64

	// clean out the transactions from the old current subtree that were in the block
	// and add the remainderSubtreeNodes to the new current subtree
	g, _ := errgroup.WithContext(ctx)

	// we need to process this in order, so we first process all subtrees in parallel, but keeping the order
	remainderSubtreeHashes := make([][]util.SubtreeNode, len(chainedSubtrees))
	for idx, subtree := range chainedSubtrees {
		idx := idx
		st := subtree
		g.Go(func() error {
			remainingTransactions, err := st.Difference(transactionMap)
			if err != nil {
				return fmt.Errorf("error calculating difference: %s", err.Error())
			}

			for _, txHash := range remainingTransactions {
				remainderSubtreeHashes[idx] = append(remainderSubtreeHashes[idx], txHash)
				hashCount.Add(1)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("error getting remainder tx difference: %s", err.Error())
	}

	// add all found tx hashes to the final list, in order
	remainderSubtreeNodes := make([]util.SubtreeNode, 0, hashCount.Load())
	for _, subtreeHashes := range remainderSubtreeHashes {
		remainderSubtreeNodes = append(remainderSubtreeNodes, subtreeHashes...)
	}

	return &remainderSubtreeNodes, nil
}

func (stp *SubtreeProcessor) createTransactionMap(ctx context.Context, blockSubtreesMap map[chainhash.Hash]int) (*util.SplitSwissMap, error) {
	startTime := time.Now()
	prometheusSubtreeProcessorCreateTransactionMap.Inc()

	mapSize := len(blockSubtreesMap) * 1024 * 1024 // TODO fix this assumption, should be gleaned from the block
	transactionMap := util.NewSplitSwissMap(mapSize)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(64)

	// get all the subtrees from the block that we have not yet cleaned out
	for subtreeHash := range blockSubtreesMap {
		st := subtreeHash
		g.Go(func() error {
			stp.logger.Infof("getting subtree: %s", st.String())
			subtreeBytes, err := stp.subtreeStore.Get(ctx, st[:])
			var subtree *util.Subtree
			if err != nil {
				return errors.Join(fmt.Errorf("error getting subtree: %s", st.String()), err)
			}

			subtree = &util.Subtree{}
			// TODO deserialize only the hashes, we don't need any of the rest
			err = subtree.Deserialize(subtreeBytes)
			if err != nil {
				return errors.Join(fmt.Errorf("error deserializing subtree: %s", st.String()), err)
			}

			for _, node := range subtree.Nodes {
				_ = transactionMap.Put(node.Hash)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("error getting subtrees: %s", err.Error())
	}

	prometheusSubtreeProcessorCreateTransactionMapDuration.Observe(time.Since(startTime).Seconds())

	return transactionMap, nil
}
