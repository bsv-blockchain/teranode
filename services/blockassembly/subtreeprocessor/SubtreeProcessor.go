package subtreeprocessor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type txIDAndFee struct {
	txID        chainhash.Hash
	fee         uint64
	sizeInBytes uint64
	waitCh      chan struct{}
}

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
	txChan              chan *txIDAndFee
	getSubtreesChan     chan chan []*util.Subtree
	moveUpBlockChan     chan moveBlockRequest
	reorgBlockChan      chan reorgBlocksRequest
	newSubtreeChan      chan *util.Subtree // used to notify of a new subtree
	chainedSubtrees     []*util.Subtree
	currentSubtree      *util.Subtree
	currentBlockHeader  *model.BlockHeader
	sync.Mutex
	txCount      uint64
	subtreeStore blob.Store
	utxoStore    utxostore.Interface
	logger       utils.Logger
}

var (
	ExpectedNumberOfSubtrees = 1024 // this is the number of subtrees we expect to be in a block, with a subtree create about every second
)

func NewSubtreeProcessor(ctx context.Context, logger utils.Logger, subtreeStore blob.Store, utxoStore utxostore.Interface,
	newSubtreeChan chan *util.Subtree) *SubtreeProcessor {

	initialItemsPerFile, _ := gocore.Config().GetInt("initial_merkle_items_per_subtree", 1_048_576)

	firstSubtree := util.NewTreeByLeafCount(initialItemsPerFile)
	// We add a placeholder for the coinbase tx because we know this is the first subtree in the chain
	if err := firstSubtree.AddNode(model.CoinbasePlaceholderHash, 0, 0); err != nil {
		panic(err)
	}

	stp := &SubtreeProcessor{
		currentItemsPerFile: initialItemsPerFile,
		txChan:              make(chan *txIDAndFee, 100_000),
		getSubtreesChan:     make(chan chan []*util.Subtree),
		moveUpBlockChan:     make(chan moveBlockRequest),
		reorgBlockChan:      make(chan reorgBlocksRequest),
		newSubtreeChan:      newSubtreeChan,
		chainedSubtrees:     make([]*util.Subtree, 0, ExpectedNumberOfSubtrees),
		currentSubtree:      firstSubtree,
		subtreeStore:        subtreeStore,
		utxoStore:           utxoStore, // TODO should this be here? It is needed to remove the coinbase on moveDownBlock
		txCount:             0,
		logger:              logger,
	}

	go func() {
		for {
			select {
			case getSubtreesChan := <-stp.getSubtreesChan:
				logger.Infof("[SubtreeProcessor] get current subtrees")
				chainedSubtrees := make([]*util.Subtree, 0, len(stp.chainedSubtrees))
				chainedSubtrees = append(chainedSubtrees, stp.chainedSubtrees...)

				// incomplete subtrees ?
				if len(stp.chainedSubtrees) == 0 && stp.currentSubtree.Length() > 1 {
					incompleteSubtree := util.NewTreeByLeafCount(stp.currentItemsPerFile)
					for _, node := range stp.currentSubtree.Nodes {
						_ = incompleteSubtree.AddNode(node.Hash, node.Fee, node.SizeInBytes)
					}
					incompleteSubtree.Fees = stp.currentSubtree.Fees
					chainedSubtrees = append(chainedSubtrees, incompleteSubtree)

					newSubtreeChan <- incompleteSubtree
				}

				getSubtreesChan <- chainedSubtrees

			case reorgReq := <-stp.reorgBlockChan:
				logger.Infof("[SubtreeProcessor] reorgReq subtree processor")
				err := stp.reorgBlocks(ctx, reorgReq.moveDownBlocks, reorgReq.moveUpBlocks)
				if err == nil {
					stp.currentBlockHeader = reorgReq.moveUpBlocks[len(reorgReq.moveUpBlocks)-1].Header
				}
				reorgReq.errChan <- err

			case moveUpReq := <-stp.moveUpBlockChan:
				logger.Infof("[SubtreeProcessor] moveUpBlock subtree processor: %s", moveUpReq.block.String())
				err := stp.moveUpBlock(ctx, moveUpReq.block, false)
				if err == nil {
					stp.currentBlockHeader = moveUpReq.block.Header
				}
				moveUpReq.errChan <- err

			case txReq := <-stp.txChan:
				stp.addNode(txReq.txID, txReq.fee, txReq.sizeInBytes)
				if txReq.waitCh != nil {
					txReq.waitCh <- struct{}{}
				}
				stp.txCount++
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
	return stp.txCount
}

func (stp *SubtreeProcessor) addNode(txID chainhash.Hash, fee uint64, sizeInBytes uint64, skipNotification ...bool) {
	err := stp.currentSubtree.AddNode(&txID, fee, sizeInBytes)
	if err != nil {
		panic(err)
	}

	if stp.currentSubtree.IsComplete() {
		subtree := stp.currentSubtree

		// create a new subtree with the same height as the previous subtree
		stp.currentSubtree = util.NewTree(subtree.Height)

		// Add the subtree to the chain
		// this needs to happen here, so we can wait for the append action to complete
		stp.logger.Infof("[SubtreeProcessor] append subtree: %s", subtree.RootHash().String())
		stp.chainedSubtrees = append(stp.chainedSubtrees, subtree)

		if len(skipNotification) == 0 || !skipNotification[0] {
			// Send the subtree to the newSubtreeChan
			stp.newSubtreeChan <- subtree
		}
	}
}

// Add adds a tx hash to a channel
func (stp *SubtreeProcessor) Add(hash chainhash.Hash, fee uint64, sizeInBytes uint64, optionalWaitCh ...chan struct{}) {
	if len(optionalWaitCh) > 0 {
		stp.txChan <- &txIDAndFee{
			txID:        hash,
			fee:         fee,
			sizeInBytes: sizeInBytes,
			waitCh:      optionalWaitCh[0],
		}
		return
	}
	stp.txChan <- &txIDAndFee{
		txID:        hash,
		fee:         fee,
		sizeInBytes: sizeInBytes,
	}
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

	for _, block := range moveDownBlocks {
		err := stp.moveDownBlock(ctx, block)
		if err != nil {
			return err
		}
	}

	for idx, block := range moveUpBlocks {
		// we skip the notifications for all but the last block
		err := stp.moveUpBlock(ctx, block, idx != len(moveUpBlocks)-1)
		if err != nil {
			return err
		}
	}

	stp.setTxCount()

	return nil
}

func (stp *SubtreeProcessor) setTxCount() {
	stp.txCount = 0
	for _, subtree := range stp.chainedSubtrees {
		stp.txCount += uint64(subtree.Length())
	}
	stp.txCount += uint64(stp.currentSubtree.Length())
}

// moveDownBlock adds all transactions that are in the block given to the current subtrees
// TODO handle conflicting transactions
func (stp *SubtreeProcessor) moveDownBlock(ctx context.Context, block *model.Block) error {
	if block == nil {
		return errors.New("you must pass in a block to moveDownBlock")
	}

	lastIncompleteSubtree := stp.currentSubtree
	chainedSubtrees := stp.chainedSubtrees

	// TODO add check for the correct parent block

	// reset the subtree processor
	stp.currentSubtree = util.NewTreeByLeafCount(stp.currentItemsPerFile)
	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddNode(model.CoinbasePlaceholderHash, 0, 0)

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
			var utxoHash *chainhash.Hash
			for outputIdx, output := range block.CoinbaseTx.Outputs {
				utxoHash, err = util.UTXOHashFromOutput(block.CoinbaseTx.TxIDChainHash(), output, uint32(outputIdx))
				if err != nil {
					return fmt.Errorf("error creating utxo hash: %s", err.Error())
				}

				if _, err = stp.utxoStore.Delete(ctx, utxoHash); err != nil {
					return fmt.Errorf("error deleting utxo (%s): %s", utxoHash, err.Error())
				}
			}

			// skip the first transaction of the first subtree (coinbase)
			for i := 1; i < len(subtree.Nodes); i++ {
				stp.addNode(*subtree.Nodes[i].Hash, subtree.Nodes[i].Fee, subtree.Nodes[i].SizeInBytes, true)
			}
		} else {
			for _, node := range subtree.Nodes {
				stp.addNode(*node.Hash, node.Fee, node.SizeInBytes, true)
			}
		}
	}

	// add all the transactions from the previous state
	for _, subtree := range chainedSubtrees {
		for _, node := range subtree.Nodes {
			stp.addNode(*node.Hash, node.Fee, node.SizeInBytes, true)
		}
	}

	// add all the transactions from the last incomplete subtree
	for _, node := range lastIncompleteSubtree.Nodes {
		stp.addNode(*node.Hash, node.Fee, node.SizeInBytes, true)
	}

	// we must set the current block header
	stp.currentBlockHeader = block.Header

	return nil
}

// moveUpBlock cleans out all transactions that are in the current subtrees and also in the block
// given. It is akin moving up the blockchain to the next block.
// TODO handle conflicting transactions
func (stp *SubtreeProcessor) moveUpBlock(ctx context.Context, block *model.Block, skipNotification bool) error {
	if block == nil {
		return errors.New("you must pass in a block to moveUpBlock")
	}

	if !block.Header.HashPrevBlock.IsEqual(stp.currentBlockHeader.Hash()) {
		return fmt.Errorf("the block passed in does not match the current block header: [%s] - [%s]", block.Header.StringDump(), stp.currentBlockHeader.StringDump())
	}

	stp.logger.Infof("moveUpBlock with block %s", block.String())
	stp.logger.Debugf("resetting subtrees: %v", block.Subtrees)

	err := stp.processCoinbaseUtxos(ctx, block)
	if err != nil {
		return err
	}

	// TODO revert the block changes back to the original configuration if an error occurred
	// create a reverse lookup map of all the subtrees in the block
	blockSubtreesMap := make(map[chainhash.Hash]int, len(block.Subtrees))
	for idx, subtree := range block.Subtrees {
		blockSubtreesMap[*subtree] = idx
	}

	// copy the current subtree into a temp variable
	lastIncompleteSubtree := stp.currentSubtree
	// reset the current subtree
	stp.currentSubtree = util.NewTreeByLeafCount(stp.currentItemsPerFile)

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
			return fmt.Errorf("error creating transaction map: %s", err.Error())
		}
	}

	// TODO make sure there are no transactions in our tx chan buffer that were in the block
	//      or are they going to be caught by the tx meta lookup?

	var remainderTxHashes *[]util.SubtreeNode
	if transactionMap != nil && transactionMap.Length() > 0 {
		remainderSubtrees := make([]*util.Subtree, 0, len(chainedSubtrees)+1)

		remainderSubtrees = append(remainderSubtrees, chainedSubtrees...)
		remainderSubtrees = append(remainderSubtrees, lastIncompleteSubtree)

		remainderTxHashes = stp.getRemainderTxHashes(remainderSubtrees, transactionMap)
		// chainedSubtrees = nil
	} else {
		chainedSubtreeSize := 0
		if len(chainedSubtrees) > 0 {
			// just use the first subtree, each subtree should be the same size
			chainedSubtreeSize = chainedSubtrees[0].Size()
		}
		r := make([]util.SubtreeNode, 0, (len(chainedSubtrees)*chainedSubtreeSize)+len(lastIncompleteSubtree.Nodes))
		remainderTxHashes = &r

		*remainderTxHashes = append(*remainderTxHashes, lastIncompleteSubtree.Nodes...)

		for _, subtree := range chainedSubtrees {
			*remainderTxHashes = append(*remainderTxHashes, subtree.Nodes...)
		}
	}

	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddNode(model.CoinbasePlaceholderHash, 0, 0)

	// remainderTxHashes is from early trees, so they need to be added before the current subtree nodes
	if remainderTxHashes != nil {
		for _, node := range *remainderTxHashes {
			if !node.Hash.Equal(*model.CoinbasePlaceholderHash) {
				stp.addNode(*node.Hash, node.Fee, node.SizeInBytes, skipNotification)
			}
		}
	}

	stp.setTxCount()

	// set the current block header
	stp.currentBlockHeader = block.Header

	return nil
}

func (stp *SubtreeProcessor) processCoinbaseUtxos(ctx context.Context, block *model.Block) error {
	if block == nil || block.CoinbaseTx == nil {
		log.Printf("********************************************* block or coinbase is nil")
		return nil
	}
	// add the utxos from the block coinbase
	txIDHash := block.CoinbaseTx.TxIDChainHash()
	// TODO this does not work for the early blocks in Bitcoin
	blockHeight, err := block.ExtractCoinbaseHeight()
	if err != nil {
		return fmt.Errorf("error extracting coinbase height: %v", err)
	}

	var utxoHash *chainhash.Hash
	var utxoHashes []*chainhash.Hash
	var resp *utxostore.UTXOResponse
	var success = true
	for i, output := range block.CoinbaseTx.Outputs {
		if output.Satoshis > 0 {
			utxoHash, err = util.UTXOHashFromOutput(txIDHash, output, uint32(i))
			if err != nil {
				stp.logger.Errorf("[BlockAssembler] error creating utxo hash: %v", err)
				success = false
				break
			}

			if resp, err = stp.utxoStore.Store(ctx, utxoHash, blockHeight+100); err != nil {
				stp.logger.Errorf("[BlockAssembler] error storing utxo (%v): %v", resp, err)
				success = false
				break
			}

			// this is used to revert if something goes wrong
			utxoHashes = append(utxoHashes, utxoHash)
		}
	}

	if err != nil || !success {
		// revert the utxos we just added
		for _, utxoHash = range utxoHashes {
			_, err = stp.utxoStore.Delete(ctx, utxoHash)
			if err != nil {
				stp.logger.Errorf("[BlockAssembler] error reverting utxo (%s): %w", utxoHash, err)
			}
		}

		return fmt.Errorf("error storing utxo (%s): %w", utxoHash, err)
	}

	return nil
}

func (stp *SubtreeProcessor) getRemainderTxHashes(chainedSubtrees []*util.Subtree, transactionMap *util.SplitSwissMap) *[]util.SubtreeNode {
	var hashCount atomic.Int64

	// clean out the transactions from the old current subtree that were in the block
	// and add the remainderSubtreeNodes to the new current subtree
	var wg sync.WaitGroup
	// we need to process this in order, so we first process all subtrees in parallel, but keeping the order
	remainderSubtreeHashes := make([][]util.SubtreeNode, len(chainedSubtrees))
	for idx, subtree := range chainedSubtrees {
		wg.Add(1)
		go func(idx int, st *util.Subtree) {
			defer wg.Done()

			remainingTransactions, err := st.Difference(transactionMap)
			if err != nil {
				stp.logger.Errorf("error calculating difference: %s", err.Error())
				return
			}

			for _, txHash := range remainingTransactions {
				// TODO add fee ???
				remainderSubtreeHashes[idx] = append(remainderSubtreeHashes[idx], txHash)
				hashCount.Add(1)
			}
		}(idx, subtree)
	}
	wg.Wait()

	// add all found tx hashes to the final list
	remainderSubtreeNodes := make([]util.SubtreeNode, 0, hashCount.Load())
	for _, subtreeHashes := range remainderSubtreeHashes {
		remainderSubtreeNodes = append(remainderSubtreeNodes, subtreeHashes...)
	}
	return &remainderSubtreeNodes
}

func (stp *SubtreeProcessor) createTransactionMap(ctx context.Context, blockSubtreesMap map[chainhash.Hash]int) (*util.SplitSwissMap, error) {
	mapSize := len(blockSubtreesMap) * 1024 * 1024 // TODO fix this assumption, should be gleaned from the block
	transactionMap := util.NewSplitSwissMap(mapSize)

	g, ctx := errgroup.WithContext(ctx)

	// get all the subtrees from the block that we have not yet cleaned out
	for subtreeHash := range blockSubtreesMap {
		st := subtreeHash
		g.Go(func() error {
			stp.logger.Infof("getting subtree: %s", st.String())
			subtreeBytes, err := stp.subtreeStore.Get(ctx, st[:])
			var subtree *util.Subtree
			if err != nil {
				return fmt.Errorf("error getting subtree: %s", err.Error())
			}

			subtree = &util.Subtree{}
			err = subtree.Deserialize(subtreeBytes)
			if err != nil {
				return fmt.Errorf("error deserializing subtree: %s", err.Error())
			}

			for _, node := range subtree.Nodes {
				_ = transactionMap.Put(*node.Hash)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("error getting subtrees: %s", err.Error())
	}

	return transactionMap, nil
}
