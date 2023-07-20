package subtreeprocessor

import (
	"context"
	"errors"
	"sync"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/stores/blob"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type txIDAndFee struct {
	txID   chainhash.Hash
	fee    uint64
	waitCh chan struct{}
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

type SubtreeProcessor struct {
	currentItemsPerFile int
	txChan              chan *txIDAndFee
	incomingBlockChan   chan string
	getSubtreesChan     chan chan []*util.Subtree
	moveDownBlockChan   chan moveBlockRequest
	moveUpBlockChan     chan moveBlockRequest
	newSubtreeChan      chan *util.Subtree // used to notify of a new subtree
	chainedSubtrees     []*util.Subtree
	currentSubtree      *util.Subtree
	sync.Mutex
	incompleteSubtrees map[chainhash.Hash]*util.Subtree
	subtreeStore       blob.Store
	logger             utils.Logger
}

var (
	ExpectedNumberOfSubtrees = 1024 // this is the number of subtrees we expect to be in a block, with a subtree create about every second
)

func NewSubtreeProcessor(logger utils.Logger, subtreeStore blob.Store, newSubtreeChan chan *util.Subtree) *SubtreeProcessor {
	initialItemsPerFile, _ := gocore.Config().GetInt("initial_merkle_items_per_subtree", 1_048_576)

	firstSubtree := util.NewTreeByLeafCount(initialItemsPerFile)
	// We add a placeholder for the coinbase tx because we know this is the first subtree in the chain
	if err := firstSubtree.AddNode(model.CoinbasePlaceholderHash, 0); err != nil {
		panic(err)
	}

	stp := &SubtreeProcessor{
		currentItemsPerFile: initialItemsPerFile,
		txChan:              make(chan *txIDAndFee, 100_000),
		incomingBlockChan:   make(chan string),
		getSubtreesChan:     make(chan chan []*util.Subtree),
		moveDownBlockChan:   make(chan moveBlockRequest),
		moveUpBlockChan:     make(chan moveBlockRequest),
		newSubtreeChan:      newSubtreeChan,
		chainedSubtrees:     make([]*util.Subtree, 0, ExpectedNumberOfSubtrees),
		currentSubtree:      firstSubtree,
		incompleteSubtrees:  make(map[chainhash.Hash]*util.Subtree, 0),
		subtreeStore:        subtreeStore,
		logger:              logger,
	}

	go func() {
		for {
			select {
			case <-stp.incomingBlockChan:
				logger.Infof("[SubtreeProcessor] received block from another miner")
				// Notified of another miner's validated block, so I need to process it.  This might be internal or external.
				// TODO if we get a block in and the txChan buffer is still full of txs?

			case getSubtreesChan := <-stp.getSubtreesChan:
				logger.Infof("[SubtreeProcessor] get current subtrees")
				chainedSubtrees := make([]*util.Subtree, len(stp.chainedSubtrees))
				copy(chainedSubtrees, stp.chainedSubtrees)

				getSubtreesChan <- chainedSubtrees

			case moveDownReq := <-stp.moveDownBlockChan:
				logger.Infof("[SubtreeProcessor] moveDownBlock subtree processor")
				err := stp.moveDownBlock(moveDownReq.block)
				moveDownReq.errChan <- err

			case moveUpReq := <-stp.moveUpBlockChan:
				logger.Infof("[SubtreeProcessor] moveUpBlock subtree processor")
				err := stp.moveUpBlock(moveUpReq.block)
				moveUpReq.errChan <- err

			case txReq := <-stp.txChan:
				stp.addNode(txReq.txID, txReq.fee)
				if txReq.waitCh != nil {
					txReq.waitCh <- struct{}{}
				}
			}
		}
	}()

	return stp
}

func (stp *SubtreeProcessor) addNode(txID chainhash.Hash, fee uint64) {
	err := stp.currentSubtree.AddNode(&txID, fee)
	if err != nil {
		panic(err)
	}

	if stp.currentSubtree.IsComplete() {
		subtree := stp.currentSubtree
		stp.currentSubtree = util.NewTreeByLeafCount(stp.currentItemsPerFile)

		// Add the subtree to the chain
		// this needs to happen here, so we can wait for the append action to complete
		stp.logger.Infof("[SubtreeProcessor] append subtree: %s", subtree.RootHash().String())
		stp.chainedSubtrees = append(stp.chainedSubtrees, subtree)
		// Send the subtree to the newSubtreeChan
		stp.newSubtreeChan <- subtree
	}
}

// Add adds a tx hash to a channel
func (stp *SubtreeProcessor) Add(hash chainhash.Hash, fee uint64, optionalWaitCh ...chan struct{}) {
	if len(optionalWaitCh) > 0 {
		stp.txChan <- &txIDAndFee{
			txID:   hash,
			fee:    fee,
			waitCh: optionalWaitCh[0],
		}
		return
	}
	stp.txChan <- &txIDAndFee{
		txID: hash,
		fee:  fee,
	}
}

func (stp *SubtreeProcessor) GetCompletedSubtreesForMiningCandidate() []*util.Subtree {
	stp.logger.Infof("GetCompletedSubtreesForMiningCandidate")
	var subtrees []*util.Subtree
	subtreesChan := make(chan []*util.Subtree)

	// get the subtrees from channel
	stp.getSubtreesChan <- subtreesChan

	subtrees = <-subtreesChan

	// if len(subtrees) == 0 {
	// TODO implement incomplete subtrees
	//	// no chained subtrees so get a sub-subtree from the current subtree
	//	if stp.currentSubtree.Length() == 0 {
	//		return nil
	//	}
	//
	//	subsubtree := util.NewIncompleteTreeByLeafCount(stp.currentSubtree.Length())
	//	subsubtree.Nodes = stp.currentSubtree.Nodes
	//	subsubtree.Height = stp.currentSubtree.Height
	//	subsubtree.Fees = stp.currentSubtree.Fees
	//
	//	subsubArray := make([]*util.Subtree, 0)
	//	subsubArray = append(subsubArray, subsubtree)
	//	stp.incompleteSubtrees[*subsubtree.RootHash()] = subsubtree
	//
	//	return subsubArray
	//}

	return subtrees
}

func (stp *SubtreeProcessor) GetCompleteSubtreesForJob(lastRoot []byte) []*util.Subtree {
	// TODO: may need mutex
	var indexToSlice = -1

	for i, subtree := range stp.chainedSubtrees {
		if *subtree.RootHash() == [32]byte(lastRoot) {
			indexToSlice = i
			break
		}
	}
	if indexToSlice != -1 {
		return stp.chainedSubtrees[:indexToSlice+1]
	}

	// check the incomplete subtrees
	key, err := chainhash.NewHash(lastRoot)
	if err != nil {
		return nil
	}

	if stp.incompleteSubtrees[*key] != nil {
		return []*util.Subtree{stp.incompleteSubtrees[*key]}
	}

	return nil
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

// moveDownBlock adds all transactions that are in the block given to the current subtrees
// TODO handle conflicting transactions
func (stp *SubtreeProcessor) moveDownBlock(block *model.Block) error {
	return nil
}

// moveUpBlock cleans out all transactions that are in the current subtrees and also in the block
// given. It is akin moving up the blockchain to the next block.
// TODO handle conflicting transactions
func (stp *SubtreeProcessor) moveUpBlock(block *model.Block) error {
	if block == nil {
		return errors.New("you must pass in a block to moveUpBlock")
	}

	stp.logger.Infof("resetting the subtrees with block %s", block.String())
	stp.logger.Debugf("resetting subtrees: %v", block.Subtrees)

	blockSubtreesMap := make(map[chainhash.Hash]int, len(block.Subtrees))
	for idx, subtree := range block.Subtrees {
		blockSubtreesMap[*subtree] = idx
	}

	currentSubtree := stp.currentSubtree
	stp.currentSubtree = util.NewTreeByLeafCount(stp.currentItemsPerFile)

	// get all the subtrees that were not in the block
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
		mapSize := len(blockSubtreesMap) * 1024 * 1024 // TODO fix this assumption, should be gleaned from the block
		transactionMap = util.NewSplitSwissMap(mapSize)

		var wg sync.WaitGroup

		// get all the subtrees from the block that we have not yet cleaned out
		for subtreeHash := range blockSubtreesMap {
			wg.Add(1)
			go func(st chainhash.Hash) {
				defer wg.Done()

				stp.logger.Infof("getting subtree: %s", st.String())
				subtreeBytes, err := stp.subtreeStore.Get(context.Background(), st[:])
				if err != nil {
					stp.logger.Errorf("error getting subtree: %s", err.Error())
					return
				}

				subtree := &util.Subtree{}
				err = subtree.Deserialize(subtreeBytes)
				if err != nil {
					stp.logger.Errorf("error deserializing subtree: %s", err.Error())
					return
				}

				for _, node := range subtree.Nodes {
					_ = transactionMap.Put(*node)
				}
			}(subtreeHash)
		}

		wg.Wait()
	}

	// TODO make sure there are no transactions in our tx chan buffer that were in the block

	if transactionMap != nil && transactionMap.Length() > 0 {
		// clean out the transactions from the old current subtree that were in the block
		// and add the remainder to the new current subtree

		var wg sync.WaitGroup

		for _, subtree := range chainedSubtrees {
			wg.Add(1)

			go func(st *util.Subtree) {
				defer wg.Done()

				remainingTransactions, err := st.Difference(transactionMap)
				if err != nil {
					stp.logger.Errorf("error calculating difference: %s", err.Error())
					return
				}

				for _, txHash := range remainingTransactions {
					_ = stp.currentSubtree.AddNode(txHash, 0)
				}
			}(subtree)
		}
		wg.Wait()
	}

	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddNode(model.CoinbasePlaceholderHash, 0)

	fees := currentSubtree.Fees

	for _, subtree := range chainedSubtrees {
		for _, node := range subtree.Nodes {
			stp.addNode(*node, 0)
		}
		fees += subtree.Fees
	}

	for _, node := range currentSubtree.Nodes {
		stp.addNode(*node, 0)
	}

	if len(stp.chainedSubtrees) > 0 {
		stp.chainedSubtrees[len(stp.chainedSubtrees)-1].Fees = fees
	} else {
		stp.currentSubtree.Fees = fees
	}

	// clear incomplete subtrees
	stp.incompleteSubtrees = make(map[chainhash.Hash]*util.Subtree, 0)

	return nil
}
