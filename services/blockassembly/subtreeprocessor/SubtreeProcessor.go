package subtreeprocessor

import (
	"errors"
	"sync"

	"github.com/TAAL-GmbH/ubsv/model"
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

type resetRequest struct {
	job     *Job
	errChan chan error
}

type SubtreeProcessor struct {
	currentItemsPerFile int
	txChan              chan *txIDAndFee
	incomingBlockChan   chan string
	getSubtreesChan     chan chan []*util.Subtree
	resetChan           chan resetRequest
	appendSubtreeChan   chan *util.Subtree // used when appending a new subtree to the chainedSubtrees list
	newSubtreeChan      chan *util.Subtree // used to notify of a new subtree
	chainedSubtrees     []*util.Subtree
	currentSubtree      *util.Subtree
	sync.Mutex
	incompleteSubtrees map[chainhash.Hash]*util.Subtree
	logger             utils.Logger
}

var (
	ExpectedNumberOfSubtrees = 1024 // this is the number of subtrees we expect to be in a block, with a subtree create about every second
)

func NewSubtreeProcessor(logger utils.Logger, newSubtreeChan chan *util.Subtree) *SubtreeProcessor {
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
		resetChan:           make(chan resetRequest),
		appendSubtreeChan:   make(chan *util.Subtree, 100),
		newSubtreeChan:      newSubtreeChan,
		chainedSubtrees:     make([]*util.Subtree, 0, ExpectedNumberOfSubtrees),
		currentSubtree:      firstSubtree,
		incompleteSubtrees:  make(map[chainhash.Hash]*util.Subtree, 0),
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

			case subtree := <-stp.appendSubtreeChan:
				logger.Infof("[SubtreeProcessor] append subtree")
				stp.chainedSubtrees = append(stp.chainedSubtrees, subtree)
				// Send the subtree to the newSubtreeChan
				stp.newSubtreeChan <- subtree

			case resetReq := <-stp.resetChan:
				logger.Infof("[SubtreeProcessor] reset subtree processor")
				// Reset the state of the subtree processor to the state of the subtree that contains the resetHash
				err := stp.reset(resetReq.job)
				resetReq.errChan <- err

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
		stp.appendSubtreeChan <- subtree
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

// Reset the subtrees when a new block is found
func (stp *SubtreeProcessor) Reset(job *Job) error {
	errChan := make(chan error)
	stp.resetChan <- resetRequest{
		job:     job,
		errChan: errChan,
	}

	return <-errChan
}

func (stp *SubtreeProcessor) reset(job *Job) error {
	stp.logger.Infof("resetting the subtrees with last root %s", utils.ReverseAndHexEncodeHash(*job.ID))
	stp.logger.Debugf("resetting subtrees: %v", job.Subtrees)

	// we need to shuffle all the transaction along because of the new coinbase placeholder

	if job.ID == nil {
		return errors.New("you must pass in the job ID")
	}

	// check that all the subtrees are still in the processor
	// this should still be locked in the go routines so we should be safe
	subtreesMap := make(map[chainhash.Hash]int, len(stp.chainedSubtrees))
	for idx, subtree := range stp.chainedSubtrees {
		subtreesMap[*subtree.RootHash()] = idx
	}

	for _, subtree := range job.Subtrees {
		if _, ok := subtreesMap[*subtree.RootHash()]; !ok {
			return errors.New("the subtree is not in the processor: " + subtree.RootHash().String())
		}
	}

	jobSubtreesMap := make(map[chainhash.Hash]int, len(job.Subtrees))
	for idx, subtree := range job.Subtrees {
		jobSubtreesMap[*subtree.RootHash()] = idx
	}

	// create SET to look up the transactions that need to be removed from the incomplete subtrees
	var incompleteSubtreeFilterMap map[chainhash.Hash]struct{}
	if stp.incompleteSubtrees[*job.ID] != nil {
		incompleteSubtreeFilterMap = make(map[chainhash.Hash]struct{}, stp.incompleteSubtrees[*job.ID].Length())
		for _, node := range stp.incompleteSubtrees[*job.ID].Nodes {
			incompleteSubtreeFilterMap[*node] = struct{}{}
		}
	}

	currentSubtree := stp.currentSubtree
	stp.currentSubtree = util.NewTreeByLeafCount(stp.currentItemsPerFile)

	chainedSubtrees := make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)
	for _, subtree := range chainedSubtrees {
		_, ok := jobSubtreesMap[*subtree.RootHash()]
		if ok {
			chainedSubtrees = append(chainedSubtrees, subtree)
		}
	}

	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)

	// add first coinbase placeholder transaction
	_ = stp.currentSubtree.AddNode(model.CoinbasePlaceholderHash, 0)

	fees := uint64(0)
	for _, node := range currentSubtree.Nodes {
		if _, ok := incompleteSubtreeFilterMap[*node]; !ok {
			stp.addNode(*node, 0)
		}
	}
	fees += currentSubtree.Fees

	if incompleteSubtreeFilterMap != nil {
		// this prevents repeating the map lookup in every iteration of the loop
		for _, subtree := range chainedSubtrees {
			for _, node := range subtree.Nodes {
				if _, ok := incompleteSubtreeFilterMap[*node]; !ok {
					stp.addNode(*node, 0)
				}
			}
			fees += subtree.Fees
		}
	} else {
		for _, subtree := range chainedSubtrees {
			for _, node := range subtree.Nodes {
				stp.addNode(*node, 0)
			}
			fees += subtree.Fees
		}
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
