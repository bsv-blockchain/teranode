package subtreeprocessor

import (
	"errors"
	"fmt"
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

type resetRequest struct {
	lastRoot []byte
	errChan  chan error
}

type SubtreeProcessor struct {
	currentItemsPerFile int
	txChan              chan *txIDAndFee
	incomingBlockChan   chan string
	getSubtreesChan     chan chan []*util.Subtree
	resetChan           chan resetRequest
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
		chainedSubtrees:     make([]*util.Subtree, 0, ExpectedNumberOfSubtrees),
		currentSubtree:      firstSubtree,
		incompleteSubtrees:  make(map[chainhash.Hash]*util.Subtree, 0),
		logger:              logger,
	}

	go func() {
		for {
			select {
			case <-stp.incomingBlockChan:
				// Notified of another miner's validated block, so I need to process it.  This might be internal or external.
				// TODO if we get a block in and the txChan buffer is still full of txs?

			case getSubtreesChan := <-stp.getSubtreesChan:
				chainedSubtrees := make([]*util.Subtree, len(stp.chainedSubtrees))
				copy(chainedSubtrees, stp.chainedSubtrees)

				getSubtreesChan <- chainedSubtrees

			case resetReq := <-stp.resetChan:
				// Reset the state of the subtree processor to the state of the subtree that contains the resetHash
				err := stp.reset(resetReq.lastRoot)
				resetReq.errChan <- err

			case txReq := <-stp.txChan:
				err := stp.currentSubtree.AddNode(&txReq.txID, txReq.fee)
				if err != nil {
					panic(err)
				}

				if stp.currentSubtree.IsComplete() {
					stp.chainedSubtrees = append(stp.chainedSubtrees, stp.currentSubtree)
					// Send the subtree to the newSubtreeChan
					newSubtreeChan <- stp.currentSubtree

					stp.currentSubtree = util.NewTreeByLeafCount(stp.currentItemsPerFile)
				}

				if txReq.waitCh != nil {
					txReq.waitCh <- struct{}{}
				}
			}
		}
	}()

	return stp
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
func (stp *SubtreeProcessor) Reset(lastRoot []byte) error {
	errChan := make(chan error)
	stp.resetChan <- resetRequest{
		lastRoot: lastRoot,
		errChan:  errChan,
	}

	return <-errChan
}

func (stp *SubtreeProcessor) reset(lastRoot []byte) error {
	stp.logger.Infof("resetting the subtrees with last root %s\n", utils.ReverseAndHexEncodeSlice(lastRoot))

	//TODO: calculate new items per file size

	// indexToSlice is where to remove the mined transactions from
	var indexToSlice = -1

	// we need to shuffle all the transaction along because of the new coinbase placeholder

	if lastRoot == nil {
		return errors.New("you must pass in the last root")
	}

	lastRootHash, err := chainhash.NewHash(lastRoot)
	if err != nil {
		return fmt.Errorf("error converting last root hash %x to chainhash.Hash: %v", lastRoot, err)
	}

	// find the index of the last root in the block that was just mined
	for i, subtree := range stp.chainedSubtrees {
		if *subtree.RootHash() == *lastRootHash {
			indexToSlice = i + 1
			break
		}
	}

	// create SET to look up the transactions that need to be removed from the incomplete subtrees
	var incompleteSubtreeFilterMap map[chainhash.Hash]struct{}
	if stp.incompleteSubtrees[*lastRootHash] != nil {
		incompleteSubtreeFilterMap = make(map[chainhash.Hash]struct{}, stp.incompleteSubtrees[*lastRootHash].Length())
		for _, node := range stp.incompleteSubtrees[*lastRootHash].Nodes {
			incompleteSubtreeFilterMap[*node] = struct{}{}
		}
	} else if indexToSlice == -1 {
		return errors.New("the last root hash %x was not found in the chained subtrees")
	}

	// SAO - I commented out the following line because the linter was complaining about it
	// chainedSubtrees := make([]*util.Subtree, 0, len(stp.chainedSubtrees)+1)
	var chainedSubtrees []*util.Subtree
	if indexToSlice == -1 {
		// chained subtree not found, this mean that the last root hash pointed to an incomplete subtree
		chainedSubtrees = append(stp.chainedSubtrees, stp.currentSubtree)
	} else {
		chainedSubtrees = append(stp.chainedSubtrees[indexToSlice:], stp.currentSubtree)
	}

	stp.currentSubtree = util.NewTreeByLeafCount(stp.currentItemsPerFile)

	stp.chainedSubtrees = make([]*util.Subtree, 0, ExpectedNumberOfSubtrees)
	stp.Add(*model.CoinbasePlaceholderHash, 0)

	fees := uint64(0)
	if indexToSlice == -1 {
		// this prevents repeating the map lookup in every iteration of the loop, if indexToSlice == -1
		for _, subtree := range chainedSubtrees {
			for _, node := range subtree.Nodes {
				if _, ok := incompleteSubtreeFilterMap[*node]; !ok {
					stp.Add(*node, 0)
				}
			}
			fees += subtree.Fees
		}
	} else {
		for _, subtree := range chainedSubtrees {
			for _, node := range subtree.Nodes {
				stp.Add(*node, 0)
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
