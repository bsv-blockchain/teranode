package subtreeprocessor

import (
	"errors"
	"sync"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/gocore"
)

type txIDAndFee struct {
	txID   chainhash.Hash
	fee    uint64
	waitCh chan struct{}
}

type SubtreeProcessor struct {
	currentItemsPerFile int
	txChan              chan *txIDAndFee
	incomingBlockChan   chan string
	chainedSubtrees     []*util.Subtree
	currentSubtree      *util.Subtree
	sync.Mutex
	incompleteSubtrees map[chainhash.Hash]*util.Subtree
}

func NewSubtreeProcessor(newSubtreeChan chan *util.Subtree) *SubtreeProcessor {
	initialItemsPerFile, _ := gocore.Config().GetInt("initial_merkle_items_per_subtree", 1_048_576)

	stp := &SubtreeProcessor{
		currentItemsPerFile: initialItemsPerFile,
		txChan:              make(chan *txIDAndFee, 100_000),
		incomingBlockChan:   make(chan string),
		chainedSubtrees:     make([]*util.Subtree, 0),
		currentSubtree:      util.NewTreeByLeafCount(initialItemsPerFile),
		incompleteSubtrees:  make(map[chainhash.Hash]*util.Subtree, 0),
	}

	go func() {
		for {
			select {
			case <-stp.incomingBlockChan:
				// Notified of another miner's validated block, so I need to process it.  This might be internal or external.

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
	// TODO: may need mutex
	stp.Lock()
	defer stp.Unlock()

	if len(stp.chainedSubtrees) == 0 {
		// no chained subtrees so get a sub-subtree from the current subtree
		if stp.currentSubtree.Length() == 0 {
			return nil
		}

		subsubtree := util.NewIncompleteTreeByLeafCount(stp.currentSubtree.Length())
		subsubtree.Nodes = stp.currentSubtree.Nodes
		subsubtree.Height = stp.currentSubtree.Height
		subsubtree.Fees = stp.currentSubtree.Fees

		subsubArray := make([]*util.Subtree, 0)
		subsubArray = append(subsubArray, subsubtree)
		stp.incompleteSubtrees[*subsubtree.RootHash()] = subsubtree
		return subsubArray
	}

	return stp.chainedSubtrees
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
	// clear incomplete subtrees
	stp.incompleteSubtrees = make(map[chainhash.Hash]*util.Subtree, 0)

	//TODO: calculate new items per file size

	// indexToSlice is where to remove the mined transactions from
	var indexToSlice = -1

	// we need to shuffle all the transaction along because of the new coinbase placeholder
	stp.Lock()
	defer stp.Unlock()

	if lastRoot == nil {
		return errors.New("you must pass in the last root")
	}
	// find the index of the last root in the block that was just mined
	for i, subtree := range stp.chainedSubtrees {
		if *subtree.RootHash() == [32]byte(lastRoot) {
			indexToSlice = i + 1
			break
		}
	}

	if indexToSlice == -1 {
		return errors.New("the last root hash %x was not found in the chained subtrees")
	}

	chainedSubtrees := stp.chainedSubtrees[indexToSlice:]
	chainedSubtrees = append(chainedSubtrees, stp.currentSubtree)

	stp.chainedSubtrees = make([]*util.Subtree, 0)
	stp.Add(*model.CoinbasePlaceholderHash, 0)

	fees := uint64(0)
	for _, subtree := range chainedSubtrees {
		for _, node := range subtree.Nodes {
			stp.Add(*node, 0)
		}
		fees += subtree.Fees
	}

	if len(stp.chainedSubtrees) > 0 {
		stp.chainedSubtrees[len(stp.chainedSubtrees)-1].Fees = fees
	} else {
		stp.currentSubtree.Fees = fees
	}

	return nil
}
