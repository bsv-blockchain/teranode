package subtreeprocessor

import (
	"errors"
	"fmt"
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
}

func NewSubtreeProcessor(newSubtreeChan chan *util.Subtree) *SubtreeProcessor {
	initialItemsPerFile, _ := gocore.Config().GetInt("initial_merkle_items_per_subtree", 1_048_576)

	stp := &SubtreeProcessor{
		currentItemsPerFile: initialItemsPerFile,
		txChan:              make(chan *txIDAndFee, 100_000),
		incomingBlockChan:   make(chan string),
		chainedSubtrees:     make([]*util.Subtree, 0),
		currentSubtree:      util.NewTreeByLeafCount(initialItemsPerFile),
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

	return nil
}

func (stp *SubtreeProcessor) GetMerkleProofForCoinbase() ([]*chainhash.Hash, error) {
	if len(stp.chainedSubtrees) == 0 {
		return nil, fmt.Errorf("no subtrees available")
	}

	merkleProof, err := stp.chainedSubtrees[0].GetMerkleProof(0)
	if err != nil {
		return nil, fmt.Errorf("failed creating merkle proof for subtree: %v", err)
	}

	// Create a new tree with the subtreeHashes of the subtrees
	topTree := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(stp.chainedSubtrees)))
	for _, subtree := range stp.chainedSubtrees {
		err = topTree.AddNode(subtree.RootHash(), subtree.Fees)
		if err != nil {
			return nil, err
		}
	}

	topMerkleProof, err := topTree.GetMerkleProof(0)
	if err != nil {
		return nil, fmt.Errorf("failed creating merkle proofs for toptree: %v", err)
	}

	return append(merkleProof, topMerkleProof...), nil
}

// Reset the subtrees when a new block is found
func (stp *SubtreeProcessor) Reset(lastRoot []byte) error {
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

	// flatten the trees
	flat := make([]*chainhash.Hash, len(stp.chainedSubtrees[indexToSlice:])*len(stp.chainedSubtrees[0].Nodes)+len(stp.currentSubtree.Nodes)+1)
	flat[0] = model.CoinbasePlaceholderHash
	// loop through all the chained subtrees from the index to slice
	flatIndex := 1 // already added the coinbase place holder
	for i := indexToSlice; i < len(stp.chainedSubtrees); i++ {
		// loop through all the nodes in the subtree
		for _, node := range stp.chainedSubtrees[i].Nodes {
			if node == nil {
				panic(fmt.Sprintf("chained subtree node %d is nil", i))
			}
			flat[flatIndex] = node
			flatIndex++
		}
	}
	// add the current subtree to flat
	for j, node := range stp.currentSubtree.Nodes {
		if node == nil {
			panic(fmt.Sprintf("current subtree node %d is nil", j))
		}
		flat[flatIndex] = node
		flatIndex++
	}

	// now split into the new size
	newSize := stp.currentItemsPerFile
	stp.chainedSubtrees = nil

	var tmpOverflow []*chainhash.Hash

	if len(flat)%newSize != 0 {
		// we have a remainder so we need to add it to the current subtree
		// but this could take the current subtree over the limit
		// so we need to split it
		splitIndex := len(flat) - len(flat)%newSize
		tmpOverflow = flat[splitIndex:]
		flat = flat[:splitIndex]
	}
	// loop through all the txs in flat
	for i := 0; i < len(flat); i += newSize {
		s := util.NewTreeByLeafCount(newSize)
		for j := i; j < i+newSize; j++ {
			s.Nodes = append(s.Nodes, flat[j])
		}
		stp.chainedSubtrees = append(stp.chainedSubtrees, s)
	}

	if len(tmpOverflow) > 0 {
		// add remainder to the current subtree
		s := util.NewTreeByLeafCount(stp.currentItemsPerFile)
		s.Nodes = append(s.Nodes, tmpOverflow...)
		// TODO add fees etc
		stp.currentSubtree = s
	} else {
		stp.currentSubtree = util.NewTreeByLeafCount(stp.currentItemsPerFile)
	}
	return nil
}
