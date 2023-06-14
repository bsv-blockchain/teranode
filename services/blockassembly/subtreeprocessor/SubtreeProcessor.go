package subtreeprocessor

import (
	"fmt"

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
	maxItemsPerFile   uint32
	txChan            chan *txIDAndFee
	incomingBlockChan chan string
	chainedSubtrees   []*util.Subtree
	currentSubtree    *util.Subtree
}

func NewSubtreeProcessor(newSubtreeChan chan *util.Subtree) *SubtreeProcessor {
	maxItemsPerFile, _ := gocore.Config().GetInt("merkle_items_per_subtree", 1_048_576)

	stp := &SubtreeProcessor{
		maxItemsPerFile:   uint32(maxItemsPerFile),
		txChan:            make(chan *txIDAndFee, 100_000),
		incomingBlockChan: make(chan string),
		chainedSubtrees:   make([]*util.Subtree, 0),
		currentSubtree:    util.NewTreeByLeafCount(maxItemsPerFile),
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

					stp.currentSubtree = util.NewTreeByLeafCount(maxItemsPerFile)
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

// Report new height
func (stp *SubtreeProcessor) Report(height uint64) {

}
