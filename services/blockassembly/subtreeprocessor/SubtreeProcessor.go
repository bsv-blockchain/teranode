package subtreeprocessor

import (
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
				err := stp.currentSubtree.AddNode(txReq.txID, txReq.fee)
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

// Add adds a txid to a channel
func (stp *SubtreeProcessor) Add(txid chainhash.Hash, fee uint64, optionalWaitCh ...chan struct{}) {
	if len(optionalWaitCh) > 0 {
		stp.txChan <- &txIDAndFee{
			txID:   txid,
			fee:    fee,
			waitCh: optionalWaitCh[0],
		}
		return
	}

	stp.txChan <- &txIDAndFee{
		txID: txid,
		fee:  fee,
	}
}

// Report new height
func (stp *SubtreeProcessor) Report(height uint64) {

}
