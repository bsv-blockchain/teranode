package subtreeprocessor

import (
	"log"

	"github.com/libsv/go-p2p/blockchain"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
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
	getFeesChan       chan chan uint64
	getLengthChan     chan chan int
	incomingBlockChan chan string
	newSubTreeChan    chan utils.Pair[[]chainhash.Hash, uint64]
	txIDs             []chainhash.Hash
	totalFees         uint64
	itemCount         uint32
}

func NewSubtreeProcessor() *SubtreeProcessor {
	maxItemsPerFile, _ := gocore.Config().GetInt("merkle_items_per_subtree", 1_048_576)

	stp := &SubtreeProcessor{
		maxItemsPerFile:   uint32(maxItemsPerFile),
		txChan:            make(chan *txIDAndFee, 100_000),
		getFeesChan:       make(chan chan uint64),
		getLengthChan:     make(chan chan int),
		incomingBlockChan: make(chan string),
		newSubTreeChan:    make(chan utils.Pair[[]chainhash.Hash, uint64], 5),
	}

	go func() {
		for {
			newSubTree := <-stp.newSubTreeChan
			log.Printf("New subtree: %d txs, %d fees", len(newSubTree.First), newSubTree.Second)
		}
	}()

	go func() {
		for {
			select {
			case <-stp.incomingBlockChan:
				// Notified of another miner's block, so I need to process it.  This might be internal or external.

			case txReq := <-stp.txChan:
				if stp.itemCount == stp.maxItemsPerFile {
					// Send the current txIDs to the block assembly service
					stp.newSubTreeChan <- utils.NewPair(stp.txIDs, stp.totalFees)

					stp.txIDs = stp.txIDs[:0]
					stp.itemCount = 0
					stp.totalFees = 0
				}

				stp.txIDs = append(stp.txIDs, txReq.txID)
				stp.totalFees += txReq.fee
				stp.itemCount++

				if txReq.waitCh != nil {
					txReq.waitCh <- struct{}{}
				}

			case responseChan := <-stp.getLengthChan:
				responseChan <- len(stp.txIDs)

			case responseChan := <-stp.getFeesChan:
				responseChan <- stp.totalFees
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

func (stp *SubtreeProcessor) Length() <-chan int {
	responseChan := make(chan int)

	stp.getLengthChan <- responseChan

	return responseChan
}

func (stp *SubtreeProcessor) TotalFees() <-chan uint64 {
	responseChan := make(chan uint64)

	stp.getFeesChan <- responseChan

	return responseChan
}

// Report new height
func (stp *SubtreeProcessor) Report(height uint64) {

}

func (stp *SubtreeProcessor) MerkleRoot(coinbase *chainhash.Hash) (*chainhash.Hash, error) {

	count := len(stp.txIDs)

	transactionHashes := make([][]byte, count)

	for i := 0; i < count; i++ {
		if coinbase != nil && i == 0 {
			transactionHashes[i] = coinbase.CloneBytes()
		} else {
			transactionHashes[i] = stp.txIDs[i].CloneBytes()
		}
	}

	calculatedMerkleRoot := blockchain.BuildMerkleTreeStore(transactionHashes)

	hash, err := chainhash.NewHash(calculatedMerkleRoot[len(calculatedMerkleRoot)-1])
	if err != nil {
		return nil, err
	}

	return hash, nil
}
