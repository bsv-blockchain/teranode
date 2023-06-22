package miner

import (
	"context"
	"math/big"
	"time"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Miner struct {
	logger              utils.Logger
	blockAssemblyClient *blockassembly.Client
}

const (
	// The number of seconds to wait before requesting a new mining candidate
	candidateRequestInterval = 10

	// The number of seconds to wait before submitting a mining solution
	blockFoundInterval = 100
)

func NewMiner() *Miner {
	return &Miner{
		logger:              gocore.Log("miner"),
		blockAssemblyClient: blockassembly.NewClient(),
	}
}

func (m *Miner) Start() {
	candidateTimer := time.NewTimer(candidateRequestInterval * time.Second)
	blockFoundTimer := time.NewTimer(blockFoundInterval * time.Second)

	for {
		select {
		case <-candidateTimer.C:
			candidateTimer.Reset(candidateRequestInterval * time.Second)
			candidate, err := m.blockAssemblyClient.GetMiningCandidate(context.Background())
			if err != nil {
				m.logger.Errorf("Error getting mining candidate: %v", err)
				continue
			}

			m.Mine(candidate)

			m.logger.Infof(candidate.Stringify())

		case <-blockFoundTimer.C:
			blockFoundTimer.Reset(blockFoundInterval * time.Second)
			m.logger.Infof("Submitting mining solution...")
			// m.blockAssemblyClient.SubmitMiningSolution()
		}
	}

}

func (m *Miner) Mine(candidate *model.MiningCandidate) {
	// Create a new coinbase transaction
	a, b, err := GetCoinbaseParts(candidate.Height, candidate.CoinbaseValue, "/TERANODE/", "18VWHjMt4ixHddPPbs6righWTs3Sg2QNcn")
	if err != nil {
		m.logger.Errorf("Error creating coinbase transaction: %v", err)
		return
	}

	// The extranonce length is 12 bytes.  We need to add 12 bytes to the coinbase a part
	extranonce := make([]byte, 12)
	a = append(a, extranonce...)
	a = append(a, b...)

	coinbaseTx, err := bt.NewTxFromBytes(a)
	if err != nil {
		m.logger.Errorf("Error decoding coinbase transaction: %v", err)
		return
	}

	merkleRoot := BuildMerkleRootFromCoinbase(coinbaseTx.TxIDBytes(), candidate.MerkleProof)

	target := util.CalculateTarget(candidate.NBits)
	previousHash, _ := chainhash.NewHash(candidate.PreviousHash)
	merkleRootHash, _ := chainhash.NewHash(merkleRoot)

	var nonce uint32
	for {
		blockHeader := model.BlockHeader{
			Version:        candidate.Version,
			HashPrevBlock:  previousHash,
			HashMerkleRoot: merkleRootHash,
			Timestamp:      candidate.Time,
			Bits:           candidate.NBits,
			Nonce:          nonce,
		}

		//  57896037716911750921221705069588091649609539881711309849342236841432341020672
		// 105246604674077689286984806481918053301334584768133419539070562900731587447610

		var hashInt big.Int
		hashInt.SetBytes(blockHeader.Hash()[:])

		if hashInt.Cmp(target) == -1 {
			m.logger.Infof("Miner Block found! target was %s > %s", target.String(), hashInt.String())
			m.logger.Infof("Miner Block header: %#v", blockHeader)
			m.logger.Infof("Miner Block header hash: %s", blockHeader.Hash().String())
			m.logger.Infof("Miner Block previous hash: %s", blockHeader.HashPrevBlock.String())
			m.logger.Infof("Miner Block merkleroot: %s", blockHeader.HashMerkleRoot.String())
			break
		}

		nonce++
	}

	err = m.blockAssemblyClient.SubmitMiningSolution(context.Background(), candidate.Id, coinbaseTx.Bytes(), uint32(time.Now().Unix()), nonce, 1)
	if err != nil {
		m.logger.Errorf("Error submitting mining solution: %v", err)
	}
}
