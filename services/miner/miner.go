package miner

import (
	"context"
	"log"
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
	/*
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
	*/
	// TEMP - use the same coinbase transaction as block 1
	coinbaseTx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000")
	if err != nil {
		m.logger.Errorf("Error decoding coinbase transaction: %v", err)
		return
	}

	merkleRoot := util.BuildMerkleRootFromCoinbase(coinbaseTx.TxIDBytes(), candidate.MerkleProof)

	target := model.NewNBitFromSlice(candidate.NBits).CalculateTarget()
	previousHash, _ := chainhash.NewHash(candidate.PreviousHash)
	merkleRootHash, _ := chainhash.NewHash(merkleRoot)

	var nonce uint32 = 2573394689
	for {
		var (
			block1          = "010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000982051fd1e4ba744bbbe680e1fee14677ba1a3c3540bf7b1cdb606e857233e0e61bc6649ffff001d01e362990101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
			block1Header    = block1[:160]
			version         = block1Header[:8]
			previousHash2   = block1Header[8:72]
			merkleRootHash2 = block1Header[72:136]
			time2           = block1Header[136:144]
			nBits           = block1Header[144:152]
			nonce2          = block1Header[152:160]
		)
		log.Printf("Version: %s", version)
		log.Printf("Previous hash: %s", previousHash2)
		log.Printf("Merkle root hash: %s", merkleRootHash2)
		log.Printf("Time: %s", time2)
		log.Printf("nBits: %s", nBits)
		log.Printf("Nonce: %s", nonce2)

		log.Printf("Block1 header: %s", block1Header)

		blockHeader := model.BlockHeader{
			Version:        candidate.Version,
			HashPrevBlock:  previousHash,
			HashMerkleRoot: merkleRootHash,
			Timestamp:      1231469665, //candidate.Time,
			Bits:           model.NewNBitFromSlice(candidate.NBits),
			Nonce:          nonce,
		}

		log.Printf("BlockX header: %x", blockHeader.Bytes())

		log.Printf("BlockX hash: %s", blockHeader.Hash().String())

		//  57896037716911750921221705069588091649609539881711309849342236841432341020672
		// 105246604674077689286984806481918053301334584768133419539070562900731587447610

		var hashInt big.Int
		hashInt.SetString(blockHeader.Hash().String(), 16)

		log.Printf("Target:  %032x", target.Bytes())
		log.Printf("HashInt: %032x", hashInt.Bytes())

		if hashInt.Cmp(target) <= 0 {
			m.logger.Infof("Miner Block found! target was %s > %s", target.String(), hashInt.String())
			m.logger.Infof("Miner Block header: %#v", blockHeader)
			m.logger.Infof("Miner Block header hash: %s", blockHeader.Hash().String())
			m.logger.Infof("Miner Block previous hash: %s", blockHeader.HashPrevBlock.String())
			m.logger.Infof("Miner Block merkleroot: %s", blockHeader.HashMerkleRoot.String())
			mp := make([]string, len(candidate.MerkleProof))
			for idx, mpp := range candidate.MerkleProof {
				h, _ := chainhash.NewHash(mpp)
				mp[idx] = h.String()
			}
			m.logger.Infof("Miner Block coinbase hash: %s", coinbaseTx.TxID())
			m.logger.Infof("Miner Block merkleproofs: %v", mp)
			break
		}

		// TODO: remove this when Siggi gets a laptop without a fan...
		// 😂
		time.Sleep(10 * time.Millisecond)

		nonce++
	}

	err = m.blockAssemblyClient.SubmitMiningSolution(context.Background(), candidate.Id, coinbaseTx.Bytes(), uint32(time.Now().Unix()), nonce, 1)
	if err != nil {
		m.logger.Errorf("Error submitting mining solution: %v", err)
	}
}
