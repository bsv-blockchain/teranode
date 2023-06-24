package miner

import (
	"context"
	"log"
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

	m.logger.Infof("Starting miner with candidate interval: %ds, block found interval %ds", candidateRequestInterval, blockFoundInterval)

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

	merkleRoot := util.BuildMerkleRootFromCoinbase(bt.ReverseBytes(coinbaseTx.TxIDBytes()), candidate.MerkleProof)

	previousHash, _ := chainhash.NewHash(candidate.PreviousHash)
	merkleRootHash, _ := chainhash.NewHash(merkleRoot)

	var nonce uint32 = 2573394689 // TEMP hardcode the nonce
	for {
		blockHeader := model.BlockHeader{
			Version:        candidate.Version,
			HashPrevBlock:  previousHash,
			HashMerkleRoot: merkleRootHash,
			Timestamp:      candidate.Time,
			Bits:           model.NewNBitFromSlice(candidate.NBits),
			Nonce:          nonce,
		}

		log.Printf("BlockX hash: %s", blockHeader.Hash().String())

		headerValid, err := blockHeader.Valid()
		if err != nil {
			m.logger.Errorf("invalid block header: %s - %v", blockHeader.Hash().String(), err)
			return
		}

		if headerValid { // header is valid if the hash is less than the target
			break
		}

		// TODO: remove this when Siggi gets a laptop without a fan...
		// 😂
		time.Sleep(10 * time.Millisecond)

		nonce++
	}

	err = m.blockAssemblyClient.SubmitMiningSolution(context.Background(), candidate.Id, coinbaseTx.Bytes(), candidate.Time, nonce, 1)
	if err != nil {
		m.logger.Errorf("Error submitting mining solution: %v", err)
	}
}
