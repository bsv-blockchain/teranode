package miner

import (
	"context"
	"encoding/binary"
	"math/big"
	"time"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2"
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
	r := make([]byte, 32)

	m.logger.Infof("%x", r)
	m.logger.Infof("%s", target.String())

	timeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(timeBytes, candidate.Time)

	var nonce uint32
	nonceBytes := make([]byte, 4)

	for {
		binary.LittleEndian.PutUint32(nonceBytes, nonce)

		header := BuildBlockHeader(candidate.Version, candidate.PreviousHash, merkleRoot, timeBytes, candidate.NBits, nonceBytes)
		hash := Sha256d(header)

		var hashInt big.Int
		hashInt.SetBytes(hash[:])

		if hashInt.Cmp(target) == -1 {
			break
		}

		nonce++
	}

	// Create a new block
	// block := bt.NewBlock()
	// block.Header.Version = 1
	// block.Header.Timestamp = time.Now()
	// block.Header.Bits = candidate.Bits
	// block.Header.Nonce = 0
	// block.Header.MerkleRoot = candidate.MerkleRoot
	// block.Header.PreviousBlockHash = candidate.PreviousBlockHash
	// block.Header.Coinbase1 = a
	// block.Header.Coinbase2 = b

	// m.blockAssemblyClient.Store(ctx context.Context, hash *chainhash.Hash)
}
