package miner

import (
	"context"
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
	candidateTimer      *time.Timer
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
	m.candidateTimer = time.NewTimer(candidateRequestInterval * time.Second)

	m.logger.Infof("Starting miner with candidate interval: %ds, block found interval %ds", candidateRequestInterval, blockFoundInterval)

	ctx, cancelCtx := context.WithCancel(context.Background())
	for range m.candidateTimer.C {
		m.candidateTimer.Reset(candidateRequestInterval * time.Second)
		// cancel the previous context
		cancelCtx()

		// create a new context to mine in
		ctx, cancelCtx = context.WithCancel(context.Background())

		candidate, err := m.blockAssemblyClient.GetMiningCandidate(ctx)
		if err != nil {
			m.logger.Errorf("Error getting mining candidate: %v", err)
			continue
		}
		m.logger.Infof(candidate.Stringify())

		m.Mine(ctx, candidate)
	}
	cancelCtx()
}

func (m *Miner) Stop(ctx context.Context) {
	m.logger.Infof("Stopping miner")
	m.candidateTimer.Stop()
}

func (m *Miner) Mine(ctx context.Context, candidate *model.MiningCandidate) {
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

	merkleRoot := util.BuildMerkleRootFromCoinbase(bt.ReverseBytes(coinbaseTx.TxIDBytes()), candidate.MerkleProof)

	previousHash, _ := chainhash.NewHash(candidate.PreviousHash)
	merkleRootHash, _ := chainhash.NewHash(merkleRoot)

	var nonce uint32

miningLoop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
			blockHeader := model.BlockHeader{
				Version:        candidate.Version,
				HashPrevBlock:  previousHash,
				HashMerkleRoot: merkleRootHash,
				Timestamp:      candidate.Time,
				Bits:           model.NewNBitFromSlice(candidate.NBits),
				Nonce:          nonce,
			}

			headerValid, _ := blockHeader.Valid()
			if headerValid { // header is valid if the hash is less than the target
				break miningLoop
			}

			// TODO: remove this when Siggi gets a laptop without a fan...
			// 😂
			time.Sleep(10 * time.Millisecond)

			nonce++
		}
	}

	m.logger.Infof("submitting mining solution: %s", utils.ReverseAndHexEncodeSlice(candidate.Id))
	err = m.blockAssemblyClient.SubmitMiningSolution(context.Background(), candidate.Id, coinbaseTx.Bytes(), candidate.Time, nonce, 1)
	if err != nil {
		m.logger.Errorf("Error submitting mining solution: %v", err)
	}
}
