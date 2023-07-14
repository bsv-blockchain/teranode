package miner

import (
	"context"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/blockassembly"
	"github.com/TAAL-GmbH/ubsv/services/miner/cpuminer"
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

		solution, err := cpuminer.Mine(ctx, candidate)
		if err != nil {
			m.logger.Errorf("Error mining block: %v", err)
		}

		m.logger.Infof("submitting mining solution: %s", utils.ReverseAndHexEncodeSlice(solution.Id))
		err = m.blockAssemblyClient.SubmitMiningSolution(context.Background(), solution)
		if err != nil {
			m.logger.Errorf("Error submitting mining solution: %v", err)
		}
	}
	cancelCtx()
}

func (m *Miner) Stop(ctx context.Context) {
	m.logger.Infof("Stopping miner")
	m.candidateTimer.Stop()
}
