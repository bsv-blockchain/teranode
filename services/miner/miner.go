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

func (m *Miner) Start(ctx context.Context) error {
	m.candidateTimer = time.NewTimer(candidateRequestInterval * time.Second)

	m.logger.Infof("Starting miner with candidate interval: %ds, block found interval %ds", candidateRequestInterval, blockFoundInterval)

	for {
		select {
		case <-ctx.Done():
			m.logger.Infof("Stopping miner as ctx is done")
			return nil // context cancelled
		case <-m.candidateTimer.C:
			m.candidateTimer.Reset(candidateRequestInterval * time.Second)

			candidate, err := m.blockAssemblyClient.GetMiningCandidate(ctx)
			if err != nil {
				m.logger.Warnf("Error getting mining candidate: %v", err)
				continue
			}
			m.logger.Infof(candidate.Stringify())

			solution, err := cpuminer.Mine(ctx, candidate)
			if err != nil {
				m.logger.Errorf("Error mining block: %v", err)
				return err
			}

			m.logger.Infof("submitting mining solution: %s", utils.ReverseAndHexEncodeSlice(solution.Id))
			m.logger.Infof(solution.Stringify())
			err = m.blockAssemblyClient.SubmitMiningSolution(context.Background(), solution)
			if err != nil {
				m.logger.Errorf("Error submitting mining solution: %v", err)
				return err
			}
		}
	}
}

func (m *Miner) Stop(ctx context.Context) {
	m.logger.Infof("Stopping miner")
	m.candidateTimer.Stop()
}
