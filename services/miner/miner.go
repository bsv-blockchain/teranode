package miner

import (
	"context"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/blockassembly"
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

			m.logger.Infof("Got candidate: %v", candidate)

		case <-blockFoundTimer.C:
			blockFoundTimer.Reset(blockFoundInterval * time.Second)
			m.logger.Infof("Submitting mining solution...")
			// m.blockAssemblyClient.SubmitMiningSolution()
		}
	}

}
