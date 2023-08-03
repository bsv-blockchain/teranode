package miner

import (
	"context"
	"fmt"
	"math/rand"
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
	logLevel, _ := gocore.Config().Get("logLevel")
	return &Miner{
		logger:              gocore.Log("miner", gocore.NewLogLevelFromString(logLevel)),
		blockAssemblyClient: blockassembly.NewClient(),
	}
}

func (m *Miner) Start(ctx context.Context) error {
	m.candidateTimer = time.NewTimer(2 * time.Second) // wait 2 seconds before starting

	m.logger.Infof("Starting miner with candidate interval: %ds, block found interval %ds", candidateRequestInterval, blockFoundInterval)

	miningCtx, cancel := context.WithCancel(ctx)
	for {
		select {
		case <-ctx.Done():
			m.logger.Infof("Stopping miner as ctx is done")
			cancel()
			return nil // context cancelled
		case <-m.candidateTimer.C:
			m.candidateTimer.Reset(candidateRequestInterval * time.Second)

			// cancel the previous mining context and start a new one
			cancel()
			miningCtx, cancel = context.WithCancel(ctx)

			// start mining in a new goroutine, so we can cancel it if we need to
			go func(ctx context.Context) {
				err := m.mine(ctx)
				if err != nil {
					m.logger.Warnf("[Miner]: %v", err)
				} else {
					// start the timer now, so we don't have to wait for the next tick
					m.candidateTimer.Reset(0)
				}
			}(miningCtx)
		}
	}
}

func (m *Miner) Stop(ctx context.Context) {
	m.logger.Infof("Stopping miner")
	m.candidateTimer.Stop()
}

func (m *Miner) mine(ctx context.Context) error {
	// wait is simulating a high difficulty
	waitSeconds, _ := gocore.Config().GetInt("miner_waitSeconds", 30)

	candidate, err := m.blockAssemblyClient.GetMiningCandidate(ctx)
	if err != nil {
		return fmt.Errorf("error getting mining candidate: %v", err)
	}
	m.logger.Debugf(candidate.Stringify())

	candidateId := utils.ReverseAndHexEncodeSlice(candidate.Id)

	solution, err := cpuminer.Mine(ctx, candidate)
	if err != nil {
		return fmt.Errorf("error mining block on %s: %v", candidateId, err)
	}

	if solution == nil {
		return fmt.Errorf("no solution found for %s", candidateId)
	}

	// Wait a bit before submitting the solution to simulate high difficulty
	if waitSeconds > 0 {
		randWait := rand.Intn(waitSeconds)

		m.logger.Warnf("Found block on job %s, waiting %ds before submitting", candidateId, randWait)

	MineWait:
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("canceled mining on job %s", candidateId)
			default:
				time.Sleep(1 * time.Second)
				randWait--
				if randWait <= 0 {
					break MineWait
				}
			}
		}
	} else {
		m.logger.Warnf("Found block on job %s, submitting", candidateId)
	}

	m.logger.Infof("submitting mining solution: %s", candidateId)
	m.logger.Debugf(solution.Stringify())
	err = m.blockAssemblyClient.SubmitMiningSolution(context.Background(), solution)
	if err != nil {
		return fmt.Errorf("error submitting mining solution for job %s: %v", candidateId, err)
	}

	return nil
}
