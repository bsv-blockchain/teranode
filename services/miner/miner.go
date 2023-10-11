package miner

import (
	"context"
	"fmt"
	"hash/maphash"
	"math/rand"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/miner/cpuminer"
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

func NewMiner(ctx context.Context) *Miner {
	initPrometheusMetrics()

	logger := gocore.Log("miner")
	return &Miner{
		logger:              logger,
		blockAssemblyClient: blockassembly.NewClient(ctx, logger),
	}
}

func (m *Miner) Init(_ context.Context) error {
	return nil
}

func (m *Miner) Start(ctx context.Context) error {
	m.candidateTimer = time.NewTimer(2 * time.Second) // wait 2 seconds before starting

	m.logger.Infof("[Miner] Starting miner with candidate interval: %ds, block found interval %ds", candidateRequestInterval, blockFoundInterval)

	var miningCtx context.Context
	var cancel context.CancelFunc

	_, cancel = context.WithCancel(context.Background())
	for {
		select {
		case <-ctx.Done():
			m.logger.Infof("[Miner] Stopping miner as ctx is done")
			cancel()
			return nil // context cancelled
		case <-m.candidateTimer.C:
			m.candidateTimer.Reset(candidateRequestInterval * time.Second)

			// cancel the previous mining context and start a new one
			cancel()
			miningCtx, cancel = context.WithCancel(context.Background())

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

func (m *Miner) Stop(ctx context.Context) error {
	m.logger.Infof("[Miner] Stopping miner")
	m.candidateTimer.Stop()

	return nil
}

func (m *Miner) mine(ctx context.Context) error {
	timeStart := time.Now()

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
	if candidate.Height > 200 && waitSeconds > 0 { // SAO - Mine the first 200 blocks without delay
		r := rand.New(rand.NewSource(int64(new(maphash.Hash).Sum64())))
		randWait := r.Intn(waitSeconds)

		m.logger.Warnf("[Miner] Found block on job %s, waiting %ds before submitting", candidateId, randWait)

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
		m.logger.Warnf("[Miner] Found block on job %s, submitting", candidateId)
	}

	m.logger.Infof("[Miner] submitting mining solution: %s", candidateId)
	m.logger.Debugf(solution.Stringify())

	ctx, ctxCancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))
	defer ctxCancel()

	err = m.blockAssemblyClient.SubmitMiningSolution(ctx, solution)
	if err != nil {
		return fmt.Errorf("error submitting mining solution for job %s: %v", candidateId, err)
	}

	prometheusBlockMined.Inc()
	prometheusBlockMinedDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return nil
}
