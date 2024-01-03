package miner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/miner/cpuminer"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Miner struct {
	logger                           ulogger.Logger
	blockAssemblyClient              *blockassembly.Client
	candidateTimer                   *time.Timer
	waitSeconds                      int
	MineBlocksNImmediatelyChan       chan int
	MineBlocksNImmediatelyCancelChan chan bool
	isMiningImmediately              bool
}

const (
	// The number of seconds to wait before requesting a new mining candidate
	candidateRequestInterval = 10

	// The number of seconds to wait before submitting a mining solution
	blockFoundInterval = 100
)

func NewMiner(ctx context.Context, logger ulogger.Logger) *Miner {
	initPrometheusMetrics()
	return &Miner{
		logger:              logger,
		blockAssemblyClient: blockassembly.NewClient(ctx, logger),
	}
}

func (m *Miner) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (m *Miner) Init(_ context.Context) error {
	m.MineBlocksNImmediatelyChan = make(chan int, 1)
	m.MineBlocksNImmediatelyCancelChan = make(chan bool, 1)
	return nil
}

func (m *Miner) Start(ctx context.Context) error {

	listenAddress, ok := gocore.Config().Get("miner_httpListenAddress")
	if !ok {
		m.logger.Fatalf("[Miner] No miner_httpListenAddress specified")
	}
	go func() {
		http.HandleFunc("/", m.handler)
		err := http.ListenAndServe(listenAddress, nil)
		if err != nil {
			m.logger.Fatalf("[Miner] Error starting http server: %v", err)
		}
	}()

	m.candidateTimer = time.NewTimer(2 * time.Second) // wait 2 seconds before starting

	// Wait a bit before submitting the solution to simulate high difficulty
	// wait is simulating a high difficulty
	m.waitSeconds, _ = gocore.Config().GetInt("miner_waitSeconds", 30)

	m.logger.Infof("[Miner] Starting miner with candidate interval: %ds, block found interval %ds", candidateRequestInterval, blockFoundInterval)

	var miningCtx context.Context
	var cancel context.CancelFunc

	for {
		select {

		case <-ctx.Done():
			m.logger.Infof("[Miner] Stopping miner as ctx is done")
			if cancel != nil {
				cancel()
			}
			return nil

		case blocks := <-m.MineBlocksNImmediatelyChan:
			m.logger.Infof("[Miner] Mining %d blocks immediately - START", blocks)
			err := m.mineBlocks(ctx, blocks)
			if err != nil {
				m.logger.Errorf("[Miner] %v", err)
			}
			m.logger.Infof("[Miner] Mining %d blocks immediately - DONE", blocks)

		case <-m.candidateTimer.C:
			m.candidateTimer.Reset(candidateRequestInterval * time.Second)

			// cancel the previous mining context and start a new one
			if cancel != nil {
				cancel()
			}
			miningCtx, cancel = context.WithCancel(context.Background())
			defer cancel() // Ensure cancel is called at the end of each iteration

			// start mining in a new goroutine, so we can cancel it if we need to
			go func(ctx context.Context) {
				err := m.mine(ctx, m.waitSeconds)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						m.logger.Infof("[Miner]: %v", err)
					} else {
						m.logger.Errorf("[Miner]: %v", err)
					}
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

func (m *Miner) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		if r.URL.Path[1:] == "mine" {
			if r.URL.Query().Get("blocks") != "" {
				if !m.isMiningImmediately {
					blocks, _ := strconv.Atoi(r.URL.Query().Get("blocks"))
					m.MineBlocksNImmediatelyChan <- blocks
				}
			} else if r.URL.Query().Get("cancel") != "" {
				if m.isMiningImmediately {
					m.MineBlocksNImmediatelyCancelChan <- true
				}
			}
		}
	}
}

func (m *Miner) mine(ctx context.Context, waitSeconds int) error {
	timeStart := time.Now()

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

	initialBlockCount, _ := gocore.Config().GetInt("mine_initial_blocks_count", 200)

	if gocore.Config().GetBool("mine_initial_blocks", false) && candidate.Height < uint32(initialBlockCount) {
		waitSeconds = 0
	}

	if waitSeconds > 0 { // SAO - Mine the first <initialBlockCount> blocks without delay
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		randWait := r.Intn(waitSeconds)

		blockHash, _ := chainhash.NewHash(solution.BlockHash)

		m.logger.Infof("[Miner] Found block solution %s, waiting %ds before submitting", blockHash.String(), randWait)

	MineWait:
		for {
			select {
			case <-ctx.Done():
				return context.Canceled
			default:
				time.Sleep(1 * time.Second)
				randWait--
				if randWait <= 0 {
					break MineWait
				}
			}
		}
	} else {
		blockHash, _ := chainhash.NewHash(solution.BlockHash)

		m.logger.Infof("[Miner] Found block solution %s, submitting", blockHash.String())
	}

	m.logger.Infof("[Miner] submitting mining solution: %s", candidateId)
	m.logger.Debugf(solution.Stringify())

	err = m.blockAssemblyClient.SubmitMiningSolution(ctx, solution)
	if err != nil {
		return fmt.Errorf("error submitting mining solution for job %s: %v", candidateId, err)
	}

	prometheusBlockMined.Inc()
	prometheusBlockMinedDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return nil
}

func (m *Miner) mineBlocks(ctx context.Context, blocks int) error {
	m.isMiningImmediately = true
	defer func() {
		m.isMiningImmediately = false
	}()

	var previousHash *chainhash.Hash

	for i := 0; i < blocks; i++ {
		m.logger.Infof("[Miner] Mining block %d of %d", i+1, blocks)

		candidate, err := m.miningCandidate(ctx, blocks, previousHash)
		if err != nil {
			return err
		}
		previousHash, _ = chainhash.NewHash(candidate.PreviousHash)

		m.logger.Debugf(candidate.Stringify())

		candidateId := utils.ReverseAndHexEncodeSlice(candidate.Id)

		solution, err := cpuminer.Mine(ctx, candidate)
		if err != nil {
			return fmt.Errorf("error mining block on %s: %v", candidateId, err)
		}
		if solution == nil {
			return fmt.Errorf("no solution found for %s", candidateId)
		}

		err = m.blockAssemblyClient.SubmitMiningSolution(ctx, solution)
		if err != nil {
			return fmt.Errorf("error submitting mining solution for job %s: %v", candidateId, err)
		}
	}
	return nil
}

func (m *Miner) miningCandidate(ctx context.Context, blocks int, previousHash *chainhash.Hash) (*model.MiningCandidate, error) {
	var candidate *model.MiningCandidate
	var err error

	// Initialize backoff parameters
	minBackoff := 100 * time.Millisecond
	maxBackoff := 10 * time.Second
	currentBackoff := minBackoff

	maxRetries := 5
	retryCount := 0

	for {
		select {

		case <-ctx.Done():
			return nil, fmt.Errorf("[Miner] canceled mining on job %s", candidate.Id)

		case <-m.MineBlocksNImmediatelyCancelChan:
			m.logger.Infof("[Miner] Cancelled mining %d blocks immediately", blocks)
			if candidate == nil {
				return nil, fmt.Errorf("[Miner] aborting mining on job %s", "unknown")
			}
			return nil, fmt.Errorf("[Miner] aborting mining on job %s", candidate.Id)

		default:

			candidate, err = m.blockAssemblyClient.GetMiningCandidate(ctx)
			if err != nil {
				return nil, fmt.Errorf("[Miner] error getting mining candidate: %v", err)
			}
			if candidate == nil {
				return nil, fmt.Errorf("[Miner] no mining candidate found")
			}
			if previousHash == nil || !bytes.Equal(previousHash[:], candidate.PreviousHash) {
				return candidate, nil
			}
			// If the previous hash is the same, apply exponential backoff
			m.logger.Infof("[Miner] Got same previous hash %s, waiting %s before retrying", previousHash.String(), currentBackoff.String())
			time.Sleep(currentBackoff)
			// Double the backoff time for the next iteration, but don't exceed maxBackoff
			currentBackoff = time.Duration(float64(currentBackoff) * 2)
			if currentBackoff > maxBackoff {
				currentBackoff = maxBackoff
			}
			// Add some jitter to prevent synchronized retries
			currentBackoff += time.Duration(rand.Int63n(int64(currentBackoff / 10)))

			retryCount++
			if retryCount > maxRetries {
				return nil, fmt.Errorf("[Miner] max retries exceeded")
			}
		}
	}
}
