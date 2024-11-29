package miner

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/miner/cpuminer"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/bitcoin-sv/ubsv/util/retry"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Miner struct {
	logger                           ulogger.Logger
	settings                         *settings.Settings
	blockchainClient                 blockchain.ClientI
	blockAssemblyClient              *blockassembly.Client
	candidateTimer                   *time.Timer
	waitSeconds                      int
	MineBlocksNImmediatelyChan       chan int
	MineBlocksNImmediatelyCancelChan chan bool
	isMiningImmediately              bool
	candidateRequestInterval         time.Duration
	maxSubtreeCount                  int
}

const (

	// The number of seconds to wait before submitting a mining solution
	blockFoundInterval = 100
)

func NewMiner(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI) (*Miner, error) {
	initPrometheusMetrics()

	// The number of seconds to wait before requesting a new mining candidate
	candidateRequestInterval, _ := gocore.Config().GetInt("mine_candidate_request_interval", 10)

	maxSubtreeCount, _ := gocore.Config().GetInt("miner_max_subtree_count", 600)
	maxSubtreeCountVariance, _ := gocore.Config().GetInt("miner_max_subtree_count_variance", 100)

	//nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	maxSubtreeCount = maxSubtreeCount + maxSubtreeCountVariance - r.Intn(maxSubtreeCountVariance*2)

	blockAssemblyClient, err := blockassembly.NewClient(ctx, logger, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("[NewMiner] failed to create block assembly client", err)
	}

	return &Miner{
		logger:                   logger,
		settings:                 tSettings,
		blockAssemblyClient:      blockAssemblyClient,
		candidateRequestInterval: time.Duration(candidateRequestInterval),
		maxSubtreeCount:          maxSubtreeCount,
		blockchainClient:         blockchainClient,
	}, nil
}

func (m *Miner) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := make([]health.Check, 0, 3)

	if m.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: m.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(m.blockchainClient)})
	}

	if m.blockAssemblyClient != nil {
		checks = append(checks, health.Check{Name: "BlockAssemblyClient", Check: m.blockAssemblyClient.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (m *Miner) Init(ctx context.Context) error {
	m.MineBlocksNImmediatelyChan = make(chan int, 1)
	m.MineBlocksNImmediatelyCancelChan = make(chan bool, 1)

	return nil
}

func (m *Miner) Start(ctx context.Context) error {
	listenAddress, ok := gocore.Config().Get("miner_httpListenAddress")
	if !ok {
		return errors.NewConfigurationError("[Miner] No miner_httpListenAddress specified")
	}
	server := &http.Server{
		Addr:         listenAddress,
		Handler:      nil,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		http.HandleFunc("/mine", m.handler)

		err := server.ListenAndServe()
		if err != nil {
			m.logger.Fatalf("[Miner] Error starting http server: %v", err)
		}
	}()

	m.candidateTimer = time.NewTimer(2 * time.Second) // wait 2 seconds before starting

	// Wait a bit before submitting the solution to simulate high difficulty
	// wait is simulating a high difficulty
	m.waitSeconds, _ = gocore.Config().GetInt("miner_waitSeconds", 30)

	m.logger.Infof("[Miner] Starting miner with candidate interval: %ds, block found interval %ds", m.candidateRequestInterval, blockFoundInterval)

	var (
		miningCtx         context.Context
		cancel            context.CancelFunc
		previousCandidate *model.MiningCandidate
	)

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

			if err := m.mineBlocks(ctx, blocks); err != nil {
				m.logger.Errorf("[Miner] %v", err)
			}

			m.logger.Infof("[Miner] Mining %d blocks immediately - DONE", blocks)

		case <-m.candidateTimer.C:
			m.candidateTimer.Reset(m.candidateRequestInterval * time.Second)

			// cancel the previous mining context and start a new one
			if cancel != nil {
				cancel()
			}

			miningCtx, cancel = context.WithCancel(ctx)
			defer cancel() // Ensure cancel is called at the end of each iteration

			var (
				candidate *model.MiningCandidate
				err       error
			)

			candidate, err = m.blockAssemblyClient.GetMiningCandidate(miningCtx)
			if err != nil {
				m.logger.Warnf("error getting mining candidate: %v", err)
				continue
			}

			startHeight, _ := gocore.Config().GetInt("mine_start_height", 0)

			if candidate.Height <= uint32(startHeight) { //nolint:gosec
				m.logger.Infof("[Miner] Node has not reached start height %d, skipping mining", startHeight)
				continue
			}

			if previousCandidate != nil && bytes.Equal(candidate.Id, previousCandidate.Id) {
				m.logger.Infof("[Miner] Got same candidate as previous, skipping %s", utils.ReverseAndHexEncodeSlice(candidate.Id))
				m.candidateTimer.Reset(0)

				continue
			}

			previousCandidate = candidate
			waitSeconds := m.waitSeconds

			if candidate.SubtreeCount > uint32(m.maxSubtreeCount) { //nolint:gosec
				// mine without waiting
				m.logger.Infof("candidate subtree count (%d) exceeds max subtree count (%d), mining immediately", candidate.SubtreeCount, m.maxSubtreeCount)
				waitSeconds = 1
			}
			// start mining in a new goroutine, so we can cancel it if we need to
			go func(miningCtx context.Context) {
				err := m.mine(miningCtx, candidate, waitSeconds)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "Canceled desc = context canceled") {
						m.logger.Infof("[Miner]: stopped waiting for new candidate (will start over)")
					} else {
						m.logger.Warnf("[Miner] %v", err)
					}
				} else {
					// start the timer now, so we don't have to wait for the next tick
					m.candidateTimer.Reset(0)
				}
			}(miningCtx)
		}
	}
}

func (m *Miner) Stop(_ context.Context) error {
	m.logger.Infof("[Miner] Stopping miner")
	m.candidateTimer.Stop()

	return nil
}

func (m *Miner) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
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

func (m *Miner) mine(ctx context.Context, candidate *model.MiningCandidate, waitSeconds int) error {
	timeStart := time.Now()

	m.logger.Debugf(candidate.Stringify(gocore.Config().GetBool("miner_verbose", false)))

	candidateID := utils.ReverseAndHexEncodeSlice(candidate.Id)

	solution, err := cpuminer.Mine(ctx, candidate)
	if err != nil {
		// use %w to wrap the error, so the caller can use errors.Is() to check for this specific error
		return errors.NewProcessingError("error mining block on %s", candidateID, err)
	}

	if solution == nil {
		return errors.NewProcessingError("mine: no solution found for %s", candidateID)
	}

	blockHash, _ := chainhash.NewHash(solution.BlockHash)

	m.logger.Infof("[Miner] Found block solution %s, submitting", blockHash.String())

	err = m.blockAssemblyClient.SubmitMiningSolution(ctx, solution)
	if err != nil {
		_, err = retry.Retry(ctx, m.logger, func() (struct{}, error) {
			return struct{}{}, m.blockAssemblyClient.SubmitMiningSolution(ctx, solution)
		}, retry.WithMessage(fmt.Sprintf("[Miner] submitting mining solution: %s %s", candidateID, blockHash.String())))

		if err != nil {
			// After all retries, if there's still an error, wrap and return it using %w
			// to wrap the error, so the caller can use errors.Is() to check for this specific error
			// TODO: 3 retries is hardcoded, as it is default in the retry package. This setting should be accessible.
			return errors.NewServiceError("error submitting mining solution after 3 retries for job %s", candidateID, err)
		}
	}

	maxSubtreeCount, _ := gocore.Config().GetInt("miner_max_subtree_count", 600)
	maxSubtreeCountVariance, _ := gocore.Config().GetInt("miner_max_subtree_count_variance", 100)

	// after mining a block, set it again to a new value -> vary the max subtree count by 10% to avoid all miners mining at the same time
	//nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	m.maxSubtreeCount = maxSubtreeCount + maxSubtreeCountVariance - r.Intn(maxSubtreeCountVariance*2)

	prometheusBlockMined.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

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

		m.logger.Debugf(candidate.Stringify(gocore.Config().GetBool("miner_verbose", false)))

		candidateID := utils.ReverseAndHexEncodeSlice(candidate.Id)

		solution, err := cpuminer.Mine(ctx, candidate)
		if err != nil {
			return errors.NewProcessingError("error mining block on %s", candidateID, err)
		}

		if solution == nil {
			return errors.NewProcessingError("mineBlocks: no solution found for %s", candidateID)
		}

		// Define retry delays
		retryDelays := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}

		for i := 0; i < len(retryDelays); i++ {
			err = m.blockAssemblyClient.SubmitMiningSolution(ctx, solution)
			if err == nil {
				break // Success, exit the loop
			}

			if i < len(retryDelays)-1 {
				// Wait for the specified period before retrying, except for the last attempt
				time.Sleep(retryDelays[i])
			}
		}

		if err != nil {
			// After all retries, if there's still an error, wrap and return it using %w
			// to wrap the error, so the caller can use errors.Is() to check for this specific error
			return errors.NewServiceError("error submitting mining solution after %d retries for job %s", len(retryDelays), candidateID, err)
		}
	}
	return nil
}

func (m *Miner) miningCandidate(ctx context.Context, blocks int, previousHash *chainhash.Hash) (*model.MiningCandidate, error) {
	var (
		candidate *model.MiningCandidate
		err       error
	)

	// Initialize backoff parameters
	minBackoff := 100 * time.Millisecond
	maxBackoff := 10 * time.Second
	currentBackoff := minBackoff

	maxRetries := 10
	retryCount := 0

	for {
		select {
		case <-ctx.Done():
			return nil, errors.NewContextCanceledError("[Miner] canceled mining on job %s", candidate.Id)

		case <-m.MineBlocksNImmediatelyCancelChan:
			m.logger.Infof("[Miner] Cancelled mining %d blocks immediately", blocks)

			if candidate == nil {
				return nil, errors.NewServiceError("[Miner] aborting mining on job %s", "unknown")
			}
			return nil, errors.NewServiceError("[Miner] aborting mining on job %s", candidate.Id)

		default:
			// Define retry delays
			retryDelays := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}

			for i := 0; i < len(retryDelays); i++ {
				candidate, err = m.blockAssemblyClient.GetMiningCandidate(ctx)
				if err == nil {
					break // Success, exit the loop
				}

				if i < len(retryDelays)-1 {
					// Wait for the specified period before retrying, except for the last attempt
					time.Sleep(retryDelays[i])
				}
			}

			if err != nil {
				// After all retries, if there's still an error, wrap and return it using %w
				// to wrap the error, so the caller can use errors.Is() to check for this specific error
				return nil, errors.NewServiceError("error getting mining candidate after %d retries: %w", len(retryDelays), err)
			}

			if candidate == nil {
				return nil, errors.NewServiceError("[Miner] no mining candidate found")
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
			//nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand)
			currentBackoff += time.Duration(rand.Int63n(int64(currentBackoff / 10)))

			retryCount++
			if retryCount > maxRetries {
				return nil, errors.NewProcessingError("[Miner] max retries exceeded")
			}
		}
	}
}
