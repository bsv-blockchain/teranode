// Package blockvalidation implements block validation for Bitcoin SV nodes in Teranode.
//
// This package provides the core functionality for validating Bitcoin blocks, managing block subtrees,
// and processing transaction metadata. It is designed for high-performance operation at scale,
// supporting features like:
//
// - Concurrent block validation with optimistic mining support
// - Subtree-based block organization and validation
// - Transaction metadata caching and management
// - Automatic chain catchup when falling behind
// - Integration with Kafka for distributed operation
//
// The package exposes gRPC interfaces for block validation operations,
// making it suitable for use in distributed Teranode deployments.
package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/blockassemblyutil"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	SourceTypeNormal  = "normal"
	SourceTypeRetry   = "retry"
	SourceTypeCatchup = "catchup"
)

// processBlockFound encapsulates information about a newly discovered block
// that requires validation. It includes both the block identifier and communication
// channels for handling validation results.
type processBlockFound struct {
	// hash uniquely identifies the discovered block using a double SHA256 hash
	hash *chainhash.Hash

	// baseURL specifies the peer URL from which additional block data can be retrieved
	// if needed during validation
	baseURL string

	// peerID is the P2P peer identifier used for peer tracking via P2P service
	peerID string

	// errCh receives any errors encountered during block validation and allows
	// synchronous waiting for validation completion
	errCh chan error
}

// processBlockCatchup contains information needed to process a block during chain catchup
// operations when the node has fallen behind the current chain tip.
type processBlockCatchup struct {
	// block contains the full block data to be validated, including header,
	// transactions, and subtrees
	block *model.Block

	// baseURL indicates the peer URL from which additional block data can be
	// retrieved if needed during catchup
	baseURL string

	// peerID is the P2P peer identifier used for peer tracking via P2P service
	peerID string
}

// Server implements a high-performance block validation service for Bitcoin SV.
// It coordinates block validation, subtree management, and transaction metadata processing
// across multiple subsystems while maintaining chain consistency. The server supports
// both synchronous and asynchronous validation modes, with automatic catchup capabilities
// when falling behind the chain tip.
type Server struct {
	// UnimplementedBlockValidationAPIServer provides default implementations of gRPC methods
	blockvalidation_api.UnimplementedBlockValidationAPIServer

	// logger provides structured logging with contextual information
	logger ulogger.Logger

	// settings contains operational parameters for block validation,
	// including timeouts, concurrency limits, and feature flags
	settings *settings.Settings

	// blockchainClient provides access to the blockchain state and operations
	// like block storage, header retrieval, and chain reorganization
	blockchainClient blockchain.ClientI

	// subtreeStore provides persistent storage for block subtrees,
	// enabling efficient block organization and validation
	subtreeStore blob.Store

	// txStore handles permanent storage of individual transactions,
	// allowing retrieval of historical transaction data
	txStore blob.Store

	// utxoStore manages the Unspent Transaction Output (UTXO) set,
	// tracking available outputs for transaction validation
	utxoStore utxo.Store

	// blockAssemblyClient provides access to the block assembly service
	// for checking if coinbase transactions have been processed
	blockAssemblyClient blockassembly.ClientI

	// blockFoundCh receives notifications of newly discovered blocks
	// that need validation. This channel buffers requests when high load occurs.
	blockFoundCh chan processBlockFound

	// blockPriorityQueue manages blocks with priority-based processing
	// to ensure chain-extending blocks are processed before fork blocks
	blockPriorityQueue *BlockPriorityQueue

	// blockClassifier determines the priority of blocks for processing
	blockClassifier *BlockClassifier

	// forkManager manages parallel processing of fork branches
	forkManager *ForkManager

	// catchupCh handles blocks that need processing during chain catchup operations.
	// This channel is used when the node falls behind the chain tip.
	catchupCh chan processBlockCatchup

	// blockValidation contains the core validation logic and state
	blockValidation *BlockValidation

	// validatorClient provides transaction validation services
	validatorClient validator.Interface

	// kafkaConsumerClient handles subscription to and consumption of
	// Kafka messages for distributed coordination
	kafkaConsumerClient kafka.KafkaConsumerGroupI

	// processBlockNotify caches subtree processing state to prevent duplicate
	// processing of the same subtree from multiple miners
	processBlockNotify *ttlcache.Cache[chainhash.Hash, bool]

	// catchupAlternatives tracks alternative peer sources for blocks in catchup
	catchupAlternatives *ttlcache.Cache[chainhash.Hash, []processBlockCatchup]

	// stats tracks operational metrics for monitoring and troubleshooting
	stats *gocore.Stat

	// peerCircuitBreakers manages circuit breakers for each peer to prevent
	// cascading failures and protect against misbehaving peers
	peerCircuitBreakers *catchup.PeerCircuitBreakers

	// headerChainCache provides efficient access to block headers during catchup
	// with proper chain validation to avoid redundant fetches during block validation
	headerChainCache *catchup.HeaderChainCache

	// p2pClient provides access to the P2P service for peer registry operations
	// including catchup metrics reporting. This is optional and may be nil if
	// BlockValidation is running in the same process as the P2P service.
	p2pClient P2PClientI

	// isCatchingUp is an atomic flag to prevent concurrent catchup operations.
	// When true, indicates that a catchup operation is currently in progress.
	// This flag ensures only one catchup can run at a time to prevent resource contention.
	isCatchingUp atomic.Bool

	// catchupStatsMu protects concurrent access to lastCatchupTime and lastCatchupResult.
	// Always acquire this mutex when reading or writing these fields.
	catchupStatsMu sync.RWMutex

	// lastCatchupTime records the timestamp of the most recent catchup attempt completion.
	// Zero value indicates no catchup has been attempted since server start.
	// This field is written at the end of each catchup operation and read during health checks.
	// Protected by catchupStatsMu for thread-safe access.
	lastCatchupTime time.Time

	// lastCatchupResult indicates whether the most recent catchup succeeded (true) or failed (false).
	// This value is undefined if no catchup has been attempted (check lastCatchupTime.IsZero()).
	// Protected by catchupStatsMu for thread-safe access.
	lastCatchupResult bool

	// catchupAttempts tracks the total number of catchup operations initiated since server start.
	// This counter is incremented at the beginning of each catchup attempt, regardless of outcome.
	// The value persists for the lifetime of the server and is never reset.
	catchupAttempts atomic.Int64

	// catchupSuccesses tracks the total number of successful catchup operations since server start.
	// This counter is incremented only when a catchup completes without error.
	// The success rate can be calculated as: catchupSuccesses / catchupAttempts.
	// The value persists for the lifetime of the server and is never reset.
	catchupSuccesses atomic.Int64

	// activeCatchupCtx stores the current catchup context for status reporting to the dashboard.
	// This is updated when a catchup operation starts and cleared when it completes.
	// Protected by activeCatchupCtxMu for thread-safe access.
	activeCatchupCtx   *CatchupContext
	activeCatchupCtxMu sync.RWMutex

	// catchupProgress tracks the current progress through block headers during catchup.
	// blocksFetched and blocksValidated are updated as blocks are processed.
	// These counters are reset at the start of each catchup operation.
	// Protected by activeCatchupCtxMu for thread-safe access.
	blocksFetched   atomic.Int64
	blocksValidated atomic.Int64

	// previousCatchupAttempt stores details about the last failed catchup attempt.
	// This is used to display in the dashboard why we switched from one peer to another.
	// Protected by activeCatchupCtxMu for thread-safe access.
	previousCatchupAttempt *PreviousAttempt
}

// New creates a new block validation server with the provided dependencies.
// It initializes internal channels, caches and metrics collection.
// Parameters:
//   - logger: provides structured logging
//   - tSettings: configuration parameters
//   - subtreeStore: storage for block subtrees
//   - txStore: storage for transactions
//   - utxoStore: manages UTXO set
//   - validatorClient: provides transaction validation
//   - blockchainClient: interfaces with the blockchain
//   - kafkaConsumerClient: handles Kafka message consumption
//   - blockAssemblyClient: interfaces with block assembly service
//   - p2pClient: interfaces with P2P service for peer registry operations
func New(
	logger ulogger.Logger,
	tSettings *settings.Settings,
	subtreeStore blob.Store,
	txStore blob.Store,
	utxoStore utxo.Store,
	validatorClient validator.Interface,
	blockchainClient blockchain.ClientI,
	kafkaConsumerClient kafka.KafkaConsumerGroupI,
	blockAssemblyClient blockassembly.ClientI,
	p2pClient P2PClientI,
) *Server {
	initPrometheusMetrics()

	// TEMP limit to 1, to prevent multiple subtrees processing at the same time
	subtreeGroup := errgroup.Group{}
	util.SafeSetLimit(&subtreeGroup, tSettings.BlockValidation.SubtreeGroupConcurrency)

	// Initialize circuit breakers for peer management
	cbConfig := &catchup.CircuitBreakerConfig{
		FailureThreshold:    tSettings.BlockValidation.CircuitBreakerFailureThreshold,
		SuccessThreshold:    tSettings.BlockValidation.CircuitBreakerSuccessThreshold,
		Timeout:             time.Duration(tSettings.BlockValidation.CircuitBreakerTimeoutSeconds) * time.Second,
		MaxHalfOpenRequests: 1,
	}

	// Determine near fork threshold (default to coinbase maturity / 2)
	nearForkThreshold := uint32(tSettings.ChainCfgParams.CoinbaseMaturity / 2)
	if tSettings.BlockValidation.NearForkThreshold > 0 {
		nearForkThreshold = uint32(tSettings.BlockValidation.NearForkThreshold)
	}

	pq := NewBlockPriorityQueue(logger)
	fm := NewForkManager(logger, tSettings)
	fm.SetPriorityQueue(pq)

	// Register fork resolution callback
	fm.OnForkResolution(func(resolution *ForkResolution) {
		logger.Infof("Fork %s resolved to %s at height %d with %d blocks", resolution.ForkID, resolution.ResolvedTo, resolution.FinalHeight, resolution.BlocksInFork)
	})

	bVal := &Server{
		logger:              logger,
		settings:            tSettings,
		subtreeStore:        subtreeStore,
		blockchainClient:    blockchainClient,
		txStore:             txStore,
		utxoStore:           utxoStore,
		validatorClient:     validatorClient,
		blockAssemblyClient: blockAssemblyClient,
		blockFoundCh:        make(chan processBlockFound, tSettings.BlockValidation.BlockFoundChBufferSize),
		blockPriorityQueue:  pq,
		blockClassifier:     NewBlockClassifier(logger, nearForkThreshold, blockchainClient),
		forkManager:         fm,
		catchupCh:           make(chan processBlockCatchup, tSettings.BlockValidation.CatchupChBufferSize),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](ttlcache.WithTTL[chainhash.Hash, []processBlockCatchup](10 * time.Minute)),
		stats:               gocore.NewStat("blockvalidation"),
		kafkaConsumerClient: kafkaConsumerClient,
		peerCircuitBreakers: catchup.NewPeerCircuitBreakers(*cbConfig),
		headerChainCache:    catchup.NewHeaderChainCache(logger),
		p2pClient:           p2pClient,
	}

	return bVal
}

// Health performs comprehensive health and readiness checks on the block validation service.
// It verifies both the core service and its dependencies are functioning correctly.
//
// When checkLiveness is true, it performs only basic service health checks without checking
// dependencies. This is useful for determining if the service needs to be restarted.
// When false, it performs complete dependency checking to ensure the service can function fully.
//
// The method returns:
//   - An HTTP status code indicating the overall health state
//   - A descriptive message about the health status
//   - Any error encountered during health checking
//
// A return of http.StatusOK indicates the service is healthy and ready to handle requests.
// http.StatusServiceUnavailable indicates the service or a dependency is not ready.
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	var brokersURL []string
	if u.kafkaConsumerClient != nil { // tests may not set this
		brokersURL = u.kafkaConsumerClient.BrokersURL()
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := make([]health.Check, 0, 7)

	// Check if the gRPC server is actually listening and accepting requests
	// Only check if the address is configured (not empty)
	if u.settings.BlockValidation.GRPCListenAddress != "" {
		checks = append(checks, health.Check{
			Name: "gRPC Server",
			Check: health.CheckGRPCServerWithSettings(u.settings.BlockValidation.GRPCListenAddress, u.settings, func(ctx context.Context, conn *grpc.ClientConn) error {
				client := blockvalidation_api.NewBlockValidationAPIClient(conn)
				_, err := client.HealthGRPC(ctx, &blockvalidation_api.EmptyMessage{})
				return err
			}),
		})
	}

	// Only check Kafka if we have a consumer client configured
	if u.kafkaConsumerClient != nil {
		checks = append(checks, health.Check{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)})
	}

	if u.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: u.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(u.blockchainClient)})
	}

	if u.subtreeStore != nil {
		checks = append(checks, health.Check{Name: "SubtreeStore", Check: u.subtreeStore.Health})
	}

	if u.txStore != nil {
		checks = append(checks, health.Check{Name: "TxStore", Check: u.txStore.Health})
	}

	if u.utxoStore != nil {
		checks = append(checks, health.Check{Name: "UTXOStore", Check: u.utxoStore.Health})
	}

	// Add catchup status check
	checks = append(checks, health.Check{
		Name: "CatchupStatus",
		Check: func(ctx context.Context, _ bool) (int, string, error) {
			attempts := u.catchupAttempts.Load()
			successes := u.catchupSuccesses.Load()

			var successRate float64
			if attempts > 0 {
				successRate = float64(successes) / float64(attempts)
			}

			// Read protected fields with mutex
			u.catchupStatsMu.RLock()
			lastTime := u.lastCatchupTime
			lastResult := u.lastCatchupResult
			u.catchupStatsMu.RUnlock()

			// Format time safely
			timeStr := "never"
			if !lastTime.IsZero() {
				timeStr = lastTime.Format(time.RFC3339)
			}

			status := fmt.Sprintf("active=%v, last_time=%s, last_success=%v, attempts=%d, successes=%d, rate=%.2f",
				u.isCatchingUp.Load(),
				timeStr,
				lastResult,
				attempts,
				successes,
				successRate,
			)

			return http.StatusOK, status, nil
		},
	})

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC provides a gRPC interface to the health checking functionality.
// It wraps the Health method to provide standardized health information via gRPC,
// including service status, detailed health information, and timestamps.
//
// The method performs a full dependency check and returns a structured response
// suitable for gRPC health monitoring systems. Any errors are properly wrapped
// to maintain gRPC error semantics.
func (u *Server) HealthGRPC(ctx context.Context, _ *blockvalidation_api.EmptyMessage) (*blockvalidation_api.HealthResponse, error) {
	prometheusBlockValidationHealth.Add(1)

	// Add context value to prevent circular dependency when checking gRPC server health
	ctx = context.WithValue(ctx, "skip-grpc-self-check", true)
	status, details, err := u.Health(ctx, false)

	return &blockvalidation_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.Now(),
	}, errors.WrapGRPC(err)
}

// GetCatchupStatus returns the current catchup status via gRPC.
// This method provides real-time information about ongoing catchup operations
// for monitoring and dashboard display purposes.
//
// The response includes details about the peer being synced from, progress metrics,
// and timing information. If no catchup is active, the response will indicate
// IsCatchingUp=false and other fields will be empty/zero.
//
// This method is thread-safe and can be called concurrently with catchup operations.
//
// Parameters:
//   - ctx: Context for the gRPC request
//   - _: Empty request message (unused but required by gRPC interface)
//
// Returns:
//   - *blockvalidation_api.CatchupStatusResponse: Current catchup status
//   - error: Any error encountered (always nil for this method)
func (u *Server) GetCatchupStatus(ctx context.Context, _ *blockvalidation_api.EmptyMessage) (*blockvalidation_api.CatchupStatusResponse, error) {
	status := u.getCatchupStatusInternal()
	if status != nil && !status.IsCatchingUp && u.blockchainClient != nil {
		_, meta, err := u.blockchainClient.GetBestBlockHeader(ctx)
		if err == nil && meta != nil {
			status.CurrentHeight = meta.Height
		}
	}

	resp := &blockvalidation_api.CatchupStatusResponse{
		IsCatchingUp:         status.IsCatchingUp,
		PeerId:               status.PeerID,
		PeerUrl:              status.PeerURL,
		TargetBlockHash:      status.TargetBlockHash,
		TargetBlockHeight:    status.TargetBlockHeight,
		CurrentHeight:        status.CurrentHeight,
		TotalBlocks:          int32(status.TotalBlocks),
		BlocksFetched:        status.BlocksFetched,
		BlocksValidated:      status.BlocksValidated,
		StartTime:            status.StartTime,
		DurationMs:           status.DurationMs,
		ForkDepth:            status.ForkDepth,
		CommonAncestorHash:   status.CommonAncestorHash,
		CommonAncestorHeight: status.CommonAncestorHeight,
	}

	// Add previous attempt if available
	if status.PreviousAttempt != nil {
		resp.PreviousAttempt = &blockvalidation_api.PreviousCatchupAttempt{
			PeerId:            status.PreviousAttempt.PeerID,
			PeerUrl:           status.PreviousAttempt.PeerURL,
			TargetBlockHash:   status.PreviousAttempt.TargetBlockHash,
			TargetBlockHeight: status.PreviousAttempt.TargetBlockHeight,
			ErrorMessage:      status.PreviousAttempt.ErrorMessage,
			ErrorType:         status.PreviousAttempt.ErrorType,
			AttemptTime:       status.PreviousAttempt.AttemptTime,
			DurationMs:        status.PreviousAttempt.DurationMs,
			BlocksValidated:   status.PreviousAttempt.BlocksValidated,
		}
	}

	return resp, nil
}

// Init initializes the block validation server with required dependencies and services.
// It establishes connections to subtree validation services, configures UTXO store access,
// and starts background processing components. This method must be called before Start().
//
// Parameters:
//   - ctx: Context for initialization operations and service setup
//
// Returns an error if initialization fails due to configuration issues or service unavailability
func (u *Server) Init(ctx context.Context) (err error) {
	u.logger.Infof("[Init] Starting block validation initialization")

	subtreeValidationClient, err := subtreevalidation.NewClient(ctx, u.logger, u.settings, "blockvalidation")
	if err != nil {
		return errors.NewServiceError("[Init] failed to create subtree validation client", err)
	}

	storeURL := u.settings.UtxoStore.UtxoStore
	if storeURL == nil {
		return errors.NewConfigurationError("could not get utxostore URL", err)
	}

	// Only create a new BlockValidation if one wasn't already set (for testing)
	if u.blockValidation == nil {
		u.blockValidation = NewBlockValidation(ctx, u.logger, u.settings, u.blockchainClient, u.subtreeStore, u.txStore, u.utxoStore, u.validatorClient, subtreeValidationClient)
	}

	// if our FSM state is CATCHINGBLOCKS, this is probably a remnant of a crash, put the node back in RUNNING state
	isCatchingBlocks, err := u.blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateCATCHINGBLOCKS)
	if err != nil {
		u.logger.Errorf("[Init] failed to check if FSM currently catching blocks: %v", err)
	}

	if isCatchingBlocks {
		u.logger.Infof("[Init] FSM is in CATCHINGBLOCKS state, setting it to RUNNING")
		if err = u.blockchainClient.Run(ctx, "blockvalidation"); err != nil {
			return errors.NewServiceError("[Init] failed to set FSM state to RUNNING", err)
		}
	}

	go u.processBlockNotify.Start()
	go u.catchupAlternatives.Start()

	// Start fork manager cleanup routine
	go u.forkManager.StartCleanupRoutine(ctx)

	// Start the priority-based block processing system
	u.startBlockProcessingSystem(ctx)

	// Process blocks from the legacy channel (for backward compatibility) with worker pool
	numLegacyWorkers := 10
	for i := 0; i < numLegacyWorkers; i++ {
		workerID := i
		go func() {
			u.logger.Debugf("[Init] Started blockFoundCh worker %d", workerID)
			for {
				select {
				case <-ctx.Done():
					u.logger.Infof("[Init] closing blockFoundCh worker %d", workerID)
					return

				case blockFound := <-u.blockFoundCh:
					u.logger.Infof("[Init] Worker %d received block %s from blockFoundCh, processing", workerID, blockFound.hash.String())
					func(bf processBlockFound) {
						defer func() {
							if r := recover(); r != nil {
								u.logger.Errorf("[Init] PANIC in processBlockFoundChannel for block %s: %v", bf.hash.String(), r)
								if bf.errCh != nil {
									bf.errCh <- errors.NewProcessingError("panic in processBlockFoundChannel: %v", r)
								}
							}
						}()
						if u.isPeerMalicious(ctx, blockFound.peerID) {
							u.logger.Warnf("[blockFound][%s] peer %s (%s) is marked as malicious, skipping", bf.hash.String(), bf.peerID, bf.baseURL)
							if bf.errCh != nil {
								bf.errCh <- errors.NewProcessingError("peer %s is marked as malicious", bf.peerID)
							}
							return
						}
						u.logger.Debugf("[Init] Worker %d starting processBlockFoundChannel for block %s", workerID, bf.hash.String())
						if err := u.processBlockFoundChannel(ctx, bf); err != nil {
							u.logger.Errorf("[Init] processBlockFoundChannel failed for block %s: %v", bf.hash.String(), err)
						} else {
							u.logger.Debugf("[Init] processBlockFoundChannel completed successfully for block %s", bf.hash.String())
						}
					}(blockFound)
				}
			}
		}()
	}

	// Process catchup channel separately
	go func() {
		for {
			select {
			case <-ctx.Done():
				u.logger.Infof("[Init] closing catchup channel")
				return

			case c := <-u.catchupCh:
				{
					// Check if peer is bad or malicious before attempting catchup
					if u.isPeerBad(c.peerID) || u.isPeerMalicious(ctx, c.peerID) {
						u.logger.Warnf("[catchup][%s] peer %s (%s) is marked as bad or malicious, trying alternative peers", c.block.Hash().String(), c.peerID, c.baseURL)

						// Try alternative peers from P2P service instead of just skipping
						if !u.tryAlternativePeersForCatchup(ctx, c.block, c.peerID) {
							blockHash := c.block.Hash()
							u.logger.Warnf("[catchup] All alternative peers failed for block %s, clearing processing marker for retry", blockHash.String())
							u.processBlockNotify.Delete(*blockHash)
							u.catchupAlternatives.Delete(*blockHash)
						}
						continue
					}

					u.logger.Infof("[catchup] Processing catchup request for block %s from peer %s (%s)", c.block.Hash().String(), c.peerID, c.baseURL)

					if err := u.catchup(ctx, c.block, c.peerID, c.baseURL); err != nil {
						// Check if the error is due to another catchup in progress
						if errors.Is(err, errors.ErrCatchupInProgress) {
							u.logger.Warnf("[catchup] Catchup already in progress, requeueing block %s from peer %s", c.block.Hash().String(), c.peerID)
							continue
						}

						// Report catchup failure to P2P service
						u.reportCatchupFailure(ctx, c.peerID)

						u.logger.Errorf("[Init] failed to process catchup signal for block [%s] from peer %s: %v", c.block.Hash().String(), c.peerID, err)

						// Report peer failure to blockchain service (which notifies P2P to switch peers)
						if reportErr := u.blockchainClient.ReportPeerFailure(ctx, c.block.Hash(), c.peerID, "catchup", err.Error()); reportErr != nil {
							u.logger.Errorf("[Init] failed to report peer failure: %v", reportErr)
						}

						// If block is invalid, don't try other peers; continue to next catchup request
						// Block is expected to be added to the block store as invalid somewhere else
						if errors.Is(err, errors.ErrBlockInvalid) ||
							errors.Is(err, errors.ErrTxMissingParent) ||
							errors.Is(err, errors.ErrTxNotFound) ||
							errors.Is(err, errors.ErrTxInvalid) {
							u.logger.Warnf("[catchup] Block %s is invalid, not trying alternative sources", c.block.Hash().String())
							// Clean up the processing notification for this block so it can be retried later if needed
							u.processBlockNotify.Delete(*c.block.Hash())
							continue
						}

						// Try alternative sources for catchup
						blockHash := c.block.Hash()

						// First, try intelligent peer selection from P2P service
						if u.tryAlternativePeersForCatchup(ctx, c.block, c.peerID) {
							continue
						}

						// If P2P service peers didn't work, fall back to cached alternatives
						catchupSucceeded := false
						alternatives := u.catchupAlternatives.Get(*blockHash)
						if alternatives != nil && alternatives.Value() != nil {
							altList := alternatives.Value()
							u.logger.Infof("[catchup] Trying %d cached alternative sources for block %s", len(altList), blockHash.String())

							// Try each alternative
							for _, alt := range altList {
								// Skip the same peer that just failed
								if alt.peerID == c.peerID {
									continue
								}

								// Check if peer is truly malicious (serving invalid blocks)
								// Don't filter by isPeerBad - low reputation peers might still work for catchup
								if u.isPeerMalicious(ctx, alt.peerID) {
									u.logger.Warnf("[catchup] Skipping alternative peer %s - marked as malicious", alt.peerID)
									continue
								}

								u.logger.Infof("[catchup] Trying cached alternative peer %s for block %s", alt.peerID, blockHash.String())

								// Try catchup with alternative peer
								if altErr := u.catchup(ctx, alt.block, alt.peerID, alt.baseURL); altErr == nil {
									u.logger.Infof("[catchup] Successfully processed block %s from alternative peer %s", blockHash.String(), alt.peerID)
									// Clear processing marker and alternatives
									u.processBlockNotify.Delete(*blockHash)
									u.catchupAlternatives.Delete(*blockHash)
									catchupSucceeded = true
									break
								} else {
									u.logger.Warnf("[catchup] Alternative peer %s also failed for block %s: %v", alt.peerID, blockHash.String(), altErr)
									u.reportCatchupFailure(ctx, alt.peerID)
								}
							}
						} else {
							u.logger.Infof("[catchup] No cached alternative sources available for block %s", blockHash.String())
						}

						if !catchupSucceeded {
							// Clear processing marker and alternatives to allow retries
							u.processBlockNotify.Delete(*blockHash)
							u.catchupAlternatives.Delete(*blockHash)
						}
					} else {
						// Success - clear alternatives for this block
						u.catchupAlternatives.Delete(*c.block.Hash())
						// Clear the processing marker
						u.processBlockNotify.Delete(*c.block.Hash())
					}
				}
			}
		}
	}()

	return nil
}

func (u *Server) consumerMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	u.logger.Infof("[consumerMessageHandler] Handler created and waiting for messages")
	return func(msg *kafka.KafkaMessage) error {
		if msg == nil {
			u.logger.Warnf("[consumerMessageHandler] Received nil Kafka message")
			return nil
		}

		u.logger.Infof("[consumerMessageHandler] Received Kafka message from topic: %s, partition: %d, offset: %d",
			msg.Topic, msg.Partition, msg.Offset)

		var kafkaMsg kafkamessage.KafkaBlockTopicMessage
		if err := proto.Unmarshal(msg.Value, &kafkaMsg); err != nil {
			u.logger.Errorf("Failed to unmarshal kafka message: %v", err)
			return nil
		}

		u.logger.Infof("[consumerMessageHandler] Processing block %s from peer %s at %s", kafkaMsg.Hash, kafkaMsg.GetPeerId(), kafkaMsg.URL)

		errCh := make(chan error, 1)
		go func() {
			errCh <- u.blockHandler(&kafkaMsg)
		}()

		select {
		case err := <-errCh:
			// if err is nil, it means function is successfully executed, return nil.
			if err == nil {
				return nil
			}

			// if error is not nil, check if the error is a recoverable error.
			// If the error is a recoverable error, then return the error, so that it kafka message is not marked as committed.
			// So the message will be consumed again.
			if errors.Is(err, errors.ErrServiceError) || errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrThresholdExceeded) || errors.Is(err, errors.ErrContextCanceled) || errors.Is(err, errors.ErrExternal) {
				u.logger.Errorf("Recoverable error (%v) processing kafka message %v for handling block, returning error, thus not marking Kafka message as complete.\n", msg, err)
				return err
			}

			// error is not nil and not recoverable, so it is unrecoverable error, and it should not be tried again
			// kafka message should be committed, so return nil to mark message.
			u.logger.Errorf("Unrecoverable error (%v) processing kafka message %v for handling block, marking Kafka message as completed.\n", msg, err)

			// mark peer failure
			blockHash, _ := chainhash.NewHashFromStr(kafkaMsg.Hash)
			if err = u.blockchainClient.ReportPeerFailure(ctx, blockHash, kafkaMsg.GetPeerId(), "block", err.Error()); err != nil {
				u.logger.Errorf("Failed to report peer failure: %v", err)
			}

			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (u *Server) blockHandler(kafkaMsg *kafkamessage.KafkaBlockTopicMessage) error {
	u.logger.Debugf("[blockHandler] Starting to process block %s from peer %s", kafkaMsg.Hash, kafkaMsg.GetPeerId())

	hash, err := chainhash.NewHashFromStr(kafkaMsg.Hash)
	if err != nil {
		u.logger.Errorf("Failed to parse block hash from message: %v", err)
		return err
	}

	baseURL, err := url.Parse(kafkaMsg.URL)
	if err != nil {
		u.logger.Errorf("Failed to parse block base url from message: %v", err)
		return err
	}

	// Validate that the URL has a proper scheme (http or https)
	// This prevents peer IDs from being incorrectly used as URLs
	// Special case: "legacy" is allowed for blocks from the legacy service
	if kafkaMsg.URL != "legacy" && baseURL.Scheme != "http" && baseURL.Scheme != "https" {
		u.logger.Errorf("[BlockFound] Invalid URL scheme '%s' for URL '%s' from peer %s - expected http or https. Possible peer ID/URL field confusion.", baseURL.Scheme, kafkaMsg.URL, kafkaMsg.GetPeerId())
		return errors.NewProcessingError("[BlockFound] invalid URL scheme '%s' - expected http or https", baseURL.Scheme)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Don't skip blocks from malicious peers entirely - we still want to add them to the queue
	// in case other peers have the same block. The malicious check will be done when fetching.
	if u.isPeerMalicious(ctx, kafkaMsg.GetPeerId()) {
		u.logger.Warnf("[BlockFound][%s] peer %s is malicious, but still adding to queue for potential alternative sources [%s]", hash.String(), kafkaMsg.GetPeerId(), baseURL.String())
		// Continue processing - the block might be available from other peers
	}

	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "BlockFound",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationBlockFound),
		tracing.WithDebugLogMessage(u.logger, "[BlockFound][%s] called from %s", hash.String(), baseURL.String()),
	)
	defer deferFn()

	// first check if the block exists, it is very expensive to do all the checks below
	u.logger.Debugf("[blockHandler] Checking if block %s exists", hash.String())
	if u.blockValidation == nil {
		u.logger.Errorf("[blockHandler] blockValidation is nil! Cannot check if block exists")
		return errors.NewServiceError("[BlockFound][%s] blockValidation not initialized", hash.String())
	}
	exists, err := u.blockValidation.GetBlockExists(ctx, hash)
	u.logger.Debugf("[blockHandler] Block %s exists check completed: exists=%v, err=%v", hash.String(), exists, err)
	if err != nil {
		return errors.NewServiceError("[BlockFound][%s] failed to check if block exists", hash.String(), err)
	}

	if exists {
		u.logger.Infof("[BlockFound][%s] already validated, skipping", hash.String())
		return nil
	}

	u.logger.Debugf("[blockHandler] Adding block %s to blockFoundCh", hash.String())

	u.blockFoundCh <- processBlockFound{
		hash:    hash,
		baseURL: baseURL.String(),
		peerID:  kafkaMsg.GetPeerId(),
		errCh:   nil, // Don't block Kafka consumer waiting for validation
	}
	prometheusBlockValidationBlockFoundCh.Set(float64(len(u.blockFoundCh)))

	u.logger.Debugf("[blockHandler] Block %s queued for processing", hash.String())
	return nil
}

// processBlockFoundChannel processes newly found blocks from the block found channel.
// This method now adds blocks to the priority queue instead of processing them directly,
// allowing for better prioritization and parallel processing.
//
// Parameters:
//   - ctx: Context for processing operations and cancellation
//   - blockFound: Block found request containing hash and processing metadata
//
// Returns an error if block processing fails or validation encounters issues
func (u *Server) processBlockFoundChannel(ctx context.Context, blockFound processBlockFound) error {
	u.logger.Debugf("[processBlockFoundChannel] Started processing block %s from %s (peer: %s)", blockFound.hash.String(), blockFound.baseURL, blockFound.peerID)

	// Validate baseURL to ensure it's not a peer ID being used as URL
	// Special case: "legacy" is allowed for blocks from the legacy service
	if blockFound.baseURL != "legacy" {
		parsedURL, parseErr := url.Parse(blockFound.baseURL)
		if parseErr != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
			u.logger.Errorf("[processBlockFoundChannel][%s] Invalid baseURL '%s' for peer %s - must be valid http/https URL, not peer ID", blockFound.hash.String(), blockFound.baseURL, blockFound.peerID)
			err := errors.NewProcessingError("[processBlockFoundChannel][%s] invalid baseURL - not a valid http/https URL", blockFound.hash.String())

			if blockFound.errCh != nil {
				blockFound.errCh <- err
			}

			return err
		}
	}

	// Check if block already exists to avoid redundant processing
	exists, err := u.blockValidation.GetBlockExists(ctx, blockFound.hash)
	if err != nil {
		u.logger.Errorf("[processBlockFoundChannel] Failed to check if block %s exists: %v", blockFound.hash.String(), err)

		if blockFound.errCh != nil {
			blockFound.errCh <- err
		}

		return errors.NewServiceError("[processBlockFoundChannel] failed to check if block exists", err)
	}

	if exists {
		u.logger.Infof("[processBlockFoundChannel][%s] already validated, skipping", blockFound.hash.String())

		if blockFound.errCh != nil {
			blockFound.errCh <- nil
		}

		return nil
	}

	// Check queue depth and determine if we might need catchup mode
	queueSize := u.blockPriorityQueue.Size()
	shouldConsiderCatchup := u.settings.BlockValidation.UseCatchupWhenBehind && (queueSize > 10 || len(u.blockFoundCh) > 3)

	if shouldConsiderCatchup {
		// Fetch the block to classify it before deciding on catchup
		block, err := u.fetchSingleBlock(ctx, blockFound.hash, blockFound.peerID, blockFound.baseURL)
		if err != nil {
			if blockFound.errCh != nil {
				blockFound.errCh <- err
			}
			return errors.NewProcessingError("[processBlockFoundChannel] failed to get block [%s]", blockFound.hash.String(), err)
		}

		// Check if parent exists
		parentExists, err := u.blockValidation.GetBlockExists(ctx, block.Header.HashPrevBlock)
		if err != nil {
			u.logger.Errorf("[processBlockFoundChannel] Failed to check if parent block %s exists: %v", block.Header.HashPrevBlock.String(), err)
			if blockFound.errCh != nil {
				blockFound.errCh <- err
			}
			return err
		}

		// If parent doesn't exist, always use catchup
		if !parentExists {
			if u.isPeerMalicious(ctx, blockFound.peerID) {
				u.logger.Warnf("[processBlockFoundChannel][%s] peer %s is malicious, skipping catchup for block with missing parent", blockFound.hash.String(), blockFound.peerID)
				return nil
			}
			u.logger.Infof("[processBlockFoundChannel] Parent block %s doesn't exist for block %s, using catchup",
				block.Header.HashPrevBlock.String(), blockFound.hash.String())

			// Send to catchup channel (non-blocking)
			select {
			case u.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: blockFound.baseURL,
				peerID:  blockFound.peerID,
			}:
				u.logger.Debugf("[processBlockFoundChannel] Sent block %s to catchup channel", block.Hash().String())
			default:
				u.logger.Warnf("[processBlockFoundChannel] Catchup channel full, dropping block %s", block.Hash().String())
			}

			if blockFound.errCh != nil {
				blockFound.errCh <- nil
			}
			return nil
		}

		// Parent exists, classify the block
		priority, err := u.blockClassifier.ClassifyBlock(ctx, block)
		if err != nil {
			u.logger.Warnf("[processBlockFoundChannel] Failed to classify block %s: %v", blockFound.hash.String(), err)
			// Continue with normal processing if classification fails
		} else if priority == PriorityChainExtending {
			// Chain-extending blocks should NOT go to catchup, process normally
			u.logger.Debugf("[processBlockFoundChannel] Block %s is chain-extending (queue depth %d), processing normally",
				blockFound.hash.String(), queueSize)
		} else {
			// Non-chain-extending blocks can go to catchup when behind
			u.logger.Infof("[processBlockFoundChannel] Queue depth %d, channel depth %d - sending non-chain-extending block [%s] to catchup",
				queueSize, len(u.blockFoundCh), blockFound.hash.String())

			// Send to catchup channel (non-blocking)
			select {
			case u.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: blockFound.baseURL,
				peerID:  blockFound.peerID,
			}:
				u.logger.Debugf("[processBlockFoundChannel] Sent block %s to catchup channel", block.Hash().String())
			default:
				u.logger.Warnf("[processBlockFoundChannel] Catchup channel full, dropping block %s", block.Hash().String())
			}

			if blockFound.errCh != nil {
				blockFound.errCh <- nil
			}
			return nil
		}
	}

	// Add block to priority queue
	u.addBlockToPriorityQueue(ctx, blockFound)

	// Update metrics
	prometheusBlockValidationBlockFoundCh.Set(float64(len(u.blockFoundCh)))

	// Send response to error channel if it exists (addBlockToPriorityQueue handles the errCh internally)
	// but if it doesn't send a response in all cases, we need to ensure we don't leave the caller hanging
	// The errCh should already be handled by addBlockToPriorityQueue, so we just return
	return nil
}

// Start begins the block validation service operations including gRPC server startup
// and Kafka consumer initialization. It waits for the blockchain FSM to transition
// from IDLE state before starting validation operations to ensure proper sequencing.
//
// Parameters:
//   - ctx: Context for service lifecycle management and cancellation
//   - readyCh: Channel to signal when the service is ready to accept requests
//
// Returns an error if service startup fails due to configuration or dependency issues
func (u *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	u.logger.Infof("[Start] Starting block validation service")

	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	g, gctx := errgroup.WithContext(ctx)

	// Start Kafka consumer only after the blockchain FSM transitions from IDLE.
	// This prevents validation processing from starting too early, while still allowing
	// the gRPC server to accept administrative calls (e.g. RevalidateBlock).
	g.Go(func() error {
		// Blocks until the FSM transitions from the IDLE state
		err := u.blockchainClient.WaitUntilFSMTransitionFromIdleState(gctx)
		if err != nil {
			u.logger.Errorf("[Block Validation Service] Failed to wait for FSM transition from IDLE state: %s", err)
			return err
		}

		u.logger.Infof("[Start] FSM transitioned from IDLE state, starting Kafka consumer")

		// start blocks kafka consumer
		if u.kafkaConsumerClient == nil {
			u.logger.Errorf("[Start] kafkaConsumerClient is nil!")
			return errors.NewServiceError("kafkaConsumerClient is nil")
		}

		u.logger.Infof("[Start] Starting Kafka consumer with handler")
		u.kafkaConsumerClient.Start(gctx, u.consumerMessageHandler(gctx), kafka.WithLogErrorAndMoveOn())

		u.logger.Infof("[Start] Kafka consumer started successfully")

		<-gctx.Done()

		u.logger.Infof("[Start] Kafka consumer context done, closing consumer")
		if err := u.kafkaConsumerClient.Close(); err != nil {
			u.logger.Errorf("[Start] failed to close kafka consumer gracefully: %v", err)
		}

		return nil
	})

	// Start gRPC server immediately (blocks until shutdown)
	g.Go(func() error {
		return util.StartGRPCServer(gctx, u.logger, u.settings, "blockvalidation", u.settings.BlockValidation.GRPCListenAddress, func(server *grpc.Server) {
			blockvalidation_api.RegisterBlockValidationAPIServer(server, u)
			closeOnce.Do(func() { close(readyCh) })
		}, nil)
	})

	return g.Wait()
}

// Stop gracefully shuts down the block validation server by stopping background
// processing components and waiting for ongoing validation operations to complete.
// It ensures clean termination of all service resources and connections.
//
// Parameters:
//   - ctx: Context for shutdown operations (currently unused)
//
// Returns an error if shutdown encounters issues, though typically returns nil
func (u *Server) Stop(_ context.Context) error {
	u.processBlockNotify.Stop()
	u.catchupAlternatives.Stop()

	// Wait for all background tasks in BlockValidation to complete
	if u.blockValidation != nil {
		u.blockValidation.Wait()
	}

	// close the kafka consumer gracefully
	if err := u.kafkaConsumerClient.Close(); err != nil {
		u.logger.Errorf("[BlockValidation] failed to close kafka consumer gracefully: %v", err)
	}

	return nil
}

// BlockFound notifies the service about a newly discovered block that needs validation.
// It initiates the block validation process, optionally waiting for completion based
// on the request parameters.
//
// The method first checks if the block already exists to avoid duplicate processing.
// If the block is new, it queues it for validation using the block processing channels.
// The validation itself happens asynchronously to avoid blocking the gRPC handler.
//
// Parameters:
//   - ctx: The context for handling timeouts and cancellation
//   - req: Contains the block hash and source URL
//
// Returns an EmptyMessage on success or an error if validation cannot be initiated.
// If WaitToComplete is set in the request, waits for validation to finish before returning.
func (u *Server) BlockFound(ctx context.Context, req *blockvalidation_api.BlockFoundRequest) (*blockvalidation_api.EmptyMessage, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "BlockFound",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationBlockFound),
		tracing.WithDebugLogMessage(u.logger, "[BlockFound][%s] called from %s", utils.ReverseAndHexEncodeSlice(req.Hash), req.GetBaseUrl()),
	)
	defer deferFn()

	hash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(
			errors.NewProcessingError("[BlockFound][%s] failed to create hash from bytes", utils.ReverseAndHexEncodeSlice(req.Hash), err))
	}

	// first check if the block exists, it is very expensive to do all the checks below
	exists, err := u.blockValidation.GetBlockExists(ctx, hash)
	if err != nil {
		return nil, errors.WrapGRPC(
			errors.NewServiceError("[BlockFound][%s] failed to check if block exists", hash.String(), err))
	}

	if exists {
		u.logger.Infof("[BlockFound][%s] already validated, skipping", utils.ReverseAndHexEncodeSlice(req.Hash))
		return &blockvalidation_api.EmptyMessage{}, nil
	}

	var errCh chan error

	if req.WaitToComplete {
		errCh = make(chan error)
	}

	// process the block in the background, in the order we receive them, but without blocking the grpc call
	go func() {
		u.logger.Infof("[BlockFound][%s] add on channel", hash.String())
		u.blockFoundCh <- processBlockFound{
			hash:    hash,
			baseURL: req.GetBaseUrl(),
			peerID:  req.GetPeerId(),
			errCh:   errCh,
		}
		prometheusBlockValidationBlockFoundCh.Set(float64(len(u.blockFoundCh)))
	}()

	if req.WaitToComplete {
		err := <-errCh
		if err != nil {
			return nil, errors.WrapGRPC(err)
		}
	}

	return &blockvalidation_api.EmptyMessage{}, nil
}

func (u *Server) RevalidateBlock(ctx context.Context, request *blockvalidation_api.RevalidateBlockRequest) (*blockvalidation_api.EmptyMessage, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "RevalidateBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[RevalidateBlock][%s] revalidate block called", utils.ReverseAndHexEncodeSlice(request.Hash)),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("[RevalidateBlock][%s] failed to create hash from bytes", utils.ReverseAndHexEncodeSlice(request.Hash), err))
	}

	block, err := u.blockchainClient.GetBlock(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewServiceError("[RevalidateBlock][%s] failed to get block", blockHash.String(), err))
	}

	_, blockHeaderMeta, err := u.blockchainClient.GetBlockHeader(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewServiceError("[RevalidateBlock][%s] failed to get block header", blockHash.String(), err))
	}

	// Create validation options
	opts := &ValidateBlockOptions{
		DisableOptimisticMining: true,
		IsRevalidation:          true,
	}

	err = u.blockValidation.ValidateBlockWithOptions(ctx, block, blockHeaderMeta.PeerID, opts)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewServiceError("[RevalidateBlock][%s] failed block re-validation", block.String(), err))
	}

	return &blockvalidation_api.EmptyMessage{}, nil
}

// ProcessBlock handles validation of a complete block at a specified height.
// This method is typically used during initial block sync or when receiving blocks
// through legacy interfaces.
//
// The method performs several key operations:
// - Validates the block height is correct, fetching from parent if needed
// - Ensures the block can be properly deserialized
// - Processes the block through the validation pipeline
// - Updates chain state if validation succeeds
//
// Parameters:
//   - ctx: Context for the operation
//   - request: Contains the raw block data and target height
//
// Returns an EmptyMessage on successful validation or an error if validation fails.
func (u *Server) ProcessBlock(ctx context.Context, request *blockvalidation_api.ProcessBlockRequest) (*blockvalidation_api.EmptyMessage, error) {
	block, err := model.NewBlockFromBytes(request.Block)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("failed to create block from bytes", err))
	}

	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "ProcessBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[ProcessBlock][%s] process block called for height %d (%d txns)", block.Hash(), request.Height, block.TransactionCount),
	)
	defer deferFn()

	// we need the height for the subsidy calculation
	height := request.Height

	if height <= 0 {
		// try to get the height from the previous block
		_, previousBlockMeta, err := u.blockchainClient.GetBlockHeader(ctx, block.Header.HashPrevBlock)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewServiceError("failed to get previous block header", err))
		}

		if previousBlockMeta != nil {
			height = previousBlockMeta.Height + 1
		}
	}

	if height <= 0 {
		return nil, errors.WrapGRPC(errors.NewProcessingError("invalid height: %d", height))
	}

	block.Height = height

	baseURL := request.BaseUrl
	if baseURL == "" {
		baseURL = "legacy" // default to legacy if not provided
	}

	if err = u.processBlockFound(ctx, block.Header.Hash(), request.PeerId, baseURL, block); err != nil {
		// error from processBlockFound is already wrapped
		return nil, errors.WrapGRPC(err)
	}

	return &blockvalidation_api.EmptyMessage{}, nil
}

// ValidateBlock validates a block directly from the block bytes
// without needing to fetch it from the network or the database.
// This method is typically used for testing or when the block is already
// available in memory, and no internal updates or database operations are needed.
//
// Parameters:
//   - ctx: Context for the operation
//   - request: Contains the raw block data and height
//
// Returns:
//   - A response indicating the validation result
//   - An error if validation fails
func (u *Server) ValidateBlock(ctx context.Context, request *blockvalidation_api.ValidateBlockRequest) (*blockvalidation_api.ValidateBlockResponse, error) {
	block, err := model.NewBlockFromBytes(request.Block)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("[Server:ValidateBlock] failed to create block from bytes", err))
	}

	// override the height if provided in the request
	if request.Height > 0 {
		block.Height = request.Height
	}

	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "ValidateBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[Server:ValidateBlock][%s] validate block called for height %d", block.Hash().String(), request.Height),
	)
	defer deferFn()

	// Wait for block assembly to be ready before processing the block
	if err = blockassemblyutil.WaitForBlockAssemblyReady(ctx, u.logger, u.blockAssemblyClient, block.Height, u.settings.BlockValidation.MaxBlocksBehindBlockAssembly); err != nil {
		// block-assembly is still behind, so we cannot process this block
		return nil, errors.WrapGRPC(err)
	}

	blockHeaders, blockHeadersMeta, err := u.blockchainClient.GetBlockHeaders(ctx, block.Header.HashPrevBlock, u.settings.BlockValidation.PreviousBlockHeaderCount)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewServiceError("[ValidateBlock][%s] failed to get block headers", block.String(), err))
	}

	blockHeaderIDs := make([]uint32, len(blockHeadersMeta))
	for i, blockHeaderMeta := range blockHeadersMeta {
		blockHeaderIDs[i] = blockHeaderMeta.ID
	}

	oldBlockIDsMap := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

	if ok, err := block.Valid(ctx, u.logger, u.subtreeStore, u.utxoStore, oldBlockIDsMap, blockHeaders, blockHeaderIDs, u.settings); !ok {
		return nil, errors.WrapGRPC(errors.NewBlockInvalidError("[ValidateBlock][%s] block is not valid", block.String(), err))
	}

	if err = u.blockValidation.checkOldBlockIDs(ctx, oldBlockIDsMap, block); err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockInvalidError("[ValidateBlock][%s] block is not valid", block.String(), err))
	}

	return &blockvalidation_api.ValidateBlockResponse{
		Ok:      true,
		Message: fmt.Sprintf("Block %s is valid", block.String()),
	}, nil
}

// processBlockFound processes a newly discovered block by validating it and managing
// parent block dependencies. It handles block retrieval, validation sequencing,
// and ensures proper processing order for blockchain consistency.
//
// Parameters:
//   - ctx: Context for tracing and operation management
//   - hash: Hash of the block to process
//   - baseURL: Base URL for block retrieval operations
//   - useBlock: Optional pre-loaded block to avoid retrieval (variadic parameter)
//
// Returns an error if block processing, validation, or dependency management fails
func (u *Server) processBlockFound(ctx context.Context, hash *chainhash.Hash, peerID, baseURL string, useBlock ...*model.Block) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "processBlockFound",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationProcessBlockFound),
		tracing.WithDebugLogMessage(u.logger, "[processBlockFound][%s] processing block found from %s", hash.String(), baseURL),
	)
	defer deferFn()

	// Check if the peer is malicious before attempting to fetch
	if u.isPeerMalicious(ctx, peerID) {
		u.logger.Warnf("[processBlockFound][%s] peer %s is malicious, not fetching from [%s]", hash.String(), peerID, baseURL)
		return errors.NewProcessingError("[processBlockFound][%s] peer %s is malicious", hash.String(), peerID)
	}

	// first check if the block exists, it might have already been processed
	exists, err := u.blockValidation.GetBlockExists(ctx, hash)
	if err != nil {
		return errors.NewServiceError("[processBlockFound][%s] failed to check if block exists", hash.String(), err)
	}

	if exists {
		u.logger.Warnf("[processBlockFound][%s] not processing block that already was found", hash.String())
		return nil
	}

	var block *model.Block
	if len(useBlock) > 0 {
		block = useBlock[0]
	} else {
		block, err = u.fetchSingleBlock(ctx, hash, peerID, baseURL)
		if err != nil {
			return err
		}
	}

	u.checkParentProcessingComplete(ctx, block, baseURL)

	// catchup if we are missing the parent block.
	parentExists, err := u.blockValidation.GetBlockExists(ctx, block.Header.HashPrevBlock)
	if err != nil {
		return errors.NewServiceError("[processBlockFound][%s] failed to check if parent block %s exists", hash.String(), block.Header.HashPrevBlock.String(), err)
	}

	if !parentExists {
		// add to catchup channel, which will block processing any new blocks until we have caught up
		go func() {
			u.logger.Debugf("[processBlockFound][%s] processBlockFound add to catchup channel", hash.String())
			select {
			case u.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: baseURL,
				peerID:  peerID,
			}:
				u.logger.Debugf("[processBlockFound] Sent block %s to catchup channel", hash.String())
			default:
				u.logger.Warnf("[processBlockFound] Catchup channel full, dropping block %s from peer %s", hash.String(), peerID)
			}
		}()

		return nil
	}

	// Wait for block assembly to be ready before processing the block
	if err = blockassemblyutil.WaitForBlockAssemblyReady(ctx, u.logger, u.blockAssemblyClient, block.Height, u.settings.BlockValidation.MaxBlocksBehindBlockAssembly); err != nil {
		// block-assembly is still behind, so we cannot process this block
		return err
	}

	// validate the block
	u.logger.Infof("[processBlockFound][%s] validate block from %s", hash.String(), baseURL)

	// Create validation options
	opts := &ValidateBlockOptions{
		DisableOptimisticMining: baseURL == "legacy",
		IsRevalidation:          false, // processBlockFound is for new blocks, not revalidation
	}

	err = u.blockValidation.ValidateBlockWithOptions(ctx, block, baseURL, opts)
	if err != nil {
		return errors.NewServiceError("failed block validation BlockFound [%s]", block.String(), err)
	}

	return nil
}

// checkParentProcessingComplete ensures that a block's parent has completed validation
// before allowing the current block's validation to proceed. This method implements
// a backoff strategy while waiting for parent block processing to complete.
//
// The method:
// - Verifies parent block validation status
// - Implements exponential backoff for retry attempts
// - Monitors both validation and bloom filter creation status
// - Provides detailed logging of the waiting process
//
// Parameters:
//   - ctx: Context for operation management
//   - block: The block whose parent requires verification
//   - baseURL: Source URL for additional data retrieval if needed
func (u *Server) checkParentProcessingComplete(ctx context.Context, block *model.Block, baseURL string) {
	_, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "checkParentProcessingComplete",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[checkParentProcessingComplete][%s] called from %s", block.Hash().String(), baseURL),
	)
	defer deferFn()

	delay := 10 * time.Millisecond
	maxDelay := 10 * time.Second

	// check if the parent block is being validated, then wait for it to finish.
	blockBeingFinalized := u.blockValidation.blockHashesCurrentlyValidated.Exists(*block.Header.HashPrevBlock)

	if blockBeingFinalized {
		u.logger.Infof("[processBlockFound][%s] parent block is being validated (hash: %s), waiting for it to finish: validated %v",
			block.Hash().String(),
			block.Header.HashPrevBlock.String(),
			u.blockValidation.blockHashesCurrentlyValidated.Exists(*block.Header.HashPrevBlock),
		)

		retries := 0

		for {
			blockBeingFinalized = u.blockValidation.blockHashesCurrentlyValidated.Exists(*block.Header.HashPrevBlock)

			if !blockBeingFinalized {
				break
			}

			if (retries % 10) == 0 {
				u.logger.Infof("[processBlockFound][%s] parent block is still (%d) being validated (hash: %s), waiting for it to finish: validated %v",
					block.Hash().String(),
					retries,
					block.Header.HashPrevBlock.String(),
					u.blockValidation.blockHashesCurrentlyValidated.Exists(*block.Header.HashPrevBlock),
				)
			}

			time.Sleep(delay)

			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}

			retries++
		}
	}
}

// startBlockProcessingSystem starts the priority-based block processing system
// with support for parallel fork processing
func (u *Server) startBlockProcessingSystem(ctx context.Context) {
	// Skip if priority queue is not initialized (e.g., in some tests)
	if u.blockPriorityQueue == nil {
		u.logger.Warnf("[startBlockProcessingSystem] Priority queue not initialized, skipping worker startup")
		return
	}

	u.logger.Infof("[startBlockProcessingSystem] Starting block processing system")

	// Start multiple worker goroutines for parallel fork processing
	numWorkers := 4
	if u.settings.BlockValidation.MaxParallelForks > 0 {
		numWorkers = u.settings.BlockValidation.MaxParallelForks
	}

	for i := 0; i < numWorkers; i++ {
		workerID := i
		u.logger.Infof("[startBlockProcessingSystem] Starting worker %d", workerID)
		go u.blockProcessingWorker(ctx, workerID)
	}

	// Update worker count metric
	if prometheusForkProcessingWorkers != nil {
		prometheusForkProcessingWorkers.Set(float64(numWorkers))
	}

	// Log queue statistics periodically
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				chainExtending, nearFork, deepFork := u.blockPriorityQueue.GetQueueStats()
				u.logger.Infof("[BlockProcessing] Queue stats - Chain extending: %d, Near fork: %d, Deep fork: %d, Fork count: %d",
					chainExtending, nearFork, deepFork, u.forkManager.GetForkCount())

				// Update fork metrics
				if prometheusForkCount != nil {
					prometheusForkCount.Set(float64(u.forkManager.GetForkCount()))
				}
			}
		}
	}()
}

// blockProcessingWorker is a worker that processes blocks from the priority queue
func (u *Server) blockProcessingWorker(ctx context.Context, workerID int) {
	u.logger.Infof("[BlockProcessing] Worker %d started", workerID)

	for {
		select {
		case <-ctx.Done():
			u.logger.Infof("[BlockProcessing] Worker %d stopping", workerID)
			return
		default:
			blockFound, status := u.blockPriorityQueue.WaitForBlock(ctx, u.forkManager)

			if status != GetOK {
				continue
			}

			if !u.forkManager.StartProcessingBlock(blockFound.hash) {
				continue
			}

			u.logger.Debugf("[BlockProcessing] Worker %d processing block %s", workerID, blockFound.hash.String())

			err := u.processBlockWithPriority(ctx, blockFound)

			u.forkManager.FinishProcessingBlock(blockFound.hash)

			if err != nil {
				u.logger.Errorf("[BlockProcessing] Worker %d failed to process block %s: %v", workerID, blockFound.hash.String(), err)

				// Note: Failures are not reported here as these are normal block processing failures
				// Catchup failures are reported by the catchup() function

				// Update processed metric with failure
				if prometheusBlockPriorityQueueProcessed != nil {
					// We don't have the priority here, so we'll use "unknown"
					prometheusBlockPriorityQueueProcessed.WithLabelValues("unknown", "failure").Inc()
				}

				// If the error indicates the block couldn't be fetched (network error, malicious node, etc),
				// we should retry the block later rather than dropping it completely
				// TODO: We might want to limit the number of retries per block to avoid infinite loops
				// For now, we'll just re-queue it with deepFork priority to ensure it gets retried eventually
				// Note: This could lead to blocks being retried indefinitely if they are always failing to fetch
				// A more robust solution would involve tracking retry counts and eventually giving up after a threshold
				if errors.IsNetworkError(err) || errors.IsMaliciousResponseError(err) {
					u.logger.Warnf("[BlockProcessing] Block %s fetch failed, will retry later", blockFound.hash.String())
					// Re-add the block to the queue for retry
					// We need to get the block metadata (height and priority) for re-queuing
					// For now, we'll use deepFork priority as a safe default for retries
					go func() {
						// Add a small delay to avoid tight loops
						time.Sleep(5 * time.Second)
						// Create a new blockFound without specific peer info so any peer can provide it
						retryBlock := processBlockFound{
							hash:    blockFound.hash,
							baseURL: SourceTypeRetry, // Special marker for retry attempts
							peerID:  "",              // Clear peer ID so any peer can be used
							errCh:   nil,             // No error channel for async retry
						}
						u.blockPriorityQueue.RequeueForRetry(retryBlock, PriorityDeepFork, 0)
						u.logger.Infof("[BlockProcessing] Re-queued block %s for retry from any available peer", blockFound.hash.String())
					}()
				}
			} else {
				// Update processed metric with success
				if prometheusBlockPriorityQueueProcessed != nil {
					prometheusBlockPriorityQueueProcessed.WithLabelValues("unknown", "success").Inc()
				}
			}
		}
	}
}

// addBlockToPriorityQueue adds a block to the priority queue with appropriate classification
func (u *Server) addBlockToPriorityQueue(ctx context.Context, blockFound processBlockFound) {
	u.logger.Debugf("[addBlockToPriorityQueue] Started for block %s from %s", blockFound.hash.String(), blockFound.baseURL)

	// First check if block already exists
	exists, err := u.blockValidation.GetBlockExists(ctx, blockFound.hash)
	if err != nil {
		u.logger.Errorf("[addBlockToPriorityQueue] Failed to check if block exists: %v", err)
		if blockFound.errCh != nil {
			blockFound.errCh <- err
		}
		return
	}

	if exists {
		u.logger.Debugf("[addBlockToPriorityQueue] Block %s already exists, skipping", blockFound.hash.String())
		if blockFound.errCh != nil {
			blockFound.errCh <- nil
		}
		return
	}

	// Create isolated context with timeout for transient fetch operation
	// This ensures fetch failures don't affect other operations using parent context
	fetchCtx, fetchCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer fetchCancel()

	// Fetch the block to classify it
	block, err := u.fetchSingleBlock(fetchCtx, blockFound.hash, blockFound.peerID, blockFound.baseURL)
	if err != nil {
		u.logger.Errorf("[addBlockToPriorityQueue] Failed to fetch block %s: %v", blockFound.hash.String(), err)
		if blockFound.errCh != nil {
			blockFound.errCh <- err
		}
		return
	}

	// Check if parent exists - if not, send directly to catchup
	parentExists, err := u.blockValidation.GetBlockExists(ctx, block.Header.HashPrevBlock)
	if err != nil {
		u.logger.Errorf("[addBlockToPriorityQueue] Failed to check if parent block %s exists: %v", block.Header.HashPrevBlock.String(), err)
		if blockFound.errCh != nil {
			blockFound.errCh <- err
		}
		return
	}

	if !parentExists {
		u.logger.Infof("[addBlockToPriorityQueue] Parent block %s doesn't exist for block %s, sending to catchup", block.Header.HashPrevBlock.String(), blockFound.hash.String())

		// Check if we're already processing this block in catchup
		if u.processBlockNotify.Get(*blockFound.hash) != nil {
			u.logger.Infof("[addBlockToPriorityQueue] Block %s already being processed in catchup, adding as alternative source", blockFound.hash.String())

			// Add to alternative sources for potential failover
			catchupBlock := processBlockCatchup{
				block:   block,
				baseURL: blockFound.baseURL,
				peerID:  blockFound.peerID,
			}

			// Get existing alternatives or create new list
			alternatives := u.catchupAlternatives.Get(*blockFound.hash)
			if alternatives == nil || alternatives.Value() == nil {
				u.catchupAlternatives.Set(*blockFound.hash, []processBlockCatchup{catchupBlock}, ttlcache.DefaultTTL)
			} else {
				// Append to existing alternatives
				altList := alternatives.Value()
				altList = append(altList, catchupBlock)
				u.catchupAlternatives.Set(*blockFound.hash, altList, ttlcache.DefaultTTL)
			}

			if blockFound.errCh != nil {
				blockFound.errCh <- nil
			}
			return
		}

		// Mark as being processed (use TTL to auto-cleanup)
		u.processBlockNotify.Set(*blockFound.hash, true, ttlcache.DefaultTTL)

		// Send directly to catchup channel (non-blocking)
		go func() {
			u.logger.Infof("[addBlockToPriorityQueue] Attempting to send block %s to catchup channel (queue size: %d/%d)",
				blockFound.hash.String(), len(u.catchupCh), cap(u.catchupCh))

			select {
			case u.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: blockFound.baseURL,
				peerID:  blockFound.peerID,
			}:
				u.logger.Infof("[addBlockToPriorityQueue] Successfully sent block %s to catchup channel", blockFound.hash.String())
			default:
				// Channel is full, log warning but don't block
				u.logger.Warnf("[addBlockToPriorityQueue] Catchup channel full (%d/%d), dropping block %s from peer %s",
					len(u.catchupCh), cap(u.catchupCh), blockFound.hash.String(), blockFound.peerID)
				// Clear the processing marker so it can be retried later
				u.processBlockNotify.Delete(*blockFound.hash)
			}
		}()

		if blockFound.errCh != nil {
			blockFound.errCh <- nil
		}
		return
	}

	// Classify the block
	priority, err := u.blockClassifier.ClassifyBlock(ctx, block)
	if err != nil {
		u.logger.Warnf("[addBlockToPriorityQueue] Failed to classify block %s, using deep fork priority: %v", blockFound.hash.String(), err)
		priority = PriorityDeepFork
	}

	// Add block to appropriate fork if it's not chain-extending
	if priority != PriorityChainExtending {
		forkID, err := u.forkManager.DetermineForkID(ctx, block, u.blockchainClient)
		if err != nil {
			u.logger.Warnf("[addBlockToPriorityQueue] Failed to determine fork ID for block %s: %v", blockFound.hash.String(), err)
		} else {
			if err := u.forkManager.AddBlockToFork(block, forkID); err != nil {
				u.logger.Errorf("[addBlockToPriorityQueue] Failed to add block to fork %s: %v", forkID, err)
			}
		}
	}

	// Add to priority queue
	u.blockPriorityQueue.Add(blockFound, priority, block.Height)

	u.logger.Infof("[addBlockToPriorityQueue] Added block %s with priority %d at height %d", blockFound.hash.String(), priority, block.Height)

	// Send success signal if someone is waiting
	if blockFound.errCh != nil {
		u.logger.Debugf("[addBlockToPriorityQueue] Sending success response to errCh for block %s", blockFound.hash.String())
		blockFound.errCh <- nil
	} else {
		u.logger.Debugf("[addBlockToPriorityQueue] No errCh to respond to for block %s", blockFound.hash.String())
	}
}

// processBlockWithPriority processes a block based on its priority
func (u *Server) processBlockWithPriority(ctx context.Context, blockFound processBlockFound) error {
	// Check if this is a retry attempt
	if blockFound.baseURL == "retry" {
		// For retries, try to get an alternative source first
		alternative, hasAlternative := u.blockPriorityQueue.GetAlternativeSource(blockFound.hash)
		if hasAlternative {
			u.logger.Infof("[processBlockWithPriority] Retry attempt using alternative source for block %s from %s (peer: %s)", blockFound.hash.String(), alternative.baseURL, alternative.peerID)
			blockFound = alternative
		} else {
			// No alternatives available, we'll need to wait for new announcements
			u.logger.Warnf("[processBlockWithPriority] No alternative sources available for retry of block %s", blockFound.hash.String())
			return errors.NewProcessingError("[processBlockWithPriority] no sources available for block %s", blockFound.hash.String())
		}
	}

	// Try to process with the primary source
	err := u.processBlockFound(ctx, blockFound.hash, blockFound.peerID, blockFound.baseURL)

	// If fetch failed and it's not a validation error, try alternative sources
	if err != nil && (errors.IsNetworkError(err) || errors.IsMaliciousResponseError(err)) {
		u.logger.Warnf("[processBlockWithPriority] Failed to fetch block %s from %s (peer: %s), trying alternative sources: %v", blockFound.hash.String(), blockFound.baseURL, blockFound.peerID, err)

		// Try alternative sources
		for {
			alternative, hasAlternative := u.blockPriorityQueue.GetAlternativeSource(blockFound.hash)
			if !hasAlternative {
				// No more alternatives, return the original error
				return err
			}

			u.logger.Infof("[processBlockWithPriority] Trying alternative source for block %s from %s (peer: %s)", blockFound.hash.String(), alternative.baseURL, alternative.peerID)

			// Try with alternative source
			altErr := u.processBlockFound(ctx, alternative.hash, alternative.peerID, alternative.baseURL)
			if altErr == nil {
				// Success with alternative source
				return nil
			}

			// Log the failure but continue trying other alternatives
			u.logger.Warnf("[processBlockWithPriority] Alternative source also failed for block %s from %s: %v", blockFound.hash.String(), alternative.baseURL, altErr)
		}
	}

	return err
}
