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
	"math"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/catchup"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/blockassemblyutil"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	// processSubtreeNotify caches subtree processing state to prevent duplicate
	// processing of the same subtree from multiple miners
	processSubtreeNotify *ttlcache.Cache[chainhash.Hash, bool]

	// stats tracks operational metrics for monitoring and troubleshooting
	stats *gocore.Stat

	// peerCircuitBreakers manages circuit breakers for each peer to prevent
	// cascading failures and protect against misbehaving peers
	peerCircuitBreakers *catchup.PeerCircuitBreakers

	// peerMetrics tracks performance and reputation metrics for each peer
	peerMetrics *catchup.CatchupMetrics

	// headerChainCache provides efficient access to block headers during catchup
	// with proper chain validation to avoid redundant fetches during block validation
	headerChainCache *catchup.HeaderChainCache

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

	bVal := &Server{
		logger:               logger,
		settings:             tSettings,
		subtreeStore:         subtreeStore,
		blockchainClient:     blockchainClient,
		txStore:              txStore,
		utxoStore:            utxoStore,
		validatorClient:      validatorClient,
		blockAssemblyClient:  blockAssemblyClient,
		blockFoundCh:         make(chan processBlockFound, tSettings.BlockValidation.BlockFoundChBufferSize),
		catchupCh:            make(chan processBlockCatchup, tSettings.BlockValidation.CatchupChBufferSize),
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
		stats:                gocore.NewStat("blockvalidation"),
		kafkaConsumerClient:  kafkaConsumerClient,
		peerCircuitBreakers:  catchup.NewPeerCircuitBreakers(*cbConfig),
		peerMetrics: &catchup.CatchupMetrics{
			PeerMetrics: make(map[string]*catchup.PeerCatchupMetrics),
		},
		headerChainCache: catchup.NewHeaderChainCache(logger), // Chain-aware cache for efficient catchup
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

// Init initializes the block validation server with required dependencies and services.
// It establishes connections to subtree validation services, configures UTXO store access,
// and starts background processing components. This method must be called before Start().
//
// Parameters:
//   - ctx: Context for initialization operations and service setup
//
// Returns an error if initialization fails due to configuration issues or service unavailability
func (u *Server) Init(ctx context.Context) (err error) {
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

	go u.processSubtreeNotify.Start()

	// process blocks found from channel
	go func() {
		for {
			select {
			case <-ctx.Done():
				u.logger.Infof("[Init] closing block found channel")
				return

			case c := <-u.catchupCh:
				{
					if u.peerMetrics != nil {
						peerMetric := u.peerMetrics.GetOrCreatePeerMetrics(c.baseURL)
						if peerMetric != nil {
							if peerMetric.IsBad() || peerMetric.IsMalicious() {
								u.logger.Warnf("[catchup][%s] peer %s is marked as bad (score: %0.0f) or malicious (attempts: %d), skipping [%s]", c.block.Hash().String(), c.baseURL, peerMetric.GetReputation(), peerMetric.GetMaliciousAttempts(), c.baseURL)
								continue
							}
						}
					}

					if err := u.catchup(ctx, c.block, c.baseURL); err != nil {
						var (
							peerMetric        *catchup.PeerCatchupMetrics
							reputationScore   float64
							maliciousAttempts int64
						)

						// this should be moved into the catchup directly...
						if u.peerMetrics != nil {
							peerMetric = u.peerMetrics.GetOrCreatePeerMetrics(c.baseURL)
							if peerMetric != nil {
								peerMetric.RecordFailure()
								reputationScore = peerMetric.ReputationScore
								maliciousAttempts = peerMetric.MaliciousAttempts

								if !peerMetric.IsTrusted() {
									u.logger.Warnf("[catchup][%s] peer %s has low reputation score: %.2f, malicious attempts: %d", c.block.Hash().String(), c.baseURL, peerMetric.ReputationScore, peerMetric.MaliciousAttempts)
								}
							}
						}

						u.logger.Errorf("[Init] failed to process catchup signal for block [%s], peer reputation: %.2f, malicious attempts: %d, [%v]", c.block.Hash().String(), reputationScore, maliciousAttempts, err)
					}
				}

			case blockFound := <-u.blockFoundCh:
				{
					if err := u.processBlockFoundChannel(ctx, blockFound); err != nil {
						if u.peerMetrics != nil {
							peerMetric := u.peerMetrics.GetOrCreatePeerMetrics(blockFound.baseURL)
							if peerMetric != nil {
								peerMetric.RecordFailure()
							}

							if !peerMetric.IsTrusted() {
								u.logger.Warnf("[catchup][%s] peer %s has low reputation score: %.2f, malicious attempts: %d", blockFound.hash.String(), blockFound.baseURL, peerMetric.ReputationScore, peerMetric.MaliciousAttempts)
							}
						}

						u.logger.Errorf("[Init] failed to process block found [%s] [%v]", blockFound.hash.String(), err)
					}
				}
			}
		}
	}()

	return nil
}

func (u *Server) consumerMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		errCh := make(chan error, 1)
		go func() {
			errCh <- u.blockHandler(msg)
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

			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (u *Server) blockHandler(msg *kafka.KafkaMessage) error {
	if msg == nil {
		return nil
	}

	var kafkaMsg kafkamessage.KafkaBlockTopicMessage
	if err := proto.Unmarshal(msg.Value, &kafkaMsg); err != nil {
		u.logger.Errorf("Failed to unmarshal kafka message: %v", err)
		return err
	}

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

	if u.peerMetrics != nil {
		peerMetrics := u.peerMetrics.GetOrCreatePeerMetrics(baseURL.String())

		if peerMetrics != nil && peerMetrics.IsMalicious() {
			u.logger.Warnf("[BlockFound][%s] peer is malicious, skipping [%s]", hash.String(), baseURL.String())
			// do not return for now
			// return nil
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "BlockFound",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationBlockFound),
		tracing.WithDebugLogMessage(u.logger, "[BlockFound][%s] called from %s", hash.String(), baseURL.String()),
	)
	defer deferFn()

	// first check if the block exists, it is very expensive to do all the checks below
	exists, err := u.blockValidation.GetBlockExists(ctx, hash)
	if err != nil {
		return errors.NewServiceError("[BlockFound][%s] failed to check if block exists", hash.String(), err)
	}

	if exists {
		u.logger.Infof("[BlockFound][%s] already validated, skipping", hash.String())
		return nil
	}

	errCh := make(chan error, 1)

	u.logger.Infof("[BlockFound][%s] add on channel", hash.String())

	u.blockFoundCh <- processBlockFound{
		hash:    hash,
		baseURL: baseURL.String(),
		errCh:   errCh,
	}
	prometheusBlockValidationBlockFoundCh.Set(float64(len(u.blockFoundCh)))

	err = <-errCh
	if err != nil {
		return errors.NewProcessingError("[BlockFound][%s] failed to process block", hash.String(), err)
	}

	return nil
}

// processBlockFoundChannel processes newly found blocks from the block found channel.
// It implements intelligent routing between normal processing and catchup mode based
// on the current backlog depth to optimize validation performance.
//
// Parameters:
//   - ctx: Context for processing operations and cancellation
//   - blockFound: Block found request containing hash and processing metadata
//
// Returns an error if block processing fails or validation encounters issues
func (u *Server) processBlockFoundChannel(ctx context.Context, blockFound processBlockFound) error {
	// TODO GOKHAN: parameterize this
	if u.settings.BlockValidation.UseCatchupWhenBehind && len(u.blockFoundCh) > 3 {
		// we are multiple blocks behind, process all the blocks per peer on the catchup channel
		u.logger.Infof("[Init] processing block found on channel [%s] - too many blocks behind", blockFound.hash.String())

		// collect all drained items to acknowledge their errCh channels
		allDrainedItems := make([]processBlockFound, 0)
		allDrainedItems = append(allDrainedItems, blockFound)

		peerBlocks := make(map[string]processBlockFound)
		peerBlocks[blockFound.baseURL] = blockFound
		// get the newest block per peer, emptying the block found channel
		for len(u.blockFoundCh) > 0 {
			pb := <-u.blockFoundCh
			allDrainedItems = append(allDrainedItems, pb)
			peerBlocks[pb.baseURL] = pb
		}

		u.logger.Infof("[Init] peerBlocks: %v", peerBlocks)
		// add that latest block of each peer to the catchup channel
		for _, pb := range peerBlocks {
			block, err := u.fetchSingleBlock(ctx, pb.hash, pb.baseURL)
			if err != nil {
				// acknowledge all errCh channels before returning error
				for _, item := range allDrainedItems {
					if item.errCh != nil {
						item.errCh <- err
					}
				}
				return errors.NewProcessingError("[Init] failed to get block [%s]", pb.hash.String(), err)
			}

			u.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: pb.baseURL,
			}
		}

		// acknowledge all errCh channels for all drained items
		for _, item := range allDrainedItems {
			if item.errCh != nil {
				item.errCh <- nil
			}
		}

		return nil
	}

	_, _, ctx1 := tracing.NewStatFromContext(ctx, "blockFoundCh", u.stats, false)

	err := u.processBlockFound(ctx1, blockFound.hash, blockFound.baseURL)
	if err != nil {
		if blockFound.errCh != nil {
			blockFound.errCh <- err
		}

		return errors.NewProcessingError("[Init] failed to process block [%s]", blockFound.hash.String(), err)
	}

	if blockFound.errCh != nil {
		blockFound.errCh <- nil
	}

	prometheusBlockValidationBlockFoundCh.Set(float64(len(u.blockFoundCh)))

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
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	// Blocks until the FSM transitions from the IDLE state
	err := u.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		u.logger.Errorf("[Block Validation Service] Failed to wait for FSM transition from IDLE state: %s", err)

		return err
	}

	// start blocks kafka consumer
	u.kafkaConsumerClient.Start(ctx, u.consumerMessageHandler(ctx), kafka.WithRetryAndMoveOn(0, 1, time.Second))

	// this will block
	if err := util.StartGRPCServer(ctx, u.logger, u.settings, "blockvalidation", u.settings.BlockValidation.GRPCListenAddress, func(server *grpc.Server) {
		blockvalidation_api.RegisterBlockValidationAPIServer(server, u)
		closeOnce.Do(func() { close(readyCh) })
	}, nil); err != nil {
		return err
	}

	return nil
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
	u.processSubtreeNotify.Stop()

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
	block, err := model.NewBlockFromBytes(request.Block, u.settings)
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

	if err = u.processBlockFound(ctx, block.Header.Hash(), "legacy", block); err != nil {
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
	block, err := model.NewBlockFromBytes(request.Block, u.settings)
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
	if err = blockassemblyutil.WaitForBlockAssemblyReady(ctx, u.logger, u.blockAssemblyClient, block.Height, uint32(u.settings.ChainCfgParams.CoinbaseMaturity/2)); err != nil {
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

	// only get the bloom filters for the current chain
	bloomFilters, err := u.blockValidation.collectNecessaryBloomFilters(ctx, block, blockHeaders)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewServiceError("[ValidateBlock][%s] failed to collect necessary bloom filters", block.String(), err))
	}

	if ok, err := block.Valid(ctx, u.logger, u.subtreeStore, u.utxoStore, oldBlockIDsMap, bloomFilters, blockHeaders, blockHeaderIDs, nil); !ok {
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
func (u *Server) processBlockFound(ctx context.Context, hash *chainhash.Hash, baseURL string, useBlock ...*model.Block) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "processBlockFound",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationProcessBlockFound),
		tracing.WithDebugLogMessage(u.logger, "[processBlockFound][%s] processing block found from %s", hash.String(), baseURL),
	)
	defer deferFn()

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
		block, err = u.fetchSingleBlock(ctx, hash, baseURL)
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
			u.logger.Infof("[processBlockFound][%s] processBlockFound add to catchup channel", hash.String())
			u.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: baseURL,
			}
		}()

		return nil
	}

	waitForBlockHeight := math.Ceil(float64(u.settings.ChainCfgParams.CoinbaseMaturity) / 2)

	// Wait for block assembly to be ready before processing the block
	if err = blockassemblyutil.WaitForBlockAssemblyReady(ctx, u.logger, u.blockAssemblyClient, block.Height, uint32(waitForBlockHeight)); err != nil {
		// block-assembly is still behind, so we cannot process this block
		return err
	}

	// validate the block
	u.logger.Infof("[processBlockFound][%s] validate block from %s", hash.String(), baseURL)

	// this is a bit of a hack, but we need to turn off optimistic mining when in legacy mode
	if baseURL == "legacy" {
		err = u.blockValidation.ValidateBlock(ctx, block, baseURL, u.blockValidation.bloomFilterStats, true)
	} else {
		err = u.blockValidation.ValidateBlock(ctx, block, baseURL, u.blockValidation.bloomFilterStats)
	}

	if err != nil {
		return errors.NewServiceError("failed block validation BlockFound [%s]", block.String(), err)
	}

	// peer sent us a valid block, so increase its reputation score
	if u.peerMetrics != nil {
		peerMetric := u.peerMetrics.GetOrCreatePeerMetrics(baseURL)
		if peerMetric != nil {
			peerMetric.RecordSuccess()
		}
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
