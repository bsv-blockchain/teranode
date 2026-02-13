// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server implements the subtree validation service and provides functionality for
// validating transaction subtrees within the blockchain.
//
// The Server is the central component of the subtreevalidation package, managing the complete
// lifecycle of subtree validation including transaction validation, storage, and integration
// with other Teranode services. It implements both the gRPC API endpoints and the core
// validation logic.
//
// The Server employs several design patterns:
// - Dependency Injection: All dependencies are provided through the constructor
// - Repository Pattern: Abstraction of storage operations behind interfaces
// - Service Layer: Clear separation between API and business logic
// - Concurrent Processing: Parallel validation of transactions where possible
//
// Thread safety is maintained through careful synchronization primitive usage and
// atomic operations where appropriate.
type Server struct {
	// UnimplementedSubtreeValidationAPIServer embeds the auto-generated gRPC server base
	subtreevalidation_api.UnimplementedSubtreeValidationAPIServer

	// logger handles all logging operations for the service
	logger ulogger.Logger

	// settings contains the configuration parameters for the service
	// including connection details, timeouts, and operational modes
	settings *settings.Settings

	// subtreeStore manages persistent storage of subtrees
	// This blob store is used to save and retrieve complete subtree structures
	subtreeStore blob.Store

	// txStore manages transaction storage
	// This blob store is used to save and retrieve individual transactions
	txStore blob.Store

	// utxoStore manages the Unspent Transaction Output (UTXO) state
	// It's used during transaction validation to verify input spending
	utxoStore utxo.Store

	// validatorClient provides transaction validation services
	// It's used to validate transactions against consensus rules
	validatorClient validator.Interface

	// subtreeCount tracks the number of subtrees processed
	// Uses atomic operations for thread-safe access
	subtreeCount atomic.Int32

	// stats tracks operational statistics for monitoring and diagnostics
	stats *gocore.Stat

	// prioritySubtreeCheckActiveMap tracks active priority subtree checks
	// Maps subtree hash strings to boolean values indicating check status
	prioritySubtreeCheckActiveMap map[string]bool

	// prioritySubtreeCheckActiveMapLock protects concurrent access to the priority map
	prioritySubtreeCheckActiveMapLock sync.Mutex

	// blockchainClient interfaces with the blockchain service
	// Used to retrieve block information and validate chain state
	blockchainClient blockchain.ClientI

	// subtreeConsumerClient consumes subtree-related Kafka messages
	// Handles incoming subtree validation requests from other services
	subtreeConsumerClient kafka.KafkaConsumerGroupI

	// txmetaConsumerClient consumes transaction metadata Kafka messages
	// Processes transaction metadata updates from other services
	txmetaConsumerClient kafka.KafkaConsumerGroupI

	// invalidSubtreeKafkaProducer publishes invalid subtree events to Kafka
	invalidSubtreeKafkaProducer kafka.KafkaAsyncProducerI

	// invalidSubtreeLock is used to synchronize access to the invalid subtree producer
	invalidSubtreeLock sync.Mutex

	// invalidSubtreeDeDuplicateMap is used to de-duplicate invalid subtree messages
	invalidSubtreeDeDuplicateMap *expiringmap.ExpiringMap[string, struct{}]

	// orphanage manages orphaned transactions that are missing their parent transactions
	orphanage *Orphanage

	// bestBlockHeader is used to store the current best block header
	bestBlockHeader atomic.Pointer[model.BlockHeader]

	// bestBlockHeaderMeta is used to store the current best block header metadata
	bestBlockHeaderMeta atomic.Pointer[model.BlockHeaderMeta]

	// currentBlockIDsMap is used to store the current block IDs for the current best block height
	currentBlockIDsMap atomic.Pointer[map[uint32]bool]

	// p2pClient interfaces with the P2P service
	// Used to report successful subtree fetches to improve peer reputation
	p2pClient P2PClientI
}

var (
	// once ensures the quorum is initialized only once
	once sync.Once
	// q is a singleton instance of the quorum manager used for subtree validation
	q *Quorum
)

// New creates a new Server instance with the provided dependencies.
//
// This factory function constructs and initializes a fully configured subtree validation service,
// injecting all required dependencies. It follows the dependency injection pattern to ensure
// testability and proper separation of concerns.
//
// The method ensures that the service is configured with proper stores, clients, and settings
// before it's made available for use. It also initializes internal tracking structures and
// statistics for monitoring.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - logger: Structured logger for operational and debug messages
//   - tSettings: Configuration settings for the service
//   - subtreeStore: Blob store for persistent subtree storage
//   - txStore: Blob store for transaction storage
//   - utxoStore: Store for UTXO set management
//   - validatorClient: Client for transaction validation services
//   - blockchainClient: Client for blockchain interaction
//   - subtreeConsumerClient: Kafka consumer for subtree-related messages
//   - txmetaConsumerClient: Kafka consumer for transaction metadata messages
//
// Returns:
//   - *Server: Fully initialized server instance ready for starting
//   - error: Any error encountered during initialization
func New(
	ctx context.Context,
	logger ulogger.Logger,
	tSettings *settings.Settings,
	subtreeStore blob.Store,
	txStore blob.Store,
	utxoStore utxo.Store,
	validatorClient validator.Interface,
	blockchainClient blockchain.ClientI,
	subtreeConsumerClient kafka.KafkaConsumerGroupI,
	txmetaConsumerClient kafka.KafkaConsumerGroupI,
	p2pClient P2PClientI,
) (*Server, error) {
	u := &Server{
		logger:                            logger,
		settings:                          tSettings,
		subtreeStore:                      subtreeStore,
		txStore:                           txStore,
		utxoStore:                         utxoStore,
		validatorClient:                   validatorClient,
		subtreeCount:                      atomic.Int32{},
		stats:                             gocore.NewStat("subtreevalidation"),
		prioritySubtreeCheckActiveMap:     map[string]bool{},
		prioritySubtreeCheckActiveMapLock: sync.Mutex{},
		blockchainClient:                  blockchainClient,
		subtreeConsumerClient:             subtreeConsumerClient,
		txmetaConsumerClient:              txmetaConsumerClient,
		invalidSubtreeDeDuplicateMap:      expiringmap.New[string, struct{}](time.Minute * 1),
		p2pClient:                         p2pClient,
	}

	var err error

	// Initialize orphanage
	u.orphanage, err = NewOrphanage(tSettings.SubtreeValidation.OrphanageTimeout, tSettings.SubtreeValidation.OrphanageMaxSize, logger)
	if err != nil {
		return nil, errors.NewConfigurationError("Failed to create orphanage: %v", err)
	}

	once.Do(func() {
		quorumPath := tSettings.SubtreeValidation.QuorumPath
		if quorumPath == "" {
			err = errors.NewConfigurationError("No subtree_quorum_path specified")
			return
		}

		var absoluteQuorumTimeout = tSettings.SubtreeValidation.QuorumAbsoluteTimeout

		q, err = NewQuorum(
			u.logger,
			u.subtreeStore,
			quorumPath,
			WithAbsoluteTimeout(absoluteQuorumTimeout),
		)
	})

	if err != nil {
		return nil, err
	}

	// create a caching tx meta store
	if tSettings.SubtreeValidation.TxMetaCacheEnabled {
		logger.Infof("Using cached version of tx meta store")

		var err error

		u.utxoStore, err = txmetacache.NewTxMetaCache(ctx, tSettings, logger, utxoStore, txmetacache.Unallocated)
		if err != nil {
			logger.Errorf("Failed to create tx meta cache: %v", err)
		}
	} else {
		u.utxoStore = utxoStore
	}

	// Initialize Kafka producer for invalid subtrees if configured
	if tSettings.Kafka.InvalidSubtrees != "" {
		logger.Infof("Initializing Kafka producer for invalid subtrees topic: %s", tSettings.Kafka.InvalidSubtrees)

		var err error

		u.invalidSubtreeKafkaProducer, err = initialiseInvalidSubtreeKafkaProducer(ctx, logger, tSettings)
		if err != nil {
			logger.Errorf("Failed to create Kafka producer for invalid subtrees: %v", err)
		} else {
			// Start the producer with a message channel
			go u.invalidSubtreeKafkaProducer.Start(ctx, make(chan *kafka.Message, 100))
		}
	} else {
		logger.Infof("No Kafka topic configured for invalid subtrees")
	}

	// get and set the initial best block
	if err = u.updateBestBlock(ctx); err != nil {
		logger.Errorf("[SubtreeValidation] failed to get initial best block: %s", err)
	}

	if u.blockchainClient != nil {
		go u.blockchainSubscriptionListener(ctx)
	}

	return u, nil
}

func (u *Server) blockchainSubscriptionListener(ctx context.Context) {
	ctxLogger := u.logger.WithTraceContext(ctx)
	var (
		subscribeCtx    context.Context
		subscribeCancel context.CancelFunc
	)

	for {
		select {
		case <-ctx.Done():
			ctxLogger.Warnf("[SubtreeValidation:blockchainSubscriptionListener] exiting setMined goroutine: %s", ctx.Err())
			return
		default:
			ctxLogger.Infof("[SubtreeValidation:blockchainSubscriptionListener] subscribing to blockchain for setTxMined signal")

			subscribeCtx, subscribeCancel = context.WithCancel(ctx)

			blockchainSubscription, err := u.blockchainClient.Subscribe(subscribeCtx, "subtreevalidation")
			if err != nil {
				ctxLogger.Errorf("[SubtreeValidation:blockchainSubscriptionListener] failed to subscribe to blockchain: %s", err)

				// Cancel context before retrying to prevent leak
				subscribeCancel()

				// backoff for 5 seconds and try again
				time.Sleep(5 * time.Second)

				continue
			}

		subscriptionLoop:
			for {
				select {
				case <-ctx.Done():
					subscribeCancel()
					return

				case notification, ok := <-blockchainSubscription:
					if !ok {
						// Channel closed, reconnect
						ctxLogger.Warnf("[SubtreeValidation:blockchainSubscriptionListener] subscription channel closed, reconnecting")
						subscribeCancel()
						time.Sleep(1 * time.Second)
						break subscriptionLoop
					}

					if notification == nil {
						continue
					}

					if notification.Type == model.NotificationType_Block {
						cHash := chainhash.Hash(notification.Hash)
						ctxLogger.Infof("[SubtreeValidation:blockchainSubscriptionListener] received Block notification: %s", cHash.String())

						// get the best block header, we might have just added an invalid block that we do not want to count
						if err = u.updateBestBlock(ctx); err != nil {
							// Check if context was cancelled - if so, exit gracefully
							if ctx.Err() != nil {
								ctxLogger.Infof("[SubtreeValidation:blockchainSubscriptionListener] context cancelled, stopping listener")
								subscribeCancel()
								return
							}
							ctxLogger.Errorf("[SubtreeValidation:blockchainSubscriptionListener] failed to update best block: %s", err)
						}
					}
				}
			}
		}
	}
}

func (u *Server) updateBestBlock(ctx context.Context) error {
	bestBlockHeader, bestBlockHeaderMeta, err := u.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return errors.NewProcessingError("[SubtreeValidation:blockchainSubscriptionListener] failed to get best block header: %s", err)
	}

	u.bestBlockHeader.Store(bestBlockHeader)
	u.bestBlockHeaderMeta.Store(bestBlockHeaderMeta)
	u.subtreeStore.SetCurrentBlockHeight(bestBlockHeaderMeta.Height)

	ids, err := u.blockchainClient.GetBlockHeaderIDs(ctx, bestBlockHeader.Hash(), uint64(u.settings.GetUtxoStoreBlockHeightRetention()*2))
	if err != nil {
		return errors.NewProcessingError("[SubtreeValidation:blockchainSubscriptionListener] failed to get block header IDs", err)
	}

	blockIDsMap := make(map[uint32]bool, len(ids))
	for _, id := range ids {
		blockIDsMap[id] = true
	}

	u.currentBlockIDsMap.Store(&blockIDsMap)

	return nil
}

// Health checks the health status of the service and its dependencies.
//
// This method implements the standard Teranode health check interface used across all services
// for consistent monitoring, alerting, and orchestration. It provides both readiness and
// liveness checking capabilities to support different operational scenarios.
//
// The method performs checks appropriate to the service's role, including:
// - Verifying store access for subtree, transaction, and UTXO data
// - Checking connections to dependent services (validator, blockchain)
// - Validating Kafka consumer health
// - Ensuring internal state consistency
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - checkLiveness: If true, performs only basic liveness checks; if false, includes
//     more thorough readiness checks
//
// Returns:
//   - int: HTTP status code representing health (200 for healthy, other codes for issues)
//   - string: Human-readable description of the current health state
//   - error: Detailed error information if the service is unhealthy
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	var brokersURL []string
	if u.txmetaConsumerClient != nil { // tests may not set this
		brokersURL = u.txmetaConsumerClient.BrokersURL()
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := make([]health.Check, 0, 6)

	// Check if the gRPC server is actually listening and accepting requests
	// Only check if the address is configured (not empty)
	if u.settings.SubtreeValidation.GRPCListenAddress != "" {
		checks = append(checks, health.Check{
			Name: "gRPC Server",
			Check: health.CheckGRPCServerWithSettings(u.settings.SubtreeValidation.GRPCListenAddress, u.settings, func(ctx context.Context, conn *grpc.ClientConn) error {
				client := subtreevalidation_api.NewSubtreeValidationAPIClient(conn)
				_, err := client.HealthGRPC(ctx, &subtreevalidation_api.EmptyMessage{})
				return err
			}),
		})
	}

	checks = append(checks, health.Check{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)})

	if u.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: u.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(u.blockchainClient)})
	}

	if u.subtreeStore != nil {
		checks = append(checks, health.Check{Name: "SubtreeStore", Check: u.subtreeStore.Health})
	}

	if u.utxoStore != nil {
		checks = append(checks, health.Check{Name: "UTXOStore", Check: u.utxoStore.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC implements the gRPC health check endpoint.
func (u *Server) HealthGRPC(ctx context.Context, _ *subtreevalidation_api.EmptyMessage) (*subtreevalidation_api.HealthResponse, error) {
	startTime := time.Now()
	defer func() {
		prometheusHealth.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	// Add context value to prevent circular dependency when checking gRPC server health
	ctx = context.WithValue(ctx, "skip-grpc-self-check", true)
	status, details, err := u.Health(ctx, false)

	return &subtreevalidation_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.Now(),
	}, errors.WrapGRPC(err)
}

// Init initializes the server metrics and performs any necessary setup.
//
// This method completes the initialization process by setting up components that
// require runtime initialization rather than construction-time setup. It's called
// after New() but before Start() to ensure all systems are properly initialized.
//
// The initialization is designed to be idempotent and can be safely called multiple times,
// though typically it's only called once after construction and before starting the service.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//
// Returns:
//   - error: Any error encountered during initialization
func (u *Server) Init(ctx context.Context) (err error) {
	InitPrometheusMetrics()

	return nil
}

func (u *Server) GetUutxoStore() utxo.Store {
	return u.utxoStore
}

func (u *Server) SetUutxoStore(s utxo.Store) {
	u.utxoStore = s
}

// Start initializes and starts the server components including Kafka consumers and gRPC server.
//
// This method launches all the operational components of the subtree validation service,
// including:
// - Kafka consumers for subtree and transaction metadata messages
// - The gRPC server for API access
// - Any background workers or timers required for operation
//
// The method implements a safe startup sequence to ensure all components are properly
// initialized before the service is marked as ready. It also handles proper error propagation
// if any component fails to start.
//
// Once all components are successfully started, the method signals readiness through the
// provided channel and then blocks until the context is canceled or an error occurs. This
// design allows the caller to coordinate the startup of multiple services.
//
// Parameters:
//   - ctx: Context for cancellation and coordination
//   - readyCh: Channel to signal when the service is fully started and ready for requests
//
// Returns:
//   - error: Any error encountered during startup or operation
func (u *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	// Blocks until the FSM transitions from the IDLE state
	err := u.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		u.logger.Errorf("[Subtree Validation Service] Failed to wait for FSM transition from IDLE state: %s", err)

		return err
	}

	// start kafka consumers
	u.subtreeConsumerClient.Start(ctx, u.subtreeMessageHandler(ctx), kafka.WithLogErrorAndMoveOn())
	u.txmetaConsumerClient.Start(ctx, u.txmetaMessageHandler(ctx), kafka.WithLogErrorAndMoveOn())

	// this will block
	if err := util.StartGRPCServer(ctx, u.logger, u.settings, "subtreevalidation", u.settings.SubtreeValidation.GRPCListenAddress, func(server *grpc.Server) {
		subtreevalidation_api.RegisterSubtreeValidationAPIServer(server, u)
		closeOnce.Do(func() { close(readyCh) })
	}, nil); err != nil {
		return err
	}

	return nil
}

// Stop gracefully shuts down the server components including Kafka consumers.
//
// This method ensures a clean and orderly shutdown of all service components,
// allowing in-progress operations to complete when possible and releasing
// all resources properly. It follows a consistent shutdown sequence that:
// 1. Stops accepting new requests
// 2. Waits for in-progress operations to complete (with reasonable timeouts)
// 3. Closes connections and releases resources
//
// The method is designed to be called when the service needs to be terminated,
// either for normal shutdown or in response to system signals.
//
// Parameters:
//   - ctx: Context for cancellation and tracing (unused in current implementation)
//
// Returns:
//   - error: Any error encountered during shutdown
func (u *Server) Stop(_ context.Context) error {
	// close the kafka consumers gracefully
	if u.subtreeConsumerClient != nil {
		if err := u.subtreeConsumerClient.Close(); err != nil {
			u.logger.Errorf("[BlockValidation] failed to close kafka consumer gracefully: %v", err)
		}
	}

	if u.txmetaConsumerClient != nil {
		if err := u.txmetaConsumerClient.Close(); err != nil {
			u.logger.Errorf("[BlockValidation] failed to close kafka consumer gracefully: %v", err)
		}
	}

	return nil
}

// CheckSubtreeFromBlock validates a subtree and its transactions based on the provided request.
//
// This method is the primary gRPC API endpoint for subtree validation, responsible for
// coordinating the validation process for an entire subtree of interdependent transactions.
// It ensures that all transactions in the subtree adhere to consensus rules and can be
// added to the blockchain.
//
// The method implements several important features:
// - Distributed locking to prevent duplicate validation of the same subtree
// - Retry logic for lock acquisition with exponential backoff
// - Support for both legacy and current validation paths for backward compatibility
// - Proper resource cleanup even in error conditions
// - Structured error responses with appropriate gRPC status codes
//
// Validation includes checking that:
// - All transactions in the subtree are valid according to consensus rules
// - All transaction inputs refer to unspent outputs or other transactions in the subtree
// - No double-spending conflicts exist within the subtree or with existing chain state
// - Transactions satisfy all policy rules (fees, standardness, etc.)
//
// Parameters:
//   - ctx: Context for cancellation and tracing, with appropriate deadlines
//   - request: Contains subtree hash, base URL for retrieving missing transactions,
//     block height, and block hash information
//
// Returns:
//   - *CheckSubtreeFromBlockResponse: Response indicating validation success or failure
//   - error: Any error encountered during validation with appropriate gRPC status codes
//
// The method will retry lock acquisition for up to 20 seconds with exponential backoff,
// making it resilient to temporary contention when multiple services attempt to validate
// the same subtree simultaneously.
func (u *Server) CheckSubtreeFromBlock(ctx context.Context, request *subtreevalidation_api.CheckSubtreeFromBlockRequest) (*subtreevalidation_api.CheckSubtreeFromBlockResponse, error) {
	subtreeBlessed, err := u.checkSubtreeFromBlock(ctx, request)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &subtreevalidation_api.CheckSubtreeFromBlockResponse{
		Blessed: subtreeBlessed,
	}, nil
}

// checkSubtreeFromBlock is the internal implementation of subtree validation logic.
//
// This method contains the core business logic for validating a subtree, separated from
// the API-level concerns handled by the public CheckSubtreeFromBlock method. The separation
// allows for cleaner testing and better separation of concerns.
//
// The method expects the subtree to be stored in the subtree store with a special extension
// (.subtreeToCheck instead of .subtree) to differentiate between validated and unvalidated
// subtrees. This prevents the validation service from mistakenly treating an unvalidated
// subtree as already validated.
//
// The validation process includes:
// 1. Retrieving the subtree data from storage
// 2. Parsing the subtree structure
// 3. Checking for existing transaction metadata
// 4. Retrieving and validating missing transactions
// 5. Verifying transaction dependencies and ordering
// 6. Confirming all transactions meet consensus rules
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - request: Contains subtree hash, base URL, and block information
//
// Returns:
//   - bool: True if the subtree is valid and can be added to the blockchain
//   - error: Detailed error information if validation fails
//
// This method is called internally by CheckSubtreeFromBlock after acquiring the
// appropriate locks to prevent duplicate processing.
func (u *Server) checkSubtreeFromBlock(ctx context.Context, request *subtreevalidation_api.CheckSubtreeFromBlockRequest) (ok bool, err error) {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "checkSubtree",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusSubtreeValidationCheckSubtree),
		tracing.WithLogMessage(u.logger, "[checkSubtree] called for subtree %s (block %s / height %d)", utils.ReverseAndHexEncodeSlice(request.Hash), utils.ReverseAndHexEncodeSlice(request.BlockHash), request.BlockHeight),
	)
	defer func() {
		deferFn(err)
	}()

	if request.BaseUrl == "" {
		return false, errors.NewInvalidArgumentError("[CheckSubtree] Missing base URL in request")
	}

	var (
		hash              *chainhash.Hash
		blockHash         *chainhash.Hash
		previousBlockHash *chainhash.Hash
	)

	hash, err = chainhash.NewHash(request.Hash)
	if err != nil {
		return false, errors.NewProcessingError("[CheckSubtree] Failed to parse subtree hash from request", err)
	}

	blockHash, err = chainhash.NewHash(request.BlockHash)
	if err != nil {
		return false, errors.NewProcessingError("[CheckSubtree] Failed to parse block hash from request", err)
	}

	previousBlockHash, err = chainhash.NewHash(request.PreviousBlockHash)
	if err != nil {
		return false, errors.NewProcessingError("[CheckSubtree] Failed to parse previous block hash from request", err)
	}

	u.logger.Debugf("[CheckSubtree] Received priority subtree message for %s:%s from %s", blockHash.String(), hash.String(), request.BaseUrl)
	defer u.logger.Debugf("[CheckSubtree] Finished processing priority subtree message for %s:%s from %s", blockHash.String(), hash.String(), request.BaseUrl)

	u.prioritySubtreeCheckActiveMapLock.Lock()
	u.prioritySubtreeCheckActiveMap[hash.String()] = true
	u.prioritySubtreeCheckActiveMapLock.Unlock()

	defer func() {
		u.prioritySubtreeCheckActiveMapLock.Lock()
		delete(u.prioritySubtreeCheckActiveMap, hash.String())
		u.prioritySubtreeCheckActiveMapLock.Unlock()
	}()

	// Note we are not giving up, we either need to see the file exists or we get the lock
	gotLock, exists, releaseLockFunc, err := q.TryLockIfNotExistsWithTimeout(ctx, hash, fileformat.FileTypeSubtree)
	if err != nil {
		return false, errors.NewError("[CheckSubtree] error getting lock for Subtree %s", hash.String(), err)
	}
	defer releaseLockFunc()

	if exists {
		u.logger.Infof("[CheckSubtree] Priority subtree request no longer needed as subtree now exists for %s from %s", hash.String(), request.BaseUrl)

		return true, nil
	}

	if !gotLock {
		return false, errors.NewError("[CheckSubtree] failed to get lock for subtree %s due to timeout", hash.String())
	}

	// get the previous block headers on this chain and pass into the validation
	blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, previousBlockHash, uint64(u.settings.GetUtxoStoreBlockHeightRetention()*2))
	if err != nil {
		return false, errors.NewProcessingError("[CheckSubtree] Failed to get block headers from blockchain client", err)
	}

	blockIds := make(map[uint32]bool, len(blockHeaderIDs))

	for _, blockID := range blockHeaderIDs {
		blockIds[blockID] = true
	}

	u.logger.Infof("[CheckSubtree] Processing priority subtree message for %s from %s", hash.String(), request.BaseUrl)

	var subtree *subtreepkg.Subtree

	// Check if the base URL is "legacy", which indicates that the subtree is coming from a block from the legacy service.
	if request.BaseUrl == "legacy" {
		// read from legacy store
		subtreeReader, err := u.subtreeStore.GetIoReader(
			ctx,
			hash[:],
			fileformat.FileTypeSubtreeToCheck,
		)
		if err != nil {
			return false, errors.NewStorageError("[getSubtreeTxHashes][%s] failed to get subtree from store", hash.String(), err)
		}

		subtree, err = subtreepkg.NewSubtreeFromReader(subtreeReader)
		_ = subtreeReader.Close() // close the reader after use
		if err != nil {
			return false, errors.NewProcessingError("[CheckSubtree] Failed to create subtree from bytes", err)
		}

		txHashes := make([]chainhash.Hash, subtree.Length())

		for i := 0; i < subtree.Length(); i++ {
			txHashes[i] = subtree.Nodes[i].Hash
		}

		v := ValidateSubtree{
			SubtreeHash:   *hash,
			BaseURL:       request.BaseUrl,
			TxHashes:      txHashes,
			AllowFailFast: false,
		}

		validatorOptions := []validator.Option{
			validator.WithSkipPolicyChecks(true),
			validator.WithCreateConflicting(true),
			validator.WithIgnoreLocked(true),
		}

		currentState, err := u.blockchainClient.GetFSMCurrentState(ctx)
		if err != nil {
			return false, errors.NewProcessingError("[CheckSubtree] Failed to get FSM current state", err)
		}

		// During legacy syncing or catching up, disable adding transactions to block assembly
		if *currentState == blockchain.FSMStateLEGACYSYNCING || *currentState == blockchain.FSMStateCATCHINGBLOCKS {
			validatorOptions = append(validatorOptions, validator.WithAddTXToBlockAssembly(false))
		}

		// Call the validateSubtreeInternal method
		// making sure to skip policy checks, since we are validating a block that has already been mined
		if _, err = u.ValidateSubtreeInternal(
			ctx,
			v,
			request.BlockHeight,
			blockIds,
			validatorOptions...,
		); err != nil {
			return false, errors.NewProcessingError("[CheckSubtree] Failed to validate legacy subtree %s", hash.String(), err)
		}

		u.logger.Debugf("[CheckSubtree] Finished processing priority legacy subtree message for %s from %s", hash.String(), request.BaseUrl)

		return true, nil
	}

	// This line is only reached when the base URL is not "legacy"
	v := ValidateSubtree{
		SubtreeHash:   *hash,
		BaseURL:       request.BaseUrl,
		AllowFailFast: false,
	}

	// Call the ValidateSubtreeInternal method
	if subtree, err = u.ValidateSubtreeInternal(
		ctx,
		v,
		request.BlockHeight,
		blockIds,
		validator.WithSkipPolicyChecks(true),
		validator.WithCreateConflicting(true),
		validator.WithIgnoreLocked(true),
	); err != nil {
		return false, errors.NewProcessingError("[CheckSubtree] Failed to validate subtree %s", hash.String(), err)
	}

	if subtree != nil {
		// remove all transactions that are part of the subtree from the orphanage
		for _, node := range subtree.Nodes {
			u.orphanage.Delete(node.Hash)
		}
	}

	u.processOrphans(ctx, *blockHash, request.BlockHeight, blockIds)

	u.logger.Debugf("[CheckSubtree] Finished processing priority subtree message for %s from %s", hash.String(), request.BaseUrl)

	return true, nil
}

func (u *Server) processOrphans(ctx context.Context, blockHash chainhash.Hash, blockHeight uint32, blockIds map[uint32]bool) {
	initialLength := u.orphanage.Len()

	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processOrphans",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusSubtreeValidationCheckSubtree),
		tracing.WithLogMessage(u.logger, "[processOrphans] Processing orphans for block %s at block height %d", blockHash.String(), blockHeight),
	)
	defer func() {
		u.logger.Infof("[processOrphans] Finished processing orphans for block %s at block height %d, initial orphanage length: %d, final orphanage length: %d", blockHash.String(), blockHeight, initialLength, u.orphanage.Len())
		deferFn()
	}()

	// process remaining orphaned transactions if any
	if u.orphanage.Len() > 0 {
		u.logger.Infof("[CheckSubtreeFromBlock] Processing orphaned transactions after subtree validation, count: %d", u.orphanage.Len())

		processedOrphans := atomic.Uint32{}
		processedValidatorOptions := validator.ProcessOptions()
		orphanTxs := u.orphanage.Items()

		// first we need to process all the orphans into levels, making sure we process them
		// in the correct order, so we can bless them correctly
		orphanMissingTxs := make([]missingTx, 0, len(orphanTxs))
		for _, item := range orphanTxs {
			orphanMissingTxs = append(orphanMissingTxs, missingTx{
				tx: item,
			})
		}

		maxLevel, txsPerLevel, err := u.selectPrepareTxsPerLevel(ctx, orphanMissingTxs)
		if err != nil {
			u.logger.Errorf("[CheckSubtreeFromBlock] Failed to prepare transactions per level: %v", err)
			return
		}

		for level := uint32(0); level <= maxLevel; level++ {
			// we process each level of transactions in parallel
			g, gCtx := errgroup.WithContext(ctx)
			util.SafeSetLimit(g, u.settings.SubtreeValidation.SpendBatcherSize*2)

			for _, mTx := range txsPerLevel[level] {
				tx := mTx.tx

				g.Go(func() error {
					txMeta, txErr := u.blessMissingTransaction(gCtx, blockHash, chainhash.Hash{}, tx, blockHeight+1, blockIds, processedValidatorOptions)
					if txErr == nil && txMeta != nil {
						// transaction was successfully blessed, now remove it from the orphanage
						u.orphanage.Delete(*tx.TxIDChainHash())
						processedOrphans.Add(1)
					} else {
						u.logger.Debugf("[CheckSubtreeFromBlock] Failed to bless orphaned transaction %s: %v", tx.TxIDChainHash().String(), txErr)
					}

					return nil
				})
			}

			if err := g.Wait(); err != nil {
				u.logger.Errorf("[CheckSubtreeFromBlock] Failed to process orphaned transactions: %v", err)
			}
		}

		u.logger.Infof("[CheckSubtreeFromBlock] Processed %d orphaned transactions after subtree validation", processedOrphans.Load())
	}
}

// initialiseInvalidSubtreeKafkaProducer creates a Kafka producer for invalid subtree events
func initialiseInvalidSubtreeKafkaProducer(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (*kafka.KafkaAsyncProducer, error) {
	logger.Infof("Initializing Kafka producer for invalid subtrees topic: %s", tSettings.Kafka.InvalidSubtrees)
	logger.Infof("InvalidBlocksConfig: %s", tSettings.Kafka.InvalidBlocksConfig)
	logger.Infof("InvalidSubtreesConfig: %s", tSettings.Kafka.InvalidSubtreesConfig)

	invalidSubtreeKafkaProducer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, tSettings.Kafka.InvalidSubtreesConfig, &tSettings.Kafka)
	if err != nil {
		return nil, err
	}

	return invalidSubtreeKafkaProducer, nil
}

// publishInvalidSubtree publishes an invalid subtree event to Kafka
func (u *Server) publishInvalidSubtree(ctx context.Context, subtreeHash, peerURL, reason string) {
	ctxLogger := u.logger.WithTraceContext(ctx)
	if u.invalidSubtreeKafkaProducer == nil {
		return
	}

	if u.blockchainClient != nil {
		var (
			state *blockchain.FSMStateType
			err   error
		)

		if state, err = u.blockchainClient.GetFSMCurrentState(ctx); err != nil {
			ctxLogger.Errorf("[publishInvalidSubtree] failed to publish invalid subtree - error getting blockchain FSM state: %v", err)

			return
		}

		if *state == blockchain_api.FSMStateType_CATCHINGBLOCKS || *state == blockchain_api.FSMStateType_LEGACYSYNCING {
			// ignore notifications while syncing or catching up
			return
		}
	}

	u.invalidSubtreeLock.Lock()
	defer u.invalidSubtreeLock.Unlock()

	// de-duplicate the subtree hash to avoid flooding Kafka with the same message
	if _, ok := u.invalidSubtreeDeDuplicateMap.Get(subtreeHash); ok {
		ctxLogger.Debugf("[publishInvalidSubtree] Skipping duplicate invalid subtree %s from peer %s to Kafka: %s", subtreeHash, peerURL, reason)

		return
	}

	u.invalidSubtreeDeDuplicateMap.Set(subtreeHash, struct{}{})

	ctxLogger.Infof("[publishInvalidSubtree] publishing invalid subtree %s from peer %s to Kafka: %s", subtreeHash, peerURL, reason)

	msg := &kafkamessage.KafkaInvalidSubtreeTopicMessage{
		SubtreeHash: subtreeHash,
		PeerUrl:     peerURL,
		Reason:      reason,
	}

	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		ctxLogger.Errorf("[publishInvalidSubtree] failed to marshal invalid subtree message: %v", err)
		return
	}

	kafkaMsg := &kafka.Message{
		Key:   []byte(subtreeHash),
		Value: msgBytes,
	}
	u.invalidSubtreeKafkaProducer.Publish(kafkaMsg)
}
