// Package blockchain provides functionality for managing the Bitcoin blockchain within the Teranode system.
//
// The blockchain package is responsible for:
// - Maintaining the blockchain data structure and state management
// - Processing new blocks and managing the chain selection logic
// - Providing access to blockchain data via gRPC and HTTP APIs
// - Managing the blockchain's finite state machine (FSM) for different operational modes
// - Supporting subscriptions and notifications for blockchain events
// - Publishing block data to Kafka for downstream services
// - Handling block validation and revalidation requests
//
// This package serves as the central source of truth for blockchain state in the Teranode
// system and integrates with other services like blockvalidation and blockpersister.
// It implements a resilient state management system using a finite state machine pattern
// that can recover from interruptions and maintain consistency across service restarts.
package blockchain

import (
	"container/ring"
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	blockchain_sql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/looplab/fsm"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// subscriber represents a subscription to blockchain notifications.
//
// subscriber encapsulates the connection to a client interested in blockchain events,
// providing a mechanism for sending notifications about new blocks, state changes,
// and other blockchain events. It maintains the gRPC stream connection until the
// client disconnects or the subscription is explicitly terminated.
//
// This struct enables the publish-subscribe pattern where multiple services can
// receive real-time updates about blockchain state changes without polling.
type subscriber struct {
	subscription blockchain_api.BlockchainAPI_SubscribeServer // The gRPC subscription server
	source       string                                       // Source identifier of the subscription
	done         chan struct{}                                // Channel to signal when subscription is done
}

// Blockchain represents the main blockchain service structure.
//
// The Blockchain struct is the central component of the blockchain service, responsible
// for maintaining the Bitcoin blockchain state and providing access to blockchain data.
// It manages the lifecycle of blocks from addition to storage, handles subscriptions for
// notifications about blockchain events, and coordinates with other services through
// both synchronous (gRPC/HTTP) and asynchronous (Kafka) communication channels.
//
// The service uses a finite state machine (FSM) to manage its operational state,
// allowing it to handle different modes of operation such as normal processing,
// synchronization, and recovery scenarios. This design provides resilience and
// ensures consistent behavior across service restarts.
//
// Concurrency is managed through multiple channels and mutex-protected data structures,
// enabling safe parallel processing of blockchain operations while maintaining data integrity.
type Blockchain struct {
	blockchain_api.UnimplementedBlockchainAPIServer
	addBlockChan                  chan *blockchain_api.AddBlockRequest // Channel for adding blocks
	store                         blockchain_store.Store               // Storage interface for blockchain data
	logger                        ulogger.Logger                       // Logger instance
	settings                      *settings.Settings                   // Configuration settings
	newSubscriptions              chan subscriber                      // Channel for new subscriptions
	deadSubscriptions             chan subscriber                      // Channel for ended subscriptions
	subscribers                   map[subscriber]bool                  // Active subscribers map
	subscribersMu                 sync.RWMutex                         // Mutex for subscribers map
	notifications                 chan *blockchain_api.Notification    // Channel for notifications
	newBlock                      chan struct{}                        // Channel signaling new block events
	difficulty                    *Difficulty                          // Difficulty calculation instance
	blocksFinalKafkaAsyncProducer kafka.KafkaAsyncProducerI            // Kafka producer for final blocks
	kafkaChan                     chan *kafka.Message                  // Channel for Kafka messages
	stats                         *gocore.Stat                         // Statistics tracking
	finiteStateMachine            *fsm.FSM                             // FSM for blockchain state
	stateChangeTimestamp          time.Time                            // Timestamp of last state change
	AppCtx                        context.Context                      // Application context
	localTestStartState           string                               // Initial state for testing
	subscriptionManagerReady      atomic.Bool                          // Flag indicating subscription manager is ready

	// Blob deletion batch token management
	batchTokens   map[string]*blobDeletionBatchToken // Active batch tokens
	batchTokensMu sync.RWMutex                       // Mutex for batch tokens map
}

// blobDeletionBatchToken represents an acquired batch of deletions with a lock.
type blobDeletionBatchToken struct {
	token         string
	acquiredAt    time.Time
	expiresAt     time.Time
	deletionIDs   []int64
	deletionCount int
}

// New creates a new Blockchain instance with the provided dependencies.
//
// This constructor initializes the core blockchain service with all required components and
// sets up internal channels for communication between different parts of the service.
// It configures metrics tracking, initializes the difficulty calculator, and prepares the
// subscription management system.
//
// The optional localTestStartFromState parameter allows initializing the blockchain service
// in a specific FSM state for testing purposes, bypassing the normal state initialization
// process that would read from persistent storage.
//
// Parameters:
// - ctx: Application context for lifecycle management and cancellation
// - logger: Logger instance for service-level logging
// - tSettings: Configuration settings for the blockchain service
// - store: Storage interface for persisting blockchain data
// - blocksFinalKafkaAsyncProducer: Kafka producer for publishing finalized blocks
// - localTestStartFromState: Optional initial FSM state for testing purposes
//
// Returns:
// - A configured Blockchain instance and nil error on success
// - nil and an error if initialization fails
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blockchain_store.Store, blocksFinalKafkaAsyncProducer kafka.KafkaAsyncProducerI, localTestStartFromState ...string) (*Blockchain, error) {
	initPrometheusMetrics()

	d, err := NewDifficulty(store, logger, tSettings)
	if err != nil {
		logger.Errorf("[BlockAssembler] Couldn't create difficulty: %v", err)
	}

	b := &Blockchain{
		store:                         store,
		logger:                        logger,
		settings:                      tSettings,
		addBlockChan:                  make(chan *blockchain_api.AddBlockRequest, 10),
		newSubscriptions:              make(chan subscriber, 10),
		deadSubscriptions:             make(chan subscriber, 10),
		subscribers:                   make(map[subscriber]bool),
		notifications:                 make(chan *blockchain_api.Notification, 100),
		newBlock:                      make(chan struct{}, 10),
		difficulty:                    d,
		stats:                         gocore.NewStat("blockchain"),
		AppCtx:                        ctx,
		blocksFinalKafkaAsyncProducer: blocksFinalKafkaAsyncProducer,
		batchTokens:                   make(map[string]*blobDeletionBatchToken),
	}

	// Initialize subscription manager as not ready
	b.subscriptionManagerReady.Store(false)

	if len(localTestStartFromState) >= 1 && localTestStartFromState[0] != "" {
		// Convert the string state to FSMStateType using the map
		_, ok := blockchain_api.FSMStateType_value[localTestStartFromState[0]]
		if !ok {
			// Handle the case where the state is not found in the map
			logger.Errorf("Invalid initial state: %s", localTestStartFromState[0])
		} else {
			b.localTestStartState = localTestStartFromState[0]
		}
	}

	return b, nil
}

// GetStoreFSMState retrieves the current FSM state from the store.
//
// This method provides access to the persisted FSM state directly from the underlying storage,
// which may differ from the in-memory state of the service's FSM. This is useful for
// diagnostics and recovery scenarios where the persisted state needs to be inspected
// without modifying the running service state.
//
// Parameters:
// - ctx: Context for the operation with timeout and cancellation support
//
// Returns:
// - The persisted FSM state as a string
// - An error if the retrieval operation fails
func (b *Blockchain) GetStoreFSMState(ctx context.Context) (string, error) {
	return b.store.GetFSMState(ctx)
}

// ResetFSMS resets the finite state machine to nil (used for testing).
//
// This method is primarily intended for testing purposes to force a clean state.
// It allows tests to control when the FSM is initialized and what state it starts in,
// enabling more deterministic test scenarios around state transitions and recovery.
//
// Note: This method should only be called in test environments, as resetting the FSM
// in a production environment would lead to inconsistent state and potential data loss.
func (b *Blockchain) ResetFSMS() {
	b.finiteStateMachine = nil
}

// Health checks the health status of the blockchain service.
//
// This method performs health checks for both liveness (whether the service is running)
// and readiness (whether the service and its dependencies are ready to accept requests).
// The behavior changes based on the checkLiveness parameter:
//
//   - Liveness check (checkLiveness=true): Verifies only the internal service state,
//     used by orchestration systems to determine if the service needs to be restarted.
//     A service might be alive but not ready to accept requests.
//
//   - Readiness check (checkLiveness=false): Verifies both the service and its dependencies
//     (Kafka, blockchain store) are operational and ready to accept requests. Used for
//     determining if traffic should be routed to this service instance.
//
// Parameters:
// - ctx: Context for the operation with timeout and cancellation support
// - checkLiveness: Boolean flag to control whether to check liveness (true) or readiness (false)
//
// Returns:
// - HTTP status code (200 for healthy, 503 for unhealthy)
// - Human-readable status message with health details
// - Error if the health check encounters an unexpected failure
func (b *Blockchain) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return health.CheckAll(ctx, checkLiveness, nil)
	}

	var brokersURL []string
	if b.blocksFinalKafkaAsyncProducer != nil { // tests may not set this
		brokersURL = b.blocksFinalKafkaAsyncProducer.BrokersURL()
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	var checks []health.Check

	// Check if the gRPC server is actually listening and accepting requests
	// Only check if the address is configured (not empty)
	if b.settings.BlockChain.GRPCListenAddress != "" {
		checks = append(checks, health.Check{
			Name: "gRPC Server",
			Check: health.CheckGRPCServerWithSettings(b.settings.BlockChain.GRPCListenAddress, b.settings, func(ctx context.Context, conn *grpc.ClientConn) error {
				client := blockchain_api.NewBlockchainAPIClient(conn)
				_, err := client.HealthGRPC(ctx, nil)
				return err
			}),
		})
	}

	// Check if the HTTP server is actually listening and accepting requests
	if b.settings.BlockChain.HTTPListenAddress != "" {
		addr := b.settings.BlockChain.HTTPListenAddress
		if strings.HasPrefix(addr, ":") {
			addr = "localhost" + addr
		}
		checks = append(checks, health.Check{
			Name:  "HTTP Server",
			Check: health.CheckHTTPServer(fmt.Sprintf("http://%s", addr), "/health"),
		})
	}

	// Only check Kafka if it's configured
	if len(brokersURL) > 0 {
		checks = append(checks, health.Check{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)})
	}

	if b.store != nil {
		checks = append(checks, health.Check{Name: "BlockchainStore", Check: b.store.Health})
	}

	// If no checks configured (test environment), return OK
	if len(checks) == 0 {
		return http.StatusOK, `{"status":"200", "dependencies":[]}`, nil
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC provides health check information via gRPC.
//
// This method exposes the readiness health check functionality through the gRPC API,
// allowing remote services to check whether this blockchain service is ready to accept
// requests. It wraps the Health method to provide a standardized gRPC response format
// used across all Teranode services.
//
// The method includes performance tracking via tracing and metrics for monitoring and
// diagnostics. It always performs a readiness check (equivalent to Health() with
// checkLiveness=false), verifying both internal service state and dependencies.
//
// Parameters:
// - ctx: Context for the operation with timeout and cancellation support
// - _: Empty request parameter (unused but required by the gRPC interface)
//
// Returns:
// - HealthResponse with current service status, human-readable details, and timestamp
// - Error if the health check fails unexpectedly (wrapped for gRPC transmission)
func (b *Blockchain) HealthGRPC(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.HealthResponse, error) {
	// Add context value to prevent circular dependency when checking gRPC server health
	ctx = context.WithValue(ctx, "skip-grpc-self-check", true)
	status, details, err := b.Health(ctx, false)

	return &blockchain_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.Now(),
	}, errors.WrapGRPC(err)
}

// Init initializes the blockchain service.
//
// This method sets up the finite state machine (FSM) that governs the service's
// operational states. It handles three initialization scenarios:
//
// 1. Test mode: Uses a predefined state for testing, bypassing normal state persistence
// 2. New deployment: Initializes a default state when no previous state exists in storage
// 3. Normal operation: Restores the previously persisted state from storage
//
// The method ensures that the service state is persisted to survive service restarts
// and updates metrics to reflect the current operational state. The FSM provides a
// consistent framework for managing the service's complex state transitions and
// recovery processes.
//
// Parameters:
// - ctx: Context for the operation with timeout and cancellation support
//
// Returns:
// - Error if initialization fails, nil on success
func (b *Blockchain) Init(ctx context.Context) error {
	b.finiteStateMachine = b.NewFiniteStateMachine()

	// check if we are in local testing mode with a defined target state for the FSM
	if b.localTestStartState != "" {
		b.finiteStateMachine.SetState(b.localTestStartState)

		err := b.store.SetFSMState(ctx, b.finiteStateMachine.Current())
		if err != nil {
			b.logger.Errorf("[Blockchain][Init] Error setting FSM state in blockchain store: %v", err)
		}

		return nil
	}

	// Set the FSM to the latest persisted state
	stateStr, err := b.store.GetFSMState(ctx)
	if err != nil {
		b.logger.Errorf("[Blockchain][Init] Error getting FSM state: %v", err)
	}

	if stateStr == "" { // if no state is stored, set the default state
		b.logger.Infof("[Blockchain][Init] Blockchain db doesn't have previous FSM state, storing FSM's default state: %v", b.finiteStateMachine.Current())

		err = b.store.SetFSMState(ctx, b.finiteStateMachine.Current())
		if err != nil {
			// TODO: just logging now, consider adding retry
			b.logger.Errorf("[Blockchain][Init] Error setting FSM state in blockchain store if the state is empty: %v", err)
		}
	} else { // if there is a state stored, set the FSM to that state
		b.logger.Infof("[Blockchain][Init] Blockchain db has previous FSM state: %v, setting FSM's current state to it.", stateStr)
		b.finiteStateMachine.SetState(stateStr)
	}

	prometheusBlockchainFSMCurrentState.Set(float64(blockchain_api.FSMStateType_value[b.finiteStateMachine.Current()]))

	return nil
}

// Start begins the blockchain service operations.
//
// This method initializes and launches all the core components of the blockchain service:
// - Starts the Kafka producer for publishing block data to other services
// - Initializes the subscription management goroutine for notifications
// - Sets up the HTTP server for administrative endpoints
// - Starts the gRPC server for client API access
//
// The method uses a synchronized approach to ensure the service is fully operational
// before signaling readiness through the provided channel. It manages clean startup
// sequence and handles initialization failures appropriately.
//
// Parameters:
// - ctx: Context for the operation with cancellation support
// - readyCh: Channel to signal when the service is fully initialized and ready
//
// Returns:
// - Error if any part of the startup sequence fails, nil on successful startup
func (b *Blockchain) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	b.startKafka()

	go b.startSubscriptions()

	// Start batch token cleanup
	go b.cleanupExpiredBatchTokens()

	if err := b.startHTTP(ctx); err != nil {
		return errors.WrapGRPC(err)
	}

	// this will block
	if err := util.StartGRPCServer(ctx, b.logger, b.settings, "blockchain", b.settings.BlockChain.GRPCListenAddress, func(server *grpc.Server) {
		blockchain_api.RegisterBlockchainAPIServer(server, b)
		closeOnce.Do(func() { close(readyCh) })
	}, nil); err != nil {
		return errors.WrapGRPC(errors.NewServiceNotStartedError("[Blockchain][Start] can't start GRPC server", err))
	}

	return nil
}

// startHTTP initializes and starts the HTTP server for the blockchain service.
//
// This method sets up an HTTP server with administrative endpoints for blockchain operations
// such as invalidating and revalidating blocks. It configures essential middleware for:
// - Error recovery to prevent crashes from HTTP request handling
// - CORS configuration for cross-origin requests
//
// The server is launched in a non-blocking manner with two goroutines: one to monitor
// for context cancellation to enable graceful shutdown, and another to run the actual
// HTTP server. This design ensures the service remains responsive and can be cleanly
// terminated when needed.
//
// Parameters:
// - ctx: Context for the operation with cancellation support for clean shutdown
//
// Returns:
// - Configuration error if HTTP listen address is not specified
// - Nil on successful server initialization (actual serving happens in background)
func (b *Blockchain) startHTTP(ctx context.Context) error {
	httpAddress := b.settings.BlockChain.HTTPListenAddress
	if httpAddress == "" {
		return errors.NewConfigurationError("[Miner] No blockchain_httpListenAddress specified")
	}

	// Get listener using util.GetListener
	listener, address, _, err := util.GetListener(b.settings.Context, "blockchain", "http://", httpAddress)
	if err != nil {
		return errors.NewServiceError("[Blockchain] failed to get HTTP listener", err)
	}

	b.logger.Infof("[Blockchain] HTTP server listening on %s", address)

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.Recover())

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
	}))

	// Add health endpoint for HTTP health checks
	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	})

	e.GET("/invalidate/:hash", b.invalidateHandler)
	e.GET("/revalidate/:hash", b.revalidateHandler)

	go func() {
		<-ctx.Done()

		err := e.Shutdown(context.Background())
		if err != nil {
			b.logger.Errorf("[Blockchain] %s (http) service shutdown error: %s", err)
		}
	}()

	go func() {
		// Use the pre-created listener
		e.Listener = listener
		if err := e.Server.Serve(listener); err != nil {
			if err == http.ErrServerClosed {
				b.logger.Infof("http server shutdown")
			} else {
				b.logger.Errorf("failed to start http server: %v", err)
			}
		}
		// Clean up the listener when server stops
		util.RemoveListener(b.settings.Context, "blockchain", "http://")
	}()

	return nil
}

// invalidateHandler handles HTTP requests to invalidate a block.
//
// This method processes HTTP requests to mark a specific block as invalid in the blockchain.
// It extracts the block hash from the URL path parameter, validates it as a proper hash,
// and delegates to the InvalidateBlock gRPC method to perform the actual invalidation.
//
// The invalidation process helps maintain blockchain integrity by marking blocks that
// should be excluded from the active chain due to consensus rule violations or other issues.
//
// Parameters:
// - c: The echo HTTP context containing the request details and response writer
//
// Returns:
// - HTTP 400 (Bad Request) if the hash parameter is invalid
// - HTTP 500 (Internal Server Error) if the invalidation operation fails
// - HTTP 200 (OK) with success message if the block is successfully invalidated
func (b *Blockchain) invalidateHandler(c echo.Context) error {
	hashStr := c.Param("hash")

	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		return c.String(http.StatusBadRequest, fmt.Sprintf("invalid hash: %v", err))
	}

	_, err = b.InvalidateBlock(b.AppCtx, &blockchain_api.InvalidateBlockRequest{
		BlockHash: hash.CloneBytes(),
	})

	if err != nil {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("error invalidating block: %v", err))
	}

	return c.String(http.StatusOK, fmt.Sprintf("block invalidated: %s", hashStr))
}

// revalidateHandler handles HTTP requests to revalidate a block.
//
// This method processes HTTP requests to revalidate a previously invalidated block
// in the blockchain. It extracts the block hash from the URL path parameter, validates
// it as a proper hash, and delegates to the RevalidateBlock gRPC method to perform the
// actual revalidation.
//
// Revalidation allows blocks that were previously marked as invalid to be reconsidered,
// which is useful for recovery from false invalidations or after rule changes.
//
// Parameters:
// - c: The echo HTTP context containing the request details and response writer
//
// Returns:
// - HTTP 400 (Bad Request) if the hash parameter is invalid
// - HTTP 500 (Internal Server Error) if the revalidation operation fails
// - HTTP 200 (OK) with success message if the block is successfully revalidated
func (b *Blockchain) revalidateHandler(c echo.Context) error {
	hashStr := c.Param("hash")

	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		return c.String(http.StatusBadRequest, fmt.Sprintf("invalid hash: %v", err))
	}

	_, err = b.RevalidateBlock(b.AppCtx, &blockchain_api.RevalidateBlockRequest{
		BlockHash: hash.CloneBytes(),
	})

	if err != nil {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("error revalidating block: %v", err))
	}

	return c.String(http.StatusOK, fmt.Sprintf("block revalidated: %s", hashStr))
}

// startKafka initializes and starts the Kafka producer.
//
// This method sets up the asynchronous Kafka messaging infrastructure used for publishing
// finalized blocks to downstream services. It creates a buffered channel for message
// queuing and activates the Kafka producer which handles the actual publishing.
//
// The Kafka producer is a critical component that enables the blockchain service to
// communicate with other services in the Teranode system in a decoupled manner.
// It allows for reliable, asynchronous propagation of block data while maintaining
// performance and resilience.
//
// Note: This method should be called during service startup before any blocks are processed.
func (b *Blockchain) startKafka() {
	b.logger.Infof("[Blockchain][startKafka] Starting Kafka producer for blocks")
	b.kafkaChan = make(chan *kafka.Message, 100)

	b.blocksFinalKafkaAsyncProducer.Start(b.AppCtx, b.kafkaChan)
}

// startSubscriptions manages blockchain subscriptions in a goroutine.
//
// This method handles all subscription management including:
// - Adding new subscribers to the notification system
// - Removing dead or disconnected subscribers
// - Broadcasting notifications to all active subscribers
// - Processing and forwarding events to interested clients
//
// The method runs in an infinite loop until the application context is cancelled,
// at which point it cleans up all subscriptions and terminates. It is designed
// to handle high-throughput notification scenarios with concurrent delivery to
// multiple subscribers.
//
// Each notification is forwarded asynchronously to prevent slow subscribers from
// impacting overall system performance, with automatic cleanup of failed connections.
//
// Note: This method must be started as a goroutine unless running in a test environment.
func (b *Blockchain) startSubscriptions() {
	// Signal that subscription manager is now ready to handle subscriptions
	b.subscriptionManagerReady.Store(true)
	b.logger.Infof("[Blockchain][startSubscriptions] Subscription manager is now ready")
	for {
		select {
		case <-b.AppCtx.Done():
			b.logger.Infof("[Blockchain][startSubscriptions] Stopping channel listeners go routine")

			for sub := range b.subscribers {
				safeClose(sub.done)
			}

			return
		case notification := <-b.notifications:
			start := gocore.CurrentTime()

			func() {
				b.logger.Debugf("[Blockchain Server] Sending notification: %s", notification)

				for sub := range b.subscribers {
					b.logger.Debugf("[Blockchain][startSubscriptions] Sending notification to %s in background: %s", sub.source, notification.Stringify())

					go func(s subscriber) {
						b.logger.Debugf("[Blockchain][startSubscriptions] Sending notification to %s: %s", s.source, notification.Stringify())

						if err := s.subscription.Send(notification); err != nil {
							b.deadSubscriptions <- s
						}
					}(sub)
				}
			}()
			b.stats.NewStat("channel-subscription.Send", true).AddTime(start)

		case s := <-b.newSubscriptions:
			b.subscribersMu.Lock()
			b.subscribers[s] = true
			b.subscribersMu.Unlock()

			// Send initial notification to let the subscriber know the subscription is ready
			// and provide the current blockchain state
			go func(sub subscriber) {
				chainTip, _, err := b.store.GetBestBlockHeader(context.Background())
				var initialNotification *blockchain_api.Notification
				if err != nil {
					// If no best block exists yet (e.g., empty blockchain), send notification with genesis hash
					b.logger.Warnf("[Blockchain][startSubscriptions] No best block header available for initial notification to %s: %v", sub.source, err)
					initialNotification = &blockchain_api.Notification{
						Type: model.NotificationType_Block,
						Hash: b.settings.ChainCfgParams.GenesisHash.CloneBytes(),
					}
				} else {
					initialNotification = &blockchain_api.Notification{
						Type: model.NotificationType_Block,
						Hash: chainTip.Hash().CloneBytes(),
					}
				}

				b.logger.Infof("[Blockchain][startSubscriptions] Sending initial notification to %s", sub.source)
				if err := sub.subscription.Send(initialNotification); err != nil {
					b.logger.Errorf("[Blockchain][startSubscriptions] Failed to send initial notification to %s: %v", sub.source, err)
					b.deadSubscriptions <- sub
				}
			}(s)

		case s := <-b.deadSubscriptions:
			delete(b.subscribers, s)
			safeClose(s.done)
			b.logger.Infof("[Blockchain][startSubscriptions] Subscription removed (Total=%d).", len(b.subscribers))
		}
	}
}

// Stop gracefully stops the blockchain service.
//
// This method handles the graceful shutdown of the blockchain service, allowing
// for proper resource cleanup and state persistence before termination.
//
// While the current implementation is minimal, this method provides an extension
// point for adding proper shutdown logic such as:
// - Persisting any in-memory state
// - Gracefully closing open connections and channels
// - Ensuring ongoing operations complete safely
// - Releasing acquired resources
//
// Parameters:
// - _: Context for the shutdown operation (currently unused)
//
// Returns:
// - Error if shutdown encounters issues, nil on successful shutdown
func (b *Blockchain) Stop(_ context.Context) error {
	return nil
}

// AddBlock processes a request to add a new block to the blockchain.
//
// This method is one of the core operations of the blockchain service and handles the full
// lifecycle of adding a new block to the blockchain:
// - Validates and parses the incoming block data (header, coinbase transaction, subtree hashes)
// - Persists the validated block to the blockchain store
// - Updates block metadata such as height information
// - Publishes the finalized block to Kafka for downstream services
// - Notifies subscribers about the new block
//
// The method includes performance tracking via tracing and metrics, with detailed logging
// at key points in the process. Error conditions are carefully handled with appropriate
// GRPC error wrapping to ensure consistent error reporting across the system.
//
// Parameters:
// - ctx: Context for the operation with timeout and cancellation support
// - request: The AddBlockRequest containing block data and metadata
//
// Returns:
// - Empty response on success
// - Error if the block addition fails (wrapped for GRPC transmission)
func (b *Blockchain) AddBlock(ctx context.Context, request *blockchain_api.AddBlockRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "AddBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainAddBlock),
		tracing.WithDebugLogMessage(b.logger, "[AddBlock] called from %s", request.PeerId),
	)
	defer deferFn()

	header, err := model.NewBlockHeaderFromBytes(request.Header)
	if err != nil {
		return nil, err
	}

	b.logger.Infof("[Blockchain][AddBlock] AddBlock called: %s", header.Hash().String())

	btCoinbaseTx, err := bt.NewTxFromBytes(request.CoinbaseTx)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][AddBlock] can't create the coinbase transaction", err))
	}

	subtreeHashes := make([]*chainhash.Hash, len(request.SubtreeHashes))
	for i, subtreeHash := range request.SubtreeHashes {
		subtreeHashes[i], err = chainhash.NewHash(subtreeHash)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][AddBlock] unable to create subtree hash", err))
		}

		if subtreeHashes[i].Equal(chainhash.Hash{}) {
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][AddBlock] unexpected empty subtree hash %d of %d", i, len(request.SubtreeHashes)))
		}
	}

	block := &model.Block{
		Header:           header,
		CoinbaseTx:       btCoinbaseTx,
		Subtrees:         subtreeHashes,
		TransactionCount: request.TransactionCount,
		SizeInBytes:      request.SizeInBytes,
	}

	// process options for storing
	storeBlockOptions := make([]blockchainoptions.StoreBlockOption, 0, 3)

	if request.OptionMinedSet {
		storeBlockOptions = append(storeBlockOptions, blockchainoptions.WithMinedSet(request.OptionMinedSet))
	}
	if request.OptionSubtreesSet {
		storeBlockOptions = append(storeBlockOptions, blockchainoptions.WithSubtreesSet(request.OptionSubtreesSet))
	}
	if request.OptionInvalid {
		storeBlockOptions = append(storeBlockOptions, blockchainoptions.WithInvalid(request.OptionInvalid))
	}
	if request.OptionID != 0 {
		storeBlockOptions = append(storeBlockOptions, blockchainoptions.WithID(request.OptionID))
	}

	ID, height, err := b.store.StoreBlock(ctx, block, request.PeerId, storeBlockOptions...)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	// Clear difficulty cache when chain state changes to prevent stale cached values
	// from causing incorrect difficulty calculations during rapid block processing
	b.difficulty.ResetCache()

	b.logger.Infof("[AddBlock] stored block %s (ID: %d, height: %d)", block.Hash(), ID, height)

	block.Height = height

	b.logger.Debugf("[AddBlock] checking for Kafka producer: %v", b.blocksFinalKafkaAsyncProducer != nil)

	// Only publish to Kafka if the block is valid. Invalid blocks (marked with OptionInvalid)
	// should not be propagated to downstream consumers via the blocks_final topic.
	if !request.OptionInvalid {
		if err = b.sendKafkaBlockFinalNotification(block); err != nil {
			b.logger.Errorf("[AddBlock] error sending Kafka notification for new block %s: %v", block.Hash(), err)
		}
	}

	if _, err = b.SendNotification(ctx, &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: block.Hash().CloneBytes(),
	}); err != nil {
		b.logger.Errorf("[AddBlock] error sending notification for new block %s: %v", block.Hash(), err)
	}

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) sendKafkaBlockFinalNotification(block *model.Block) error {
	if b.blocksFinalKafkaAsyncProducer != nil {
		key := block.Header.Hash().String()

		subtreeHashes := make([][]byte, len(block.Subtrees))
		for i, subtreeHash := range block.Subtrees {
			subtreeHashes[i] = subtreeHash.CloneBytes()
		}

		message := &kafkamessage.KafkaBlocksFinalTopicMessage{
			Header:           block.Header.Bytes(),
			TransactionCount: block.TransactionCount,
			SizeInBytes:      block.SizeInBytes,
			SubtreeHashes:    subtreeHashes,
			CoinbaseTx:       block.CoinbaseTx.Bytes(),
			Height:           block.Height,
		}

		value, err := proto.Marshal(message)
		if err != nil {
			b.logger.Errorf("[AddBlock] error creating block bytes: %v", err)
			return err
		}

		if len(value) >= 500_000 { // kafka default limit is actually 1MB and we don't ever expecta block to be even close to that
			b.logger.Warnf("[AddBlock] blocks-final message size %d bytes maybe too large for Kafka, block hash: %s (height: %d)", len(value), block.Header.Hash(), block.Height)
		}

		b.kafkaChan <- &kafka.Message{
			Key:   []byte(key),
			Value: value,
		}
	}

	return nil
}

// GetBlock retrieves a block by its hash.
//
// This method fetches a complete Bitcoin block from the blockchain store based on its hash.
// It performs the following operations:
// - Validates the requested block hash format
// - Retrieves the block data from the blockchain store
// - Converts internal block representation to the API response format
// - Includes all block components: header, coinbase transaction, and subtree hashes
//
// The method includes performance tracking via tracing and metrics for monitoring
// and diagnostics. Error handling includes specific error types for various failure
// scenarios, such as invalid hash format or block not found conditions.
//
// Parameters:
// - ctx: Context for the operation with timeout and cancellation support
// - request: Request containing the hash of the block to retrieve
//
// Returns:
// - Complete block data in API response format on success
// - Error if block retrieval fails (wrapped for GRPC transmission)
func (b *Blockchain) GetBlock(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlock),
		tracing.WithDebugLogMessage(b.logger, "[GetBlock] called for %s", utils.ReverseAndHexEncodeSlice(request.Hash)),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlock] request's hash is not valid", err))
	}

	block, height, err := b.store.GetBlock(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	subtreeHashes := make([][]byte, len(block.Subtrees))
	for i, subtreeHash := range block.Subtrees {
		subtreeHashes[i] = subtreeHash[:]
	}

	var coinbaseBytes []byte
	if block.CoinbaseTx != nil {
		coinbaseBytes = block.CoinbaseTx.Bytes()
	}

	return &blockchain_api.GetBlockResponse{
		Header:           block.Header.Bytes(),
		Height:           height,
		CoinbaseTx:       coinbaseBytes,
		SubtreeHashes:    subtreeHashes,
		TransactionCount: block.TransactionCount,
		SizeInBytes:      block.SizeInBytes,
		Id:               block.ID,
	}, nil
}

// GetBlocks retrieves multiple blocks starting from a specific hash.
func (b *Blockchain) GetBlocks(ctx context.Context, req *blockchain_api.GetBlocksRequest) (*blockchain_api.GetBlocksResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlocks",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeaders),
		tracing.WithLogMessage(b.logger, "[GetBlocks] called for %s", utils.ReverseAndHexEncodeSlice(req.Hash)),
	)
	defer deferFn()

	startHash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlocks] request's hash is not valid", err))
	}

	blocks, err := b.store.GetBlocks(ctx, startHash, req.Count)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blocks))

	for i, block := range blocks {
		blockBytes, err := block.Bytes()
		if err != nil {
			return nil, errors.WrapGRPC(err)
		}

		blockHeaderBytes[i] = blockBytes
	}

	return &blockchain_api.GetBlocksResponse{
		Blocks: blockHeaderBytes,
	}, nil
}

// GetBlockByHeight retrieves a block at a specific height in the blockchain.
func (b *Blockchain) GetBlockByHeight(ctx context.Context, request *blockchain_api.GetBlockByHeightRequest) (*blockchain_api.GetBlockResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockByHeight",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlock),
		tracing.WithLogMessage(b.logger, "[GetBlockByHeight] called for %d", request.Height),
	)
	defer deferFn()

	block, err := b.store.GetBlockByHeight(ctx, request.Height)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain] block not found at height", err))
	}

	subtreeHashes := make([][]byte, len(block.Subtrees))
	for i, subtreeHash := range block.Subtrees {
		subtreeHashes[i] = subtreeHash[:]
	}

	var coinbaseBytes []byte
	if block.CoinbaseTx != nil {
		coinbaseBytes = block.CoinbaseTx.Bytes()
	}

	return &blockchain_api.GetBlockResponse{
		Header:           block.Header.Bytes(),
		Height:           request.Height,
		CoinbaseTx:       coinbaseBytes,
		SubtreeHashes:    subtreeHashes,
		TransactionCount: block.TransactionCount,
		SizeInBytes:      block.SizeInBytes,
		Id:               block.ID,
	}, nil
}

// GetBlockByID retrieves a block by its ID.
func (b *Blockchain) GetBlockByID(ctx context.Context, request *blockchain_api.GetBlockByIDRequest) (*blockchain_api.GetBlockResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockByHeight",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlock),
		tracing.WithLogMessage(b.logger, "[GetBlockByID] called for %d", request.Id),
	)
	defer deferFn()

	block, err := b.store.GetBlockByID(ctx, request.Id)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	subtreeHashes := make([][]byte, len(block.Subtrees))
	for i, subtreeHash := range block.Subtrees {
		subtreeHashes[i] = subtreeHash[:]
	}

	var coinbaseBytes []byte
	if block.CoinbaseTx != nil {
		coinbaseBytes = block.CoinbaseTx.Bytes()
	}

	return &blockchain_api.GetBlockResponse{
		Header:           block.Header.Bytes(),
		Height:           block.Height,
		CoinbaseTx:       coinbaseBytes,
		SubtreeHashes:    subtreeHashes,
		TransactionCount: block.TransactionCount,
		SizeInBytes:      block.SizeInBytes,
		Id:               block.ID,
	}, nil
}

// GetNextBlockID retrieves the next available block ID.
func (b *Blockchain) GetNextBlockID(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetNextBlockIDResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetNextBlockID",
		tracing.WithParentStat(b.stats),
		tracing.WithLogMessage(b.logger, "[GetNextBlockID] called"),
	)
	defer deferFn()

	nextID, err := b.store.GetNextBlockID(ctx)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetNextBlockIDResponse{
		NextBlockId: nextID,
	}, nil
}

// GetBlockStats retrieves statistical information about the blockchain.
func (b *Blockchain) GetBlockStats(ctx context.Context, _ *emptypb.Empty) (*model.BlockStats, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockStats",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockStats),
	)
	defer deferFn()

	resp, err := b.store.GetBlockStats(ctx)

	return resp, errors.WrapGRPC(err)
}

// GetBlockGraphData retrieves data points for blockchain visualization.
func (b *Blockchain) GetBlockGraphData(ctx context.Context, req *blockchain_api.GetBlockGraphDataRequest) (*model.BlockDataPoints, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockGraphData",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockGraphData),
	)
	defer deferFn()

	resp, err := b.store.GetBlockGraphData(ctx, req.PeriodMillis)

	return resp, errors.WrapGRPC(err)
}

// GetLastNBlocks retrieves the most recent N blocks from the blockchain.
func (b *Blockchain) GetLastNBlocks(ctx context.Context, request *blockchain_api.GetLastNBlocksRequest) (*blockchain_api.GetLastNBlocksResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetLastNBlocks",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetLastNBlocks),
	)
	defer deferFn()

	blockInfo, err := b.store.GetLastNBlocks(ctx, request.NumberOfBlocks, request.IncludeOrphans, request.FromHeight)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetLastNBlocksResponse{
		Blocks: blockInfo,
	}, nil
}

// GetLastNInvalidBlocks retrieves the most recent N blocks that have been marked as invalid.
func (b *Blockchain) GetLastNInvalidBlocks(ctx context.Context, request *blockchain_api.GetLastNInvalidBlocksRequest) (*blockchain_api.GetLastNInvalidBlocksResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetLastNInvalidBlocks",
		tracing.WithParentStat(b.stats),
	)
	defer deferFn()

	blockInfo, err := b.store.GetLastNInvalidBlocks(ctx, request.N)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetLastNInvalidBlocksResponse{
		Blocks: blockInfo,
	}, nil
}

// GetSuitableBlock finds a suitable block for mining purposes.
func (b *Blockchain) GetSuitableBlock(ctx context.Context, request *blockchain_api.GetSuitableBlockRequest) (*blockchain_api.GetSuitableBlockResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetSuitableBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetSuitableBlock),
	)
	defer deferFn()

	blockInfo, err := b.store.GetSuitableBlock(ctx, (*chainhash.Hash)(request.Hash))
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetSuitableBlockResponse{
		Block: blockInfo,
	}, nil
}

// GetNextWorkRequired calculates the required proof of work for the next block.
func (b *Blockchain) GetNextWorkRequired(ctx context.Context, request *blockchain_api.GetNextWorkRequiredRequest) (*blockchain_api.GetNextWorkRequiredResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetNextWorkRequired",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetNextWorkRequired),
	)
	defer deferFn()

	if request.CurrentBlockTime == 0 {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][GetNextWorkRequired] request's current block time is not valid", nil))
	}

	bytesLittleEndian := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesLittleEndian, b.settings.ChainCfgParams.PowLimitBits)

	hash, err := chainhash.NewHash(request.PreviousBlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetNextWorkRequired] request's block hash is not valid", err))
	}

	blockHeader, meta, err := b.store.GetBlockHeader(ctx, hash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	nBits, err := b.difficulty.CalcNextWorkRequired(ctx, blockHeader, meta.Height, request.CurrentBlockTime)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	b.logger.Debugf("difficulty adjustment. Difficulty set to %s", nBits.String())

	return &blockchain_api.GetNextWorkRequiredResponse{
		Bits: nBits.CloneBytes(),
	}, nil
}

// GetHashOfAncestorBlock retrieves the hash of an ancestor block at a specific depth.
func (b *Blockchain) GetHashOfAncestorBlock(ctx context.Context, request *blockchain_api.GetHashOfAncestorBlockRequest) (*blockchain_api.GetHashOfAncestorBlockResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetHashOfAncestorBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetHashOfAncestorBlock),
	)
	defer deferFn()

	hash, err := b.store.GetHashOfAncestorBlock(ctx, (*chainhash.Hash)(request.Hash), int(request.Depth))
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetHashOfAncestorBlockResponse{
		Hash: hash[:],
	}, nil
}

func (b *Blockchain) GetLatestBlockHeaderFromBlockLocatorRequest(ctx context.Context, request *blockchain_api.GetLatestBlockHeaderFromBlockLocatorRequest) (*blockchain_api.GetBlockHeaderResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetLatestBlockHeaderFromBlockLocatorRequest",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetLatestBlockHeaderFromBlockLocator),
	)
	defer deferFn()

	bestBlockHash, err := chainhash.NewHash(request.BestBlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][GetLatestBlockHeaderFromBlockLocatorRequest] request's best block hash is not valid", err))
	}

	locatorHashes := make([]chainhash.Hash, len(request.BlockLocatorHashes))
	for i, hashBytes := range request.BlockLocatorHashes {
		hash, err := chainhash.NewHash(hashBytes)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][GetLatestBlockHeaderFromBlockLocatorRequest] request's block locator hash is not valid", err))
		}

		locatorHashes[i] = *hash
	}

	blockHeader, meta, err := b.store.GetLatestBlockHeaderFromBlockLocator(ctx, bestBlockHash, locatorHashes)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetBlockHeaderResponse{
		BlockHeader: blockHeader.Bytes(),
		Height:      meta.Height,
		TxCount:     meta.TxCount,
		SizeInBytes: meta.SizeInBytes,
		Miner:       meta.Miner,
		ChainWork:   meta.ChainWork,
		BlockTime:   meta.BlockTime,
		Timestamp:   meta.Timestamp,
	}, nil
}

func (b *Blockchain) GetBlockHeadersFromOldestRequest(ctx context.Context, request *blockchain_api.GetBlockHeadersFromOldestRequest) (*blockchain_api.GetBlockHeadersResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockHeadersFromOldestRequest",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeadersFromOldest),
	)
	defer deferFn()

	chainTipHash, err := chainhash.NewHash(request.ChainTipHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][GetBlockHeadersFromOldestRequest] request's start hash is not valid", err))
	}

	targetHash, err := chainhash.NewHash(request.TargetHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][GetBlockHeadersFromOldestRequest] request's target hash is not valid", err))
	}

	blockHeaders, blockHeaderMetas, err := b.store.GetBlockHeadersFromOldest(ctx, chainTipHash, targetHash, request.GetNumberOfHeaders())
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))

	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	blockHeaderMetaBytes := make([][]byte, len(blockHeaderMetas))
	for i, meta := range blockHeaderMetas {
		blockHeaderMetaBytes[i] = meta.Bytes()
	}

	return &blockchain_api.GetBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
		Metas:        blockHeaderMetaBytes,
	}, nil
}

// GetBlockExists checks if a block with the given hash exists in the blockchain.
func (b *Blockchain) GetBlockExists(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockExistsResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockExists",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockExists),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockExists] request's hash is not valid", err))
	}

	exists, err := b.store.GetBlockExists(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetBlockExistsResponse{
		Exists: exists,
	}, nil
}

// GetBestBlockHeader retrieves the header of the current best block.
func (b *Blockchain) GetBestBlockHeader(ctx context.Context, empty *emptypb.Empty) (*blockchain_api.GetBlockHeaderResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBestBlockHeader",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBestBlockHeader),
	)
	defer deferFn()

	chainTip, meta, err := b.store.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetBlockHeaderResponse{
		BlockHeader: chainTip.Bytes(),
		Height:      meta.Height,
		TxCount:     meta.TxCount,
		SizeInBytes: meta.SizeInBytes,
		Miner:       meta.Miner,
		BlockTime:   meta.BlockTime,
		Timestamp:   meta.Timestamp,
		ChainWork:   meta.ChainWork,
	}, nil
}

// CheckBlockIsInCurrentChain verifies if a block is part of the current main chain.
func (b *Blockchain) CheckBlockIsInCurrentChain(ctx context.Context, req *blockchain_api.CheckBlockIsCurrentChainRequest) (*blockchain_api.CheckBlockIsCurrentChainResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "CheckBlockIsInCurrentChain",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainCheckBlockIsInCurrentChain),
	)
	defer deferFn()

	result, err := b.store.CheckBlockIsInCurrentChain(ctx, req.BlockIDs)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.CheckBlockIsCurrentChainResponse{
		IsPartOfCurrentChain: result,
	}, nil
}

// CheckBlockIsAncestorOfBlock verifies if any of the given block IDs are ancestors of a specific block.
// This is used for double-spend detection on fork blocks where we check against the fork's ancestor chain.
func (b *Blockchain) CheckBlockIsAncestorOfBlock(ctx context.Context, req *blockchain_api.CheckBlockIsAncestorOfBlockRequest) (*blockchain_api.CheckBlockIsAncestorOfBlockResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "CheckBlockIsAncestorOfBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainCheckBlockIsAncestorOfBlock),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(req.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][CheckBlockIsAncestorOfBlock] request's hash is not valid", err))
	}

	result, err := b.store.CheckBlockIsAncestorOfBlock(ctx, req.BlockIDs, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.CheckBlockIsAncestorOfBlockResponse{
		IsAncestor: result,
	}, nil
}

// GetChainTips retrieves information about all known tips in the block tree.
func (b *Blockchain) GetChainTips(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetChainTipsResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetChainTips",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetChainTips),
	)
	defer deferFn()

	chainTips, err := b.store.GetChainTips(ctx)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetChainTipsResponse{
		Tips: chainTips,
	}, nil
}

// GetBlockHeader retrieves the header of a specific block.
func (b *Blockchain) GetBlockHeader(ctx context.Context, req *blockchain_api.GetBlockHeaderRequest) (*blockchain_api.GetBlockHeaderResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBestBlockHeader",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeader),
	)
	defer deferFn()

	hash, err := chainhash.NewHash(req.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockHeader] request's hash is not valid", err))
	}

	blockHeader, meta, err := b.store.GetBlockHeader(ctx, hash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	var processedAt *timestamppb.Timestamp
	if meta.ProcessedAt != nil {
		processedAt = timestamppb.New(*meta.ProcessedAt)
	}

	return &blockchain_api.GetBlockHeaderResponse{
		BlockHeader: blockHeader.Bytes(),
		Id:          meta.ID,
		Height:      meta.Height,
		TxCount:     meta.TxCount,
		SizeInBytes: meta.SizeInBytes,
		Miner:       meta.Miner,
		PeerId:      meta.PeerID,
		BlockTime:   meta.BlockTime,
		Timestamp:   meta.Timestamp,
		MinedSet:    meta.MinedSet,
		ChainWork:   meta.ChainWork,
		SubtreesSet: meta.SubtreesSet,
		Invalid:     meta.Invalid,
		ProcessedAt: processedAt,
	}, nil
}

// GetBlockHeaders retrieves multiple block headers starting from a specific hash.
func (b *Blockchain) GetBlockHeaders(ctx context.Context, req *blockchain_api.GetBlockHeadersRequest) (*blockchain_api.GetBlockHeadersResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockHeaders",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeaders),
	)
	defer deferFn()

	startHash, err := chainhash.NewHash(req.StartHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockHeaders] request's hash is not valid", err))
	}

	blockHeaders, blockHeaderMetas, err := b.store.GetBlockHeaders(ctx, startHash, req.NumberOfHeaders)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	blockHeaderMetaBytes := make([][]byte, len(blockHeaders))
	for i, meta := range blockHeaderMetas {
		blockHeaderMetaBytes[i] = meta.Bytes()
	}

	return &blockchain_api.GetBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
		Metas:        blockHeaderMetaBytes,
	}, nil
}

func (b *Blockchain) GetBlockHeadersToCommonAncestor(ctx context.Context, req *blockchain_api.GetBlockHeadersToCommonAncestorRequest) (*blockchain_api.GetBlockHeadersResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockHeaders",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeaders),
	)
	defer deferFn()

	var err error

	targetHash, err := chainhash.NewHash(req.TargetHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockHeadersToCommonAncestor] request's hash is not valid", err))
	}

	blockLocatorHashes := make([]*chainhash.Hash, len(req.BlockLocatorHashes))

	for i, hash := range req.BlockLocatorHashes {
		blockLocatorHashes[i], err = chainhash.NewHash(hash)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockHeadersToCommonAncestor] request's hash is not valid", err))
		}
	}

	blockHeaders, blockHeaderMetas, err := getBlockHeadersToCommonAncestor(ctx, b.store, targetHash, blockLocatorHashes, req.MaxHeaders)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	blockHeaderMetaBytes := make([][]byte, len(blockHeaders))
	for i, meta := range blockHeaderMetas {
		blockHeaderMetaBytes[i] = meta.Bytes()
	}

	return &blockchain_api.GetBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
		Metas:        blockHeaderMetaBytes,
	}, nil
}

// GetBlockHeadersFromTill retrieves block headers between two specified blocks.
func (b *Blockchain) GetBlockHeadersFromTill(ctx context.Context, req *blockchain_api.GetBlockHeadersFromTillRequest) (*blockchain_api.GetBlockHeadersResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockHeadersFromTill",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeaders),
	)
	defer deferFn()

	startHash, err := chainhash.NewHash(req.StartHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockHeadersFromTill] request's start hash is not valid", err))
	}

	endHash, err := chainhash.NewHash(req.EndHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockHeadersFromTill] request's end hash is not valid", err))
	}

	blockHeaders, blockHeaderMetas, err := b.store.GetBlockHeadersFromTill(ctx, startHash, endHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	blockHeaderMetaBytes := make([][]byte, len(blockHeaders))
	for i, meta := range blockHeaderMetas {
		blockHeaderMetaBytes[i] = meta.Bytes()
	}

	return &blockchain_api.GetBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
		Metas:        blockHeaderMetaBytes,
	}, nil
}

// GetBlockHeadersFromHeight retrieves block headers starting from a specific height.
func (b *Blockchain) GetBlockHeadersFromHeight(ctx context.Context, req *blockchain_api.GetBlockHeadersFromHeightRequest) (*blockchain_api.GetBlockHeadersFromHeightResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockHeadersFromHeight",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeadersFromHeight),
	)
	defer deferFn()

	blockHeaders, metas, err := b.store.GetBlockHeadersFromHeight(ctx, req.StartHeight, req.Limit)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	metasBytes := make([][]byte, len(metas))
	for i, meta := range metas {
		metasBytes[i] = meta.Bytes()
	}

	return &blockchain_api.GetBlockHeadersFromHeightResponse{
		BlockHeaders: blockHeaderBytes,
		Metas:        metasBytes,
	}, nil
}

// GetBlockHeadersByHeight retrieves block headers between two specified heights.
func (b *Blockchain) GetBlockHeadersByHeight(ctx context.Context, req *blockchain_api.GetBlockHeadersByHeightRequest) (*blockchain_api.GetBlockHeadersByHeightResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockHeadersByHeight",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeadersByHeight),
	)
	defer deferFn()

	blockHeaders, metas, err := b.store.GetBlockHeadersByHeight(ctx, req.StartHeight, req.EndHeight)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	metasBytes := make([][]byte, len(metas))
	for i, meta := range metas {
		metasBytes[i] = meta.Bytes()
	}

	return &blockchain_api.GetBlockHeadersByHeightResponse{
		BlockHeaders: blockHeaderBytes,
		Metas:        metasBytes,
	}, nil
}

// GetBlocksByHeight retrieves full blocks within a specified height range.
// This method implements the gRPC service endpoint for fetching complete blocks
// between two heights in a single efficient operation. It delegates to the
// blockchain store's GetBlocksByHeight method and serializes the results
// for gRPC transmission.
//
// Parameters:
//   - ctx: Request context for timeout and cancellation
//   - req: GetBlocksByHeightRequest containing startHeight and endHeight
//
// Returns:
//   - GetBlocksByHeightResponse containing serialized full blocks
//   - error: Any error encountered during block retrieval
func (b *Blockchain) GetBlocksByHeight(ctx context.Context, req *blockchain_api.GetBlocksByHeightRequest) (*blockchain_api.GetBlocksByHeightResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlocksByHeight",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlocksByHeight),
	)
	defer deferFn()

	blocks, err := b.store.GetBlocksByHeight(ctx, req.StartHeight, req.EndHeight)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockBytes := make([][]byte, len(blocks))
	for i, block := range blocks {
		blockBytes[i], err = block.Bytes()
		if err != nil {
			return nil, errors.WrapGRPC(err)
		}
	}

	return &blockchain_api.GetBlocksByHeightResponse{
		Blocks: blockBytes,
	}, nil
}

func (b *Blockchain) FindBlocksContainingSubtree(ctx context.Context, req *blockchain_api.FindBlocksContainingSubtreeRequest) (*blockchain_api.FindBlocksContainingSubtreeResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "FindBlocksContainingSubtree",
		tracing.WithParentStat(b.stats),
	)
	defer deferFn()

	subtreeHash, err := chainhash.NewHash(req.SubtreeHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("invalid subtree hash"))
	}

	blocks, err := b.store.FindBlocksContainingSubtree(ctx, subtreeHash, req.MaxBlocks)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockBytes := make([][]byte, len(blocks))
	for i, block := range blocks {
		blockBytes[i], err = block.Bytes()
		if err != nil {
			return nil, errors.WrapGRPC(err)
		}
	}

	return &blockchain_api.FindBlocksContainingSubtreeResponse{
		Blocks: blockBytes,
	}, nil
}

// Subscribe handles subscription requests to blockchain notifications.
// This method establishes a persistent gRPC streaming connection that allows
// clients to receive real-time notifications about blockchain events. It serves
// as the primary mechanism for event-driven communication between the blockchain
// service and its consumers.
//
// The subscription system is essential for:
// - Real-time notification of new blocks added to the blockchain
// - Broadcasting blockchain reorganizations and invalidations
// - Distributing FSM state changes to monitoring and management systems
// - Coordinating mining operations and block processing workflows
// - Supporting event-driven architectures across Teranode components
//
// The method creates a long-lived streaming connection that remains active
// until one of the following occurs:
// - The client context is cancelled or times out
// - The client disconnects from the gRPC stream
// - The blockchain service is shutting down
// - An unrecoverable error occurs in the notification system
//
// Each subscriber is registered with the internal notification system and
// will receive all applicable notifications through the streaming connection.
// The subscription is managed asynchronously to ensure high throughput and
// prevent slow subscribers from impacting overall system performance.
//
// The source parameter from the request is used for:
// - Logging and debugging subscription lifecycle events
// - Identifying subscribers in monitoring and metrics systems
// - Troubleshooting notification delivery issues
//
// This method implements the gRPC server streaming pattern and blocks until
// the subscription ends, making it suitable for long-running connections.
//
// Parameters:
//   - req: SubscribeRequest containing the source identifier and subscription parameters
//   - sub: gRPC server stream for sending notifications to the client
//
// Returns:
//   - error: Any error encountered during subscription management or stream handling
func (b *Blockchain) Subscribe(req *blockchain_api.SubscribeRequest, sub blockchain_api.BlockchainAPI_SubscribeServer) error {
	b.logger.Infof("[Blockchain] Subscribe called from source: %s", req.Source)
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(sub.Context(), "Subscribe",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainSubscribe),
		tracing.WithDebugLogMessage(b.logger, "[Subscribe] called"),
	)
	defer deferFn()

	// Keep this subscription alive without endless loop - use a channel that blocks forever.
	ch := make(chan struct{})

	b.logger.Infof("[Blockchain] Sending new subscription to handler for source: %s", req.Source)
	b.newSubscriptions <- subscriber{
		subscription: sub,
		done:         ch,
		source:       req.Source,
	}

	b.subscribersMu.RLock()
	noOfSubscribers := len(b.subscribers)
	b.subscribersMu.RUnlock()
	b.logger.Infof("[Blockchain] New Subscription received from %s (Total=%d).", req.Source, noOfSubscribers)

	for {
		select {
		case <-ctx.Done():
			// Client disconnected.
			b.logger.Infof("[Blockchain] GRPC client disconnected: %s", req.Source)
			return nil
		case <-ch:
			// Subscription ended.
			return nil
		}
	}
}

// GetState retrieves a value from the blockchain state storage by its key.
// This method provides access to arbitrary state data stored in the blockchain
// service's persistent state storage system. The state storage is used for
// maintaining operational data, configuration values, and other persistent
// information that needs to survive service restarts.
//
// The blockchain state storage is essential for:
// - Storing service configuration and operational parameters
// - Maintaining persistent counters and metrics
// - Caching frequently accessed blockchain data
// - Storing temporary operational state during processing
// - Supporting stateful operations across service restarts
// - Coordinating state between distributed blockchain components
//
// The method performs a direct lookup in the underlying state storage using
// the provided key. Keys are treated as opaque strings, allowing flexible
// data organization and naming schemes by calling services.
//
// Common use cases include:
// - Retrieving service configuration values
// - Accessing cached blockchain metadata
// - Reading operational counters and statistics
// - Fetching temporary processing state
// - Coordinating distributed processing state
//
// The method communicates with the blockchain store via the state storage
// interface to retrieve the requested data.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - req: GetStateRequest containing the key for the data to retrieve
//
// Returns:
//   - *blockchain_api.StateResponse: Response containing the retrieved data
//   - error: Any error encountered during state retrieval
func (b *Blockchain) GetState(ctx context.Context, req *blockchain_api.GetStateRequest) (*blockchain_api.StateResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetState",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetState),
		tracing.WithDebugLogMessage(b.logger, "[GetState] called"),
	)
	defer deferFn()

	data, err := b.store.GetState(ctx, req.Key)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.StateResponse{
		Data: data,
	}, nil
}

// SetState stores a value in the blockchain state storage with the specified key.
// This method provides the ability to store arbitrary state data in the blockchain
// service's persistent state storage system. The state storage persists across
// service restarts and is used for maintaining operational data, configuration
// values, and other persistent information.
//
// The blockchain state storage is essential for:
// - Storing service configuration and operational parameters
// - Maintaining persistent counters and metrics
// - Caching frequently accessed blockchain data for performance
// - Storing temporary operational state during processing
// - Supporting stateful operations across service restarts
// - Coordinating distributed processing state
//
// The method performs a direct write to the underlying state storage using
// the provided key-value pair. Keys are treated as opaque strings, allowing
// flexible data organization and naming schemes by calling services. The data
// is stored as binary data and can represent any serialized format.
//
// Common use cases include:
// - Storing service configuration values
// - Caching blockchain metadata for performance
// - Maintaining operational counters and statistics
// - Persisting temporary processing state
// - Coordinating distributed processing state
//
// The method communicates with the blockchain store via the state storage
// interface to persist the provided data.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - req: SetStateRequest containing the key and data to store
//
// Returns:
//   - *emptypb.Empty: Empty response on successful storage
//   - error: Any error encountered during state storage
func (b *Blockchain) SetState(ctx context.Context, req *blockchain_api.SetStateRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "SetState",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainSetState),
		tracing.WithDebugLogMessage(b.logger, "[SetState] called with state %s", req.Key),
	)
	defer deferFn()

	err := b.store.SetState(ctx, req.Key, req.Data)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// GetBlockHeaderIDs retrieves block header IDs starting from a specific hash.
// This method fetches a sequence of lightweight block header identifiers (uint32 IDs)
// from the blockchain service, beginning with the block identified by the provided
// hash and continuing for the requested number of headers. These IDs provide an
// efficient way to reference blocks without transmitting full header data.
//
// Block header IDs are essential for:
// - Efficient block referencing in distributed systems
// - Lightweight blockchain synchronization operations
// - Indexing and caching systems that need compact block references
// - Performance optimization in block processing pipelines
// - Reducing network traffic in blockchain communication protocols
//
// The method returns internal blockchain store identifiers that are:
// - Unique within the blockchain service instance
// - Compact (32-bit integers) for efficient storage and transmission
// - Suitable for use in indexing and caching operations
// - Compatible with other blockchain service operations that accept IDs
//
// The returned IDs maintain the sequential order of blocks in the blockchain,
// starting from the specified hash and continuing in chain order. This makes
// them suitable for range operations and sequential processing.
//
// Common use cases include:
// - Building efficient block indexes for fast lookups
// - Implementing lightweight synchronization protocols
// - Caching block references in memory-constrained environments
// - Optimizing network communication between blockchain components
//
// The method communicates with the blockchain store to retrieve the header
// IDs from the underlying storage system.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - request: GetBlockHeadersRequest containing the starting hash and number of headers
//
// Returns:
//   - *blockchain_api.GetBlockHeaderIDsResponse: Response containing the array of header IDs
//   - error: Any error encountered during header ID retrieval
func (b *Blockchain) GetBlockHeaderIDs(ctx context.Context, request *blockchain_api.GetBlockHeadersRequest) (*blockchain_api.GetBlockHeaderIDsResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockHeaderIDs",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockHeaderIDs),
		tracing.WithDebugLogMessage(b.logger, "[GetBlockHeaderIDs] called with start hash %x", request.StartHash),
	)
	defer deferFn()

	startHash, err := chainhash.NewHash(request.StartHash)
	if err != nil {
		return nil, err
	}

	ids, err := b.store.GetBlockHeaderIDs(ctx, startHash, request.NumberOfHeaders)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetBlockHeaderIDsResponse{
		Ids: ids,
	}, nil
}

// InvalidateBlock marks a block as invalid in the blockchain.
// This method permanently marks a specific block as invalid in the blockchain
// store, effectively removing it from the valid chain and triggering any
// necessary blockchain reorganization processes. This is a critical operation
// used for handling consensus failures and blockchain integrity issues.
//
// Block invalidation is essential for:
// - Handling consensus rule violations discovered after block acceptance
// - Responding to network-wide block rejections and reorganizations
// - Managing blockchain forks and chain selection decisions
// - Correcting blocks that fail post-acceptance validation checks
// - Supporting manual intervention in blockchain integrity issues
//
// When a block is invalidated, the following occurs:
// - The block is marked as invalid in the blockchain store
// - Any dependent blocks may also be invalidated (chain reorganization)
// - The blockchain may switch to an alternative valid chain
// - Notifications are sent to subscribers about the invalidation
// - Mining and processing operations are updated to reflect the change
//
// This operation is irreversible through normal blockchain operations and
// should only be used when there is clear evidence of block invalidity.
// The invalidation process ensures blockchain consistency and consensus
// compliance across the network.
//
// Common scenarios for block invalidation include:
// - Discovery of consensus rule violations after acceptance
// - Network-wide rejection of previously accepted blocks
// - Resolution of blockchain forks in favor of alternative chains
// - Manual intervention for blockchain integrity issues
//
// The method communicates with the blockchain store to perform the
// invalidation and triggers any necessary reorganization processes.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - request: InvalidateBlockRequest containing the hash of the block to invalidate
//
// Returns:
//   - *emptypb.Empty: Empty response on successful invalidation
//   - error: Any error encountered during the invalidation process
func (b *Blockchain) InvalidateBlock(ctx context.Context, request *blockchain_api.InvalidateBlockRequest) (*blockchain_api.InvalidateBlockResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "InvalidateBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainInvalidateBlock),
		tracing.WithDebugLogMessage(b.logger, "[InvalidateBlock] called with hash %s", utils.ReverseAndHexEncodeSlice(request.BlockHash)),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockInvalidError("[Blockchain][InvalidateBlock] request's hash is not valid", err))
	}

	// invalidate block will also invalidate all child blocks
	invalidatedHashes, err := b.store.InvalidateBlock(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	// Log successful invalidation with count
	if len(invalidatedHashes) > 1 {
		b.logger.Infof("[InvalidateBlock] Invalidated block %s and %d child blocks", blockHash.String(), len(invalidatedHashes)-1)
	} else {
		b.logger.Infof("[InvalidateBlock] Invalidated block %s", blockHash.String())
	}

	invalidatedHashBytes := make([][]byte, len(invalidatedHashes))

	for i, hash := range invalidatedHashes {
		invalidatedHashBytes[i] = hash.CloneBytes()
	}

	// Clear any cached difficulty that may depend on the previous best tip
	b.difficulty.ResetCache()

	// send notification about the block being invalidated, this will trigger all listeners to reconsider best block
	if _, err = b.SendNotification(ctx, &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: blockHash.CloneBytes(),
	}); err != nil {
		b.logger.Errorf("[Blockchain] Error sending notification for best block %s: %v", blockHash, err)
	}

	// send BlockMinedUnset notification to trigger immediate processing by BlockValidation
	// This ensures transaction status is updated immediately for invalidated blocks
	// instead of waiting for the periodic job (which runs every 1 minute in production)
	if _, err = b.SendNotification(ctx, &blockchain_api.Notification{
		Type: model.NotificationType_BlockMinedUnset,
		Hash: blockHash.CloneBytes(),
	}); err != nil {
		b.logger.Errorf("[Blockchain] Error sending BlockMinedUnset notification for %s: %v", blockHash, err)
	}

	return &blockchain_api.InvalidateBlockResponse{
		InvalidatedBlocks: invalidatedHashBytes,
	}, nil
}

// RevalidateBlock restores a previously invalidated block.
// This method reverses a previous block invalidation, marking a previously
// invalid block as valid again in the blockchain store. This operation is
// used to correct erroneous invalidations or restore blocks that were
// temporarily invalidated during blockchain reorganization processes.
//
// Block revalidation is essential for:
// - Correcting mistaken block invalidations
// - Restoring blocks after resolving temporary consensus issues
// - Managing blockchain reorganizations and fork resolutions
// - Supporting manual intervention in blockchain integrity recovery
// - Handling network-wide consensus corrections
//
// When a block is revalidated, the following occurs:
// - The block's invalid status is removed from the blockchain store
// - The block becomes eligible for inclusion in the valid chain again
// - Dependent blocks may also be revalidated if appropriate
// - The blockchain may reorganize to include the restored block
// - Notifications are sent to subscribers about the revalidation
//
// This operation should be used carefully and only when there is clear
// evidence that the previous invalidation was incorrect or no longer
// applicable. The revalidation process ensures that blockchain integrity
// is maintained while allowing for correction of previous decisions.
//
// Common scenarios for block revalidation include:
// - Correction of erroneous invalidations
// - Resolution of temporary network consensus issues
// - Recovery from blockchain fork resolution errors
// - Manual intervention for blockchain integrity restoration
//
// The method communicates with the blockchain store to perform the
// revalidation and may trigger blockchain reorganization processes.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - request: RevalidateBlockRequest containing the hash of the block to revalidate
//
// Returns:
//   - *emptypb.Empty: Empty response on successful revalidation
//   - error: Any error encountered during the revalidation process
func (b *Blockchain) RevalidateBlock(ctx context.Context, request *blockchain_api.RevalidateBlockRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "RevalidateBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainRevalidateBlock),
		tracing.WithDebugLogMessage(b.logger, "[RevalidateBlock] called with hash %x", request.BlockHash),
	)
	defer deferFn()

	blockHash, err := chainhash.NewHash(request.BlockHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockInvalidError("[Blockchain][RevalidateBlock] request's hash is not valid", err))
	}

	// revalidate block will NOT revalidate child blocks - they need to be revalidated manually if needed
	err = b.store.RevalidateBlock(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	block, _, err := b.store.GetBlock(ctx, blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	if err = b.sendKafkaBlockFinalNotification(block); err != nil {
		b.logger.Errorf("[AddBlock] error sending Kafka notification for new block %s: %v", block.Hash(), err)
	}

	// send notification about the revalidated block
	if _, err = b.SendNotification(ctx, &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: blockHash.CloneBytes(),
	}); err != nil {
		b.logger.Errorf("[Blockchain] Error sending notification for revalidated block %s: %v", blockHash, err)
	}

	// Clear any cached difficulty that may depend on the previous best tip
	b.difficulty.ResetCache()

	return &emptypb.Empty{}, nil
}

// SendNotification broadcasts a notification to all subscribers.
// This method provides a mechanism for broadcasting blockchain events and
// notifications to all active subscribers in the notification system. It
// serves as the central hub for distributing real-time blockchain events
// across the Teranode system.
//
// The notification system is essential for:
// - Broadcasting new block arrivals to interested services
// - Notifying about blockchain reorganizations and invalidations
// - Distributing FSM state changes to monitoring and management systems
// - Coordinating mining operations and block processing workflows
// - Supporting event-driven architectures across Teranode components
//
// The method accepts various notification types including:
// - Block notifications for new blocks added to the chain
// - FSM state transition notifications
// - Mining status updates and confirmations
// - Block invalidation and revalidation events
// - Custom application-specific notifications
//
// The notification is queued in the internal notification channel and
// processed asynchronously by the subscription management system. This
// ensures that the sending operation is non-blocking and doesn't impact
// the performance of the calling service.
//
// All active subscribers will receive the notification through their
// respective subscription channels, enabling real-time event processing
// across the blockchain service ecosystem.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - req: Notification containing the event type, data, and metadata
//
// Returns:
//   - *emptypb.Empty: Empty response indicating successful notification queuing
//   - error: Any error encountered during notification processing
func (b *Blockchain) SendNotification(ctx context.Context, req *blockchain_api.Notification) (*emptypb.Empty, error) {
	_, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "RevalidateBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainSendNotification),
		tracing.WithLogMessage(b.logger, "[SendNotification] called for %s notification type %s", utils.ReverseAndHexEncodeSlice(req.Hash), req.Type.String()),
	)
	defer deferFn()

	b.notifications <- req

	return &emptypb.Empty{}, nil
}

// GetBlockIsMined checks if a block has been mined in the blockchain.
func (b *Blockchain) GetBlockIsMined(ctx context.Context, req *blockchain_api.GetBlockIsMinedRequest) (*blockchain_api.GetBlockIsMinedResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockIsMined",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockIsMined),
		tracing.WithDebugLogMessage(b.logger, "[GetBlockIsMined] called with hash %x", req.BlockHash),
	)
	defer deferFn()

	blockHash := chainhash.Hash(req.BlockHash)

	isMined, err := b.store.GetBlockIsMined(ctx, &blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &blockchain_api.GetBlockIsMinedResponse{
		IsMined: isMined,
	}, nil
}

// SetBlockMinedSet marks a block as mined in the blockchain.
func (b *Blockchain) SetBlockMinedSet(ctx context.Context, req *blockchain_api.SetBlockMinedSetRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "SetBlockMinedSet",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainSetBlockMinedSet),
		tracing.WithDebugLogMessage(b.logger, "[SetBlockMinedSet] called with hash %x", req.BlockHash),
	)
	defer deferFn()

	blockHash := chainhash.Hash(req.BlockHash)

	err := b.store.SetBlockMinedSet(ctx, &blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &emptypb.Empty{}, nil
}

// ClearBlockMinedSet resets the mined_set flag to false for a block.
func (b *Blockchain) ClearBlockMinedSet(ctx context.Context, req *blockchain_api.ClearBlockMinedSetRequest) (*emptypb.Empty, error) {
	blockHash := chainhash.Hash(req.BlockHash)

	err := b.store.ClearBlockMinedSet(ctx, &blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &emptypb.Empty{}, nil
}

// GetBlocksMinedNotSet retrieves blocks that haven't been marked as mined.
func (b *Blockchain) GetBlocksMinedNotSet(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetBlocksMinedNotSetResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlocksMinedNotSet",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlocksMinedNotSet),
		tracing.WithDebugLogMessage(b.logger, "[GetBlocksMinedNotSet] called"),
	)
	defer deferFn()

	blocks, err := b.store.GetBlocksMinedNotSet(ctx)
	if err != nil {
		return nil, err
	}

	blockBytes := make([][]byte, len(blocks))
	for i, block := range blocks {
		blockBytes[i], err = block.Bytes()
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][GetBlocksMinedNotSet] request's hash is not valid", err))
		}
	}

	return &blockchain_api.GetBlocksMinedNotSetResponse{
		BlockBytes: blockBytes,
	}, nil
}

// SetBlockPersistedAt marks a block as persisted in blob storage.
func (b *Blockchain) SetBlockPersistedAt(ctx context.Context, req *blockchain_api.SetBlockPersistedAtRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "SetBlockPersistedAt",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainSetBlockPersistedAt),
		tracing.WithDebugLogMessage(b.logger, "[SetBlockPersistedAt] called with hash %x", req.BlockHash),
	)
	defer deferFn()

	blockHash := chainhash.Hash(req.BlockHash)

	err := b.store.SetBlockPersistedAt(ctx, &blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &emptypb.Empty{}, nil
}

// GetBlocksNotPersisted retrieves blocks that haven't been persisted to blob storage yet.
func (b *Blockchain) GetBlocksNotPersisted(ctx context.Context, req *blockchain_api.GetBlocksNotPersistedRequest) (*blockchain_api.GetBlocksNotPersistedResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlocksNotPersisted",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlocksNotPersisted),
		tracing.WithDebugLogMessage(b.logger, "[GetBlocksNotPersisted] called with limit %d", req.Limit),
	)
	defer deferFn()

	blocks, err := b.store.GetBlocksNotPersisted(ctx, int(req.Limit))
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockBytes := make([][]byte, len(blocks))
	for i, block := range blocks {
		blockBytes[i], err = block.Bytes()
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][GetBlocksNotPersisted] failed to serialize block", err))
		}
	}

	return &blockchain_api.GetBlocksNotPersistedResponse{
		BlockBytes: blockBytes,
	}, nil
}

// SetBlockSubtreesSet marks a block's subtrees as set in the blockchain.
func (b *Blockchain) SetBlockSubtreesSet(ctx context.Context, req *blockchain_api.SetBlockSubtreesSetRequest) (*emptypb.Empty, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "SetBlockSubtreesSet",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainSetBlockSubtreesSet),
		tracing.WithDebugLogMessage(b.logger, "[SetBlockSubtreesSet] called with hash %x", req.BlockHash),
	)
	defer deferFn()

	blockHash := chainhash.Hash(req.BlockHash)

	err := b.store.SetBlockSubtreesSet(ctx, &blockHash)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	_, _ = b.SendNotification(ctx, &blockchain_api.Notification{
		Type: model.NotificationType_BlockSubtreesSet,
		Hash: blockHash.CloneBytes(),
	})

	return &emptypb.Empty{}, nil
}

// GetBlocksSubtreesNotSet retrieves blocks whose subtrees haven't been set.
func (b *Blockchain) GetBlocksSubtreesNotSet(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetBlocksSubtreesNotSetResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlocksSubtreesNotSet",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlocksSubtreesNotSet),
		tracing.WithDebugLogMessage(b.logger, "[GetBlocksSubtreesNotSet] called"),
	)
	defer deferFn()

	blocks, err := b.store.GetBlocksSubtreesNotSet(ctx)
	if err != nil {
		return nil, err
	}

	blockBytes := make([][]byte, len(blocks))
	for i, block := range blocks {
		blockBytes[i], err = block.Bytes()
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][GetBlocksSubtreesNotSet] request's hash is not valid", err))
		}
	}

	return &blockchain_api.GetBlocksSubtreesNotSetResponse{
		BlockBytes: blockBytes,
	}, nil
}

// FSM related endpoints

// GetFSMCurrentState retrieves the current state of the finite state machine.
// This method provides access to the blockchain service's operational state,
// which is managed by a finite state machine (FSM) that coordinates the
// service's lifecycle and processing modes. The FSM is central to the
// blockchain service's operational coordination and state management.
//
// The FSM manages critical blockchain service states such as:
// - IDLE: Service is idle and ready for operations
// - RUNNING: Service is actively processing blocks and transactions
// - SYNCING: Service is synchronizing with the network
// - CATCHING_BLOCKS: Service is catching up on missing blocks
// - LEGACY_SYNCING: Service is using legacy synchronization protocols
// - STOPPING: Service is gracefully shutting down
// - ERROR: Service has encountered an error condition
//
// The current state information is essential for:
// - Coordinating operations between blockchain components
// - Monitoring service health and operational status
// - Implementing proper shutdown and startup sequences
// - Debugging service state transitions and issues
// - Ensuring proper service coordination in distributed systems
// - Supporting automated operational workflows and monitoring
//
// The method returns the authoritative current state directly from the
// finite state machine instance, providing real-time operational status
// information that can be used by monitoring systems, other services,
// and operational tools.
//
// This is the server-side implementation that provides the authoritative
// FSM state, complementing the client-side caching mechanisms for
// performance optimization.
//
// Parameters:
//   - ctx: Context for the operation (unused but required for gRPC interface)
//   - _: Empty request parameter (unused but required for gRPC interface)
//
// Returns:
//   - *blockchain_api.GetFSMStateResponse: Response containing the current FSM state
//   - error: Any error encountered during state retrieval (typically nil)
func (b *Blockchain) GetFSMCurrentState(_ context.Context, _ *emptypb.Empty) (*blockchain_api.GetFSMStateResponse, error) {
	startTime := time.Now()
	defer func() {
		prometheusBlockchainGetFSMCurrentState.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	var state string

	if b.finiteStateMachine == nil {
		return nil, errors.WrapGRPC(errors.NewStateInitializationError("FSM is not initialized"))
	}

	// Get the current state of the FSM
	actualState := b.finiteStateMachine.Current()

	// If subscription manager is not ready, always return IDLE regardless of actual FSM state
	// This prevents services from proceeding until the blockchain service is fully operational
	if !b.subscriptionManagerReady.Load() {
		b.logger.Debugf("[Blockchain] GetFSMCurrentState: Subscription manager not ready, returning IDLE (actual state: %s)", actualState)
		state = blockchain_api.FSMStateType_IDLE.String()
	} else {
		state = actualState
	}

	// Convert the string state to FSMStateType using the map
	enumState, ok := blockchain_api.FSMStateType_value[state]
	if !ok {
		// Handle the case where the state is not found in the map
		return nil, errors.WrapGRPC(errors.NewProcessingError("invalid state: %s", state))
	}

	return &blockchain_api.GetFSMStateResponse{
		State: blockchain_api.FSMStateType(enumState),
	}, nil
}

// WaitForFSMtoTransitionToGivenState waits for the FSM to reach a specific state.
func (b *Blockchain) WaitForFSMtoTransitionToGivenState(ctx context.Context, targetState blockchain_api.FSMStateType) error {
	for b.finiteStateMachine.Current() != targetState.String() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		b.logger.Debugf("Waiting 1 second for FSM to transition to %v state, currently at: %v", targetState.String(), b.finiteStateMachine.Current())
		time.Sleep(1 * time.Second) // Wait and check again in 1 second
	}

	return nil
}

// WaitUntilFSMTransitionFromIdleState waits for the FSM to transition from the IDLE state.
func (b *Blockchain) WaitUntilFSMTransitionFromIdleState(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	// Wait until:
	// 1. FSM is initialized and not in IDLE state
	// 2. Subscription manager is ready to handle subscriptions
	// This ensures services don't proceed until the blockchain service is fully operational
	for b.finiteStateMachine.Current() == "" ||
		b.finiteStateMachine.Current() == blockchain_api.FSMStateType_IDLE.String() ||
		!b.subscriptionManagerReady.Load() {

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		actualState := b.finiteStateMachine.Current()
		subscriptionReady := b.subscriptionManagerReady.Load()
		b.logger.Debugf("Waiting for full readiness - FSM state: %v, Subscription ready: %v", actualState, subscriptionReady)
		time.Sleep(1 * time.Second) // Wait and check again in 1 second
	}

	b.logger.Infof("[Blockchain] Service is now fully ready - FSM: %v, Subscriptions: ready", b.finiteStateMachine.Current())
	return &emptypb.Empty{}, nil
}

// IsFullyReady checks if the blockchain service is fully operational.
// This includes both FSM being in a non-IDLE state and subscription infrastructure being ready.
// Services should use this method to determine if they can safely proceed with blockchain operations.
func (b *Blockchain) IsFullyReady(ctx context.Context) (bool, error) {
	if b.finiteStateMachine == nil {
		return false, nil
	}

	actualState := b.finiteStateMachine.Current()
	subscriptionReady := b.subscriptionManagerReady.Load()

	// Service is fully ready if:
	// 1. FSM is initialized and not in IDLE state
	// 2. Subscription manager is ready
	isReady := actualState != "" &&
		actualState != blockchain_api.FSMStateType_IDLE.String() &&
		subscriptionReady

	b.logger.Debugf("[Blockchain] IsFullyReady check - FSM: %v, Subscription ready: %v, Result: %v", actualState, subscriptionReady, isReady)
	return isReady, nil
}

// SendFSMEvent sends an event to the finite state machine.
func (b *Blockchain) SendFSMEvent(ctx context.Context, eventReq *blockchain_api.SendFSMEventRequest) (*blockchain_api.GetFSMStateResponse, error) {
	b.logger.Infof("[Blockchain Server] Received FSM event req: %v, will send event to the FSM", eventReq)

	priorState := b.finiteStateMachine.Current()

	// Prevent manual transitions from CATCHINGBLOCKS state
	// The state should only exit CATCHINGBLOCKS programmatically when catchup completes
	if priorState == blockchain_api.FSMStateType_CATCHINGBLOCKS.String() {
		// Only allow RUN event (catchup completion) to exit CATCHINGBLOCKS
		if eventReq.Event != blockchain_api.FSMEventType_RUN {
			errMsg := "cannot manually transition from CATCHINGBLOCKS state - catchup must complete first"
			b.logger.Warnf("[Blockchain Server] %s (attempted event: %v)", errMsg, eventReq.Event)
			return nil, errors.NewInvalidArgumentError(errMsg)
		}
	}

	err := b.finiteStateMachine.Event(ctx, eventReq.Event.String())
	if err != nil {
		b.logger.Debugf("[Blockchain Server] Error sending event to FSM, state has not changed.")
		return nil, err
	}

	state := b.finiteStateMachine.Current()

	// set the state in persistent storage
	err = b.store.SetFSMState(ctx, state)
	// check if there was an error setting the state
	if err != nil {
		b.logger.Errorf("[Blockchain Server] Error setting the state in blockchain db: %v", err)
	}

	// Log the state immediately after storing it
	// b.logger.Infof("[Blockchain Server] state immediately after storing: %v", state)

	resp := &blockchain_api.GetFSMStateResponse{
		State: blockchain_api.FSMStateType(blockchain_api.FSMStateType_value[state]),
	}

	b.logger.Infof("[Blockchain Server] FSM current state: %v, response: %v", b.finiteStateMachine.Current(), resp)

	// For test purposes, we want to ensure that the state changes cannot happen too fast
	// This is to ensure that the state change is captured in the test
	duration := time.Since(b.stateChangeTimestamp)
	if duration < b.settings.BlockChain.FSMStateChangeDelay {
		b.logger.Warnf("[Blockchain Server] State transition too fast for tests. From %v to %v. Sleeping for %v before returning the response (should only happen in tests)", priorState, state, b.settings.BlockChain.FSMStateChangeDelay-duration)
		time.Sleep(b.settings.BlockChain.FSMStateChangeDelay - duration)
	}

	b.stateChangeTimestamp = time.Now()

	return resp, nil
}

// Run transitions the blockchain service to the running state.
func (b *Blockchain) Run(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	// check whether the FSM is already in the RUNNING state
	if b.finiteStateMachine.Is(blockchain_api.FSMStateType_RUNNING.String()) {
		return &emptypb.Empty{}, nil
	}

	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_RUN,
	}

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		return nil, err
	}

	return nil, nil
}

// CatchUpBlocks transitions the service to catch up missing blocks.
func (b *Blockchain) CatchUpBlocks(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	// check whether the FSM is already in the CATCHINGBLOCKS state
	if b.finiteStateMachine.Is(blockchain_api.FSMStateType_CATCHINGBLOCKS.String()) {
		return &emptypb.Empty{}, nil
	}

	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_CATCHUPBLOCKS,
	}

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		return nil, err
	}

	return nil, nil
}

// ReportPeerFailure handles reports of peer download failures and broadcasts to subscribers.
func (b *Blockchain) ReportPeerFailure(ctx context.Context, req *blockchain_api.ReportPeerFailureRequest) (*emptypb.Empty, error) {
	b.logger.Warnf("[ReportPeerFailure] Peer %s failed: type=%s, reason=%s", req.PeerId, req.FailureType, req.Reason)

	// Send notification to all subscribers (including P2P)
	notification := &blockchain_api.Notification{
		Type: model.NotificationType_PeerFailure,
		Hash: req.Hash,
		Metadata: &blockchain_api.NotificationMetadata{
			Metadata: map[string]string{
				"peer_id":      req.PeerId,
				"failure_type": req.FailureType,
				"reason":       req.Reason,
			},
		},
	}

	if _, err := b.SendNotification(ctx, notification); err != nil {
		b.logger.Errorf("[ReportPeerFailure] Failed to send notification: %v", err)
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// LegacySync transitions the service to legacy sync mode.
func (b *Blockchain) LegacySync(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	// check whether the FSM is already in the LEGACYSYNC state
	if b.finiteStateMachine.Is(blockchain_api.FSMStateType_LEGACYSYNCING.String()) {
		return &emptypb.Empty{}, nil
	}

	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_LEGACYSYNC,
	}

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (b *Blockchain) Idle(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	// check whether the FSM is already in the Idle state
	if b.finiteStateMachine.Is(blockchain_api.FSMStateType_IDLE.String()) {
		return &emptypb.Empty{}, nil
	}

	req := &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_STOP,
	}

	_, err := b.SendFSMEvent(ctx, req)
	if err != nil {
		// unable to send the event, no need to update the state.
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// Legacy endpoints

// GetBlockLocator retrieves a block locator for synchronization purposes.
func (b *Blockchain) GetBlockLocator(ctx context.Context, req *blockchain_api.GetBlockLocatorRequest) (*blockchain_api.GetBlockLocatorResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "GetBlockLocator",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainGetBlockLocator),
		tracing.WithDebugLogMessage(b.logger, "[GetBlockLocator] called with hash %x", req.Hash),
	)
	defer deferFn()

	blockHeader, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewBlockNotFoundError("[Blockchain][GetBlockLocator] request's hash is not valid", err))
	}

	blockHeaderHeight := req.Height

	locatorHashes, err := getBlockLocator(ctx, b.store, blockHeader, blockHeaderHeight)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewStorageError("[Blockchain][GetBlockLocator] error using blockchain store", err))
	}

	locator := make([][]byte, len(locatorHashes))
	for i, hash := range locatorHashes {
		locator[i] = hash.CloneBytes()
	}

	return &blockchain_api.GetBlockLocatorResponse{Locator: locator}, nil
}

// LocateBlockHeaders finds block headers using a locator.
func (b *Blockchain) LocateBlockHeaders(ctx context.Context, request *blockchain_api.LocateBlockHeadersRequest) (*blockchain_api.LocateBlockHeadersResponse, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "LocateBlockHeaders",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockchainLocateBlockHeaders),
		tracing.WithDebugLogMessage(b.logger, "[LocateBlockHeaders] called with %d hashes", len(request.Locator)),
	)
	defer deferFn()

	locator := make([]*chainhash.Hash, len(request.Locator))
	for i, hash := range request.Locator {
		locator[i], _ = chainhash.NewHash(hash)
	}

	hashStop, _ := chainhash.NewHash(request.HashStop)

	// Get the blocks
	blockHeaders, err := b.store.LocateBlockHeaders(ctx, locator, hashStop, request.MaxHashes)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeaderBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeaderBytes[i] = blockHeader.Bytes()
	}

	return &blockchain_api.LocateBlockHeadersResponse{
		BlockHeaders: blockHeaderBytes,
	}, nil
}

// GetBestHeightAndTime retrieves the current best block height and median time.
func (b *Blockchain) GetBestHeightAndTime(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetBestHeightAndTimeResponse, error) {
	blockHeader, meta, err := b.store.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	// get the median block time for the last 11 blocks
	headers, _, err := b.store.GetBlockHeaders(ctx, blockHeader.Hash(), 11)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	prevTimeStamps := make([]time.Time, 0, 11)
	for _, header := range headers {
		prevTimeStamps = append(prevTimeStamps, time.Unix(int64(header.Timestamp), 0))
	}

	medianTimestamp, err := model.CalculateMedianTimestamp(prevTimeStamps)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("[Blockchain][GetBestHeightAndTime] could not calculate median block time", err))
	}

	medianTimestampUint32, err := safeconversion.TimeToUint32(*medianTimestamp)
	if err != nil {
		return nil, err
	}

	return &blockchain_api.GetBestHeightAndTimeResponse{
		Height: meta.Height,
		Time:   medianTimestampUint32,
	}, nil
}

// safeClose safely closes a channel without panicking if it's already closed.
func safeClose[T any](ch chan T) {
	defer func() {
		_ = recover()
	}()

	close(ch)
}

// getBlockLocator creates a block locator for chain synchronization.
func getBlockLocator(ctx context.Context, store blockchain_store.Store, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error) {
	// From https://github.com/bitcoinsv/bsvd/blob/20910511e9006a12e90cddc9f292af8b82950f81/blockchain/chainview.go#L351
	if blockHeaderHash == nil {
		// return genesis block
		genesisBlock, err := store.GetBlockByHeight(ctx, 0)
		if err != nil {
			return nil, err
		}

		return []*chainhash.Hash{genesisBlock.Header.Hash()}, nil
	}

	// From https://github.com/bitcoinsv/bsvd/blob/20910511e9006a12e90cddc9f292af8b82950f81/blockchain/chainview.go#L351
	// Calculate the max number of entries that will ultimately be in the
	// block locator. See the description of the algorithm for how these
	// numbers are derived.
	var maxEntries uint8

	if blockHeaderHeight <= 12 {
		blockHeaderHeightUint8, err := safeconversion.Uint32ToUint8(blockHeaderHeight)
		if err != nil {
			return nil, errors.WrapGRPC(err)
		}

		maxEntries = blockHeaderHeightUint8 + 1
	} else {
		// Requested hash itself + previous 10 entries + genesis block.
		// Then floor(log2(height-10)) entries for the skip portion.
		adjustedHeight := blockHeaderHeight - 10
		maxEntries = 12 + fastLog2Floor(adjustedHeight)
	}

	locator := make([]*chainhash.Hash, 0, maxEntries)
	step := uint32(1)
	height := blockHeaderHeight
	hash := blockHeaderHash

	for {
		block, _, err := store.GetBlockInChainByHeightHash(ctx, height, hash)
		if err != nil {
			return nil, err
		}

		hash = block.Header.Hash()
		locator = append(locator, hash)

		if height == 0 {
			break
		}

		if step > height {
			step = height
		}

		height -= step

		if len(locator) > 10 {
			step *= 2
		}
	}

	return locator, nil
}

func getBlockHeadersToCommonAncestor(ctx context.Context, store blockchain_store.Store, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	const (
		numberOfHeaders = 1_000
		searchLimit     = 10_000
	)

	var (
		commonAncestorMeta *model.BlockHeaderMeta
	)

	blockLocatorMap := make(map[chainhash.Hash]struct{}, len(blockLocatorHashes))
	for _, hash := range blockLocatorHashes {
		blockLocatorMap[*hash] = struct{}{}
	}

	max := int(maxHeaders)
	hashStart := hashTarget
	lastNHeaders := ring.New(max)
	lastNMetas := ring.New(max)

out:
	for searchCount := 0; searchCount < searchLimit; searchCount++ {
		headers, headerMetas, err := store.GetBlockHeaders(ctx, hashStart, numberOfHeaders)
		if err != nil {
			return nil, nil, errors.NewStorageError("failed to get block headers", err)
		}

		if len(headers) <= 1 {
			break
		}

		for idx, header := range headers {
			lastNHeaders.Value = header
			lastNHeaders = lastNHeaders.Next()
			lastNMetas.Value = headerMetas[idx]
			lastNMetas = lastNMetas.Next()

			if _, ok := blockLocatorMap[*header.Hash()]; ok {
				commonAncestorMeta = headerMetas[idx]
				break out
			}
		}

		// start over with the next 100 block headers
		// to find the common ancestor
		hashStart = headers[len(headers)-1].HashPrevBlock
	}

	if commonAncestorMeta == nil {
		return nil, nil, errors.NewNotFoundError("common ancestor hash not found after scanning last %d headers", searchLimit*numberOfHeaders)
	}

	headerHistory := sliceFromRing[*model.BlockHeader](lastNHeaders)
	headerMetaHistory := sliceFromRing[*model.BlockHeaderMeta](lastNMetas)

	return headerHistory, headerMetaHistory, nil
}

// GetBlockHeadersFromCommonAncestor retrieves block headers from a common ancestor.
func (b *Blockchain) GetBlockHeadersFromCommonAncestor(ctx context.Context, request *blockchain_api.GetBlockHeadersFromCommonAncestorRequest) (*blockchain_api.GetBlockHeadersResponse, error) {
	targetHash, err := chainhash.NewHash(request.TargetHash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][GetBlockHeadersFromCommonAncestor] request's target hash is not valid", err))
	}

	blockLocatorHashes := make([]chainhash.Hash, len(request.BlockLocatorHashes))
	for i, hash := range request.BlockLocatorHashes {
		blockHash, err := chainhash.NewHash(hash)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewInvalidArgumentError("[Blockchain][GetBlockHeadersFromCommonAncestor] request's block locator hash is not valid", err))
		}

		blockLocatorHashes[i] = *blockHash
	}

	blockHeaders, blockHeaderMetas, err := getBlockHeadersFromCommonAncestor(ctx, b.store, targetHash, blockLocatorHashes, request.MaxHeaders)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	blockHeadersBytes := make([][]byte, len(blockHeaders))
	for i, blockHeader := range blockHeaders {
		blockHeadersBytes[i] = blockHeader.Bytes()
	}

	blockHeaderMetasBytes := make([][]byte, len(blockHeaderMetas))
	for i, blockHeaderMeta := range blockHeaderMetas {
		blockHeaderMetasBytes[i] = blockHeaderMeta.Bytes()
	}

	return &blockchain_api.GetBlockHeadersResponse{
		BlockHeaders: blockHeadersBytes,
		Metas:        blockHeaderMetasBytes,
	}, nil
}

func getBlockHeadersFromCommonAncestor(ctx context.Context, store blockchain_store.Store, chainTipHash *chainhash.Hash, blockLocatorHashes []chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	// first we need to get the common ancestor of the target hash and the block locator hashes
	commonBlockHeader, _, err := store.GetLatestBlockHeaderFromBlockLocator(ctx, chainTipHash, blockLocatorHashes)
	if err != nil {
		return nil, nil, errors.NewProcessingError("failed to get latest block header from block locator", err)
	}

	// now get the headers from the common ancestor to the target hash
	return store.GetBlockHeadersFromOldest(ctx, chainTipHash, commonBlockHeader.Hash(), uint64(maxHeaders)) // golint:nolint
}

func sliceFromRing[T any](ring *ring.Ring) []T {
	slice := make([]T, 0, ring.Len())

	ring.Do(func(value interface{}) {
		if value != nil {
			slice = append(slice, value.(T))
		}
	})

	return slice
}

// SetBlockProcessedAt sets or clears the processed_at timestamp for a block.
func (b *Blockchain) SetBlockProcessedAt(ctx context.Context, req *blockchain_api.SetBlockProcessedAtRequest) (*emptypb.Empty, error) {
	blockHash, err := chainhash.NewHash(req.BlockHash)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("invalid block hash", err)
	}

	b.logger.Debugf("[Blockchain] Setting block processed at timestamp for %x, clear=%v", blockHash, req.Clear)

	// Check if the block exists
	exists, err := b.store.GetBlockExists(ctx, blockHash)
	if err != nil {
		return nil, errors.NewStorageError("failed to check if block exists", err)
	}

	if !exists {
		return nil, errors.NewNotFoundError("block not found: %x", blockHash)
	}

	// Update the processed_at timestamp in the database
	if err := b.store.SetBlockProcessedAt(ctx, blockHash, req.Clear); err != nil {
		return nil, errors.NewStorageError("failed to set block processed_at", err)
	}

	return &emptypb.Empty{}, nil
}

// SetSubscriptionManagerReadyForTesting sets the subscription manager ready flag for testing purposes.
// This method should only be used in tests to simulate subscription manager readiness.
func (b *Blockchain) SetSubscriptionManagerReadyForTesting(ready bool) {
	b.subscriptionManagerReady.Store(ready)
}

// ScheduleBlobDeletion schedules a blob for deletion at a specific block height.
func (b *Blockchain) ScheduleBlobDeletion(ctx context.Context, req *blockchain_api.ScheduleBlobDeletionRequest) (*blockchain_api.ScheduleBlobDeletionResponse, error) {
	if len(req.BlobKey) == 0 {
		return &blockchain_api.ScheduleBlobDeletionResponse{
			Scheduled: false,
			Message:   "blob_key is required",
		}, nil
	}

	if req.FileType == "" {
		return &blockchain_api.ScheduleBlobDeletionResponse{
			Scheduled: false,
			Message:   "file_type is required",
		}, nil
	}

	storeWithBlobDeletion, ok := b.store.(interface {
		ScheduleBlobDeletion(ctx context.Context, req *blockchain_sql.ScheduleRequest) (int64, error)
	})
	if !ok {
		return nil, errors.NewStorageError("blockchain store does not support blob deletion")
	}

	schedReq := &blockchain_sql.ScheduleRequest{
		BlobKey:        req.BlobKey,
		FileType:       req.FileType,
		StoreType:      int32(req.StoreType),
		DeleteAtHeight: req.DeleteAtHeight,
	}

	id, err := storeWithBlobDeletion.ScheduleBlobDeletion(ctx, schedReq)
	if err != nil {
		b.logger.Errorf("Failed to schedule blob deletion: %v", err)
		return nil, errors.NewStorageError("failed to schedule deletion", err)
	}

	return &blockchain_api.ScheduleBlobDeletionResponse{
		DeletionId: id,
		Scheduled:  true,
		Message:    "Deletion scheduled successfully",
	}, nil
}

// CancelBlobDeletion cancels a previously scheduled blob deletion.
func (b *Blockchain) CancelBlobDeletion(ctx context.Context, req *blockchain_api.CancelBlobDeletionRequest) (*blockchain_api.CancelBlobDeletionResponse, error) {
	if len(req.BlobKey) == 0 {
		return &blockchain_api.CancelBlobDeletionResponse{
			Cancelled: false,
			Message:   "blob_key is required",
		}, nil
	}

	storeWithBlobDeletion, ok := b.store.(interface {
		CancelBlobDeletion(ctx context.Context, blobKey []byte, fileType string, storeType int32) error
	})
	if !ok {
		return nil, errors.NewStorageError("blockchain store does not support blob deletion")
	}

	err := storeWithBlobDeletion.CancelBlobDeletion(ctx, req.BlobKey, req.FileType, int32(req.StoreType))
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			return &blockchain_api.CancelBlobDeletionResponse{
				Cancelled: false,
				Message:   "No pending deletion found",
			}, nil
		}
		return nil, err
	}

	return &blockchain_api.CancelBlobDeletionResponse{
		Cancelled: true,
		Message:   "Deletion cancelled",
	}, nil
}

// ListScheduledDeletions lists all scheduled blob deletions with optional filtering.
func (b *Blockchain) ListScheduledDeletions(ctx context.Context, req *blockchain_api.ListScheduledDeletionsRequest) (*blockchain_api.ListScheduledDeletionsResponse, error) {
	limit := int(req.Limit)
	if limit == 0 {
		limit = 100
	}

	storeWithBlobDeletion, ok := b.store.(interface {
		ListScheduledBlobDeletions(ctx context.Context, filters *blockchain_sql.ListFilters) ([]*blockchain_sql.ScheduledDeletion, int, error)
	})
	if !ok {
		return nil, errors.NewStorageError("blockchain store does not support blob deletion")
	}

	filters := &blockchain_sql.ListFilters{
		MinHeight:     req.MinHeight,
		MaxHeight:     req.MaxHeight,
		StoreType:     int32(req.StoreType),
		FilterByStore: req.FilterByStore,
		Limit:         limit,
		Offset:        int(req.Offset),
	}

	deletions, total, err := storeWithBlobDeletion.ListScheduledBlobDeletions(ctx, filters)
	if err != nil {
		return nil, errors.NewStorageError("failed to list deletions", err)
	}

	protoDeletions := make([]*blockchain_api.ScheduledDeletion, len(deletions))
	for i, d := range deletions {
		protoDeletions[i] = &blockchain_api.ScheduledDeletion{
			Id:             d.ID,
			BlobKey:        d.BlobKey,
			FileType:       d.FileType,
			StoreType:      d.StoreType,
			DeleteAtHeight: d.DeleteAtHeight,
			RetryCount:     uint32(d.RetryCount),
		}
	}

	return &blockchain_api.ListScheduledDeletionsResponse{
		Deletions:  protoDeletions,
		TotalCount: int32(total),
	}, nil
}

// GetPendingBlobDeletions retrieves blob deletions ready for processing at a specific height.
func (b *Blockchain) GetPendingBlobDeletions(ctx context.Context, req *blockchain_api.GetPendingBlobDeletionsRequest) (*blockchain_api.GetPendingBlobDeletionsResponse, error) {
	limit := int(req.Limit)
	if limit == 0 {
		limit = 100
	}

	storeWithBlobDeletion, ok := b.store.(interface {
		GetPendingBlobDeletions(ctx context.Context, height uint32, limit int) ([]*blockchain_sql.ScheduledDeletion, error)
	})
	if !ok {
		return nil, errors.NewStorageError("blockchain store does not support blob deletion")
	}

	deletions, err := storeWithBlobDeletion.GetPendingBlobDeletions(ctx, req.Height, limit)
	if err != nil {
		return nil, errors.NewStorageError("failed to get pending deletions", err)
	}

	protoDeletions := make([]*blockchain_api.ScheduledDeletion, len(deletions))
	for i, d := range deletions {
		protoDeletions[i] = &blockchain_api.ScheduledDeletion{
			Id:             d.ID,
			BlobKey:        d.BlobKey,
			FileType:       d.FileType,
			StoreType:      d.StoreType,
			DeleteAtHeight: d.DeleteAtHeight,
			RetryCount:     uint32(d.RetryCount),
		}
	}

	return &blockchain_api.GetPendingBlobDeletionsResponse{
		Deletions: protoDeletions,
	}, nil
}

// RemoveBlobDeletion removes a blob deletion from the schedule (after successful deletion).
func (b *Blockchain) RemoveBlobDeletion(ctx context.Context, req *blockchain_api.RemoveBlobDeletionRequest) (*emptypb.Empty, error) {
	storeWithBlobDeletion, ok := b.store.(interface {
		RemoveBlobDeletion(ctx context.Context, id int64) error
	})
	if !ok {
		return nil, errors.NewStorageError("blockchain store does not support blob deletion")
	}

	err := storeWithBlobDeletion.RemoveBlobDeletion(ctx, req.DeletionId)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// IncrementBlobDeletionRetry increments the retry counter for a failed blob deletion.
func (b *Blockchain) IncrementBlobDeletionRetry(ctx context.Context, req *blockchain_api.IncrementBlobDeletionRetryRequest) (*blockchain_api.IncrementBlobDeletionRetryResponse, error) {
	storeWithBlobDeletion, ok := b.store.(interface {
		IncrementBlobDeletionRetry(ctx context.Context, id int64, maxRetries int) (shouldRemove bool, newRetryCount int, err error)
	})
	if !ok {
		return nil, errors.NewStorageError("blockchain store does not support blob deletion")
	}

	shouldRemove, newRetryCount, err := storeWithBlobDeletion.IncrementBlobDeletionRetry(ctx, req.DeletionId, int(req.MaxRetries))
	if err != nil {
		return nil, err
	}

	return &blockchain_api.IncrementBlobDeletionRetryResponse{
		ShouldRemove:  shouldRemove,
		NewRetryCount: int32(newRetryCount),
	}, nil
}

// CompleteBlobDeletions handles batch completion of multiple deletions.
func (b *Blockchain) CompleteBlobDeletions(ctx context.Context, req *blockchain_api.CompleteBlobDeletionsRequest) (*blockchain_api.CompleteBlobDeletionsResponse, error) {
	storeWithBatchCompletion, ok := b.store.(interface {
		CompleteBlobDeletions(ctx context.Context, completedIDs []int64, failedIDs []int64, maxRetries int) (int, int, error)
	})
	if !ok {
		return nil, errors.NewStorageError("blockchain store does not support batch blob deletion")
	}

	removedCount, retryIncrementedCount, err := storeWithBatchCompletion.CompleteBlobDeletions(ctx, req.CompletedIds, req.FailedIds, int(req.MaxRetries))
	if err != nil {
		return nil, err
	}

	b.logger.Infof("Batch completed: %d removed, %d retry incremented", removedCount, retryIncrementedCount)

	return &blockchain_api.CompleteBlobDeletionsResponse{
		RemovedCount:          int32(removedCount),
		RetryIncrementedCount: int32(retryIncrementedCount),
	}, nil
}

// AcquireBlobDeletionBatch acquires a batch of deletions with locking.
func (b *Blockchain) AcquireBlobDeletionBatch(ctx context.Context, req *blockchain_api.AcquireBlobDeletionBatchRequest) (*blockchain_api.AcquireBlobDeletionBatchResponse, error) {
	storeWithBatchAcquisition, ok := b.store.(interface {
		AcquireBlobDeletionBatch(ctx context.Context, height uint32, limit int, lockTimeoutSeconds int) ([]*blockchain_sql.ScheduledDeletion, error)
	})
	if !ok {
		return nil, errors.NewStorageError("blockchain store does not support batch acquisition")
	}

	lockTimeout := int(req.LockTimeoutSeconds)
	if lockTimeout == 0 {
		lockTimeout = 300 // Default: 5 minutes
	}

	deletions, err := storeWithBatchAcquisition.AcquireBlobDeletionBatch(ctx, req.Height, int(req.Limit), lockTimeout)
	if err != nil {
		return nil, err
	}

	if len(deletions) == 0 {
		return &blockchain_api.AcquireBlobDeletionBatchResponse{
			BatchToken: "",
			Deletions:  nil,
		}, nil
	}

	// Generate batch token
	token := b.generateBatchToken()

	// Store batch token info
	deletionIDs := make([]int64, len(deletions))
	for i, d := range deletions {
		deletionIDs[i] = d.ID
	}

	b.batchTokensMu.Lock()
	b.batchTokens[token] = &blobDeletionBatchToken{
		token:         token,
		acquiredAt:    time.Now(),
		expiresAt:     time.Now().Add(time.Duration(lockTimeout) * time.Second),
		deletionIDs:   deletionIDs,
		deletionCount: len(deletions),
	}
	b.batchTokensMu.Unlock()

	// Convert to protobuf format
	protoDeletions := make([]*blockchain_api.ScheduledDeletion, len(deletions))
	for i, d := range deletions {
		protoDeletions[i] = &blockchain_api.ScheduledDeletion{
			Id:             d.ID,
			BlobKey:        d.BlobKey,
			FileType:       d.FileType,
			StoreType:      d.StoreType,
			DeleteAtHeight: d.DeleteAtHeight,
			RetryCount:     uint32(d.RetryCount),
		}
	}

	b.logger.Infof("Acquired blob deletion batch: token=%s, count=%d, height=%d", token, len(deletions), req.Height)

	return &blockchain_api.AcquireBlobDeletionBatchResponse{
		BatchToken: token,
		Deletions:  protoDeletions,
	}, nil
}

// CompleteBlobDeletionBatch completes a previously acquired batch.
func (b *Blockchain) CompleteBlobDeletionBatch(ctx context.Context, req *blockchain_api.CompleteBlobDeletionBatchRequest) (*emptypb.Empty, error) {
	// Validate batch token
	b.batchTokensMu.Lock()
	tokenInfo, exists := b.batchTokens[req.BatchToken]
	if !exists {
		b.batchTokensMu.Unlock()
		return nil, errors.NewInvalidArgumentError("invalid or expired batch token")
	}

	// Check if expired
	if time.Now().After(tokenInfo.expiresAt) {
		delete(b.batchTokens, req.BatchToken)
		b.batchTokensMu.Unlock()
		return nil, errors.NewInvalidArgumentError("batch token expired")
	}

	// Remove token (it's single-use)
	delete(b.batchTokens, req.BatchToken)
	b.batchTokensMu.Unlock()

	// Complete the batch
	storeWithBatchCompletion, ok := b.store.(interface {
		CompleteBlobDeletions(ctx context.Context, completedIDs []int64, failedIDs []int64, maxRetries int) (int, int, error)
	})
	if !ok {
		return nil, errors.NewStorageError("blockchain store does not support batch completion")
	}

	removedCount, retryIncrementedCount, err := storeWithBatchCompletion.CompleteBlobDeletions(ctx, req.CompletedIds, req.FailedIds, int(req.MaxRetries))
	if err != nil {
		return nil, err
	}

	b.logger.Infof("Batch %s completed: %d removed, %d retry incremented", req.BatchToken, removedCount, retryIncrementedCount)

	return &emptypb.Empty{}, nil
}

// generateBatchToken generates a unique batch token.
func (b *Blockchain) generateBatchToken() string {
	// Use timestamp + counter for uniqueness (no crypto needed for batch tokens)
	return fmt.Sprintf("batch_%d_%p", time.Now().UnixNano(), b)
}

// cleanupExpiredBatchTokens removes expired batch tokens periodically.
// This should be called as a goroutine during service startup.
func (b *Blockchain) cleanupExpiredBatchTokens() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-b.AppCtx.Done():
			return
		case <-ticker.C:
			b.batchTokensMu.Lock()
			now := time.Now()
			for token, info := range b.batchTokens {
				if now.After(info.expiresAt) {
					b.logger.Warnf("Cleaning up expired batch token: %s (acquired: %s, expired: %s)",
						token, info.acquiredAt, info.expiresAt)
					delete(b.batchTokens, token)
				}
			}
			b.batchTokensMu.Unlock()
		}
	}
}
