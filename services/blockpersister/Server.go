// Package blockpersister provides comprehensive functionality for persisting blockchain blocks and their associated data.
//
// The blockpersister service is responsible for taking blocks from the blockchain service and ensuring they are
// properly stored in persistent storage along with all related data (transactions, UTXOs, etc.). It plays a
// critical role in the overall blockchain data persistence strategy by:
//
// - Processing and storing complete blocks in the blob store
// - Managing subtree processing for efficient transaction handling
// - Maintaining UTXO set differences for each block
// - Ensuring data consistency and integrity during persistence operations
// - Providing resilient error handling and recovery mechanisms
//
// The service integrates with multiple stores (block store, subtree store, UTXO store) and
// coordinates between them to ensure consistent and reliable block data persistence.
// It employs concurrency and batching techniques to optimize performance for high transaction volumes.
package blockpersister

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/ordishs/gocore"
)

// Server represents the main block persister service that handles block storage and processing.
// It coordinates the persistence of blocks and their associated data across multiple storage systems,
// ensuring data integrity and consistency throughout the process.
type Server struct {
	// ctx is the context for controlling server lifecycle and handling cancellation signals
	ctx context.Context

	// logger provides structured logging functionality for operational monitoring and debugging
	logger ulogger.Logger

	// settings contains configuration settings for the server, controlling behavior such as
	// concurrency levels, batch sizes, and persistence strategies
	settings *settings.Settings

	// blockStore provides persistent storage for complete blocks
	// This is typically implemented as a blob store capable of handling large block data
	blockStore blob.Store

	// subtreeStore provides storage for block subtrees, which are hierarchical structures
	// containing transaction references that make up parts of a block
	subtreeStore blob.Store

	// utxoStore provides storage for UTXO (Unspent Transaction Output) data
	// Used to track the current state of the UTXO set and process changes
	utxoStore utxo.Store

	// stats tracks operational statistics for monitoring and performance analysis
	stats *gocore.Stat

	// blockchainClient interfaces with the blockchain service to retrieve block data
	// and coordinate persistence operations with blockchain state
	blockchainClient blockchain.ClientI
}

// New creates a new block persister server instance with the provided dependencies.
//
// This constructor initializes all components required for block persistence operations,
// including stores and client connections. It accepts optional configuration functions
// to customize the server instance after construction.
//
// Parameters:
//   - ctx: Context for controlling the server lifecycle
//   - logger: Logger for recording operational events and errors
//   - tSettings: Configuration settings that control server behavior
//   - blockStore: Storage interface for blocks
//   - subtreeStore: Storage interface for block subtrees
//   - utxoStore: Storage interface for UTXO data
//   - blockchainClient: Client for interacting with the blockchain service
//   - opts: Optional configuration functions to apply after construction
//
// Returns a fully constructed and configured Server instance ready for initialization.
func New(
	ctx context.Context,
	logger ulogger.Logger,
	tSettings *settings.Settings,
	blockStore blob.Store,
	subtreeStore blob.Store,
	utxoStore utxo.Store,
	blockchainClient blockchain.ClientI,
	opts ...func(*Server),
) *Server {
	u := &Server{
		ctx:              ctx,
		logger:           logger,
		settings:         tSettings,
		blockStore:       blockStore,
		subtreeStore:     subtreeStore,
		utxoStore:        utxoStore,
		stats:            gocore.NewStat("blockpersister"),
		blockchainClient: blockchainClient,
	}

	// Apply optional configuration functions
	for _, opt := range opts {
		opt(u)
	}

	return u
}

// Health performs health checks on the server and its dependencies.
// This method implements the health.Check interface and is used by monitoring systems
// to determine the operational status of the service.
//
// The health check distinguishes between liveness (is the service running?) and
// readiness (is the service able to handle requests?) checks:
//   - Liveness checks verify the service process is running and responsive
//   - Readiness checks verify all dependencies are available and functioning
//
// Parameters:
//   - ctx: Context for coordinating cancellation or timeouts
//   - checkLiveness: When true, only liveness checks are performed; when false, both liveness
//     and readiness checks are performed
//
// Returns:
//   - int: HTTP status code (200 for healthy, 503 for unhealthy)
//   - string: Human-readable status message
//   - error: Any error encountered during health checking
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
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
	checks := make([]health.Check, 0, 5)

	if u.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: u.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(u.blockchainClient)})
	}

	if u.blockStore != nil {
		checks = append(checks, health.Check{Name: "BlockStore", Check: u.blockStore.Health})
	}

	if u.subtreeStore != nil {
		checks = append(checks, health.Check{Name: "SubtreeStore", Check: u.subtreeStore.Health})
	}

	if u.utxoStore != nil {
		checks = append(checks, health.Check{Name: "UTXOStore", Check: u.utxoStore.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// Init initializes the server, setting up any required resources.
//
// This method is called after construction but before the server starts processing blocks.
// It performs one-time initialization tasks such as setting up Prometheus metrics.
//
// Parameters:
//   - ctx: Context for coordinating initialization operations
//
// Returns an error if initialization fails, or nil on success.
func (u *Server) Init(ctx context.Context) (err error) {
	initPrometheusMetrics()

	return nil
}

// getNextBlockToProcess retrieves the next block that needs to be persisted to blob storage.
//
// This method queries the database for blocks that haven't been persisted yet (persisted_at IS NULL)
// and aren't marked as invalid. The database stores block metadata and tracks persistence status,
// eliminating the need for external state files.
//
// Parameters:
//   - ctx: Context for coordinating the block retrieval operation
//
// Returns:
//   - *model.Block: The next block to process, or nil if no block needs processing yet
//   - error: Any error encountered during the operation
func (u *Server) getNextBlockToProcess(ctx context.Context) (*model.Block, error) {
	blocks, err := u.blockchainClient.GetBlocksNotPersisted(ctx, 1)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get blocks not persisted", err)
	}

	if len(blocks) == 0 {
		return nil, nil
	}

	return blocks[0], nil
}

// Start initializes and begins the block persister service operations.
//
// This method starts the main processing loop and sets up HTTP services if configured.
// It waits for the blockchain FSM to transition from IDLE state before beginning
// block persistence operations to ensure the blockchain is ready.
//
// The method implements the following key operations:
// - Waits for blockchain service readiness
// - Sets up HTTP blob server if required by configuration
// - Starts the main processing loop in a background goroutine
// - Signals service readiness through the provided channel
//
// Parameters:
//   - ctx: Context for controlling the service lifecycle and handling cancellation
//   - readyCh: Channel used to signal when the service is ready to accept requests
//
// Returns an error if the service fails to start properly, nil otherwise.
func (u *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	// Blocks until the FSM transitions from the IDLE state
	err := u.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		u.logger.Errorf("[Block Persister Service] Failed to wait for FSM transition from IDLE state: %s", err)

		return err
	}

	blockPersisterHTTPListenAddress := u.settings.Block.PersisterHTTPListenAddress

	if blockPersisterHTTPListenAddress != "" {
		blockStoreURL := u.settings.Block.BlockStore
		if blockStoreURL == nil {
			return errors.NewConfigurationError("blockstore setting error")
		}

		// Get listener using util.GetListener
		listener, address, _, err := util.GetListener(u.settings.Context, "blockpersister", "http://", blockPersisterHTTPListenAddress)
		if err != nil {
			return errors.NewServiceError("failed to get HTTP listener for block persister", err)
		}

		u.logger.Infof("[BlockPersister] HTTP server listening on %s", address)

		blobStoreServer, err := blob.NewHTTPBlobServer(u.logger, blockStoreURL)
		if err != nil {
			return errors.NewServiceError("failed to create blob store server", err)
		}

		srv := &http.Server{
			Handler:      blobStoreServer,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  60 * time.Second,
		}

		go func() {
			if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
				u.logger.Warnf("blockStoreServer ended: %v", err)
			}

			// Clean up the listener when server stops
			util.RemoveListener(u.settings.Context, "blockpersister", "http://")
		}()

		// Handle shutdown
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := srv.Shutdown(shutdownCtx); err != nil {
				u.logger.Errorf("HTTP blob server shutdown error: %v", err)
			}
		}()
	}

	// STARTUP COORDINATION: Publish last persisted height for cleanup coordination
	//
	// PROBLEM: BlockAssembler's cleanup service may start before block persister and begin
	// deleting transactions. Without knowing how far block persister has progressed, cleanup
	// could delete transactions that block persister still needs to create .subtree_data files.
	//
	// SOLUTION: Publish our last persisted height to blockchain state on startup. BlockAssembler
	// reads this state during its startup initialization, ensuring it knows the persisted height
	// before cleanup runs for the first time.
	//
	// This prevents the startup race condition where:
	//   1. BlockAssembler starts and begins cleanup
	//   2. Block persister starts later
	//   3. Cleanup deletes transactions before first BlockPersisted notification arrives
	//
	// By publishing state on startup, BlockAssembler can initialize immediately from this state.
	// During runtime, BlockPersisted notifications keep the height current.
	blocks, err := u.blockchainClient.GetBlocksNotPersisted(ctx, 1)
	if err != nil {
		u.logger.Warnf("[BlockPersister] Failed to query initial state: %v", err)
	} else if len(blocks) > 0 && blocks[0].Height > 0 {
		// GetBlocksNotPersisted returns blocks in ascending height order, so blocks[0] is the lowest unpersisted block
		lastHeight := blocks[0].Height - 1
		heightBytes := binary.LittleEndian.AppendUint32(nil, lastHeight)
		if err := u.blockchainClient.SetState(ctx, "BlockPersisterHeight", heightBytes); err != nil {
			u.logger.Warnf("[BlockPersister] Failed to publish initial state: %v", err)
		} else {
			u.logger.Infof("[BlockPersister] Published initial state: height %d", lastHeight)
		}
	}

	// Start the processing loop in a goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				u.logger.Infof("Shutting down block processing loop")
				return
			default:
				block, err := u.getNextBlockToProcess(ctx)
				if err != nil {
					u.logger.Errorf("Error getting next block to process: %v", err)
					time.Sleep(time.Minute) // Sleep after error

					continue
				}

				if block == nil {
					if u.settings.Block.BlockPersisterPersistSleep >= time.Minute {
						u.logger.Infof("No new blocks to process, waiting...")
					}

					time.Sleep(u.settings.Block.BlockPersisterPersistSleep) // Sleep when no blocks available

					continue
				}

				// Get block bytes
				blockBytes, err := block.Bytes()
				if err != nil {
					u.logger.Errorf("Failed to get block bytes: %v", err)
					time.Sleep(time.Minute)

					continue
				}

				// Process the block
				if err := u.persistBlock(ctx, block.Hash(), blockBytes); err != nil {
					if errors.Is(err, errors.NewBlobAlreadyExistsError("")) {
						// We log the error but continue processing
						u.logger.Infof("Block %s already exists, skipping...", block.Hash())
					} else {
						u.logger.Errorf("Failed to persist block %s: %v", block.Hash(), err)
						time.Sleep(time.Minute)

						continue
					}
				}

				// Mark block as persisted in database
				if err := u.blockchainClient.SetBlockPersistedAt(ctx, block.Hash()); err != nil {
					u.logger.Errorf("Failed to mark block %s as persisted: %v", block.Hash(), err)
					time.Sleep(time.Minute)

					continue
				}

				// RUNTIME COORDINATION: Notify subscribers that block has been persisted
				//
				// After successfully creating .subtree_data file, notify BlockAssembler of our progress.
				// BlockAssembler's cleanup service uses this to track how far we've progressed and
				// ensure it doesn't delete transactions we still need.
				//
				// This notification includes the block height in metadata. BlockAssembler's notification
				// handler updates its lastPersistedHeight, which cleanup queries via GetLastPersistedHeight()
				// to calculate safe deletion bounds: min(requested_height, persisted_height + retention)
				//
				// See BlockAssembler.startChannelListeners for the notification handler.
				notification := &blockchain_api.Notification{
					Type: model.NotificationType_BlockPersisted,
					Hash: block.Hash().CloneBytes(),
					Metadata: &blockchain_api.NotificationMetadata{
						Metadata: map[string]string{
							"height": fmt.Sprintf("%d", block.Height),
						},
					},
				}
				if err := u.blockchainClient.SendNotification(ctx, notification); err != nil {
					u.logger.Warnf("[BlockPersister] Failed to send persisted notification for block %s at height %d: %v",
						block.Hash().String(), block.Height, err)
				}

				// Update BlockPersisterHeight state for P2P storage mode determination
				heightBytes := binary.LittleEndian.AppendUint32(nil, block.Height)
				if err := u.blockchainClient.SetState(ctx, "BlockPersisterHeight", heightBytes); err != nil {
					u.logger.Warnf("[BlockPersister] Failed to update BlockPersisterHeight state for block %s at height %d: %v",
						block.Hash().String(), block.Height, err)
				}

				u.logger.Infof("Successfully processed block %s", block.Hash())
			}
		}
	}()

	closeOnce.Do(func() { close(readyCh) })

	<-ctx.Done()

	return nil
}

// Stop gracefully shuts down the server.
//
// This method is called when the service is being stopped and provides an opportunity
// to perform any necessary cleanup operations, such as closing connections, flushing
// buffers, or persisting state.
//
// Currently, the Server doesn't need to perform any specific cleanup actions during shutdown
// as resource cleanup is handled by the context cancellation mechanism in the Start method.
//
// Parameters:
//   - ctx: Context for controlling the shutdown operation (currently unused)
//
// Returns an error if shutdown fails, or nil on successful shutdown.
func (u *Server) Stop(_ context.Context) error {
	// Currently, the Server doesn't need to perform any action on shutdown
	return nil
}
