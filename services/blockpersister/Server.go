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

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/blockpersister/state"
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

	// state manages the persister's internal state, tracking which blocks have been
	// successfully persisted and allowing for recovery after interruptions
	state *state.State
}

// WithSetInitialState is an optional configuration function that sets the initial state
// of the block persister server. This can be used during initialization to establish
// a known starting point for block persistence operations.
//
// Parameters:
//   - height: The blockchain height to set as the initial state
//   - hash: The block hash corresponding to the specified height
//
// Returns a function that, when called with a Server instance, will set the initial state
// of that server. If the state cannot be set, an error is logged but not returned.
func WithSetInitialState(height uint32, hash *chainhash.Hash) func(*Server) {
	return func(s *Server) {
		if err := s.state.AddBlock(height, hash.String()); err != nil {
			s.logger.Errorf("Failed to set initial state: %v", err)
		}
	}
}

// New creates a new block persister server instance with the provided dependencies.
//
// This constructor initializes all components required for block persistence operations,
// including stores, state management, and client connections. It accepts optional
// configuration functions to customize the server instance after construction.
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
	// Get blocks file path from config, or use default
	state := state.New(logger, tSettings.Block.StateFile)

	u := &Server{
		ctx:              ctx,
		logger:           logger,
		settings:         tSettings,
		blockStore:       blockStore,
		subtreeStore:     subtreeStore,
		utxoStore:        utxoStore,
		stats:            gocore.NewStat("blockpersister"),
		blockchainClient: blockchainClient,
		state:            state,
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

// getNextBlockToProcess retrieves the next block that needs to be processed
// based on the current state and configuration.
//
// This method determines the next block to persist by comparing the last persisted block height
// with the current blockchain tip. It ensures blocks are persisted in sequence without gaps
// and respects the configured persistence age policy to control how far behind persistence
// can lag.
//
// The method includes reorg detection by verifying that the last persisted block is still
// on the current chain, and validates parent hash continuity when retrieving the next block.
//
// Parameters:
//   - ctx: Context for coordinating the block retrieval operation
//
// Returns:
//   - *model.Block: The next block to process, or nil if no block needs processing yet
//   - error: Any error encountered during the operation
//
// The method follows these steps:
//  1. Get the last persisted block height and hash from the state
//  2. If we have a persisted block, check if it's still on the current chain (reorg detection)
//  3. Get the current best block from the blockchain
//  4. If the difference between them exceeds BlockPersisterPersistAge, return the next block
//  5. Validate that the retrieved block's parent hash matches the last persisted block hash
//  6. Otherwise, return nil to indicate no blocks need processing yet
func (u *Server) getNextBlockToProcess(ctx context.Context) (*model.Block, error) {
	lastPersistedHeight, lastPersistedHash, err := u.state.GetLastPersistedBlock()
	if err != nil {
		return nil, errors.NewProcessingError("failed to get last persisted block", err)
	}

	// REORG DETECTION: Check if the last persisted block is still on the current chain
	// This detects blockchain reorganizations that may have occurred since the last persistence
	// This check can be disabled via settings for testing or special scenarios
	if u.settings.Block.BlockPersisterEnableDefensiveReorgCheck && lastPersistedHeight > 0 && lastPersistedHash != nil {
		// Get the block to obtain its ID
		lastBlock, err := u.blockchainClient.GetBlock(ctx, lastPersistedHash)
		if err != nil {
			u.logger.Warnf("[BlockPersister] Could not retrieve last persisted block %s for reorg check: %v. Continuing with next block.", lastPersistedHash.String(), err)
		} else {
			// Check if this block is still on the current chain using its ID
			onCurrentChain, err := u.blockchainClient.CheckBlockIsInCurrentChain(ctx, []uint32{lastBlock.ID})
			if err != nil {
				return nil, errors.NewProcessingError("failed to check if block is on current chain", err)
			}

			if !onCurrentChain {
				// Reorg detected - last persisted block is no longer on the current chain
				// We must return nil to trigger recovery logic - continuing would cause an infinite loop
				// because the parent hash validation will fail (new chain block's parent != orphaned block)
				u.logger.Infof("[BlockPersister] Detected reorg: last persisted block %s at height %d is no longer on current chain. Returning nil to trigger recovery.",
					lastPersistedHash.String(), lastPersistedHeight)
				// Return nil to prevent infinite loop - manual intervention or proper reorg recovery needed
				return nil, nil
			}
		}
	}

	_, blockMeta, err := u.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get best block header", err)
	}

	if blockMeta.Height > lastPersistedHeight+u.settings.Block.BlockPersisterPersistAge {
		block, err := u.blockchainClient.GetBlockByHeight(ctx, lastPersistedHeight+1)
		if err != nil {
			return nil, errors.NewProcessingError("failed to get block headers by height", err)
		}

		// PARENT HASH VALIDATION: Verify chain continuity
		// This provides early detection of chain inconsistencies or reorgs that occurred
		// between the reorg check above and this block retrieval
		// This check can be disabled via settings for testing or special scenarios
		if u.settings.Block.BlockPersisterEnableDefensiveReorgCheck && lastPersistedHeight > 0 && lastPersistedHash != nil {
			// Get the parent hash from the retrieved block's header
			parentHash := block.Header.HashPrevBlock
			if !parentHash.IsEqual(lastPersistedHash) {
				u.logger.Infof("[BlockPersister] Chain discontinuity detected: block %s at height %d has parent %s, expected %s. Reorg likely occurred during processing. Will retry and recover on next iteration.",
					block.Hash().String(), block.Height, parentHash.String(), lastPersistedHash.String())
				// Return nil to skip this block and retry on next iteration
				// GetBlockByHeight will return the correct block from the current chain on the next attempt
				return nil, nil
			}
		}

		return block, nil
	}

	return nil, nil
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
	if lastHeight, err := u.state.GetLastPersistedBlockHeight(); err == nil && lastHeight > 0 {
		if u.blockchainClient != nil {
			heightBytes := binary.LittleEndian.AppendUint32(nil, lastHeight)
			if err := u.blockchainClient.SetState(ctx, "BlockPersisterHeight", heightBytes); err != nil {
				u.logger.Warnf("[BlockPersister] Failed to publish initial state: %v", err)
			} else {
				u.logger.Infof("[BlockPersister] Published initial state: height %d", lastHeight)
			}
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

				// Add this after successful persistence
				if err := u.state.AddBlock(block.Height, block.Hash().String()); err != nil {
					u.logger.Errorf("Failed to record block %s: %v", block.Hash(), err)
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
				if u.blockchainClient != nil {
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
