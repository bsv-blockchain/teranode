// Package blockpersister provides functionality for persisting blockchain blocks and their associated data.
package blockpersister

import (
	"context"
	"net/http"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockpersister/state"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

// Server represents the main block persister service that handles block storage and processing.
type Server struct {
	// ctx is the context for controlling server lifecycle
	ctx context.Context

	// logger provides logging functionality
	logger ulogger.Logger

	// settings contains configuration settings for the server
	settings *settings.Settings

	// blockStore provides storage for blocks
	blockStore blob.Store

	// subtreeStore provides storage for block subtrees
	subtreeStore blob.Store

	// utxoStore provides storage for UTXO data
	utxoStore utxo.Store

	// stats tracks server statistics
	stats *gocore.Stat

	// blockchainClient interfaces with the blockchain
	blockchainClient blockchain.ClientI

	// state manages the persister's internal state
	state *state.State
}

// WithSetInitialState is an optional configuration function that sets the initial state
// of the block persister server.
func WithSetInitialState(height uint32, hash *chainhash.Hash) func(*Server) {
	return func(s *Server) {
		if err := s.state.AddBlock(height, hash.String()); err != nil {
			s.logger.Errorf("Failed to set initial state: %v", err)
		}
	}
}

// New creates a new block persister server instance with the provided dependencies.
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
// If checkLiveness is true, only liveness checks are performed.
// Returns HTTP status code, status message, and any error encountered.
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
func (u *Server) Init(ctx context.Context) (err error) {
	initPrometheusMetrics()

	return nil
}

// getNextBlockToProcess retrieves the next block that needs to be processed
// based on the current state and configuration.
func (u *Server) getNextBlockToProcess(ctx context.Context) (*model.Block, error) {
	lastPersistedHeight, err := u.state.GetLastPersistedBlockHeight()
	if err != nil {
		return nil, errors.NewProcessingError("failed to get last persisted block height", err)
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

		return block, nil
	}

	return nil, nil
}

// Start begins the block processing loop and runs until the context is cancelled.
func (u *Server) Start(ctx context.Context) error {
	// Blocks until the FSM transitions from the IDLE state
	err := u.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		u.logger.Errorf("[Block Persister Service] Failed to wait for FSM transition from IDLE state: %s", err)

		return err
	}

	blockPersisterHTTPListenAddress := u.settings.Block.PersisterHTTPListenAddress

	if blockPersisterHTTPListenAddress == "" {
		blockStoreURL := u.settings.Block.BlockStore
		if blockStoreURL == nil {
			return errors.NewConfigurationError("blockstore setting error")
		}

		blobStoreServer, err := blob.NewHTTPBlobServer(u.logger, blockStoreURL)
		if err != nil {
			return errors.NewServiceError("failed to create blob store server", err)
		}

		go func() {
			u.logger.Warnf("blockStoreServer ended: %v", blobStoreServer.Start(ctx, blockPersisterHTTPListenAddress))
		}()
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
					u.logger.Errorf("Failed to persist block %s: %v", block.Hash(), err)
					time.Sleep(time.Minute)

					continue
				}

				// Add this after successful persistence
				if err := u.state.AddBlock(block.Height, block.Hash().String()); err != nil {
					u.logger.Errorf("Failed to record block %s: %v", block.Hash(), err)
					time.Sleep(time.Minute)

					continue
				}

				u.logger.Infof("Successfully processed block %s", block.Hash())
			}
		}
	}()

	<-ctx.Done()

	return nil
}

// Stop gracefully shuts down the server.
func (u *Server) Stop(_ context.Context) error {
	return nil
}
