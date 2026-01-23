// Package pruner provides the Pruner Service which handles periodic pruning of unmined transaction
// parents and delete-at-height (DAH) records in the UTXO store.
//
// Trigger mechanism (event-driven):
// The pruning trigger mode is controlled by pruner_block_trigger setting:
//   - "OnBlockPersisted" (default): Pruning triggers on BlockPersisted notifications from block persister.
//     If block persister is not running, automatically falls back to Block notifications.
//   - "OnBlockMined": Pruning triggers on Block notifications after block has mined_set=true,
//     regardless of block persister status.
//
// Pruner operations only execute when safe to do so (i.e., when block assembly is in "running"
// state and not performing reorgs or resets).
package pruner

import (
	"context"
	"encoding/binary"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/pruner/pruner_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
)

// Server implements the Pruner service which handles periodic pruner operations
// for the UTXO store. It uses event-driven triggers: BlockPersisted notifications (primary)
// and Block notifications with mined_set wait (fallback when persister not running or forced via setting).
type Server struct {
	pruner_api.UnsafePrunerAPIServer

	// Dependencies (injected via constructor)
	ctx                 context.Context
	logger              ulogger.Logger
	settings            *settings.Settings
	utxoStore           utxo.Store
	blockchainClient    blockchain.ClientI
	blockAssemblyClient blockassembly.ClientI

	// Internal state
	prunerService       pruner.Service
	lastProcessedHeight atomic.Uint32
	lastPersistedHeight atomic.Uint32
	prunerCh            chan uint32
	stats               *gocore.Stat
}

// New creates a new Pruner server instance with the provided dependencies.
// This function initializes the server but does not start any background processes.
// Call Init() and then Start() to begin operation.
func New(
	ctx context.Context,
	logger ulogger.Logger,
	tSettings *settings.Settings,
	utxoStore utxo.Store,
	blockchainClient blockchain.ClientI,
	blockAssemblyClient blockassembly.ClientI,
) *Server {
	return &Server{
		ctx:                 ctx,
		logger:              logger,
		settings:            tSettings,
		utxoStore:           utxoStore,
		blockchainClient:    blockchainClient,
		blockAssemblyClient: blockAssemblyClient,
		stats:               gocore.NewStat("pruner"),
	}
}

// Init initializes the pruner service. This is called before Start() and is responsible
// for setting up the pruner service provider from the UTXO store and subscribing to
// block persisted notifications for coordination with the block persister service.
func (s *Server) Init(ctx context.Context) error {
	s.ctx = ctx

	// Initialize metrics
	initPrometheusMetrics()

	// Initialize pruner service from UTXO store
	prunerProvider, ok := s.utxoStore.(pruner.PrunerServiceProvider)
	if !ok {
		return errors.NewServiceError("UTXO store does not provide pruner service")
	}

	var err error
	s.prunerService, err = prunerProvider.GetPrunerService()
	if err != nil {
		return errors.NewServiceError("failed to get pruner service", err)
	}
	if s.prunerService == nil {
		return errors.NewServiceError("pruner service not available from UTXO store")
	}

	// Set persisted height getter for block persister coordination
	s.prunerService.SetPersistedHeightGetter(s.GetLastPersistedHeight)

	// Validate block trigger mode
	blockTrigger := s.settings.Pruner.BlockTrigger
	if blockTrigger != settings.PrunerBlockTriggerOnBlockPersisted && blockTrigger != settings.PrunerBlockTriggerOnBlockMined {
		return errors.NewConfigurationError("pruner_block_trigger must be either '%s' or '%s' (got '%s')",
			settings.PrunerBlockTriggerOnBlockPersisted, settings.PrunerBlockTriggerOnBlockMined, blockTrigger)
	}

	// Subscribe to blockchain notifications for event-driven pruning:
	// - BlockPersisted: Triggers pruning when block persister completes (primary)
	// - Block: Waits for mined_set=true and triggers if persister not running (fallback)
	// Also tracks persisted height for coordination with store-level pruner safety checks
	subscriptionCh, err := s.blockchainClient.Subscribe(ctx, "Pruner")
	if err != nil {
		return errors.NewServiceError("failed to subscribe to blockchain notifications", err)
	}

	// Start a goroutine to handle blockchain notifications
	go func() {
		for notification := range subscriptionCh {
			switch notification.Type {
			case model.NotificationType_BlockPersisted:
				// Skip BlockPersisted notifications if using OnBlockMined trigger mode
				if s.settings.Pruner.BlockTrigger == settings.PrunerBlockTriggerOnBlockMined {
					s.logger.Debugf("Ignoring BlockPersisted notification (pruner_block_trigger=%s)", s.settings.Pruner.BlockTrigger)
					continue
				}

				// Track persisted height for coordination with block persister
				if notification.Metadata != nil && notification.Metadata.Metadata != nil {
					if heightStr, ok := notification.Metadata.Metadata["height"]; ok {
						if height, err := strconv.ParseUint(heightStr, 10, 32); err == nil {
							height32 := uint32(height)
							oldHeight := s.lastPersistedHeight.Swap(height32)

							// Log at INFO level when Block Persister first becomes active (transition from 0)
							if oldHeight == 0 && height32 > 0 {
								s.logger.Infof("Block Persister is now active, pruner will follow BlockPersisted notifications (persisted height: %d)", height32)
							} else {
								s.logger.Debugf("Updated persisted height to %d", height32)
							}

							// Trigger pruning when block persister completes a block
							if height32 > s.lastProcessedHeight.Load() {
								// Try to queue pruning (non-blocking - channel has buffer of 1)
								select {
								case s.prunerCh <- height32:
									s.logger.Infof("Queued pruning for height %d from BlockPersisted notification", height32)
								default:
									s.logger.Warnf("Pruner channel full, skipping height %d (pruner already running)", height32)
								}
							}
						}
					}
				}

			case model.NotificationType_Block:
				// Fallback trigger: if block persister is not running, wait for block to have mined_set=true
				// Check if we should ignore block persister height
				persistedHeight := s.lastPersistedHeight.Load()

				// Skip Block notifications if using OnBlockPersisted trigger mode AND persister is running
				if s.settings.Pruner.BlockTrigger == settings.PrunerBlockTriggerOnBlockPersisted {
					s.logger.Debugf("Block notification received but pruner configured for BlockPersisted trigger (persisted height: %d)", persistedHeight)
					continue
				}

				// Block persister not running - wait for block to have mined_set=true before triggering
				if notification.Hash == nil {
					s.logger.Debugf("Block notification missing hash, skipping")
					continue
				}

				blockHash, err := chainhash.NewHash(notification.Hash)
				if err != nil {
					s.logger.Debugf("Failed to parse block hash from notification: %v", err)
					continue
				}

				// Wait for block to have mined_set=true (block validation completed)
				if !s.waitForBlockMinedStatus(ctx, blockHash) {
					continue
				}

				// Block has mined_set=true, get its height and trigger pruning
				state, err := s.blockAssemblyClient.GetBlockAssemblyState(ctx)
				if err != nil {
					s.logger.Debugf("Failed to get block assembly state on Block notification: %v", err)
					continue
				}

				if state.CurrentHeight > s.lastProcessedHeight.Load() {
					// Try to queue pruning (non-blocking - channel has buffer of 1)
					select {
					case s.prunerCh <- state.CurrentHeight:
						s.logger.Infof("Queued pruning for height %d from Block notification (mined_set=true)", state.CurrentHeight)
					default:
						s.logger.Warnf("Pruner channel full, skipping height %d (pruner already running)", state.CurrentHeight)
					}
				}
			}
		}
	}()

	// Read initial persisted height from blockchain state
	if state, err := s.blockchainClient.GetState(ctx, "BlockPersisterHeight"); err == nil && len(state) >= 4 {
		height := binary.LittleEndian.Uint32(state)
		s.lastPersistedHeight.Store(height)
		s.logger.Infof("Loaded initial block persister height: %d", height)
	}

	return nil
}

// Start begins the pruner service operation. It starts the pruner processor goroutine,
// then starts the gRPC server. Pruning is triggered by BlockPersisted and Block notifications.
// This function blocks until the server shuts down or encounters an error.
func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	// Wait for blockchain FSM to be ready
	err := s.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		return err
	}

	// Initialize pruner channel (buffer of 1 to prevent blocking while ensuring only one pruner)
	s.prunerCh = make(chan uint32, 1)

	// Start the pruner service (Aerospike or SQL)
	if s.prunerService != nil {
		s.prunerService.Start(ctx)
	}

	// Start pruner processor goroutine
	go s.prunerProcessor(ctx)

	// Note: Polling worker not needed - pruning is triggered by:
	// 1. BlockPersisted notifications (when block persister is running)
	// 2. Block notifications with mined_set wait (when persister not running)

	// Start gRPC server (BLOCKING - must be last)
	if err := util.StartGRPCServer(ctx, s.logger, s.settings, "pruner",
		s.settings.Pruner.GRPCListenAddress,
		func(server *grpc.Server) {
			pruner_api.RegisterPrunerAPIServer(server, s)
			closeOnce.Do(func() { close(readyCh) })
		}, nil); err != nil {
		return err
	}

	return nil
}

// Stop gracefully shuts down the pruner service. Context cancellation will stop
// the polling worker and pruner processor goroutines.
func (s *Server) Stop(ctx context.Context) error {
	// Stop the pruner service if it has a Stop method
	if s.prunerService != nil {
		// Check if the pruner service implements Stop
		// Aerospike has Stop, SQL doesn't
		type stopper interface {
			Stop(ctx context.Context) error
		}
		if stoppable, ok := s.prunerService.(stopper); ok {
			if err := stoppable.Stop(ctx); err != nil {
				s.logger.Errorf("Error stopping pruner service: %v", err)
			}
		}
	}

	// Context cancellation will stop goroutines
	s.logger.Infof("Pruner service stopped")
	return nil
}

// Health implements the health check for the pruner service. When checkLiveness is true,
// it only checks if the service process is running. When false, it checks all dependencies
// including gRPC server, block assembly client, blockchain client, and UTXO store.
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// LIVENESS: Is the service process running?
		return http.StatusOK, "OK", nil
	}

	// READINESS: Can the service handle requests?
	checks := make([]health.Check, 0, 5)

	// Check gRPC server is listening
	if s.settings.Pruner.GRPCListenAddress != "" {
		checks = append(checks, health.Check{
			Name: "gRPC Server",
			Check: health.CheckGRPCServerWithSettings(s.settings.Pruner.GRPCListenAddress, s.settings, func(ctx context.Context, conn *grpc.ClientConn) error {
				// Simple connection check - if we can create a client, server is up
				return nil
			}),
		})
	}

	// Check block assembly client
	if s.blockAssemblyClient != nil {
		checks = append(checks, health.Check{
			Name:  "BlockAssemblyClient",
			Check: s.blockAssemblyClient.Health,
		})
	}

	// Check blockchain client
	if s.blockchainClient != nil {
		checks = append(checks, health.Check{
			Name:  "BlockchainClient",
			Check: s.blockchainClient.Health,
		})
		checks = append(checks, health.Check{
			Name:  "FSM",
			Check: blockchain.CheckFSM(s.blockchainClient),
		})
	}

	// Check UTXO store
	if s.utxoStore != nil {
		checks = append(checks, health.Check{
			Name:  "UTXOStore",
			Check: s.utxoStore.Health,
		})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC implements the gRPC health check endpoint.
func (s *Server) HealthGRPC(ctx context.Context, _ *pruner_api.EmptyMessage) (*pruner_api.HealthResponse, error) {
	// Add context value to prevent circular dependency when checking gRPC server
	ctx = context.WithValue(ctx, "skip-grpc-self-check", true)

	status, details, err := s.Health(ctx, false)

	return &pruner_api.HealthResponse{
		Ok:      status == http.StatusOK,
		Details: details,
	}, errors.WrapGRPC(err)
}

// GetLastPersistedHeight returns the last known block height that has been persisted
// by the block persister service. This is used to coordinate cleanup operations to
// avoid deleting data that the block persister still needs.
func (s *Server) GetLastPersistedHeight() uint32 {
	return s.lastPersistedHeight.Load()
}
