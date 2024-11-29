// Package asset provides functionality for managing and querying Teranode Bitcoin SV blockchain assets.
// It implements a server that handles both the HTTP and the Centrifuge protocol for asset-related operations.

package asset

import (
	"context"
	"net/http"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/asset/centrifuge_impl"
	"github.com/bitcoin-sv/ubsv/services/asset/http_impl"
	"github.com/bitcoin-sv/ubsv/services/asset/repository"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/health"
	"golang.org/x/sync/errgroup"
)

// Server represents the main asset service handler, managing blockchain data access and API endpoints.
// It coordinates between different storage backends and provides both HTTP and Centrifuge interfaces
// for accessing blockchain data.
type Server struct {
	logger              ulogger.Logger
	settings            *settings.Settings
	utxoStore           utxo.Store
	txStore             blob.Store
	subtreeStore        blob.Store
	blockPersisterStore blob.Store
	httpAddr            string
	httpServer          *http_impl.HTTP
	centrifugeAddr      string
	centrifugeServer    *centrifuge_impl.Centrifuge
	blockchainClient    blockchain.ClientI
}

// NewServer creates a new Server instance with the provided dependencies.
// It initializes the server with necessary stores and clients for handling
// blockchain data.
//
// Parameters:
//   - logger: Logger instance for service logging
//   - utxoStore: Store for UTXO (Unspent Transaction Output) data
//   - txStore: Store for transaction data
//   - subtreeStore: Store for subtree data
//   - blockPersisterStore: Store for block persistence
//   - blockchainClient: Client interface for blockchain operations
//
// Returns:
//   - *Server: Newly created server instance
func NewServer(logger ulogger.Logger, tSettings *settings.Settings, utxoStore utxo.Store, txStore blob.Store, subtreeStore blob.Store, blockPersisterStore blob.Store, blockchainClient blockchain.ClientI) *Server {
	s := &Server{
		logger:              logger,
		settings:            tSettings,
		utxoStore:           utxoStore,
		txStore:             txStore,
		subtreeStore:        subtreeStore,
		blockPersisterStore: blockPersisterStore,
		blockchainClient:    blockchainClient,
	}

	return s
}

// Health performs health checks on the server and its dependencies.
// It supports both liveness and readiness checks based on the checkLiveness parameter.
//
// Parameters:
//   - ctx: Context for the health check operation
//   - checkLiveness: If true, performs liveness check; if false, performs readiness check
//
// Returns:
//   - int: HTTP status code indicating health status
//   - string: Description of the health status
//   - error: Any error encountered during health check
func (v *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
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

	if v.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: v.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(v.blockchainClient)})
	}

	if v.utxoStore != nil {
		checks = append(checks, health.Check{Name: "UTXOStore", Check: v.utxoStore.Health})
	}

	if v.txStore != nil {
		checks = append(checks, health.Check{Name: "TxStore", Check: v.txStore.Health})
	}

	if v.blockPersisterStore != nil {
		checks = append(checks, health.Check{Name: "BlockPersisterStore", Check: v.blockPersisterStore.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// Init initializes the server by setting up HTTP and Centrifuge endpoints.
// It configures the necessary components based on the provided configuration.
//
// Parameters:
//   - ctx: Context for initialization
//
// Returns:
//   - error: Any error encountered during initialization
func (v *Server) Init(ctx context.Context) (err error) {

	v.httpAddr = v.settings.Asset.HTTPListenAddress
	if v.httpAddr == "" {
		return errors.NewConfigurationError("no asset_httpListenAddress setting found")
	}

	repo, err := repository.NewRepository(v.logger, v.settings, v.utxoStore, v.txStore, v.blockchainClient, v.subtreeStore, v.blockPersisterStore)
	if err != nil {
		return errors.NewServiceError("error creating repository", err)
	}

	v.httpServer, err = http_impl.New(v.logger, v.settings, repo)
	if err != nil {
		return errors.NewServiceError("error creating http server", err)
	}

	err = v.httpServer.Init(ctx)
	if err != nil {
		return errors.NewServiceError("error initializing http server", err)
	}

	if !v.settings.Asset.CentrifugeDisable {
		v.centrifugeAddr = v.settings.Asset.CentrifugeListenAddress
		if v.centrifugeAddr != "" && v.httpServer != nil {
			v.centrifugeServer, err = centrifuge_impl.New(v.logger, v.settings, repo, v.httpServer)
			if err != nil {
				return errors.NewServiceError("error creating centrifuge server: %s", err)
			}

			err = v.centrifugeServer.Init(ctx)
			if err != nil {
				return errors.NewServiceError("error initializing centrifuge server: %s", err)
			}
		}
	}

	return nil
}

// Start begins the server operation, launching HTTP and Centrifuge servers
// if configured. It also handles FSM state restoration if required.
//
// Parameters:
//   - ctx: Context for server operation
//
// Returns:
//   - error: Any error encountered during server startup
func (v *Server) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	if v.settings.BlockChain.FSMStateRestore {
		// Send Restore event to FSM
		err := v.blockchainClient.Restore(ctx)
		if err != nil {
			v.logger.Errorf("[Asset] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		v.logger.Infof("[Asset] Node is restoring, waiting for FSM to transition to Running state")
		_ = v.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain.FSMStateRUNNING)
		v.logger.Infof("[Asset] Node finished restoring and has transitioned to Running state, continuing to start Asset service")
	}

	if v.httpServer != nil {
		g.Go(func() error {
			err := v.httpServer.Start(ctx, v.httpAddr)
			if err != nil {
				v.logger.Errorf("[Asset] error in http server: %v", err)
			}
			return err
		})

	}

	if v.centrifugeServer != nil {
		g.Go(func() error {
			return v.centrifugeServer.Start(ctx, v.centrifugeAddr)
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewServiceError("the main server has ended with error: %w", err)
	}

	return nil
}

// Stop gracefully shuts down the server and its components.
//
// Parameters:
//   - ctx: Context for shutdown operation
//
// Returns:
//   - error: Any error encountered during shutdown
func (v *Server) Stop(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if v.httpServer != nil {
		v.logger.Infof("[Asset] Stopping http server")
		if err := v.httpServer.Stop(ctx); err != nil {
			v.logger.Errorf("[Asset] error stopping http server: %v", err)
		}
	}

	return nil
}
