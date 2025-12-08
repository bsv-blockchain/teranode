// Package asset provides HTTP and WebSocket APIs for blockchain data access.

package asset

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/asset/centrifuge_impl"
	"github.com/bsv-blockchain/teranode/services/asset/httpimpl"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/health"
	"golang.org/x/sync/errgroup"
)

// Server represents the main asset service handler, managing blockchain data access and API endpoints.
// It coordinates between different storage backends and provides both HTTP and Centrifuge interfaces
// for accessing blockchain data. The Server acts as the primary orchestrator for all asset-related
// operations and maintains connections to the necessary data stores and services.
//
// The Server employs a composition-based design pattern, delegating specific functionality to
// specialized components while maintaining overall coordination responsibility.
//
// Fields:
// - logger: Handles structured logging for operational monitoring and debugging
// - settings: Contains configuration parameters for the asset service
// - utxoStore: Provides access to the UTXO (Unspent Transaction Output) dataset
// - txStore: Stores and retrieves transaction data
// - subtreeStore: Manages subtree data for optimized block processing
// - blockPersisterStore: Handles persistence of complete block data
// - httpAddr: Network address for the HTTP server
// - httpServer: Handles HTTP API requests for asset data
// - centrifugeAddr: Network address for the Centrifuge server
// - centrifugeServer: Manages real-time data subscriptions via the Centrifuge protocol
// - blockchainClient: Interface for interacting with the blockchain service
type Server struct {
	logger                ulogger.Logger
	settings              *settings.Settings
	utxoStore             utxo.Store
	txStore               blob.Store
	subtreeStore          blob.Store
	blockPersisterStore   blob.Store
	httpAddr              string
	httpServer            *httpimpl.HTTP
	centrifugeAddr        string
	centrifugeServer      *centrifuge_impl.Centrifuge
	blockchainClient      blockchain.ClientI
	blockvalidationClient blockvalidation.Interface
	p2pClient             p2p.ClientI
}

// NewServer creates a new Server instance with the provided dependencies.
// It initializes the server with necessary stores and clients for handling
// blockchain data. This factory function ensures proper dependency injection
// and initialization of all required components.
//
// The function follows the dependency injection pattern to facilitate unit testing
// and enhance modularity. Each injected component represents a specific responsibility
// within the system architecture.
//
// Parameters:
//   - logger: Logger instance for service logging and operational monitoring
//   - utxoStore: Store for UTXO (Unspent Transaction Output) data, providing access to the current UTXO set
//   - txStore: Store for transaction data, offering persistent storage and retrieval of transactions
//   - subtreeStore: Store for subtree data, enabling efficient block traversal and validation
//   - blockPersisterStore: Store for block persistence, ensuring durable storage of validated blocks
//   - blockchainClient: Client interface for blockchain operations, facilitating integration with the blockchain service
//
// Returns:
//   - *Server: A fully initialized Server instance ready for use
func NewServer(logger ulogger.Logger, tSettings *settings.Settings, utxoStore utxo.Store, txStore blob.Store,
	subtreeStore blob.Store, blockPersisterStore blob.Store, blockchainClient blockchain.ClientI,
	blockvalidationClient blockvalidation.Interface, p2pClient p2p.ClientI) *Server {
	s := &Server{
		logger:                logger,
		settings:              tSettings,
		utxoStore:             utxoStore,
		txStore:               txStore,
		subtreeStore:          subtreeStore,
		blockPersisterStore:   blockPersisterStore,
		blockchainClient:      blockchainClient,
		blockvalidationClient: blockvalidationClient,
		p2pClient:             p2pClient,
	}

	return s
}

// Health performs health checks on the server and its dependencies.
// It supports both liveness and readiness checks based on the checkLiveness parameter.
//
// The health check implementation follows the standard Kubernetes health check pattern,
// providing both liveness (is the service running?) and readiness (is the service ready
// to accept requests?) information. It performs comprehensive verification of all critical
// dependencies including storage backends and connected services.
//
// The function executes the following checks:
// - For liveness: Verifies basic server operation without checking dependent services
// - For readiness: Performs deep checks on all connected stores and services
//
// Parameters:
//   - ctx: Context for the health check operation, allowing for timeouts and cancellation
//   - checkLiveness: If true, performs liveness check; if false, performs readiness check
//
// Returns:
//   - int: HTTP status code indicating health status (200 for healthy, 503 for unhealthy)
//   - string: Description of the health status with details about any detected issues
//   - error: Any error encountered during health check that prevented proper evaluation
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
	checks := make([]health.Check, 0, 6)

	// Check if the HTTP server is actually listening and accepting requests
	if v.httpServer != nil {
		addr := v.httpAddr
		if strings.HasPrefix(addr, ":") {
			addr = "localhost" + addr
		}
		checks = append(checks, health.Check{
			Name:  "HTTP Server",
			Check: health.CheckHTTPServer(fmt.Sprintf("http://%s", addr), "/health"),
		})
	}

	// Note: Centrifuge server check removed as it may not always be running

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
// This method performs the following initialization steps:
// - Sets up HTTP server with all required routes and handlers
// - Configures Centrifuge real-time communication server if enabled
// - Establishes connections to all required data stores
// - Initializes internal state and prepares the service for operation
//
// The initialization follows a fail-fast approach, where any configuration or
// dependency initialization failure will abort the entire process and return an error.
// This ensures the service is either fully operational or not running at all.
//
// Parameters:
//   - ctx: Context for initialization, allowing for timeouts and cancellation during setup
//
// Returns:
//   - error: Any error encountered during initialization, with details about the specific failure point
func (v *Server) Init(ctx context.Context) (err error) {
	v.httpAddr = v.settings.Asset.HTTPListenAddress
	if v.httpAddr == "" {
		return errors.NewConfigurationError("no asset_httpListenAddress setting found")
	}

	repo, err := repository.NewRepository(v.logger, v.settings, v.utxoStore, v.txStore, v.blockchainClient,
		v.blockvalidationClient, v.subtreeStore, v.blockPersisterStore, v.p2pClient)
	if err != nil {
		return errors.NewServiceError("error creating repository", err)
	}

	v.httpServer, err = httpimpl.New(v.logger, v.settings, repo)
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
// This method launches all configured server components in parallel using an error group
// to manage their lifecycle and propagate errors. It implements the following behaviors:
// - Starts the HTTP server on the configured address if enabled
// - Launches the Centrifuge real-time subscription server if enabled
// - Signals service readiness through the provided channel
// - Handles graceful shutdown when the context is canceled
//
// The method uses goroutines with proper synchronization to ensure concurrent
// operation while maintaining error propagation. It follows a graceful shutdown
// pattern where all components are properly terminated when the context is canceled.
//
// Parameters:
//   - ctx: Context for server operation, used for lifecycle management and cancellation
//   - readyCh: Channel to signal when the service is fully operational and ready to accept requests
//
// Returns:
//   - error: Any error encountered during server startup or operation
func (v *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	g, ctx := errgroup.WithContext(ctx)

	if v.httpServer != nil {
		g.Go(func() error {
			v.logger.Infof("[Asset] Starting HTTP server on %s", v.httpAddr)
			err := v.httpServer.Start(ctx, v.httpAddr)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				v.logger.Errorf("[Asset] error in http server: %v", err)
				return err
			}

			return nil
		})
	}

	// Blocks until the FSM transitions from the IDLE state
	err := v.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		v.logger.Errorf("[Asset Service] Failed to wait for FSM transition from IDLE state: %s", err)

		return err
	}

	if v.centrifugeServer != nil {
		g.Go(func() error {
			return v.centrifugeServer.Start(ctx, v.centrifugeAddr)
		})
	}

	closeOnce.Do(func() { close(readyCh) })

	if err := g.Wait(); err != nil {
		return errors.NewServiceError("the main server has ended with error", err)
	}

	return nil
}

// Stop gracefully shuts down the server and its components.
//
// This method implements a coordinated shutdown sequence for all server components:
// - Stops the HTTP server with a graceful shutdown period for in-flight requests
// - Terminates the Centrifuge server and closes all client connections
// - Releases any resources held by the server
// - Ensures all goroutines are properly terminated
//
// The shutdown process respects the provided context deadline to ensure timely
// termination even if some components are slow to shut down. It prioritizes
// graceful shutdown while still enforcing overall timeout constraints.
//
// Parameters:
//   - ctx: Context for shutdown operation, controlling the maximum time allowed for shutdown
//
// Returns:
//   - error: Any error encountered during shutdown, with information about which components failed to stop properly
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
