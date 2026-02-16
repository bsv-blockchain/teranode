// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-alert-system/app/config"
	"github.com/bsv-blockchain/go-alert-system/app/models"
	"github.com/bsv-blockchain/go-alert-system/app/models/model"
	"github.com/bsv-blockchain/go-alert-system/app/p2p"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/alert/alert_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	p2pservice "github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/mrz1836/go-datastore"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server represents the main alert system server structure.
// The Server is the central component of the alert service, implementing the gRPC API
// that external systems use to interact with the alert system. It manages connections
// to all required subsystems and handles the lifecycle of the alert service.
//
// The Server integrates with multiple Teranode components including blockchain,
// UTXO store, and peer systems to implement alert system functionality such as
// fund blacklisting, transaction whitelisting, and peer banning. It also maintains
// a persistent store of alert-related data and runs a peer-to-peer communication subsystem
// to distribute alerts across the network.
//
// The Server implements the alert_api.AlertAPIServer interface, allowing it to serve gRPC
// requests for alert system functions defined in the API protobuf files.
type Server struct {
	// UnimplementedAlertAPIServer is embedded for forward compatibility with the alert API,
	// ensuring that new methods added to the API interface won't break existing implementations
	alert_api.UnimplementedAlertAPIServer

	// logger handles all logging operations across the alert service,
	// providing consistent and configurable logging capabilities
	logger ulogger.Logger

	// settings contains the server configuration settings from Teranode's
	// global configuration system, controlling the behavior of the alert service
	settings *settings.Settings

	// stats tracks operational statistics for monitoring and diagnostic purposes,
	// recording metrics such as request counts and response times
	stats *gocore.Stat

	// blockchainClient provides access to blockchain operations such as
	// retrieving block information and invalidating blocks
	blockchainClient blockchain.ClientI

	// peerClient provides access to legacy peer operations,
	// including banning and unbanning network peers
	peerClient peer.ClientI

	// p2pClient provides access to p2p network operations,
	// allowing for communication with other nodes in the network
	p2pClient p2pservice.ClientI

	// utxoStore manages UTXO operations, providing the ability to
	// mark UTXOs as locked for blacklisting or whitelisting
	utxoStore utxo.Store

	// blockassemblyClient handles block assembly operations,
	// allowing the alert system to influence block creation when necessary
	blockassemblyClient blockassembly.ClientI

	// appConfig contains alert system specific configuration loaded from
	// the alert system configuration file, separate from Teranode settings
	appConfig *config.Config

	// p2pServer manages peer-to-peer communication for distributing
	// alerts across the network to other alert system nodes
	p2pServer *p2p.Server
}

// New creates and returns a new Server instance with the provided dependencies.
// This factory function initializes an alert system server with all necessary dependencies
// injected, making it highly testable and configurable. It initializes Prometheus metrics
// and creates a statistics tracker for monitoring the server.
//
// Parameters:
//   - logger: Logger for server operations
//   - tSettings: Teranode configuration settings
//   - blockchainClient: Interface for blockchain operations
//   - utxoStore: Store for UTXO operations
//   - blockassemblyClient: Client for block assembly operations
//   - peerClient: Client for legacy peer operations
//   - p2pClient: Client for P2P network operations
//
// Returns:
//   - *Server: A new Server instance configured with the provided dependencies,
//     but not yet initialized or started
func New(logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, utxoStore utxo.Store, blockassemblyClient blockassembly.ClientI, peerClient peer.ClientI, p2pClient p2pservice.ClientI) *Server {
	initPrometheusMetrics()

	return &Server{
		logger:              logger,
		settings:            tSettings,
		stats:               gocore.NewStat("alert"),
		blockchainClient:    blockchainClient,
		utxoStore:           utxoStore,
		blockassemblyClient: blockassemblyClient,
		peerClient:          peerClient,
		p2pClient:           p2pClient,
	}
}

// Health performs health checks on the server and its dependencies.
// This method implements the standard Teranode health check protocol, supporting both
// liveness and readiness probes. Liveness checks verify that the service is running and
// responsive, while readiness checks also verify that dependencies are available.
//
// The health check increments the prometheusHealth counter for monitoring purposes
// and performs checks on key dependencies such as the blockchain client, UTXO store,
// and P2P systems when checking readiness.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//   - checkLiveness: When true, only check if the service itself is alive;
//     when false, also check if dependencies are available and ready
//
// Returns:
//   - int: HTTP status code indicating health status (200 for healthy, 503 for unhealthy)
//   - string: Human-readable health status message
//   - error: Any error encountered during health checking
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
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
	checks := make([]health.Check, 0, 4)

	if s.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: s.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(s.blockchainClient)})
	}

	if s.blockassemblyClient != nil {
		checks = append(checks, health.Check{Name: "BlockassemblyClient", Check: s.blockassemblyClient.Health})
	}

	if s.utxoStore != nil {
		checks = append(checks, health.Check{Name: "UTXOStore", Check: s.utxoStore.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC implements the gRPC health check endpoint.
// This method provides a gRPC-compatible version of the health check, allowing clients
// to query the service's health status via the gRPC API. It uses the same health check
// logic as the HTTP-based Health method but formats the response as a gRPC message.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//   - _: Empty message, not used but required by the gRPC interface
//
// Returns:
//   - *alert_api.HealthResponse: gRPC response containing health status information
//   - error: Any error encountered during health checking
func (s *Server) HealthGRPC(ctx context.Context, _ *emptypb.Empty) (*alert_api.HealthResponse, error) {
	prometheusHealth.Add(1)

	status, details, err := s.Health(ctx, false)

	return &alert_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.New(time.Now()),
	}, errors.WrapGRPC(err)
}

// Init initializes the server by loading configuration.
// This method performs first-stage initialization of the alert service, loading
// configuration and setting up essential services. It should be called after
// creating a new Server instance and before starting the server.
//
// The initialization process includes loading the alert system configuration,
// connecting to the database, and initializing the P2P communication subsystem.
// Any errors during initialization are returned, allowing the caller to handle them appropriately.
//
// Parameters:
//   - ctx: Context for initialization, allowing for cancellation and timeouts
//
// Returns:
//   - error: Any error encountered during initialization
func (s *Server) Init(ctx context.Context) (err error) {
	// Load the alert system configuration
	if err = s.loadConfig(ctx, models.BaseModels, false); err != nil {
		return errors.NewConfigurationError("error loading configuration", err)
	}

	return nil
}

// Start begins the server operation and blocks until shutdown.
// This method starts all server components, including the gRPC server for the API,
// and signals readiness through the provided channel when startup is complete.
// It then blocks until the context is cancelled or an error occurs.
//
// If any errors occur during startup, they are returned before signaling readiness.
// This method follows the standard Teranode service lifecycle pattern, making it
// compatible with the service manager's startup and shutdown sequence.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and shutdown
//   - readyCh: Channel to signal when the server is ready to accept requests
//
// Returns:
//   - error: Any error encountered during startup or operation
func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) (err error) {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	// Ensure we have the genesis alert in the database
	if err = models.CreateGenesisAlert(
		ctx, model.WithAllDependencies(s.appConfig),
	); err != nil {
		return errors.NewConfigurationError("error creating genesis alert", err)
	}

	// Ensure that RPC connection is valid
	if !s.appConfig.DisableRPCVerification {
		if _, err = s.appConfig.Services.Node.BestBlockHash(context.Background()); err != nil {
			return errors.NewServiceError("error talking to Bitcoin node with supplied RPC credentials", err)
		}
	}

	// Create the p2p server
	s.p2pServer, err = p2p.NewServer(p2p.ServerOptions{
		TopicNames: []string{s.appConfig.P2P.TopicName},
		Config:     s.appConfig,
	})
	if err != nil {
		return errors.NewServiceError("error creating p2p server", err)
	}

	// delay closing readyCh until p2p server is started
	time.AfterFunc(500*time.Millisecond, func() {
		closeOnce.Do(func() { close(readyCh) })
	})

	// Start the p2p server - this blocks
	if err = s.p2pServer.Start(ctx); err != nil {
		return errors.NewServiceError("error starting p2p server", err)
	}

	// wait for a shutdown signal
	<-ctx.Done()

	return nil
}

// Stop gracefully shuts down the server.
// This method performs a clean shutdown of all server components, ensuring that
// ongoing operations can complete and resources are properly released. It follows
// the standard Teranode service lifecycle pattern for orderly shutdown.
//
// The shutdown sequence includes stopping the P2P server if it's running and
// any other cleanup operations needed for a graceful termination.
//
// Parameters:
//   - ctx: Context for the shutdown operation, potentially with a deadline
//
// Returns:
//   - error: Any error encountered during shutdown
func (s *Server) Stop(ctx context.Context) error {
	if s.appConfig != nil {
		s.appConfig.CloseAll(ctx)
	}

	// Shutdown the p2p server
	if s.p2pServer != nil {
		if err := s.p2pServer.Stop(ctx); err != nil {
			s.logger.Errorf("error shutting down p2p server: %s", err)
		}
	}

	return nil
}

// loadConfig loads the server configuration and initializes required services.
// This method performs the core configuration loading and service initialization for the alert server.
// It reads configuration from Teranode settings, sets up network-specific topic names, validates
// P2P configuration, creates necessary directories, and initializes the datastore connection.
//
// The configuration process includes:
// - Reading alert system configuration from Teranode settings
// - Adjusting topic names for non-mainnet networks (prefixed with network name)
// - Validating P2P configuration requirements
// - Creating private key directories if needed
// - Initializing and connecting to the configured datastore
// - Auto-migrating database models if specified
//
// Parameters:
//   - ctx: Context for the configuration loading operation, allowing for cancellation and timeouts
//   - models: List of database models to auto-migrate when the datastore is created.
//     Pass nil or empty slice to skip auto-migration
//   - isTesting: When true, the P2P node will be mocked for testing purposes instead of
//     creating a real P2P connection
//
// Returns:
//   - error: Any error encountered during configuration loading or service initialization
func (s *Server) loadConfig(ctx context.Context, models []interface{}, isTesting bool) (err error) {
	// read configuration from Teranode settings file
	network := s.settings.ChainCfgParams.Name
	topicName := s.settings.Alert.TopicName

	if network != "mainnet" {
		topicName = "bitcoin_alert_system_" + network
	}

	// create the app config
	s.appConfig = &config.Config{
		AlertProcessingInterval: 5 * time.Minute,
		RequestLogging:          true,
		Datastore: config.DatastoreConfig{
			AutoMigrate: true,
			SQLite:      &datastore.SQLiteConfig{},
			SQLRead:     &datastore.SQLConfig{},
			SQLWrite:    &datastore.SQLConfig{},
		},
		P2P: config.P2PConfig{
			IP:                    "0.0.0.0",
			Port:                  strconv.Itoa(s.settings.Alert.P2PPort),
			DHTMode:               "client",
			AlertSystemProtocolID: s.settings.Alert.ProtocolID,
			TopicName:             topicName,
			PrivateKey:            s.settings.Alert.P2PPrivateKey,
		},
		Services: config.Services{
			Log:        NewLogger(s.logger.Duplicate(ulogger.WithSkipFrame(1))),
			HTTPClient: http.DefaultClient,
		},
		RPCConnections: []config.RPCConfig{},
		GenesisKeys:    s.settings.Alert.GenesisKeys,
	}

	// Require list of genesis keys
	if len(s.appConfig.GenesisKeys) == 0 {
		return config.ErrNoGenesisKeys
	}

	// Ensure the P2P configuration is valid
	if err = s.requireP2P(); err != nil {
		return err
	}

	// Set the node config (either a real node or a mock node)
	if isTesting {
		s.appConfig.Services.Node = config.NewNodeMock(
			"test",
			"test",
			"localhost:8332",
		)
	} else {
		s.appConfig.Services.Node = NewNodeConfig(s.logger, s.blockchainClient, s.utxoStore, s.blockassemblyClient, s.peerClient, s.p2pClient, s.settings)
	}

	// Load the datastore service
	if err = s.loadDatastore(ctx, models, s.settings.Alert.StoreURL); err != nil {
		return err
	}

	return
}

// requireP2P validates and ensures the P2P configuration is complete and valid.
// This method performs validation and setup of P2P-related configuration parameters,
// applying default values where necessary to ensure the alert system can properly
// participate in the peer-to-peer network for alert distribution.
//
// The validation process includes:
// - Setting the P2P alert system protocol ID to the default if not specified
// - Setting the P2P topic name to the default if not specified
// - Validating that required P2P configuration fields are present and valid
// - Ensuring IP address and port configurations meet minimum requirements
//
// This method should be called during server initialization after loading the
// basic configuration but before starting P2P services.
//
// Returns:
//   - error: Any error encountered during P2P configuration validation, including
//     missing required fields or invalid configuration values
func (s *Server) requireP2P() error {
	// Set the P2P alert system protocol ID if it's missing
	if len(s.appConfig.P2P.AlertSystemProtocolID) == 0 {
		s.appConfig.P2P.AlertSystemProtocolID = config.DefaultAlertSystemProtocolID
	}

	// Set the p2p alert system topic name if it's missing
	if len(s.appConfig.P2P.TopicName) == 0 {
		s.appConfig.P2P.TopicName = config.DefaultTopicName
	}

	// Load the private key path if a private key is not set
	// If not found, create a default one
	if len(s.appConfig.P2P.PrivateKeyPath) == 0 && len(s.appConfig.P2P.PrivateKey) == 0 {
		if err := s.createPrivateKeyDirectory(); err != nil {
			return err
		}
	}

	// Load the peer discovery interval
	if s.appConfig.P2P.PeerDiscoveryInterval <= 0 {
		s.appConfig.P2P.PeerDiscoveryInterval = config.DefaultPeerDiscoveryInterval
	}

	// Load the p2p ip (local, ip address or domain name)
	// @TODO better validation of what is a valid IP, domain name or local address
	if len(s.appConfig.P2P.IP) < 5 {
		return config.ErrNoP2PIP
	}

	// Load the p2p port ( >= XX)
	if len(s.appConfig.P2P.Port) < 2 {
		return config.ErrNoP2PPort
	}

	return nil
}

// createPrivateKeyDirectory creates the directory for storing P2P private keys.
// This method ensures that the directory structure exists for storing the alert system's
// P2P private key files. It creates the directory in the user's home directory if it
// doesn't already exist, using appropriate permissions for security.
//
// The directory is created at $HOME/.alert-system/ with permissions 0750 (owner: rwx, group: r-x, other: none)
// to ensure that private key files stored within are protected from unauthorized access.
// If the directory already exists, no error is returned.
//
// This method is typically called during server initialization when P2P configuration
// requires a private key file but no explicit path has been provided.
//
// Returns:
//   - error: Any error encountered during directory creation, including permission issues
//     or inability to determine the user's home directory
func (s *Server) createPrivateKeyDirectory() error {
	dirName, err := os.UserHomeDir()
	if err != nil {
		return errors.NewConfigurationError("failed to initialize p2p private key file", err)
	}

	if err = os.Mkdir(fmt.Sprintf("%s/%s", dirName, config.LocalPrivateKeyDirectory), 0750); err != nil && !errors.Is(err, os.ErrExist) {
		return errors.NewConfigurationError("failed to ensure %s dir exists", config.LocalPrivateKeyDirectory, err)
	}

	s.appConfig.P2P.PrivateKeyPath = fmt.Sprintf("%s/%s/%s", dirName, config.LocalPrivateKeyDirectory, config.LocalPrivateKeyDefault)

	return nil
}

// loadDatastore initializes and configures the datastore connection for the alert service.
// This method creates a datastore client instance with appropriate configuration for the
// alert system's persistent storage needs. It supports multiple database backends including
// SQLite, PostgreSQL, and MySQL, with connection pooling and auto-migration capabilities.
//
// The datastore initialization process includes:
// - Configuring logging integration with the alert service logger
// - Setting up SSL/TLS connection parameters based on database URL
// - Configuring connection pooling parameters for optimal performance
// - Performing auto-migration of specified database models if provided
// - Establishing the connection and verifying database accessibility
//
// Supported database schemes:
// - sqlite:// and sqlitememory:// for SQLite databases
// - postgres:// for PostgreSQL databases
// - mysql:// for MySQL databases
//
// Parameters:
//   - ctx: Context for the datastore initialization, allowing for cancellation and timeouts
//   - models: List of database models to auto-migrate after connection establishment.
//     Pass nil or empty slice to skip auto-migration
//   - dbURL: Parsed database URL containing connection parameters including scheme, host,
//     port, database name, and connection options
//
// Returns:
//   - error: Any error encountered during datastore initialization, including connection
//     failures, unsupported database schemes, or migration errors
func (s *Server) loadDatastore(ctx context.Context, models []interface{}, dbURL *url.URL) error {
	// Sync collecting the options
	var options []datastore.ClientOps

	options = append(options, datastore.WithLogger(NewGormLogger(s.logger)))

	debugging := s.logger.LogLevel() == ulogger.LogLevelDebug

	sslMode := "disable"

	queryParams := dbURL.Query()
	if val, ok := queryParams["sslmode"]; ok && len(val) > 0 {
		sslMode = val[0]
	}

	// Select the datastore
	switch dbURL.Scheme {
	case "sqlite":
		fallthrough
	case "sqlitememory":
		folder := s.settings.DataFolder

		err := os.MkdirAll(folder, 0755)
		if err != nil {
			return errors.NewServiceError("failed to create data folder %s", folder, err)
		}

		dbName := dbURL.Path[1:]

		filename := "" // default memory store
		if dbURL.Scheme == "sqlite" {
			filename, err = filepath.Abs(path.Join(folder, fmt.Sprintf("%s.db", dbName)))
			if err != nil {
				return errors.NewServiceError("failed to get absolute path for sqlite DB", err)
			}
		}

		options = append(options, datastore.WithSQLite(&datastore.SQLiteConfig{
			CommonConfig: datastore.CommonConfig{
				Debug:              debugging,
				MaxIdleConnections: 1,
				MaxOpenConnections: 1,
				TablePrefix:        dbName,
			},
			DatabasePath: filename, // "" for in memory
			Shared:       false,    // never shared
		}))
	case "postgres":
		fallthrough
	case "mysql":
		dbEngine := datastore.PostgreSQL
		if dbURL.Scheme == "mysql" {
			dbEngine = datastore.MySQL
		}

		commonConfig := datastore.CommonConfig{
			Debug:                 debugging,
			MaxConnectionIdleTime: 20 * time.Second,
			MaxConnectionTime:     20 * time.Second,
			MaxIdleConnections:    2,
			MaxOpenConnections:    5,
			TablePrefix:           "alert_system",
		}

		password, _ := dbURL.User.Password()
		datastoreConfig := &datastore.SQLConfig{
			CommonConfig: commonConfig,
			Driver:       dbEngine.String(),
			Host:         dbURL.Hostname(),
			Name:         dbURL.Path[1:],
			Password:     password,
			Port:         dbURL.Port(),
			Replica:      false,
			TimeZone:     "UTC",
			TxTimeout:    20 * time.Second,
			User:         dbURL.User.Username(),
			SslMode:      sslMode,
		}

		// Create the read/write options
		options = append(options, datastore.WithSQL(dbEngine, []*datastore.SQLConfig{
			datastoreConfig, // MASTER - WRITE
			datastoreConfig, // READ REPLICA
		}))
	default:
		return config.ErrDatastoreUnsupported
	}

	// Add the auto migrate
	if s.appConfig.Datastore.AutoMigrate && models != nil {
		options = append(options, datastore.WithAutoMigrate(models...))
	}

	// Load datastore or return an error
	var err error
	s.appConfig.Services.Datastore, err = datastore.NewClient(ctx, options...)

	return err
}
