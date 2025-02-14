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

	"github.com/bitcoin-sv/alert-system/app/config"
	"github.com/bitcoin-sv/alert-system/app/models"
	"github.com/bitcoin-sv/alert-system/app/models/model"
	"github.com/bitcoin-sv/alert-system/app/p2p"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/alert/alert_api"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/mrz1836/go-datastore"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server represents the main alert system server structure.
type Server struct {
	// UnimplementedAlertAPIServer is embedded for forward compatibility with the alert API
	alert_api.UnimplementedAlertAPIServer

	// logger handles all logging operations
	logger ulogger.Logger

	// settings contains the server configuration settings
	settings *settings.Settings

	// stats tracks server statistics
	stats *gocore.Stat

	// blockchainClient provides access to blockchain operations
	blockchainClient blockchain.ClientI

	// utxoStore manages UTXO operations
	utxoStore utxo.Store

	// blockassemblyClient handles block assembly operations
	blockassemblyClient *blockassembly.Client

	// appConfig contains alert system specific configuration
	appConfig *config.Config

	// p2pServer manages peer-to-peer communication
	p2pServer *p2p.Server
}

// New creates and returns a new Server instance with the provided dependencies.
func New(logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, utxoStore utxo.Store, blockassemblyClient *blockassembly.Client) *Server {
	initPrometheusMetrics()

	return &Server{
		logger:              logger,
		settings:            tSettings,
		stats:               gocore.NewStat("alert"),
		blockchainClient:    blockchainClient,
		utxoStore:           utxoStore,
		blockassemblyClient: blockassemblyClient,
	}
}

// Health performs health checks on the server and its dependencies.
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
func (s *Server) HealthGRPC(ctx context.Context, _ *emptypb.Empty) (*alert_api.HealthResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "HealthGRPC",
		tracing.WithParentStat(s.stats),
		tracing.WithCounter(prometheusHealth),
		tracing.WithDebugLogMessage(s.logger, "[HealthGRPC] called"),
	)
	defer deferFn()

	status, details, err := s.Health(ctx, false)

	return &alert_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.New(time.Now()),
	}, errors.WrapGRPC(err)
}

// Init initializes the server by loading configuration.
func (s *Server) Init(ctx context.Context) (err error) {
	// Load the alert system configuration
	if err = s.loadConfig(ctx, models.BaseModels, false); err != nil {
		return errors.NewConfigurationError("error loading configuration", err)
	}

	return nil
}

// Start begins the server operation and blocks until shutdown.
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

	// Start the p2p server
	if err = s.p2pServer.Start(ctx); err != nil {
		return errors.NewServiceError("error starting p2p server", err)
	}

	closeOnce.Do(func() { close(readyCh) })

	// wait for a shutdown signal
	<-ctx.Done()

	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	s.appConfig.CloseAll(ctx)

	// Shutdown the p2p server
	if err := s.p2pServer.Stop(ctx); err != nil {
		s.logger.Errorf("error shutting down p2p server: %s", err)
	}

	return nil
}

// loadConfig loads the server configuration and initializes required services.
// models is a list of models to auto-migrate when the datastore is created
// if testing is true, the node will be mocked
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
		s.appConfig.Services.Node = NewNodeConfig(s.logger, s.blockchainClient, s.utxoStore, s.blockassemblyClient, s.settings)
	}

	// Load the datastore service
	if err = s.loadDatastore(ctx, models, s.settings.Alert.StoreURL); err != nil {
		return err
	}

	return
}

// requireP2P will ensure the P2P configuration is valid
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

// createPrivateKeyDirectory will create the private key directory
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

// loadDatastore will load an instance of Datastore into the dependencies
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
