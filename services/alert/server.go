package alert

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/bitcoin-sv/alert-system/app/config"
	"github.com/bitcoin-sv/alert-system/app/models"
	"github.com/bitcoin-sv/alert-system/app/models/model"
	"github.com/bitcoin-sv/alert-system/app/p2p"
	"github.com/bitcoin-sv/alert-system/app/webserver"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/alert/alert_api"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/mrz1836/go-datastore"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server type carries the logger within it
type Server struct {
	alert_api.UnimplementedAlertAPIServer
	logger ulogger.Logger
	stats  *gocore.Stat

	// other services
	blockchainClient    blockchain.ClientI
	utxoStore           utxo.Store
	blockassemblyClient *blockassembly.Client // blockassembly does not have an interface yet

	// alert system specific configuration
	appConfig *config.Config
	p2pServer *p2p.Server
	webServer *webserver.Server
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, blockchainClient blockchain.ClientI, utxoStore utxo.Store, blockassemblyClient *blockassembly.Client) *Server {
	initPrometheusMetrics()

	return &Server{
		logger:              logger,
		stats:               gocore.NewStat("alert"),
		blockchainClient:    blockchainClient,
		utxoStore:           utxoStore,
		blockassemblyClient: blockassemblyClient,
	}
}

func (s *Server) Health(_ context.Context) (int, string, error) {
	return 0, "", nil
}

func (s *Server) Init(ctx context.Context) (err error) {
	// Load the alert system configuration
	if err = s.loadConfig(ctx, models.BaseModels, false); err != nil {
		return errors.NewConfigurationError("error loading configuration", err)
	}

	return nil
}

// Start function
func (s *Server) Start(ctx context.Context) (err error) {
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

	// Create a new (web) server
	s.webServer = webserver.NewServer(s.appConfig, s.p2pServer)

	// Start the p2p server
	if err = s.p2pServer.Start(ctx); err != nil {
		return errors.NewServiceError("error starting p2p server", err)
	}

	// Serve the web server and then wait endlessly
	s.webServer.Serve()

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.appConfig.CloseAll(ctx)

	if err := s.webServer.Shutdown(ctx); err != nil {
		s.logger.Errorf("error shutting down webserver: %s", err)
	}

	// Shutdown the p2p server
	if err := s.p2pServer.Stop(ctx); err != nil {
		s.logger.Errorf("error shutting down p2p server: %s", err)
	}

	return nil
}

func (s *Server) HealthGRPC(_ context.Context, _ *emptypb.Empty) (*alert_api.HealthResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		s.stats.NewStat("Health_grpc").AddTime(start)
	}()

	prometheusHealth.Inc()

	ok := true
	if !s.p2pServer.Connected() || s.p2pServer.ActivePeers() == 0 {
		ok = false
	}

	return &alert_api.HealthResponse{
		Ok:        ok,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

// LoadDependencies will load the configuration and services
// models is a list of models to auto-migrate when the datastore is created
// if testing is true, the node will be mocked
func (s *Server) loadConfig(ctx context.Context, models []interface{}, isTesting bool) (err error) {
	// read configuration from Teranode settings file
	genesisKeys, _ := gocore.Config().GetMulti("alert_genesis_keys", "|", []string{})
	dbURL, _, _ := gocore.Config().GetURL("alert_store", "sqlite:///alert")
	p2pPort, _ := gocore.Config().Get("ALERT_P2P_PORT", "9908")
	network, _ := gocore.Config().Get("network", "mainnet")
	privateKey, _ := gocore.Config().Get("alert_p2p_private_key", "")
	topicName, _ := gocore.Config().Get("alert_topic_name", "bitcoin_alert_system")
	protocolID, _ := gocore.Config().Get("alert_protocol_id", "/bitcoin/alert-system/1.0.0")

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
			Port:                  p2pPort,
			DHTMode:               "client",
			AlertSystemProtocolID: protocolID,
			TopicName:             topicName,
			PrivateKey:            privateKey,
		},
		Services: config.Services{
			Log:        NewLogger(s.logger.Duplicate(ulogger.WithSkipFrame(1))),
			HTTPClient: http.DefaultClient,
		},
		WebServer: config.WebServerConfig{
			IdleTimeout:  60 * time.Second,
			Port:         "3000",
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
		},
		RPCConnections: []config.RPCConfig{},
		GenesisKeys:    genesisKeys,
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
		s.appConfig.Services.Node = NewNodeConfig(s.logger, s.blockchainClient, s.utxoStore, s.blockassemblyClient)
	}

	// Load the datastore service
	if err = s.loadDatastore(ctx, models, dbURL); err != nil {
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
	// todo better validation of what is a valid IP, domain name or local address
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
		return errors.NewConfigurationError("failed to initialize p2p private key file: %w", err)
	}

	if err = os.Mkdir(fmt.Sprintf("%s/%s", dirName, config.LocalPrivateKeyDirectory), 0750); err != nil && !errors.Is(err, os.ErrExist) {
		return errors.NewConfigurationError("failed to ensure %s dir exists: %w", config.LocalPrivateKeyDirectory, err)
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

	// Select the datastore
	switch dbURL.Scheme {
	case "sqlite":
		fallthrough
	case "sqlitememory":
		folder, _ := gocore.Config().Get("dataFolder", "data")

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
