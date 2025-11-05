package p2p

// Package p2p provides peer-to-peer networking functionality for the Teranode system.
// It implements a robust distributed network for blockchain data propagation using libp2p.
//
// Key features include:
// - Decentralized peer discovery and management
// - Topic-based publish/subscribe for blocks, transactions, and control messages
// - Configurable ban management for misbehaving peers
// - Integration with blockchain and block validation services
// - Support for both public and private DHT networks
// - Websocket API for external connectivity
//
// The p2p service serves as a communication backbone for Teranode, connecting
// multiple nodes in a resilient network topology and facilitating efficient
// propagation of blockchain data across the network.

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/servicemanager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	banActionAdd = "add" // Action constant for adding a ban

	// Default values for peer map cleanup
	defaultPeerMapMaxSize         = 100000           // Maximum entries in peer maps
	defaultPeerMapTTL             = 30 * time.Minute // Time-to-live for peer map entries
	defaultPeerMapCleanupInterval = 5 * time.Minute  // Cleanup interval
	protocolIDVersion             = "1.0.0"          // Protocol version identifier
)

// peerMapEntry stores peer information with timestamp for TTL tracking
type peerMapEntry struct {
	peerID    string
	timestamp time.Time
}

// Server represents the P2P server instance and implements the P2P service functionality.
// It is the main entry point for the p2p service and coordinates all peer-to-peer communication.
// The Server manages topics, subscriptions, message propagation, and peer lifecycle management.
//
// Server integrates with multiple Teranode components, including the blockchain service
// for retrieving block data and the block validation service for verifying incoming blocks.
// It implements both HTTP and gRPC interfaces for external communication and control.
//
// Concurrency notes:
// - The Server uses multiple goroutines for handling different topics and events
// - Network I/O operations are performed asynchronously
// - Ban management is thread-safe across connections
type Server struct {
	p2p_api.UnimplementedPeerServiceServer
	P2PClient                         p2pMessageBus.P2PClient // The P2P network client
	logger                            ulogger.Logger          // Logger instance for the server
	settings                          *settings.Settings      // Configuration settings
	bitcoinProtocolVersion            string                  // Bitcoin protocol identifier
	blockchainClient                  blockchain.ClientI      // Client for blockchain interactions
	blockValidationClient             blockvalidation.Interface
	blockAssemblyClient               blockassembly.ClientI     // Client for block assembly operations
	AssetHTTPAddressURL               string                    // HTTP address URL for assets
	e                                 *echo.Echo                // Echo server instance
	notificationCh                    chan *notificationMsg     // Channel for notifications
	rejectedTxKafkaConsumerClient     kafka.KafkaConsumerGroupI // Kafka consumer for rejected transactions
	invalidBlocksKafkaConsumerClient  kafka.KafkaConsumerGroupI // Kafka consumer for invalid blocks
	invalidSubtreeKafkaConsumerClient kafka.KafkaConsumerGroupI // Kafka consumer for invalid subtrees
	subtreeKafkaProducerClient        kafka.KafkaAsyncProducerI // Kafka producer for subtrees
	blocksKafkaProducerClient         kafka.KafkaAsyncProducerI // Kafka producer for blocks
	banList                           BanListI                  // List of banned peers
	banChan                           chan BanEvent             // Channel for ban events
	banManager                        PeerBanManagerI           // Manager for peer banning
	gCtx                              context.Context
	blockTopicName                    string
	subtreeTopicName                  string
	rejectedTxTopicName               string
	invalidBlocksTopicName            string             // Kafka topic for invalid blocks
	invalidSubtreeTopicName           string             // Kafka topic for invalid subtrees
	nodeStatusTopicName               string             // pubsub topic for node status messages
	topicPrefix                       string             // Chain identifier prefix for topic validation
	blockPeerMap                      sync.Map           // Map to track which peer sent each block (hash -> peerMapEntry)
	subtreePeerMap                    sync.Map           // Map to track which peer sent each subtree (hash -> peerMapEntry)
	startTime                         time.Time          // Server start time for uptime calculation
	peerRegistry                      *PeerRegistry      // Central registry for all peer information
	peerSelector                      *PeerSelector      // Stateless peer selection logic
	peerHealthChecker                 *PeerHealthChecker // Async health monitoring
	syncCoordinator                   *SyncCoordinator   // Orchestrates sync operations
	syncConnectionTimes               sync.Map           // Map to track when we first connected to each sync peer (peerID -> timestamp)

	// Cleanup configuration
	peerMapCleanupTicker *time.Ticker  // Ticker for periodic cleanup of peer maps
	peerMapMaxSize       int           // Maximum number of entries in peer maps
	peerMapTTL           time.Duration // Time-to-live for peer map entries
}

// NewServer creates a new P2P server instance with the provided configuration and dependencies.
// It initializes the core components of the P2P service including the network node, topic subscriptions,
// ban management, and integration with external services.
//
// Parameters:
// - ctx: The parent context for lifecycle management
// - logger: Logging interface for all P2P operations
// - tSettings: Configuration settings containing network topology and behavior parameters
// - blockchainClient: Client for retrieving and querying blockchain data
// - rejectedTxKafkaConsumerClient: Kafka consumer client for receiving rejected transaction notifications
// - invalidBlocksKafkaConsumerClient: Kafka consumer client for receiving invalid block notifications
// - invalidSubtreeKafkaConsumerClient: Kafka consumer client for receiving invalid subtree notifications
// - subtreeKafkaProducerClient: Kafka producer client for publishing subtree data
// - blocksKafkaProducerClient: Kafka producer client for publishing block data
//
// Returns a configured Server instance ready to be initialized and started, or an error if configuration
// validation fails or any dependencies cannot be properly initialized.

// getPeerCacheFilePath constructs the full path to the teranode_peers.json file based on the configured directory.
// If no directory is specified, it defaults to the current working directory.
// The filename is always "teranode_peers.json" for consistency.
func getPeerCacheFilePath(configuredDir string) string {
	var dir string
	if configuredDir != "" {
		dir = configuredDir
	} else {
		// Default to current working directory
		dir = "."
	}
	return filepath.Join(dir, "teranode_peers.json")
}

func NewServer(
	ctx context.Context,
	logger ulogger.Logger,
	tSettings *settings.Settings,
	blockchainClient blockchain.ClientI,
	blockAssemblyClient blockassembly.ClientI,
	rejectedTxKafkaConsumerClient kafka.KafkaConsumerGroupI,
	invalidBlocksKafkaConsumerClient kafka.KafkaConsumerGroupI,
	invalidSubtreeKafkaConsumerClient kafka.KafkaConsumerGroupI,
	subtreeKafkaProducerClient kafka.KafkaAsyncProducerI,
	blocksKafkaProducerClient kafka.KafkaAsyncProducerI,
) (*Server, error) {
	logger.Debugf("Creating P2P service")

	listenAddresses := tSettings.P2P.ListenAddresses
	if listenAddresses == nil {
		return nil, errors.NewConfigurationError("p2p_listen_addresses not set in config")
	}

	p2pPort := tSettings.P2P.Port
	if p2pPort == 0 {
		return nil, errors.NewConfigurationError("p2p_port not set in config")
	}

	if tSettings.ChainCfgParams.TopicPrefix == "" {
		return nil, errors.NewConfigurationError("missing config ChainCfgParams.TopicPrefix")
	}
	topicPrefix := tSettings.ChainCfgParams.TopicPrefix

	blockTopic := tSettings.P2P.BlockTopic
	if blockTopic == "" {
		return nil, errors.NewConfigurationError("p2p_block_topic not set in config")
	}

	subtreeTopic := tSettings.P2P.SubtreeTopic
	if subtreeTopic == "" {
		return nil, errors.NewConfigurationError("p2p_subtree_topic not set in config")
	}

	rejectedTxTopic := tSettings.P2P.RejectedTxTopic
	if rejectedTxTopic == "" {
		return nil, errors.NewConfigurationError("p2p_rejected_tx_topic not set in config")
	}

	nodeStatusTopic := tSettings.P2P.NodeStatusTopic
	if nodeStatusTopic == "" {
		nodeStatusTopic = "node_status" // Default value for backward compatibility
	}

	listenMode := tSettings.P2P.ListenMode
	if listenMode != settings.ListenModeFull && listenMode != settings.ListenModeListenOnly {
		return nil, errors.NewConfigurationError("listen_mode must be either '%s' or '%s' (got '%s')", settings.ListenModeFull, settings.ListenModeListenOnly, listenMode)
	}

	banlist, banChan, err := GetBanList(ctx, logger, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("error getting banlist", err)
	}

	staticPeers := tSettings.P2P.StaticPeers

	privateKey := tSettings.P2P.PrivateKey

	// Attempt to get the private key if not provided in settings
	// The private key can come from:
	// 1. tSettings.P2P.PrivateKey (already loaded from config/environment)
	// 2. Read from p2p.key file
	// 3. Generate new key and save to p2p.key file
	if privateKey == "" {
		// Construct the key file path (same directory as teranode_peers.json)
		keyFilePath := getPeerCacheFilePath(tSettings.P2P.PeerCacheDir)
		// Replace the filename from teranode_peers.json to p2p.key
		keyFilePath = filepath.Join(filepath.Dir(keyFilePath), "p2p.key")

		if keyData, err := os.ReadFile(keyFilePath); err == nil {
			// File exists, use its content
			privateKey = strings.TrimSpace(string(keyData))
			logger.Infof("[P2P] Loaded private key from file: %s", keyFilePath)
		} else if os.IsNotExist(err) {
			// File doesn't exist, generate new key and save it
			logger.Infof("[P2P] Private key not found, generating new key...")

			// Generate a new Ed25519 private key for libp2p
			privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
			if err != nil {
				return nil, errors.NewServiceError("failed to generate P2P private key", err)
			}

			// Get raw bytes for the hex format (private + public)
			rawPriv, err := privKey.Raw()
			if err != nil {
				return nil, errors.NewServiceError("failed to get raw private key bytes", err)
			}

			pubKey := privKey.GetPublic()
			rawPub, err := pubKey.Raw()
			if err != nil {
				return nil, errors.NewServiceError("failed to get raw public key bytes", err)
			}

			// Combine private (32 bytes) + public (32 bytes) = 64 bytes total
			ed25519Key := append(rawPriv, rawPub...)
			privateKey = hex.EncodeToString(ed25519Key)

			// Ensure the directory exists before attempting to write the key file
			if err := os.MkdirAll(filepath.Dir(keyFilePath), 0o755); err != nil {
				logger.Errorf("[P2P] Failed to create directory for private key %s: %v", keyFilePath, err)
				return nil, errors.NewServiceError(fmt.Sprintf("failed to create directory for private key %s", keyFilePath), err)
			}
			if err := os.WriteFile(keyFilePath, []byte(privateKey), 0600); err != nil {
				logger.Errorf("[P2P] Failed to save private key to file %s: %v", keyFilePath, err)
				return nil, errors.NewServiceError(fmt.Sprintf("failed to save private key to file %s", keyFilePath), err)
			}

			logger.Infof("[P2P] Generated and saved new P2P private key to file: %s", keyFilePath)
		} else {
			// Some other error reading the file
			return nil, errors.NewServiceError(fmt.Sprintf("error reading private key file %s", keyFilePath), err)
		}
	}

	// Configure advertise addresses
	// With go-p2p v1.2.1, address advertisement is handled more intelligently:
	// - If AdvertiseAddresses is explicitly set, those addresses are used
	// - If SharePrivateAddresses is true, we pass listen addresses to ensure local connectivity
	// - Otherwise, go-p2p will automatically filter private IPs and detect public addresses
	var advertiseAddresses []string
	if len(tSettings.P2P.AdvertiseAddresses) > 0 {
		// Use explicitly configured advertise addresses
		advertiseAddresses = tSettings.P2P.AdvertiseAddresses
		logger.Infof("Using configured advertise addresses: %v", advertiseAddresses)
	} else if tSettings.P2P.SharePrivateAddresses {
		// Share private addresses for local/test environments
		advertiseAddresses = listenAddresses
		logger.Infof("Sharing private addresses for local connectivity: %v", advertiseAddresses)
	} else {
		// Let go-p2p auto-detect and filter private addresses
		advertiseAddresses = []string{}
		logger.Infof("Private address sharing disabled - go-p2p will auto-detect public addresses only")
	}

	// Construct the full Bitcoin protocol ID with version and network topic prefix
	// This ensures we only connect to peers on the same network (e.g. mainnet/testnet)
	// The format results in "teranode/bitcoin/<network>/<protocolIDVersion>"
	bitcoinProtocolVersion := fmt.Sprintf("/teranode/bitcoin/%s/%s", tSettings.ChainCfgParams.Name, protocolIDVersion)

	// Decode the hex-encoded private key into standard crypto library privkey
	privDecoded, err := hex.DecodeString(privateKey)
	if err != nil {
		return nil, errors.NewServiceError("failed to decode key", err)
	}
	privKey, err := crypto.UnmarshalEd25519PrivateKey(privDecoded)
	if err != nil {
		return nil, errors.NewServiceError("failed to unmarshal key", err)
	}
	conf := p2pMessageBus.Config{
		PrivateKey:      privKey,
		Name:            tSettings.ClientName,
		Logger:          logger,
		PeerCacheFile:   getPeerCacheFilePath(tSettings.P2P.PeerCacheDir),
		BootstrapPeers:  staticPeers,
		RelayPeers:      tSettings.P2P.RelayPeers,
		ProtocolVersion: bitcoinProtocolVersion,
		DisableNAT:      tSettings.P2P.DisableNAT,
	}

	if len(advertiseAddresses) > 0 {
		conf.AnnounceAddrs = advertiseAddresses
		conf.Port = tSettings.P2P.Port
	}

	p2pClient, err := p2pMessageBus.NewClient(conf)
	if err != nil {
		return nil, errors.NewServiceError("failed to create p2p client", err)
	}
	// Log P2P node creation
	logger.Infof("P2P node created successfully")
	// The node will learn its external address via libp2p's Identify protocol
	// when peers connect and tell us what address they see us from

	p2pServer := &Server{
		P2PClient:              p2pClient,
		logger:                 logger,
		settings:               tSettings,
		bitcoinProtocolVersion: bitcoinProtocolVersion,
		notificationCh:         make(chan *notificationMsg, 1_000),
		blockchainClient:       blockchainClient,
		blockAssemblyClient:    blockAssemblyClient,

		banChan: banChan,
		banList: banlist,

		rejectedTxKafkaConsumerClient:     rejectedTxKafkaConsumerClient,
		invalidBlocksKafkaConsumerClient:  invalidBlocksKafkaConsumerClient,
		invalidSubtreeKafkaConsumerClient: invalidSubtreeKafkaConsumerClient,
		subtreeKafkaProducerClient:        subtreeKafkaProducerClient,
		blocksKafkaProducerClient:         blocksKafkaProducerClient,
		gCtx:                              ctx,
		blockTopicName:                    fmt.Sprintf("%s-%s", topicPrefix, blockTopic),
		subtreeTopicName:                  fmt.Sprintf("%s-%s", topicPrefix, subtreeTopic),
		rejectedTxTopicName:               fmt.Sprintf("%s-%s", topicPrefix, rejectedTxTopic),
		invalidBlocksTopicName:            tSettings.Kafka.InvalidBlocks,
		invalidSubtreeTopicName:           tSettings.Kafka.InvalidSubtrees,
		nodeStatusTopicName:               fmt.Sprintf("%s-%s", topicPrefix, nodeStatusTopic),
		topicPrefix:                       topicPrefix,
		startTime:                         time.Now(),

		// Initialize cleanup configuration with defaults
		peerMapMaxSize: defaultPeerMapMaxSize,
		peerMapTTL:     defaultPeerMapTTL,
	}

	// Override defaults with settings if provided
	if tSettings.P2P.PeerMapMaxSize > 0 {
		p2pServer.peerMapMaxSize = tSettings.P2P.PeerMapMaxSize
	}
	if tSettings.P2P.PeerMapTTL > 0 {
		p2pServer.peerMapTTL = tSettings.P2P.PeerMapTTL
	}

	// Initialize the ban manager first so it can be used by sync coordinator
	p2pServer.banManager = NewPeerBanManager(ctx, &myBanEventHandler{server: p2pServer}, tSettings)

	// Initialize new clean architecture components
	p2pServer.peerRegistry = NewPeerRegistry()
	p2pServer.peerSelector = NewPeerSelector(logger, tSettings)
	p2pServer.peerHealthChecker = NewPeerHealthChecker(logger, p2pServer.peerRegistry, tSettings)
	p2pServer.syncCoordinator = NewSyncCoordinator(
		logger,
		tSettings,
		p2pServer.peerRegistry,
		p2pServer.peerSelector,
		p2pServer.peerHealthChecker,
		p2pServer.banManager,
		blockchainClient,
		p2pServer.blocksKafkaProducerClient,
	)

	// Set local height callback for sync coordinator
	p2pServer.syncCoordinator.SetGetLocalHeightCallback(p2pServer.getLocalHeight)

	return p2pServer, nil
}

// Health performs health checks on the P2P server and its dependencies.
// This method implements the standard Teranode health check interface used by the service manager
// and monitoring systems to verify that the P2P service is operational.
//
// When checkLiveness is true, it only verifies that the service process is responsive.
// When checkLiveness is false, it performs deeper checks including dependency status.
//
// Returns:
// - HTTP status code (200 for healthy, 503 for unhealthy)
// - Status message describing the health state
// - Error details if any issues were encountered during the health check
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	var brokersURL []string
	if s.rejectedTxKafkaConsumerClient != nil { // tests may not set this
		brokersURL = s.rejectedTxKafkaConsumerClient.BrokersURL()
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := make([]health.Check, 0, 4)
	checks = append(checks, health.Check{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)})

	if s.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: s.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(s.blockchainClient)})
	}

	if s.blockValidationClient != nil {
		checks = append(checks, health.Check{Name: "BlockValidationClient", Check: s.blockValidationClient.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// Init initializes the P2P server and its components.
// This method prepares the server for operation but does not yet start network services or connect to peers.
// It performs initial setup of HTTP endpoints and sets configuration variables used during the main Start phase.
//
// The initialization process configures the service's public-facing HTTP address for asset discovery
// and prepares internal data structures and channels.
//
// Returns an error if any component initialization fails, or nil if successful.
func (s *Server) Init(ctx context.Context) (err error) {
	s.logger.Infof("[Init] P2P service initialising")

	AssetHTTPAddressURLString := s.settings.Asset.HTTPPublicAddress
	if AssetHTTPAddressURLString == "" {
		AssetHTTPAddressURLString = s.settings.Asset.HTTPAddress
	}

	s.AssetHTTPAddressURL = AssetHTTPAddressURLString

	return nil
}

// Start begins the P2P server operations and starts listening for connections.
// This method is the main entry point for activating the P2P network functionality.
// It performs several key operations:
// - Waits for the blockchain FSM to transition from idle state
// - Sets up topic handlers for blocks, and subtrees
// - Initializes the P2P node and starts listening on configured addresses
// - Starts Kafka consumers for rejected transactions
// - Launches the HTTP server for external API access
// - Begins periodic peer height synchronization
// - Establishes connections to static peers if configured
//
// The method signals service readiness by closing the provided readyCh channel when
// all components have started successfully.
//
// Returns an error if any component fails to start, or nil on successful startup.

func (s *Server) setupHTTPServer() *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.Recover())

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
	}))

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	})

	e.GET("/p2p-ws", s.HandleWebSocket(s.notificationCh, s.AssetHTTPAddressURL))

	return e
}

func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	var err error

	// Blocks until the FSM transitions from the IDLE state
	err = s.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		s.logger.Errorf("[P2P Service] Failed to wait for FSM transition from IDLE state: %s", err)

		return err
	}

	s.logger.Infof("[Start] P2P service starting")

	// For TxMeta, we are using autocommit, as we want to consume every message as fast as possible, and it is okay if some of the messages are not properly processed.
	// We don't need manual kafka commit and error handling here, as it is not necessary to retry the message, we have the message in stores.
	// Therefore, autocommit is set to true.
	s.rejectedTxKafkaConsumerClient.Start(ctx, s.rejectedTxHandler(ctx), kafka.WithLogErrorAndMoveOn())

	// Handler for invalid blocks Kafka messages
	if s.invalidBlocksKafkaConsumerClient != nil {
		s.logger.Infof("[Start] Starting invalid blocks Kafka consumer on topic: %s", s.invalidBlocksTopicName)
		s.invalidBlocksKafkaConsumerClient.Start(ctx, s.invalidBlockHandler(ctx), kafka.WithLogErrorAndMoveOn())
	}

	// Handler for invalid subtrees Kafka messages
	if s.invalidSubtreeKafkaConsumerClient != nil {
		s.logger.Infof("[Start] Starting invalid subtrees Kafka consumer on topic: %s", s.invalidSubtreeTopicName)
		s.invalidSubtreeKafkaConsumerClient.Start(ctx, s.invalidSubtreeHandler(ctx), kafka.WithLogErrorAndMoveOn())
	}

	s.subtreeKafkaProducerClient.Start(ctx, make(chan *kafka.Message, 10))
	s.blocksKafkaProducerClient.Start(ctx, make(chan *kafka.Message, 10))

	s.blockValidationClient, err = blockvalidation.NewClient(ctx, s.logger, s.settings, "p2p")
	if err != nil {
		return errors.NewServiceError("could not create block validation client [%w]", err)
	}

	s.e = s.setupHTTPServer()

	go func() {
		if err := s.StartHTTP(ctx); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				s.logger.Infof("http server shutdown")
			} else {
				s.logger.Errorf("failed to start http server: %v", err)
			}

			return
		}
	}()

	// Start a goroutine to periodically log observed addresses (for debugging NAT traversal)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if s.P2PClient != nil {
					// Get current peer addresses from the P2P node
					peers := s.P2PClient.GetPeers()
					s.logger.Debugf("P2P node currently connected to %d peers", len(peers))

					// Log our advertised addresses (these should include observed addresses)
					// The go-p2p library should be handling this via libp2p's Identify protocol
					if len(peers) > 0 {
						s.logger.Debugf("Node is reachable - peers can connect to us")
					} else if time.Since(s.startTime) > 2*time.Minute {
						s.logger.Warnf("No peers connected after %v - check NAT/firewall configuration", time.Since(s.startTime))
					}
				}
			}
		}
	}()

	// Subscribe to all topics
	s.subscribeToTopic(ctx, s.blockTopicName, s.handleBlockTopic)
	s.subscribeToTopic(ctx, s.subtreeTopicName, s.handleSubtreeTopic)
	s.subscribeToTopic(ctx, s.nodeStatusTopicName, s.handleNodeStatusTopic)
	s.subscribeToTopic(ctx, s.rejectedTxTopicName, s.handleRejectedTxTopic)

	// Start blockchain subscription before marking service as ready
	// This ensures we don't miss any block notifications
	blockchainSubscription, err := s.blockchainClient.Subscribe(ctx, "p2pServer")
	if err != nil {
		return errors.NewServiceError("error subscribing to blockchain service", err)
	}

	// Now start the listener goroutine with the established subscription
	go s.blockchainSubscriptionListener(ctx, blockchainSubscription)

	go s.listenForBanEvents(ctx)

	// disconnect any pre-existing banned peers at startup
	go s.disconnectPreExistingBannedPeers(ctx)

	// start the invalid blocks consumer
	if err := s.startInvalidBlockConsumer(ctx); err != nil {
		return errors.NewServiceError("failed to start invalid blocks consumer", err)
	}

	// Start periodic cleanup of peer maps
	s.startPeerMapCleanup(ctx)

	// Start sync coordinator (it handles all sync logic internally)
	if s.syncCoordinator != nil {
		s.syncCoordinator.Start(ctx)
	}

	// Start node status publisher
	go s.publishNodeStatus(ctx)

	apiKey := s.settings.GRPCAdminAPIKey
	if apiKey == "" {
		// Generate a random API key if not provided
		apiKey, err = generateRandomKey()
		if err != nil {
			return errors.NewServiceError("error generating random API key", err)
		}
	}

	// Define protected methods - use the full gRPC method path
	protectedMethods := map[string]bool{
		"/p2p_api.PeerService/BanPeer":   true,
		"/p2p_api.PeerService/UnbanPeer": true,
	}

	// Create auth options
	authOptions := &util.AuthOptions{
		APIKey:           apiKey,
		ProtectedMethods: protectedMethods,
	}

	// this will block
	if err = util.StartGRPCServer(ctx, s.logger, s.settings, "p2p", s.settings.P2P.GRPCListenAddress, func(server *grpc.Server) {
		p2p_api.RegisterPeerServiceServer(server, s)
		closeOnce.Do(func() { close(readyCh) })
	}, authOptions); err != nil {
		return errors.WrapGRPC(errors.NewServiceNotStartedError("[P2P] can't start GRPC server", err))
	}

	<-ctx.Done()

	return nil
}

func (s *Server) subscribeToTopic(ctx context.Context, topicName string, handler func(context.Context, []byte, string)) {
	topicChannel := s.P2PClient.Subscribe(topicName)
	go func() {
		// Process messages until the topic channel is closed
		// DO NOT check ctx.Done() here - context cancellation during operations like Kafka consumer recovery
		// should not stop P2P message processing. The subscription ends when the topic channel closes.
		for msg := range topicChannel {
			handler(ctx, msg.Data, msg.From)
		}
		s.logger.Warnf("%s topic channel closed", topicName)
	}()
}

func (s *Server) invalidSubtreeHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		var (
			syncing bool
			err     error
		)

		if syncing, err = s.isBlockchainSyncingOrCatchingUp(ctx); err != nil {
			return err
		}

		if syncing {
			return nil
		}

		var m kafkamessage.KafkaInvalidSubtreeTopicMessage
		if err = proto.Unmarshal(msg.Value, &m); err != nil {
			s.logger.Errorf("[invalidSubtreeHandler] error unmarshalling invalidSubtreeMessage: %v", err)
			return err
		}

		s.logger.Infof("[invalidSubtreeHandler] Received invalid subtree notification via Kafka: hash=%s, peerUrl=%s, reason=%s",
			m.SubtreeHash, m.PeerUrl, m.Reason)

		// Use the existing ReportInvalidSubtree method to handle the invalid subtree
		err = s.ReportInvalidSubtree(ctx, m.SubtreeHash, m.PeerUrl, m.Reason)
		if err != nil {
			// Don't return error here, as we want to continue processing messages
			s.logger.Errorf("[invalidSubtreeHandler] Failed to report invalid subtree from Kafka: %v", err)
		}

		return nil
	}
}

func (s *Server) invalidBlockHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		var (
			syncing bool
			err     error
		)

		if syncing, err = s.isBlockchainSyncingOrCatchingUp(ctx); err != nil {
			return err
		}

		if syncing {
			return nil
		}

		var m kafkamessage.KafkaInvalidBlockTopicMessage
		if err := proto.Unmarshal(msg.Value, &m); err != nil {
			s.logger.Errorf("[invalidBlockHandler] error unmarshalling invalidBlocksMessage: %v", err)
			return err
		}

		s.logger.Infof("[invalidBlockHandler] Received invalid block notification via Kafka: %s, reason: %s", m.BlockHash, m.Reason)

		// Use the existing ReportInvalidBlock method to handle the invalid block
		err = s.ReportInvalidBlock(ctx, m.BlockHash, m.Reason)
		if err != nil {
			// Don't return error here, as we want to continue processing messages
			s.logger.Errorf("[invalidBlockHandler] Failed to report invalid block from Kafka: %v", err)
		}

		return nil
	}
}

func (s *Server) rejectedTxHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		if s.settings.P2P.ListenMode == settings.ListenModeListenOnly {
			return nil
		}

		var (
			syncing bool
			err     error
		)

		if syncing, err = s.isBlockchainSyncingOrCatchingUp(ctx); err != nil {
			return err
		}

		if syncing {
			return nil
		}

		var m kafkamessage.KafkaRejectedTxTopicMessage
		if err := proto.Unmarshal(msg.Value, &m); err != nil {
			s.logger.Errorf("[rejectedTxHandler] error unmarshalling rejectedTxMessage: %v", err)
			return err
		}

		hash, err := chainhash.NewHashFromStr(m.TxHash)
		if err != nil {
			s.logger.Errorf("[rejectedTxHandler] error getting chainhash from string %s: %v", m.TxHash, err)
			return err
		}

		// Check if this is an internal rejection (empty peer_id) or external (non-empty peer_id)
		if m.PeerId != "" {
			// External rejection from another peer - already broadcast by that peer
			s.logger.Debugf("[rejectedTxHandler] Received external rejected tx notification for %s from peer %s: %s (not re-broadcasting)",
				hash.String(), m.PeerId, m.Reason)
			return nil
		}

		// Internal rejection from our Validator - broadcast to p2p network
		s.logger.Debugf("[rejectedTxHandler] Received internal rejected tx notification for %s: %s (broadcasting to p2p network)",
			hash.String(), m.Reason)

		rejectedTxMessage := RejectedTxMessage{
			TxID:   hash.String(),
			Reason: m.Reason,
			PeerID: s.P2PClient.GetID(),
		}

		msgBytes, err := json.Marshal(rejectedTxMessage)
		if err != nil {
			s.logger.Errorf("[rejectedTxHandler] json marshal error: %v", err)

			return err
		}

		s.logger.Debugf("[rejectedTxHandler] publishing rejectedTxMessage to p2p network")

		if err = s.P2PClient.Publish(ctx, s.rejectedTxTopicName, msgBytes); err != nil {
			s.logger.Errorf("[rejectedTxHandler] publish error: %v", err)
		}

		return nil
	}
}

func (s *Server) disconnectPreExistingBannedPeers(ctx context.Context) {
	for _, banned := range s.banList.ListBanned() {
		s.handleBanEvent(ctx, BanEvent{Action: banActionAdd, IP: banned})
	}
}

func generateRandomKey() (string, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return "", errors.WrapGRPC(errors.NewServiceNotStartedError("[P2P] failed to generate API key", err))
	}

	apiKey := hex.EncodeToString(key)

	return apiKey, nil
}

// updatePeerLastMessageTime updates the last message time for both the sender and originator.
// It handles the common pattern of updating message timestamps when receiving P2P messages.
// Parameters:
//   - from: the immediate sender's peer ID string
//   - originatorPeerID: the original message creator's peer ID string (may be same as from)
func (s *Server) updatePeerLastMessageTime(from string, originatorPeerID string) {
	if s.peerRegistry == nil {
		return
	}

	// Mark sender as connected and update last message time
	// The sender is the peer we're directly connected to
	senderID := peer.ID(from)
	s.addConnectedPeer(senderID)
	s.peerRegistry.UpdateLastMessageTime(senderID)

	// Also update for the originator if different (gossiped message)
	// The originator is not directly connected to us
	if originatorPeerID != "" {
		if peerID, err := peer.Decode(originatorPeerID); err == nil && peerID != senderID {
			// Add as gossiped peer (not connected)
			s.addPeer(peerID)
			s.peerRegistry.UpdateLastMessageTime(peerID)
		}
	}
}

func (s *Server) handleNodeStatusTopic(_ context.Context, m []byte, from string) {
	var nodeStatusMessage NodeStatusMessage

	if err := json.Unmarshal(m, &nodeStatusMessage); err != nil {
		s.logger.Errorf("[handleNodeStatusTopic] json unmarshal error: %v", err)
		return
	}

	// Check if this is our own message
	isSelf := from == s.P2PClient.GetID()

	// Log all received node_status messages for debugging
	if from == nodeStatusMessage.PeerID {
		s.logger.Infof("[handleNodeStatusTopic] DIRECT node_status from %s (is_self: %v, version: %s, height: %d, storage: %q)",
			nodeStatusMessage.PeerID, isSelf, nodeStatusMessage.Version, nodeStatusMessage.BestHeight, nodeStatusMessage.Storage)
	} else {
		s.logger.Infof("[handleNodeStatusTopic] RELAY  node_status (originator: %s, via: %s, is_self: %v, version: %s, height: %d, storage: %q)",
			nodeStatusMessage.PeerID, from, isSelf, nodeStatusMessage.Version, nodeStatusMessage.BestHeight, nodeStatusMessage.Storage)
	}
	s.logger.Debugf("[handleNodeStatusTopic] Received JSON: %s", string(m))

	// Skip further processing for our own messages (peer height updates, etc.)
	// but still forward to WebSocket
	if !isSelf {
		s.logger.Debugf("[handleNodeStatusTopic] Processing node_status from remote peer %s (peer_id: %s)", from, nodeStatusMessage.PeerID)

		// Update last message time for the sender and originator
		s.updatePeerLastMessageTime(from, nodeStatusMessage.PeerID)

		// Skip processing from unhealthy peers (but still forward to WebSocket for monitoring)
		if s.shouldSkipUnhealthyPeer(from, "handleNodeStatusTopic") {
			s.logger.Debugf("[handleNodeStatusTopic] Skipping peer data processing from unhealthy peer %s, but forwarding to WebSocket", from)
			// Set isSelf to true to skip peer data updates below while still forwarding to WebSocket
			isSelf = true
		}
	} else {
		s.logger.Debugf("[handleNodeStatusTopic] forwarding our own node status (peer_id: %s) with is_self=true", nodeStatusMessage.PeerID)
	}

	// Send to notification channel for WebSocket clients
	select {
	case s.notificationCh <- &notificationMsg{
		Timestamp:           time.Now().UTC().Format(isoFormat),
		Type:                "node_status",
		BaseURL:             nodeStatusMessage.BaseURL,
		PeerID:              nodeStatusMessage.PeerID,
		Version:             nodeStatusMessage.Version,
		CommitHash:          nodeStatusMessage.CommitHash,
		BestBlockHash:       nodeStatusMessage.BestBlockHash,
		BestHeight:          nodeStatusMessage.BestHeight,
		TxCount:             nodeStatusMessage.TxCount,
		SubtreeCount:        nodeStatusMessage.SubtreeCount,
		FSMState:            nodeStatusMessage.FSMState,
		StartTime:           nodeStatusMessage.StartTime,
		Uptime:              nodeStatusMessage.Uptime,
		ClientName:          nodeStatusMessage.ClientName,
		MinerName:           nodeStatusMessage.MinerName,
		ListenMode:          nodeStatusMessage.ListenMode,
		ChainWork:           nodeStatusMessage.ChainWork,
		SyncPeerID:          nodeStatusMessage.SyncPeerID,
		SyncPeerHeight:      nodeStatusMessage.SyncPeerHeight,
		SyncPeerBlockHash:   nodeStatusMessage.SyncPeerBlockHash,
		SyncConnectedAt:     nodeStatusMessage.SyncConnectedAt,
		MinMiningTxFee:      nodeStatusMessage.MinMiningTxFee,
		ConnectedPeersCount: nodeStatusMessage.ConnectedPeersCount,
		Storage:             nodeStatusMessage.Storage,
	}:
	default:
		s.logger.Warnf("[handleNodeStatusTopic] notification channel full, dropped node_status notification for %s", nodeStatusMessage.PeerID)
	}

	// Update peer height if provided (but not for our own messages)
	if !isSelf && nodeStatusMessage.BestHeight > 0 && nodeStatusMessage.PeerID != "" {
		if peerID, err := peer.Decode(nodeStatusMessage.PeerID); err == nil {
			// Ensure this peer is in the registry
			s.addPeer(peerID)

			// Update sync manager with peer height from node status
			// Update peer height in registry
			s.updatePeerHeight(peerID, int32(nodeStatusMessage.BestHeight))

			// Update DataHubURL if provided in the node status message
			// This is important for peers we learn about through gossip (not directly connected).
			// When we receive node_status messages forwarded by other peers, we still need to
			// store the DataHubURL so we can potentially sync from them later if we establish
			// a direct connection.
			if nodeStatusMessage.BaseURL != "" {
				s.updateDataHubURL(peerID, nodeStatusMessage.BaseURL)
				s.logger.Debugf("[handleNodeStatusTopic] Updated DataHub URL %s for peer %s", nodeStatusMessage.BaseURL, peerID)
			}

			// Update block hash if provided
			// Similar to DataHubURL, we store the best block hash from gossiped peers
			// to maintain a complete picture of the network state
			if nodeStatusMessage.BestBlockHash != "" {
				s.updateBlockHash(peerID, nodeStatusMessage.BestBlockHash)
				s.logger.Debugf("[handleNodeStatusTopic] Updated block hash %s for peer %s", nodeStatusMessage.BestBlockHash, peerID)
			}

			// Update storage mode if provided
			// Store whether the peer is a full node or pruned node
			if nodeStatusMessage.Storage != "" {
				s.updateStorage(peerID, nodeStatusMessage.Storage)
				s.logger.Debugf("[handleNodeStatusTopic] Updated storage mode to %s for peer %s", nodeStatusMessage.Storage, peerID)
			}
		}
	}

	// Also ensure the sender is in the registry
	if !isSelf && from != "" {
		if senderID, err := peer.Decode(from); err == nil {
			s.addPeer(senderID)
		}
	}
}

func (s *Server) handleBlockNotification(ctx context.Context, hash *chainhash.Hash) error {
	if s.settings.P2P.ListenMode == settings.ListenModeListenOnly {
		return nil
	}

	var msgBytes []byte

	h, meta, err := s.blockchainClient.GetBlockHeader(ctx, hash)
	if err != nil {
		return errors.NewError("error getting block header and meta for BlockMessage: %w", err)
	}

	if meta.Invalid {
		// do not announce invalid blocks
		s.logger.Infof("[handleBlockNotification] Not announcing invalid block %s", hash.String())
		return nil
	}

	blockMessage := BlockMessage{
		Hash:       hash.String(),
		Height:     meta.Height,
		DataHubURL: s.AssetHTTPAddressURL,
		PeerID:     s.P2PClient.GetID(),
		Header:     hex.EncodeToString(h.Bytes()),
		ClientName: s.settings.ClientName,
	}

	msgBytes, err = json.Marshal(blockMessage)
	if err != nil {
		return errors.NewError("blockMessage - json marshal error: %w", err)
	}

	if err = s.P2PClient.Publish(ctx, s.blockTopicName, msgBytes); err != nil {
		return errors.NewError("blockMessage - publish error: %w", err)
	}

	// Also send a node_status update when best block changes
	if err = s.handleNodeStatusNotification(ctx); err != nil {
		// Log the error but don't fail the block notification
		s.logger.Warnf("[handleBlockNotification] error sending node status update: %v", err)
	}

	return nil
}

func (s *Server) publishNodeStatus(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Publish initial status immediately
	if err := s.handleNodeStatusNotification(ctx); err != nil {
		s.logger.Errorf("[publishNodeStatus] error sending initial node status: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[publishNodeStatus] node status publisher shutting down")
			return
		case <-ticker.C:
			if err := s.handleNodeStatusNotification(ctx); err != nil {
				s.logger.Errorf("[publishNodeStatus] error sending node status: %v", err)
			}
		}
	}
}

// getNodeStatusMessage creates a notification message with the current node's status.
// This is used both for periodic broadcasts and for sending to newly connected WebSocket clients.
func (s *Server) getNodeStatusMessage(ctx context.Context) *notificationMsg {
	// Get best block info
	var bestBlockHeader *model.BlockHeader
	var bestBlockMeta *model.BlockHeaderMeta
	var err error

	if s.blockchainClient != nil {
		bestBlockHeader, bestBlockMeta, err = s.blockchainClient.GetBestBlockHeader(ctx)
	}
	if err != nil {
		s.logger.Errorf("[handleNodeStatusNotification] error getting best block header: %s", err)
		// Use genesis block as fallback when we can't get the best block
		bestBlockHeader = model.GenesisBlockHeader
		bestBlockMeta = model.GenesisBlockHeaderMeta
	}

	// Calculate uptime
	uptime := time.Since(s.startTime).Seconds()

	// Get FSM state from blockchain client
	fsmState := "UNKNOWN"
	if s.blockchainClient != nil {
		currentState, err := s.blockchainClient.GetFSMCurrentState(ctx)
		if err != nil {
			s.logger.Warnf("[handleNodeStatusNotification] error getting FSM state: %s", err)
		} else if currentState != nil {
			// Convert FSMStateType to string
			fsmState = currentState.String()
		}
	}

	// Get client name from settings
	clientName := ""
	if s.settings != nil {
		clientName = s.settings.ClientName
	}

	// Get miner name from the best block metadata
	minerName := ""
	if bestBlockMeta != nil {
		minerName = bestBlockMeta.Miner
	}

	// Get block hash string
	blockHashStr := ""
	if bestBlockHeader != nil {
		hash := bestBlockHeader.Hash()
		if hash != nil {
			blockHashStr = hash.String()
		}
	}

	// Get height
	height := uint32(0)
	if bestBlockMeta != nil {
		height = bestBlockMeta.Height
	}

	// Get chainwork
	chainWorkStr := ""
	if bestBlockMeta != nil && bestBlockMeta.ChainWork != nil {
		chainWorkStr = hex.EncodeToString(bestBlockMeta.ChainWork)
	}

	// Get sync peer information
	syncPeerID := ""
	syncPeerHeight := int32(0)
	syncPeerBlockHash := ""
	syncConnectedAt := int64(0)

	// Get current sync peer
	syncPeer := s.getSyncPeer()
	if syncPeer != "" {
		if syncPeer != "" {
			syncPeerID = syncPeer.String()

			// Track when we first connected to this sync peer
			if existingTime, ok := s.syncConnectionTimes.Load(syncPeerID); ok {
				syncConnectedAt = existingTime.(int64)
			} else {
				// First time connecting to this sync peer
				syncConnectedAt = time.Now().Unix()
				s.syncConnectionTimes.Store(syncPeerID, syncConnectedAt)
				s.logger.Debugf("[handleNodeStatusNotification] Recording sync connection time for peer %s: %d", syncPeerID, syncConnectedAt)
			}

			// Get sync peer's height and block hash
			for _, peerInfo := range s.P2PClient.GetPeers() {
				if peerInfo.ID == syncPeer.String() {
					// Get the peer's best block hash from registry
					if pInfo, exists := s.getPeer(syncPeer); exists {
						syncPeerBlockHash = pInfo.BlockHash
						syncPeerHeight = pInfo.Height
					}
					break
				}
			}
		} else {
			// No sync peer - clear any old connection time tracking
			s.syncConnectionTimes.Range(func(key, value interface{}) bool {
				s.syncConnectionTimes.Delete(key)
				return true
			})
		}
	}

	// Get peer ID safely
	peerID := ""
	if s.P2PClient != nil {
		peerID = s.P2PClient.GetID()
	}

	// Get version, commit, and listen mode safely
	version := ""
	commit := ""
	listenMode := ""

	if s.settings != nil {
		version = s.settings.Version
		commit = s.settings.Commit
		listenMode = s.settings.P2P.ListenMode
	}

	// Get start time safely
	startTime := int64(0)
	if !s.startTime.IsZero() {
		startTime = s.startTime.Unix()
	}

	// Set empty baseURL if in listen only mode
	baseURL := s.AssetHTTPAddressURL
	if s.settings.P2P.ListenMode == settings.ListenModeListenOnly {
		baseURL = ""
	}

	// Get minimum mining transaction fee from settings
	// Use a pointer to distinguish between nil (unknown) and 0 (no fee)
	var minMiningTxFee *float64
	if s.settings != nil && s.settings.Policy != nil {
		fee := s.settings.Policy.GetMinMiningTxFee()
		minMiningTxFee = &fee
		s.logger.Debugf("[getNodeStatusMessage] MinMiningTxFee from settings: %f", fee)
	} else {
		// For our own node, we always know the fee (even if it's 0)
		// Only leave nil for messages from other peers
		defaultFee := float64(0)
		minMiningTxFee = &defaultFee
		s.logger.Debugf("[getNodeStatusMessage] Policy settings not available, using default MinMiningTxFee: %f", defaultFee)
	}

	// Get connected peers count from the registry
	connectedPeersCount := 0
	if s.peerRegistry != nil {
		allPeers := s.peerRegistry.GetAllPeers()
		connectedPeersCount = len(allPeers)
	}

	// Get block assembly state (tx count and subtree count)
	txCount := uint64(0)
	subtreeCount := uint32(0)
	if s.blockAssemblyClient != nil {
		if state, err := s.blockAssemblyClient.GetBlockAssemblyState(ctx); err == nil && state != nil {
			txCount = state.TxCount
			subtreeCount = state.SubtreeCount
		} else if err != nil {
			s.logger.Debugf("[getNodeStatusMessage] Failed to get block assembly state: %v", err)
		}
	}

	// Determine storage mode (full vs pruned) based on block persister status
	storage := s.determineStorage(ctx, height)
	s.logger.Infof("[getNodeStatusMessage] Determined storage=%q for this node at height %d", storage, height)

	// Return the notification message
	return &notificationMsg{
		Timestamp:           time.Now().UTC().Format(isoFormat),
		Type:                "node_status",
		BaseURL:             baseURL,
		PeerID:              peerID,
		Version:             version,
		CommitHash:          commit,
		BestBlockHash:       blockHashStr,
		BestHeight:          height,
		TxCount:             txCount,
		SubtreeCount:        subtreeCount,
		FSMState:            fsmState,
		StartTime:           startTime,
		Uptime:              uptime,
		ClientName:          clientName,
		MinerName:           minerName,
		ListenMode:          listenMode,
		ChainWork:           chainWorkStr,
		SyncPeerID:          syncPeerID,
		SyncPeerHeight:      syncPeerHeight,
		SyncPeerBlockHash:   syncPeerBlockHash,
		SyncConnectedAt:     syncConnectedAt,
		MinMiningTxFee:      minMiningTxFee,
		ConnectedPeersCount: connectedPeersCount,
		Storage:             storage,
	}
}

// determineStorage determines whether this node is a full node or pruned node.
// A full node has the block persister running and within the retention window (default: 288 blocks).
// Since data isn't purged until older than the retention period, a node can serve as "full"
// as long as the persister lag is within this window.
// A pruned node either doesn't have block persister running or it's lagging beyond the retention window.
// Always returns "full" or "pruned" - never returns empty string.
func (s *Server) determineStorage(ctx context.Context, bestHeight uint32) (mode string) {
	if s.blockchainClient == nil {
		return "pruned"
	}

	// Check if context is already canceled (e.g., during test shutdown)
	select {
	case <-ctx.Done():
		return "pruned"
	default:
	}

	// Handle mock panics gracefully in tests
	defer func() {
		if r := recover(); r != nil {
			// Classify as pruned for safety
			mode = "pruned"
		}
	}()

	// Query block persister height from blockchain state
	stateData, err := s.blockchainClient.GetState(ctx, "BlockPersisterHeight")
	if err != nil || len(stateData) < 4 {
		// Block persister not running or state not available - classify as pruned
		return "pruned"
	}

	// Decode persisted height (little-endian uint32)
	persistedHeight := binary.LittleEndian.Uint32(stateData)

	// Calculate lag
	var lag uint32
	if bestHeight > persistedHeight {
		lag = bestHeight - persistedHeight
	} else {
		lag = 0
	}

	// Get lag threshold from GlobalBlockHeightRetention
	// Since data isn't purged until it's older than this retention window, the node can still
	// serve as a full node as long as the persister is within this retention period.
	lagThreshold := uint32(288) // Default 2 days of blocks (144 blocks/day * 2)
	if s.settings != nil && s.settings.GlobalBlockHeightRetention > 0 {
		lagThreshold = s.settings.GlobalBlockHeightRetention
	}

	// Determine mode based on retention window
	// If BlockPersister is within the retention window, node is "full"
	// If BlockPersister lags beyond the retention window, node is "pruned"
	if lag <= lagThreshold {
		return "full"
	}

	return "pruned"
}

func (s *Server) handleNodeStatusNotification(ctx context.Context) error {
	// Get the node status message
	msg := s.getNodeStatusMessage(ctx)
	if msg == nil {
		return errors.NewError("failed to get node status message", nil)
	}

	// Create the NodeStatusMessage for P2P publishing
	nodeStatusMessage := NodeStatusMessage{
		Type:                "node_status",
		BaseURL:             msg.BaseURL,
		PeerID:              msg.PeerID,
		Version:             msg.Version,
		CommitHash:          msg.CommitHash,
		BestBlockHash:       msg.BestBlockHash,
		BestHeight:          msg.BestHeight,
		TxCount:             msg.TxCount,
		SubtreeCount:        msg.SubtreeCount,
		FSMState:            msg.FSMState,
		StartTime:           msg.StartTime,
		Uptime:              msg.Uptime,
		ClientName:          msg.ClientName,
		MinerName:           msg.MinerName,
		ListenMode:          msg.ListenMode,
		ChainWork:           msg.ChainWork,
		SyncPeerID:          msg.SyncPeerID,
		SyncPeerHeight:      msg.SyncPeerHeight,
		SyncPeerBlockHash:   msg.SyncPeerBlockHash,
		SyncConnectedAt:     msg.SyncConnectedAt,
		MinMiningTxFee:      msg.MinMiningTxFee,
		ConnectedPeersCount: msg.ConnectedPeersCount,
		Storage:             msg.Storage,
	}

	msgBytes, err := json.Marshal(nodeStatusMessage)
	if err != nil {
		return errors.NewError("nodeStatusMessage - json marshal error: %w", err)
	}

	s.logger.Infof("[handleNodeStatusNotification] P2P publishing node_status to topic %s (height=%d, version=%s, storage=%q)", s.nodeStatusTopicName, nodeStatusMessage.BestHeight, nodeStatusMessage.Version, nodeStatusMessage.Storage)
	s.logger.Debugf("[handleNodeStatusNotification] JSON payload: %s", string(msgBytes))

	if err = s.P2PClient.Publish(ctx, s.nodeStatusTopicName, msgBytes); err != nil {
		return errors.NewError("nodeStatusMessage - publish error: %w", err)
	}

	s.logger.Debugf("[handleNodeStatusNotification] Successfully published node_status message")

	// Send to local WebSocket clients
	select {
	case s.notificationCh <- msg:
	default:
		s.logger.Warnf("[handleNodeStatusNotification] notification channel full, dropped node_status notification for %s", msg.PeerID)
	}

	return nil
}

func (s *Server) handleSubtreeNotification(ctx context.Context, hash *chainhash.Hash) error {
	if s.settings.P2P.ListenMode == settings.ListenModeListenOnly {
		return nil
	}

	var msgBytes []byte

	subtreeMessage := SubtreeMessage{
		Hash:       hash.String(),
		DataHubURL: s.AssetHTTPAddressURL,
		PeerID:     s.P2PClient.GetID(),
		ClientName: s.settings.ClientName,
	}

	msgBytes, err := json.Marshal(subtreeMessage)
	if err != nil {
		return errors.NewError("subtreeMessage - json marshal error: %w", err)
	}

	if err := s.P2PClient.Publish(ctx, s.subtreeTopicName, msgBytes); err != nil {
		return errors.NewError("subtreeMessage - publish error: %w", err)
	}

	return nil
}

func (s *Server) handlePeerFailureNotification(ctx context.Context, notification *blockchain.Notification) error {
	// Extract failure details from metadata
	if notification.Metadata == nil || notification.Metadata.Metadata == nil {
		s.logger.Warnf("[handlePeerFailureNotification] Received PeerFailure notification with no metadata")
		return nil
	}

	peerID := notification.Metadata.Metadata["peer_id"]
	failureType := notification.Metadata.Metadata["failure_type"]
	reason := notification.Metadata.Metadata["reason"]

	s.logger.Infof("[handlePeerFailureNotification] Peer %s failed: type=%s, reason=%s", peerID, failureType, reason)

	// For catchup failures, trigger peer switch via sync coordinator
	if failureType == "catchup" && s.syncCoordinator != nil {
		s.syncCoordinator.HandleCatchupFailure(reason)
	}

	return nil
}

func (s *Server) processBlockchainNotification(ctx context.Context, notification *blockchain.Notification) error {
	hash, err := chainhash.NewHash(notification.Hash)
	if err != nil {
		// Specific error about hash conversion, not logged here, but returned to caller.
		return errors.NewError(fmt.Sprintf("error getting chainhash from notification hash %s: %%w", notification.Hash), err)
	}

	s.logger.Debugf("[processBlockchainNotification] Processing %s notification: %s", notification.Type, hash.String())

	switch notification.Type {
	case model.NotificationType_Block:
		return s.handleBlockNotification(ctx, hash) // These handlers return wrapped errors
	case model.NotificationType_Subtree:
		return s.handleSubtreeNotification(ctx, hash)
	case model.NotificationType_PeerFailure:
		return s.handlePeerFailureNotification(ctx, notification)
	default:
		s.logger.Warnf("[processBlockchainNotification] Received unhandled notification type: %s for hash %s", notification.Type, hash.String())
	}

	return nil // For unhandled types, not an error that stops the listener
}

func (s *Server) blockchainSubscriptionListener(ctx context.Context, blockchainSubscription <-chan *blockchain.Notification) {

	// define vars here to prevent too many allocs
	var notification *blockchain.Notification

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[blockchainSubscriptionListener] P2P service shutting down")
			return
		case notification = <-blockchainSubscription:
			if notification == nil {
				continue
			}

			var (
				syncing bool
				err     error
			)

			if syncing, err = s.isBlockchainSyncingOrCatchingUp(ctx); err != nil {
				s.logger.Errorf("[blockchainSubscriptionListener] error getting blockchain FSM state: %v", err)

				continue
			}

			// Process PeerFailure notifications even during sync (needed to switch peers on catchup failure)
			if syncing && notification.Type != model.NotificationType_PeerFailure {
				continue
			}

			// received a message
			if err := s.processBlockchainNotification(ctx, notification); err != nil {
				s.logger.Errorf("[blockchainSubscriptionListener] Error processing notification (Type: %s, Hash: %s): %v", notification.Type, notification.Hash, err)
				continue // Continue to next notification on error
			}
		}
	}
}

// StartHTTP starts the HTTP server component of the P2P server.
func (s *Server) StartHTTP(ctx context.Context) error {
	addr := s.settings.P2P.HTTPListenAddress
	if addr == "" {
		s.logger.Errorf("[StartHTTP] p2p HTTP listen address is not set")
		return errors.NewConfigurationError("p2p HTTP listen address is not set")
	}

	// Get listener using util.GetListener
	listener, address, _, err := util.GetListener(s.settings.Context, "p2p", "http://", addr)
	if err != nil {
		return errors.NewServiceError("[StartHTTP] failed to get listener", err)
	}

	s.logger.Infof("[StartHTTP] p2p service listening on %s", address)
	s.e.Listener = listener

	go func() {
		<-ctx.Done()
		s.logger.Infof("[StartHTTP] p2p service shutting down")

		if err := s.e.Shutdown(ctx); err != nil {
			s.logger.Errorf("[StartHTTP] p2p service shutdown error: %v", err)
		}
	}()

	go func() {
		defer util.RemoveListener(s.settings.Context, "p2p", "http://")

		if s.settings.SecurityLevelHTTP == 0 {
			servicemanager.AddListenerInfo(fmt.Sprintf("[StartHTTP] p2p HTTP listening on %s", address))
			err = s.e.Server.Serve(listener)
		} else {
			certFile := s.settings.ServerCertFile
			if certFile == "" {
				s.logger.Errorf("server_certFile is required for HTTPS")
				return
			}

			keyFile := s.settings.ServerKeyFile
			if keyFile == "" {
				s.logger.Errorf("server_keyFile is required for HTTPS")
				return
			}

			servicemanager.AddListenerInfo(fmt.Sprintf("[StartHTTP] p2p HTTPS listening on %s", address))
			err = s.e.Server.ServeTLS(listener, certFile, keyFile)
		}

		if err != nil && err != http.ErrServerClosed {
			s.logger.Errorf("[StartHTTP] server error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the P2P server and its components.
// This method coordinates an orderly shutdown of all P2P service components, including:
// - Stopping the underlying libp2p P2P node
// - Closing Kafka consumer connections
// - Shutting down the HTTP server
//
// The method attempts to stop all components even if some fail, collecting errors along the way.
// If multiple errors occur during shutdown, the first error is returned.
//
// Context cancellation is honored for time-bound shutdown operations.
//
// Returns any error encountered during the shutdown process, or nil if all components
// shut down successfully.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Infof("[Stop] Stopping P2P service")

	var errs []error

	// Stop the underlying P2P node
	if s.P2PClient != nil {
		if err := s.P2PClient.Close(); err != nil {
			s.logger.Errorf("[Stop] failed to stop P2P node: %v", err)
			errs = append(errs, err)
		}
	}

	// close the kafka consumers gracefully
	if s.rejectedTxKafkaConsumerClient != nil {
		if err := s.rejectedTxKafkaConsumerClient.Close(); err != nil {
			s.logger.Errorf("[Stop] failed to close rejected tx kafka consumer gracefully: %v", err)
			errs = append(errs, err)
		}
	}

	if s.invalidBlocksKafkaConsumerClient != nil {
		if err := s.invalidBlocksKafkaConsumerClient.Close(); err != nil {
			s.logger.Errorf("[Stop] failed to close invalid blocks kafka consumer gracefully: %v", err)
			errs = append(errs, err)
		}
	}

	if s.e != nil {
		if err := s.e.Shutdown(ctx); err != nil {
			s.logger.Errorf("[Stop] failed to shutdown Echo server: %v", err)
			errs = append(errs, err)
		}
	}

	// Stop the peer map cleanup ticker
	if s.peerMapCleanupTicker != nil {
		s.peerMapCleanupTicker.Stop()
		s.logger.Infof("[Stop] stopped peer map cleanup ticker")
	}

	// Clear the peer maps to free memory
	s.blockPeerMap.Range(func(key, value interface{}) bool {
		s.blockPeerMap.Delete(key)
		return true
	})
	s.subtreePeerMap.Range(func(key, value interface{}) bool {
		s.subtreePeerMap.Delete(key)
		return true
	})
	s.logger.Infof("[Stop] cleared peer maps")

	if len(errs) > 0 {
		// Combine errors if multiple occurred
		// This simple approach just returns the first error, consider a multi-error type if needed
		return errs[0]
	}

	return nil
}

func (s *Server) handleBlockTopic(_ context.Context, m []byte, from string) {
	var (
		blockMessage BlockMessage
		hash         *chainhash.Hash
		err          error
	)

	// decode request
	blockMessage = BlockMessage{}

	err = json.Unmarshal(m, &blockMessage)
	if err != nil {
		s.logger.Errorf("[handleBlockTopic] json unmarshal error: %v", err)
		return
	}

	if from == blockMessage.PeerID {
		s.logger.Infof("[handleBlockTopic] DIRECT block %s from %s", blockMessage.Hash, blockMessage.PeerID)
	} else {
		s.logger.Infof("[handleBlockTopic] RELAY  block %s (originator: %s, via: %s)", blockMessage.Hash, blockMessage.PeerID, from)
	}

	select {
	case s.notificationCh <- &notificationMsg{
		Timestamp:  time.Now().UTC().Format(isoFormat),
		Type:       "block",
		Hash:       blockMessage.Hash,
		Height:     blockMessage.Height,
		BaseURL:    blockMessage.DataHubURL,
		PeerID:     blockMessage.PeerID,
		ClientName: blockMessage.ClientName,
	}:
	default:
		s.logger.Warnf("[handleBlockTopic] notification channel full, dropped block notification for %s", blockMessage.Hash)
	}

	// Ignore our own messages
	if s.isOwnMessage(from, blockMessage.PeerID) {
		s.logger.Debugf("[handleBlockTopic] ignoring own block message for %s", blockMessage.Hash)
		return
	}

	// Update last message time for the sender and originator
	s.updatePeerLastMessageTime(from, blockMessage.PeerID)

	// Skip notifications from banned peers
	if s.shouldSkipBannedPeer(from, "handleBlockTopic") {
		return
	}

	// Skip notifications from unhealthy peers
	if s.shouldSkipUnhealthyPeer(from, "handleBlockTopic") {
		return
	}

	now := time.Now().UTC()

	hash, err = s.parseHash(blockMessage.Hash, "handleBlockTopic")
	if err != nil {
		return
	}

	// Store the peer ID that sent this block
	s.storePeerMapEntry(&s.blockPeerMap, blockMessage.Hash, from, now)
	s.logger.Debugf("[handleBlockTopic] storing peer %s for block %s", from, blockMessage.Hash)

	// Store the peer's latest block hash from block announcement
	if blockMessage.Hash != "" {
		// Store using the originator's peer ID
		if peerID, err := peer.Decode(blockMessage.PeerID); err == nil {
			s.updateBlockHash(peerID, blockMessage.Hash)
			s.logger.Debugf("[handleBlockTopic] Stored latest block hash %s for peer %s", blockMessage.Hash, peerID)
		}
		// Also store using the immediate sender for redundancy
		s.updateBlockHash(peer.ID(from), blockMessage.Hash)
		s.logger.Debugf("[handleBlockTopic] Stored latest block hash %s for sender %s", blockMessage.Hash, from)
	}

	// Update peer height if provided
	if blockMessage.Height > 0 {
		// Update peer height in registry
		if peerID, err := peer.Decode(blockMessage.PeerID); err == nil {
			s.updatePeerHeight(peerID, int32(blockMessage.Height))
		}
	}

	// Always send block to kafka - let block validation service decide what to do based on sync state
	// send block to kafka, if configured
	if s.blocksKafkaProducerClient != nil {
		msg := &kafkamessage.KafkaBlockTopicMessage{
			Hash:   hash.String(),
			URL:    blockMessage.DataHubURL,
			PeerId: blockMessage.PeerID,
		}

		s.logger.Debugf("[handleBlockTopic] Sending block %s to Kafka", hash.String())

		value, err := proto.Marshal(msg)
		if err != nil {
			s.logger.Errorf("[handleBlockTopic] error marshaling KafkaBlockTopicMessage: %v", err)
			return
		}

		s.blocksKafkaProducerClient.Publish(&kafka.Message{
			Key:   hash.CloneBytes(),
			Value: value,
		})
	}
}

func (s *Server) handleSubtreeTopic(_ context.Context, m []byte, from string) {
	var (
		subtreeMessage SubtreeMessage
		hash           *chainhash.Hash
		err            error
	)

	// decode request
	subtreeMessage = SubtreeMessage{}

	err = json.Unmarshal(m, &subtreeMessage)
	if err != nil {
		s.logger.Errorf("[handleSubtreeTopic] json unmarshal error: %v", err)
		return
	}

	if from == subtreeMessage.PeerID {
		s.logger.Debugf("[handleSubtreeTopic] DIRECT subtree %s from %s", subtreeMessage.Hash, subtreeMessage.PeerID)
	} else {
		s.logger.Debugf("[handleSubtreeTopic] RELAY  subtree %s (originator: %s, via: %s)", subtreeMessage.Hash, subtreeMessage.PeerID, from)
	}

	if s.isBlacklistedBaseURL(subtreeMessage.DataHubURL) {
		s.logger.Errorf("[handleSubtreeTopic] Blocked subtree notification from blacklisted baseURL: %s", subtreeMessage.DataHubURL)
		return
	}

	now := time.Now().UTC()

	select {
	case s.notificationCh <- &notificationMsg{
		Timestamp:  now.Format(isoFormat),
		Type:       "subtree",
		Hash:       subtreeMessage.Hash,
		BaseURL:    subtreeMessage.DataHubURL,
		PeerID:     subtreeMessage.PeerID,
		ClientName: subtreeMessage.ClientName,
	}:
	default:
		s.logger.Warnf("[handleSubtreeTopic] notification channel full, dropped subtree notification for %s", subtreeMessage.Hash)
	}

	// Ignore our own messages
	if s.isOwnMessage(from, subtreeMessage.PeerID) {
		s.logger.Debugf("[handleSubtreeTopic] ignoring own subtree message for %s", subtreeMessage.Hash)
		return
	}

	// Update last message time for the sender and originator
	s.updatePeerLastMessageTime(from, subtreeMessage.PeerID)

	// Skip notifications from banned peers
	if s.shouldSkipBannedPeer(from, "handleSubtreeTopic") {
		s.logger.Debugf("[handleSubtreeTopic] skipping banned peer %s", from)
		return
	}

	// Skip notifications from unhealthy peers
	if s.shouldSkipUnhealthyPeer(from, "handleSubtreeTopic") {
		return
	}

	hash, err = s.parseHash(subtreeMessage.Hash, "handleSubtreeTopic")
	if err != nil {
		s.logger.Errorf("[handleSubtreeTopic] error parsing hash: %v", err)
		return
	}

	// Store the peer ID that sent this subtree
	s.storePeerMapEntry(&s.subtreePeerMap, subtreeMessage.Hash, from, now)
	s.logger.Debugf("[handleSubtreeTopic] storing peer %s for subtree %s", from, subtreeMessage.Hash)

	if s.subtreeKafkaProducerClient != nil { // tests may not set this
		msg := &kafkamessage.KafkaSubtreeTopicMessage{
			Hash:   hash.String(),
			URL:    subtreeMessage.DataHubURL,
			PeerId: subtreeMessage.PeerID,
		}

		value, err := proto.Marshal(msg)
		if err != nil {
			s.logger.Errorf("[handleSubtreeTopic] error marshaling KafkaSubtreeTopicMessage: %v", err)
			return
		}

		s.subtreeKafkaProducerClient.Publish(&kafka.Message{
			Key:   hash.CloneBytes(),
			Value: value,
		})
	}
}

// isBlacklistedBaseURL checks if the given baseURL matches any entry in the blacklist.
func (s *Server) isBlacklistedBaseURL(baseURL string) bool {
	inputHost := s.extractHost(baseURL)
	if inputHost == "" {
		// Fall back to exact string matching for invalid URLs
		for blocked := range s.settings.SubtreeValidation.BlacklistedBaseURLs {
			if baseURL == blocked {
				return true
			}
		}

		return false
	}

	// Check each blacklisted URL
	for blocked := range s.settings.SubtreeValidation.BlacklistedBaseURLs {
		blockedHost := s.extractHost(blocked)
		if blockedHost == "" {
			// Fall back to exact string matching for invalid blacklisted URLs
			if baseURL == blocked {
				return true
			}

			continue
		}

		if inputHost == blockedHost {
			return true
		}
	}

	return false
}

// extractHost extracts and normalizes the host component from a URL
func (s *Server) extractHost(urlStr string) string {
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return ""
	}

	host := parsedURL.Hostname()
	if host == "" {
		return ""
	}

	return strings.ToLower(host)
}

func (s *Server) handleRejectedTxTopic(_ context.Context, m []byte, from string) {
	var (
		rejectedTxMessage RejectedTxMessage
		err               error
	)

	rejectedTxMessage = RejectedTxMessage{}

	err = json.Unmarshal(m, &rejectedTxMessage)
	if err != nil {
		s.logger.Errorf("[handleRejectedTxTopic] json unmarshal error: %v", err)
		return
	}

	if from == rejectedTxMessage.PeerID {
		s.logger.Debugf("[handleRejectedTxTopic] DIRECT rejected tx %s from %s (reason: %s)",
			rejectedTxMessage.TxID, rejectedTxMessage.PeerID, rejectedTxMessage.Reason)
	} else {
		s.logger.Debugf("[handleRejectedTxTopic] RELAY  rejected tx %s (originator: %s, via: %s, reason: %s)",
			rejectedTxMessage.TxID, rejectedTxMessage.PeerID, from, rejectedTxMessage.Reason)
	}

	if s.isOwnMessage(from, rejectedTxMessage.PeerID) {
		s.logger.Debugf("[handleRejectedTxTopic] ignoring own rejected tx message for %s", rejectedTxMessage.TxID)
		return
	}

	s.updatePeerLastMessageTime(from, rejectedTxMessage.PeerID)

	if s.shouldSkipBannedPeer(from, "handleRejectedTxTopic") {
		return
	}

	// Skip notifications from unhealthy peers
	if s.shouldSkipUnhealthyPeer(from, "handleRejectedTxTopic") {
		return
	}

	// Rejected TX messages from other peers are informational only.
	// They help us understand network state but don't trigger re-broadcasting.
	// If we wanted to take action (e.g., remove from our mempool), we would do it here.
}

// GetPeers returns a list of connected peers.
func (s *Server) GetPeers(ctx context.Context, _ *emptypb.Empty) (*p2p_api.GetPeersResponse, error) {
	s.logger.Debugf("GetPeers called")

	if s.P2PClient == nil {
		return nil, errors.NewError("[GetPeers] P2PClient is not initialised")
	}

	s.logger.Debugf("Creating reply channel")
	serverPeers := s.P2PClient.GetPeers()

	resp := &p2p_api.GetPeersResponse{}

	for _, sp := range serverPeers {
		if sp.ID == "" || sp.Addrs == nil {
			continue
		}

		// ignore localhost
		if sp.ID == s.P2PClient.GetID() {
			continue
		}

		// ignore bootstrap server
		if contains(s.settings.P2P.BootstrapAddresses, sp.ID) {
			continue
		}

		banScore, _, _ := s.banManager.GetBanScore(sp.ID)

		var addr string

		if len(sp.Addrs) > 0 {
			// For GetPeers API, we always return connected peers regardless of address type
			// The SharePrivateAddresses setting only controls what we advertise to other peers,
			// not what we report in our own peer list
			addr = sp.Addrs[0]
		}

		// Include all connected peers
		if addr != "" {
			resp.Peers = append(resp.Peers, &p2p_api.Peer{
				Id:       sp.ID,
				Addr:     addr,
				Banscore: int32(banScore), //nolint:gosec
			})
		}
	}

	return resp, nil
}

func (s *Server) BanPeer(ctx context.Context, peer *p2p_api.BanPeerRequest) (*p2p_api.BanPeerResponse, error) {
	err := s.banList.Add(ctx, peer.Addr, time.Unix(peer.Until, 0))
	if err != nil {
		return nil, err
	}

	return &p2p_api.BanPeerResponse{Ok: true}, nil
}

func (s *Server) UnbanPeer(ctx context.Context, peer *p2p_api.UnbanPeerRequest) (*p2p_api.UnbanPeerResponse, error) {
	err := s.banList.Remove(ctx, peer.Addr)
	if err != nil {
		return nil, err
	}

	return &p2p_api.UnbanPeerResponse{Ok: true}, nil
}

func (s *Server) IsBanned(ctx context.Context, peer *p2p_api.IsBannedRequest) (*p2p_api.IsBannedResponse, error) {
	// Only check PeerID-based bans
	// Note: The field is still called IpOrSubnet for backward compatibility, but we only accept PeerIDs
	return &p2p_api.IsBannedResponse{IsBanned: s.banManager.IsBanned(peer.IpOrSubnet)}, nil
}

func (s *Server) ListBanned(ctx context.Context, _ *emptypb.Empty) (*p2p_api.ListBannedResponse, error) {
	return &p2p_api.ListBannedResponse{Banned: s.banList.ListBanned()}, nil
}

func (s *Server) ClearBanned(ctx context.Context, _ *emptypb.Empty) (*p2p_api.ClearBannedResponse, error) {
	s.banList.Clear()
	return &p2p_api.ClearBannedResponse{Ok: true}, nil
}

func (s *Server) AddBanScore(ctx context.Context, req *p2p_api.AddBanScoreRequest) (*p2p_api.AddBanScoreResponse, error) {
	// Map the reason string to a BanReason enum
	reason := ReasonUnknown

	switch req.Reason {
	case "invalid_subtree":
		reason = ReasonInvalidSubtree
	case "protocol_violation":
		reason = ReasonProtocolViolation
	case "spam":
		reason = ReasonSpam
	case "invalid_block":
		reason = ReasonInvalidBlock
	default:
		s.logger.Warnf("[AddBanScore] Unknown ban reason: %s", req.Reason)
	}

	score, banned := s.banManager.AddScore(req.PeerId, reason)
	s.logger.Infof("[AddBanScore] Added score to peer %s for reason %s. New score: %d, Banned: %t", req.PeerId, req.Reason, score, banned)

	// Update the sync coordinator's peer registry with the new ban status
	if s.syncCoordinator != nil {
		if peerID, err := peer.Decode(req.PeerId); err == nil {
			s.syncCoordinator.UpdateBanStatus(peerID)
		}
	}

	return &p2p_api.AddBanScoreResponse{Ok: true}, nil
}

func (s *Server) listenForBanEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-s.banChan:
			s.handleBanEvent(ctx, event)
		}
	}
}

func (s *Server) handleBanEvent(ctx context.Context, event BanEvent) {
	if event.Action != banActionAdd {
		return // we only care about new bans
	}

	// Only handle PeerID-based banning
	if event.PeerID == "" {
		s.logger.Warnf("[handleBanEvent] Ban event received without PeerID, ignoring (PeerID-only banning enabled)")
		return
	}

	s.logger.Infof("[handleBanEvent] Received ban event for PeerID: %s (reason: %s)", event.PeerID, event.Reason)

	// Parse the PeerID
	peerID, err := peer.Decode(event.PeerID)
	if err != nil {
		s.logger.Errorf("[handleBanEvent] Invalid PeerID in ban event: %s, error: %v", event.PeerID, err)
		return
	}

	// Disconnect by PeerID
	s.disconnectBannedPeerByID(ctx, peerID, event.Reason)
}

// disconnectBannedPeerByID disconnects a specific peer by their PeerID
func (s *Server) disconnectBannedPeerByID(ctx context.Context, peerID peer.ID, reason string) {
	// Check if we're connected to this peer
	peers := s.P2PClient.GetPeers()

	for _, peer := range peers {
		if peer.ID == peerID.String() {
			s.logger.Infof("[disconnectBannedPeerByID] Disconnecting banned peer: %s (reason: %s)", peerID, reason)

			// Remove peer from SyncCoordinator before disconnecting
			// Remove peer from registry
			s.removePeer(peerID)

			return
		}
	}

	s.logger.Debugf("[disconnectBannedPeerByID] Peer %s not found in connected peers", peerID)
}

func (s *Server) getIPFromMultiaddr(ctx context.Context, maddr ma.Multiaddr) (net.IP, error) {
	// try to get the IP address component
	if ip, err := maddr.ValueForProtocol(ma.P_IP4); err == nil {
		return net.ParseIP(ip), nil
	}

	if ip, err := maddr.ValueForProtocol(ma.P_IP6); err == nil {
		return net.ParseIP(ip), nil
	}

	// if it's a DNS multiaddr, resolve it
	if _, err := maddr.ValueForProtocol(ma.P_DNS4); err == nil {
		return s.resolveDNS(ctx, maddr)
	}

	if _, err := maddr.ValueForProtocol(ma.P_DNS6); err == nil {
		return s.resolveDNS(ctx, maddr)
	}

	return nil, nil // not an IP or resolvable DNS address
}

// ReportInvalidBlock adds ban score to the peer that sent an invalid block.
// This method is called by the block validation service when a block is found to be invalid.
// Parameters:
//   - ctx: Context for the operation
//   - blockHash: Hash of the invalid block
//   - reason: Reason for the block being invalid
//
// Returns an error if the peer cannot be found or the ban score cannot be added.
func (s *Server) ReportInvalidBlock(ctx context.Context, blockHash string, reason string) error {
	// Look up the peer ID that sent this block
	peerID, err := s.getPeerFromMap(&s.blockPeerMap, blockHash, "block")
	if err != nil {
		return err
	}

	// Add ban score to the peer
	s.logger.Infof("[ReportInvalidBlock] adding ban score to peer %s for invalid block %s: %s", peerID, blockHash, reason)

	// Create the request to add ban score
	req := &p2p_api.AddBanScoreRequest{
		PeerId: peerID,
		Reason: "invalid_block",
	}

	// Call the AddBanScore method
	_, err = s.AddBanScore(ctx, req)
	if err != nil {
		s.logger.Errorf("[ReportInvalidBlock] error adding ban score to peer %s: %v", peerID, err)
		return errors.NewServiceError("error adding ban score to peer %s", peerID, err)
	}

	// Remove the block from the map to avoid memory leaks
	s.blockPeerMap.Delete(blockHash)

	return nil
}

// getPeerIDFromDataHubURL finds the peer ID that has the given DataHub URL
func (s *Server) getPeerIDFromDataHubURL(dataHubURL string) string {
	if s.peerRegistry == nil {
		return ""
	}

	peers := s.peerRegistry.GetAllPeers()
	for _, peerInfo := range peers {
		if peerInfo.DataHubURL == dataHubURL {
			return peerInfo.ID.String()
		}
	}
	return ""
}

// ReportInvalidSubtree handles invalid subtree reports with explicit peer URL
func (s *Server) ReportInvalidSubtree(ctx context.Context, subtreeHash string, peerURL string, reason string) error {
	var peerID string

	// First try to get peer ID from the subtreePeerMap (for subtrees received via P2P)
	peerID, err := s.getPeerFromMap(&s.subtreePeerMap, subtreeHash, "subtree")
	if err != nil && peerURL != "" {
		// If not found in map and we have a peer URL, look up the peer ID from the URL
		peerID = s.getPeerIDFromDataHubURL(peerURL)
		if peerID == "" {
			s.logger.Warnf("[ReportInvalidSubtree] could not find peer ID for URL %s, subtree %s, reason: %s",
				peerURL, subtreeHash, reason)
			return nil // Don't return error, just log and continue
		}
		s.logger.Debugf("[ReportInvalidSubtree] found peer %s from URL %s for subtree %s",
			peerID, peerURL, subtreeHash)
	}

	if peerID == "" {
		s.logger.Warnf("[ReportInvalidSubtree] could not determine peer for subtree %s, reason: %s",
			subtreeHash, reason)
		return nil
	}

	// Add ban score to the peer
	s.logger.Infof("[ReportInvalidSubtree] adding ban score to peer %s for invalid subtree %s: %s",
		peerID, subtreeHash, reason)

	// Create the request to add ban score
	req := &p2p_api.AddBanScoreRequest{
		PeerId: peerID,
		Reason: "invalid_subtree",
	}

	// Call the AddBanScore method
	_, err = s.AddBanScore(ctx, req)
	if err != nil {
		s.logger.Errorf("[ReportInvalidSubtree] error adding ban score to peer %s: %v", peerID, err)
		return errors.NewServiceError("error adding ban score to peer %s", peerID, err)
	}

	// Remove the subtree from the map to avoid memory leaks
	s.subtreePeerMap.Delete(subtreeHash)

	return nil
}

func (s *Server) resolveDNS(ctx context.Context, dnsAddr ma.Multiaddr) (net.IP, error) {
	resolver := madns.DefaultResolver

	addrs, err := resolver.Resolve(ctx, dnsAddr)
	if err != nil {
		return nil, err
	}

	if len(addrs) == 0 {
		return nil, errors.New(errors.ERR_ERROR, fmt.Sprintf("[resolveDNS] no addresses found for %s", dnsAddr))
	}
	// get the IP from the first resolved address
	for _, proto := range []int{ma.P_IP4, ma.P_IP6} {
		if ipStr, err := addrs[0].ValueForProtocol(proto); err == nil {
			return net.ParseIP(ipStr), nil
		}
	}

	return nil, errors.New(errors.ERR_ERROR, fmt.Sprintf("[resolveDNS] no IP address found in resolved multiaddr %s", dnsAddr))
}

// myBanEventHandler implements BanEventHandler for the Server.
type myBanEventHandler struct {
	server *Server
}

// Ensure Server implements BanEventHandler
func (h *myBanEventHandler) OnPeerBanned(peerID string, until time.Time, reason string) {
	h.server.logger.Infof("Peer %s banned until %s for reason: %s", peerID, until.Format(time.RFC3339), reason)
	// get the ip for the peer id
	pid, err := peer.Decode(peerID)
	if err != nil {
		h.server.logger.Errorf("Failed to decode peer ID %s: %v", peerID, err)
		return
	}

	var ids []string
	allPeers := h.server.P2PClient.GetPeers()
	for _, p := range allPeers {
		if p.ID == pid.String() {
			h.server.logger.Infof("Found connected peer %s for banning", peerID)
			ids = append(ids, p.Addrs...)
			break
		}
	}

	// add to ban list
	for _, id := range ids {
		if h.server.banList != nil {
			if err := h.server.banList.Add(context.Background(), id, until); err != nil {
				h.server.logger.Errorf("Failed to add peer %s to ban list: %v", id, err)
			}
		}
	}

	// Remove peer from SyncCoordinator before disconnecting
	// Remove peer from registry
	h.server.removePeer(pid)
}

// contains checks if a slice of strings contains a specific string.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		bootstrapAddr, err := ma.NewMultiaddr(s)
		if err != nil {
			continue
		}

		peerInfo, err := peer.AddrInfoFromP2pAddr(bootstrapAddr)
		if err != nil {
			continue
		}

		if peerInfo.ID.String() == item {
			return true
		}
	}

	return false
}

// startInvalidBlockConsumer initializes and starts the Kafka consumer for invalid blocks
func (s *Server) startInvalidBlockConsumer(ctx context.Context) error {
	var kafkaURL *url.URL

	var brokerURLs []string

	// Use InvalidBlocksConfig URL if available, otherwise construct one
	if s.settings.Kafka.InvalidBlocksConfig != nil {
		s.logger.Infof("Using InvalidBlocksConfig URL: %s", s.settings.Kafka.InvalidBlocksConfig.String())
		kafkaURL = s.settings.Kafka.InvalidBlocksConfig

		// For non-memory schemes, we need to extract broker URLs from the host
		if kafkaURL.Scheme != "memory" {
			brokerURLs = strings.Split(kafkaURL.Host, ",")
		}
	} else {
		// Fall back to the old way of constructing the URL
		host := s.settings.Kafka.Hosts

		s.logger.Infof("Starting invalid block consumer on topic: %s", s.settings.Kafka.InvalidBlocks)
		s.logger.Infof("Raw Kafka host from settings: %s", host)

		// Split the host string in case it contains multiple hosts
		hosts := strings.Split(host, ",")
		brokerURLs = make([]string, 0, len(hosts))

		// Process each host to ensure it has a port
		for _, h := range hosts {
			// Trim any whitespace
			h = strings.TrimSpace(h)

			// Skip empty hosts
			if h == "" {
				continue
			}

			// Check if the host string contains a port
			if !strings.Contains(h, ":") {
				// If no port is specified, use the default Kafka port from settings
				h = h + ":" + strconv.Itoa(s.settings.Kafka.Port)
				s.logger.Infof("Added default port to Kafka host: %s", h)
			}

			brokerURLs = append(brokerURLs, h)
		}

		if len(brokerURLs) == 0 {
			return errors.NewConfigurationError("no valid Kafka hosts found")
		}

		s.logger.Infof("Using Kafka brokers: %v", brokerURLs)

		// Create a valid URL for the Kafka consumer
		kafkaURLString := fmt.Sprintf("kafka://%s/%s?partitions=%d",
			brokerURLs[0], // Use the first broker for the URL
			s.settings.Kafka.InvalidBlocks,
			s.settings.Kafka.Partitions)

		s.logger.Infof("Kafka URL: %s", kafkaURLString)

		var err error

		kafkaURL, err = url.Parse(kafkaURLString)
		if err != nil {
			return errors.NewConfigurationError("invalid Kafka URL: %w", err)
		}
	}

	// Create the Kafka consumer config
	cfg := kafka.KafkaConsumerConfig{
		Logger:            s.logger,
		URL:               kafkaURL,
		BrokersURL:        brokerURLs,
		Topic:             s.settings.Kafka.InvalidBlocks,
		Partitions:        s.settings.Kafka.Partitions,
		ConsumerGroupID:   s.settings.Kafka.InvalidBlocks + "-consumer",
		AutoCommitEnabled: true,
		Replay:            false,
		// TLS/Auth configuration
		EnableTLS:          s.settings.Kafka.EnableTLS,
		TLSSkipVerify:      s.settings.Kafka.TLSSkipVerify,
		TLSCAFile:          s.settings.Kafka.TLSCAFile,
		TLSCertFile:        s.settings.Kafka.TLSCertFile,
		TLSKeyFile:         s.settings.Kafka.TLSKeyFile,
		EnableDebugLogging: s.settings.Kafka.EnableDebugLogging,
	}

	// Create the Kafka consumer group - this will handle the memory scheme correctly
	consumer, err := kafka.NewKafkaConsumerGroup(cfg)
	if err != nil {
		return errors.NewServiceError("failed to create Kafka consumer", err)
	}

	// Store the consumer for cleanup
	s.invalidBlocksKafkaConsumerClient = consumer

	// Start the consumer
	consumer.Start(ctx, s.processInvalidBlockMessage)

	return nil
}

// getLocalHeight returns the current local blockchain height.
func (s *Server) getLocalHeight() uint32 {
	if s.blockchainClient == nil {
		return 0
	}

	_, bhMeta, err := s.blockchainClient.GetBestBlockHeader(s.gCtx)
	if err != nil || bhMeta == nil {
		return 0
	}

	return bhMeta.Height
}

// sendSyncTriggerToKafka sends a sync trigger message to Kafka for the given peer and block hash.

// Compatibility methods to ease migration from old architecture

func (s *Server) updatePeerHeight(peerID peer.ID, height int32) {
	// Update in registry and coordinator
	if s.peerRegistry != nil {
		// Ensure peer exists in registry
		s.addPeer(peerID)

		// Get the existing block hash from registry
		blockHash := ""
		if peerInfo, exists := s.getPeer(peerID); exists {
			blockHash = peerInfo.BlockHash
		}
		s.peerRegistry.UpdateHeight(peerID, height, blockHash)

		// Also update sync coordinator if it exists
		if s.syncCoordinator != nil {
			dataHubURL := ""
			if peerInfo, exists := s.getPeer(peerID); exists {
				dataHubURL = peerInfo.DataHubURL
			}
			s.syncCoordinator.UpdatePeerInfo(peerID, height, blockHash, dataHubURL)
		}
	}
}

func (s *Server) addPeer(peerID peer.ID) {
	if s.peerRegistry != nil {
		s.peerRegistry.AddPeer(peerID)
	}
}

// addConnectedPeer adds a peer and marks it as directly connected
func (s *Server) addConnectedPeer(peerID peer.ID) {
	if s.peerRegistry != nil {
		s.peerRegistry.AddPeer(peerID)
		s.peerRegistry.UpdateConnectionState(peerID, true)
	}
}

func (s *Server) removePeer(peerID peer.ID) {
	if s.peerRegistry != nil {
		// Mark as disconnected before removing
		s.peerRegistry.UpdateConnectionState(peerID, false)
		s.peerRegistry.RemovePeer(peerID)
	}
	if s.syncCoordinator != nil {
		s.syncCoordinator.HandlePeerDisconnected(peerID)
	}
}

func (s *Server) updateBlockHash(peerID peer.ID, blockHash string) {
	if s.peerRegistry != nil && blockHash != "" {
		s.peerRegistry.UpdateBlockHash(peerID, blockHash)
	}
}

// getPeer gets peer information from the registry
func (s *Server) getPeer(peerID peer.ID) (*PeerInfo, bool) {
	if s.peerRegistry != nil {
		return s.peerRegistry.GetPeer(peerID)
	}
	return nil, false
}

func (s *Server) getSyncPeer() peer.ID {
	if s.syncCoordinator != nil {
		return s.syncCoordinator.GetCurrentSyncPeer()
	}
	return ""
}

// updateDataHubURL updates peer DataHub URL in the registry
func (s *Server) updateDataHubURL(peerID peer.ID, url string) {
	if s.peerRegistry != nil && url != "" {
		s.peerRegistry.UpdateDataHubURL(peerID, url)
	}
}

// updateStorage updates peer storage mode in the registry
func (s *Server) updateStorage(peerID peer.ID, mode string) {
	if s.peerRegistry != nil && mode != "" {
		s.peerRegistry.UpdateStorage(peerID, mode)
	}
}

func (s *Server) processInvalidBlockMessage(message *kafka.KafkaMessage) error {
	ctx := context.Background()

	var invalidBlockMsg kafkamessage.KafkaInvalidBlockTopicMessage
	if err := proto.Unmarshal(message.Value, &invalidBlockMsg); err != nil {
		s.logger.Errorf("failed to unmarshal invalid block message: %v", err)
		return err
	}

	blockHash := invalidBlockMsg.GetBlockHash()
	reason := invalidBlockMsg.GetReason()

	s.logger.Infof("[handleInvalidBlockMessage] processing invalid block %s: %s", blockHash, reason)

	// Look up the peer ID that sent this block
	peerID, err := s.getPeerFromMap(&s.blockPeerMap, blockHash, "block")
	if err != nil {
		s.logger.Warnf("[handleInvalidBlockMessage] %v", err)
		return nil // Not an error, just no peer to ban
	}

	// Add ban score to the peer
	s.logger.Infof("[handleInvalidBlockMessage] adding ban score to peer %s for invalid block %s: %s",
		peerID, blockHash, reason)

	req := &p2p_api.AddBanScoreRequest{
		PeerId: peerID,
		Reason: "invalid_block",
	}

	if _, err := s.AddBanScore(ctx, req); err != nil {
		s.logger.Errorf("[handleInvalidBlockMessage] error adding ban score to peer %s: %v", peerID, err)
		return err
	}

	// Remove the block from the map to avoid memory leaks
	s.blockPeerMap.Delete(blockHash)

	return nil
}

func (s *Server) isBlockchainSyncingOrCatchingUp(ctx context.Context) (bool, error) {
	if s.blockchainClient == nil {
		return false, nil
	}
	var (
		state *blockchain.FSMStateType
		err   error
	)

	// Retry for up to 15 seconds if we get an error getting FSM state
	// This handles the case where blockchain service isn't ready yet
	retryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	retryCount := 0
	for {
		state, err = s.blockchainClient.GetFSMCurrentState(retryCtx)
		if err == nil {
			// Successfully got state
			if retryCount > 0 {
				s.logger.Infof("[isBlockchainSyncingOrCatchingUp] successfully got FSM state after %d retries", retryCount)
			}
			break
		}

		retryCount++

		// Check if context is done (timeout or cancellation)
		select {
		case <-retryCtx.Done():
			s.logger.Errorf("[isBlockchainSyncingOrCatchingUp] timeout after 15s getting blockchain FSM state (tried %d times): %v", retryCount, err)
			// On timeout, allow sync to proceed rather than blocking
			return false, nil
		case <-time.After(1 * time.Second):
			// Retry after short delay
			if retryCount == 1 || retryCount%10 == 0 {
				s.logger.Infof("[isBlockchainSyncingOrCatchingUp] retrying FSM state check (attempt %d) after error: %v", retryCount, err)
			}
		}
	}

	if *state == blockchain_api.FSMStateType_CATCHINGBLOCKS || *state == blockchain_api.FSMStateType_LEGACYSYNCING {
		// ignore notifications while syncing or catching up
		return true, nil
	}

	return false, nil
}

// cleanupPeerMaps performs periodic cleanup of blockPeerMap and subtreePeerMap
// It removes entries older than TTL and enforces size limits using LRU eviction
func (s *Server) cleanupPeerMaps() {
	now := time.Now()

	// Collect entries to delete
	var blockKeysToDelete []string
	var subtreeKeysToDelete []string
	blockCount := 0
	subtreeCount := 0

	// First pass: count entries and collect expired ones
	s.blockPeerMap.Range(func(key, value interface{}) bool {
		blockCount++
		if entry, ok := value.(peerMapEntry); ok {
			if now.Sub(entry.timestamp) > s.peerMapTTL {
				blockKeysToDelete = append(blockKeysToDelete, key.(string))
			}
		}
		return true
	})

	s.subtreePeerMap.Range(func(key, value interface{}) bool {
		subtreeCount++
		if entry, ok := value.(peerMapEntry); ok {
			if now.Sub(entry.timestamp) > s.peerMapTTL {
				subtreeKeysToDelete = append(subtreeKeysToDelete, key.(string))
			}
		}
		return true
	})

	// Delete expired entries
	for _, key := range blockKeysToDelete {
		s.blockPeerMap.Delete(key)
	}
	for _, key := range subtreeKeysToDelete {
		s.subtreePeerMap.Delete(key)
	}

	// Log cleanup stats
	if len(blockKeysToDelete) > 0 || len(subtreeKeysToDelete) > 0 {
		s.logger.Infof("[cleanupPeerMaps] removed %d expired block entries and %d expired subtree entries",
			len(blockKeysToDelete), len(subtreeKeysToDelete))
	}

	// Second pass: enforce size limits if needed
	remainingBlockCount := blockCount - len(blockKeysToDelete)
	remainingSubtreeCount := subtreeCount - len(subtreeKeysToDelete)

	if remainingBlockCount > s.peerMapMaxSize {
		s.enforceMapSizeLimit(&s.blockPeerMap, s.peerMapMaxSize, "block")
	}

	if remainingSubtreeCount > s.peerMapMaxSize {
		s.enforceMapSizeLimit(&s.subtreePeerMap, s.peerMapMaxSize, "subtree")
	}

	// Log current sizes
	s.logger.Infof("[cleanupPeerMaps] current map sizes - blocks: %d, subtrees: %d",
		remainingBlockCount, remainingSubtreeCount)
}

// enforceMapSizeLimit removes oldest entries from a map to enforce size limit
func (s *Server) enforceMapSizeLimit(m *sync.Map, maxSize int, mapType string) {
	type entryWithKey struct {
		key       string
		timestamp time.Time
	}

	var entries []entryWithKey

	// Collect all entries with their timestamps
	m.Range(func(key, value interface{}) bool {
		if entry, ok := value.(peerMapEntry); ok {
			entries = append(entries, entryWithKey{
				key:       key.(string),
				timestamp: entry.timestamp,
			})
		}
		return true
	})

	// Sort by timestamp (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].timestamp.Before(entries[j].timestamp)
	})

	// Remove oldest entries to get under the limit
	toRemove := len(entries) - maxSize
	if toRemove > 0 {
		for i := 0; i < toRemove; i++ {
			m.Delete(entries[i].key)
		}
		s.logger.Warnf("[enforceMapSizeLimit] removed %d oldest %s entries to enforce size limit of %d",
			toRemove, mapType, maxSize)
	}
}

// startPeerMapCleanup starts the periodic cleanup goroutine
// Helper methods to reduce redundancy

// isOwnMessage checks if a message is from this node
func (s *Server) isOwnMessage(from string, peerID string) bool {
	return from == s.P2PClient.GetID() || peerID == s.P2PClient.GetID()
}

// shouldSkipBannedPeer checks if we should skip a message from a banned peer
func (s *Server) shouldSkipBannedPeer(from string, messageType string) bool {
	if s.banManager.IsBanned(from) {
		s.logger.Debugf("[%s] ignoring notification from banned peer %s", messageType, from)
		return true
	}
	return false
}

// shouldSkipUnhealthyPeer checks if we should skip a message from an unhealthy peer
// Only checks health for directly connected peers (not gossiped peers)
func (s *Server) shouldSkipUnhealthyPeer(from string, messageType string) bool {
	// If no peer registry, allow all messages
	if s.peerRegistry == nil {
		return false
	}

	peerID, err := peer.Decode(from)
	if err != nil {
		// If we can't decode the peer ID (e.g., from is a hostname/identifier in gossiped messages),
		// we can't check health status, so allow the message through.
		// This is normal for gossiped messages where 'from' is the relay peer's identifier, not a valid peer ID.
		return false
	}

	peerInfo, exists := s.peerRegistry.GetPeer(peerID)
	if !exists {
		// Peer not in registry - allow message (peer might be new)
		return false
	}

	// Only filter unhealthy peers if they're directly connected
	// Gossiped peers aren't health-checked, so we don't filter them
	if peerInfo.IsConnected && !peerInfo.IsHealthy {
		s.logger.Debugf("[%s] ignoring notification from unhealthy connected peer %s", messageType, from)
		return true
	}

	return false
}

// storePeerMapEntry stores a peer entry in the specified map
func (s *Server) storePeerMapEntry(peerMap *sync.Map, hash string, from string, timestamp time.Time) {
	entry := peerMapEntry{
		peerID:    from,
		timestamp: timestamp,
	}
	peerMap.Store(hash, entry)
}

// getPeerFromMap retrieves and validates a peer entry from a map
func (s *Server) getPeerFromMap(peerMap *sync.Map, hash string, mapType string) (string, error) {
	peerIDVal, ok := peerMap.Load(hash)
	if !ok {
		s.logger.Warnf("[getPeerFromMap] no peer found for %s %s", mapType, hash)
		return "", errors.NewNotFoundError("no peer found for %s %s", mapType, hash)
	}

	entry, ok := peerIDVal.(peerMapEntry)
	if !ok {
		s.logger.Errorf("[getPeerFromMap] peer entry for %s %s is not a peerMapEntry: %v", mapType, hash, peerIDVal)
		return "", errors.NewInvalidArgumentError("peer entry for %s %s is not a peerMapEntry", mapType, hash)
	}
	return entry.peerID, nil
}

// parseHash converts a string hash to chainhash
func (s *Server) parseHash(hashStr string, context string) (*chainhash.Hash, error) {
	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		s.logger.Errorf("[%s] error getting chainhash from string %s: %v", context, hashStr, err)
		return nil, err
	}
	return hash, nil
}

// shouldSkipDuringSync checks if we should skip processing during sync
func (s *Server) shouldSkipDuringSync(from string, originatorPeerID string, messageHeight uint32, messageType string) bool {
	syncPeer := s.getSyncPeer()
	if syncPeer == "" {
		return false
	}

	syncing, err := s.isBlockchainSyncingOrCatchingUp(s.gCtx)
	if err != nil || !syncing {
		return false
	}

	// Get sync peer's height from registry
	syncPeerHeight := int32(0)
	if peerInfo, exists := s.getPeer(syncPeer); exists {
		syncPeerHeight = peerInfo.Height
	}

	// Discard announcements from peers that are behind our sync peer
	if messageHeight < uint32(syncPeerHeight) {
		s.logger.Debugf("[%s] Discarding announcement at height %d from %s (below sync peer height %d)",
			messageType, messageHeight, from, syncPeerHeight)
		return true
	}

	// Skip if it's not from our sync peer
	peerID, err := peer.Decode(originatorPeerID)
	if err != nil || peerID != syncPeer {
		s.logger.Debugf("[%s] Skipping announcement during sync (not from sync peer)", messageType)
		return true
	}

	return false
}

func (s *Server) startPeerMapCleanup(ctx context.Context) {
	// Use configured interval or default
	cleanupInterval := defaultPeerMapCleanupInterval
	if s.settings.P2P.PeerMapCleanupInterval > 0 {
		cleanupInterval = s.settings.P2P.PeerMapCleanupInterval
	}

	s.peerMapCleanupTicker = time.NewTicker(cleanupInterval)

	go func() {
		for {
			select {
			case <-ctx.Done():
				s.logger.Infof("[startPeerMapCleanup] stopping peer map cleanup")
				return
			case <-s.peerMapCleanupTicker.C:
				s.cleanupPeerMaps()
			}
		}
	}()

	s.logger.Infof("[startPeerMapCleanup] started peer map cleanup with interval %v", cleanupInterval)
}
