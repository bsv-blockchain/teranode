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
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
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
	P2PClient                         p2pMessageBus.P2PClient   // The P2P network client
	logger                            ulogger.Logger            // Logger instance for the server
	settings                          *settings.Settings        // Configuration settings
	bitcoinProtocolVersion            string                    // Bitcoin protocol identifier
	blockchainClient                  blockchain.ClientI        // Client for blockchain interactions
	blockAssemblyClient               blockassembly.ClientI     // Client for block assembly operations
	AssetHTTPAddressURL               string                    // HTTP address URL for assets
	PropagationURL                    string                    // URL for peers to use for propagating txs (defaults to AssetHTTPAddressURL)
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
	invalidBlocksTopicName            string           // Kafka topic for invalid blocks
	invalidSubtreeTopicName           string           // Kafka topic for invalid subtrees
	nodeStatusTopicName               string           // pubsub topic for node status messages
	topicPrefix                       string           // Chain identifier prefix for topic validation
	blockPeerMap                      sync.Map         // Map to track which peer sent each block (hash -> peerMapEntry)
	subtreePeerMap                    sync.Map         // Map to track which peer sent each subtree (hash -> peerMapEntry)
	startTime                         time.Time        // Server start time for uptime calculation
	peerRegistry                      *PeerRegistry    // Central registry for all peer information
	peerSelector                      *PeerSelector    // Stateless peer selection logic
	syncCoordinator                   *SyncCoordinator // Orchestrates sync operations
	syncConnectionTimes               sync.Map         // Map to track when we first connected to each sync peer (peerID -> timestamp)

	// Cleanup configuration
	peerMapCleanupTicker    *time.Ticker  // Ticker for periodic cleanup of peer maps
	peerMapMaxSize          int           // Maximum number of entries in peer maps
	peerMapTTL              time.Duration // Time-to-live for peer map entries
	registryCacheSaveTicker *time.Ticker  // Ticker for periodic saving of peer registry cache
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
		PrivateKey:         privKey,
		Name:               tSettings.ClientName,
		Logger:             logger,
		PeerCacheFile:      getPeerCacheFilePath(tSettings.P2P.PeerCacheDir),
		BootstrapPeers:     tSettings.P2P.BootstrapPeers,
		ProtocolVersion:    bitcoinProtocolVersion,
		DHTMode:            tSettings.P2P.DHTMode,
		DHTCleanupInterval: tSettings.P2P.DHTCleanupInterval,
		EnableNAT:          tSettings.P2P.EnableNAT,
		EnableMDNS:         tSettings.P2P.EnableMDNS,
		AllowPrivateIPs:    tSettings.P2P.AllowPrivateIPs,
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

	// Initialize new clean architecture components
	// Note: peer registry must be created first so it can be passed to ban manager
	p2pServer.peerRegistry = NewPeerRegistry()
	p2pServer.peerSelector = NewPeerSelector(logger, tSettings)

	// Load cached peer registry data if available
	if err := p2pServer.peerRegistry.LoadPeerRegistryCache(tSettings.P2P.PeerCacheDir); err != nil {
		// Log error but continue - cache loading is not critical
		logger.Warnf("Failed to load peer registry cache: %v", err)
	} else {
		logger.Infof("Loaded peer registry cache with %d peers", p2pServer.peerRegistry.PeerCount())
	}

	// Initialize the ban manager with peer registry so it can sync ban statuses
	p2pServer.banManager = NewPeerBanManager(ctx, &myBanEventHandler{server: p2pServer}, tSettings, p2pServer.peerRegistry)
	p2pServer.syncCoordinator = NewSyncCoordinator(
		logger,
		tSettings,
		p2pServer.peerRegistry,
		p2pServer.peerSelector,
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

	// Set propagation URL - defaults to AssetHTTPAddressURL if not configured
	propagationURL := s.settings.Asset.PropagationPublicURL
	if propagationURL == "" {
		propagationURL = s.AssetHTTPAddressURL
	}
	s.PropagationURL = propagationURL

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

	e.GET("/p2p-ws", s.HandleWebSocket(s.notificationCh))

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

	// Start periodic save of peer registry cache
	s.startPeerRegistryCacheSave(ctx)

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
			handler(ctx, msg.Data, msg.FromID)
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

		s.logger.Infof("[invalidSubtreeHandler] Received invalid subtree notification via Kafka: hash=%s, peerUrl=%s, reason=%s", m.SubtreeHash, m.PeerUrl, m.Reason)

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
		/*err = s.ReportInvalidBlock(ctx, m.BlockHash, m.Reason)
		if err != nil {
			// Don't return error here, as we want to continue processing messages
			s.logger.Errorf("[invalidBlockHandler] Failed to report invalid block from Kafka: %v", err)
		}*/

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
	// Note: We don't have the sender's client name here, only the originator's
	senderID, err := peer.Decode(from)
	if err != nil {
		s.logger.Errorf("failed to decode sender peer ID %s: %v", from, err)
		return
	}

	s.addConnectedPeer(senderID, "", 0, nil, "")
	s.peerRegistry.UpdateLastMessageTime(senderID)

	// Also update for the originator if different (gossiped message)
	// The originator is not directly connected to us
	if originatorPeerID != "" {
		if peerID, err := peer.Decode(originatorPeerID); err == nil && peerID != senderID {
			// Don't add ourselves as a peer (prevent self-gossip in single-node environments)
			if originatorPeerID == s.P2PClient.GetID() {
				return
			}
			// Add as gossiped peer (not connected) before updating last message time
			s.addPeer(peerID, "", 0, nil, "")
			s.peerRegistry.UpdateLastMessageTime(peerID)
		}
	}
}

// updateBytesReceived increments the bytes received counter for a peer
// It updates both the direct sender and the originator (if different) for gossiped messages
func (s *Server) updateBytesReceived(from string, originatorPeerID string, messageSize uint64) {
	if s.peerRegistry == nil {
		return
	}

	// Update bytes for the sender (peer we're directly connected to)
	senderID, err := peer.Decode(from)
	if err != nil {
		s.logger.Errorf("failed to decode sender peer ID %s: %v", from, err)
		return
	}
	if info, exists := s.peerRegistry.Get(senderID); exists {
		newTotal := info.BytesReceived + messageSize
		s.peerRegistry.UpdateNetworkStats(senderID, newTotal)
	}

	// Also update for the originator if different (gossiped message)
	if originatorPeerID != "" {
		if peerID, err := peer.Decode(originatorPeerID); err == nil && peerID != senderID {
			if info, exists := s.peerRegistry.Get(peerID); exists {
				newTotal := info.BytesReceived + messageSize
				s.peerRegistry.UpdateNetworkStats(peerID, newTotal)
			}
		}
	}
}

func (s *Server) handleNodeStatusTopic(_ context.Context, m []byte, peerID string) {
	var nodeStatusMessage NodeStatusMessage

	if err := json.Unmarshal(m, &nodeStatusMessage); err != nil {
		s.logger.Errorf("[handleNodeStatusTopic] json unmarshal error: %v", err)
		return
	}

	// Check if this is our own message
	isSelf := peerID == s.P2PClient.GetID()

	// If sender ID doesn't match node status ID log an error and return
	// In future, consider banning this peer as they are maliciously spoofing
	if peerID != nodeStatusMessage.PeerID {
		s.logger.Errorf("[handleNodeStatusTopic] node_status peerID %s does not match message ID %s",
			peerID, nodeStatusMessage.PeerID)
		return
	}

	// Skip further processing for our own messages (peer height updates, etc.)
	// but still forward to WebSocket
	if !isSelf {
		s.logger.Debugf("[handleNodeStatusTopic] Processing node_status from remote peer %s (peer_id: %s)", peerID, nodeStatusMessage.PeerID)

		// Update last message time for the sender and originator with client name
		s.updatePeerLastMessageTime(peerID, nodeStatusMessage.PeerID)

		// Track bytes received from this message
		s.updateBytesReceived(peerID, nodeStatusMessage.PeerID, uint64(len(m)))

		// Skip processing peerID unhealthy peers (but still forward to WebSocket for monitoring)
		if s.shouldSkipUnhealthyPeer(peerID, "handleNodeStatusTopic") {
			s.logger.Debugf("[handleNodeStatusTopic] Skipping peer data processing peerID unhealthy peer %s, but forwarding to WebSocket", peerID)
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
		peerID, err := peer.Decode(nodeStatusMessage.PeerID)
		if err != nil {
			s.logger.Errorf("[handleNodeStatusTopic] failed to decode peer ID %s: %v", nodeStatusMessage.PeerID, err)
			return
		}

		hash, err := chainhash.NewHashFromStr(nodeStatusMessage.BestBlockHash)
		if err != nil {
			s.logger.Warnf("[handleNodeStatusTopic] failed to create hash from best block hash %s: %v", nodeStatusMessage.BestBlockHash, err)
			return
		}

		s.addPeer(peerID, nodeStatusMessage.ClientName, nodeStatusMessage.BestHeight, hash, nodeStatusMessage.BaseURL)
		s.logger.Debugf("[handleNodeStatusTopic] Updated block hash %s for peer %s", nodeStatusMessage.BestBlockHash, peerID)

		// Update storage mode if provided
		// Store whether the peer is a full node or pruned node
		if nodeStatusMessage.Storage != "" {
			s.updateStorage(peerID, nodeStatusMessage.Storage)
			s.logger.Debugf("[handleNodeStatusTopic] Updated storage mode to %s for peer %s", nodeStatusMessage.Storage, peerID)
		}
	}

	// Also ensure the sender is in the registry
	if !isSelf && peerID != "" {
		if senderID, err := peer.Decode(peerID); err == nil {
			s.addPeer(senderID, "", 0, nil, "")
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
	syncPeerHeight := uint32(0)
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
						if pInfo.BlockHash != nil {
							syncPeerBlockHash = pInfo.BlockHash.String()
						}

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

	// Set propagation URL - empty if in listen only mode
	propagationURL := s.PropagationURL
	if s.settings.P2P.ListenMode == settings.ListenModeListenOnly {
		propagationURL = ""
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
		allPeers := s.peerRegistry.GetAll()
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
	// Query block persister height from blockchain state
	var blockPersisterHeight uint32
	if s.blockchainClient != nil {
		if stateData, err := s.blockchainClient.GetState(ctx, "BlockPersisterHeight"); err == nil && len(stateData) >= 4 {
			blockPersisterHeight = binary.LittleEndian.Uint32(stateData)
		}
	}

	retentionWindow := uint32(0)
	if s.settings != nil && s.settings.GlobalBlockHeightRetention > 0 {
		retentionWindow = s.settings.GlobalBlockHeightRetention
	}

	prunerBlockTrigger := ""
	if s.settings != nil {
		prunerBlockTrigger = s.settings.Pruner.BlockTrigger
	}

	storage := util.DetermineStorageMode(blockPersisterHeight, height, retentionWindow, prunerBlockTrigger)
	s.logger.Debugf("[getNodeStatusMessage] Determined storage=%q for this node (persisterHeight=%d, bestHeight=%d, retention=%d, prunerTrigger=%s)",
		storage, blockPersisterHeight, height, retentionWindow, prunerBlockTrigger)

	// Return the notification message
	return &notificationMsg{
		Timestamp:           time.Now().UTC().Format(isoFormat),
		Type:                "node_status",
		BaseURL:             baseURL,
		PropagationURL:      propagationURL,
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
		PropagationURL:      msg.PropagationURL,
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

func (s *Server) handlePeerFailureNotification(_ context.Context, notification *blockchain.Notification) error {
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

	switch notification.Type {
	case model.NotificationType_Block:
		s.logger.Infof("[processBlockchainNotification] Processing %s notification: %s", notification.Type, hash.String())
		return s.handleBlockNotification(ctx, hash) // These handlers return wrapped errors

	case model.NotificationType_Subtree:
		s.logger.Infof("[processBlockchainNotification] Processing %s notification: %s", notification.Type, hash.String())
		return s.handleSubtreeNotification(ctx, hash)

	case model.NotificationType_PeerFailure:
		s.logger.Infof("[processBlockchainNotification] Processing %s notification: %s", notification.Type, hash.String())
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

// GetPeers returns a list of connected peers with full registry data.
func (s *Server) GetPeers(ctx context.Context, _ *emptypb.Empty) (*p2p_api.GetPeersResponse, error) {
	s.logger.Debugf("GetPeers called")

	// If peer registry is available, use it as it has richer data
	if s.peerRegistry != nil {
		// Get connected peers from the registry with full metadata
		connectedPeers := s.peerRegistry.GetConnectedPeers()

		resp := &p2p_api.GetPeersResponse{}
		for _, peer := range connectedPeers {
			// Get address from libp2p if available
			addr := ""
			if s.P2PClient != nil {
				libp2pPeers := s.P2PClient.GetPeers()
				for _, sp := range libp2pPeers {
					if sp.ID == peer.ID.String() && len(sp.Addrs) > 0 {
						addr = sp.Addrs[0]
						break
					}
				}
			}

			resp.Peers = append(resp.Peers, &p2p_api.Peer{
				Id:       peer.ID.String(),
				Addr:     addr,
				Banscore: int32(peer.BanScore), //nolint:gosec
			})
		}

		return resp, nil
	}

	// Fallback to libp2p client data if registry not available
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

// RecordBytesDownloaded records the number of bytes downloaded via HTTP from a peer.
// This method is called by services (blockvalidation, subtreevalidation) after downloading
// data from a peer's DataHub URL to track total network usage per peer.
// Parameters:
//   - ctx: Context for the operation
//   - req: Request containing peer_id and bytes_downloaded
//
// Returns a response indicating success or an error if the peer cannot be found.
func (s *Server) RecordBytesDownloaded(ctx context.Context, req *p2p_api.RecordBytesDownloadedRequest) (*p2p_api.RecordBytesDownloadedResponse, error) {
	// Decode the peer ID string
	peerID, err := peer.Decode(req.PeerId)
	if err != nil {
		s.logger.Errorf("[RecordBytesDownloaded] failed to decode peer ID %s: %v", req.PeerId, err)
		return &p2p_api.RecordBytesDownloadedResponse{Ok: false}, errors.NewServiceError("failed to decode peer ID", err)
	}

	// Get current peer info from registry
	peerInfo, exists := s.peerRegistry.Get(peerID)
	if !exists {
		s.logger.Warnf("[RecordBytesDownloaded] peer %s not found in registry", req.PeerId)
		// Still return success - peer might not be in registry yet
		return &p2p_api.RecordBytesDownloadedResponse{Ok: true}, nil
	}

	// Calculate new total bytes received
	newTotal := peerInfo.BytesReceived + req.BytesDownloaded

	// Update the peer registry with the new total
	s.peerRegistry.UpdateNetworkStats(peerID, newTotal)

	s.logger.Debugf("[RecordBytesDownloaded] Updated peer %s: added %d bytes, new total: %d bytes", req.PeerId, req.BytesDownloaded, newTotal)

	return &p2p_api.RecordBytesDownloadedResponse{Ok: true}, nil
}

func (s *Server) ResetReputation(ctx context.Context, req *p2p_api.ResetReputationRequest) (*p2p_api.ResetReputationResponse, error) {
	peersReset := s.peerRegistry.ResetReputation(req.PeerId)

	if req.PeerId == "" {
		s.logger.Infof("[ResetReputation] Reset reputation for all peers. Count: %d", peersReset)
	} else {
		s.logger.Infof("[ResetReputation] Reset reputation for peer %s", req.PeerId)
	}

	return &p2p_api.ResetReputationResponse{
		Ok:         true,
		PeersReset: int32(peersReset),
	}, nil
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

	// Record as malicious interaction for reputation tracking
	s.peerRegistry.RecordMaliciousInteraction(peer.ID(peerID))

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

	s.logger.Infof("[ReportInvalidSubtree] invalid subtree report for peer %s, subtree %s: %s",
		peerID, subtreeHash, reason)

	// TODO: The penalty system here should be refactored. Currently all reasons
	// (peer_cannot_provide_transactions, malformed_transaction_data, transaction_count_mismatch)
	// are more likely network issues than malicious behavior. This will be addressed in a separate PR.
	// For now, both RecordMaliciousInteraction and AddBanScore are disabled.
	//
	// // Record as malicious interaction for reputation tracking
	// s.peerRegistry.RecordMaliciousInteraction(peer.ID(peerID))
	//
	// // Create the request to add ban score
	// req := &p2p_api.AddBanScoreRequest{
	// 	PeerId: peerID,
	// 	Reason: "invalid_subtree",
	// }
	//
	// // Call the AddBanScore method
	// _, err = s.AddBanScore(ctx, req)
	// if err != nil {
	// 	s.logger.Errorf("[ReportInvalidSubtree] error adding ban score to peer %s: %v", peerID, err)
	// 	return errors.NewServiceError("error adding ban score to peer %s", peerID, err)
	// }

	// Remove the subtree from the map to avoid memory leaks
	s.subtreePeerMap.Delete(subtreeHash)

	return nil
}

// myBanEventHandler implements BanEventHandler for the Server.
type myBanEventHandler struct {
	server *Server
}

// OnPeerBanned is called when a peer is banned.
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

// GetPeerRegistry returns comprehensive peer registry data with all metadata
func (s *Server) GetPeerRegistry(_ context.Context, _ *emptypb.Empty) (*p2p_api.GetPeerRegistryResponse, error) {
	s.logger.Debugf("[GetPeerRegistry] called")

	if s.peerRegistry == nil {
		return &p2p_api.GetPeerRegistryResponse{
			Peers: []*p2p_api.PeerRegistryInfo{},
		}, nil
	}

	// Get all peers from the registry
	allPeers := s.peerRegistry.GetAll()

	// Helper function to convert time to Unix timestamp, returning 0 for zero times
	timeToUnix := func(t time.Time) int64 {
		if t.IsZero() {
			return 0
		}

		return t.Unix()
	}

	// Convert to protobuf format
	peers := make([]*p2p_api.PeerRegistryInfo, 0, len(allPeers))
	for _, p := range allPeers {
		blockHashStr := ""
		if p.BlockHash != nil {
			blockHashStr = p.BlockHash.String()
		}

		peers = append(peers, &p2p_api.PeerRegistryInfo{
			Id:              p.ID.String(),
			Height:          p.Height,
			BlockHash:       blockHashStr,
			DataHubUrl:      p.DataHubURL,
			BanScore:        int32(p.BanScore),
			IsBanned:        p.IsBanned,
			IsConnected:     p.IsConnected,
			ConnectedAt:     timeToUnix(p.ConnectedAt),
			BytesReceived:   p.BytesReceived,
			LastBlockTime:   timeToUnix(p.LastBlockTime),
			LastMessageTime: timeToUnix(p.LastMessageTime),

			// Interaction/catchup metrics
			InteractionAttempts:    p.InteractionAttempts,
			InteractionSuccesses:   p.InteractionSuccesses,
			InteractionFailures:    p.InteractionFailures,
			LastInteractionAttempt: timeToUnix(p.LastInteractionAttempt),
			LastInteractionSuccess: timeToUnix(p.LastInteractionSuccess),
			LastInteractionFailure: timeToUnix(p.LastInteractionFailure),
			ReputationScore:        p.ReputationScore,
			MaliciousCount:         p.MaliciousCount,
			AvgResponseTimeMs:      p.AvgResponseTime.Milliseconds(),
			Storage:                p.Storage,
			ClientName:             p.ClientName,
			LastCatchupError:       p.LastCatchupError,
			LastCatchupErrorTime:   timeToUnix(p.LastCatchupErrorTime),
		})
	}

	return &p2p_api.GetPeerRegistryResponse{
		Peers: peers,
	}, nil
}

// GetPeer returns information about a specific peer by peer ID
func (s *Server) GetPeer(_ context.Context, req *p2p_api.GetPeerRequest) (*p2p_api.GetPeerResponse, error) {
	s.logger.Debugf("[GetPeer] called for peer %s", req.PeerId)

	if s.peerRegistry == nil {
		return &p2p_api.GetPeerResponse{
			Found: false,
		}, nil
	}

	// Decode peer ID
	peerID, err := peer.Decode(req.PeerId)
	if err != nil {
		s.logger.Warnf("[GetPeer] invalid peer ID %s: %v", req.PeerId, err)
		return &p2p_api.GetPeerResponse{
			Found: false,
		}, nil
	}

	// Get peer from registry
	peerInfo, found := s.peerRegistry.Get(peerID)
	if !found {
		s.logger.Debugf("[GetPeer] peer %s not found in registry", req.PeerId)
		return &p2p_api.GetPeerResponse{
			Found: false,
		}, nil
	}

	// Helper function to convert time to Unix timestamp, returning 0 for zero times
	timeToUnix := func(t time.Time) int64 {
		if t.IsZero() {
			return 0
		}
		return t.Unix()
	}

	// Convert to protobuf format
	blockHashStr := ""
	if peerInfo.BlockHash != nil {
		blockHashStr = peerInfo.BlockHash.String()
	}

	peerRegistryInfo := &p2p_api.PeerRegistryInfo{
		Id:              peerInfo.ID.String(),
		Height:          peerInfo.Height,
		BlockHash:       blockHashStr,
		DataHubUrl:      peerInfo.DataHubURL,
		BanScore:        int32(peerInfo.BanScore),
		IsBanned:        peerInfo.IsBanned,
		IsConnected:     peerInfo.IsConnected,
		ConnectedAt:     timeToUnix(peerInfo.ConnectedAt),
		BytesReceived:   peerInfo.BytesReceived,
		LastBlockTime:   timeToUnix(peerInfo.LastBlockTime),
		LastMessageTime: timeToUnix(peerInfo.LastMessageTime),

		// Interaction/catchup metrics
		InteractionAttempts:    peerInfo.InteractionAttempts,
		InteractionSuccesses:   peerInfo.InteractionSuccesses,
		InteractionFailures:    peerInfo.InteractionFailures,
		LastInteractionAttempt: timeToUnix(peerInfo.LastInteractionAttempt),
		LastInteractionSuccess: timeToUnix(peerInfo.LastInteractionSuccess),
		LastInteractionFailure: timeToUnix(peerInfo.LastInteractionFailure),
		ReputationScore:        peerInfo.ReputationScore,
		MaliciousCount:         peerInfo.MaliciousCount,
		AvgResponseTimeMs:      peerInfo.AvgResponseTime.Milliseconds(),
		Storage:                peerInfo.Storage,
		ClientName:             peerInfo.ClientName,
		LastCatchupError:       peerInfo.LastCatchupError,
		LastCatchupErrorTime:   timeToUnix(peerInfo.LastCatchupErrorTime),
	}

	return &p2p_api.GetPeerResponse{
		Peer:  peerRegistryInfo,
		Found: true,
	}, nil
}
