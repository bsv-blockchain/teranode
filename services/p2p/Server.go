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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/p2p/p2p_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/bitcoin-sv/teranode/util/servicemanager"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-p2p"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
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
	P2PNode                           p2p.NodeI          // The P2P network node instance - using interface instead of concrete type
	logger                            ulogger.Logger     // Logger instance for the server
	settings                          *settings.Settings // Configuration settings
	bitcoinProtocolID                 string             // Bitcoin protocol identifier
	blockchainClient                  blockchain.ClientI // Client for blockchain interactions
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
	miningOnTopicName                 string
	rejectedTxTopicName               string
	invalidBlocksTopicName            string             // Kafka topic for invalid blocks
	invalidSubtreeTopicName           string             // Kafka topic for invalid subtrees
	handshakeTopicName                string             // pubsub topic for version/verack
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

	topicPrefix := tSettings.ChainCfgParams.TopicPrefix
	if topicPrefix == "" {
		return nil, errors.NewConfigurationError("missing config ChainCfgParams.TopicPrefix")
	}

	btn := tSettings.P2P.BlockTopic
	if btn == "" {
		return nil, errors.NewConfigurationError("p2p_block_topic not set in config")
	}

	stn := tSettings.P2P.SubtreeTopic
	if stn == "" {
		return nil, errors.NewConfigurationError("p2p_subtree_topic not set in config")
	}

	htn := tSettings.P2P.HandshakeTopic
	if htn == "" {
		return nil, errors.NewConfigurationError("p2p_handshake_topic not set in config")
	}

	miningOntn := tSettings.P2P.MiningOnTopic
	if miningOntn == "" {
		return nil, errors.NewConfigurationError("p2p_mining_on_topic not set in config")
	}

	rtn := tSettings.P2P.RejectedTxTopic
	if rtn == "" {
		return nil, errors.NewConfigurationError("p2p_rejected_tx_topic not set in config")
	}

	nstn := tSettings.P2P.NodeStatusTopic
	if nstn == "" {
		nstn = "node_status" // Default value for backward compatibility
	}

	sharedKey := tSettings.P2P.SharedKey
	if sharedKey == "" {
		return nil, errors.NewConfigurationError("error getting p2p_shared_key")
	}

	listenMode := tSettings.P2P.ListenMode
	if listenMode != settings.ListenModeFull && listenMode != settings.ListenModeListenOnly {
		return nil, errors.NewConfigurationError("listen_mode must be either '%s' or '%s' (got '%s')", settings.ListenModeFull, settings.ListenModeListenOnly, listenMode)
	}

	banlist, banChan, err := GetBanList(ctx, logger, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("error getting banlist", err)
	}

	usePrivateDht := tSettings.P2P.DHTUsePrivate
	optimiseRetries := tSettings.P2P.OptimiseRetries

	staticPeers := tSettings.P2P.StaticPeers

	// Merge bootstrap addresses into static peers if persistent bootstrap is enabled
	if tSettings.P2P.BootstrapPersistent {
		logger.Infof("Bootstrap persistent mode enabled - adding %d bootstrap addresses to static peers", len(tSettings.P2P.BootstrapAddresses))
		staticPeers = append(staticPeers, tSettings.P2P.BootstrapAddresses...)
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

	// Log NAT configuration
	if tSettings.P2P.EnableNATService {
		logger.Infof("AutoNAT service enabled for address detection")
	}
	if tSettings.P2P.EnableNATPortMap {
		logger.Infof("NAT port mapping enabled (UPnP/NAT-PMP)")
	}
	if tSettings.P2P.EnableHolePunching {
		logger.Infof("NAT hole punching enabled (DCUtR)")
	}
	if tSettings.P2P.EnableRelay {
		logger.Infof("Relay service enabled (Circuit Relay v2)")
	}

	// Important: When behind NAT, enabling NAT features helps with connectivity
	if len(advertiseAddresses) == 0 && !tSettings.P2P.EnableNATService {
		logger.Infof("NAT service disabled - node will rely on go-p2p's automatic public IP detection")
	}

	config := p2p.Config{
		ProcessName:        "peer",
		ListenAddresses:    listenAddresses,
		AdvertiseAddresses: advertiseAddresses,
		Port:               p2pPort,
		PrivateKey:         privateKey,
		SharedKey:          sharedKey,
		UsePrivateDHT:      usePrivateDht,
		OptimiseRetries:    optimiseRetries,
		Advertise:          true,
		StaticPeers:        staticPeers,
		BootstrapAddresses: tSettings.P2P.BootstrapAddresses,
		DHTProtocolID:      tSettings.P2P.DHTProtocolID,
		// libp2p feature toggles
		EnableNATService:   tSettings.P2P.EnableNATService,
		EnableHolePunching: tSettings.P2P.EnableHolePunching,
		EnableRelay:        tSettings.P2P.EnableRelay,
		EnableNATPortMap:   tSettings.P2P.EnableNATPortMap,
		// Enhanced NAT traversal features
		EnableAutoNATv2:    tSettings.P2P.EnableAutoNATv2,
		ForceReachability:  tSettings.P2P.ForceReachability,
		EnableRelayService: tSettings.P2P.EnableRelayService,
		// Connection management
		EnableConnManager: tSettings.P2P.EnableConnManager,
		ConnLowWater:      tSettings.P2P.ConnLowWater,
		ConnHighWater:     tSettings.P2P.ConnHighWater,
		ConnGracePeriod:   tSettings.P2P.ConnGracePeriod,
		EnableConnGater:   tSettings.P2P.EnableConnGater,
		MaxConnsPerPeer:   tSettings.P2P.MaxConnsPerPeer,
		// Peer persistence
		EnablePeerCache: tSettings.P2P.EnablePeerCache,
		PeerCacheFile:   getPeerCacheFilePath(tSettings.P2P.PeerCacheDir),
		MaxCachedPeers:  tSettings.P2P.MaxCachedPeers,
		PeerCacheTTL:    tSettings.P2P.PeerCacheTTL,
	}

	// Log peer cache configuration for debugging
	logger.Infof("P2P Peer Cache Config - Enabled: %v, File: %s, MaxCached: %d, TTL: %v",
		config.EnablePeerCache, config.PeerCacheFile, config.MaxCachedPeers, config.PeerCacheTTL)

	p2pNode, err := p2p.NewNode(ctx, logger, config)
	if err != nil {
		return nil, errors.NewServiceError("Error creating P2PNode", err)
	}

	// Log P2P node creation
	logger.Infof("P2P node created successfully")
	// The node will learn its external address via libp2p's Identify protocol
	// when peers connect and tell us what address they see us from

	p2pServer := &Server{
		P2PNode:             p2pNode,
		logger:              logger,
		settings:            tSettings,
		bitcoinProtocolID:   fmt.Sprintf("teranode/bitcoin/%s", tSettings.Version),
		notificationCh:      make(chan *notificationMsg, 1_000),
		blockchainClient:    blockchainClient,
		blockAssemblyClient: blockAssemblyClient,
		banList:             banlist,
		banChan:             banChan,

		rejectedTxKafkaConsumerClient:     rejectedTxKafkaConsumerClient,
		invalidBlocksKafkaConsumerClient:  invalidBlocksKafkaConsumerClient,
		invalidSubtreeKafkaConsumerClient: invalidSubtreeKafkaConsumerClient,
		subtreeKafkaProducerClient:        subtreeKafkaProducerClient,
		blocksKafkaProducerClient:         blocksKafkaProducerClient,
		gCtx:                              ctx,
		blockTopicName:                    fmt.Sprintf("%s-%s", topicPrefix, btn),
		subtreeTopicName:                  fmt.Sprintf("%s-%s", topicPrefix, stn),
		miningOnTopicName:                 fmt.Sprintf("%s-%s", topicPrefix, miningOntn),
		rejectedTxTopicName:               fmt.Sprintf("%s-%s", topicPrefix, rtn),
		invalidBlocksTopicName:            tSettings.Kafka.InvalidBlocks,
		invalidSubtreeTopicName:           tSettings.Kafka.InvalidSubtrees,
		handshakeTopicName:                fmt.Sprintf("%s-%s", topicPrefix, htn),
		nodeStatusTopicName:               fmt.Sprintf("%s-%s", topicPrefix, nstn),
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
	p2pServer.peerSelector = NewPeerSelector(logger)
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
// - Sets up topic handlers for blocks, subtrees, mining notifications, and peer handshakes
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
	s.rejectedTxKafkaConsumerClient.Start(ctx, s.rejectedHandler(ctx), kafka.WithRetryAndMoveOn(0, 1, time.Second))

	// Handler for invalid blocks Kafka messages
	if s.invalidBlocksKafkaConsumerClient != nil {
		s.logger.Infof("[Start] Starting invalid blocks Kafka consumer on topic: %s", s.invalidBlocksTopicName)
		s.invalidBlocksKafkaConsumerClient.Start(ctx, s.invalidBlockHandler(ctx), kafka.WithRetryAndMoveOn(0, 1, time.Second))
	}

	// Handler for invalid subtrees Kafka messages
	if s.invalidSubtreeKafkaConsumerClient != nil {
		s.logger.Infof("[Start] Starting invalid subtrees Kafka consumer on topic: %s", s.invalidSubtreeTopicName)
		s.invalidSubtreeKafkaConsumerClient.Start(ctx, s.invalidSubtreeHandler(ctx), kafka.WithRetryAndMoveOn(0, 1, time.Second))
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
			if err == http.ErrServerClosed {
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
				if s.P2PNode != nil {
					// Get current peer addresses from the P2P node
					peers := s.P2PNode.ConnectedPeers()
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

	// Set up the peer connected callback to announce our best block when a new peer connects
	s.P2PNode.SetPeerConnectedCallback(s.P2PNodeConnected)

	err = s.P2PNode.Start(
		ctx,
		s.receiveHandshakeStreamHandler,
		s.handshakeTopicName,
		s.blockTopicName,
		s.subtreeTopicName,
		s.miningOnTopicName,
		s.rejectedTxTopicName,
		s.nodeStatusTopicName,
	)
	if err != nil {
		return errors.NewServiceError("error starting p2p node", err)
	}

	// wait for P2P node to be ready for handler registration using a polling mechanism
	if err := s.waitForP2PNodeReadiness(ctx); err != nil {
		return err
	}

	// register remaining handlers
	if err := s.registerRemainingHandlers(ctx); err != nil {
		// continue despite errors, but log the issue
		s.logger.Warnf("[Server] Some P2P topic handlers failed to register: %v", err)
	}

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

	// Send initial handshake (version)
	s.sendHandshake(ctx)

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

func (s *Server) rejectedHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
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

		var m kafkamessage.KafkaRejectedTxTopicMessage
		if err := proto.Unmarshal(msg.Value, &m); err != nil {
			s.logger.Errorf("[rejectedHandler] error unmarshalling rejectedTxMessage: %v", err)
			return err
		}

		hash, err := chainhash.NewHashFromStr(m.TxHash)
		if err != nil {
			s.logger.Errorf("[rejectedHandler] error getting chainhash from string %s: %v", m.TxHash, err)
			return err
		}

		s.logger.Debugf("[rejectedHandler] Received %s rejected tx notification: %s", hash.String(), m.Reason)

		rejectedTxMessage := p2p.RejectedTxMessage{
			TxID:   hash.String(),
			Reason: m.Reason,
			PeerID: s.P2PNode.HostID().String(),
		}

		msgBytes, err := json.Marshal(rejectedTxMessage)
		if err != nil {
			s.logger.Errorf("[rejectedHandler] json marshal error: %v", err)

			return err
		}

		s.logger.Debugf("[rejectedHandler] publishing rejectedTxMessage")

		if err := s.P2PNode.Publish(ctx, s.rejectedTxTopicName, msgBytes); err != nil {
			s.logger.Errorf("[rejectedHandler] publish error: %v", err)
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

func (s *Server) sendHandshake(ctx context.Context) {
	localHeight := uint32(0)
	bestHash := ""

	if header, bhMeta, err := s.blockchainClient.GetBestBlockHeader(ctx); err == nil && bhMeta != nil {
		localHeight = bhMeta.Height
		bestHash = header.Hash().String()
	}

	msg := p2p.HandshakeMessage{
		Type:        "version",
		PeerID:      s.P2PNode.HostID().String(),
		BestHeight:  localHeight,
		BestHash:    bestHash,
		DataHubURL:  s.AssetHTTPAddressURL,
		UserAgent:   s.bitcoinProtocolID,
		Services:    0,
		TopicPrefix: s.topicPrefix,
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		s.logger.Errorf("[sendHandshake][p2p-handshake] json marshal error: %v", err)
		return
	}

	s.logger.Infof("[sendHandshake] Sending version handshake: PeerID=%s, URL=%s, BestHeight=%d, Topic=%s", msg.PeerID, msg.DataHubURL, msg.BestHeight, s.handshakeTopicName)

	go func() {
		var err error

		// should we wait for the topic to have enough subscribers before publishing?
		if s.settings != nil && s.settings.P2P.HandshakeTopicSize > 0 {
			ctx, cancel := context.WithTimeout(ctx, s.settings.P2P.HandshakeTopicTimeout)
			defer cancel()

			err = s.P2PNode.GetTopic(s.handshakeTopicName).Publish(ctx, msgBytes, pubsub.WithReadiness(pubsub.MinTopicSize(s.settings.P2P.HandshakeTopicSize)))
		} else {
			err = s.P2PNode.Publish(ctx, s.handshakeTopicName, msgBytes)
		}

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				s.logger.Warnf("[sendHandshake][p2p-handshake] (handshake topic size: %d), publish timeout - no subscribers yet", s.settings.P2P.HandshakeTopicSize)
				return
			}

			// Don't log errors if context was canceled (during shutdown)
			if errors.Is(err, context.Canceled) {
				s.logger.Debugf("[sendHandshake][p2p-handshake] context canceled during shutdown")
				return
			}

			s.logger.Errorf("[sendHandshake][p2p-handshake] (handshake topic size: %d), publish error: %v", s.settings.P2P.HandshakeTopicSize, err)
			return
		}

		s.logger.Infof("[sendHandshake] Successfully published handshake to topic %s", s.handshakeTopicName)
	}()
}

func (s *Server) sendDirectHandshake(ctx context.Context, peerID peer.ID) {
	localHeight := uint32(0)
	bestHash := ""

	if header, bhMeta, err := s.blockchainClient.GetBestBlockHeader(ctx); err == nil && bhMeta != nil {
		localHeight = bhMeta.Height
		bestHash = header.Hash().String()
	}

	msg := p2p.HandshakeMessage{
		Type:        "version",
		PeerID:      s.P2PNode.HostID().String(),
		BestHeight:  localHeight,
		BestHash:    bestHash,
		DataHubURL:  s.AssetHTTPAddressURL,
		UserAgent:   s.bitcoinProtocolID,
		Services:    0,
		TopicPrefix: s.topicPrefix,
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		s.logger.Errorf("[sendDirectHandshake] json marshal error: %v", err)
		return
	}

	s.logger.Infof("[sendDirectHandshake] Sending direct handshake to peer %s: BestHeight=%d", peerID.String(), localHeight)

	// try to send handshake directly to the peer via stream
	if err := s.sendHandshakeToPeer(ctx, peerID, msgBytes); err != nil {
		// if stream-based handshake fails (e.g., protocol not supported), fall back to pubsub
		s.logger.Warnf("[sendDirectHandshake] Stream handshake failed for peer %s: %v, falling back to pubsub", peerID.String(), err)

		// send via pubsub topic with a small delay to ensure peer is subscribed
		go func() {
			time.Sleep(2 * time.Second)
			if err := s.P2PNode.Publish(ctx, s.handshakeTopicName, msgBytes); err != nil {
				s.logger.Errorf("[sendDirectHandshake] Fallback pubsub handshake also failed: %v", err)
			} else {
				s.logger.Infof("[sendDirectHandshake] Successfully sent handshake via pubsub fallback to peer %s", peerID.String())
			}
		}()
	} else {
		s.logger.Infof("[sendDirectHandshake] Successfully sent direct handshake to peer %s", peerID.String())
	}
}

func (s *Server) sendHandshakeToPeer(ctx context.Context, peerID peer.ID, msgBytes []byte) error {
	// check listen mode - allow handshakes even in listen_only mode
	// note: SendToPeer has its own listen mode check, but we want handshakes to work
	// so we temporarily bypass it or modify SendToPeer to allow handshakes
	return s.P2PNode.SendToPeer(ctx, peerID, msgBytes)
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

	// Update last message time for the sender
	s.peerRegistry.UpdateLastMessageTime(peer.ID(from))

	// Also update for the originator if different
	if originatorPeerID != "" {
		if peerID, err := peer.Decode(originatorPeerID); err == nil && peerID != peer.ID(from) {
			s.peerRegistry.UpdateLastMessageTime(peerID)
		}
	}
}

// NodeStatusMessage represents a node status update message
type NodeStatusMessage struct {
	Type                 string                          `json:"type"`
	BaseURL              string                          `json:"base_url"`
	PeerID               string                          `json:"peer_id"`
	Version              string                          `json:"version"`
	CommitHash           string                          `json:"commit_hash"`
	BestBlockHash        string                          `json:"best_block_hash"`
	BestHeight           uint32                          `json:"best_height"`
	BlockAssemblyDetails *blockassembly_api.StateMessage `json:"block_assembly_details,omitempty"` // Details about the current block assembly state
	FSMState             string                          `json:"fsm_state"`
	StartTime            int64                           `json:"start_time"`
	Uptime               float64                         `json:"uptime"`
	ClientName           string                          `json:"client_name"` // Name of this node client
	MinerName            string                          `json:"miner_name"`  // Name of the miner that mined the best block
	ListenMode           string                          `json:"listen_mode"`
	ChainWork            string                          `json:"chain_work"`                     // Chain work as hex string
	SyncPeerID           string                          `json:"sync_peer_id,omitempty"`         // ID of the peer we're syncing from
	SyncPeerHeight       int32                           `json:"sync_peer_height,omitempty"`     // Height of the sync peer
	SyncPeerBlockHash    string                          `json:"sync_peer_block_hash,omitempty"` // Best block hash of the sync peer
	SyncConnectedAt      int64                           `json:"sync_connected_at,omitempty"`    // Unix timestamp when we first connected to this sync peer
}

func (s *Server) handleNodeStatusTopic(_ context.Context, m []byte, from string) {
	var nodeStatusMessage NodeStatusMessage
	if err := json.Unmarshal(m, &nodeStatusMessage); err != nil {
		s.logger.Errorf("[handleNodeStatusTopic] json unmarshal error: %v", err)
		return
	}

	// Check if this is our own message
	isSelf := from == s.P2PNode.HostID().String()

	// Log all received node_status messages for debugging
	s.logger.Debugf("[handleNodeStatusTopic] Received node_status from %s (peer_id: %s, is_self: %v, version: %s, height: %d)",
		from, nodeStatusMessage.PeerID, isSelf, nodeStatusMessage.Version, nodeStatusMessage.BestHeight)

	// Skip further processing for our own messages (peer height updates, etc.)
	// but still forward to WebSocket
	if !isSelf {
		s.logger.Debugf("[handleNodeStatusTopic] Processing node_status from remote peer %s (peer_id: %s)", from, nodeStatusMessage.PeerID)

		// Update last message time for the sender and originator
		s.updatePeerLastMessageTime(from, nodeStatusMessage.PeerID)
	} else {
		s.logger.Debugf("[handleNodeStatusTopic] forwarding our own node status (peer_id: %s) with is_self=true", nodeStatusMessage.PeerID)
	}

	// Send to notification channel for WebSocket clients
	s.notificationCh <- &notificationMsg{
		Timestamp:            time.Now().UTC().Format(isoFormat),
		Type:                 "node_status",
		BaseURL:              nodeStatusMessage.BaseURL,
		PeerID:               nodeStatusMessage.PeerID,
		Version:              nodeStatusMessage.Version,
		CommitHash:           nodeStatusMessage.CommitHash,
		BestBlockHash:        nodeStatusMessage.BestBlockHash,
		BestHeight:           nodeStatusMessage.BestHeight,
		BlockAssemblyDetails: nodeStatusMessage.BlockAssemblyDetails,
		FSMState:             nodeStatusMessage.FSMState,
		StartTime:            nodeStatusMessage.StartTime,
		Uptime:               nodeStatusMessage.Uptime,
		ClientName:           nodeStatusMessage.ClientName,
		MinerName:            nodeStatusMessage.MinerName,
		ListenMode:           nodeStatusMessage.ListenMode,
		ChainWork:            nodeStatusMessage.ChainWork,
		SyncPeerID:           nodeStatusMessage.SyncPeerID,
		SyncPeerHeight:       nodeStatusMessage.SyncPeerHeight,
		SyncPeerBlockHash:    nodeStatusMessage.SyncPeerBlockHash,
		SyncConnectedAt:      nodeStatusMessage.SyncConnectedAt,
	}

	// Update peer height if provided (but not for our own messages)
	if !isSelf && nodeStatusMessage.BestHeight > 0 && nodeStatusMessage.PeerID != "" {
		if peerID, err := peer.Decode(nodeStatusMessage.PeerID); err == nil {
			// Ensure this peer is in the registry
			s.addPeer(peerID)

			s.P2PNode.UpdatePeerHeight(peerID, int32(nodeStatusMessage.BestHeight)) //nolint:gosec

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
		}
	}

	// Also ensure the sender is in the registry
	if !isSelf && from != "" {
		if senderID, err := peer.Decode(from); err == nil {
			s.addPeer(senderID)
		}
	}
}

func (s *Server) handleHandshakeTopic(ctx context.Context, m []byte, from string) {
	s.logger.Debugf("[handleHandshakeTopic] Received handshake from %s, message: %s", from, string(m))

	var hs p2p.HandshakeMessage
	if err := json.Unmarshal(m, &hs); err != nil {
		s.logger.Errorf("[handleHandshakeTopic][p2p-handshake] json unmarshal error: %v", err)
		return
	}

	s.logger.Debugf("[handleHandshakeTopic] Parsed handshake: Type=%s, PeerID=%s, BestHeight=%d, TopicPrefix=%s", hs.Type, hs.PeerID, hs.BestHeight, hs.TopicPrefix)
	s.logger.Debugf("[handleHandshakeTopic] Our HostID=%s, Message from=%s, Message PeerID=%s", s.P2PNode.HostID().String(), from, hs.PeerID)

	if hs.PeerID == s.P2PNode.HostID().String() {
		s.logger.Debugf("[handleHandshakeTopic] Ignoring self handshake (PeerID matches our HostID)")
		return // ignore self
	}

	// Update last message time for the sender and originator
	s.updatePeerLastMessageTime(from, hs.PeerID)

	// Validate topic prefix to ensure we're on the same chain
	if hs.TopicPrefix != s.topicPrefix {
		s.logger.Warnf("[handleHandshakeTopic] Ignoring peer %s with incompatible topic prefix: got %s, expected %s", hs.PeerID, hs.TopicPrefix, s.topicPrefix)
		return // ignore peers on different chains
	}
	s.logger.Debugf("[handleHandshakeTopic] Topic prefix validation passed for peer %s", hs.PeerID)
	// update peer height and store starting height if first time seeing this peer
	if peerID, err := peer.Decode(hs.PeerID); err == nil {
		// store starting height if we haven't seen this peer before
		if _, exists := s.P2PNode.GetPeerStartingHeight(peerID); !exists {
			s.logger.Infof("[handleHandshakeTopic] Setting starting height for peer %s to %d", peerID.String(), hs.BestHeight)
			s.P2PNode.SetPeerStartingHeight(peerID, int32(hs.BestHeight)) //nolint:gosec
		} else {
			s.logger.Debugf("[handleHandshakeTopic] Peer %s already has starting height set", peerID.String())
		}
		s.P2PNode.UpdatePeerHeight(peerID, int32(hs.BestHeight)) //nolint:gosec

		// Ensure peer exists in registry before updating
		s.addPeer(peerID)

		// Store the peer's best block hash and DataHub URL for sync purposes
		if hs.BestHash != "" {
			s.updateBlockHash(peerID, hs.BestHash)
			s.logger.Debugf("[handleHandshakeTopic] Stored best hash %s for peer %s", hs.BestHash, peerID.String())
		}
		if hs.DataHubURL != "" {
			s.updateDataHubURL(peerID, hs.DataHubURL)
			s.logger.Debugf("[handleHandshakeTopic] Stored DataHub URL %s for peer %s", hs.DataHubURL, peerID.String())
		} else {
			s.logger.Warnf("[handleHandshakeTopic] Peer %s has no DataHub URL - cannot be used as sync peer", peerID.String())
		}

		s.updatePeerHeight(peerID, int32(hs.BestHeight))
	}

	s.logger.Debugf("[handleHandshakeTopic] Message type: %s, from peer: %s, height: %d", hs.Type, hs.PeerID, hs.BestHeight)

	switch hs.Type {
	case "version":
		if err := s.sendVerack(ctx, from, hs); err != nil {
			s.logger.Errorf("[handleHandshakeTopic][p2p-handshake] error sending verack: %v", err)
		}
	case "verack":
		s.logger.Infof("[handleHandshakeTopic][p2p-handshake] received verack from %s url=%s height=%d hash=%s agent=%s services=%d", hs.PeerID, hs.DataHubURL, hs.BestHeight, hs.BestHash, hs.UserAgent, hs.Services)

		// Get our best block for comparison
		localHeight := uint32(0)

		var localMeta *model.BlockHeaderMeta

		_, localMeta, err := s.blockchainClient.GetBestBlockHeader(ctx)
		if err == nil && localMeta != nil {
			localHeight = localMeta.Height

			// If we have a higher block than the peer who just connected
			if localHeight > hs.BestHeight {
				s.logger.Debugf("[handleHandshakeTopic][p2p-handshake] our height (%d) is higher than peer %s (%d)", localHeight, hs.PeerID, hs.BestHeight)
			} else if hs.BestHeight > localHeight && s.syncCoordinator != nil {
				// If peer is ahead and we don't have a bootstrap, so no other peers are going
				// to become available therefore we can trigger immediate sync
				if len(s.settings.P2P.BootstrapAddresses) == 0 {
					s.logger.Infof("[handleHandshakeTopic][p2p-handshake] no bootstrap detected, peer %s is ahead (height %d > %d), triggering immediate sync",
						hs.PeerID, hs.BestHeight, localHeight)
					if err := s.syncCoordinator.TriggerSync(); err != nil {
						s.logger.Warnf("[handleHandshakeTopic] Failed to trigger sync: %v", err)
					}
				}
			}
		}
	}
}

func (s *Server) sendVerack(ctx context.Context, from string, hs p2p.HandshakeMessage) error {
	localHeight := uint32(0)
	bestHash := ""

	if header, bhMeta, err := s.blockchainClient.GetBestBlockHeader(ctx); err == nil && bhMeta != nil {
		localHeight = bhMeta.Height
		bestHash = header.Hash().String()
	}

	ack := p2p.HandshakeMessage{
		Type:        "verack",
		PeerID:      s.P2PNode.HostID().String(),
		BestHeight:  localHeight,
		BestHash:    bestHash,
		DataHubURL:  s.AssetHTTPAddressURL,
		UserAgent:   s.bitcoinProtocolID,
		Services:    0,
		TopicPrefix: s.topicPrefix,
	}

	ackBytes, err := json.Marshal(ack)
	if err != nil {
		s.logger.Errorf("[sendVerack][p2p-handshake] json marshal error: %v", err)
		return err
	}
	// Decode the 'from' peer ID string just before sending the response
	requesterPID, err := peer.Decode(from)
	if err != nil {
		s.logger.Errorf("[handleBestBlockTopic][p2p-handshake] error decoding requester peerId '%s': %v", from, err)
		return err
	}

	// send best block to the requester using the decoded peer.ID
	err = s.P2PNode.SendToPeer(ctx, requesterPID, ackBytes)
	if err != nil {
		s.logger.Errorf("[handleBestBlockTopic][p2p-handshake] error sending peer message: %v", err)
		return err
	}
	return nil
}

func (s *Server) receiveHandshakeStreamHandler(ns network.Stream) {
	defer ns.Close()
	s.logger.Debugf("[streamHandler][%s][p2p-handshake]", s.P2PNode.GetProcessName())

	var (
		buf []byte
		err error
	)

	for {
		buf, err = io.ReadAll(ns)
		if err != nil {
			_ = ns.Reset()

			s.logger.Errorf("[streamHandler][%s][p2p-handshake] failed to read network stream: %+v", s.P2PNode.GetProcessName(), err)

			return
		}

		_ = ns.Close()

		if len(buf) > 0 {
			s.logger.Debugf("[streamHandler][%s][p2p-handshake] Received message: %s", s.P2PNode.GetProcessName(), string(buf))

			break
		}

		s.logger.Debugf("[streamHandler][%s][p2p-handshake] No message received, waiting...", s.P2PNode.GetProcessName())

		time.Sleep(1 * time.Second)
	}

	s.P2PNode.UpdateBytesReceived(uint64(len(buf)))
	s.P2PNode.UpdateLastReceived()
	s.handleHandshakeTopic(s.gCtx, buf, ns.Conn().RemotePeer().String())
}

func (s *Server) P2PNodeConnected(ctx context.Context, peerID peer.ID) {
	s.logger.Infof("[P2PNodeConnected] Peer connected: %s", peerID.String())
	s.logger.Debugf("[P2PNodeConnected] Total connected peers: %d", len(s.P2PNode.ConnectedPeers()))

	// Add peer to SyncCoordinator
	// Add peer to registry
	s.addPeer(peerID)

	// try to get the peer's current height from the first message/block data we have
	// and use it as starting height since handshake protocol isn't used by other peers
	go func() {
		// give some time for the peer to potentially send initial data
		time.Sleep(500 * time.Millisecond)

		// check if we already have height info for this peer from other sources (block messages, etc)
		peerInfos := s.P2PNode.ConnectedPeers()
		for _, peerInfo := range peerInfos {
			if peerInfo.ID == peerID {
				// if we don't have a starting height yet, use current height as starting height
				if _, exists := s.P2PNode.GetPeerStartingHeight(peerID); !exists && peerInfo.CurrentHeight > 0 {
					s.logger.Debugf("[P2PNodeConnected] Setting starting height for peer %s to %d (from initial connection data)", peerID.String(), peerInfo.CurrentHeight)
					s.P2PNode.SetPeerStartingHeight(peerID, peerInfo.CurrentHeight)
				}
				break
			}
		}
	}()

	// send handshake (version) when a new peer connects using direct stream (no timing issues)
	s.logger.Debugf("[P2PNodeConnected] Sending direct handshake in response to new peer connection")
	go s.sendDirectHandshake(ctx, peerID)
}

func (s *Server) handleBlockNotification(ctx context.Context, hash *chainhash.Hash) error {
	var msgBytes []byte

	h, meta, err := s.blockchainClient.GetBlockHeader(ctx, hash)
	if err != nil {
		return errors.NewError("error getting block header and meta for BlockMessage: %w", err)
	}

	blockMessage := p2p.BlockMessage{
		Hash:       hash.String(),
		Height:     meta.Height,
		DataHubURL: s.AssetHTTPAddressURL,
		PeerID:     s.P2PNode.HostID().String(),
		Header:     hex.EncodeToString(h.Bytes()),
	}

	msgBytes, err = json.Marshal(blockMessage)
	if err != nil {
		return errors.NewError("blockMessage - json marshal error: %w", err)
	}

	if err = s.P2PNode.Publish(ctx, s.blockTopicName, msgBytes); err != nil {
		return errors.NewError("blockMessage - publish error: %w", err)
	}

	// Also send a node_status update when best block changes
	if err = s.handleNodeStatusNotification(ctx); err != nil {
		// Log the error but don't fail the block notification
		s.logger.Warnf("[handleBlockNotification] error sending node status update: %v", err)
	}

	return nil
}

func (s *Server) handleMiningOnNotification(ctx context.Context) error {
	var msgBytes []byte

	header, meta, err := s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return errors.NewError("error getting block header for MiningOnMessage: %w", err)
	}

	miningOnMessage := p2p.MiningOnMessage{
		Hash:         header.Hash().String(),
		PreviousHash: header.HashPrevBlock.String(),
		DataHubURL:   s.AssetHTTPAddressURL,
		PeerID:       s.P2PNode.HostID().String(),
		Height:       meta.Height,
		Miner:        meta.Miner,
		SizeInBytes:  meta.SizeInBytes,
		TxCount:      meta.TxCount,
	}

	msgBytes, err = json.Marshal(miningOnMessage)
	if err != nil {
		return errors.NewError("miningOnMessage - json marshal error: %w", err)
	}

	s.logger.Debugf("P2P publishing miningOnMessage")

	if err = s.P2PNode.Publish(ctx, s.miningOnTopicName, msgBytes); err != nil {
		return errors.NewError("miningOnMessage - publish error: %w", err)
	}

	// Also send to local WebSocket clients
	s.notificationCh <- &notificationMsg{
		Timestamp:    time.Now().UTC().Format(isoFormat),
		Type:         "mining_on",
		Hash:         miningOnMessage.Hash,
		BaseURL:      miningOnMessage.DataHubURL,
		PeerID:       miningOnMessage.PeerID,
		PreviousHash: miningOnMessage.PreviousHash,
		Height:       miningOnMessage.Height,
		Miner:        miningOnMessage.Miner,
		SizeInBytes:  miningOnMessage.SizeInBytes,
		TxCount:      miningOnMessage.TxCount,
	}

	// Also send a node_status update when mining starts on a new block
	if err := s.handleNodeStatusNotification(ctx); err != nil {
		// Log the error but don't fail the mining notification
		s.logger.Warnf("[handleMiningOnNotification] error sending node status update: %v", err)
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
		// Provide empty values if we can't get the best block
		bestBlockHeader = &model.BlockHeader{}
		bestBlockMeta = &model.BlockHeaderMeta{}
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

	// Get current block assembly details
	blockAssemblyDetails := &blockassembly_api.StateMessage{}
	if s.blockAssemblyClient != nil {
		blockAssemblyDetails, err = s.blockAssemblyClient.GetBlockAssemblyState(ctx)
		if err != nil {
			s.logger.Warnf("[handleNodeStatusNotification] error getting block assembly details: %s", err)
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
			for _, peerInfo := range s.P2PNode.ConnectedPeers() {
				if peerInfo.ID == syncPeer {
					syncPeerHeight = peerInfo.CurrentHeight

					// Get the peer's best block hash from registry
					if pInfo, exists := s.getPeer(syncPeer); exists {
						syncPeerBlockHash = pInfo.BlockHash
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
	if s.P2PNode != nil {
		peerID = s.P2PNode.HostID().String()
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

	// Return the notification message
	return &notificationMsg{
		Timestamp:            time.Now().UTC().Format(isoFormat),
		Type:                 "node_status",
		BaseURL:              s.AssetHTTPAddressURL,
		PeerID:               peerID,
		Version:              version,
		CommitHash:           commit,
		BestBlockHash:        blockHashStr,
		BestHeight:           height,
		BlockAssemblyDetails: blockAssemblyDetails,
		FSMState:             fsmState,
		StartTime:            startTime,
		Uptime:               uptime,
		ClientName:           clientName,
		MinerName:            minerName,
		ListenMode:           listenMode,
		ChainWork:            chainWorkStr,
		SyncPeerID:           syncPeerID,
		SyncPeerHeight:       syncPeerHeight,
		SyncPeerBlockHash:    syncPeerBlockHash,
		SyncConnectedAt:      syncConnectedAt,
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
		Type:                 "node_status",
		BaseURL:              msg.BaseURL,
		PeerID:               msg.PeerID,
		Version:              msg.Version,
		CommitHash:           msg.CommitHash,
		BestBlockHash:        msg.BestBlockHash,
		BestHeight:           msg.BestHeight,
		BlockAssemblyDetails: msg.BlockAssemblyDetails,
		FSMState:             msg.FSMState,
		StartTime:            msg.StartTime,
		Uptime:               msg.Uptime,
		ClientName:           msg.ClientName,
		MinerName:            msg.MinerName,
		ListenMode:           msg.ListenMode,
		ChainWork:            msg.ChainWork,
		SyncPeerID:           msg.SyncPeerID,
		SyncPeerHeight:       msg.SyncPeerHeight,
		SyncPeerBlockHash:    msg.SyncPeerBlockHash,
		SyncConnectedAt:      msg.SyncConnectedAt,
	}

	msgBytes, err := json.Marshal(nodeStatusMessage)
	if err != nil {
		return errors.NewError("nodeStatusMessage - json marshal error: %w", err)
	}

	s.logger.Debugf("[handleNodeStatusNotification] P2P publishing nodeStatusMessage to topic %s (height: %d, version: %s)", s.nodeStatusTopicName, nodeStatusMessage.BestHeight, nodeStatusMessage.Version)

	if err = s.P2PNode.Publish(ctx, s.nodeStatusTopicName, msgBytes); err != nil {
		return errors.NewError("nodeStatusMessage - publish error: %w", err)
	}

	s.logger.Debugf("[handleNodeStatusNotification] Successfully published node_status message")

	// Send to local WebSocket clients
	s.notificationCh <- msg

	return nil
}

func (s *Server) handleSubtreeNotification(ctx context.Context, hash *chainhash.Hash) error {
	var msgBytes []byte

	subtreeMessage := p2p.SubtreeMessage{
		Hash:       hash.String(),
		DataHubURL: s.AssetHTTPAddressURL,
		PeerID:     s.P2PNode.HostID().String(),
	}

	msgBytes, err := json.Marshal(subtreeMessage)
	if err != nil {
		return errors.NewError("subtreeMessage - json marshal error: %w", err)
	}

	if err := s.P2PNode.Publish(ctx, s.subtreeTopicName, msgBytes); err != nil {
		return errors.NewError("subtreeMessage - publish error: %w", err)
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
	case model.NotificationType_MiningOn:
		return s.handleMiningOnNotification(ctx)
	case model.NotificationType_Subtree:
		return s.handleSubtreeNotification(ctx, hash)
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

			if syncing {
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
	if s.P2PNode != nil {
		if err := s.P2PNode.Stop(ctx); err != nil {
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
		blockMessage p2p.BlockMessage
		hash         *chainhash.Hash
		err          error
	)

	// decode request
	blockMessage = p2p.BlockMessage{}

	err = json.Unmarshal(m, &blockMessage)
	if err != nil {
		s.logger.Errorf("[handleBlockTopic] json unmarshal error: %v", err)
		return
	}

	s.logger.Infof("[handleBlockTopic] got p2p block notification for %s from %s (originator: %s)", blockMessage.Hash, from, blockMessage.PeerID)

	s.notificationCh <- &notificationMsg{
		Timestamp: time.Now().UTC().Format(isoFormat),
		Type:      "block",
		Hash:      blockMessage.Hash,
		Height:    blockMessage.Height,
		BaseURL:   blockMessage.DataHubURL,
		PeerID:    blockMessage.PeerID,
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
		s.P2PNode.UpdatePeerHeight(peer.ID(blockMessage.PeerID), int32(blockMessage.Height))
		// Update peer height in registry
		if peerID, err := peer.Decode(blockMessage.PeerID); err == nil {
			s.updatePeerHeight(peerID, int32(blockMessage.Height))
		}
	}

	// Check if we should skip during sync
	if s.shouldSkipDuringSync(from, blockMessage.PeerID, blockMessage.Height, "handleBlockTopic") {
		return
	}

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
			Value: value,
		})
	}
}

func (s *Server) handleSubtreeTopic(_ context.Context, m []byte, from string) {
	var (
		subtreeMessage p2p.SubtreeMessage
		hash           *chainhash.Hash
		err            error
	)

	// decode request
	subtreeMessage = p2p.SubtreeMessage{}

	err = json.Unmarshal(m, &subtreeMessage)
	if err != nil {
		s.logger.Errorf("[handleSubtreeTopic] json unmarshal error: %v", err)
		return
	}

	s.logger.Debugf("[handleSubtreeTopic] got p2p subtree notification for %s from %s (originator: %s)", subtreeMessage.Hash, from, subtreeMessage.PeerID)

	if s.isBlacklistedBaseURL(subtreeMessage.DataHubURL) {
		s.logger.Errorf("[handleSubtreeTopic] Blocked subtree notification from blacklisted baseURL: %s", subtreeMessage.DataHubURL)
		return
	}

	now := time.Now().UTC()

	s.notificationCh <- &notificationMsg{
		Timestamp: now.Format(isoFormat),
		Type:      "subtree",
		Hash:      subtreeMessage.Hash,
		BaseURL:   subtreeMessage.DataHubURL,
		PeerID:    subtreeMessage.PeerID,
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
		return
	}

	hash, err = s.parseHash(subtreeMessage.Hash, "handleSubtreeTopic")
	if err != nil {
		return
	}

	// Store the peer ID that sent this subtree
	s.storePeerMapEntry(&s.subtreePeerMap, subtreeMessage.Hash, from, now)
	s.logger.Debugf("[handleSubtreeTopic] storing peer %s for subtree %s", from, subtreeMessage.Hash)

	if s.subtreeKafkaProducerClient != nil { // tests may not set this
		msg := &kafkamessage.KafkaSubtreeTopicMessage{
			Hash: hash.String(),
			URL:  subtreeMessage.DataHubURL,
		}

		value, err := proto.Marshal(msg)
		if err != nil {
			s.logger.Errorf("[handleSubtreeTopic] error marshaling KafkaSubtreeTopicMessage: %v", err)
			return
		}

		s.subtreeKafkaProducerClient.Publish(&kafka.Message{
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

func (s *Server) handleMiningOnTopic(ctx context.Context, m []byte, from string) {
	var (
		miningOnMessage p2p.MiningOnMessage
		err             error
	)

	// decode request
	miningOnMessage = p2p.MiningOnMessage{}

	err = json.Unmarshal(m, &miningOnMessage)
	if err != nil {
		s.logger.Errorf("[handleMiningOnTopic] json unmarshal error: %v", err)
		return
	}

	s.logger.Debugf("[handleMiningOnTopic] got p2p mining on notification for %s from %s (originator: %s)", miningOnMessage.Hash, from, miningOnMessage.PeerID)

	// Send to notification channel first (including our own messages for WebSocket clients)
	s.logger.Debugf("[handleMiningOnTopic] sending mining_on message to notificationCh for WebSocket clients")
	s.notificationCh <- &notificationMsg{
		Timestamp:    time.Now().UTC().Format(isoFormat),
		Type:         "mining_on",
		Hash:         miningOnMessage.Hash,
		BaseURL:      miningOnMessage.DataHubURL,
		PeerID:       miningOnMessage.PeerID,
		PreviousHash: miningOnMessage.PreviousHash,
		Height:       miningOnMessage.Height,
		Miner:        miningOnMessage.Miner,
		SizeInBytes:  miningOnMessage.SizeInBytes,
		TxCount:      miningOnMessage.TxCount,
	}

	// Skip further processing for our own messages - check both the immediate sender and the original peer
	if s.isOwnMessage(from, miningOnMessage.PeerID) {
		s.logger.Debugf("[handleMiningOnTopic] ignoring own mining_on message for %s", miningOnMessage.Hash)
		return
	}

	// Update last message time for the sender and originator
	s.updatePeerLastMessageTime(from, miningOnMessage.PeerID)

	// add height to peer info
	s.P2PNode.UpdatePeerHeight(peer.ID(miningOnMessage.PeerID), int32(miningOnMessage.Height)) //nolint:gosec

	// Update peer height from mining message
	if peerID, err := peer.Decode(miningOnMessage.PeerID); err == nil {
		s.updatePeerHeight(peerID, int32(miningOnMessage.Height))

		// Update DataHubURL if provided in the mining-on message
		// Mining-on messages are gossiped across the network when peers mine new blocks.
		// Peers we're not directly connected to will have their information forwarded
		// through the gossip network. We store their DataHubURL here so if we later
		// establish a direct connection, we already know their API endpoint for syncing.
		if miningOnMessage.DataHubURL != "" {
			s.updateDataHubURL(peerID, miningOnMessage.DataHubURL)
			s.logger.Debugf("[handleMiningOnTopic] Updated DataHub URL %s for peer %s", miningOnMessage.DataHubURL, peerID)
		}
	}

	// Store the peer's latest block hash from mining-on message
	if miningOnMessage.Hash != "" {
		// Store using the originator's peer ID
		if peerID, err := peer.Decode(miningOnMessage.PeerID); err == nil {
			s.updateBlockHash(peerID, miningOnMessage.Hash)
			s.logger.Debugf("[handleMiningOnTopic] Stored latest block hash %s for peer %s", miningOnMessage.Hash, peerID)
		}
		// Also store using the immediate sender for redundancy
		s.updateBlockHash(peer.ID(from), miningOnMessage.Hash)
		s.logger.Debugf("[handleMiningOnTopic] Stored latest block hash %s for sender %s", miningOnMessage.Hash, from)
	}

	// Skip notifications from banned peers
	if s.shouldSkipBannedPeer(from, "handleMiningOnTopic") {
		return
	}

	// Check if we should skip during sync
	if s.shouldSkipDuringSync(from, miningOnMessage.PeerID, miningOnMessage.Height, "handleMiningOnTopic") {
		return
	}

	// Mining-on messages should NOT trigger Kafka messages to blockvalidation.
	// They are only used to store peer URL associations and block hashes.
	// Block processing should only be triggered by sync messages or block announcements.
}

// GetPeers returns a list of connected peers.
func (s *Server) GetPeers(ctx context.Context, _ *emptypb.Empty) (*p2p_api.GetPeersResponse, error) {
	s.logger.Debugf("GetPeers called")

	if s.P2PNode == nil {
		return nil, errors.NewError("[GetPeers] P2PNode is not initialised")
	}

	s.logger.Debugf("Creating reply channel")
	serverPeers := s.P2PNode.ConnectedPeers()

	resp := &p2p_api.GetPeersResponse{}

	for _, sp := range serverPeers {
		if sp.ID == "" || sp.Addrs == nil {
			continue
		}

		// ignore localhost
		if sp.ID == s.P2PNode.HostID() {
			continue
		}

		// ignore bootstrap server
		if contains(s.settings.P2P.BootstrapAddresses, sp.ID.String()) {
			continue
		}

		banScore, _, _ := s.banManager.GetBanScore(sp.ID.String())

		// convert connection time to unix timestamp
		var connTime int64
		if sp.ConnTime != nil {
			connTime = sp.ConnTime.Unix()
		}

		var addr string

		if len(sp.Addrs) > 0 {
			// For GetPeers API, we always return connected peers regardless of address type
			// The SharePrivateAddresses setting only controls what we advertise to other peers,
			// not what we report in our own peer list
			addr = sp.Addrs[0].String()
		}

		// Include all connected peers
		if addr != "" {
			resp.Peers = append(resp.Peers, &p2p_api.Peer{
				Id:             sp.ID.String(),
				Addr:           addr,
				StartingHeight: sp.StartingHeight,
				CurrentHeight:  sp.CurrentHeight,
				Banscore:       int32(banScore), //nolint:gosec
				ConnTime:       connTime,
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

// ConnectPeer connects to a specific peer using the provided multiaddr
func (s *Server) ConnectPeer(ctx context.Context, req *p2p_api.ConnectPeerRequest) (*p2p_api.ConnectPeerResponse, error) {
	s.logger.Infof("[ConnectPeer] Attempting to connect to peer: %s", req.PeerAddress)

	// Check if P2P node is available
	if s.P2PNode == nil {
		return &p2p_api.ConnectPeerResponse{
			Success: false,
			Error:   "P2P node not available",
		}, nil
	}

	// Use the P2PNode's ConnectToPeer method
	if err := s.P2PNode.ConnectToPeer(ctx, req.PeerAddress); err != nil {
		s.logger.Errorf("[ConnectPeer] Failed to connect to peer %s: %v", req.PeerAddress, err)
		return &p2p_api.ConnectPeerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	s.logger.Infof("[ConnectPeer] Successfully connected to peer: %s", req.PeerAddress)
	return &p2p_api.ConnectPeerResponse{
		Success: true,
	}, nil
}

// DisconnectPeer disconnects from a specific peer
func (s *Server) DisconnectPeer(ctx context.Context, req *p2p_api.DisconnectPeerRequest) (*p2p_api.DisconnectPeerResponse, error) {
	s.logger.Infof("[DisconnectPeer] Attempting to disconnect from peer: %s", req.PeerId)

	// Check if P2P node is available
	if s.P2PNode == nil {
		return &p2p_api.DisconnectPeerResponse{
			Success: false,
			Error:   "P2P node not available",
		}, nil
	}

	// Parse the peer ID
	peerID, err := peer.Decode(req.PeerId)
	if err != nil {
		s.logger.Errorf("[DisconnectPeer] Invalid peer ID %s: %v", req.PeerId, err)
		return &p2p_api.DisconnectPeerResponse{
			Success: false,
			Error:   fmt.Sprintf("invalid peer ID: %v", err),
		}, nil
	}

	// Remove peer from SyncCoordinator
	// Remove peer from registry
	s.removePeer(peerID)

	// Clean up stored peer data (handled by removePeer which removes from registry)

	// Use the P2PNode's DisconnectPeer method
	if err := s.P2PNode.DisconnectPeer(ctx, peerID); err != nil {
		s.logger.Errorf("[DisconnectPeer] Failed to disconnect from peer %s: %v", req.PeerId, err)
		return &p2p_api.DisconnectPeerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	s.logger.Infof("[DisconnectPeer] Successfully disconnected from peer: %s", req.PeerId)
	return &p2p_api.DisconnectPeerResponse{
		Success: true,
	}, nil
}

// registerRemainingHandlers registers all topic handlers except the block handler (which is registered during readiness check)
func (s *Server) registerRemainingHandlers(ctx context.Context) error {
	var registerErrors []string

	// register subtree topic handler
	if err := s.P2PNode.SetTopicHandler(ctx, s.subtreeTopicName, s.handleSubtreeTopic); err != nil {
		s.logger.Errorf("[Server] Error setting subtree topic handler: %v", err)
		registerErrors = append(registerErrors, fmt.Sprintf("subtree:%v", err))
	} else {
		s.logger.Debugf("[Server] Successfully registered subtree topic handler")
	}

	// register mining-on topic handler
	if err := s.P2PNode.SetTopicHandler(ctx, s.miningOnTopicName, s.handleMiningOnTopic); err != nil {
		s.logger.Errorf("[Server] Error setting mining-on topic handler: %v", err)
		registerErrors = append(registerErrors, fmt.Sprintf("mining-on:%v", err))
	} else {
		s.logger.Debugf("[Server] Successfully registered mining-on topic handler")
	}

	// register handshake topic handler
	if err := s.P2PNode.SetTopicHandler(ctx, s.handshakeTopicName, s.handleHandshakeTopic); err != nil {
		s.logger.Errorf("[Server] Error setting handshake topic handler: %v", err)
		registerErrors = append(registerErrors, fmt.Sprintf("handshake:%v", err))
	} else {
		s.logger.Debugf("[Server] Successfully registered handshake topic handler")
	}
	// register node-status topic handler
	if err := s.P2PNode.SetTopicHandler(ctx, s.nodeStatusTopicName, s.handleNodeStatusTopic); err != nil {
		s.logger.Errorf("[Server] Error setting node-status topic handler: %v", err)
		registerErrors = append(registerErrors, fmt.Sprintf("node-status:%v", err))
	} else {
		s.logger.Debugf("[Server] Successfully registered node-status topic handler")
	}

	// if any errors occurred, return them as a combined error
	if len(registerErrors) > 0 {
		return errors.NewServiceError("failed to register handlers: %s", strings.Join(registerErrors, "; "))
	}

	return nil
}

// waitForP2PNodeReadiness polls until the P2P node is ready to register handlers or times out
func (s *Server) waitForP2PNodeReadiness(ctx context.Context) error {
	const (
		maxWaitTime     = 10 * time.Second
		initialInterval = 100 * time.Millisecond
		maxInterval     = 500 * time.Millisecond
		factor          = 1.5 // backoff multiplier
	)

	startTime := time.Now()
	timeout := time.After(maxWaitTime)
	interval := initialInterval
	attempts := 0

	for {
		attempts++
		s.logger.Debugf("[Server] Attempt %d: Checking if P2P node is ready for handler registration", attempts)

		// check if topics are ready by attempting to register the block topic handler
		err := s.P2PNode.SetTopicHandler(ctx, s.blockTopicName, s.handleBlockTopic)
		if err == nil {
			s.logger.Infof("[Server] P2P node ready for handler registration after %d attempts (%.2f seconds)",
				attempts, time.Since(startTime).Seconds())
			return nil
		}

		s.logger.Debugf("[Server] P2P node not ready yet (attempt %d): %v", attempts, err)

		// use select to handle timeout and context cancellation
		select {
		case <-time.After(interval):
			// increase the interval for next attempt (with exponential backoff)
			interval = time.Duration(float64(interval) * factor)
			if interval > maxInterval {
				interval = maxInterval
			}
		case <-timeout:
			// fallback to ensure we don't hang forever
			s.logger.Warnf("[Server] Timeout after %d attempts waiting for P2P node to be ready, proceeding anyway", attempts)
			return nil // continue despite timeout
		case <-ctx.Done():
			return errors.NewServiceError("context canceled while waiting for P2P node to be ready", ctx.Err())
		}
	}
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
	peers := s.P2PNode.ConnectedPeers()

	for _, peer := range peers {
		if peer.ID == peerID {
			s.logger.Infof("[disconnectBannedPeerByID] Disconnecting banned peer: %s (reason: %s)", peerID, reason)

			// Remove peer from SyncCoordinator before disconnecting
			// Remove peer from registry
			s.removePeer(peerID)

			// Block hash is cleared when peer is removed from registry

			// Disconnect the peer
			err := s.P2PNode.DisconnectPeer(ctx, peerID)
			if err != nil {
				s.logger.Errorf("[disconnectBannedPeerByID] Error disconnecting peer %s: %v", peerID, err)
			}

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

	ids := h.server.P2PNode.GetPeerIPs(pid)

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

	// Clean up stored peer data (handled by removePeer which removes from registry)

	if h.server.P2PNode != nil {
		if err := h.server.P2PNode.DisconnectPeer(context.Background(), pid); err != nil {
			h.server.logger.Warnf("Failed to disconnect banned peer %s: %v", peerID, err)
		}
	}
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
		ConsumerCount:     1, // We only need one consumer for invalid blocks
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

func (s *Server) removePeer(peerID peer.ID) {
	if s.peerRegistry != nil {
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
	return from == s.P2PNode.HostID().String() || peerID == s.P2PNode.HostID().String()
}

// shouldSkipBannedPeer checks if we should skip a message from a banned peer
func (s *Server) shouldSkipBannedPeer(from string, messageType string) bool {
	if s.banManager.IsBanned(from) {
		s.logger.Debugf("[%s] ignoring notification from banned peer %s", messageType, from)
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
