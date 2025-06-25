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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/p2p/p2p_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/bitcoin-sv/teranode/util/servicemanager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libsv/go-bt/v2/chainhash"
	ma "github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	banActionAdd = "add" // Action constant for adding a ban
)

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
	P2PNode                          P2PNodeI           // The P2P network node instance - using interface instead of concrete type
	logger                           ulogger.Logger     // Logger instance for the server
	settings                         *settings.Settings // Configuration settings
	bitcoinProtocolID                string             // Bitcoin protocol identifier
	blockchainClient                 blockchain.ClientI // Client for blockchain interactions
	blockValidationClient            blockvalidation.Interface
	AssetHTTPAddressURL              string                    // HTTP address URL for assets
	e                                *echo.Echo                // Echo server instance
	notificationCh                   chan *notificationMsg     // Channel for notifications
	rejectedTxKafkaConsumerClient    kafka.KafkaConsumerGroupI // Kafka consumer for rejected transactions
	invalidBlocksKafkaConsumerClient kafka.KafkaConsumerGroupI // Kafka consumer for invalid blocks
	subtreeKafkaProducerClient       kafka.KafkaAsyncProducerI // Kafka producer for subtrees
	blocksKafkaProducerClient        kafka.KafkaAsyncProducerI // Kafka producer for blocks
	banList                          BanListI                  // List of banned peers
	banChan                          chan BanEvent             // Channel for ban events
	banManager                       *PeerBanManager           // Manager for peer banning
	gCtx                             context.Context
	blockTopicName                   string
	subtreeTopicName                 string
	miningOnTopicName                string
	rejectedTxTopicName              string
	invalidBlocksTopicName           string   // Kafka topic for invalid blocks
	handshakeTopicName               string   // pubsub topic for version/verack
	blockPeerMap                     sync.Map // Map to track which peer sent each block (hash -> peerID)
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
// - subtreeKafkaProducerClient: Kafka producer client for publishing subtree data
// - blocksKafkaProducerClient: Kafka producer client for publishing block data
//
// Returns a configured Server instance ready to be initialized and started, or an error if configuration
// validation fails or any dependencies cannot be properly initialized.
func NewServer(
	ctx context.Context,
	logger ulogger.Logger,
	tSettings *settings.Settings,
	blockchainClient blockchain.ClientI,
	rejectedTxKafkaConsumerClient kafka.KafkaConsumerGroupI,
	invalidBlocksKafkaConsumerClient kafka.KafkaConsumerGroupI,
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

	sharedKey := tSettings.P2P.SharedKey
	if sharedKey == "" {
		return nil, errors.NewConfigurationError("error getting p2p_shared_key")
	}

	banlist, banChan, err := GetBanList(ctx, logger, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("error getting banlist", err)
	}

	usePrivateDht := tSettings.P2P.DHTUsePrivate
	optimiseRetries := tSettings.P2P.OptimiseRetries

	staticPeers := tSettings.P2P.StaticPeers
	privateKey := tSettings.P2P.PrivateKey

	config := P2PConfig{
		ProcessName:        "peer",
		ListenAddresses:    listenAddresses,
		AdvertiseAddresses: tSettings.P2P.AdvertiseAddresses,
		Port:               p2pPort,
		PrivateKey:         privateKey,
		SharedKey:          sharedKey,
		UsePrivateDHT:      usePrivateDht,
		OptimiseRetries:    optimiseRetries,
		Advertise:          true,
		StaticPeers:        staticPeers,
	}

	p2pNode, err := NewP2PNode(ctx, logger, tSettings, config, blockchainClient)
	if err != nil {
		return nil, errors.NewServiceError("Error creating P2PNode", err)
	}

	p2pServer := &Server{
		P2PNode:           p2pNode,
		logger:            logger,
		settings:          tSettings,
		bitcoinProtocolID: "teranode/bitcoin/1.0.0",
		notificationCh:    make(chan *notificationMsg),
		blockchainClient:  blockchainClient,
		banList:           banlist,
		banChan:           banChan,

		rejectedTxKafkaConsumerClient:    rejectedTxKafkaConsumerClient,
		invalidBlocksKafkaConsumerClient: invalidBlocksKafkaConsumerClient,
		subtreeKafkaProducerClient:       subtreeKafkaProducerClient,
		blocksKafkaProducerClient:        blocksKafkaProducerClient,
		gCtx:                             ctx,
		blockTopicName:                   fmt.Sprintf("%s-%s", topicPrefix, btn),
		subtreeTopicName:                 fmt.Sprintf("%s-%s", topicPrefix, stn),
		miningOnTopicName:                fmt.Sprintf("%s-%s", topicPrefix, miningOntn),
		rejectedTxTopicName:              fmt.Sprintf("%s-%s", topicPrefix, rtn),
		invalidBlocksTopicName:           "invalid-blocks", // Fixed topic name for invalid blocks
		handshakeTopicName:               fmt.Sprintf("%s-%s", topicPrefix, htn),
	}

	p2pServer.banManager = NewPeerBanManager(ctx, &myBanEventHandler{server: p2pServer}, tSettings)

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
	rejectedTxHandler := func(msg *kafka.KafkaMessage) error {
		var m kafkamessage.KafkaRejectedTxTopicMessage
		if err := proto.Unmarshal(msg.Value, &m); err != nil {
			s.logger.Errorf("[Start] error unmarshalling rejectedTxMessage: %v", err)
			return err
		}

		hash, err := chainhash.NewHashFromStr(m.TxHash)
		if err != nil {
			s.logger.Errorf("[Start] error getting chainhash from string %s: %v", m.TxHash, err)
			return err
		}

		s.logger.Debugf("[Start] Received %s rejected tx notification: %s", hash.String(), m.Reason)

		rejectedTxMessage := RejectedTxMessage{
			TxID:   hash.String(),
			Reason: m.Reason,
			PeerID: s.P2PNode.HostID().String(),
		}

		msgBytes, err := json.Marshal(rejectedTxMessage)
		if err != nil {
			s.logger.Errorf("[Start] json marshal error: %v", err)

			return err
		}

		s.logger.Debugf("[Start] publishing rejectedTxMessage")

		if err := s.P2PNode.Publish(ctx, s.rejectedTxTopicName, msgBytes); err != nil {
			s.logger.Errorf("[Start] publish error: %v", err)
		}

		return nil
	}

	s.rejectedTxKafkaConsumerClient.Start(ctx, rejectedTxHandler, kafka.WithRetryAndMoveOn(0, 1, time.Second))

	// Handler for invalid blocks Kafka messages
	if s.invalidBlocksKafkaConsumerClient != nil {
		invalidBlocksHandler := func(msg *kafka.KafkaMessage) error {
			var m kafkamessage.KafkaInvalidBlockTopicMessage
			if err := proto.Unmarshal(msg.Value, &m); err != nil {
				s.logger.Errorf("[Start] error unmarshalling invalidBlocksMessage: %v", err)
				return err
			}

			s.logger.Infof("[Start] Received invalid block notification via Kafka: %s, reason: %s", m.BlockHash, m.Reason)

			// Use the existing ReportInvalidBlock method to handle the invalid block
			err = s.ReportInvalidBlock(ctx, m.BlockHash, m.Reason)
			if err != nil {
				// Don't return error here, as we want to continue processing messages
				s.logger.Errorf("[Start] Failed to report invalid block from Kafka: %v", err)
			}

			return nil
		}

		s.logger.Infof("[Start] Starting invalid blocks Kafka consumer on topic: %s", s.invalidBlocksTopicName)
		s.invalidBlocksKafkaConsumerClient.Start(ctx, invalidBlocksHandler, kafka.WithRetryAndMoveOn(0, 1, time.Second))
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

	err = s.P2PNode.Start(
		ctx,
		s.receiveHandshakeStreamHandler,
		s.handshakeTopicName,
		s.blockTopicName,
		s.subtreeTopicName,
		s.miningOnTopicName,
		s.rejectedTxTopicName,
	)
	if err != nil {
		return errors.NewServiceError("error starting p2p node", err)
	}

	// give the P2P node a moment to initialise before checking readiness
	time.Sleep(500 * time.Millisecond)

	// wait for P2P node to be ready for handler registration using a polling mechanism
	if err := s.waitForP2PNodeReadiness(ctx); err != nil {
		return err
	}

	// register remaining handlers
	if err := s.registerRemainingHandlers(ctx); err != nil {
		// continue despite errors, but log the issue
		s.logger.Warnf("[Server] Some P2P topic handlers failed to register: %v", err)
	}

	go s.blockchainSubscriptionListener(ctx)

	go s.listenForBanEvents(ctx)

	// disconnect any pre-existing banned peers at startup
	go s.disconnectPreExistingBannedPeers(ctx)

	// start the invalid blocks consumer
	if err := s.startInvalidBlockConsumer(ctx); err != nil {
		return errors.NewServiceError("failed to start invalid blocks consumer", err)
	}

	// Set up the peer connected callback to announce our best block when a new peer connects
	s.P2PNode.SetPeerConnectedCallback(s.P2PNodeConnected)

	// Send initial handshake (version)
	s.sendHandshake(ctx)

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
		"/p2p_api.PeerService/BanPeer":     true,
		"/p2p_api.PeerService/Unba§knPeer": true,
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

	msg := HandshakeMessage{
		Type:       "version",
		PeerID:     s.P2PNode.HostID().String(),
		BestHeight: localHeight,
		BestHash:   bestHash,
		DataHubURL: s.AssetHTTPAddressURL,
		UserAgent:  s.bitcoinProtocolID,
		Services:   0,
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		s.logger.Errorf("[sendHandshake][p2p-handshake] json marshal error: %v", err)
		return
	}

	go func() {
		if err := s.P2PNode.Publish(ctx, s.handshakeTopicName, msgBytes); err != nil {
			s.logger.Errorf("[sendHandshake][p2p-handshake] publish error: %v", err)
		}
	}()
}

func (s *Server) handleHandshakeTopic(ctx context.Context, m []byte, from string) {
	var hs HandshakeMessage
	if err := json.Unmarshal(m, &hs); err != nil {
		s.logger.Errorf("[handleHandshakeTopic][p2p-handshake] json unmarshal error: %v", err)
		return
	}

	if hs.PeerID == s.P2PNode.HostID().String() {
		return // ignore self
	}
	// update peer height
	if peerID2, err := peer.Decode(hs.PeerID); err == nil {
		s.P2PNode.UpdatePeerHeight(peerID2, int32(hs.BestHeight)) //nolint:gosec
	}

	if hs.Type == "version" {
		err := s.sendVerack(ctx, from, hs)
		if err != nil {
			s.logger.Errorf("[handleHandshakeTopic][p2p-handshake] error sending verack: %v", err)
		}
	} else if hs.Type == "verack" {
		s.logger.Infof("[handleHandshakeTopic][p2p-handshake] received verack from %s height=%d hash=%s agent=%s services=%d",
			hs.PeerID, hs.BestHeight, hs.BestHash, hs.UserAgent, hs.Services)

		// Get our best block for comparison
		localHeight := uint32(0)

		var localMeta *model.BlockHeaderMeta

		_, localMeta, err := s.blockchainClient.GetBestBlockHeader(ctx)
		if err == nil && localMeta != nil {
			localHeight = localMeta.Height

			// If we have a higher block than the peer who just connected
			if localHeight > hs.BestHeight {
				s.logger.Infof("[handleHandshakeTopic][p2p-handshake] our height (%d) is higher than peer %s (%d)",
					localHeight, hs.PeerID, hs.BestHeight)
			}
		}

		// Check if peer has a higher height and valid hash
		s.SyncHeights(hs, localHeight)
	}
}

func (s *Server) sendVerack(ctx context.Context, from string, hs HandshakeMessage) error {
	localHeight := uint32(0)
	bestHash := ""

	if header, bhMeta, err := s.blockchainClient.GetBestBlockHeader(ctx); err == nil && bhMeta != nil {
		localHeight = bhMeta.Height
		bestHash = header.Hash().String()
	}

	ack := HandshakeMessage{
		Type:       "verack",
		PeerID:     s.P2PNode.HostID().String(),
		BestHeight: localHeight,
		BestHash:   bestHash,
		DataHubURL: s.AssetHTTPAddressURL,
		UserAgent:  s.bitcoinProtocolID,
		Services:   0,
	}

	ackBytes, err := json.Marshal(ack)
	if err != nil {
		s.logger.Errorf("[handleHandshakeTopic][p2p-handshake] json marshal error: %v", err)
		return err
	}
	// if err := s.P2PNode.Publish(ctx, s.handshakeTopicName, ackBytes); err != nil {
	// 	s.logger.Errorf("[handleHandshakeTopic][p2p-handshake] publish error: %v", err)
	// }
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

	// Check if peer has a higher height and valid hash
	s.SyncHeights(hs, localHeight)

	return nil
}

func (s *Server) SyncHeights(hs HandshakeMessage, localHeight uint32) {
	if hs.BestHeight > localHeight {
		s.logger.Infof("[handleHandshakeTopic][p2p-handshake] type:version - peer %s has higher block (%s) at height %d > %d, sending to Kafka",
			hs.PeerID, hs.BestHash, hs.BestHeight, localHeight)

		// Send peer's block info to Kafka if we have a producer client
		if s.blocksKafkaProducerClient != nil {
			hash, err := chainhash.NewHashFromStr(hs.BestHash)
			if err != nil {
				s.logger.Errorf("[handleHandshakeTopic][p2p-handshake] type:version - error getting chainhash from string %s: %v", hs.BestHash, err)
			} else {
				msg := &kafkamessage.KafkaBlockTopicMessage{
					Hash: hash.String(),
					URL:  hs.DataHubURL,
				}

				value, err := proto.Marshal(msg)
				if err != nil {
					s.logger.Errorf("[handleHandshakeTopic][p2p-handshake] type:version - error marshaling KafkaBlockTopicMessage: %v", err)
				} else {
					s.blocksKafkaProducerClient.Publish(&kafka.Message{
						Value: value,
					})
				}
			}
		}
	} else if hs.BestHash != "" && hs.BestHeight > 0 {
		s.logger.Debugf("[handleHandshakeTopic][p2p-handshake] peer %s has block %s at height %d (our height: %d), not requesting",
			hs.PeerID, hs.BestHash, hs.BestHeight, localHeight)
	}

	// update peer height
	if peerID2, err := peer.Decode(hs.PeerID); err == nil {
		s.P2PNode.UpdatePeerHeight(peerID2, int32(hs.BestHeight)) //nolint:gosec
	}
}

func (s *Server) receiveHandshakeStreamHandler(ns network.Stream) {
	defer ns.Close()
	s.logger.Infof("[streamHandler][%s][p2p-handshake]", s.P2PNode.GetProcessName())

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
			s.logger.Infof("[streamHandler][%s][p2p-handshake] Received message: %s", s.P2PNode.GetProcessName(), string(buf))

			break
		}

		s.logger.Infof("[streamHandler][%s][p2p-handshake] No message received, waiting...", s.P2PNode.GetProcessName())

		time.Sleep(1 * time.Second)
	}

	s.P2PNode.UpdateBytesReceived(uint64(len(buf)))
	s.P2PNode.UpdateLastReceived()
	s.handleHandshakeTopic(s.gCtx, buf, ns.Conn().RemotePeer().String())
}

func (s *Server) P2PNodeConnected(ctx context.Context, peerID peer.ID) {
	s.logger.Infof("[P2PNodeConnected] Peer connected: %s", peerID.String())
	// Send handshake (version) when a new peer connects
	s.sendHandshake(ctx)
}

func (s *Server) handleBlockNotification(ctx context.Context, hash *chainhash.Hash) error {
	var msgBytes []byte

	_, meta, err := s.blockchainClient.GetBlockHeader(ctx, hash)
	if err != nil {
		return errors.NewError("error getting block header and meta for BlockMessage: %w", err)
	}

	blockMessage := BlockMessage{
		Hash:       hash.String(),
		Height:     meta.Height,
		DataHubURL: s.AssetHTTPAddressURL,
		PeerID:     s.P2PNode.HostID().String(),
	}

	msgBytes, err = json.Marshal(blockMessage)
	if err != nil {
		return errors.NewError("blockMessage - json marshal error: %w", err)
	}

	if err := s.P2PNode.Publish(ctx, s.blockTopicName, msgBytes); err != nil {
		return errors.NewError("blockMessage - publish error: %w", err)
	}

	return nil
}

func (s *Server) handleMiningOnNotification(ctx context.Context) error {
	var msgBytes []byte

	header, meta, err := s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return errors.NewError("error getting block header for MiningOnMessage: %w", err)
	}

	miningOnMessage := MiningOnMessage{
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

	if err := s.P2PNode.Publish(ctx, s.miningOnTopicName, msgBytes); err != nil {
		return errors.NewError("miningOnMessage - publish error: %w", err)
	}

	return nil
}

func (s *Server) handleSubtreeNotification(ctx context.Context, hash *chainhash.Hash) error {
	var msgBytes []byte

	subtreeMessage := SubtreeMessage{
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

func (s *Server) blockchainSubscriptionListener(ctx context.Context) {
	// Subscribe to the blockchain service
	blockchainSubscription, err := s.blockchainClient.Subscribe(ctx, "p2pServer")
	if err != nil {
		s.logger.Errorf("[blockchainSubscriptionListener] error subscribing to blockchain service: %v", err)
		return
	}

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

	s.logger.Infof("[StartHTTP] p2p service listening on %s", addr)

	go func() {
		<-ctx.Done()
		s.logger.Infof("[StartHTTP] p2p service shutting down")

		if err := s.e.Shutdown(ctx); err != nil {
			s.logger.Errorf("[StartHTTP] p2p service shutdown error: %v", err)
		}
	}()

	var err error

	if s.settings.SecurityLevelHTTP == 0 {
		servicemanager.AddListenerInfo(fmt.Sprintf("[StartHTTP] p2p HTTP listening on %s", addr))
		err = s.e.Start(addr)
	} else {
		certFile := s.settings.ServerCertFile
		if certFile == "" {
			return errors.NewConfigurationError("server_certFile is required for HTTPS")
		}

		keyFile := s.settings.ServerKeyFile
		if keyFile == "" {
			return errors.NewConfigurationError("server_keyFile is required for HTTPS")
		}

		servicemanager.AddListenerInfo(fmt.Sprintf("[StartHTTP] p2p HTTPS listening on %s", addr))
		err = s.e.StartTLS(addr, certFile, keyFile)
	}

	if err != http.ErrServerClosed {
		return err
	}

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

	if len(errs) > 0 {
		// Combine errors if multiple occurred
		// This simple approach just returns the first error, consider a multi-error type if needed
		return errs[0]
	}

	return nil
}

func (s *Server) handleBlockTopic(ctx context.Context, m []byte, from string) {
	s.logger.Debugf("[handleBlockTopic] got p2p block notification")

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

	s.logger.Debugf("[handleBlockTopic] got p2p block notification for %s from %s", blockMessage.Hash, blockMessage.PeerID)

	s.notificationCh <- &notificationMsg{
		Timestamp: time.Now().UTC().Format(isoFormat),
		Type:      "block",
		Hash:      blockMessage.Hash,
		Height:    blockMessage.Height,
		BaseURL:   blockMessage.DataHubURL,
		PeerID:    blockMessage.PeerID,
	}

	if from == s.P2PNode.HostID().String() {
		return
	}

	// Skip notifications from banned peers by IP
	if pid, err := peer.Decode(from); err == nil {
		for _, ip := range s.P2PNode.GetPeerIPs(pid) {
			if s.banList.IsBanned(ip) {
				s.logger.Debugf("[handleBlockTopic] ignoring block notification from banned peer %s (ip %s)", from, ip)
				return
			}
		}
	}

	hash, err = chainhash.NewHashFromStr(blockMessage.Hash)
	if err != nil {
		s.logger.Errorf("[handleBlockTopic] error getting chainhash from string %s: %v", blockMessage.Hash, err)
		return
	}

	// Store the peer ID that sent this block in the blockPeerMap
	s.blockPeerMap.Store(blockMessage.Hash, from)
	s.logger.Debugf("[handleBlockTopic] storing peer %s for block %s", from, blockMessage.Hash)

	// send block to kafka, if configured
	if s.blocksKafkaProducerClient != nil {
		msg := &kafkamessage.KafkaBlockTopicMessage{
			Hash: hash.String(),
			URL:  blockMessage.DataHubURL,
		}

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

func (s *Server) handleSubtreeTopic(ctx context.Context, m []byte, from string) {
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

	s.logger.Debugf("[handleSubtreeTopic] got p2p subtree notification for %s from %s", subtreeMessage.Hash, subtreeMessage.PeerID)

	s.notificationCh <- &notificationMsg{
		Timestamp: time.Now().UTC().Format(isoFormat),
		Type:      "subtree",
		Hash:      subtreeMessage.Hash,
		BaseURL:   subtreeMessage.DataHubURL,
		PeerID:    subtreeMessage.PeerID,
	}

	if from == s.P2PNode.HostID().String() {
		return
	}

	// is it from a banned peer
	// get the ip address of the peer
	peerID, err := peer.Decode(from)
	if err != nil {
		s.logger.Errorf("[handleSubtreeTopic] error decoding peer ID %s: %v", from, err)
		return
	}

	// get the ip address of the peer
	peerIPs := s.P2PNode.GetPeerIPs(peerID)
	for _, ip := range peerIPs {
		if s.banList.IsBanned(ip) {
			s.logger.Debugf("[handleSubtreeTopic] got p2p subtree notification from banned peer %s", from)
			return
		}
	}

	hash, err = chainhash.NewHashFromStr(subtreeMessage.Hash)
	if err != nil {
		s.logger.Errorf("[handleSubtreeTopic] error getting chainhash from string %s: %v", subtreeMessage.Hash, err)
		return
	}

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

func (s *Server) handleMiningOnTopic(ctx context.Context, m []byte, from string) {
	var (
		miningOnMessage MiningOnMessage
		err             error
	)

	// decode request
	miningOnMessage = MiningOnMessage{}

	err = json.Unmarshal(m, &miningOnMessage)
	if err != nil {
		s.logger.Errorf("[handleMiningOnTopic] json unmarshal error: %v", err)
		return
	}

	if from == s.P2PNode.HostID().String() {
		return
	}

	// is it from a banned peer
	peerID, err := peer.Decode(from)
	if err != nil {
		s.logger.Errorf("[handleMiningOnTopic] error decoding peer ID %s: %v", from, err)
		return
	}

	peerIPs := s.P2PNode.GetPeerIPs(peerID)
	for _, ip := range peerIPs {
		if s.banList.IsBanned(ip) {
			s.logger.Debugf("[handleMiningOnTopic] got p2p mining on notification from banned peer %s", from)
			return
		}
	}

	s.logger.Debugf("[handleMiningOnTopic] got p2p mining on notification for %s from %s", miningOnMessage.Hash, miningOnMessage.PeerID)

	// add height to peer info
	s.P2PNode.UpdatePeerHeight(peer.ID(miningOnMessage.PeerID), int32(miningOnMessage.Height)) //nolint:gosec

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

		resp.Peers = append(resp.Peers, &p2p_api.Peer{
			Id:            sp.ID.String(),
			Addr:          sp.Addrs[0].String(),
			CurrentHeight: sp.CurrentHeight,
			Banscore:      int32(banScore), //nolint:gosec
		})
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
	return &p2p_api.IsBannedResponse{IsBanned: s.banList.IsBanned(peer.IpOrSubnet)}, nil
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
	s.logger.Infof("[AddBanScore] Added score to peer %s for reason %s. New score: %d, Banned: %t",
		req.PeerId, req.Reason, score, banned)

	return &p2p_api.AddBanScoreResponse{Ok: true}, nil
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

	s.logger.Infof("[handleBanEvent] Received ban event for %s", event.IP)

	// parse the banned IP or subnet
	var bannedIP net.IP

	var bannedSubnet *net.IPNet

	if event.Subnet != nil {
		bannedSubnet = event.Subnet
	} else {
		bannedIP = net.ParseIP(event.IP)
		if bannedIP == nil {
			s.logger.Errorf("[handleBanEvent] Invalid IP address in ban event: %s", event.IP)
			return
		}
	}

	s.disconnectBannedPeers(ctx, bannedIP, bannedSubnet)
}

func (s *Server) disconnectBannedPeers(ctx context.Context, bannedIP net.IP, bannedSubnet *net.IPNet) {
	// check if we're connected to the banned IP
	peers := s.P2PNode.ConnectedPeers()

	for _, peer := range peers {
		for _, addr := range peer.Addrs {
			peerIP, err := s.getIPFromMultiaddr(ctx, addr)
			if err != nil || peerIP == nil {
				s.logger.Errorf("[disconnectBannedPeers] PeerIP is either nil or an error occurred getting IP from multiaddr %s: %v", addr, err)
				continue
			}

			s.logger.Debugf("[disconnectBannedPeers] bannedSubnet: %v, bannedIP: %v, peerIP: %s", bannedSubnet, bannedIP, peerIP)

			if isBanned(bannedIP, bannedSubnet, peerIP) {
				s.logger.Infof("[disconnectBannedPeers] Disconnecting from banned peer: %s (%s)", peer.ID, peerIP)

				err := s.P2PNode.DisconnectPeer(ctx, peer.ID)
				if err != nil {
					s.logger.Errorf("[disconnectBannedPeers] Error disconnecting from peer %s: %v", peer.ID, err)
				}

				break // no need to check other addresses for this peer
			}
		}
	}
}

func isBanned(bannedIP net.IP, bannedSubnet *net.IPNet, peerIP net.IP) bool {
	return (bannedSubnet != nil && bannedSubnet.Contains(peerIP)) ||
		(bannedIP != nil && peerIP.Equal(bannedIP))
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
	peerIDVal, ok := s.blockPeerMap.Load(blockHash)
	if !ok {
		s.logger.Warnf("[ReportInvalidBlock] no peer found for invalid block %s", blockHash)
		return errors.NewNotFoundError("no peer found for invalid block %s", blockHash)
	}

	peerID, ok := peerIDVal.(string)
	if !ok {
		s.logger.Errorf("[ReportInvalidBlock] peer ID for block %s is not a string: %v", blockHash, peerIDVal)
		return errors.NewInvalidArgumentError("peer ID for block %s is not a string", blockHash)
	}

	// Add ban score to the peer
	s.logger.Infof("[ReportInvalidBlock] adding ban score to peer %s for invalid block %s: %s", peerID, blockHash, reason)

	// Create the request to add ban score
	req := &p2p_api.AddBanScoreRequest{
		PeerId: peerID,
		Reason: "invalid_block",
	}

	// Call the AddBanScore method
	_, err := s.AddBanScore(ctx, req)
	if err != nil {
		s.logger.Errorf("[ReportInvalidBlock] error adding ban score to peer %s: %v", peerID, err)
		return errors.NewServiceError("error adding ban score to peer %s", peerID, err)
	}

	// Remove the block from the map to avoid memory leaks
	s.blockPeerMap.Delete(blockHash)

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
	peerIDVal, ok := s.blockPeerMap.Load(blockHash)
	if !ok {
		s.logger.Warnf("[handleInvalidBlockMessage] no peer found for invalid block %s", blockHash)
		return nil // Not an error, just no peer to ban
	}

	peerID, ok := peerIDVal.(string)
	if !ok {
		s.logger.Errorf("[handleInvalidBlockMessage] peer ID for block %s is not a string: %v", blockHash, peerIDVal)
		return nil
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
