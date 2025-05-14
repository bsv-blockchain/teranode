// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
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
type Server struct {
	p2p_api.UnimplementedPeerServiceServer
	P2PNode                       P2PNodeI                  // The P2P network node instance - using interface instead of concrete type
	logger                        ulogger.Logger            // Logger instance for the server
	settings                      *settings.Settings        // Configuration settings
	bitcoinProtocolID             string                    // Bitcoin protocol identifier
	blockchainClient              blockchain.ClientI        // Client for blockchain interactions
	blockValidationClient         blockvalidation.Interface // Client for block validation
	AssetHTTPAddressURL           string                    // HTTP address URL for assets
	e                             *echo.Echo                // Echo server instance
	notificationCh                chan *notificationMsg     // Channel for notifications
	rejectedTxKafkaConsumerClient kafka.KafkaConsumerGroupI // Kafka consumer for rejected transactions
	subtreeKafkaProducerClient    kafka.KafkaAsyncProducerI // Kafka producer for subtrees
	blocksKafkaProducerClient     kafka.KafkaAsyncProducerI // Kafka producer for blocks
	banList                       BanListI                  // List of banned peers
	banChan                       chan BanEvent             // Channel for ban events
	banManager                    *PeerBanManager           // Manager for peer banning
	gCtx                          context.Context
	blockTopicName                string
	subtreeTopicName              string
	miningOnTopicName             string
	rejectedTxTopicName           string
	handshakeTopicName            string // pubsub topic for version/verack
}

// NewServer creates a new P2P server instance with the provided configuration and dependencies.
func NewServer(
	ctx context.Context,
	logger ulogger.Logger,
	tSettings *settings.Settings,
	blockchainClient blockchain.ClientI,
	rejectedTxKafkaConsumerClient kafka.KafkaConsumerGroupI,
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
		return nil, errors.NewConfigurationError("p2p_topic_prefix not set in config")
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

		rejectedTxKafkaConsumerClient: rejectedTxKafkaConsumerClient,
		subtreeKafkaProducerClient:    subtreeKafkaProducerClient,
		blocksKafkaProducerClient:     blocksKafkaProducerClient,
		gCtx:                          ctx,
		blockTopicName:                fmt.Sprintf("%s-%s", topicPrefix, btn),
		subtreeTopicName:              fmt.Sprintf("%s-%s", topicPrefix, stn),
		miningOnTopicName:             fmt.Sprintf("%s-%s", topicPrefix, miningOntn),
		rejectedTxTopicName:           fmt.Sprintf("%s-%s", topicPrefix, rtn),
		handshakeTopicName:            fmt.Sprintf("%s-%s", topicPrefix, htn),
	}

	p2pServer.banManager = NewPeerBanManager(ctx, &myBanEventHandler{server: p2pServer}, tSettings)

	return p2pServer, nil
}

// Health performs health checks on the P2P server and its dependencies.
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
	s.subtreeKafkaProducerClient.Start(ctx, make(chan *kafka.Message, 10))
	s.blocksKafkaProducerClient.Start(ctx, make(chan *kafka.Message, 10))

	s.blockValidationClient, err = blockvalidation.NewClient(ctx, s.logger, s.settings, "p2p")
	if err != nil {
		return errors.NewServiceError("could not create block validation client [%w]", err)
	}

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.Recover())

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
	}))

	s.e = e

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	})

	e.GET("/p2p-ws", s.HandleWebSocket(s.notificationCh, s.AssetHTTPAddressURL))

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

	_ = s.P2PNode.SetTopicHandler(ctx, s.blockTopicName, s.handleBlockTopic)
	_ = s.P2PNode.SetTopicHandler(ctx, s.subtreeTopicName, s.handleSubtreeTopic)
	_ = s.P2PNode.SetTopicHandler(ctx, s.miningOnTopicName, s.handleMiningOnTopic)
	_ = s.P2PNode.SetTopicHandler(ctx, s.handshakeTopicName, s.handleHandshakeTopic)

	go s.blockchainSubscriptionListener(ctx)

	go s.listenForBanEvents(ctx)

	// Disconnect any pre-existing banned peers at startup
	go func() {
		for _, banned := range s.banList.ListBanned() {
			s.handleBanEvent(ctx, BanEvent{Action: banActionAdd, IP: banned})
		}
	}()

	// Set up the peer connected callback to announce our best block when a new peer connects
	s.P2PNode.SetPeerConnectedCallback(s.P2PNodeConnected)

	// Send initial handshake (version)
	s.sendHandshake(ctx)

	// this will block
	if err = util.StartGRPCServer(ctx, s.logger, s.settings, "p2p", s.settings.P2P.GRPCListenAddress, func(server *grpc.Server) {
		p2p_api.RegisterPeerServiceServer(server, s)
		closeOnce.Do(func() { close(readyCh) })
	}); err != nil {
		return errors.WrapGRPC(errors.NewServiceNotStartedError("[Legacy] can't start GRPC server", err))
	}

	<-ctx.Done()

	return nil
}

func (s *Server) sendHandshake(ctx context.Context) {
	localHeight := uint32(0)
	if _, bhMeta, err := s.blockchainClient.GetBestBlockHeader(ctx); err == nil && bhMeta != nil {
		localHeight = bhMeta.Height
	}

	msg := HandshakeMessage{
		Type:       "version",
		PeerID:     s.P2PNode.HostID().String(),
		BestHeight: localHeight,
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
		// reply with verack
		localHeight := uint32(0)
		if _, bhMeta, err := s.blockchainClient.GetBestBlockHeader(ctx); err == nil && bhMeta != nil {
			localHeight = bhMeta.Height
		}

		ack := HandshakeMessage{
			Type:       "verack",
			PeerID:     s.P2PNode.HostID().String(),
			BestHeight: localHeight,
			UserAgent:  s.bitcoinProtocolID,
			Services:   0,
		}

		ackBytes, err := json.Marshal(ack)
		if err != nil {
			s.logger.Errorf("[handleHandshakeTopic][p2p-handshake] json marshal error: %v", err)
			return
		}
		// if err := s.P2PNode.Publish(ctx, s.handshakeTopicName, ackBytes); err != nil {
		// 	s.logger.Errorf("[handleHandshakeTopic][p2p-handshake] publish error: %v", err)
		// }
		// Decode the 'from' peer ID string just before sending the response
		requesterPID, err := peer.Decode(from)
		if err != nil {
			s.logger.Errorf("[handleBestBlockTopic][p2p-handshake] error decoding requester peerId '%s': %v", from, err)
			return
		}

		// send best block to the requester using the decoded peer.ID
		err = s.P2PNode.SendToPeer(ctx, requesterPID, ackBytes)
		if err != nil {
			s.logger.Errorf("[handleBestBlockTopic][p2p-handshake] error sending peer message: %v", err)
			return
		}
	} else if hs.Type == "verack" {
		s.logger.Infof("[handleHandshakeTopic][p2p-handshake] received verack from %s height=%d agent=%s services=%d", hs.PeerID, hs.BestHeight, hs.UserAgent, hs.Services)
		// update peer height
		if peerID2, err := peer.Decode(hs.PeerID); err == nil {
			s.P2PNode.UpdatePeerHeight(peerID2, int32(hs.BestHeight)) //nolint:gosec
		}
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

func (s *Server) blockchainSubscriptionListener(ctx context.Context) {
	// Subscribe to the blockchain service
	blockchainSubscription, err := s.blockchainClient.Subscribe(ctx, "p2pServer")
	if err != nil {
		s.logger.Errorf("[blockchainSubscriptionListener] error subscribing to blockchain service: %v", err)
		return
	}

	// define vars here to prevent too many allocs
	var (
		notification    *blockchain.Notification
		blockMessage    BlockMessage
		miningOnMessage MiningOnMessage
		subtreeMessage  SubtreeMessage
		header          *model.BlockHeader
		meta            *model.BlockHeaderMeta
		msgBytes        []byte
	)

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
			cHash := chainhash.Hash(notification.Hash)
			s.logger.Debugf("P2P Received %s notification: %s", notification.Type, cHash.String())

			hash, err := chainhash.NewHash(notification.Hash)
			if err != nil {
				s.logger.Errorf("[blockchainSubscriptionListener] error getting chainhash from notification hash %s: %v", notification.Hash, err)
				continue
			}

			switch notification.Type {
			case model.NotificationType_Block:
				_, meta, err := s.blockchainClient.GetBlockHeader(ctx, hash)
				// _, meta, err := s.blockchainClient.GetBestBlockHeader(ctx)
				// // block, err := s.blockchainClient.GetBlock(ctx, notification.Hash)
				if err != nil {
					s.logger.Errorf("[blockchainSubscriptionListener] error getting block header and meta for BlockMessage: %v", err)
					continue
				}

				blockMessage = BlockMessage{
					Hash:       hash.String(),
					Height:     meta.Height,
					DataHubURL: s.AssetHTTPAddressURL,
					PeerID:     s.P2PNode.HostID().String(),
				}

				msgBytes, err = json.Marshal(blockMessage)
				if err != nil {
					s.logger.Errorf("[blockchainSubscriptionListener] blockMessage - json marshal error: %v", err)
					continue
				}

				if err := s.P2PNode.Publish(ctx, s.blockTopicName, msgBytes); err != nil {
					s.logger.Errorf("[blockchainSubscriptionListener] blockMessage - publish error: %v", err)
				}
			case model.NotificationType_MiningOn:
				header, meta, err = s.blockchainClient.GetBestBlockHeader(ctx)
				if err != nil {
					s.logger.Errorf("[blockchainSubscriptionListener] error getting block header for MiningOnMessage: %v", err)
					continue
				}

				miningOnMessage = MiningOnMessage{
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
					s.logger.Errorf("[blockchainSubscriptionListener] miningOnMessage - json marshal error: %v", err)
					continue
				}

				s.logger.Debugf("P2P publishing miningOnMessage")

				if err := s.P2PNode.Publish(ctx, s.miningOnTopicName, msgBytes); err != nil {
					s.logger.Errorf("[blockchainSubscriptionListener] miningOnMessage - publish error: %v", err)
				}
			case model.NotificationType_Subtree:
				// if it's a subtree notification send it on the subtree channel.
				subtreeMessage = SubtreeMessage{
					Hash:       hash.String(),
					DataHubURL: s.AssetHTTPAddressURL,
					PeerID:     s.P2PNode.HostID().String(),
				}

				msgBytes, err = json.Marshal(subtreeMessage)
				if err != nil {
					s.logger.Errorf("[blockchainSubscriptionListener] subtreeMessage - json marshal error: %v", err)

					continue
				}

				if err := s.P2PNode.Publish(ctx, s.subtreeTopicName, msgBytes); err != nil {
					s.logger.Errorf("[blockchainSubscriptionListener] subtreeMessage - publish error: %v", err)
				}
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

	// close the kafka consumer gracefully
	if s.rejectedTxKafkaConsumerClient != nil {
		if err := s.rejectedTxKafkaConsumerClient.Close(); err != nil {
			s.logger.Errorf("[BlockValidation] failed to close kafka consumer gracefully: %v", err)
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

		banScore, _, _ := s.banManager.GetBanScore(sp.Addrs[0].String())

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
	s.banManager.AddScore(req.PeerId, ReasonInvalidSubtree) // todo: add reason dynamically
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

	// check if we're connected to the banned IP
	peers := s.P2PNode.ConnectedPeers()
	for _, peer := range peers {
		for _, addr := range peer.Addrs {
			peerIP, err := s.getIPFromMultiaddr(ctx, addr)
			if err != nil {
				s.logger.Errorf("[handleBanEvent] Error getting IP from multiaddr %s: %v", addr, err)
				continue
			}

			if peerIP == nil {
				continue
			}

			s.logger.Debugf("[handleBanEvent] bannedSubnet: %v, bannedIP: %v, peerIP: %s", bannedSubnet, bannedIP, peerIP)

			if (bannedSubnet != nil && bannedSubnet.Contains(peerIP)) ||
				(bannedIP != nil && peerIP.Equal(bannedIP)) {
				s.logger.Infof("[handleBanEvent] Disconnecting from banned peer: %s (%s)", peer.ID, peerIP)

				err := s.P2PNode.DisconnectPeer(ctx, peer.ID)
				if err != nil {
					s.logger.Errorf("[handleBanEvent] Error disconnecting from peer %s: %v", peer.ID, err)
				}

				break // no need to check other addresses for this peer
			}
		}
	}
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
