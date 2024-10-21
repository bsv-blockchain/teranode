package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/p2p/p2p_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/bitcoin-sv/ubsv/util/p2p"
	"github.com/bitcoin-sv/ubsv/util/retry"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	blockTopicName      string
	bestBlockTopicName  string
	subtreeTopicName    string
	miningOnTopicName   string
	rejectedTxTopicName string
)

type Server struct {
	p2p_api.UnimplementedPeerServiceServer
	P2PNode                       *p2p.P2PNode
	logger                        ulogger.Logger
	bitcoinProtocolID             string
	blockchainClient              blockchain.ClientI
	blockValidationClient         *blockvalidation.Client
	AssetHTTPAddressURL           string
	e                             *echo.Echo
	notificationCh                chan *notificationMsg
	rejectedTxKafkaConsumerClient *kafka.KafkaConsumerGroup
	subtreeKafkaProducerClient    *kafka.KafkaAsyncProducer
	blocksKafkaProducerClient     *kafka.KafkaAsyncProducer
	kafkaHealthURL                *url.URL
}

func NewServer(ctx context.Context, logger ulogger.Logger, blockchainClient blockchain.ClientI) (*Server, error) {
	logger.Debugf("Creating P2P service")

	p2pIP, ok := gocore.Config().Get("p2p_ip")
	if !ok {
		return nil, errors.NewConfigurationError("p2p_ip not set in config")
	}

	p2pPort, ok := gocore.Config().GetInt("p2p_port")
	if !ok {
		return nil, errors.NewConfigurationError("p2p_port not set in config")
	}

	topicPrefix, ok := gocore.Config().Get("p2p_topic_prefix")
	if !ok {
		return nil, errors.NewConfigurationError("p2p_topic_prefix not set in config")
	}

	btn, ok := gocore.Config().Get("p2p_block_topic")
	if !ok {
		return nil, errors.NewConfigurationError("p2p_block_topic not set in config")
	}

	stn, ok := gocore.Config().Get("p2p_subtree_topic")
	if !ok {
		return nil, errors.NewConfigurationError("p2p_subtree_topic not set in config")
	}

	bbtn, ok := gocore.Config().Get("p2p_bestblock_topic")
	if !ok {
		return nil, errors.NewConfigurationError("p2p_bestblock_topic not set in config")
	}

	miningOntn, ok := gocore.Config().Get("p2p_mining_on_topic")
	if !ok {
		return nil, errors.NewConfigurationError("p2p_mining_on_topic not set in config")
	}

	rtn, ok := gocore.Config().Get("p2p_rejected_tx_topic")
	if !ok {
		return nil, errors.NewConfigurationError("p2p_rejected_tx_topic not set in config")
	}

	sharedKey, ok := gocore.Config().Get("p2p_shared_key")
	if !ok {
		return nil, errors.NewConfigurationError("error getting p2p_shared_key")
	}

	usePrivateDht := gocore.Config().GetBool("p2p_dht_use_private", false)
	optimiseRetries := gocore.Config().GetBool("p2p_optimise_retries", false)

	blockTopicName = fmt.Sprintf("%s-%s", topicPrefix, btn)
	subtreeTopicName = fmt.Sprintf("%s-%s", topicPrefix, stn)
	bestBlockTopicName = fmt.Sprintf("%s-%s", topicPrefix, bbtn)
	miningOnTopicName = fmt.Sprintf("%s-%s", topicPrefix, miningOntn)
	rejectedTxTopicName = fmt.Sprintf("%s-%s", topicPrefix, rtn)

	staticPeers, _ := gocore.Config().GetMulti("p2p_static_peers", "|")
	privateKey, _ := gocore.Config().Get("p2p_private_key")

	config := p2p.P2PConfig{
		ProcessName:     "peer",
		IP:              p2pIP,
		Port:            p2pPort,
		PrivateKey:      privateKey,
		SharedKey:       sharedKey,
		UsePrivateDHT:   usePrivateDht,
		OptimiseRetries: optimiseRetries,
		Advertise:       true,
		StaticPeers:     staticPeers,
	}

	p2pNode, err := p2p.NewP2PNode(logger, config)
	if err != nil {
		return nil, errors.NewServiceError("Error creating P2PNode", err)
	}

	p2pServer := &Server{
		P2PNode:           p2pNode,
		logger:            logger,
		bitcoinProtocolID: "ubsv/bitcoin/1.0.0",
		notificationCh:    make(chan *notificationMsg),
		blockchainClient:  blockchainClient,
	}

	return p2pServer, nil
}

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
	checks := []health.Check{
		{Name: "BlockchainClient", Check: s.blockchainClient.Health},
		{Name: "BlockValidationClient", Check: s.blockValidationClient.Health},
		{Name: "FSM", Check: blockchain.CheckFSM(s.blockchainClient)},
		{Name: "Kafka", Check: kafka.HealthChecker(ctx, s.kafkaHealthURL)},
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (s *Server) Init(ctx context.Context) (err error) {
	s.logger.Infof("P2P service initialising")

	AssetHTTPAddressURL, _, _ := gocore.Config().GetURL("asset_httpAddress")
	securityLevel, _ := gocore.Config().GetInt("securityLevelHTTP", 0)

	if AssetHTTPAddressURL.Scheme == "http" && securityLevel == 1 {
		AssetHTTPAddressURL.Scheme = "https"

		s.logger.Warnf("asset_httpAddress is HTTP but securityLevel is 1, changing to HTTPS")
	} else if AssetHTTPAddressURL.Scheme == "https" && securityLevel == 0 {
		AssetHTTPAddressURL.Scheme = "http"

		s.logger.Warnf("asset_httpAddress is HTTPS but securityLevel is 0, changing to HTTP")
	}

	s.AssetHTTPAddressURL = AssetHTTPAddressURL.String()

	subtreesKafkaURL, err, found := gocore.Config().GetURL("kafka_subtreesConfig")
	if err != nil {
		return errors.NewConfigurationError("[P2P] error getting kafka url", err)
	}

	if found {
		s.kafkaHealthURL = subtreesKafkaURL

		s.subtreeKafkaProducerClient, err = retry.Retry(ctx, s.logger, func() (*kafka.KafkaAsyncProducer, error) {
			return kafka.NewKafkaAsyncProducer(s.logger, subtreesKafkaURL, make(chan *kafka.Message, 10))
		}, retry.WithMessage("[P2P] error starting kafka subtree producer"))
		if err != nil {
			s.logger.Fatalf("[P2P] failed to start kafka subtree producer: %v", err)
			return
		}

		s.logger.Infof("[P2P] connected to kafka at %s", subtreesKafkaURL.Host)
	}

	blocksKafkaURL, err, found := gocore.Config().GetURL("kafka_blocksConfig")
	if err != nil {
		return errors.NewConfigurationError("[P2P] error getting kafka url", err)
	}

	if found {
		s.kafkaHealthURL = blocksKafkaURL

		s.blocksKafkaProducerClient, err = retry.Retry(ctx, s.logger, func() (*kafka.KafkaAsyncProducer, error) {
			return kafka.NewKafkaAsyncProducer(s.logger, blocksKafkaURL, make(chan *kafka.Message, 10))
		}, retry.WithMessage("[P2P] error starting kafka block producer"))
		if err != nil {
			s.logger.Fatalf("[P2P] failed to start kafka block producer: %v", err)
			return
		}

		s.logger.Infof("[P2P] connected to kafka at %s", blocksKafkaURL.Host)
	}

	rejectedTxKafkaURL, err, ok := gocore.Config().GetURL("kafka_rejectedTxConfig")
	if err == nil && ok {
		s.kafkaHealthURL = rejectedTxKafkaURL

		var partitions int

		if partitions, err = strconv.Atoi(rejectedTxKafkaURL.Query().Get("partitions")); err != nil {
			return errors.NewInvalidArgumentError("[Subtreevalidation] unable to parse Kafka partitions from %s", rejectedTxKafkaURL, err)
		}

		consumerRatio := util.GetQueryParamInt(rejectedTxKafkaURL, "consumer_ratio", 8)
		if consumerRatio < 1 {
			consumerRatio = 1
		}

		consumerCount := partitions / consumerRatio
		if consumerCount < 0 {
			consumerCount = 1
		}

		// Generate a unique group ID for the txmeta Kafka listener, to ensure that each instance of this service will process all txmeta messages.
		// This is necessary because the txmeta messages are used to populate the txmeta cache, which is shared across all instances of this service.
		groupID := "subtreevalidation-" + uuid.New().String()

		s.logger.Infof("Starting %d Kafka consumers for rejected tx messages", consumerCount)

		// For TxMeta, we are using autocommit, as we want to consume every message as fast as possible, and it is okay if some of the messages are not properly processed.
		// We don't need manual kafka commit and error handling here, as it is not necessary to retry the message, we have the message in stores.
		// Therefore, autocommit is set to true.
		rejectedTxHandler := func(msg kafka.KafkaMessage) error {
			hash, err := chainhash.NewHash(msg.Message.Value[:chainhash.HashSize])
			if err != nil {
				s.logger.Errorf("error getting chainhash from string %s: %v", msg.Message.Value[:chainhash.HashSize], err)
				return err
			}

			reason := string(msg.Message.Value[chainhash.HashSize:])

			s.logger.Debugf("P2P Received %s rejected tx notification: %s", hash.String(), reason)

			rejectedTxMessage := p2p.RejectedTxMessage{
				TxId:   hash.String(),
				Reason: reason,
				PeerId: s.P2PNode.HostID().String(),
			}

			msgBytes, err := json.Marshal(rejectedTxMessage)
			if err != nil {
				s.logger.Errorf("json marshal error: %v", err)

				return err
			}

			s.logger.Debugf("P2P publishing rejectedTxMessage")

			if err := s.P2PNode.Publish(ctx, rejectedTxTopicName, msgBytes); err != nil {
				s.logger.Errorf("publish error: %v", err)
			}

			return nil
		}
		s.rejectedTxKafkaConsumerClient, err = kafka.NewKafkaConsumeGroup(ctx, kafka.KafkaListenerConfig{
			Logger:            s.logger,
			URL:               rejectedTxKafkaURL,
			GroupID:           groupID,
			ConsumerCount:     consumerCount,
			AutoCommitEnabled: true,
			ConsumerFn:        rejectedTxHandler,
		})

		if err != nil {
			return errors.NewConfigurationError("failed to create new Kafka listener for %s: %v", rejectedTxKafkaURL.String(), err)
		}
	}

	return nil
}

func (s *Server) Start(ctx context.Context) error {
	s.logger.Infof("P2P service starting")

	go s.rejectedTxKafkaConsumerClient.Start(ctx)
	go s.subtreeKafkaProducerClient.Start(ctx)
	go s.blocksKafkaProducerClient.Start(ctx)

	var err error

	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	fsmStateRestore := gocore.Config().GetBool("fsm_state_restore", false)
	if fsmStateRestore {
		// Send Restore event to FSM
		if err = s.blockchainClient.Restore(ctx); err != nil {
			s.logger.Errorf("[p2p] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		s.logger.Infof("[p2p] Node is restoring, waiting for FSM to transition to Running state")
		_ = s.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain.FSMStateRUNNING)
		s.logger.Infof("[p2p] Node finished restoring and has transitioned to Running state, continuing to start p2p service")
	}

	s.blockValidationClient, err = blockvalidation.NewClient(ctx, s.logger, "p2p")
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
		err := s.StartHTTP(ctx)
		if err != nil {
			s.logger.Errorf("error starting http server: %s", err)
			return
		}
	}()

	err = s.P2PNode.Start(
		ctx,
		bestBlockTopicName,
		blockTopicName,
		subtreeTopicName,
		miningOnTopicName,
		rejectedTxTopicName,
	)
	if err != nil {
		return errors.NewServiceError("error starting p2p node: %w", err)
	}

	_ = s.P2PNode.SetTopicHandler(ctx, bestBlockTopicName, s.handleBestBlockTopic)
	_ = s.P2PNode.SetTopicHandler(ctx, blockTopicName, s.handleBlockTopic)
	_ = s.P2PNode.SetTopicHandler(ctx, subtreeTopicName, s.handleSubtreeTopic)
	_ = s.P2PNode.SetTopicHandler(ctx, miningOnTopicName, s.handleMiningOnTopic)

	go s.blockchainSubscriptionListener(ctx)

	s.sendBestBlockMessage(ctx)

	// this will block
	if err = util.StartGRPCServer(ctx, s.logger, "p2p", func(server *grpc.Server) {
		p2p_api.RegisterPeerServiceServer(server, s)
	}); err != nil {
		return errors.WrapGRPC(errors.NewServiceNotStartedError("[Legacy] can't start GRPC server", err))
	}

	<-ctx.Done()

	return nil
}

func (s *Server) sendBestBlockMessage(ctx context.Context) {
	msgBytes, err := json.Marshal(p2p.BestBlockMessage{PeerId: s.P2PNode.HostID().String()})
	if err != nil {
		s.logger.Errorf("json marshal error: %v", err)
	}

	if err := s.P2PNode.Publish(ctx, bestBlockTopicName, msgBytes); err != nil {
		s.logger.Errorf("publish error: %v", err)
	}
}

func (s *Server) blockchainSubscriptionListener(ctx context.Context) {
	// Subscribe to the blockchain service
	blockchainSubscription, err := s.blockchainClient.Subscribe(ctx, "p2pServer")
	if err != nil {
		s.logger.Errorf("error subscribing to blockchain service: %v", err)
		return
	}

	// define vars here to prevent too many allocs
	var (
		notification    *blockchain.Notification
		blockMessage    p2p.BlockMessage
		miningOnMessage p2p.MiningOnMessage
		subtreeMessage  p2p.SubtreeMessage
		header          *model.BlockHeader
		meta            *model.BlockHeaderMeta
		msgBytes        []byte
	)

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("P2P service shutting down")
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
				s.logger.Errorf("error getting chainhash from notification hash %s: %v", notification.Hash, err)
				continue
			}

			switch notification.Type {
			case model.NotificationType_Block:
				_, meta, err := s.blockchainClient.GetBlockHeader(ctx, hash)
				// // _, meta, err := s.blockchainClient.GetBestBlockHeader(ctx)
				// // // block, err := s.blockchainClient.GetBlock(ctx, notification.Hash)
				if err != nil {
					s.logger.Errorf("error getting block header and meta for BlockMessage: %v", err)
					continue
				}

				blockMessage = p2p.BlockMessage{
					Hash:       hash.String(),
					Height:     meta.Height,
					DataHubUrl: s.AssetHTTPAddressURL,
					PeerId:     s.P2PNode.HostID().String(),
				}

				msgBytes, err = json.Marshal(blockMessage)
				if err != nil {
					s.logger.Errorf("json mmarshal error: %v", err)
					continue
				}

				if err := s.P2PNode.Publish(ctx, blockTopicName, msgBytes); err != nil {
					s.logger.Errorf("publish error: %v", err)
				}
			case model.NotificationType_MiningOn:
				header, meta, err = s.blockchainClient.GetBestBlockHeader(ctx)
				if err != nil {
					s.logger.Errorf("error getting block header for MiningOnMessage: %v", err)
					continue
				}

				miningOnMessage = p2p.MiningOnMessage{
					Hash:         header.Hash().String(),
					PreviousHash: header.HashPrevBlock.String(),
					DataHubUrl:   s.AssetHTTPAddressURL,
					PeerId:       s.P2PNode.HostID().String(),
					Height:       meta.Height,
					Miner:        meta.Miner,
					SizeInBytes:  meta.SizeInBytes,
					TxCount:      meta.TxCount,
				}

				msgBytes, err = json.Marshal(miningOnMessage)
				if err != nil {
					s.logger.Errorf("json marshal error: %v", err)
					continue
				}

				s.logger.Debugf("P2P publishing miningOnMessage")

				if err := s.P2PNode.Publish(ctx, miningOnTopicName, msgBytes); err != nil {
					s.logger.Errorf("publish error: %v", err)
				}
			case model.NotificationType_Subtree:
				// if it's a subtree notification send it on the subtree channel.
				subtreeMessage = p2p.SubtreeMessage{
					Hash:       hash.String(),
					DataHubUrl: s.AssetHTTPAddressURL,
					PeerId:     s.P2PNode.HostID().String(),
				}

				msgBytes, err = json.Marshal(subtreeMessage)
				if err != nil {
					s.logger.Errorf("json marshal error: %v", err)

					continue
				}

				if err := s.P2PNode.Publish(ctx, subtreeTopicName, msgBytes); err != nil {
					s.logger.Errorf("publish error: %v", err)
				}
			}
		}
	}
}

func (s *Server) StartHTTP(ctx context.Context) error {
	addr, _ := gocore.Config().Get("p2p_httpListenAddress")
	securityLevel, _ := gocore.Config().GetInt("securityLevelHTTP", 0)

	s.logger.Infof("p2p service listening on %s", addr)

	go func() {
		<-ctx.Done()
		s.logger.Infof("[p2p] service shutting down")

		if err := s.e.Shutdown(ctx); err != nil {
			s.logger.Errorf("[p2p] service shutdown error: %v", err)
		}
	}()

	// err := h.e.Start(addr)
	// if err != nil && !errors.Is(err, http.ErrServerClosed) {
	// 	return err
	// }

	var err error

	if securityLevel == 0 {
		servicemanager.AddListenerInfo(fmt.Sprintf("p2p HTTP listening on %s", addr))
		err = s.e.Start(addr)
	} else {
		certFile, found := gocore.Config().Get("server_certFile")
		if !found {
			return errors.NewConfigurationError("server_certFile is required for HTTPS")
		}

		keyFile, found := gocore.Config().Get("server_keyFile")
		if !found {
			return errors.NewConfigurationError("server_keyFile is required for HTTPS")
		}

		servicemanager.AddListenerInfo(fmt.Sprintf("p2p HTTPS listening on %s", addr))
		err = s.e.StartTLS(addr, certFile, keyFile)
	}

	if err != http.ErrServerClosed {
		return err
	}

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.logger.Infof("Stopping P2P service")
	return s.e.Shutdown(ctx)
}

func (s *Server) handleBestBlockTopic(ctx context.Context, m []byte, from string) {
	var (
		bestBlockMessage p2p.BestBlockMessage
		pid              peer.ID
		bh               *model.BlockHeader
		bhMeta           *model.BlockHeaderMeta
		blockMessage     p2p.BlockMessage
		msgBytes         []byte
	)

	if from == s.P2PNode.HostID().String() {
		return
	}

	// decode request
	bestBlockMessage = p2p.BestBlockMessage{}

	err := json.Unmarshal(m, &bestBlockMessage)
	if err != nil {
		s.logger.Errorf("json unmarshal error: %v", err)
		return
	}

	pid, err = peer.Decode(bestBlockMessage.PeerId)
	if err != nil {
		s.logger.Errorf("error decoding peerId: %v", err)
		return
	}

	s.logger.Debugf("got p2p best block notification from %s", bestBlockMessage.PeerId)

	// get best block from blockchain service
	bh, bhMeta, err = s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		s.logger.Errorf("error getting best block header: %v", err)
		return
	}

	if bh == nil {
		s.logger.Errorf("error getting best block header: %v", err)
		return
	}

	blockMessage = p2p.BlockMessage{
		Hash:       bh.Hash().String(),
		Height:     bhMeta.Height,
		DataHubUrl: s.AssetHTTPAddressURL,
	}

	msgBytes, err = json.Marshal(blockMessage)
	if err != nil {
		s.logger.Errorf("json marshal error: %v", err)
		return
	}

	// send best block to the requester
	err = s.P2PNode.SendToPeer(ctx, pid, msgBytes)
	if err != nil {
		s.logger.Errorf("error sending peer message: %v", err)
	}
}

func (s *Server) handleBlockTopic(ctx context.Context, m []byte, from string) {
	s.logger.Debugf("handleBlockTopic")

	var (
		blockMessage p2p.BlockMessage
		hash         *chainhash.Hash
		err          error
	)

	// decode request
	blockMessage = p2p.BlockMessage{}

	err = json.Unmarshal(m, &blockMessage)
	if err != nil {
		s.logger.Errorf("json unmarshal error: %v", err)
		return
	}

	s.logger.Debugf("got p2p block notification for %s from %s", blockMessage.Hash, blockMessage.PeerId)

	s.notificationCh <- &notificationMsg{
		Timestamp: time.Now().UTC().Format(isoFormat),
		Type:      "block",
		Hash:      blockMessage.Hash,
		Height:    blockMessage.Height,
		BaseURL:   blockMessage.DataHubUrl,
		PeerId:    blockMessage.PeerId,
	}

	if from == s.P2PNode.HostID().String() {
		return
	}

	hash, err = chainhash.NewHashFromStr(blockMessage.Hash)
	if err != nil {
		s.logger.Errorf("error getting chainhash from string %s: %v", blockMessage.Hash, err)
		return
	}

	// send block to kafka, if configured
	if s.blocksKafkaProducerClient != nil {
		value := make([]byte, 0, chainhash.HashSize+len(blockMessage.DataHubUrl))
		value = append(value, hash.CloneBytes()...)
		value = append(value, []byte(blockMessage.DataHubUrl)...)
		s.blocksKafkaProducerClient.PublishChannel <- &kafka.Message{
			Value: value,
		}
	}
}

func (s *Server) handleSubtreeTopic(ctx context.Context, m []byte, from string) {
	var (
		subtreeMessage p2p.SubtreeMessage
		hash           *chainhash.Hash
		err            error
	)

	// decode request
	subtreeMessage = p2p.SubtreeMessage{}

	err = json.Unmarshal(m, &subtreeMessage)
	if err != nil {
		s.logger.Errorf("json unmarshal error: %v", err)
		return
	}

	s.logger.Debugf("got p2p subtree notification for %s from %s", subtreeMessage.Hash, subtreeMessage.PeerId)

	s.notificationCh <- &notificationMsg{
		Timestamp: time.Now().UTC().Format(isoFormat),
		Type:      "subtree",
		Hash:      subtreeMessage.Hash,
		BaseURL:   subtreeMessage.DataHubUrl,
		PeerId:    subtreeMessage.PeerId,
	}

	if from == s.P2PNode.HostID().String() {
		return
	}

	hash, err = chainhash.NewHashFromStr(subtreeMessage.Hash)
	if err != nil {
		s.logger.Errorf("error getting chainhash from string %s: %v", subtreeMessage.Hash, err)
		return
	}

	if s.subtreeKafkaProducerClient != nil {
		value := make([]byte, 0, chainhash.HashSize+len(subtreeMessage.DataHubUrl))
		value = append(value, hash.CloneBytes()...)
		value = append(value, []byte(subtreeMessage.DataHubUrl)...)
		s.subtreeKafkaProducerClient.PublishChannel <- &kafka.Message{
			Value: value,
		}
	} else {
		if err = s.blockValidationClient.SubtreeFound(ctx, hash, subtreeMessage.DataHubUrl); err != nil {
			s.logger.Errorf("[p2p] error validating subtree from %s: %v", subtreeMessage.DataHubUrl, err)
		}
	}
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
		s.logger.Errorf("json unmarshal error: %v", err)
		return
	}

	s.logger.Debugf("got p2p mining on notification for %s from %s", miningOnMessage.Hash, miningOnMessage.PeerId)

	s.notificationCh <- &notificationMsg{
		Timestamp:    time.Now().UTC().Format(isoFormat),
		Type:         "mining_on",
		Hash:         miningOnMessage.Hash,
		BaseURL:      miningOnMessage.DataHubUrl,
		PeerId:       miningOnMessage.PeerId,
		PreviousHash: miningOnMessage.PreviousHash,
		Height:       miningOnMessage.Height,
		Miner:        miningOnMessage.Miner,
		SizeInBytes:  miningOnMessage.SizeInBytes,
		TxCount:      miningOnMessage.TxCount,
	}
}

func (s *Server) GetPeers(ctx context.Context, _ *emptypb.Empty) (*p2p_api.GetPeersResponse, error) {
	s.logger.Debugf("GetPeers called")

	if s.P2PNode == nil {
		return nil, errors.NewError("P2PNode is not initialised")
	}

	s.logger.Debugf("Creating reply channel")
	serverPeers := s.P2PNode.ConnectedPeers()

	resp := &p2p_api.GetPeersResponse{}

	for _, sp := range serverPeers {
		// s.logger.Debugf("peer: %v", sp.ID)
		// s.logger.Debugf("peer: %v", sp.Addrs)
		if sp.ID == s.P2PNode.HostID() {
			continue
		}

		if len(sp.Addrs) == 0 {
			continue
		}

		resp.Peers = append(resp.Peers, &p2p_api.Peer{
			Addr: sp.Addrs[0].String(),
		})
	}

	return resp, nil
}
