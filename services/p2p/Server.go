package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/bitcoin-sv/ubsv/util/p2p"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libsv/go-bt/v2/chainhash"

	"github.com/ordishs/gocore"
)

var (
	blockTopicName      string
	bestBlockTopicName  string
	subtreeTopicName    string
	miningOnTopicName   string
	rejectedTxTopicName string
)

type Server struct {
	P2PNode               *p2p.P2PNode
	logger                ulogger.Logger
	bitcoinProtocolId     string
	blockchainClient      blockchain.ClientI
	blockValidationClient *blockvalidation.Client
	validatorClient       *validator.Client
	AssetHttpAddressURL   string
	e                     *echo.Echo
	notificationCh        chan *notificationMsg
}

type BestBlockMessage struct {
	PeerId string
}

type MiningOnMessage struct {
	Hash         string
	PreviousHash string
	DataHubUrl   string
	PeerId       string
	Height       uint32
	Miner        string
	SizeInBytes  uint64
	TxCount      uint64
}

type BlockMessage struct {
	Hash       string
	Height     uint32
	DataHubUrl string
	PeerId     string
}
type SubtreeMessage struct {
	Hash       string
	DataHubUrl string
	PeerId     string
}
type RejectedTxMessage struct {
	TxId   string
	Reason string
	PeerId string
}

func NewServer(logger ulogger.Logger) *Server {
	logger.Debugf("Creating P2P service")

	p2pIp, ok := gocore.Config().Get("p2p_ip")
	if !ok {
		panic("p2p_ip not set in config")
	}
	p2pPort, ok := gocore.Config().GetInt("p2p_port")
	if !ok {
		panic("p2p_port not set in config")
	}

	topicPrefix, ok := gocore.Config().Get("p2p_topic_prefix")
	if !ok {
		panic("p2p_topic_prefix not set in config")
	}
	btn, ok := gocore.Config().Get("p2p_block_topic")
	if !ok {
		panic("p2p_block_topic not set in config")
	}
	stn, ok := gocore.Config().Get("p2p_subtree_topic")
	if !ok {
		panic("p2p_subtree_topic not set in config")
	}
	bbtn, ok := gocore.Config().Get("p2p_bestblock_topic")
	if !ok {
		panic("p2p_bestblock_topic not set in config")
	}

	miningOntn, ok := gocore.Config().Get("p2p_mining_on_topic")
	if !ok {
		panic("p2p_mining_on_topic not set in config")
	}
	rtn, ok := gocore.Config().Get("p2p_rejected_tx_topic")
	if !ok {
		panic("p2p_rejected_tx_topic not set in config")
	}

	sharedKey, ok := gocore.Config().Get("p2p_shared_key")
	if !ok {
		panic(fmt.Errorf("error getting p2p_shared_key"))
	}
	usePrivateDht := gocore.Config().GetBool("p2p_dht_use_private", false)
	optimiseRetries := gocore.Config().GetBool("p2p_optimise_retries", false)

	blockTopicName = fmt.Sprintf("%s-%s", topicPrefix, btn)
	subtreeTopicName = fmt.Sprintf("%s-%s", topicPrefix, stn)
	bestBlockTopicName = fmt.Sprintf("%s-%s", topicPrefix, bbtn)
	miningOnTopicName = fmt.Sprintf("%s-%s", topicPrefix, miningOntn)
	rejectedTxTopicName = fmt.Sprintf("%s-%s", topicPrefix, rtn)

	config := p2p.P2PConfig{
		ProcessName:     "peer",
		IP:              p2pIp,
		Port:            p2pPort,
		SharedKey:       sharedKey,
		UsePrivateDHT:   usePrivateDht,
		OptimiseRetries: optimiseRetries,
		Advertise:       true,
	}

	p2pNode := p2p.NewP2PNode(logger, config)

	return &Server{
		P2PNode:           p2pNode,
		logger:            logger,
		bitcoinProtocolId: "ubsv/bitcoin/1.0.0",
		notificationCh:    make(chan *notificationMsg),
	}
}

func (s *Server) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (s *Server) Init(ctx context.Context) (err error) {
	s.logger.Infof("P2P service initialising")

	s.blockchainClient, err = blockchain.NewClient(ctx, s.logger)
	if err != nil {
		return fmt.Errorf("could not create blockchain client [%w]", err)
	}

	AssetHttpAddressURL, _, _ := gocore.Config().GetURL("asset_httpAddress")
	securityLevel, _ := gocore.Config().GetInt("securityLevelHTTP", 0)

	if AssetHttpAddressURL.Scheme == "http" && securityLevel == 1 {
		AssetHttpAddressURL.Scheme = "https"
		s.logger.Warnf("asset_httpAddress is HTTP but securityLevel is 1, changing to HTTPS")
	} else if AssetHttpAddressURL.Scheme == "https" && securityLevel == 0 {
		AssetHttpAddressURL.Scheme = "http"
		s.logger.Warnf("asset_httpAddress is HTTPS but securityLevel is 0, changing to HTTP")
	}
	s.AssetHttpAddressURL = AssetHttpAddressURL.String()

	s.blockValidationClient = blockvalidation.NewClient(ctx, s.logger)

	s.validatorClient, err = validator.NewClient(ctx, s.logger)
	if err != nil {
		return fmt.Errorf("could not create validator client [%w]", err)
	}
	return nil
}

func (s *Server) Start(ctx context.Context) error {
	s.logger.Infof("P2P service starting")
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

	e.GET("/p2p-ws", s.HandleWebSocket(s.notificationCh))

	go func() {
		err := s.StartHttp(ctx)
		if err != nil {
			s.logger.Errorf("error starting http server: %s", err)
			return
		}
	}()

	err := s.P2PNode.Start(
		ctx,
		bestBlockTopicName,
		blockTopicName,
		subtreeTopicName,
		miningOnTopicName,
		rejectedTxTopicName,
	)
	if err != nil {
		return fmt.Errorf("error starting p2p node: %w", err)
	}

	_ = s.P2PNode.SetTopicHandler(ctx, bestBlockTopicName, s.handleBestBlockTopic)
	_ = s.P2PNode.SetTopicHandler(ctx, blockTopicName, s.handleBlockTopic)
	_ = s.P2PNode.SetTopicHandler(ctx, subtreeTopicName, s.handleSubtreeTopic)
	_ = s.P2PNode.SetTopicHandler(ctx, miningOnTopicName, s.handleMiningOnTopic)

	go s.blockchainSubscriptionListener(ctx)
	go s.validatorSubscriptionListener(ctx)

	s.sendBestBlockMessage(ctx)

	<-ctx.Done()

	return nil
}

func (s *Server) sendBestBlockMessage(ctx context.Context) {
	msgBytes, err := json.Marshal(BestBlockMessage{PeerId: s.P2PNode.HostID().String()})
	if err != nil {
		s.logger.Errorf("json marshal error: ", err)
	}
	if err = s.P2PNode.Publish(ctx, bestBlockTopicName, msgBytes); err != nil {
		s.logger.Errorf("publish error:", err)
	}
}

func (s *Server) blockchainSubscriptionListener(ctx context.Context) {
	// Subscribe to the blockchain service
	blockchainSubscription, err := s.blockchainClient.Subscribe(ctx, "p2pServer")
	if err != nil {
		s.logger.Errorf("error subscribing to blockchain service: ", err)
		return
	}

	// define vars here to prevent too many allocs
	var notification *model.Notification
	var blockMessage BlockMessage
	var miningOnMessage MiningOnMessage
	var subtreeMessage SubtreeMessage
	var header *model.BlockHeader
	var meta *model.BlockHeaderMeta
	var msgBytes []byte

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
			s.logger.Debugf("P2P Received %s notification: %s", notification.Type, notification.Hash.String())

			if notification.Type == model.NotificationType_Block {
				// if it's a block notification send it on the block channel.
				blockMessage = BlockMessage{
					Hash:       notification.Hash.String(),
					DataHubUrl: s.AssetHttpAddressURL,
					PeerId:     s.P2PNode.HostID().String(),
				}

				msgBytes, err = json.Marshal(blockMessage)
				if err != nil {
					s.logger.Errorf("json mmarshal error: ", err)
					continue
				}
				if err = s.P2PNode.Publish(ctx, blockTopicName, msgBytes); err != nil {
					s.logger.Errorf("publish error:", err)
				}

			} else if notification.Type == model.NotificationType_MiningOn {
				header, meta, err = s.blockchainClient.GetBestBlockHeader(ctx)
				if err != nil {
					s.logger.Errorf("error getting block header for MiningOnMessage: ", err)
					continue
				}

				miningOnMessage = MiningOnMessage{
					Hash:         header.Hash().String(),
					PreviousHash: header.HashPrevBlock.String(),
					DataHubUrl:   s.AssetHttpAddressURL,
					PeerId:       s.P2PNode.HostID().String(),
					Height:       meta.Height,
					Miner:        meta.Miner,
					SizeInBytes:  meta.SizeInBytes,
					TxCount:      meta.TxCount,
				}
				msgBytes, err = json.Marshal(miningOnMessage)
				if err != nil {
					s.logger.Errorf("json marshal error: ", err)
					continue
				}
				s.logger.Debugf("P2P publishing miningOnMessage")
				if err = s.P2PNode.Publish(ctx, miningOnTopicName, msgBytes); err != nil {
					s.logger.Errorf("publish error:", err)
				}

			} else if notification.Type == model.NotificationType_Subtree {
				// if it's a subtree notification send it on the subtree channel.
				subtreeMessage = SubtreeMessage{
					Hash:       notification.Hash.String(),
					DataHubUrl: s.AssetHttpAddressURL,
					PeerId:     s.P2PNode.HostID().String(),
				}
				msgBytes, err = json.Marshal(subtreeMessage)
				if err != nil {
					s.logger.Errorf("json marshal error: ", err)
					continue
				}
				if err = s.P2PNode.Publish(ctx, subtreeTopicName, msgBytes); err != nil {
					s.logger.Errorf("publish error:", err)
				}
			}
		}
	}
}

func (s *Server) validatorSubscriptionListener(ctx context.Context) {
	s.logger.Debugf("validatorSubscriptionListener")
	// Subscribe to the validator service
	validatorSubscription, err := s.validatorClient.Subscribe(ctx, "p2pServer")
	if err != nil {
		s.logger.Errorf("error subscribing to validator service: ", err)
		return
	}
	// define vars here to prevent too many allocs
	var rejectedTxNotification *model.RejectedTxNotification
	var rejectedTxMessage RejectedTxMessage
	var msgBytes []byte

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("P2P service shutting down")
			return
		case rejectedTxNotification = <-validatorSubscription:
			if rejectedTxNotification == nil {
				s.logger.Debugf("P2P Received nil rejected tx notification")
				continue
			}
			// received a message
			s.logger.Debugf("P2P Received %s rejected tx notification: %s", rejectedTxNotification.TxId, rejectedTxNotification.Reason)

			rejectedTxMessage = RejectedTxMessage{
				TxId:   rejectedTxNotification.TxId,
				Reason: rejectedTxNotification.Reason,
				PeerId: s.P2PNode.HostID().String(),
			}
			msgBytes, err = json.Marshal(rejectedTxMessage)
			if err != nil {
				s.logger.Errorf("json marshal error: ", err)
				continue
			}
			s.logger.Debugf("P2P publishing rejectedTxMessage")
			if err = s.P2PNode.Publish(ctx, rejectedTxTopicName, msgBytes); err != nil {
				s.logger.Errorf("publish error:", err)
			}
		}
	}

}

func (s *Server) StartHttp(ctx context.Context) error {
	addr, _ := gocore.Config().Get("p2p_httpListenAddress")
	securityLevel, _ := gocore.Config().GetInt("securityLevelHTTP", 0)

	s.logger.Infof("p2p service listening on %s", addr)

	go func() {
		<-ctx.Done()
		s.logger.Infof("[p2p] service shutting down")
		err := s.e.Shutdown(ctx)
		if err != nil {
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
			return errors.New("server_certFile is required for HTTPS")
		}
		keyFile, found := gocore.Config().Get("server_keyFile")
		if !found {
			return errors.New("server_keyFile is required for HTTPS")
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
	var bestBlockMessage BestBlockMessage
	var pid peer.ID
	var bh *model.BlockHeader
	var bhMeta *model.BlockHeaderMeta
	var blockMessage BlockMessage
	var msgBytes []byte

	// decode request
	bestBlockMessage = BestBlockMessage{}
	err := json.Unmarshal(m, &bestBlockMessage)
	if err != nil {
		s.logger.Errorf("json unmarshal error: ", err)
		return
	}
	pid, err = peer.Decode(bestBlockMessage.PeerId)
	if err != nil {
		s.logger.Errorf("error decoding peerId: ", err)
		return
	}

	// get best block from blockchain service
	bh, bhMeta, err = s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		s.logger.Errorf("error getting best block header: ", err)
		return
	}
	if bh == nil {
		s.logger.Errorf("error getting best block header: ", err)
		return
	}

	blockMessage = BlockMessage{
		Hash:       bh.Hash().String(),
		Height:     bhMeta.Height,
		DataHubUrl: s.AssetHttpAddressURL,
	}

	msgBytes, err = json.Marshal(blockMessage)
	if err != nil {
		s.logger.Errorf("json marshal error: ", err)
		return
	}

	// send best block to the requester
	err = s.P2PNode.SendToPeer(ctx, pid, msgBytes)
	if err != nil {
		s.logger.Errorf("error sending peer message: ", err)
	}
}

func (s *Server) handleBlockTopic(ctx context.Context, m []byte, from string) {
	s.logger.Debugf("handleBlockTopic")

	var blockMessage BlockMessage
	var hash *chainhash.Hash
	var err error

	// decode request
	blockMessage = BlockMessage{}
	err = json.Unmarshal(m, &blockMessage)
	if err != nil {
		s.logger.Errorf("json unmarshal error: ", err)
		return
	}

	s.notificationCh <- &notificationMsg{
		Timestamp: time.Now().UTC().Format(isoFormat),
		Type:      "block",
		Hash:      blockMessage.Hash,
		BaseURL:   blockMessage.DataHubUrl,
		PeerId:    blockMessage.PeerId,
	}

	hash, err = chainhash.NewHashFromStr(blockMessage.Hash)
	if err != nil {
		s.logger.Errorf("error getting chainhash from string %s", blockMessage.Hash, err)
		return
	}
	if err = s.blockValidationClient.BlockFound(ctx, hash, blockMessage.DataHubUrl); err != nil {
		s.logger.Errorf("[p2p] error validating block from %s: %s", blockMessage.DataHubUrl, err)
	}
}

func (s *Server) handleSubtreeTopic(ctx context.Context, m []byte, from string) {
	var subtreeMessage SubtreeMessage
	var hash *chainhash.Hash
	var err error

	// decode request
	subtreeMessage = SubtreeMessage{}
	err = json.Unmarshal(m, &subtreeMessage)
	if err != nil {
		s.logger.Errorf("json unmarshal error: ", err)
		return
	}

	s.notificationCh <- &notificationMsg{
		Timestamp: time.Now().UTC().Format(isoFormat),
		Type:      "subtree",
		Hash:      subtreeMessage.Hash,
		BaseURL:   subtreeMessage.DataHubUrl,
		PeerId:    subtreeMessage.PeerId,
	}

	hash, err = chainhash.NewHashFromStr(subtreeMessage.Hash)
	if err != nil {
		s.logger.Errorf("error getting chainhash from string %s", subtreeMessage.Hash, err)
		return
	}
	if err = s.blockValidationClient.SubtreeFound(ctx, hash, subtreeMessage.DataHubUrl); err != nil {
		s.logger.Errorf("[p2p] error validating subtree from %s: %s", subtreeMessage.DataHubUrl, err)
	}
}

func (s *Server) handleMiningOnTopic(ctx context.Context, m []byte, from string) {
	var miningOnMessage MiningOnMessage
	var err error

	// decode request
	miningOnMessage = MiningOnMessage{}
	err = json.Unmarshal(m, &miningOnMessage)
	if err != nil {
		s.logger.Errorf("json unmarshal error: ", err)
		return
	}

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
