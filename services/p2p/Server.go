package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/multiformats/go-multiaddr"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/protocol"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"

	"github.com/ordishs/gocore"
)

var privateKeyFilename = fmt.Sprintf("peer.%s.p2p.private_key", gocore.Config().GetContext())

var (
	topicPrefix         string
	blockTopicName      string
	bestBlockTopicName  string
	subtreeTopicName    string
	miningOnTopicName   string
	rejectedTxTopicName string
)

type Server struct {
	host              host.Host
	topics            map[string]*pubsub.Topic
	subscriptions     map[string]*pubsub.Subscription
	logger            ulogger.Logger
	bitcoinProtocolId string

	blockchainClient      blockchain.ClientI
	blockValidationClient *blockvalidation.Client
	validatorClient       *validator.Client

	AssetHttpAddressURL string
	e                   *echo.Echo
	notificationCh      chan *notificationMsg
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

var usePrivateDht bool = false
var dhtProtocolIdStr string

func NewServer(logger ulogger.Logger) *Server {
	logger.Debugf("Creating P2P service")
	var pk *crypto.PrivKey
	var err error

	pk, err = readPrivateKey()
	if err != nil {
		pk, err = generatePrivateKey()
		if err != nil {
			panic(err)
		}
	}
	p2pIp, ok := gocore.Config().Get("p2p_ip")
	if !ok {
		panic("p2p_ip not set in config")
	}
	p2pPort, ok := gocore.Config().GetInt("p2p_port")
	if !ok {
		panic("p2p_port not set in config")
	}

	topicPrefix, ok = gocore.Config().Get("p2p_topic_prefix")
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

	dhtProtocolIdStr, ok = gocore.Config().Get("p2p_dht_protocol_id")
	if !ok {
		panic(fmt.Errorf("error getting p2p_dht_protocol_id"))
	}
	sharedKey, ok := gocore.Config().Get("p2p_shared_key")
	if !ok {
		panic(fmt.Errorf("error getting p2p_shared_key"))
	}
	usePrivateDht = gocore.Config().GetBool("p2p_dht_use_private", false)

	blockTopicName = fmt.Sprintf("%s-%s", topicPrefix, btn)
	subtreeTopicName = fmt.Sprintf("%s-%s", topicPrefix, stn)
	bestBlockTopicName = fmt.Sprintf("%s-%s", topicPrefix, bbtn)
	miningOnTopicName = fmt.Sprintf("%s-%s", topicPrefix, miningOntn)
	rejectedTxTopicName = fmt.Sprintf("%s-%s", topicPrefix, rtn)
	var h host.Host
	if usePrivateDht {
		s := ""
		s += fmt.Sprintln("/key/swarm/psk/1.0.0/")
		s += fmt.Sprintln("/base16/")
		s += sharedKey

		psk, err := pnet.DecodeV1PSK(bytes.NewBuffer([]byte(s)))
		if err != nil {
			panic(err)
		}
		h, err = libp2p.New(
			libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%d", p2pIp, p2pPort)),
			libp2p.Identity(*pk),
			libp2p.PrivateNetwork(psk),
		)
		if err != nil {
			panic(err)
		}
	} else {
		h, err = libp2p.New(libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%d", p2pIp, p2pPort)), libp2p.Identity(*pk))
		if err != nil {
			panic(err)
		}
	}
	logger.Infof("peer ID: %s", h.ID().Pretty())
	logger.Infof("Connect to me on:")
	for _, addr := range h.Addrs() {
		logger.Infof("  %s/p2p/%s", addr, h.ID().Pretty())
	}

	return &Server{
		logger:            logger,
		host:              h,
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

	e.GET("/ws", s.HandleWebSocket(s.notificationCh))

	go func() {
		err := s.StartHttp(ctx)
		if err != nil {
			s.logger.Errorf("error starting http server: %s", err)
			return
		}
	}()

	topicNames := []string{bestBlockTopicName, blockTopicName, subtreeTopicName, miningOnTopicName, rejectedTxTopicName}
	go s.discoverPeers(ctx, topicNames, usePrivateDht)

	ps, err := pubsub.NewGossipSub(ctx, s.host)
	if err != nil {
		return err
	}
	topics := map[string]*pubsub.Topic{}
	subscriptions := map[string]*pubsub.Subscription{}

	var topic *pubsub.Topic
	var sub *pubsub.Subscription
	for _, topicName := range topicNames {
		topic, err = ps.Join(topicName)
		if err != nil {
			return err
		}
		topics[topicName] = topic
		// don't subscribe to rejectedTxTopicName
		// TODO: subscribe topic list?
		if topicName != rejectedTxTopicName {
			sub, err = topic.Subscribe()
			if err != nil {
				return err
			}
			subscriptions[topicName] = sub
		}
	}
	s.topics = topics
	s.subscriptions = subscriptions

	s.host.SetStreamHandler(protocol.ID(s.bitcoinProtocolId), s.handleBlockchainMessage)

	go s.handleBestBlockTopic(ctx)
	go s.handleBlockTopic(ctx)
	go s.handleSubtreeTopic(ctx)
	go s.handleMiningOnTopic(ctx)
	go s.blockchainSubscriptionListener(ctx)
	go s.validatorSubscriptionListener(ctx)

	s.sendBestBlockMessage(ctx)

	<-ctx.Done()

	return nil
}

func (s *Server) sendBestBlockMessage(ctx context.Context) {
	msgBytes, err := json.Marshal(BestBlockMessage{PeerId: s.host.ID().String()})
	if err != nil {
		s.logger.Errorf("json marshal error: ", err)
	}
	// send  bestblock msg on topic
	if err = s.topics[bestBlockTopicName].Publish(ctx, msgBytes); err != nil {
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
					PeerId:     s.host.ID().String(),
				}

				msgBytes, err = json.Marshal(blockMessage)
				if err != nil {
					s.logger.Errorf("json mmarshal error: ", err)
					continue
				}
				if err = s.topics[blockTopicName].Publish(ctx, msgBytes); err != nil {
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
					PeerId:       s.host.ID().String(),
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
				if err = s.topics[miningOnTopicName].Publish(ctx, msgBytes); err != nil {
					s.logger.Errorf("publish error:", err)
				}

			} else if notification.Type == model.NotificationType_Subtree {
				// if it's a subtree notification send it on the subtree channel.
				subtreeMessage = SubtreeMessage{
					Hash:       notification.Hash.String(),
					DataHubUrl: s.AssetHttpAddressURL,
					PeerId:     s.host.ID().String(),
				}
				msgBytes, err = json.Marshal(subtreeMessage)
				if err != nil {
					s.logger.Errorf("json marshal error: ", err)
					continue
				}
				if err = s.topics[subtreeTopicName].Publish(ctx, msgBytes); err != nil {
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
				PeerId: s.host.ID().String(),
			}
			msgBytes, err = json.Marshal(rejectedTxMessage)
			if err != nil {
				s.logger.Errorf("json marshal error: ", err)
				continue
			}
			s.logger.Debugf("P2P publishing rejectedTxMessage")
			if err = s.topics[rejectedTxTopicName].Publish(ctx, msgBytes); err != nil {
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

func generatePrivateKey() (*crypto.PrivKey, error) {
	// Generate a new key pair
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}

	// Convert private key to bytes
	privBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}

	// Save private key to a file
	err = os.WriteFile(privateKeyFilename, privBytes, 0644)
	if err != nil {
		return nil, err
	}

	return &priv, nil
}

func readPrivateKey() (*crypto.PrivKey, error) {
	// Read private key from a file
	privBytes, err := os.ReadFile(privateKeyFilename)
	if err != nil {
		return nil, err
	}

	// Unmarshal the private key bytes into a key
	priv, err := crypto.UnmarshalPrivateKey(privBytes)
	if err != nil {
		return nil, err
	}

	return &priv, nil
}

func (s *Server) discoverPeers(ctx context.Context, tn []string, usePrivateDht bool) {
	var kademliaDHT *dht.IpfsDHT
	var err error

	if usePrivateDht {

		bootstrapAddresses, _ := gocore.Config().GetMulti("p2p_bootstrapAddresses", "|")
		if len(bootstrapAddresses) == 0 {
			panic(fmt.Errorf("bootstrapAddresses not set in config"))
		}
		for _, ba := range bootstrapAddresses {
			bootstrapAddr, err := multiaddr.NewMultiaddr(ba)
			if err != nil {
				panic(fmt.Sprintf("failed to create bootstrap multiaddress %s: %v", ba, err))
			}

			peerInfo, err := peer.AddrInfoFromP2pAddr(bootstrapAddr)
			if err != nil {
				panic(fmt.Sprintf("failed to get peerInfo from  %s: %v", ba, err))
			}

			// Connect to the bootstrap node.
			err = s.host.Connect(ctx, *peerInfo)
			if err != nil {
				panic(fmt.Sprintf("failed to connect to bootstrap address %s: %v", ba, err))
			}
		}
		dhtProtocolID := protocol.ID(dhtProtocolIdStr)
		var options []dht.Option
		options = append(options, dht.ProtocolPrefix(dhtProtocolID))
		options = append(options, dht.Mode(dht.ModeAuto))

		// initialise the DHT
		kademliaDHT, err = dht.New(ctx, s.host, options...)
		if err != nil {
			panic(err)
		}

		err = kademliaDHT.Bootstrap(ctx)
		if err != nil {
			panic(err)
		}

	} else {
		kademliaDHT = InitDHT(ctx, s.host)
	}
	routingDiscovery := drouting.NewRoutingDiscovery(kademliaDHT)
	for _, topicName := range tn {
		dutil.Advertise(ctx, routingDiscovery, topicName)
	}

	// Look for others who have announced and attempt to connect to them
	anyConnected := false
ConnectLoop:
	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("P2P service shutting down")
			return
		default:
			if !anyConnected {
				s.logger.Debugf("Searching for peers for topics %d", len(tn))
				for _, topicName := range tn {
					s.logger.Debugf("Searching for peers for topic %s..", topicName)

					peerChan, err := routingDiscovery.FindPeers(ctx, topicName)
					if err != nil {
						s.logger.Errorf("error finding peers: %+v", err)
					}

					for p := range peerChan {
						if p.ID == s.host.ID() {
							continue // No self connection
						}
						err = s.host.Connect(ctx, p)
						if err != nil {
							//  we fail to connect to a lot of peers. Just ignore it for now.
							// s.logger.Debugf("Failed connecting to ", peer.ID.Pretty(), ", error:", err)
						} else {
							s.logger.Infof("Connected to:", p.String())
							anyConnected = true
						}
					}
				}
				time.Sleep(5 * time.Second)
			} else {
				s.logger.Debugf("Peer discovery complete")
				s.logger.Debugf("connected to %d peers\n", len(s.host.Network().Peers()))
				s.logger.Debugf("peerstore has %d peers\n", len(s.host.Peerstore().Peers()))
				break ConnectLoop
			}
		}
	}
}

func (s *Server) handleBlockchainMessage(ns network.Stream) {
	buf, err := io.ReadAll(ns)
	if err != nil {
		_ = ns.Reset()
		s.logger.Errorf("failed to read network stream: %+v              ", err.Error())
		return
	}
	_ = ns.Close()
	if len(buf) > 0 {
		s.logger.Debugf("Received block topic message: %s", string(buf))
	}
}

func (s *Server) handleBestBlockTopic(ctx context.Context) {
	var bestBlockMessage BestBlockMessage
	var pid peer.ID
	var bh *model.BlockHeader
	var bhMeta *model.BlockHeaderMeta
	var blockMessage BlockMessage
	var msgBytes []byte

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("P2P service shutting down")
			return
		default:
			m, err := s.subscriptions[bestBlockTopicName].Next(ctx)
			if err != nil {
				s.logger.Errorf("error getting msg from best block topic: %v", err)
				continue
			}
			if m.ReceivedFrom != s.host.ID() {
				s.logger.Debugf("BESTBLOCK: topic: %s - from: %s - message: %s\n", *m.Message.Topic, m.ReceivedFrom.ShortString(), strings.TrimSpace(string(m.Message.Data)))

				// decode request
				bestBlockMessage = BestBlockMessage{}
				err = json.Unmarshal(m.Data, &bestBlockMessage)
				if err != nil {
					s.logger.Errorf("json unmarshal error: ", err)
					continue
				}
				pid, err = peer.Decode(bestBlockMessage.PeerId)
				if err != nil {
					s.logger.Errorf("error decoding peerId: ", err)
					continue
				}

				// get best block from blockchain service
				bh, bhMeta, err = s.blockchainClient.GetBestBlockHeader(ctx)
				if err != nil {
					s.logger.Errorf("error getting best block header: ", err)
					continue
				}
				if bh == nil {
					s.logger.Errorf("error getting best block header: ", err)
					continue
				}

				blockMessage = BlockMessage{
					Hash:       bh.Hash().String(),
					Height:     bhMeta.Height,
					DataHubUrl: s.AssetHttpAddressURL,
				}

				msgBytes, err = json.Marshal(blockMessage)
				if err != nil {
					s.logger.Errorf("json marshal error: ", err)
					continue
				}

				// send best block to the requester
				err = s.sendPeerMessage(ctx, pid, msgBytes)
				if err != nil {
					s.logger.Errorf("error sending peer message: ", err)
				}
			}
		}
	}
}

func (s *Server) sendPeerMessage(ctx context.Context, pid peer.ID, msg []byte) (err error) {
	h2pi := s.host.Peerstore().PeerInfo(pid)
	s.logger.Infof("dialing %s", h2pi.Addrs)
	if err = s.host.Connect(ctx, h2pi); err != nil {
		s.logger.Errorf("failed to connect: %+v", err)
	}

	var st network.Stream
	st, err = s.host.NewStream(
		ctx,
		pid,
		protocol.ID(s.bitcoinProtocolId),
	)
	if err != nil {
		return err
	}
	defer func() {
		err = st.Close()
		if err != nil {
			s.logger.Errorf("error closing stream: %s", err)
		}
	}()

	_, err = st.Write(msg)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) handleBlockTopic(ctx context.Context) {
	s.logger.Debugf("handleBlockTopic")

	var pubSubMessage *pubsub.Message
	var blockMessage BlockMessage
	var hash *chainhash.Hash
	var err error

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("P2P service shutting down")
			return
		default:
			pubSubMessage, err = s.subscriptions[blockTopicName].Next(ctx)
			if err != nil {
				s.logger.Errorf("error getting msg from block topic: %v", err)
				continue
			}
			// decode request
			blockMessage = BlockMessage{}
			err = json.Unmarshal(pubSubMessage.Data, &blockMessage)
			if err != nil {
				s.logger.Errorf("json unmarshal error: ", err)
				continue
			}

			s.notificationCh <- &notificationMsg{
				Timestamp: time.Now().UTC().Format(isoFormat),
				Type:      "block",
				Hash:      blockMessage.Hash,
				BaseURL:   blockMessage.DataHubUrl,
				PeerId:    blockMessage.PeerId,
			}

			if pubSubMessage.ReceivedFrom != s.host.ID() {
				s.logger.Debugf("BLOCK: topic: %s - from: %s - message: %s\n", *pubSubMessage.Message.Topic, pubSubMessage.ReceivedFrom.ShortString(), blockMessage)
				hash, err = chainhash.NewHashFromStr(blockMessage.Hash)
				if err != nil {
					s.logger.Errorf("error getting chainhash from string %s", blockMessage.Hash, err)
					continue
				}
				if err = s.blockValidationClient.BlockFound(ctx, hash, blockMessage.DataHubUrl); err != nil {
					s.logger.Errorf("[p2p] error validating block from %s: %s", blockMessage.DataHubUrl, err)
				}
			} else {
				s.logger.Debugf("block message received from myself %s- ignoring\n", pubSubMessage.ReceivedFrom.ShortString())
			}
		}
	}
}

func (s *Server) handleSubtreeTopic(ctx context.Context) {
	var pubSubMessage *pubsub.Message
	var subtreeMessage SubtreeMessage
	var hash *chainhash.Hash
	var err error

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("P2P service shutting down")
			return
		default:
			pubSubMessage, err = s.subscriptions[subtreeTopicName].Next(ctx)
			if err != nil {
				s.logger.Errorf("error getting msg from subtree topic: %v", err)
				continue
			}
			// decode request
			subtreeMessage = SubtreeMessage{}
			err = json.Unmarshal(pubSubMessage.Data, &subtreeMessage)
			if err != nil {
				s.logger.Errorf("json unmarshal error: ", err)
				continue
			}

			s.notificationCh <- &notificationMsg{
				Timestamp: time.Now().UTC().Format(isoFormat),
				Type:      "subtree",
				Hash:      subtreeMessage.Hash,
				BaseURL:   subtreeMessage.DataHubUrl,
				PeerId:    subtreeMessage.PeerId,
			}

			if pubSubMessage.ReceivedFrom != s.host.ID() {
				s.logger.Debugf("SUBTREE: topic: %s - from: %s - message: %s\n", *pubSubMessage.Message.Topic, pubSubMessage.ReceivedFrom.ShortString(), subtreeMessage)
				hash, err = chainhash.NewHashFromStr(subtreeMessage.Hash)
				if err != nil {
					s.logger.Errorf("error getting chainhash from string %s", subtreeMessage.Hash, err)
					continue
				}
				if err = s.blockValidationClient.SubtreeFound(ctx, hash, subtreeMessage.DataHubUrl); err != nil {
					s.logger.Errorf("[p2p] error validating subtree from %s: %s", subtreeMessage.DataHubUrl, err)
				}
			} else {
				s.logger.Debugf("subtree message received from myself %s- ignoring\n", pubSubMessage.ReceivedFrom.ShortString())
			}
		}
	}
}

func (s *Server) handleMiningOnTopic(ctx context.Context) {
	var pubSubMessage *pubsub.Message
	var miningOnMessage MiningOnMessage
	var err error

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("P2P service shutting down")
			return
		default:
			pubSubMessage, err = s.subscriptions[miningOnTopicName].Next(ctx)
			if err != nil {
				s.logger.Errorf("error getting msg from miningOn topic: %v", err)
				continue
			}
			// decode request
			miningOnMessage = MiningOnMessage{}
			err = json.Unmarshal(pubSubMessage.Data, &miningOnMessage)
			if err != nil {
				s.logger.Errorf("json unmarshal error: ", err)
				continue
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
	}
}
