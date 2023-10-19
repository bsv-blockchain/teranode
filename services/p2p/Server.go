package p2p

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libsv/go-bt/v2/chainhash"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/protocol"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"

	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

const privateKeyFilename = "private_key"

var (
	topicPrefix        string
	blockTopicName     string
	bestBlockTopicName string
	subtreeTopicName   string
)

type Server struct {
	host              host.Host
	topics            map[string]*pubsub.Topic
	subscriptions     map[string]*pubsub.Subscription
	logger            utils.Logger
	bitcoinProtocolId string

	blockchainClient         blockchain.ClientI
	blobServerHttpAddressURL string
	e                        *echo.Echo
	notificationCh           chan *notificationMsg
}

type BestBlockMessage struct {
	PeerId string
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

func NewServer(logger utils.Logger) *Server {
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
	p2pPort, ok := gocore.Config().Get("p2p_port")
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
	blockTopicName = fmt.Sprintf("%s-%s", topicPrefix, btn)
	subtreeTopicName = fmt.Sprintf("%s-%s", topicPrefix, stn)
	bestBlockTopicName = fmt.Sprintf("%s-%s", topicPrefix, bbtn)

	h, err := libp2p.New(libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%s", p2pIp, p2pPort)), libp2p.Identity(*pk))
	if err != nil {
		panic(err)
	}
	logger.Debugf("peer ID: %s", h.ID().Pretty())
	logger.Debugf("Connect to me on:")
	for _, addr := range h.Addrs() {
		logger.Debugf("  %s/p2p/%s", addr, h.ID().Pretty())
	}

	return &Server{
		logger:            logger,
		host:              h,
		bitcoinProtocolId: "ubsv/bitcoin/1.0.0",
		notificationCh:    make(chan *notificationMsg),
	}
}

func (s *Server) Init(ctx context.Context) (err error) {
	s.logger.Infof("P2P service initialising")

	s.blockchainClient, err = blockchain.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("could not create blockchain client [%w]", err)
	}

	blobServerHttpAddressURL, _, _ := gocore.Config().GetURL("blobserver_httpAddress")
	securityLevel, _ := gocore.Config().GetInt("securityLevelHTTP", 0)

	if blobServerHttpAddressURL.Scheme == "http" && securityLevel == 1 {
		blobServerHttpAddressURL.Scheme = "https"
		s.logger.Warnf("blobserver_httpAddress is HTTP but securityLevel is 1, changing to HTTPS")
	} else if blobServerHttpAddressURL.Scheme == "https" && securityLevel == 0 {
		blobServerHttpAddressURL.Scheme = "http"
		s.logger.Warnf("blobserver_httpAddress is HTTPS but securityLevel is 0, changing to HTTP")
	}
	s.blobServerHttpAddressURL = blobServerHttpAddressURL.String()

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
	topicNames := []string{bestBlockTopicName, blockTopicName, subtreeTopicName}
	go s.discoverPeers(ctx, topicNames)

	ps, err := pubsub.NewGossipSub(ctx, s.host)
	if err != nil {
		return err
	}
	topics := map[string]*pubsub.Topic{}
	subscriptions := map[string]*pubsub.Subscription{}

	for _, topicName := range topicNames {
		topic, err := ps.Join(topicName)
		if err != nil {
			return err
		}
		topics[topicName] = topic

		sub, err := topic.Subscribe()
		if err != nil {
			return err
		}
		subscriptions[topicName] = sub
	}
	s.topics = topics
	s.subscriptions = subscriptions

	s.host.SetStreamHandler(protocol.ID(s.bitcoinProtocolId), s.handleBlockchainMessage)

	go s.handleBestBlockTopic(ctx)
	go s.handleBlockTopic(ctx)
	go s.handleSubtreeTopic(ctx)

	// Subscribe to the blockchain service
	blockchainSubscription, err := s.blockchainClient.Subscribe(ctx, "p2pServer")
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				s.logger.Infof("P2P service shutting down")
				return
			case notification := <-blockchainSubscription:
				if notification == nil {
					continue
				}
				// received a message
				s.logger.Debugf("P2P Received %s notification: %s", notification.Type, notification.Hash.String())

				if notification.Type == model.NotificationType_Block {
					// if it's a block notification send it on the block channel.
					b := BlockMessage{
						Hash:       notification.Hash.String(),
						DataHubUrl: s.blobServerHttpAddressURL,
						PeerId:     s.host.ID().String(),
					}

					msgBytes, err := json.Marshal(b)
					if err != nil {
						s.logger.Errorf("json mmarshal error: ", err)
						continue
					}
					if err := s.topics[blockTopicName].Publish(ctx, msgBytes); err != nil {
						s.logger.Errorf("publish error:", err)
					}

				} else if notification.Type == model.NotificationType_Subtree {
					// if it's a subtree notification send it on the subtree channel.
					sm := SubtreeMessage{
						Hash:       notification.Hash.String(),
						DataHubUrl: s.blobServerHttpAddressURL,
						PeerId:     s.host.ID().String(),
					}
					msgBytes, err := json.Marshal(sm)
					if err != nil {
						s.logger.Errorf("json mmarshal error: ", err)
						continue
					}
					if err := s.topics[subtreeTopicName].Publish(ctx, msgBytes); err != nil {
						s.logger.Errorf("publish error:", err)
					}
				}
			}
		}
	}()

	bbr := &BestBlockMessage{PeerId: s.host.ID().String()}

	msgBytes, err := json.Marshal(bbr)
	if err != nil {
		s.logger.Errorf("json mmarshal error: ", err)
	}
	// send  bestblock msg on topic
	if err := s.topics[bestBlockTopicName].Publish(ctx, []byte(msgBytes)); err != nil {
		s.logger.Errorf("publish error:", err)
	}

	return nil
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

func (s *Server) discoverPeers(ctx context.Context, tn []string) {
	kademliaDHT := initDHT(ctx, s.host)
	routingDiscovery := drouting.NewRoutingDiscovery(kademliaDHT)
	for _, topicName := range tn {
		dutil.Advertise(ctx, routingDiscovery, topicName)
	}

	// Look for others who have announced and attempt to connect to them
	anyConnected := false
	for !anyConnected {
		for _, topicName := range tn {
			s.logger.Debugf("Searching for peers for topic %s..\n", topicName)

			peerChan, err := routingDiscovery.FindPeers(ctx, topicName)
			if err != nil {
				panic(err)
			}

			for peer := range peerChan {
				if peer.ID == s.host.ID() {
					continue // No self connection
				}
				err := s.host.Connect(ctx, peer)
				if err != nil {
					//  we fail to connect to a lot of peers. Just ignore it for now.
					// s.logger.Debugf("Failed connecting to ", peer.ID.Pretty(), ", error:", err)
				} else {
					s.logger.Debugf("Connected to:", peer.ID.Pretty())
					anyConnected = true
				}
			}
		}
	}
	s.logger.Debugf("Peer discovery complete")
	s.logger.Debugf("connected to %d peers\n", len(s.host.Network().Peers()))
	s.logger.Debugf("peerstore has %d peers\n", len(s.host.Peerstore().Peers()))
}

func (s *Server) handleBlockchainMessage(ns network.Stream) {
	buf, err := io.ReadAll(ns)
	if err != nil {
		_ = ns.Reset()
		s.logger.Errorf("failed to read network stream: %+v              ", err.Error())
		return
	}
	ns.Close()
	if len(buf) > 0 {
		s.logger.Debugf("Received block topic message: %s", string(buf))
	}
}

func (s *Server) handleBestBlockTopic(ctx context.Context) {
	for {
		m, err := s.subscriptions[bestBlockTopicName].Next(ctx)
		if err != nil {
			s.logger.Errorf("error getting msg from best block topic: %v", err)
			continue
		}
		if m.ReceivedFrom != s.host.ID() {
			s.logger.Debugf("BESTBLOCK: topic: %s - from: %s - message: %s\n", *m.Message.Topic, m.ReceivedFrom.ShortString(), strings.TrimSpace(string(m.Message.Data)))

			// decode request
			msg := new(BestBlockMessage)
			err = json.Unmarshal(m.Data, msg)
			if err != nil {
				s.logger.Errorf("json unmarshal error: ", err)
				continue
			}
			pid, err := peer.Decode(msg.PeerId)
			if err != nil {
				s.logger.Errorf("error decoding peerId: ", err)
				continue
			}

			// get best block from blockchain service
			bh, h, err := s.blockchainClient.GetBestBlockHeader(ctx)
			if err != nil {
				s.logger.Errorf("error getting best block header: ", err)
				continue
			}
			if bh == nil {
				s.logger.Errorf("error getting best block header: ", err)
				continue
			}

			bbr := BlockMessage{
				Hash:       bh.Hash().String(),
				Height:     h,
				DataHubUrl: s.blobServerHttpAddressURL,
			}

			msgBytes, err := json.Marshal(bbr)
			if err != nil {
				s.logger.Errorf("json mmarshal error: ", err)
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

func (s *Server) sendPeerMessage(ctx context.Context, pid peer.ID, msg []byte) error {
	h2pi := s.host.Peerstore().PeerInfo(pid)
	log.Printf("dialing %s\n", h2pi.Addrs)
	if err := s.host.Connect(ctx, h2pi); err != nil {
		log.Printf("Failed to connect: %+v\n", err)
	}

	st, err := s.host.NewStream(
		ctx,
		pid,
		protocol.ID(s.bitcoinProtocolId),
	)
	if err != nil {
		return err
	}
	_, err = st.Write(msg)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) handleBlockTopic(ctx context.Context) {
	s.logger.Debugf("handleBlockTopic\n")
	for {
		m, err := s.subscriptions[blockTopicName].Next(ctx)
		if err != nil {
			s.logger.Errorf("error getting msg from block topic: %v", err)
			continue
		}
		// decode request
		msg := new(BlockMessage)
		err = json.Unmarshal(m.Data, msg)
		if err != nil {
			s.logger.Errorf("json unmarshal error: ", err)
			continue
		}

		s.notificationCh <- &notificationMsg{
			Type:    "block",
			Hash:    msg.Hash,
			BaseURL: msg.DataHubUrl,
			PeerId:  msg.PeerId,
		}

		if m.ReceivedFrom != s.host.ID() {
			s.logger.Debugf("BLOCK: topic: %s - from: %s - message: %s\n", *m.Message.Topic, m.ReceivedFrom.ShortString(), msg)
			validationClient := blockvalidation.NewClient(ctx)
			hash, err := chainhash.NewHashFromStr(msg.Hash)
			if err != nil {
				s.logger.Errorf("error getting chainhash from string %s", msg.Hash, err)
				continue
			}
			if err = validationClient.BlockFound(ctx, hash, msg.DataHubUrl); err != nil {
				s.logger.Errorf("[p2p] error validating block from %s: %s", msg.DataHubUrl, err)
			}
		} else {
			s.logger.Debugf("block message received from myself %s- ignoring\n", m.ReceivedFrom.ShortString())
		}
	}
}

func (s *Server) handleSubtreeTopic(ctx context.Context) {
	for {
		m, err := s.subscriptions[subtreeTopicName].Next(ctx)
		if err != nil {
			s.logger.Errorf("error getting msg from subtree topic: %v", err)
			continue
		}
		// decode request
		msg := new(SubtreeMessage)
		err = json.Unmarshal(m.Data, msg)
		if err != nil {
			s.logger.Errorf("json unmarshal error: ", err)
			continue
		}

		s.notificationCh <- &notificationMsg{
			Type:    "subtree",
			Hash:    msg.Hash,
			BaseURL: msg.DataHubUrl,
			PeerId:  msg.PeerId,
		}

		if m.ReceivedFrom != s.host.ID() {
			s.logger.Debugf("SUBTREE: topic: %s - from: %s - message: %s\n", *m.Message.Topic, m.ReceivedFrom.ShortString(), msg)
			validationClient := blockvalidation.NewClient(ctx)
			hash, err := chainhash.NewHashFromStr(msg.Hash)
			if err != nil {
				s.logger.Errorf("error getting chainhash from string %s", msg.Hash, err)
				continue
			}
			if err = validationClient.SubtreeFound(ctx, hash, msg.DataHubUrl); err != nil {
				s.logger.Errorf("[p2p] error validating subtree from %s: %s", msg.DataHubUrl, err)
			}
		} else {
			s.logger.Debugf("subtree message received from myself %s- ignoring\n", m.ReceivedFrom.ShortString())
		}
	}
}
