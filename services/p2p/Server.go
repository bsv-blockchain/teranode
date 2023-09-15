package p2p

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"

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

type Server struct {
	host              host.Host
	topics            map[string]*pubsub.Topic
	subscriptions     map[string]*pubsub.Subscription
	logger            utils.Logger
	blockTopicName    string
	subtreeTopicName  string
	bitcoinProtocolId string

	blockchainClient blockchain.ClientI
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
		bitcoinProtocolId: "/bitcoin/1.0.0",
	}
}

func (s *Server) Init(ctx context.Context) (err error) {
	s.logger.Infof("P2P service initialising")
	var ok bool
	s.blockTopicName, ok = gocore.Config().Get("p2p_block_topic")
	if !ok {
		panic("p2p_block_topic not set")
	}
	s.subtreeTopicName, ok = gocore.Config().Get("p2p_subtree_topic")
	if !ok {
		panic("p2p_subtree_topic not set")
	}

	s.blockchainClient, err = blockchain.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("could not create blockchain client [%w]", err)
	}

	return nil
}

func (s *Server) Start(ctx context.Context) error {
	s.logger.Infof("P2P service starting")

	topicNames := []string{s.blockTopicName, s.subtreeTopicName}
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

	s.host.SetStreamHandler(protocol.ID(s.bitcoinProtocolId), handleBlockchainMessage)
	s.logger.Infof("P2P service start ending")

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

				// if it's a block notificdation send it on the block channel.
				if notification.Type == model.NotificationType_Block {
					if err := s.topics[s.blockTopicName].Publish(ctx, []byte(notification.Hash.CloneBytes())); err != nil {
						log.Println("publish error:", err)
					}
				}
			}
		}
	}()

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.logger.Infof("Stopping P2P service")

	return nil
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
					log.Println("Connected to:", peer.ID.Pretty())
					anyConnected = true
				}
			}
		}
	}
	s.logger.Debugf("Peer discovery complete")
	s.logger.Debugf("connected to %d peers\n", len(s.host.Network().Peers()))
	s.logger.Debugf("peerstore has %d peers\n", len(s.host.Peerstore().Peers()))

}

func handleBlockchainMessage(s network.Stream) {
	buf, err := io.ReadAll(s)
	if err != nil {
		s.Reset()
		log.Println(err)
		return
	}
	s.Close()
	log.Printf("Received block topic message: %s", string(buf))
}
