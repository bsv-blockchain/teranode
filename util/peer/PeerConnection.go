package peer

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/services/p2p"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/multiformats/go-multiaddr"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/protocol"
	dRouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dUtil "github.com/libp2p/go-libp2p/p2p/discovery/util"

	"github.com/ordishs/gocore"
)

type PeerConnection struct {
	host              host.Host
	topics            map[string]*pubsub.Topic
	subscriptions     map[string]*pubsub.Subscription
	logger            ulogger.Logger
	bitcoinProtocolId string
	usePrivateDHT     bool
	handlerByTopic    map[string]Handler
	startTime         time.Time
}

type Handler func(msg []byte, from string)

type PeerConfig struct {
	ProcessName   string
	IP            string
	Port          int
	SharedKey     string
	UsePrivateDHT bool
}

func NewPeerConnection(logger ulogger.Logger, config PeerConfig) *PeerConnection {
	logger.Debugf("[PeerConnection] Creating PeerConnection")

	var pk *crypto.PrivKey
	var err error
	privateKeyFilename := fmt.Sprintf("%s.%s.p2p.private_key", config.ProcessName, gocore.Config().GetContext())

	pk, err = readPrivateKey(privateKeyFilename)
	if err != nil {
		pk, err = generatePrivateKey(privateKeyFilename)
		if err != nil {
			panic(err)
		}
	}

	var h host.Host
	if config.UsePrivateDHT {
		s := ""
		s += fmt.Sprintln("/key/swarm/psk/1.0.0/")
		s += fmt.Sprintln("/base16/")
		s += config.SharedKey

		psk, err := pnet.DecodeV1PSK(bytes.NewBuffer([]byte(s)))
		if err != nil {
			panic(err)
		}
		h, err = libp2p.New(
			libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%d", config.IP, config.Port)),
			libp2p.Identity(*pk),
			libp2p.PrivateNetwork(psk),
		)
		if err != nil {
			panic(err)
		}
	} else {
		// copied from txblaster
		// h, err = libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"), libp2p.Identity(*pk))

		// p2p service did this
		h, err = libp2p.New(libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%d", config.IP, config.Port)), libp2p.Identity(*pk))
		if err != nil {
			panic(err)
		}
	}

	logger.Debugf("[PeerConnection] peer ID: %s", h.ID().Pretty())
	logger.Debugf("[PeerConnection] Connect to me on:")
	for _, addr := range h.Addrs() {
		logger.Debugf("[PeerConnection]   %s/p2p/%s", addr, h.ID().Pretty())
	}

	return &PeerConnection{
		logger:            logger,
		host:              h,
		bitcoinProtocolId: "ubsv/bitcoin/1.0.0",
		usePrivateDHT:     config.UsePrivateDHT,
		handlerByTopic:    make(map[string]Handler),
		startTime:         time.Now(),
	}
}

func (s *PeerConnection) Start(ctx context.Context, topicNames ...string) error {
	s.logger.Infof("[PeerConnection] starting")

	go s.discoverPeers(ctx, topicNames)

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
		// if topicName != rejectedTxTopicName {
		sub, err = topic.Subscribe()
		if err != nil {
			return err
		}
		subscriptions[topicName] = sub
		// }
	}
	s.topics = topics
	s.subscriptions = subscriptions

	s.host.SetStreamHandler(protocol.ID(s.bitcoinProtocolId), s.streamHandler)

	return nil
}

func (s *PeerConnection) Stop(ctx context.Context) error {
	s.logger.Infof("[PeerConnection] stopping")
	return nil
}

func (s *PeerConnection) SetTopicHandler(ctx context.Context, topicName string, handler Handler) error {
	_, ok := s.handlerByTopic[topicName]
	if ok {
		return fmt.Errorf("[PeerConnection][SetTopicHandler] handler already exists for topic: %s", topicName)
	}

	s.handlerByTopic[topicName] = handler

	go func() {
		for {
			select {
			case <-ctx.Done():
				s.logger.Infof("[PeerConnection][SetTopicHandler] shutting down")
				return
			default:
				m, err := s.subscriptions[topicName].Next(ctx)
				if err != nil {
					s.logger.Errorf("[PeerConnection][SetTopicHandler] error getting msg from %s topic: %v", topicName, err)
					continue
				}

				if m.ReceivedFrom == s.host.ID() {
					continue
				}

				s.logger.Debugf("[PeerConnection][SetTopicHandler]: topic: %s - from: %s - message: %s\n", *m.Message.Topic, m.ReceivedFrom.ShortString(), strings.TrimSpace(string(m.Message.Data)))
				handler(m.Data, m.ReceivedFrom.String())
			}
		}
	}()

	return nil
}

func (s *PeerConnection) Publish(ctx context.Context, topicName string, msgBytes []byte) {
	if err := s.topics[topicName].Publish(ctx, msgBytes); err != nil {
		s.logger.Errorf("[PeerConnection][Publish] publish error:", err)
	}
}

/* SendToPeer sends a message to a peer. It will attempt to connect to the peer if not already connected. */
func (s *PeerConnection) SendToPeer(ctx context.Context, pid peer.ID, msg []byte) (err error) {
	h2pi := s.host.Peerstore().PeerInfo(pid)
	s.logger.Infof("[PeerConnection][SendToPeer] dialing %s", h2pi.Addrs)
	if err = s.host.Connect(ctx, h2pi); err != nil {
		s.logger.Errorf("[PeerConnection][SendToPeer] failed to connect: %+v", err)
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
			s.logger.Errorf("[PeerConnection][SendToPeer] error closing stream: %s", err)
		}
	}()

	_, err = st.Write(msg)
	if err != nil {
		return err
	}

	return nil
}

func generatePrivateKey(privateKeyFilename string) (*crypto.PrivKey, error) {
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

func readPrivateKey(privateKeyFilename string) (*crypto.PrivKey, error) {
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

func (s *PeerConnection) discoverPeers(ctx context.Context, topicNames []string) {
	var kademliaDHT *dht.IpfsDHT

	if s.usePrivateDHT {
		kademliaDHT = initPrivateDHT(ctx, s.host)
	} else {
		kademliaDHT = p2p.InitDHT(ctx, s.host)
	}
	routingDiscovery := dRouting.NewRoutingDiscovery(kademliaDHT)
	for _, topicName := range topicNames {
		dUtil.Advertise(ctx, routingDiscovery, topicName)
	}
	s.logger.Debugf("[PeerConnection] connected to %d peers\n", len(s.host.Network().Peers()))
	s.logger.Debugf("[PeerConnection] peerstore has %d peers\n", len(s.host.Peerstore().Peers()))

	// Look for others who have announced and attempt to connect to them
	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[PeerConnection] shutting down")
			return
		default:
			// s.logger.Debugf("[PeerConnection] Searching for peers for %d topic(s)", len(topicNames))
			for _, topicName := range topicNames {
				// s.logger.Debugf("[PeerConnection] Searching for peers for topic %s..", topicName)

				peerChan, err := routingDiscovery.FindPeers(ctx, topicName)
				if err != nil {
					s.logger.Errorf("[PeerConnection] error finding peers: %+v", err)
				}

				// s.logger.Debugf("[PeerConnection] Checking peerChan for topic %s", topicName)
				for p := range peerChan {

					if p.ID == s.host.ID() {
						// s.logger.Debugf("[PeerConnection][%s] Ignoring self for topic %s", p.String(), topicName)
						continue // No self connection
					}

					// no point trying to connect to a peer that is already connected
					if s.host.Network().Connectedness(p.ID) == network.Connected {
						continue
					}

					// s.logger.Debugf("[PeerConnection]%+v[%s] Connecting for topic %s", p, p.String(), topicName)
					err = s.host.Connect(ctx, p)
					if err != nil {
						// A peer may not be available at the time of discovery.
						// A peer stays in the DHT for around 24 hours before it is removed from the peerstore
						// Logging each attempt to connect to these peers is too noisy

						s.logger.Debugf("[PeerConnection][%s] Failed connecting for topic %s: %+v", p.String(), topicName, err)
					} else {
						s.logger.Infof("[PeerConnection][%s] Connected to topic %s : discovered and connected %s after startup", p.String(), topicName, time.Since(s.startTime))
					}
				}
				time.Sleep(5 * time.Second)
			}
		}
	}
}

func initPrivateDHT(ctx context.Context, host host.Host) *dht.IpfsDHT {
	bootstrapAddresses, _ := gocore.Config().GetMulti("p2p_bootstrapAddresses", "|")
	if len(bootstrapAddresses) == 0 {
		panic(fmt.Errorf("[PeerConnection] bootstrapAddresses not set in config"))
	}
	for _, ba := range bootstrapAddresses {
		bootstrapAddr, err := multiaddr.NewMultiaddr(ba)
		if err != nil {
			panic(fmt.Sprintf("[PeerConnection] failed to create bootstrap multiaddress %s: %v", ba, err))
		}

		peerInfo, err := peer.AddrInfoFromP2pAddr(bootstrapAddr)
		if err != nil {
			panic(fmt.Sprintf("[PeerConnection] failed to get peerInfo from  %s: %v", ba, err))
		}

		err = host.Connect(ctx, *peerInfo)
		if err != nil {
			panic(fmt.Sprintf("[PeerConnection] failed to connect to bootstrap address %s: %v", ba, err))
		}
	}

	dhtProtocolIdStr, ok := gocore.Config().Get("p2p_dht_protocol_id")
	if !ok {
		panic(fmt.Errorf("[PeerConnection] error getting p2p_dht_protocol_id"))
	}
	dhtProtocolID := protocol.ID(dhtProtocolIdStr)

	var options []dht.Option
	options = append(options, dht.ProtocolPrefix(dhtProtocolID))
	options = append(options, dht.Mode(dht.ModeAuto))

	kademliaDHT, err := dht.New(ctx, host, options...)
	if err != nil {
		panic(err)
	}

	err = kademliaDHT.Bootstrap(ctx)
	if err != nil {
		panic(err)
	}

	return kademliaDHT
}

func (s *PeerConnection) streamHandler(ns network.Stream) {
	buf, err := io.ReadAll(ns)
	if err != nil {
		_ = ns.Reset()
		s.logger.Errorf("[PeerConnection] failed to read network stream: %+v              ", err.Error())
		return
	}
	_ = ns.Close()
	if len(buf) > 0 {
		s.logger.Debugf("[PeerConnection] Received message: %s", string(buf))
	}
}
