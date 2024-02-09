package peer

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
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

type PeerNode struct {
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

func NewPeerNode(logger ulogger.Logger, config PeerConfig) *PeerNode {
	logger.Debugf("[PeerNode] Creating node")

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
		h, err = libp2p.New(
			libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%d", config.IP, config.Port)),
			libp2p.Identity(*pk),
		)
		if err != nil {
			panic(err)
		}
	}

	logger.Infof("[PeerNode] peer ID: %s", h.ID().Pretty())
	logger.Infof("[PeerNode] Connect to me on:")
	for _, addr := range h.Addrs() {
		logger.Infof("[PeerNode]   %s/p2p/%s", addr, h.ID().Pretty())
	}

	return &PeerNode{
		logger:            logger,
		host:              h,
		bitcoinProtocolId: "ubsv/bitcoin/1.0.0",
		usePrivateDHT:     config.UsePrivateDHT,
		handlerByTopic:    make(map[string]Handler),
		startTime:         time.Now(),
	}
}

func (s *PeerNode) Start(ctx context.Context, topicNames ...string) error {
	s.logger.Infof("[PeerNode] starting")

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

func (s *PeerNode) Stop(ctx context.Context) error {
	s.logger.Infof("[PeerNode] stopping")
	return nil
}

func (s *PeerNode) SetTopicHandler(ctx context.Context, topicName string, handler Handler) error {
	_, ok := s.handlerByTopic[topicName]
	if ok {
		return fmt.Errorf("[PeerNode][SetTopicHandler] handler already exists for topic: %s", topicName)
	}

	s.handlerByTopic[topicName] = handler

	go func() {
		for {
			select {
			case <-ctx.Done():
				s.logger.Infof("[PeerNode][SetTopicHandler] shutting down")
				return
			default:
				m, err := s.subscriptions[topicName].Next(ctx)
				if err != nil {
					s.logger.Errorf("[PeerNode][SetTopicHandler] error getting msg from %s topic: %v", topicName, err)
					continue
				}

				if m.ReceivedFrom == s.host.ID() {
					continue
				}

				s.logger.Debugf("[PeerNode][SetTopicHandler]: topic: %s - from: %s - message: %s\n", *m.Message.Topic, m.ReceivedFrom.ShortString(), strings.TrimSpace(string(m.Message.Data)))
				handler(m.Data, m.ReceivedFrom.String())
			}
		}
	}()

	return nil
}

func (s *PeerNode) Publish(ctx context.Context, topicName string, msgBytes []byte) {
	if err := s.topics[topicName].Publish(ctx, msgBytes); err != nil {
		s.logger.Errorf("[PeerNode][Publish] publish error:", err)
	}
}

/* SendToPeer sends a message to a peer. It will attempt to connect to the peer if not already connected. */
func (s *PeerNode) SendToPeer(ctx context.Context, pid peer.ID, msg []byte) (err error) {
	h2pi := s.host.Peerstore().PeerInfo(pid)
	s.logger.Infof("[PeerNode][SendToPeer] dialing %s", h2pi.Addrs)
	if err = s.host.Connect(ctx, h2pi); err != nil {
		s.logger.Errorf("[PeerNode][SendToPeer] failed to connect: %+v", err)
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
			s.logger.Errorf("[PeerNode][SendToPeer] error closing stream: %s", err)
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

func (s *PeerNode) discoverPeers(ctx context.Context, topicNames []string) {
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
	s.logger.Debugf("[PeerNode] connected to %d peers\n", len(s.host.Network().Peers()))
	s.logger.Debugf("[PeerNode] peerstore has %d peers\n", len(s.host.Peerstore().Peers()))

	ctx = network.WithSimultaneousConnect(ctx, true, "hole punching")
	peerAddrErrorMap := sync.Map{}

	// Look for others who have announced and attempt to connect to them
	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[PeerNode] shutting down")
			return
		default:

			peerAddrMap := sync.Map{}

			g := sync.WaitGroup{}
			g.Add(len(topicNames))

			start := time.Now()

			for _, topicName := range topicNames {

				// search for everything all at once
				go func(topicName string) {
					addrChan, err := routingDiscovery.FindPeers(ctx, topicName)
					if err != nil {
						s.logger.Errorf("[PeerNode] error finding peers: %+v", err)
					}

					for addr := range addrChan {

						if addr.ID == s.host.ID() {
							continue // No self connection
						}

						// no point trying to connect to a peer that is already connected
						if s.host.Network().Connectedness(addr.ID) == network.Connected {
							continue
						}

						if peerConnectionErrorString, ok := peerAddrErrorMap.Load(addr.ID.String()); ok {
							// peer id mismatch is where the node has started using a new private key, no point trying to connect to it
							if strings.Contains(peerConnectionErrorString.(string), "peer id mismatch") {
								continue
							}
						}

						peerAddrMap.Store(addr.ID.String(), addr)
					}

					g.Done()
				}(topicName)

			}

			g.Wait()

			s.logger.Infof("[PeerNode] Concurrent peer search completed in %s", time.Since(start))

			peerAddrMap.Range(func(_, peerAddr interface{}) bool {

				/* A connection has a timeout of 5 seconds. Lets make parallel connect attempts rather than one at a time. */
				go func(addr peer.AddrInfo) {
					err := s.host.Connect(ctx, addr)
					if err != nil {
						// A peer may not be available at the time of discovery.
						// A peer stays in the DHT for around 24 hours before it is removed from the peerstore
						// Logging each attempt to connect to these peers is too noisy

						s.logger.Debugf("[PeerNode][%s] Connection failed : %+v", addr.String(), err)
						peerAddrErrorMap.Store(addr.ID.String(), err.Error())
					} else {
						s.logger.Infof("[PeerNode][%s] Connected in %s", addr.String(), time.Since(s.startTime))
					}
				}(peerAddr.(peer.AddrInfo))

				return true
			})

			time.Sleep(5 * time.Second)
		}
	}
}

func initPrivateDHT(ctx context.Context, host host.Host) *dht.IpfsDHT {
	bootstrapAddresses, _ := gocore.Config().GetMulti("p2p_bootstrapAddresses", "|")
	if len(bootstrapAddresses) == 0 {
		panic(fmt.Errorf("[PeerNode] bootstrapAddresses not set in config"))
	}
	for _, ba := range bootstrapAddresses {
		bootstrapAddr, err := multiaddr.NewMultiaddr(ba)
		if err != nil {
			panic(fmt.Sprintf("[PeerNode] failed to create bootstrap multiaddress %s: %v", ba, err))
		}

		peerInfo, err := peer.AddrInfoFromP2pAddr(bootstrapAddr)
		if err != nil {
			panic(fmt.Sprintf("[PeerNode] failed to get peerInfo from  %s: %v", ba, err))
		}

		err = host.Connect(ctx, *peerInfo)
		if err != nil {
			panic(fmt.Sprintf("[PeerNode] failed to connect to bootstrap address %s: %v", ba, err))
		}
	}

	dhtProtocolIdStr, ok := gocore.Config().Get("p2p_dht_protocol_id")
	if !ok {
		panic(fmt.Errorf("[PeerNode] error getting p2p_dht_protocol_id"))
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

func (s *PeerNode) streamHandler(ns network.Stream) {
	buf, err := io.ReadAll(ns)
	if err != nil {
		_ = ns.Reset()
		s.logger.Errorf("[PeerNode] failed to read network stream: %+v              ", err.Error())
		return
	}
	_ = ns.Close()
	if len(buf) > 0 {
		s.logger.Debugf("[PeerNode] Received message: %s", string(buf))
	}
}
