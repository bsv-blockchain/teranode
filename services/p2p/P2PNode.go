package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/libp2p/go-libp2p/core/protocol"
	dRouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dUtil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/multiformats/go-multiaddr"
	"github.com/ordishs/gocore"
)

const errorCreatingDhtMessage = "[P2PNode] error creating DHT"

type P2PNode struct {
	config                    P2PConfig
	settings                  *settings.Settings
	host                      host.Host
	pubSub                    *pubsub.PubSub
	topics                    map[string]*pubsub.Topic
	logger                    ulogger.Logger
	bitcoinProtocolID         string
	handlerByTopic            map[string]Handler
	startTime                 time.Time
	blocksKafkaProducerClient kafka.KafkaAsyncProducerI

	// The following variables must only be used atomically.
	bytesReceived uint64
	bytesSent     uint64
	lastRecv      int64
	lastSend      int64
}

type Handler func(ctx context.Context, msg []byte, from string)

type P2PConfig struct {
	ProcessName     string
	IP              string
	Port            int
	PrivateKey      string
	SharedKey       string
	UsePrivateDHT   bool
	OptimiseRetries bool
	Advertise       bool
	StaticPeers     []string
}

func NewP2PNode(logger ulogger.Logger, tSettings *settings.Settings, config P2PConfig, blocksKafkaProducerClient kafka.KafkaAsyncProducerI) (*P2PNode, error) {
	logger.Infof("[P2PNode] Creating node")

	var (
		pk  *crypto.PrivKey // Private key for the node
		err error
	)

	// If no private key is provided in the configuration, attempt to read or generate one
	if config.PrivateKey == "" {
		privateKeyFilename := fmt.Sprintf("%s.%s.p2p.private_key", config.ProcessName, gocore.Config().GetContext())

		pk, err = readPrivateKey(privateKeyFilename)
		if err != nil {
			// If reading fails, attempt to generate a new private key
			pk, err = generatePrivateKey(privateKeyFilename)
			if err != nil {
				return nil, errors.NewConfigurationError("[P2PNode] error generating private key", err)
			}
		}
	} else {
		// If a private key is provided, decode it
		pk, err = decodeHexEd25519PrivateKey(config.PrivateKey)
		if err != nil {
			return nil, errors.NewInvalidArgumentError("[P2PNode] error decoding private key", err)
		}
	}

	var h host.Host // The libp2p host for the node

	// If a private DHT is configured, set up the private network
	if config.UsePrivateDHT {
		h, err = setUpPrivateNetwork(config, pk)
		if err != nil {
			return nil, errors.NewServiceError("[P2PNode] error setting up private network", err)
		}
	} else {
		// If no private DHT is configured, create a standard libp2p host
		h, err = libp2p.New(
			libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%d", config.IP, config.Port)),
			libp2p.Identity(*pk),
		)
		if err != nil {
			return nil, errors.NewServiceError("[P2PNode] error creating libp2p host", err)
		}
	}

	logger.Infof("[P2PNode] peer ID: %s", h.ID().String())
	logger.Infof("[P2PNode] Connect to me on:")

	for _, addr := range h.Addrs() {
		logger.Infof("[P2PNode]   %s/p2p/%s", addr, h.ID().String())
	}

	node := &P2PNode{
		config:                    config,
		logger:                    logger,
		settings:                  tSettings,
		host:                      h,
		bitcoinProtocolID:         "teranode/bitcoin/1.0.0",
		handlerByTopic:            make(map[string]Handler),
		startTime:                 time.Now(),
		blocksKafkaProducerClient: blocksKafkaProducerClient,
	}

	// Set up connection notifications
	h.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(n network.Network, conn network.Conn) {
			node.logger.Debugf("[P2PNode] Peer connected: %s", conn.RemotePeer().String())
		},
		DisconnectedF: func(n network.Network, conn network.Conn) {
			node.logger.Debugf("[P2PNode] Peer disconnected: %s", conn.RemotePeer().String())
		},
	})

	return node, nil
}

func setUpPrivateNetwork(config P2PConfig, pk *crypto.PrivKey) (host.Host, error) {
	var h host.Host

	s := ""
	s += fmt.Sprintln("/key/swarm/psk/1.0.0/")
	s += fmt.Sprintln("/base16/")
	s += config.SharedKey

	psk, err := pnet.DecodeV1PSK(bytes.NewBuffer([]byte(s)))
	if err != nil {
		return nil, errors.NewInvalidArgumentError("[P2PNode] error decoding shared key", err)
	}

	h, err = libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%d", config.IP, config.Port)),
		libp2p.Identity(*pk),
		libp2p.PrivateNetwork(psk),
	)
	if err != nil {
		return nil, errors.NewServiceError("[P2PNode] error creating private network", err)
	}

	return h, nil
}

func (s *P2PNode) startStaticPeerConnector(ctx context.Context) {
	if len(s.config.StaticPeers) == 0 {
		s.logger.Infof("[P2PNode] no static peers to connect to - skipping connection attempt")
		return
	}

	go func() {
		logged := false

		for {
			select {
			case <-ctx.Done():
				s.logger.Infof("[P2PNode] shutting down")
				return
			default:
				allConnected := s.connectToStaticPeers(ctx, s.config.StaticPeers)
				if allConnected {
					if !logged {
						s.logger.Infof("[P2PNode] all static peers connected")
					}

					logged = true
					// it is possible that a peer disconnects, so we need to keep checking
					time.Sleep(30 * time.Second)
				} else {
					logged = false

					s.logger.Infof("[P2PNode] all static peers NOT connected")

					time.Sleep(5 * time.Second)
				}
			}
		}
	}()
}

func (s *P2PNode) initGossipSub(ctx context.Context, topicNames []string) error {
	ps, err := pubsub.NewGossipSub(ctx, s.host,
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign)) // Ensure messages are signed and verified
	if err != nil {
		return err
	}

	topics, shouldReturn, err := subscribeToTopics(topicNames, ps, s)
	if shouldReturn {
		return err
	}

	s.pubSub = ps
	s.topics = topics

	return nil
}

func (s *P2PNode) Start(ctx context.Context, topicNames ...string) error {
	s.logger.Infof("[%s] starting", s.config.ProcessName)

	s.startStaticPeerConnector(ctx)

	go func() {
		if err := s.discoverPeers(ctx, topicNames); err != nil {
			s.logger.Errorf("[P2PNode] error discovering peers: %+v", err)
		}
	}()

	if err := s.initGossipSub(ctx, topicNames); err != nil {
		return err
	}

	s.host.SetStreamHandler(protocol.ID(s.bitcoinProtocolID), s.streamHandler)

	return nil
}

func subscribeToTopics(topicNames []string, ps *pubsub.PubSub, s *P2PNode) (map[string]*pubsub.Topic, bool, error) {
	topics := map[string]*pubsub.Topic{}

	var topic *pubsub.Topic

	var err error

	for _, topicName := range topicNames {
		topic, err = ps.Join(topicName)
		if err != nil {
			return nil, true, err
		}

		s.logger.Infof("[P2PNode] joined topic: %s", topicName)

		topics[topicName] = topic
	}

	return topics, false, nil
}

func (s *P2PNode) Stop(ctx context.Context) error {
	s.logger.Infof("[P2PNode] stopping")
	return nil
}

func (s *P2PNode) SetTopicHandler(ctx context.Context, topicName string, handler Handler) error {
	_, ok := s.handlerByTopic[topicName]
	if ok {
		return errors.NewServiceError("[P2PNode][SetTopicHandler] handler already exists for topic: %s", topicName)
	}

	topic := s.topics[topicName]

	sub, err := topic.Subscribe()
	if err != nil {
		return err
	}

	s.handlerByTopic[topicName] = handler

	go func() {
		s.logger.Infof("[P2PNode][SetTopicHandler] starting handler for topic: %s", topicName)

		for {
			select {
			case <-ctx.Done():
				s.logger.Infof("[P2PNode][SetTopicHandler] shutting down")
				return
			default:
				m, err := sub.Next(ctx)
				if err != nil {
					s.logger.Errorf("[P2PNode][SetTopicHandler] error getting msg from %s topic: %v", topicName, err)
					continue
				}

				s.logger.Debugf("[P2PNode][SetTopicHandler]: topic: %s - from: %s - message: %s\n", *m.Message.Topic, m.ReceivedFrom.ShortString(), strings.TrimSpace(string(m.Message.Data)))
				handler(ctx, m.Data, m.ReceivedFrom.String())
			}
		}
	}()

	return nil
}

func (s *P2PNode) HostID() peer.ID {
	return s.host.ID()
}

func (s *P2PNode) GetTopic(topicName string) *pubsub.Topic {
	return s.topics[topicName]
}

func (s *P2PNode) Publish(ctx context.Context, topicName string, msgBytes []byte) error {
	if len(s.topics) == 0 {
		return errors.NewServiceError("[P2PNode][Publish] topics not initialised")
	}

	if _, ok := s.topics[topicName]; !ok {
		return errors.NewServiceError("[P2PNode][Publish] topic not found: %s", topicName)
	}

	if err := s.topics[topicName].Publish(ctx, msgBytes); err != nil {
		return errors.NewServiceError("[P2PNode][Publish] publish error", err)
	}

	s.logger.Debugf("[P2PNode][Publish] topic: %s - message: %s\n", topicName, strings.TrimSpace(string(msgBytes)))

	// Increment bytesSent using atomic operations
	atomic.AddUint64(&s.bytesSent, uint64(len(msgBytes)))

	// Update lastSend timestamp
	atomic.StoreInt64(&s.lastSend, time.Now().Unix())

	return nil
}

/* SendToPeer sends a message to a peer. It will attempt to connect to the peer if not already connected. */
func (s *P2PNode) SendToPeer(ctx context.Context, pid peer.ID, msg []byte) (err error) {
	h2pi := s.host.Peerstore().PeerInfo(pid)
	s.logger.Infof("[P2PNode][SendToPeer] dialing %s", h2pi.Addrs)

	if err = s.host.Connect(ctx, h2pi); err != nil {
		s.logger.Errorf("[P2PNode][SendToPeer] failed to connect: %+v", err)
	}

	var st network.Stream

	st, err = s.host.NewStream(
		ctx,
		pid,
		protocol.ID(s.bitcoinProtocolID),
	)
	if err != nil {
		return err
	}

	defer func() {
		err = st.Close()
		if err != nil {
			s.logger.Errorf("[P2PNode][SendToPeer] error closing stream: %s", err)
		}
	}()

	_, err = st.Write(msg)
	if err != nil {
		return err
	}

	// Increment bytesSent using atomic operations
	atomic.AddUint64(&s.bytesSent, uint64(len(msg)))

	// Update lastSend timestamp
	atomic.StoreInt64(&s.lastSend, time.Now().Unix())

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
	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less
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

func decodeHexEd25519PrivateKey(hexEncodedPrivateKey string) (*crypto.PrivKey, error) {
	privKeyBytes, err := hex.DecodeString(hexEncodedPrivateKey)
	if err != nil {
		return nil, err
	}

	privKey, err := crypto.UnmarshalEd25519PrivateKey(privKeyBytes)
	if err != nil {
		return nil, err
	}

	return &privKey, nil
}

func (s *P2PNode) connectToStaticPeers(ctx context.Context, staticPeers []string) bool {
	i := len(staticPeers)

	for _, peerAddr := range staticPeers {
		peerInfo, err := peer.AddrInfoFromP2pAddr(multiaddr.StringCast(peerAddr))
		if err != nil {
			s.logger.Errorf("[P2PNode] failed to get peerInfo from  %s: %v", peerAddr, err)
			continue
		}

		if s.host.Network().Connectedness(peerInfo.ID) == network.Connected {
			i--
			continue
		}

		err = s.host.Connect(ctx, *peerInfo)
		if err != nil {
			s.logger.Debugf("[P2PNode] failed to connect to static peer %s: %v", peerAddr, err)
		} else {
			i--

			s.logger.Infof("[P2PNode] connected to static peer: %s", peerAddr)
		}
	}

	return i == 0
}

func (s *P2PNode) discoverPeers(ctx context.Context, topicNames []string) error {
	var (
		kademliaDHT *dht.IpfsDHT
		err         error
	)

	if s.config.UsePrivateDHT {
		kademliaDHT, err = s.initPrivateDHT(ctx, s.host)
	} else {
		kademliaDHT, err = s.initDHT(ctx, s.host)
	}

	if err != nil {
		return errors.NewServiceError(errorCreatingDhtMessage, err)
	}

	if kademliaDHT == nil {
		return nil
	}

	routingDiscovery := dRouting.NewRoutingDiscovery(kademliaDHT)

	if s.config.Advertise {
		for _, topicName := range topicNames {
			s.logger.Infof("[P2PNode] advertising topic: %s", topicName)
			dUtil.Advertise(ctx, routingDiscovery, topicName)
		}
	}

	s.logger.Debugf("[P2PNode] %d peer connections\n", len(s.host.Network().Peers()))
	s.logger.Debugf("[P2PNode] %d peers in peerstore\n", len(s.host.Peerstore().Peers()))

	ctx = network.WithSimultaneousConnect(ctx, true, "hole punching")
	peerAddrErrorMap := sync.Map{}

	// Look for others who have announced and attempt to connect to them
	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[P2PNode] shutting down")
			return nil
		default:
			peerAddrMap := sync.Map{}

			g := sync.WaitGroup{}
			g.Add(len(topicNames))

			start := time.Now()

			for _, topicName := range topicNames {
				// search for everything all at once
				go func(topicName string) {
					defer g.Done()

					addrChan, err := routingDiscovery.FindPeers(ctx, topicName)
					if err != nil {
						s.logger.Errorf("[P2PNode] error finding peers: %+v", err)
					}

					for addr := range addrChan {
						if addr.ID == s.host.ID() {
							continue // No self connection
						}

						// no point trying to connect to a peer that is already connected
						if s.host.Network().Connectedness(addr.ID) == network.Connected {
							continue
						}

						// s.logger.Debugf("[P2PNode] found peer: %s, %+v", addr.ID.String(), addr.Addrs)

						if s.config.OptimiseRetries {
							if peerConnectionErrorString, ok := peerAddrErrorMap.Load(addr.ID.String()); ok {
								if strings.Contains(peerConnectionErrorString.(string), "no good addresses") {
									numAddresses := len(addr.Addrs)
									switch numAddresses {
									case 0:
										// peer has no addresses, no point trying to connect to it
										continue
									case 1:
										address := addr.Addrs[0].String()
										if strings.Contains(address, "127.0.0.1") {
											// Peer has a single localhost address and it failed on first attempt
											// You aren't allowed to dial 'yourself' and there are no other addresses available
											continue
										}
									}
								}

								if strings.Contains(peerConnectionErrorString.(string), "peer id mismatch") {
									// "peer id mismatch" is where the node has started using a new private key
									// No point trying to connect to it
									continue
								}
							}
						}

						peerAddr, loaded := peerAddrMap.LoadOrStore(addr.ID.String(), addr)

						if !loaded {
							/* A connection has a timeout of 5 seconds. Lets make parallel connect attempts rather than one at a time. */
							go func(addr peer.AddrInfo) {
								// A peer may not be available at the time of discovery.
								// A peer stays in the DHT for around 24 hours (according to ChatGPT) before it is removed from the peerstore
								// Logging each attempt to connect to these peers is too noisy
								err := s.host.Connect(ctx, addr)
								if err != nil {
									s.logger.Debugf("[P2PNode][%s] Connection failed : %+v", addr.String(), err)
									peerAddrErrorMap.Store(addr.ID.String(), err.Error())
								} else {
									s.logger.Infof("[P2PNode][%s] Connected in %s", addr.String(), time.Since(s.startTime))
								}
							}(peerAddr.(peer.AddrInfo))
						}
					}
				}(topicName)
			}

			g.Wait()

			s.logger.Debugf("[P2PNode] Completed discovery process in %v", time.Since(start))

			time.Sleep(5 * time.Second)
		}
	}
}

func (s *P2PNode) initDHT(ctx context.Context, h host.Host) (*dht.IpfsDHT, error) {
	// Start a DHT, for use in peer discovery. We can't just make a new DHT
	// client because we want each peer to maintain its own local copy of the
	// DHT, so that the bootstrapping node of the DHT can go down without
	// inhibiting future peer discovery.
	var options []dht.Option

	options = append(options, dht.Mode(dht.ModeAutoServer))

	kademliaDHT, err := dht.New(ctx, h, options...)
	if err != nil {
		return nil, errors.NewServiceError(errorCreatingDhtMessage, err)
	} else if err = kademliaDHT.Bootstrap(ctx); err != nil {
		return nil, errors.NewServiceError("[P2PNode] error bootstrapping DHT", err)
	}

	var wg sync.WaitGroup

	for _, peerAddr := range dht.DefaultBootstrapPeers {
		peerinfo, _ := peer.AddrInfoFromP2pAddr(peerAddr)

		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := h.Connect(ctx, *peerinfo); err != nil {
				fmt.Println("DHT Bootstrap warning:", err)
			}
		}()
	}

	wg.Wait()

	return kademliaDHT, nil
}

func (s *P2PNode) initPrivateDHT(ctx context.Context, host host.Host) (*dht.IpfsDHT, error) {
	bootstrapAddresses := s.settings.P2P.BootstrapAddresses
	s.logger.Infof("[P2PNode] bootstrapAddresses: %v", bootstrapAddresses)

	if len(bootstrapAddresses) == 0 {
		return nil, errors.NewServiceError("[P2PNode] bootstrapAddresses not set in config")
	}

	// Track if we successfully connected to at least one bootstrap address
	connectedToBootstrap := false

	for _, ba := range bootstrapAddresses {
		bootstrapAddr, err := multiaddr.NewMultiaddr(ba)
		if err != nil {
			s.logger.Warnf("[P2PNode] failed to create bootstrap multiaddress %s: %v", ba, err)
			continue // Try the next bootstrap address
		}

		peerInfo, err := peer.AddrInfoFromP2pAddr(bootstrapAddr)
		if err != nil {
			s.logger.Warnf("[P2PNode] failed to get peerInfo from %s: %v", ba, err)
			continue // Try the next bootstrap address
		}

		// get the IP from the multiaddress
		ip, err := getIPFromMultiaddr(peerInfo.Addrs[0])
		if err != nil {
			s.logger.Warnf("[P2PNode] failed to get IP from multiaddress %s: %v", ba, err)
			s.logger.Warnf("peerInfo: %+v\n", peerInfo)
		}

		s.logger.Infof("[P2PNode] bootstrap address %s has IP %s", ba, ip)

		err = host.Connect(ctx, *peerInfo)
		if err != nil {
			s.logger.Warnf("[P2PNode] failed to connect to bootstrap address %s: %v", ba, err)
			continue // Try the next bootstrap address
		}

		// Successfully connected to this bootstrap address
		connectedToBootstrap = true

		s.logger.Infof("[P2PNode] successfully connected to bootstrap address %s", ba)
	}

	// Only return an error if we couldn't connect to any bootstrap addresses
	if !connectedToBootstrap {
		return nil, errors.NewServiceError("[P2PNode] failed to connect to any bootstrap addresses")
	}

	dhtProtocolIDStr := s.settings.P2P.DHTProtocolID
	if dhtProtocolIDStr == "" {
		return nil, errors.NewServiceError("[P2PNode] error getting p2p_dht_protocol_id")
	}

	dhtProtocolID := protocol.ID(dhtProtocolIDStr)

	var options []dht.Option
	options = append(options, dht.ProtocolPrefix(dhtProtocolID))
	options = append(options, dht.Mode(dht.ModeAuto))

	kademliaDHT, err := dht.New(ctx, host, options...)
	if err != nil {
		return nil, errors.NewServiceError(errorCreatingDhtMessage, err)
	}

	err = kademliaDHT.Bootstrap(ctx)
	if err != nil {
		return nil, errors.NewServiceError("[P2PNode] error bootstrapping DHT", err)
	}

	return kademliaDHT, nil
}

func (s *P2PNode) streamHandler(ns network.Stream) {
	defer ns.Close()
	fmt.Printf("[%s] streamHandler\n", s.config.ProcessName)

	buf, err := io.ReadAll(ns)
	if err != nil {
		_ = ns.Reset()

		s.logger.Errorf("[P2PNode] failed to read network stream: %+v", err)

		fmt.Printf("[P2PNode] failed to read network stream: %+v", err)

		return
	}

	_ = ns.Close()

	atomic.AddUint64(&s.bytesReceived, uint64(len(buf)))

	if len(buf) > 0 {
		atomic.StoreInt64(&s.lastRecv, time.Now().Unix())
		s.logger.Debugf("[P2PNode] Received message: %s", string(buf))

		// try to decode it to a block message
		var blockMessage BlockMessage

		err := json.Unmarshal(buf, &blockMessage)
		if err != nil {
			s.logger.Errorf("[P2PNode] error unmarshalling block message: %v", err)
			return
		}

		// send block to kafka, if configured
		hash, err := chainhash.NewHashFromStr(blockMessage.Hash)
		if err != nil {
			s.logger.Errorf("[P2PNode] error parsing hash from string: %v", err)
			return
		}

		if s.blocksKafkaProducerClient != nil {
			value := make([]byte, 0, chainhash.HashSize+len(blockMessage.DataHubURL))
			value = append(value, hash.CloneBytes()...)
			value = append(value, []byte(blockMessage.DataHubURL)...)
			s.blocksKafkaProducerClient.Publish(&kafka.Message{
				Value: value,
			})
		}

		s.logger.Debugf("[P2PNode] Received block message: %+v", blockMessage)
	}
}

// LastSend returns the last send time of the peer.
//
// This function is safe for concurrent access.
func (s *P2PNode) LastSend() time.Time {
	return time.Unix(atomic.LoadInt64(&s.lastSend), 0)
}

// LastRecv returns the last recv time of the peer.
//
// This function is safe for concurrent access.
func (s *P2PNode) LastRecv() time.Time {
	return time.Unix(atomic.LoadInt64(&s.lastRecv), 0)
}

// BytesSent returns the total number of bytes sent by the peer.
//
// This function is safe for concurrent access.
func (s *P2PNode) BytesSent() uint64 {
	return atomic.LoadUint64(&s.bytesSent)
}

// BytesReceived returns the total number of bytes received by the peer.
//
// This function is safe for concurrent access.
func (s *P2PNode) BytesReceived() uint64 {
	return atomic.LoadUint64(&s.bytesReceived)
}

type PeerInfo struct {
	ID    peer.ID
	Addrs []multiaddr.Multiaddr
}

func (s *P2PNode) ConnectedPeers() []PeerInfo {
	// Get all peers from the peerstore
	peerIDs := s.host.Network().Peerstore().Peers()

	// Create a slice with zero initial length but with capacity for all peers
	peers := make([]PeerInfo, 0, len(peerIDs))

	// Add each peer to the slice
	for _, peerID := range peerIDs {
		peers = append(peers, PeerInfo{
			ID:    peerID,
			Addrs: s.host.Network().Peerstore().PeerInfo(peerID).Addrs,
		})
	}

	return peers
}

func (s *P2PNode) DisconnectPeer(ctx context.Context, peerID peer.ID) error {
	return s.host.Network().ClosePeer(peerID)
}

// TODO: remove

func getIPFromMultiaddr(addr multiaddr.Multiaddr) (string, error) {
	// First try to get DNS component
	if value, err := addr.ValueForProtocol(multiaddr.P_DNS4); err == nil {
		return value, nil
	}

	if value, err := addr.ValueForProtocol(multiaddr.P_DNS6); err == nil {
		return value, nil
	}

	// If no DNS, try IP
	if value, err := addr.ValueForProtocol(multiaddr.P_IP4); err == nil {
		return value, nil
	}

	if value, err := addr.ValueForProtocol(multiaddr.P_IP6); err == nil {
		return value, nil
	}

	return "", errors.New(errors.ERR_ERROR, "no IP or DNS component found in multiaddr")
}
