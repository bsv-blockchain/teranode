package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	alertP2P "github.com/bitcoin-sv/alert-system/app/p2p"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
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
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/sync/errgroup"
)

const (
	errorCreatingDhtMessage = "[P2PNode] error creating DHT"
	privateKeyKey           = "p2p.privateKey"
	multiAddrIPTemplate     = "/ip4/%s/tcp/%d"
)

// P2PNodeI defines the interface for P2P node functionality.
// This interface abstracts the concrete implementation to allow for better testability.
// It provides methods for managing the core peer-to-peer networking operations, including
// node lifecycle management, topic subscription, peer discovery, and message propagation.
//
// The interface is designed to be robust for both standard network operation and
// specialized testing scenarios. It encapsulates all libp2p functionality behind
// a clean API that integrates with the rest of the Teranode system.
type P2PNodeI interface {
	// Core lifecycle methods
	Start(ctx context.Context, streamHandler func(network.Stream), topicNames ...string) error
	Stop(ctx context.Context) error

	// Topic-related methods
	SetTopicHandler(ctx context.Context, topicName string, handler Handler) error
	GetTopic(topicName string) *pubsub.Topic
	Publish(ctx context.Context, topicName string, msgBytes []byte) error

	// Peer management methods
	HostID() peer.ID
	ConnectedPeers() []PeerInfo
	CurrentlyConnectedPeers() []PeerInfo
	ConnectToPeer(ctx context.Context, peerAddr string) error
	DisconnectPeer(ctx context.Context, peerID peer.ID) error
	SendToPeer(ctx context.Context, pid peer.ID, msg []byte) error
	SetPeerConnectedCallback(callback func(context.Context, peer.ID))

	UpdatePeerHeight(peerID peer.ID, height int32)
	SetPeerStartingHeight(peerID peer.ID, height int32)
	GetPeerStartingHeight(peerID peer.ID) (int32, bool)

	// Stats methods
	LastSend() time.Time
	LastRecv() time.Time
	BytesSent() uint64
	BytesReceived() uint64

	// Additional accessors needed by Server
	GetProcessName() string
	UpdateBytesReceived(bytesCount uint64)
	UpdateLastReceived()

	GetPeerIPs(peerID peer.ID) []string
}

// P2PNode implements the P2PNodeI interface and provides the core functionality
// for peer-to-peer networking in Teranode using libp2p.
// It manages peer connections, topic subscriptions, message routing, and network discovery.
//
// The P2PNode encapsulates several critical components:
// - libp2p host for network transport and connection management
// - PubSub for topic-based message distribution
// - Peer height tracking for blockchain synchronization
// - Bandwidth and activity metrics for monitoring
//
// Thread safety is maintained for all concurrent operations across multiple goroutines.
type P2PNode struct {
	config            P2PConfig                      // Configuration parameters for the node
	settings          *settings.Settings             // Global settings for the Teranode system
	host              host.Host                      // libp2p host for network communication
	pubSub            *pubsub.PubSub                 // Publish-subscribe system for topic-based messaging
	topics            map[string]*pubsub.Topic       // Map of topic names to topic objects
	logger            ulogger.Logger                 // Logger for P2P operations
	bitcoinProtocolID string                         // Protocol identifier for Bitcoin-specific streams
	handlerByTopic    map[string]Handler             // Map of topic handlers for message processing
	startTime         time.Time                      // Time when the node was started
	onPeerConnected   func(context.Context, peer.ID) // Callback for peer connection events
	callbackMutex     sync.RWMutex                   // Mutex for thread-safe callback access

	// IMPORTANT: The following variables must only be used atomically.
	bytesReceived       uint64   // Counter for bytes received over the network
	bytesSent           uint64   // Counter for bytes sent over the network
	lastRecv            int64    // Timestamp of last message received
	lastSend            int64    // Timestamp of last message sent
	peerHeights         sync.Map // Thread-safe map tracking peer blockchain heights
	peerStartingHeights sync.Map // Thread-safe map tracking peer starting heights when first connected
	peerConnTimes       sync.Map // Thread-safe map tracking peer connection times (peer.ID -> time.Time)
}

// Handler defines the function signature for topic message handlers.
// Each topic in the P2P network can have a dedicated handler that processes incoming messages.
//
// Parameters:
// - ctx: Context for the handler execution, allowing for cancellation and timeouts
// - msg: Raw message bytes received from the network
// - from: Identifier of the peer that sent the message
//
// Handlers should process messages efficiently as they may be called frequently
// in high-traffic scenarios. Any long-running operations should be delegated to separate goroutines.
type Handler func(ctx context.Context, msg []byte, from string)

// P2PConfig defines the configuration parameters for a P2P node.
// It encapsulates all the settings needed to establish and maintain
// a functional peer-to-peer network presence.
type P2PConfig struct {
	ProcessName        string   // Identifier for this node in logs and metrics
	ListenAddresses    []string // Network addresses to listen on for incoming connections
	AdvertiseAddresses []string // Addresses to advertise to other peers (may differ from listen addresses)
	Port               int      // Port number for P2P communication
	PrivateKey         string   // Node's private key for secure communication
	SharedKey          string   // Shared key for private network communication
	UsePrivateDHT      bool     // Whether to use a private DHT instead of the public IPFS DHT
	OptimiseRetries    bool     // Whether to optimize connection retry behavior
	Advertise          bool     // Whether to advertise this node's presence on the network
	StaticPeers        []string // List of peer addresses to always attempt to connect to
}

// NewP2PNode creates and initializes a new P2P network node with the provided configuration.
// This constructor performs the core setup of the libp2p networking stack, including:
// - Setting up the node's cryptographic identity (private key)
// - Configuring network transports and listeners
// - Initializing the DHT (Distributed Hash Table) for peer discovery
// - Preparing topic handlers and message routing systems
//
// Parameters:
// - ctx: Context for controlling the initialization process
// - logger: Logger for recording initialization and operational events
// - tSettings: Global Teranode settings with additional configuration parameters
// - config: P2P-specific configuration parameters defining network behavior
// - blockchainClient: Client for blockchain operations and service integration
//
// Returns a fully initialized P2P node ready for starting, or an error if initialization fails.
func NewP2PNode(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, config P2PConfig, blockchainClient blockchain.ClientI) (*P2PNode, error) {
	logger.Infof("[P2PNode] Creating node")

	var (
		pk  *crypto.PrivKey // Private key for the node
		err error
	)

	// If no private key is provided in the configuration, attempt to read or generate one
	if config.PrivateKey == "" {
		pk, err = readPrivateKey(ctx, blockchainClient)
		if err != nil {
			// If reading fails, attempt to generate a new private key
			pk, err = generatePrivateKey(ctx, blockchainClient)
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
		listenMultiAddresses := []string{}
		for _, addr := range config.ListenAddresses {
			listenMultiAddresses = append(listenMultiAddresses, fmt.Sprintf("/ip4/%s/tcp/%d", addr, config.Port))
		}

		opts := []libp2p.Option{
			libp2p.ListenAddrStrings(listenMultiAddresses...),
			libp2p.Identity(*pk),
		}

		// If advertise addresses are specified, add them to the options
		addrsToAdvertise := buildAdvertiseMultiAddrs(config.AdvertiseAddresses, config.Port)
		if len(addrsToAdvertise) > 0 {
			opts = append(opts, libp2p.AddrsFactory(func(_ []multiaddr.Multiaddr) []multiaddr.Multiaddr {
				return addrsToAdvertise
			}))
		} else {
			// User has not specified any broadcast addresses in their config, and we are not using a private DHT
			// define address factory to remove all private IPs from being advertised
			opts = append(opts, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
				var publicAddrs []multiaddr.Multiaddr

				for _, addr := range addrs {
					// if IP is not private add it to the list
					if !isPrivateIP(addr) {
						publicAddrs = append(publicAddrs, addr)
					}
				}

				// If we still don't have any advertisable addresses then attempt to grab it from `https://ifconfig.me/ip`
				if len(publicAddrs) > 0 {
					return publicAddrs
				}

				// If no public addresses are set, let's attempt to grab it publicly
				// Ignore errors because we don't care if we can't find it
				ifconfig, err := alertP2P.GetPublicIP(context.Background())
				if err != nil {
					logger.Debugf("[P2PNode] error getting public IP: %v", err)
				}

				if len(ifconfig) == 0 {
					return publicAddrs
				}

				addr, err := multiaddr.NewMultiaddr(fmt.Sprintf(multiAddrIPTemplate, ifconfig, config.Port))
				if err != nil {
					logger.Debugf("[P2PNode] error creating public multiaddr: %v", err)
				}

				if addr != nil {
					publicAddrs = append(publicAddrs, addr)
				}

				return publicAddrs
			}))
		}

		h, err = libp2p.New(opts...)
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
		config:            config,
		logger:            logger,
		settings:          tSettings,
		host:              h,
		bitcoinProtocolID: "teranode/bitcoin/1.0.0",
		handlerByTopic:    make(map[string]Handler),
		startTime:         time.Now(),
		peerHeights:       sync.Map{},
		peerConnTimes:     sync.Map{},
	}

	// Set up connection notifications
	h.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(n network.Network, conn network.Conn) {
			peerID := conn.RemotePeer()
			node.logger.Debugf("[P2PNode] Peer connected: %s", peerID.String())

			// Store connection time
			node.peerConnTimes.Store(peerID, time.Now())

			// Notify any connection handlers about the new peer
			node.callPeerConnected(context.Background(), peerID)
		},
		DisconnectedF: func(n network.Network, conn network.Conn) {
			peerID := conn.RemotePeer()
			node.logger.Debugf("[P2PNode] Peer disconnected: %s", peerID.String())

			// Remove connection time when peer disconnects
			node.peerConnTimes.Delete(peerID)
		},
	})

	return node, nil
}

// setUpPrivateNetwork creates a libp2p host configured for a private network using a pre-shared key.
// This function establishes a secure, isolated P2P network that only allows connections between
// nodes that possess the same shared key. It's used for creating private blockchain networks
// or testing environments where public network access should be restricted.
//
// The function constructs a pre-shared key (PSK) from the provided shared key string and
// configures the libp2p host to use this PSK for all network communications. Only peers
// with the matching PSK can establish connections and participate in the network.
//
// Parameters:
//   - config: P2P configuration containing the shared key and other network parameters
//   - pk: Private key for the node's cryptographic identity
//
// Returns:
//   - A configured libp2p host ready for private network operation
//   - Error if the shared key is invalid or host creation fails
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

	listenMultiAddresses := []string{}
	for _, addr := range config.ListenAddresses {
		listenMultiAddresses = append(listenMultiAddresses, fmt.Sprintf(multiAddrIPTemplate, addr, config.Port))
	}

	// Set up libp2p options
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(listenMultiAddresses...),
		libp2p.Identity(*pk),
		libp2p.PrivateNetwork(psk),
	}

	// If advertise addresses are specified, add them to the options
	addrsToAdvertise := buildAdvertiseMultiAddrs(config.AdvertiseAddresses, config.Port)
	if len(addrsToAdvertise) > 0 {
		opts = append(opts, libp2p.AddrsFactory(func(_ []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			return addrsToAdvertise
		}))
	}

	h, err = libp2p.New(opts...)
	if err != nil {
		return nil, errors.NewServiceError("[P2PNode] error creating libp2p node", err)
	}

	return h, nil
}

// buildAdvertiseMultiAddrs constructs multiaddrs from host strings with optional ports.
// logs warnings via fmt.Printf.
func buildAdvertiseMultiAddrs(addrs []string, defaultPort int) []multiaddr.Multiaddr {
	result := make([]multiaddr.Multiaddr, 0, len(addrs))

	for _, addr := range addrs {
		hostStr := addr
		portNum := defaultPort

		if h, p, err := net.SplitHostPort(addr); err == nil {
			hostStr = h

			if pi, err2 := strconv.Atoi(p); err2 != nil {
				fmt.Printf("[P2PNode] invalid port in advertise address: %s, error: %v\n", addr, err2)
				continue
			} else {
				portNum = pi
			}
		}

		var maddr multiaddr.Multiaddr

		var err error

		if net.ParseIP(hostStr) != nil {
			maddr, err = multiaddr.NewMultiaddr(fmt.Sprintf(multiAddrIPTemplate, hostStr, portNum))
		} else {
			maddr, err = multiaddr.NewMultiaddr(fmt.Sprintf("/dns4/%s/tcp/%d", hostStr, portNum))
		}

		if err != nil {
			fmt.Printf("[P2PNode] invalid advertise address: %s, error: %v\n", addr, err)
			continue
		}

		result = append(result, maddr)
	}

	return result
}

func (s *P2PNode) startStaticPeerConnector(ctx context.Context) {
	if len(s.config.StaticPeers) == 0 {
		s.logger.Infof("[P2PNode] no static peers to connect to - skipping connection attempt")
		return
	}

	go func() {
		logged := false

		delay := 0 * time.Second

		for {
			// Use a ticker with context to handle cancellation during sleep
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
				// Timer completed, continue as normal
			case <-ctx.Done():
				// Context was canceled during wait, clean up and return
				if !timer.Stop() {
					<-timer.C
				}

				s.logger.Infof("[P2PNode] shutting down")

				return
			}

			allConnected := s.connectToStaticPeers(ctx, s.config.StaticPeers)

			select {
			case <-ctx.Done():
				return
			default:
			}

			if allConnected {
				if !logged {
					s.logger.Infof("[P2PNode] all static peers connected")
				}

				logged = true
				delay = 30 * time.Second // it is possible that a peer disconnects, so we need to keep checking
			} else {
				s.logger.Infof("[P2PNode] all static peers NOT connected")

				logged = false
				delay = 5 * time.Second
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

// Start activates the P2P node and begins network operations.
// This method initializes peer discovery, topic subscriptions, and stream handlers.
// It performs several key operations:
// - Launches static peer connector to maintain connections with configured static peers
// - Starts peer discovery in a background goroutine to find and connect to network peers
// - Initializes the GossipSub protocol for pub/sub messaging
// - Sets up stream handlers for direct peer-to-peer communication
//
// Parameters:
// - ctx: Context for controlling the start process and subsequent operations
// - streamHandler: Handler for incoming protocol streams (can be nil if not using direct streams)
// - topicNames: List of topic names to subscribe to for pub/sub messaging
//
// The method is non-blocking for peer discovery but waits for GossipSub initialization to complete.
// Returns an error if any critical component fails to initialize.
func (s *P2PNode) Start(ctx context.Context, streamHandler func(network.Stream), topicNames ...string) error {
	s.logger.Infof("[%s] starting", s.config.ProcessName)

	s.startStaticPeerConnector(ctx)

	go func() {
		if err := s.discoverPeers(ctx, topicNames); err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Warnf("[P2PNode] error discovering peers: %v", err)
		}
	}()

	if err := s.initGossipSub(ctx, topicNames); err != nil {
		return err
	}

	if streamHandler != nil {
		s.host.SetStreamHandler(protocol.ID(s.bitcoinProtocolID), streamHandler)
	}

	return nil
}

// subscribeToTopics joins the P2P node to multiple pubsub topics for message distribution.
// This function iterates through the provided topic names and joins each one using the
// libp2p pubsub system. It creates a mapping of topic names to topic objects that can
// be used for publishing and subscribing to messages.
//
// The function is used during node initialization to establish participation in the
// various communication channels used by the Teranode network, such as block propagation,
// transaction distribution, and control message topics.
//
// Parameters:
//   - topicNames: Array of topic names to join
//   - ps: The pubsub instance to use for joining topics
//   - s: The P2P node instance for logging purposes
//
// Returns:
//   - Map of topic names to topic objects for successful subscriptions
//   - Boolean indicating if an error occurred (true if error, false if success)
//   - Error if any topic join operation fails
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

	// Close the underlying libp2p host
	if s.host != nil {
		if err := s.host.Close(); err != nil {
			s.logger.Errorf("[P2PNode] error closing host: %v", err)
			return err // Return the error if closing fails
		}

		s.logger.Infof("[P2PNode] host closed")
	}

	return nil
}

func (s *P2PNode) SetTopicHandler(ctx context.Context, topicName string, handler Handler) error {
	_, ok := s.handlerByTopic[topicName]
	if ok {
		return errors.NewServiceError("[P2PNode][SetTopicHandler] handler already exists for topic: %s", topicName)
	}

	topic := s.topics[topicName]
	if topic == nil {
		return errors.NewServiceError("[P2PNode][SetTopicHandler] topic not found: %s", topicName)
	}

	sub, err := topic.Subscribe()
	if err != nil {
		return err
	}

	s.handlerByTopic[topicName] = handler
	s.logger.Infof("[P2PNode][SetTopicHandler] Successfully subscribed to topic: %s", topicName)

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
					if !errors.Is(err, context.Canceled) {
						s.logger.Errorf("[P2PNode][SetTopicHandler] error getting msg from %s topic: %v", topicName, err)
					}

					continue
				}

				if strings.Contains(*m.Message.Topic, "handshake") {
					s.logger.Infof("[P2PNode][SetTopicHandler]: topic: %s - from: %s - self: %s - message: %s\n", *m.Message.Topic, m.ReceivedFrom.ShortString(), s.host.ID().ShortString(), strings.TrimSpace(string(m.Message.Data)))
				} else {
					s.logger.Debugf("[P2PNode][SetTopicHandler]: topic: %s - from: %s - message: %s\n", *m.Message.Topic, m.ReceivedFrom.ShortString(), strings.TrimSpace(string(m.Message.Data)))
				}
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
	// check listen mode - if listen_only, don't publish outbound messages except handshakes
	if s.settings.P2P.ListenMode == settings.ListenModeListenOnly {
		// allow handshake messages even in listen_only mode as they're essential for peer discovery
		if !strings.Contains(topicName, "handshake") {
			s.logger.Debugf("[P2PNode][Publish] skipping publish in listen_only mode for topic: %s", topicName)
			return nil
		} else {
			s.logger.Debugf("[P2PNode][Publish] allowing handshake message in listen_only mode for topic: %s", topicName)
		}
	}

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
func (s *P2PNode) SendToPeer(ctx context.Context, peerID peer.ID, msg []byte) (err error) {
	// check listen mode - if listen_only, don't send direct messages except handshakes
	if s.settings.P2P.ListenMode == settings.ListenModeListenOnly {
		// allow handshake messages even in listen_only mode
		if !strings.Contains(string(msg), "\"type\":\"version\"") && !strings.Contains(string(msg), "\"type\":\"verack\"") {
			s.logger.Debugf("[P2PNode][SendToPeer] skipping send in listen_only mode to peer: %s", peerID.String())
			return nil
		}
		s.logger.Debugf("[P2PNode][SendToPeer] allowing handshake message in listen_only mode to peer: %s", peerID.String())
	}

	h2pi := s.host.Peerstore().PeerInfo(peerID)
	s.logger.Infof("[P2PNode][SendToPeer] dialing %s", h2pi.Addrs)

	if err = s.host.Connect(ctx, h2pi); err != nil {
		s.logger.Errorf("[P2PNode][SendToPeer] failed to connect: %+v", err)
		return err
	}

	var st network.Stream

	st, err = s.host.NewStream(
		ctx,
		peerID,
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

	s.logger.Debugf("[P2PNode][SendToPeer] sent %v bytes to %s", strings.TrimSpace(string(msg)), peerID.String())
	// Increment bytesSent using atomic operations
	atomic.AddUint64(&s.bytesSent, uint64(len(msg)))

	// Update lastSend timestamp
	atomic.StoreInt64(&s.lastSend, time.Now().Unix())

	return nil
}

// generatePrivateKey creates a new Ed25519 private key for P2P node identity and stores it persistently.
// This function generates a cryptographically secure key pair using the Ed25519 algorithm,
// which provides the node's unique identity in the P2P network. The private key is stored
// in the blockchain client's state store for persistence across node restarts.
//
// The generated key serves as the node's cryptographic identity for:
//   - Peer authentication and verification
//   - Message signing and validation
//   - Secure communication establishment
//
// Parameters:
//   - ctx: Context for the operation, used for state store operations
//   - blockchainClient: Client for persistent key storage (can be nil for testing)
//
// Returns:
//   - Pointer to the generated private key ready for libp2p use
//   - Error if key generation or storage fails
func generatePrivateKey(ctx context.Context, blockchainClient blockchain.ClientI) (*crypto.PrivKey, error) {
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

	if blockchainClient != nil {
		// save the private key to the state store
		if err := blockchainClient.SetState(ctx, privateKeyKey, privBytes); err != nil {
			return nil, err
		}
	}

	return &priv, nil
}

// readPrivateKey retrieves a previously stored Ed25519 private key from the blockchain state store.
// This function loads the node's persistent cryptographic identity from storage, allowing
// the node to maintain the same peer ID across restarts. If no key is found in storage,
// it indicates this is a fresh node that needs key generation.
//
// The function attempts to read the key from the blockchain client's state store using
// a predefined key identifier. The retrieved key bytes are unmarshaled back into a
// usable private key object for libp2p operations.
//
// Parameters:
//   - ctx: Context for the operation, used for state store operations
//   - blockchainClient: Client for accessing persistent key storage
//
// Returns:
//   - Pointer to the retrieved private key, or nil if no key exists
//   - Error if storage access fails or key unmarshaling fails
func readPrivateKey(ctx context.Context, blockchainClient blockchain.ClientI) (*crypto.PrivKey, error) {
	// Read private key from the state store
	if blockchainClient == nil {
		return nil, errors.NewServiceError("error reading private key", nil)
	}

	privBytes, err := blockchainClient.GetState(ctx, privateKeyKey)
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
		select {
		case <-ctx.Done():
			return false
		default:
		}

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

	// Log peer store info
	peerCount := len(s.host.Peerstore().Peers())
	s.logger.Debugf("[P2PNode] %d peers in peerstore", peerCount)

	// Use simultaneous connect for hole punching
	ctx = network.WithSimultaneousConnect(ctx, true, "hole punching")
	peerAddrErrorMap := sync.Map{}

	// Look for others who have announced and attempt to connect to them
	for {
		select {
		case <-ctx.Done():
			// Exit immediately if context is done
			s.logger.Infof("[P2PNode] shutting down")
			return nil
		default:
			// Create a copy of the map to avoid concurrent modifications
			peerAddrMap := sync.Map{}

			eg := errgroup.Group{}

			start := time.Now()

			// Start all peer finding goroutines
			for _, topicName := range topicNames {
				// We need to create a copy of the topic name for each goroutine
				// to avoid data races on the loop variable
				topicNameCopy := topicName

				eg.Go(func() error {
					return s.findPeers(ctx, topicNameCopy, routingDiscovery, &peerAddrMap, &peerAddrErrorMap)
				})
			}

			if err := eg.Wait(); err != nil {
				return err
			}

			duration := time.Since(start)
			if duration > 0 { // Avoid logging negative durations due to clock skew
				s.logger.Debugf("[P2PNode] Completed discovery process in %v", duration)
			}

			// Using a timer with context to handle cancellation during sleep
			sleepTimer := time.NewTimer(5 * time.Second)
			select {
			case <-sleepTimer.C:
				// Timer completed normally, continue the loop
			case <-ctx.Done():
				// Context was canceled, clean up and return
				if !sleepTimer.Stop() {
					select {
					case <-sleepTimer.C:
					default:
					}
				}

				return ctx.Err()
			}
		}
	}
}

func (s *P2PNode) findPeers(ctx context.Context, topicName string, routingDiscovery *dRouting.RoutingDiscovery, peerAddrMap *sync.Map, peerAddrErrorMap *sync.Map) error {
	// Find peers subscribed to the topic
	addrChan, err := routingDiscovery.FindPeers(ctx, topicName)
	if err != nil {
		s.logger.Errorf("[P2PNode] error finding peers: %+v", err)

		return err
	}

	wg := &sync.WaitGroup{}

	// Process each peer address discovered
	for addr := range addrChan {
		// Check if context is done before processing each peer
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Skip peers we shouldn't connect to
		if s.shouldSkipPeer(addr, peerAddrErrorMap) {
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()
			s.attemptConnection(ctx, addr, peerAddrMap, peerAddrErrorMap)
		}()
	}

	wg.Wait()

	return nil
}

// shouldSkipPeer determines if a peer should be skipped based on filtering criteria
func (s *P2PNode) shouldSkipPeer(addr peer.AddrInfo, peerAddrErrorMap *sync.Map) bool {
	// Skip self connection
	if addr.ID == s.host.ID() {
		return true
	}

	// Skip already connected peers
	if s.host.Network().Connectedness(addr.ID) == network.Connected {
		return true
	}

	// Skip peers with no addresses
	if len(addr.Addrs) == 0 {
		return true
	}

	// Skip based on previous errors if optimizing retries
	if s.config.OptimiseRetries {
		return s.shouldSkipBasedOnErrors(addr, peerAddrErrorMap)
	}

	return false
}

// shouldSkipBasedOnErrors determines if a peer should be skipped based on previous errors
func (s *P2PNode) shouldSkipBasedOnErrors(addr peer.AddrInfo, peerAddrErrorMap *sync.Map) bool {
	peerConnectionErrorString, ok := peerAddrErrorMap.Load(addr.ID.String())
	if !ok {
		return false
	}

	errorStr := peerConnectionErrorString.(string)

	// Check for "no good addresses" error
	if strings.Contains(errorStr, "no good addresses") {
		return s.shouldSkipNoGoodAddresses(addr)
	}

	// Check for "peer id mismatch" error
	if strings.Contains(errorStr, "peer id mismatch") {
		// "peer id mismatch" is where the node has started using a new private key
		// No point trying to connect to it
		return true
	}

	return false
}

// shouldSkipNoGoodAddresses determines if a peer with "no good addresses" error should be skipped
func (s *P2PNode) shouldSkipNoGoodAddresses(addr peer.AddrInfo) bool {
	numAddresses := len(addr.Addrs)

	switch numAddresses {
	case 0:
		// peer has no addresses, no point trying to connect to it
		return true
	case 1:
		address := addr.Addrs[0].String()
		if strings.Contains(address, "127.0.0.1") {
			// Peer has a single localhost address and it failed on first attempt
			// You aren't allowed to dial 'yourself' and there are no other addresses available
			return true
		}
	}

	return false
}

// attemptConnection tries to connect to a peer if it hasn't been attempted already
func (s *P2PNode) attemptConnection(ctx context.Context, peerAddr peer.AddrInfo, peerAddrMap *sync.Map, peerAddrErrorMap *sync.Map) {
	if _, ok := peerAddrMap.Load(peerAddr.ID.String()); ok {
		return
	}

	peerAddrMap.Store(peerAddr.ID.String(), true)

	err := s.host.Connect(ctx, peerAddr)
	if err != nil {
		peerAddrErrorMap.Store(peerAddr.ID.String(), true)
		s.logger.Debugf("[P2PNode][%s] Failed to connect: %v", peerAddr.String(), err)
	} else {
		s.logger.Infof("[P2PNode][%s] Connected in %s", peerAddr.String(), time.Since(s.startTime))
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

	// Create a context with timeout to ensure bootstrap connections don't hang
	connectCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	// Create a synchronization channel for handling connection errors
	errorChan := make(chan error, len(dht.DefaultBootstrapPeers))

	for _, peerAddr := range dht.DefaultBootstrapPeers {
		peerinfo, _ := peer.AddrInfoFromP2pAddr(peerAddr)

		wg.Add(1)

		go func(pi *peer.AddrInfo) {
			defer wg.Done()

			if err := h.Connect(connectCtx, *pi); err != nil {
				errorChan <- err
			}
		}(peerinfo)
	}

	// Launch a separate goroutine to collect and log errors
	var wgLogging sync.WaitGroup

	wgLogging.Add(1)

	// Create a done channel to signal when to stop receiving from errorChan
	doneChan := make(chan struct{})

	go func() {
		defer wgLogging.Done()

		for {
			select {
			case err, ok := <-errorChan:
				if !ok {
					// Channel closed, exit
					return
				}
				// Check context before logging
				select {
				case <-ctx.Done():
					// Context canceled, stop logging
					return
				default:
					s.logger.Debugf("DHT Bootstrap warning: %v", err)
				}
			case <-ctx.Done():
				// Context canceled, stop logging
				return
			case <-doneChan:
				// Signal to stop, exit
				return
			}
		}
	}()

	// Wait for all connection attempts to complete
	wg.Wait()

	// Signal the logging goroutine to exit and close the error channel
	close(doneChan)
	close(errorChan)

	// Wait for logging to complete
	wgLogging.Wait()

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

func (s *P2PNode) LastSend() time.Time {
	return time.Unix(atomic.LoadInt64(&s.lastSend), 0)
}

func (s *P2PNode) LastRecv() time.Time {
	return time.Unix(atomic.LoadInt64(&s.lastRecv), 0)
}

func (s *P2PNode) BytesSent() uint64 {
	return atomic.LoadUint64(&s.bytesSent)
}

func (s *P2PNode) BytesReceived() uint64 {
	return atomic.LoadUint64(&s.bytesReceived)
}

type PeerInfo struct {
	ID             peer.ID
	Addrs          []multiaddr.Multiaddr
	CurrentHeight  int32
	StartingHeight int32      // Height when peer first connected
	ConnTime       *time.Time // Connection time (nil if not connected)
}

func (s *P2PNode) ConnectedPeers() []PeerInfo {
	// Get all connected peers from the network
	peerIDs := s.host.Network().Peerstore().Peers()

	// Create a slice with zero initial length but with capacity for all peers
	peers := make([]PeerInfo, 0, len(peerIDs))

	// Add each peer to the slice
	for _, peerID := range peerIDs {
		var height int32
		if h, ok := s.peerHeights.Load(peerID); ok {
			height = h.(int32)
		}

		var startingHeight int32
		if sh, ok := s.peerStartingHeights.Load(peerID); ok {
			startingHeight = sh.(int32)
		}

		var connTime *time.Time
		if ct, ok := s.peerConnTimes.Load(peerID); ok {
			t := ct.(time.Time)
			connTime = &t
		}

		peers = append(peers, PeerInfo{
			ID:             peerID,
			Addrs:          s.host.Network().Peerstore().PeerInfo(peerID).Addrs,
			CurrentHeight:  height,
			StartingHeight: startingHeight,
			ConnTime:       connTime,
		})
	}

	s.logger.Debugf("[P2PNode] %d peers in peerstore\n", len(peers))

	return peers
}

func (s *P2PNode) CurrentlyConnectedPeers() []PeerInfo {
	// Get all connected peers from the network
	peerIDs := s.host.Network().Peers()

	// Create a slice with zero initial length but with capacity for all peers
	peers := make([]PeerInfo, 0, len(peerIDs))

	// Add each peer to the slice
	for _, peerID := range peerIDs {
		var height int32
		if h, ok := s.peerHeights.Load(peerID); ok {
			height = h.(int32)
		}

		var startingHeight int32
		if sh, ok := s.peerStartingHeights.Load(peerID); ok {
			startingHeight = sh.(int32)
		}

		var connTime *time.Time
		if ct, ok := s.peerConnTimes.Load(peerID); ok {
			t := ct.(time.Time)
			connTime = &t
		}

		peers = append(peers, PeerInfo{
			ID:             peerID,
			Addrs:          s.host.Network().Peerstore().PeerInfo(peerID).Addrs,
			CurrentHeight:  height,
			StartingHeight: startingHeight,
			ConnTime:       connTime,
		})
	}

	return peers
}

func (s *P2PNode) DisconnectPeer(ctx context.Context, peerID peer.ID) error {
	// Close all connections to this peer to ensure disconnection events are triggered
	conns := s.host.Network().ConnsToPeer(peerID)
	for _, conn := range conns {
		err := conn.Close()
		if err != nil {
			s.logger.Debugf("[P2PNode] Error closing connection to %s: %v", peerID.String(), err)
		}
	}

	// Clean up connection time immediately as a fallback
	s.peerConnTimes.Delete(peerID)

	return s.host.Network().ClosePeer(peerID)
}

func (s *P2PNode) UpdatePeerHeight(peerID peer.ID, height int32) {
	s.logger.Debugf("[P2PNode] UpdatePeerHeight: %s %d\n", peerID.String(), height)

	// if this is the first height we see from this peer, use it as starting height
	if _, exists := s.peerStartingHeights.Load(peerID); !exists && height > 0 {
		s.logger.Infof("[P2PNode] Setting starting height for peer %s to %d (first height update)", peerID.String(), height)
		s.peerStartingHeights.Store(peerID, height)
	}

	s.peerHeights.Store(peerID, height)
}

// SetPeerStartingHeight stores the starting height for a peer (height when first connected)
func (s *P2PNode) SetPeerStartingHeight(peerID peer.ID, height int32) {
	s.logger.Infof("[P2PNode] SetPeerStartingHeight: %s %d", peerID.String(), height)
	s.peerStartingHeights.Store(peerID, height)
}

// GetPeerStartingHeight retrieves the starting height for a peer
func (s *P2PNode) GetPeerStartingHeight(peerID peer.ID) (int32, bool) {
	if height, exists := s.peerStartingHeights.Load(peerID); exists {
		if h, ok := height.(int32); ok {
			return h, true
		}
	}
	return 0, false
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

func (s *P2PNode) GetProcessName() string {
	return s.config.ProcessName
}

func (s *P2PNode) UpdateBytesReceived(bytesCount uint64) {
	atomic.AddUint64(&s.bytesReceived, bytesCount)
}

func (s *P2PNode) UpdateLastReceived() {
	atomic.StoreInt64(&s.lastRecv, time.Now().Unix())
}

func (s *P2PNode) GetPeerIPs(peerID peer.ID) []string {
	addrs := s.host.Network().Peerstore().PeerInfo(peerID).Addrs
	ips := make([]string, 0, len(addrs))

	for _, addr := range addrs {
		ip := extractIPFromMultiaddr(addr)
		if ip != "" {
			ips = append(ips, ip)
		}
	}

	return ips
}

// SetPeerConnectedCallback sets a callback function to be called when a new peer connects
func (s *P2PNode) SetPeerConnectedCallback(callback func(context.Context, peer.ID)) {
	s.callbackMutex.Lock()
	defer s.callbackMutex.Unlock()
	s.onPeerConnected = callback
}

// callPeerConnected safely calls the peer connected callback if it exists
func (s *P2PNode) callPeerConnected(ctx context.Context, peerID peer.ID) {
	s.callbackMutex.RLock()
	callback := s.onPeerConnected
	s.callbackMutex.RUnlock()

	if callback != nil {
		go callback(ctx, peerID)
	}
}

func extractIPFromMultiaddr(multiaddr multiaddr.Multiaddr) string {
	str := multiaddr.String()

	parts := strings.Split(str, "/")
	for i, part := range parts {
		if part == "ip4" || part == "ip6" {
			if i+1 < len(parts) {
				return parts[i+1]
			}
		}
	}

	return ""
}

// Function to check if an IP address is private
func isPrivateIP(addr multiaddr.Multiaddr) bool {
	ipStr := extractIPFromMultiaddr(addr)
	if ipStr == "" {
		return false
	}

	ip := net.ParseIP(ipStr)
	if ip == nil || ip.To4() == nil {
		return false
	}

	// Define private IPv4 ranges according to RFC 1918 and RFC 3927
	// These are standard private network ranges and are safe to use
	privateRanges := []*net.IPNet{
		{IP: net.ParseIP("10.0.0.0"), Mask: net.CIDRMask(8, 32)},     // RFC 1918: Class A private network
		{IP: net.ParseIP("172.16.0.0"), Mask: net.CIDRMask(12, 32)},  // RFC 1918: Class B private network
		{IP: net.ParseIP("192.168.0.0"), Mask: net.CIDRMask(16, 32)}, // RFC 1918: Class C private network
		{IP: net.ParseIP("127.0.0.0"), Mask: net.CIDRMask(8, 32)},    // RFC 3927: Loopback addresses
	}

	// Check if the IP falls into any of the private ranges
	for _, r := range privateRanges {
		if r.Contains(ip) {
			return true
		}
	}

	return false
}

// ConnectToPeer connects to a specific peer using the provided multiaddr string
func (s *P2PNode) ConnectToPeer(ctx context.Context, peerAddr string) error {
	// Parse the multiaddr string
	maddr, err := multiaddr.NewMultiaddr(peerAddr)
	if err != nil {
		return errors.NewServiceError("[P2PNode][ConnectToPeer] invalid multiaddr: %v", err)
	}

	// Extract peer info from the multiaddr
	peerInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return errors.NewServiceError("[P2PNode][ConnectToPeer] failed to get peer info: %v", err)
	}

	// Connect to the peer
	s.logger.Infof("[P2PNode][ConnectToPeer] connecting to peer %s at %s", peerInfo.ID, peerAddr)
	if err := s.host.Connect(ctx, *peerInfo); err != nil {
		return errors.NewServiceError("[P2PNode][ConnectToPeer] failed to connect to peer %s: %v", peerInfo.ID, err)
	}

	return nil
}
