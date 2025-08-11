# P2P Server Reference Documentation

## Overview

The P2P Server facilitates peer-to-peer communication within the Bitcoin SV network, managing the distribution of blocks, transactions, and network-related data. The server integrates with blockchain services, validation systems, and Kafka messaging to ensure efficient data propagation across the network. It implements both WebSocket and HTTP interfaces for peer communication while maintaining secure, scalable connections.

## Core Components

### Server Structure

The P2P Server is implemented through the Server struct, which coordinates all peer-to-peer communication:

```go
type Server struct {
    p2p_api.UnimplementedPeerServiceServer
    P2PNode                           P2PNodeI           // The P2P network node instance - using interface instead of concrete type
    logger                            ulogger.Logger     // Logger instance for the server
    settings                          *settings.Settings // Configuration settings
    bitcoinProtocolID                 string             // Bitcoin protocol identifier
    blockchainClient                  blockchain.ClientI // Client for blockchain interactions
    blockValidationClient             blockvalidation.Interface
    AssetHTTPAddressURL               string                    // HTTP address URL for assets
    e                                 *echo.Echo                // Echo server instance
    notificationCh                    chan *notificationMsg     // Channel for notifications
    rejectedTxKafkaConsumerClient     kafka.KafkaConsumerGroupI // Kafka consumer for rejected transactions
    invalidBlocksKafkaConsumerClient  kafka.KafkaConsumerGroupI // Kafka consumer for invalid blocks
    invalidSubtreeKafkaConsumerClient kafka.KafkaConsumerGroupI // Kafka consumer for invalid subtrees
    subtreeKafkaProducerClient        kafka.KafkaAsyncProducerI // Kafka producer for subtrees
    blocksKafkaProducerClient         kafka.KafkaAsyncProducerI // Kafka producer for blocks
    banList                           BanListI                  // List of banned peers
    banChan                           chan BanEvent             // Channel for ban events
    banManager                        *PeerBanManager           // Manager for peer banning
    gCtx                              context.Context
    blockTopicName                    string
    subtreeTopicName                  string
    miningOnTopicName                 string
    rejectedTxTopicName               string
    invalidBlocksTopicName            string   // Kafka topic for invalid blocks
    invalidSubtreeTopicName           string   // Kafka topic for invalid subtrees
    handshakeTopicName                string   // pubsub topic for version/verack
    blockPeerMap                      sync.Map // Map to track which peer sent each block (hash -> peerID)
    subtreePeerMap                    sync.Map // Map to track which peer sent each subtree (hash -> peerID)
}
```

The server manages several key components, each serving a specific purpose in the P2P network:

- The P2PNode handles direct peer connections and message routing through the P2PNodeI interface
- The various Kafka clients manage message distribution across the network
- The ban system maintains network security by managing peer access through BanListI and PeerBanManager
- The notification channel handles real-time event propagation

### P2PNodeI Interface

The P2P Server uses a P2PNodeI interface rather than a concrete P2PNode implementation to enable better testability:

```go
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
    DisconnectPeer(ctx context.Context, peerID peer.ID) error
    SendToPeer(ctx context.Context, pid peer.ID, msg []byte) error
    SetPeerConnectedCallback(callback func(context.Context, peer.ID))
    UpdatePeerHeight(peerID peer.ID, height int32)

    // Stats methods
    LastSend() time.Time
    LastRecv() time.Time
    BytesSent() uint64
    BytesReceived() uint64

    // Additional accessors
    GetProcessName() string
    UpdateBytesReceived(bytesCount uint64)
}
```

## Server Operations

### Server Initialization

The server initializes through the NewServer function:

```go
func NewServer( ctx context.Context,
    logger ulogger.Logger,
    tSettings *settings.Settings,
    blockchainClient blockchain.ClientI,
    rejectedTxKafkaConsumerClient kafka.KafkaConsumerGroupI,
    subtreeKafkaProducerClient kafka.KafkaAsyncProducerI,
    blocksKafkaProducerClient kafka.KafkaAsyncProducerI,
) (*Server, error)
```

This function establishes the server with required settings and dependencies, including:

- P2P network configuration (IP, port, topics)
- Topic name generation for various message types
- Ban list initialization
- Connection to blockchain services
- Kafka producer and consumer setup

### Health Management

The server implements comprehensive health checking through:

```go
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

This method performs two types of health verification:

For liveness checks, it verifies basic server operation without dependency checks.

For readiness checks, it verifies:

- Kafka broker connectivity
- Blockchain client functionality
- FSM state verification
- Block validation client status

### Server Lifecycle Management

The server manages its lifecycle through several key methods:

The Init method prepares the server for operation:

```go
func (s *Server) Init(ctx context.Context) (err error)
```

It verifies and adjusts HTTP/HTTPS settings based on security requirements and establishes necessary connection URLs.

The Start method initiates server operations:

```go
func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error
```

It begins:

- Kafka message processing
- Block validation client setup
- HTTP/WebSocket server operation
- P2P network communication
- Blockchain subscription monitoring
- Ban event processing
- Once initialization is complete, it signals readiness by closing the readyCh channel

The Stop method ensures graceful shutdown:

```go
func (s *Server) Stop(ctx context.Context) error
```

It manages the orderly shutdown of all server components and connections.

### Ban Management

The P2P Server implements a sophisticated peer banning system through several components:

```go
type BanEventHandler interface {
    OnPeerBanned(peerID string, until time.Time, reason string)
}
```

The `BanEventHandler` interface allows the system to react to ban events, which is implemented by the P2P Server.

```go
type BanListI interface {
    // IsBanned checks if a peer is banned by its IP address
    IsBanned(ipStr string) bool

    // Add adds an IP or subnet to the ban list with an expiration time
    Add(ctx context.Context, ipOrSubnet string, expirationTime time.Time) error

    // Remove removes an IP or subnet from the ban list
    Remove(ctx context.Context, ipOrSubnet string) error

    // ListBanned returns a list of all currently banned IP addresses and subnets
    ListBanned() []string

    // Subscribe returns a channel to receive ban events
    Subscribe() chan BanEvent

    // Unsubscribe removes a subscription to ban events
    Unsubscribe(ch chan BanEvent)

    // Init initializes the ban list and starts any background processes
    Init(ctx context.Context) error

    // Clear removes all entries from the ban list and cleans up resources
    Clear()
}
```

The `BanListI` interface defines the contract for managing banned peers by IP address or subnet, with methods for adding, removing, listing, and checking ban status. The system uses an SQL-backed implementation that persists ban information.

```go
type PeerBanManager struct {
    ctx           context.Context
    mu            sync.RWMutex
    peerBanScores map[string]*BanScore
    reasonPoints  map[BanReason]int
    banThreshold  int
    banDuration   time.Duration
    decayInterval time.Duration
    decayAmount   int
    handler       BanEventHandler
}
```

The `PeerBanManager` maintains scores for peers and automatically bans them when they exceed a threshold. It provides methods like:

- `AddScore`: Increments a peer's score for specific violations
- `GetBanScore`: Retrieves the current ban score and status
- `IsBanned`: Checks if a peer is currently banned
- `ListBanned`: Returns all currently banned peers
- `ResetBanScore`: Clears a peer's ban score

The system defines standard ban reasons with associated scoring:

```go
type BanReason int

const (
    ReasonUnknown BanReason = iota
    ReasonInvalidSubtree     // 10 points
    ReasonProtocolViolation  // 20 points
    ReasonSpam              // 50 points
)
```

Peer scores automatically decay over time to allow for recovery from temporary issues.

### Message Handlers

- `handleHandshakeTopic`: Handles incoming handshake messages.
- `handleBlockTopic`: Handles incoming block messages.
- `handleSubtreeTopic`: Handles incoming subtree messages.
- `handleMiningOnTopic`: Handles incoming mining-on messages.
- `handleBanEvent`: Handles banning and unbanning events.

## Key Processes

### P2P Communication

The server uses a P2P node for communication with other peers in the network. It publishes and subscribes to various topics such as best blocks, blocks, subtrees, and mining-on notifications.

### Blockchain Interaction

The server interacts with the blockchain client to retrieve and validate block information. It also subscribes to blockchain notifications to propagate relevant information to peers.

### Kafka Integration

The server uses Kafka producers and consumers for efficient distribution of block and subtree data across the network.

## Configuration

The following settings can be configured for the p2p service:

- `p2p_listen_addresses`: Specifies the IP addresses for the P2P service to bind to.
- `p2p_advertise_addresses`: Addresses to advertise to other peers in the network. Each address can be specified with or without a port (e.g., `192.168.1.1` or `example.com:9906`). When a port is not specified, the system will use the value from `p2p_port` as the default. Both IP addresses and domain names are supported. Format examples: `192.168.1.1`, `example.com:9906`, `node.local:8001`.
- `p2p_port`: Defines the port number on which the P2P service listens.
- `p2p_block_topic`: The topic name used for block-related messages in the P2P network.
- `p2p_subtree_topic`: Specifies the topic for subtree-related messages within the P2P network.
- `p2p_handshake_topic`: Defines the topic for peer handshake messages, used for version and verack exchanges.
- `p2p_mining_on_topic`: The topic used for messages related to the start of mining a new block.
- `p2p_rejected_tx_topic`: Specifies the topic for broadcasting information about rejected transactions.
- `p2p_shared_key`: A shared key for securing P2P communications, required for private network configurations.
- `p2p_dht_protocol_id`: Identifier for the DHT protocol used by the P2P network.
- `p2p_dht_use_private`: A boolean flag indicating whether a private Distributed Hash Table (DHT) should be used, enhancing network privacy.
- `p2p_optimise_retries`: A boolean setting to optimize retry behavior in P2P communications, potentially improving network efficiency.
- `p2p_static_peers`: A list of static peer addresses to connect to, ensuring the P2P node can always reach known peers.
- `p2p_private_key`: The private key for the P2P node, used for secure communications within the network. If not provided, a new Ed25519 key is automatically generated and persistently stored in the blockchain database.
- `p2p_http_address`: Specifies the HTTP address for external clients to connect to the P2P service.
- `p2p_http_listen_address`: Specifies the HTTP listen address for the P2P service, enabling HTTP-based interactions.
- `p2p_grpc_address`: Specifies the gRPC address for external clients to connect to the P2P service.
- `p2p_grpc_listen_address`: Specifies the gRPC listen address for the P2P service.
- `p2p_bootstrap_addresses`: List of bootstrap peer addresses for initial network discovery.
- `p2p_bootstrap_persistent`: A boolean flag (default: false) that controls whether bootstrap addresses are treated as persistent connections that automatically reconnect after disconnection.
- `p2p_ban_threshold`: Score threshold at which peers are banned from the network.
- `p2p_ban_duration`: Duration of time a peer remains banned after exceeding the ban threshold.
- `securityLevelHTTP`: Defines the security level for HTTP communications, where a higher level might enforce HTTPS.
- `server_certFile` and `server_keyFile`: These settings specify the paths to the SSL certificate and key files, respectively, required for setting up HTTPS.
- `p2p_ban_default_duration`: Specifies the default duration for peer bans (defaults to 24 hours if not set).
- `p2p_ban_persist_path`: Defines the path where ban list information is stored persistently.
- `p2p_ban_max_entries`: Sets the maximum number of entries allowed in the ban list to prevent memory exhaustion.

## Dependencies

The P2P Server depends on several components:

- `blockchain.ClientI`: Interface for blockchain operations
- `blockvalidation.Client`: Client for block validation operations
- `p2p.P2PNode`: Node for P2P communication
- Kafka producers and consumers for message distribution

These dependencies are injected into the `Server` struct during initialization.

## Error Handling

Errors are logged using the provided logger. Critical errors may result in the server shutting down or specific components failing to start.

## Concurrency

The server uses goroutines for handling concurrent operations, such as message processing, HTTP server, and blockchain subscription listening. It also uses contexts for cancellation and timeout management.

## Security

The server supports both HTTP and HTTPS configurations based on the `securityLevelHTTP` setting. When using HTTPS, it requires certificate and key files to be specified in the configuration.
